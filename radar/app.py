import asyncio
import logging
import re
import signal
import time
from collections import defaultdict, deque
from datetime import UTC, datetime
from urllib.parse import urlsplit

import httpx
from aiohttp import web

from .agents import BudgetUnavailable, Models, fresh, paper_step, scenarios
from .config import Config
from .sources import Bybit, Hyperliquid, UpstreamError, cryptopanic, event_id, rss, x_recent
from .store import Store
from .telegram import LABELS, Telegram
from .wallets import WalletMonitor

LOG = logging.getLogger("radar")
NOISY = {"A", "AI", "AN", "ARE", "AS", "AT", "BE", "BY", "FOR", "GO", "HAS", "IN", "IS", "IT",
         "NEW", "NO", "OF", "ON", "OR", "TO", "UP", "US", "USD", "USDT", "WE", "WHO"}
ALIASES = {"CHAINLINK": "LINK", "HYPERLIQUID": "HYPE", "AVALANCHE": "AVAX", "ARBITRUM": "ARB",
           "SUI NETWORK": "SUI", "PENDLE FINANCE": "PENDLE", "INJECTIVE": "INJ",
           "OPTIMISM": "OP", "DOGECOIN": "DOGE", "LITECOIN": "LTC", "AAVE": "AAVE"}


def matched_assets(row, available):
    title = row["title"]
    tags = {tag.upper() for tag in row.get("tags", [])}
    explicit = set(re.findall(r"\$([A-Z0-9]{2,18})\b|\b([A-Z0-9]{2,18})[/_-]?USDT\b", title))
    symbols = tags | {s for pair in explicit for s in pair if s}
    symbols.update(set(re.findall(r"\b[A-Z][A-Z0-9]{2,12}\b", title)) - NOISY)
    for name, symbol in ALIASES.items():
        if re.search(r"\b" + name + r"\b", title.upper()):
            symbols.add(symbol)
    return symbols & available


class Radar:
    def __init__(self, config, store, client):
        self.config, self.store, self.client = config, store, client
        self.bybit = Bybit(client, config)
        self.llm = Models(client, store, config)
        self.wallets = WalletMonitor(Hyperliquid(client, config), store, config, self.add_event)
        self.telegram = Telegram(client, store, config, self.report)
        self.quotes = {}
        self.history = defaultdict(lambda: deque(maxlen=72))
        self.status = {}
        self.model_hold_until = 0

    async def loop(self, name, action, interval):
        while True:
            started = time.monotonic()
            try:
                await action()
                self.status[name] = (time.time(), "")
            except Exception as exc:
                message = str(exc) if isinstance(exc, UpstreamError) else type(exc).__name__
                self.status[name] = (time.time(), message)
                LOG.warning("%s: %s", name, message)
                if interval < 15:
                    await asyncio.sleep(5)
            await asyncio.sleep(max(.05, interval - (time.monotonic() - started)))

    def add_event(self, data):
        if data["asset"] not in self.config.excluded:
            self.store.put("event", data["id"], data, replace=False)

    async def market(self):
        self.quotes = await self.bybit.quotes()
        for key in tuple(self.history):
            if key not in self.quotes:
                del self.history[key]
        triggers = []
        now = int(time.time())
        for key, quote in self.quotes.items():
            if not fresh(quote) or not quote["eligible"]:
                continue
            history = self.history[key]
            while history and history[0][0] < now - 1080:
                history.popleft()
            old = next((point for point in reversed(history) if point[0] <= now - 840), None)
            history.append((now, quote["price"], quote["open_interest"]))
            if not old:
                continue
            move = (quote["price"] / old[1] - 1) * 100
            oi = (quote["open_interest"] / old[2] - 1) * 100 if old[2] > 0 else 0
            if abs(move) < 2 or abs(move) > 12 or quote["spread_bps"] > self.config.spread_bps:
                continue
            if not self.store.get("market_bucket", key + ":" + str(now // 1800)):
                triggers.append((abs(move) + abs(oi) * .05, key, quote, move, oi))
        for _, key, quote, move, oi in sorted(triggers, reverse=True)[:3]:
            bucket = key + ":" + str(now // 1800)
            if self.store.put("market_bucket", bucket, True, replace=False):
                self.add_event({"id": event_id("market", bucket), "market": key,
                                "asset": quote["asset"], "kind": "market", "source": "Bybit",
                                "title": f"Движение Bybit за ~15 минут {move:+.2f}%",
                                "text": f"Изменение открытого интереса {oi:+.2f}%. Цена {quote['price']}.",
                                "ts": quote["ts"], "url": "https://bybit-exchange.github.io/docs/v5/market/tickers"})

    async def news(self):
        if not self.quotes:
            return
        available = {q["asset"] for q in self.quotes.values() if q["eligible"]}
        sources = [("Bybit", self.bybit.announcements())]
        sources.extend((urlsplit(url).hostname, rss(self.client, url)) for url in self.config.feeds)
        if self.config.news_key:
            sources.append(("CryptoPanic", cryptopanic(self.client, self.config)))
        if self.config.x_key and self.config.x_query:
            sources.append(("X", x_recent(self.client, self.config)))
        results = await asyncio.gather(*(call for _, call in sources), return_exceptions=True)
        for (source, _), rows in zip(sources, results):
            if isinstance(rows, BaseException):
                message = str(rows) if isinstance(rows, UpstreamError) else type(rows).__name__
                self.status[f"news/{source}"] = (time.time(), message)
                continue
            self.status[f"news/{source}"] = (time.time(), "")
            for row in rows:
                if not row["title"] or not row["url"] or not 0 <= time.time() - row["ts"] <= 7200:
                    continue
                kind = row["kind"]
                if kind == "media" and re.search(r"\b(rumou?r|could|may|reportedly|unconfirmed|allegedly|unlock|proposal|governance|vote)\b",
                                                 row["title"], re.I):
                    kind = "early"
                headline = re.sub(r"[\W_]+", "", row["title"].casefold())
                for asset in sorted(matched_assets(row, available))[:8]:
                    # Normalize syndicated headlines across feed URLs.
                    self.add_event({**row, "asset": asset, "kind": kind,
                                    "id": event_id("news", asset, headline)})

    def record(self, key, state, event, stage_results=None, details=""):
        self.store.put("audit", key, {"state": state, "event": event, "stages": stage_results or [],
                                      "details": details, "ts": int(time.time())})

    def evidence(self, event):
        out = [event]
        for key, item in self.store.recent("event", time.time() - 7200, 600):
            if (key != event["id"] and item["asset"] == event["asset"]
                    and item["kind"] != "market" and abs(item["ts"] - event["ts"]) <= 3600):
                out.append(item)
            if len(out) >= 5:
                break
        return out

    async def analyse(self, event):
        key = event["id"]
        existing = self.store.get("audit", key)
        if existing and existing["state"] != "deferred":
            return
        age = time.time() - event["ts"]
        max_age = self.config.wallet_poll + 120 if event["kind"] == "wallet" else (
            300 if event["kind"] == "market" else 3600)
        if not 0 <= age <= max_age:
            self.record(key, "expired", event)
            return
        candidates = [(k, q) for k, q in self.quotes.items() if q["asset"] == event["asset"]
                      and q["eligible"] and fresh(q)
                      and (event["kind"] != "wallet" or q["category"] == "linear")]
        if event.get("market"):
            candidates = [(k, q) for k, q in candidates if k == event["market"]]
        if not candidates:
            return
        market_key, quote = max(candidates, key=lambda pair: pair[1]["turnover"])
        if self.store.get("idea", key):
            self.record(key, "approved", event, (existing or {}).get("stages"))
            return
        cooldown = self.store.get("cooldown", market_key) or 0
        if time.time() < cooldown:
            self.record(key, "watch", event, details="У инструмента уже был недавний сигнал.")
            return
        raw = self.evidence(event)
        ids = {item["id"] for item in raw}
        output = []
        try:
            screen = await self.llm.analyse("screen", key, {"evidence": raw}, ["keep", "watch"], ids)
            output.append(screen)
            if screen["decision"] == "watch":
                self.record(key, "watch", event, output)
                return
            if event["kind"] == "early" and not any(
                    item["kind"] == "official" and item["id"] in screen["evidence"] for item in raw):
                self.record(key, "watch", event, output, "Ранний сигнал ждёт подтверждения первоисточника.")
                return
            plans = scenarios(quote, await self.bybit.candles(quote), self.config)
            if event.get("direction"):
                plans = {side: plan for side, plan in plans.items() if side == event["direction"]}
            if not plans:
                self.record(key, "watch", event, output, "Фильтр спреда, волатильности или прибыль/риск.")
                return
            # Audit uses the raw evidence and computed levels without the previous model's conclusion.
            packet = {"evidence": raw, "quote": quote, "plans": plans, "orderbook": await self.bybit.depth(quote)}
            scenario = await self.llm.analyse("scenario", key, packet, [*plans, "watch"], ids)
            output.append(scenario)
            side = scenario["decision"]
            if side not in plans:
                self.record(key, "watch", event, output)
                return
            plan = plans[side]
            audit = await self.llm.analyse("review", key, {**packet, "side": side}, ["approve", "veto"], ids)
            output.append(audit)
            if audit["decision"] != "approve":
                self.record(key, "veto", event, output)
                return
            current = self.quotes.get(market_key)
            if (not fresh(current) or not current["eligible"] or current["spread_bps"] > self.config.spread_bps
                    or (current["ask" if plan["side"] > 0 else "bid"] - plan["trigger"]) * plan["side"] >= 0
                    or (current["price"] - plan["stop"]) * plan["side"] <= 0):
                self.record(key, "watch", event, output, "Рынок изменился во время проверки.")
                return
            now = int(time.time())
            with self.store.transaction():
                self.store.put("idea", key, {"key": market_key, "created": now, "valid_until": now + 2700,
                                            "state": "waiting", "reason": scenario["reason"], "plan": plan,
                                            "max_spread_bps": self.config.spread_bps, "evidence": raw},
                               replace=False)
                self.store.put("cooldown", market_key, now + 1800)
                self.record(key, "approved", event, output)
        except BudgetUnavailable:
            self.record(key, "deferred", event, output, "Лимит OpenRouter или отсутствует ключ.")
            self.model_hold_until = time.time() + 120

    async def process(self):
        if not self.config.router_key or time.time() < self.model_hold_until or not self.quotes:
            return
        priority = {"official": 0, "wallet": 1, "market": 2, "media": 3, "early": 4}
        recent = self.store.recent("event", time.time() - 3600, 500)
        pending = [event for key, event in recent
                   if (self.store.get("audit", key) or {}).get("state") in {None, "deferred"}]
        pending.sort(key=lambda e: (priority.get(e["kind"], 9), -e["ts"]))
        for event in pending[:4]:
            await self.analyse(event)
            if time.time() < self.model_hold_until:
                break

    async def delivery(self):
        for key, idea in self.store.recent("idea", 0, 250):
            if self.config.telegram_token:
                try:
                    await self.telegram.publish(key, idea)
                except UpstreamError as exc:
                    self.status["telegram/send"] = (time.time(), str(exc))
                else:
                    self.status["telegram/send"] = (time.time(), "")
            now = int(time.time())
            announcement = self.store.get("telegram_message", key)
            if idea["state"] == "waiting" and not announcement and now < idea["valid_until"]:
                continue
            changes = paper_step(idea, self.quotes.get(idea["key"]), now)
            if changes:
                self.store.put("idea", key, {**idea, **changes})

    def report(self, command, arg):
        if command == "why":
            audit = self.store.get("audit", arg)
            if not audit:
                return "Запись не найдена. Используй /history."
            idea = self.store.get("idea", arg)
            event = audit["event"]
            rows = idea["evidence"] if idea else [event]
            lines = [f"{event['asset']} · {arg} · {audit['state']}", audit.get("details", "")]
            lines.extend(f"{item['id']} · {item['source']} · {item['title']}\n{item['url']}" for item in rows)
            lines.extend(f"{stage['model']}: {stage['reason']}\nДоказательства: {', '.join(stage['evidence'])}"
                         for stage in audit["stages"])
            return "\n\n".join(filter(None, lines))
        if command == "cost":
            entries = self.store.usage()
            actual = sum(amount for _, state, amount, *_ in entries if state == "settled")
            reserve = sum(amount for _, state, amount, *_ in entries if state == "reserved")
            lines = [f"OpenRouter сегодня по UTC: ${actual / 1e6:.4f} из ${self.config.budget}",
                     f"Зарезервировано или стоимость ещё не подтверждена: ${reserve / 1e6:.4f}"]
            lines.extend(f"{model}: ${amount / 1e6:.4f} · {calls} выз. · {tin}/{tout} токенов"
                         for model, state, amount, calls, tin, tout in entries if state == "settled")
            return "\n".join(lines)
        if command == "status":
            count = sum(q["eligible"] and bool(fresh(q)) for q in self.quotes.values())
            last = int(time.time() - self.wallets.last_scan) if self.wallets.last_scan else "ещё нет"
            lines = [f"Свежих пар выше порога: {count}", f"Кошельков: {len(self.wallets.addresses)}",
                     f"WebSocket: {'подключён' if self.wallets.ws_connected else 'ожидание'}",
                     f"Последний полный опрос: {last} с назад · ошибок {self.wallets.errors}",
                     f"OpenRouter: {'ключ задан' if self.config.router_key else 'ожидание ключа'}"]
            if self.wallets.wallet_error:
                lines.append(self.wallets.wallet_error)
            lines.extend(f"{name}: {message}" for name, (_, message) in self.status.items() if message)
            return "\n".join(lines)
        if command == "watchlist":
            tracked = sorted((s for s in self.quotes.values() if s["eligible"] and fresh(s)),
                             key=lambda s: -s["turnover"])
            return "\n".join([f"Пар выше порога: {len(tracked)}. Первые 25 по обороту:"] +
                             [f"{q['symbol']} {q['category']} · {q['turnover'] / 1e6:.1f} млн USDT"
                              for q in tracked[:25]])
        if command == "wallets":
            return "\n".join([f"Наблюдаем {len(self.wallets.addresses)} адресов. Первые 20:"] +
                             [f"{'LIVE' if a in self.wallets.live else 'REST'} {a}"
                              for a in self.wallets.addresses[:20]])
        if command in {"history", "digest"}:
            since = time.time() - 86400 if command == "digest" else 0
            ideas = self.store.recent("idea", since, 15)
            audits = self.store.recent("audit", since, 10)
            lines = [f"{k} · {d['key']} · {LABELS[d['state']]}" +
                     (f" · {d['pnl_pct']:+.2f}%" if "pnl_pct" in d else "") for k, d in ideas]
            lines.extend(f"{k} · {d['event']['asset']} · {d['state']}" for k, d in audits
                         if d["state"] != "approved")
            return "\n".join(lines) or "История пока пуста."
        return ("Crypto Radar подключён к этому чату. Торговля вручную.\n"
                "/status — источники\n/watchlist — пары\n/wallets — кошельки\n"
                "/cost — расходы\n/history — идеи и отказы\n/why ID — доказательства\n/digest — сутки")

    async def housekeep(self):
        now = time.time()
        for kind, days in (("event", 3), ("model", 7), ("market_bucket", 2), ("cooldown", 2)):
            self.store.delete_before(kind, now - days * 86400)
        for kind in ("audit", "idea", "telegram_message", "wallet_sample"):
            self.store.delete_before(kind, now - 120 * 86400)
        day = datetime.now(UTC).date().isoformat()
        if (self.config.telegram_token and self.telegram.chat and datetime.now(UTC).hour == 20
                and not self.store.get("digest", day)):
            await self.telegram.send("Сводка за сутки\n" + self.report("digest", ""))
            self.store.put("digest", day, True)


async def run():
    config = Config.load()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    with Store(config.database) as store:
        async with httpx.AsyncClient(follow_redirects=False, headers={"User-Agent": "CryptoRadar/1.0"}) as client:
            radar = Radar(config, store, client)
            app = web.Application()

            async def health(_):
                store.execute("SELECT 1").fetchone()
                return web.json_response({"ok": True, "market_fresh": any(fresh(q) for q in radar.quotes.values()),
                                          "models_configured": bool(config.router_key),
                                          "telegram_configured": bool(config.telegram_token)})

            app.router.add_get("/health", health)
            runner = web.AppRunner(app, access_log=None)
            await runner.setup()
            await web.TCPSite(runner, "0.0.0.0", config.port).start()
            loops = [
                ("bybit", radar.market, 15), ("news", radar.news, 90),
                ("wallet discovery", radar.wallets.discover, 12 * 3600),
                ("wallet poll", radar.wallets.scan, config.wallet_poll),
                ("signals", radar.process, 6), ("delivery", radar.delivery, 4),
                ("housekeeping", radar.housekeep, 60),
            ]
            if config.telegram_token:
                loops.append(("telegram/poll", radar.telegram.poll, .1))
            tasks = [asyncio.create_task(radar.loop(*params)) for params in loops]
            tasks.append(asyncio.create_task(radar.wallets.stream()))
            LOG.info("Radar started")
            try:
                await stop.wait()
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await runner.cleanup()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run())
