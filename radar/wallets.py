import asyncio
import json
import time
from collections import defaultdict

from websockets.asyncio.client import connect

from .sources import event_id, number


def fill_id(fill):
    return event_id(*(fill.get(k) for k in ("coin", "tid", "hash", "oid", "time", "px", "sz", "dir")))


def opens_position(fill):
    direction = fill.get("dir", "")
    return ("long" if direction in {"Open Long", "Short > Long"} else
            "short" if direction in {"Open Short", "Long > Short"} else "")


def summarize(fills, fundings):
    ledger, trade_sizes, closes = [], [], 0
    for fill in fills:
        if str(fill.get("coin", "")).startswith("@") or fill.get("dir") in {"Buy", "Sell"}:
            continue
        if fill.get("feeToken", "USDC") not in {"USDC", "USDT"}:
            continue
        start = number(fill.get("startPosition"))
        size = number(fill.get("sz"))
        sign = 1 if fill.get("side") == "B" else -1
        if start and start * sign < 0 and size >= abs(start) - 1e-10:
            closes += 1
        pnl = number(fill.get("closedPnl")) - number(fill.get("fee"))
        ledger.append((int(number(fill.get("time"))), pnl))
        trade_sizes.append(size * number(fill.get("px")))
    for funding in fundings:
        ledger.append((int(number(funding.get("time"))), number((funding.get("delta") or {}).get("usdc"))))
    balance = peak = drawdown = 0.0
    for _, change in sorted(ledger):
        balance += change
        peak = max(peak, balance)
        drawdown = max(drawdown, peak - balance)
    return {"net_pnl": round(balance, 2), "pnl_drawdown": round(drawdown, 2), "closed": closes,
            "fills": len(trade_sizes), "avg_trade": round(sum(trade_sizes) / max(1, len(trade_sizes)), 2)}


class WalletMonitor:
    def __init__(self, source, store, config, emit):
        self.source, self.store, self.config, self.emit = source, store, config, emit
        self.addresses = list(store.get("wallets", "list") or [])
        self.live = self.addresses[:config.live_wallets]
        self.last_scan = 0
        self.errors = 0
        self.wallet_error = ""
        self.ws_connected = False
        self.discovered = asyncio.Event()
        if self.addresses:
            self.discovered.set()

    async def discover(self):
        picked = await self.source.candidates()
        if not picked:
            raise ValueError("No qualifying wallet addresses returned")
        self.addresses = picked
        self.live = self.addresses[:self.config.live_wallets]
        self.store.put("wallets", "list", picked)
        self.discovered.set()

    def ingest(self, address, fills, funding=None):
        now = int(time.time())
        old = self.store.get("wallet_sample", address) or {}
        current = {fill_id(row): row for row in old.get("fills", [])}
        prior_stats = old.get("stats", {})
        eligible = (old.get("funding_checked") and prior_stats.get("closed", 0) >= 12
                    and prior_stats.get("net_pnl", 0) > 0)
        cutoff_ms = (now - 7 * 86400) * 1000
        observed = defaultdict(list)
        for row in fills:
            fill_time = int(number(row.get("time")))
            if fill_time < cutoff_ms or fill_time > (now + 10) * 1000:
                continue
            fid = fill_id(row)
            duplicate = fid in current
            current[fid] = row
            side = opens_position(row)
            asset = str(row.get("coin", "")).upper()
            if (not duplicate and eligible and side and asset and not str(asset).startswith("@")
                    and now - fill_time / 1000 <= self.config.wallet_poll + 25):
                observed[(asset, side)].append(row)
        wallet_fills = sorted((r for r in current.values() if number(r.get("time")) >= cutoff_ms),
                              key=lambda r: number(r["time"]))[-2500:]
        fundings = funding if funding is not None else old.get("funding", [])
        fundings = [entry for entry in fundings if number(entry.get("time")) >= cutoff_ms]
        summary = summarize(wallet_fills, fundings)
        self.store.put("wallet_sample", address, {
            "fills": wallet_fills, "funding": fundings, "stats": summary,
            "funding_checked": now if funding is not None else old.get("funding_checked", 0),
            "ts": now,
        })
        latest = {str(row.get("coin", "")).upper(): row for row in wallet_fills}
        for (asset, side), rows in observed.items():
            recent = latest[asset]
            existing_position = number(recent.get("startPosition")) + (
                (1 if recent.get("side") == "B" else -1) * number(recent.get("sz")))
            if existing_position * (1 if side == "long" else -1) <= 0:
                continue
            last = max(rows, key=lambda row: number(row["time"]))
            amount = sum(number(row.get("sz")) * number(row.get("px")) for row in rows)
            if amount < max(10_000, prior_stats["avg_trade"] * 2):
                continue
            self.emit({
                "id": event_id("wallet", address, fill_id(last)), "asset": asset, "kind": "wallet",
                "title": f"Hyperliquid: крупное открытие {side}, объём {amount:,.0f} USD",
                "text": (f"Цена {last.get('px')}. По доступному 7-дневному окну до сделки: "
                         f"реализованный PnL после учтённых комиссий и funding {prior_stats['net_pnl']:.2f} USD, "
                         f"полных закрытий {prior_stats['closed']}, "
                         f"просадка накопленного PnL {prior_stats['pnl_drawdown']:.2f} USD. "
                         "История ограничена доступными исполнениями."),
                "ts": int(number(last["time"]) / 1000), "direction": side, "address": address,
                "url": f"https://app.hyperliquid.xyz/explorer/address/{address}",
                "source": "Hyperliquid",
            })

    async def scan(self):
        await self.discovered.wait()
        self.errors = 0
        self.wallet_error = ""
        for address in tuple(self.addresses):
            try:
                last = self.store.get("wallet_sample", address) or {}
                now = int(time.time())
                fills = last.get("fills", [])
                start = max((int(number(f.get("time"))) for f in fills), default=(now - 7 * 86400) * 1000)
                rows = await self.source.info("userFillsByTime", user=address, startTime=start)
                if len(rows) >= 2000 and last:
                    # A very active wallet may exceed the incremental endpoint's page size.
                    # Re-anchor to recent executions and do not imply complete seven-day coverage.
                    rows = await self.source.info("userFills", user=address)
                funding = None
                if now - last.get("funding_checked", 0) > 6 * 3600:
                    funding = await self.source.info("userFunding", user=address, startTime=(now - 7 * 86400) * 1000)
                self.ingest(address, rows, funding)
            except Exception as exc:
                self.errors += 1
                self.wallet_error = str(exc) if type(exc).__name__ == "UpstreamError" else type(exc).__name__
        self.last_scan = time.time()

    async def stream(self):
        backoff = 2
        while True:
            live = tuple(self.live)
            if not live:
                await asyncio.sleep(5)
                continue
            try:
                async with connect("wss://api.hyperliquid.xyz/ws", open_timeout=10, close_timeout=3) as socket:
                    for address in live:
                        await socket.send(json.dumps({"method": "subscribe",
                                                      "subscription": {"type": "userFills", "user": address}}))
                    self.ws_connected = True
                    backoff = 2
                    while tuple(self.live) == live:
                        try:
                            message = json.loads(await asyncio.wait_for(socket.recv(), 35))
                        except TimeoutError:
                            await socket.send('{"method":"ping"}')
                            continue
                        if message.get("channel") == "userFills":
                            result = message["data"]
                            address = str(result["user"]).lower()
                            if address in live:
                                self.ingest(address, result.get("fills", []))
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            finally:
                self.ws_connected = False
