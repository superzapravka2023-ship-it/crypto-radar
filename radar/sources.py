import asyncio
import hashlib
import math
import re
import time
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from html import unescape
from urllib.parse import urlsplit

import httpx

from .config import ADDRESS


class UpstreamError(Exception):
    def __init__(self, service, code):
        self.code = code
        super().__init__(f"{service}: {code}")


async def request(client, service, method, url, *, raw=False, timeout=20, **kwargs):
    try:
        response = await client.request(method, url, timeout=timeout, **kwargs)
        response.raise_for_status()
        return response.content if raw else response.json()
    except httpx.HTTPStatusError as exc:
        raise UpstreamError(service, exc.response.status_code) from None
    except (httpx.RequestError, ValueError):
        raise UpstreamError(service, "invalid response or connection") from None


def number(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else 0.0
    except (ValueError, TypeError, OverflowError):
        return 0.0


def clean(s, limit):
    return " ".join(unescape(re.sub(r"<[^>]*>", " ", str(s or ""))).split())[:limit]


def web_url(link):
    try:
        parsed = urlsplit(str(link or ""))
        if parsed.scheme in {"http", "https"} and parsed.hostname and parsed.username is None:
            return str(link)[:1024]
    except ValueError:
        pass
    return ""


def event_id(*parts):
    return hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:20]


def timestamp(value):
    if not value:
        return 0
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except (ValueError, OverflowError, TypeError):
        try:
            return int(parsedate_to_datetime(value).timestamp())
        except (ValueError, OverflowError, TypeError):
            return 0


class Bybit:
    def __init__(self, client, config):
        self.client = client
        self.base = config.bybit
        self.config = config
        self.instruments = {}
        self.refreshed = 0

    async def get(self, path, **params):
        data = await request(self.client, "Bybit", "GET", self.base + path, params=params)
        if data.get("retCode") != 0:
            raise UpstreamError("Bybit", data.get("retCode"))
        return data

    async def update_instruments(self):
        async def load(category):
            mapping = {}
            params = {"category": category, "status": "Trading"}
            if category == "linear":
                params["limit"] = 1000
            while True:
                result = (await self.get("/v5/market/instruments-info", **params))["result"]
                for item in result["list"]:
                    base = item["baseCoin"].upper()
                    if item.get("quoteCoin") != "USDT" or item.get("status") != "Trading":
                        continue
                    if category == "linear" and item.get("contractType") != "LinearPerpetual":
                        continue
                    if re.sub(r"^\d+", "", base) in self.config.excluded:
                        continue
                    tick = number(item["priceFilter"].get("tickSize"))
                    if tick > 0:
                        mapping[f"{category}:{item['symbol']}"] = (base, tick)
                cursor = result.get("nextPageCursor")
                if not cursor or cursor == params.get("cursor"):
                    return mapping
                params["cursor"] = cursor

        spot, linear = await asyncio.gather(load("spot"), load("linear"))
        self.instruments = spot | linear
        self.refreshed = time.time()

    async def quotes(self):
        if time.time() - self.refreshed > 1800:
            await self.update_instruments()

        async def load(category):
            data = await self.get("/v5/market/tickers", category=category)
            now = int(number(data.get("time")) / 1000)
            quotes = {}
            for item in data["result"]["list"]:
                symbol = item["symbol"]
                key = f"{category}:{symbol}"
                if key not in self.instruments:
                    continue
                base, tick = self.instruments[key]
                turnover = number(item.get("turnover24h"))
                price = number(item.get("lastPrice"))
                bid, ask = number(item.get("bid1Price")), number(item.get("ask1Price"))
                if price <= 0 or not 0 < bid <= ask:
                    continue
                quotes[key] = {
                    "symbol": symbol, "category": category, "asset": base, "tick": tick,
                    "turnover": turnover, "price": price, "bid": bid, "ask": ask, "ts": now,
                    "spread_bps": (ask - bid) / ((ask + bid) / 2) * 10000,
                    "funding": number(item.get("fundingRate")),
                    "next_funding": int(number(item.get("nextFundingTime")) / 1000),
                    "open_interest": number(item.get("openInterestValue")),
                    "eligible": turnover >= self.config.turnover,
                }
            return quotes

        spot, linear = await asyncio.gather(load("spot"), load("linear"))
        return spot | linear

    async def candles(self, quote):
        data = await self.get("/v5/market/kline", category=quote["category"], symbol=quote["symbol"],
                              interval="15", limit=25)
        end = time.time() - 900
        return [tuple(map(number, row[1:5])) for row in reversed(data["result"]["list"])
                if number(row[0]) / 1000 <= end]

    async def depth(self, quote):
        data = await self.get("/v5/market/orderbook", category=quote["category"],
                              symbol=quote["symbol"], limit=25)
        result = data["result"]
        mid = (quote["ask"] + quote["bid"]) / 2
        def notional(side):
            return round(sum(number(px) * number(size) for px, size in result.get(side, [])
                             if abs(number(px) / mid - 1) <= .005), 2)
        return {"bids_within_50bps_usdt": notional("b"), "asks_within_50bps_usdt": notional("a")}

    async def announcements(self):
        result = await self.get("/v5/announcements/index", locale="en-US", limit=30)
        return [
            {"title": clean(item["title"], 350), "text": clean(item.get("description"), 1400),
             "source": "Bybit", "kind": "official", "url": web_url(item.get("url")),
             "ts": int(number(item.get("publishTime")) / 1000)}
            for item in result["result"]["list"]
        ]


async def rss(client, url):
    data = await request(client, "RSS", "GET", url, raw=True, follow_redirects=True)
    if len(data) > 4_000_000 or b"<!DOCTYPE" in data.upper():
        raise UpstreamError("RSS", "feed too large or unsupported XML")
    root = ET.fromstring(data)
    rows = []
    for item in root.iter():
        if item.tag.rsplit("}", 1)[-1] not in {"item", "entry"}:
            continue
        fields = {node.tag.rsplit("}", 1)[-1]: node for node in item}

        def read(*names):
            return next((fields[name].text for name in names if name in fields and fields[name].text), "")

        link = read("link")
        if not link and "link" in fields:
            link = fields["link"].attrib.get("href", "")
        rows.append({"title": clean(read("title"), 350), "text": clean(read("description", "summary"), 1400),
                     "ts": timestamp(read("pubDate", "published", "updated")), "source": urlsplit(url).hostname,
                     "url": web_url(link), "kind": "media"})
    return rows[:40]


async def cryptopanic(client, config):
    value = await request(client, "CryptoPanic", "GET", config.news_api,
                          params={"auth_token": config.news_key, "kind": "news"})
    return [
        {"title": clean(row.get("title"), 350), "text": clean(row.get("description"), 1400),
         "ts": timestamp(row.get("published_at")), "source": (row.get("source") or {}).get("title", "CryptoPanic"),
         "url": web_url(row.get("original_url") or row.get("url")), "kind": "media",
         "tags": [str(item.get("code", "")) for item in row.get("instruments", row.get("currencies", []))]}
        for row in value.get("results", [])[:40]
    ]


async def x_recent(client, config):
    data = await request(client, "X", "GET", "https://api.x.com/2/tweets/search/recent",
                         headers={"Authorization": "Bearer " + config.x_key},
                         params={"query": config.x_query, "max_results": 20, "tweet.fields": "created_at"})
    return [{"title": clean(row["text"], 350), "text": clean(row["text"], 1400),
             "ts": timestamp(row.get("created_at")), "source": "X", "kind": "early",
             "url": f"https://x.com/i/web/status/{row['id']}"}
            for row in data.get("data", []) if str(row.get("id", "")).isdigit()]


class Hyperliquid:
    def __init__(self, client, config):
        self.client = client
        self.config = config
        self.rest_lock = asyncio.Lock()
        self.next_rest = 0.0

    async def info(self, kind, **body):
        # Serialize weighted requests and leave room for funding, reconnects and busy addresses.
        async with self.rest_lock:
            await asyncio.sleep(max(0, self.next_rest - time.monotonic()))
            started = time.monotonic()
            self.next_rest = started + 1.4
            rows = await request(self.client, "Hyperliquid", "POST", "https://api.hyperliquid.xyz/info",
                                 json={"type": kind, **body})
            weight = 20 + (len(rows) + 19) // 20 if isinstance(rows, list) else 20
            self.next_rest = started + max(1.4, weight * 60 / 1000)
            return rows

    async def candidates(self):
        if self.config.addresses:
            return list(self.config.addresses[:self.config.wallet_count])
        if self.config.nansen_key:
            try:
                addresses = await self.nansen()
                if addresses:
                    return addresses
            except UpstreamError:
                pass
        return await self.public_ranking()

    async def nansen(self):
        today = datetime.now(UTC).date()
        rows = await request(self.client, "Nansen", "POST", "https://api.nansen.ai/api/v1/perp-leaderboard",
                             headers={"apiKey": self.config.nansen_key},
                             json={"date": {"from": (today - timedelta(days=30)).isoformat(),
                                            "to": today.isoformat()}, "premium_labels": False,
                                   "pagination": {"page": 1, "per_page": 250},
                                   "filters": {"account_value": {"min": 10000},
                                               "total_pnl": {"min": 1000}},
                                   "order_by": [{"field": "realized_pnl_usd", "direction": "DESC"}]})
        candidates = [x for x in rows.get("data", []) if number(x.get("realized_pnl_usd")) > 0
                      and number(x.get("total_trades")) >= 20]
        return unique_addresses((row.get("trader_address") for row in candidates), self.config.wallet_count)

    async def public_ranking(self):
        data = await request(self.client, "Hyperliquid leaderboard", "GET",
                             "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard", timeout=35)
        scored = []
        for row in data.get("leaderboardRows", []):
            windows = dict(row.get("windowPerformances", []))
            week, month = windows.get("week", {}), windows.get("month", {})
            pnl, weekly, volume = number(month.get("pnl")), number(week.get("pnl")), number(month.get("vlm"))
            equity = number(row.get("accountValue"))
            if pnl > 1000 and weekly > 0 and volume >= 100_000 and equity > 10_000:
                scored.append((pnl / volume, pnl, row.get("ethAddress") or ""))
        scored.sort(reverse=True)
        return unique_addresses((row[2] for row in scored), self.config.wallet_count)


def unique_addresses(candidates, limit):
    return list(dict.fromkeys(addr.lower() for addr in candidates
                              if isinstance(addr, str) and ADDRESS.fullmatch(addr)))[:limit]
