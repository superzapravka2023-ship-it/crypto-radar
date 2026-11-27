import json
import os
import time
import unittest
import uuid
from dataclasses import replace
from unittest.mock import patch

import httpx

from radar.agents import BudgetUnavailable, Models, paper_step
from radar.app import Radar, matched_assets
from radar.config import Config
from radar.store import Store
from radar.wallets import summarize


def settings(**changes):
    with patch.dict(os.environ, {}, clear=True):
        return replace(Config.load(), **changes)


class EndToEnd(unittest.IsolatedAsyncioTestCase):
    async def test_news_review_telegram_and_paper(self):
        states = []
        router_calls = []
        current = int(time.time())
        with Store("sqlite::memory:") as store:
            async def respond(req):
                path = req.url.path
                if path == "/v5/market/instruments-info":
                    rows = [{"symbol": coin + "USDT", "baseCoin": coin, "quoteCoin": "USDT",
                             "status": "Trading", "contractType": "LinearPerpetual",
                             "priceFilter": {"tickSize": "0.001"}}
                            for coin in ("LINK", "BTC")]
                    return httpx.Response(200, json={"retCode": 0, "result": {"list": rows}})
                if path == "/v5/market/tickers":
                    return httpx.Response(200, json={"retCode": 0, "time": current * 1000, "result": {"list": [
                        {"symbol": s + "USDT", "turnover24h": v, "lastPrice": "10",
                         "bid1Price": "9.999", "ask1Price": "10.001"}
                        for s, v in (("LINK", "9000000" if req.url.params["category"] == "spot" else "18000000"),
                                     ("BTC", "999999999"))]}})
                if path == "/v5/announcements/index":
                    return httpx.Response(200, json={"retCode": 0, "result": {"list": [
                        {"title": "Bybit announces new Chainlink integration", "url": "https://announcements.bybit.com/link/",
                         "publishTime": current * 1000}]}})
                if path == "/v5/market/orderbook":
                    return httpx.Response(200, json={"retCode": 0, "result": {
                        "b": [["9.999", "4000"]], "a": [["10.001", "4000"]]}})
                if path == "/v5/market/kline":
                    rows = [[str((current - i * 900) * 1000), "10", "10.10", "9.90", "10", "1", "10"]
                            for i in range(25)]
                    return httpx.Response(200, json={"retCode": 0, "result": {"list": rows}})
                if path == "/api/v1/chat/completions":
                    data = json.loads(req.content)
                    idx = len(router_calls)
                    router_calls.append(data["model"])
                    evidence = json.loads(data["messages"][1]["content"])["evidence"]
                    return httpx.Response(200, json={"usage": {"cost": .004, "prompt_tokens": 110,
                                                               "completion_tokens": 44},
                        "choices": [{"message": {"content": json.dumps({
                            "decision": ["keep", "long", "approve"][idx], "reason": "Событие подтверждено",
                            "evidence": [evidence[0]["id"]]})}}]})
                if path.endswith(("sendMessage", "editMessageText", "answerCallbackQuery")):
                    states.append(json.loads(req.content))
                    return httpx.Response(200, json={"ok": True, "result": {"message_id": 5}})
                self.fail(f"Unexpected route {path}")

            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                radar = Radar(settings(router_key="mock", telegram_token="mock", chat_id="42", feeds=()), store, client)
                await radar.market()
                self.assertFalse(radar.quotes["spot:LINKUSDT"]["eligible"])
                self.assertTrue(radar.quotes["linear:LINKUSDT"]["eligible"])
                self.assertNotIn("linear:BTCUSDT", radar.quotes)
                await radar.news()
                await radar.news()
                self.assertEqual(len(store.recent("event")), 1)
                await radar.process()
                key, idea = store.recent("idea")[0]
                self.assertEqual(store.get("audit", key)["state"], "approved")
                await radar.delivery()
                await radar.delivery()
                self.assertEqual(len(states), 1)
                self.assertIn("ЛОНГ", states[0]["text"])
                self.assertEqual(len(router_calls), 3)
                self.assertIn("$0.0120", radar.report("cost", ""))
                self.assertIn("announcements.bybit.com", radar.report("why", key))
                q = radar.quotes["linear:LINKUSDT"]
                q.update(ask=idea["plan"]["trigger"], bid=idea["plan"]["trigger"] - .001)
                await radar.delivery()
                await radar.delivery()
                self.assertEqual(store.get("idea", key)["state"], "open")
                self.assertEqual(len(states), 2)
                self.assertIn("выполнено", states[-1]["text"])

    async def test_budget_and_chat_pairing(self):
        sent = []
        async def endpoint(req):
            sent.append(json.loads(req.content))
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})
        code = "c" * 25
        with Store("sqlite::memory:") as store:
            async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
                config = settings(router_key="mock", telegram_token="mock", pairing_code=code, budget=0)
                models = Models(client, store, config)
                with self.assertRaises(BudgetUnavailable):
                    await models.analyse("screen", "id", {}, ["keep", "watch"], {"test"})
                radar = Radar(config, store, client)
                for chat, content in ((43, "/start"), (42, "/start " + code), (43, "/cost")):
                    await radar.telegram.handle({"message": {"chat": {"id": chat, "type": "private"},
                                                             "text": content}})
                self.assertEqual(radar.telegram.chat, "42")
                self.assertEqual(len(sent), 1)


class DataTests(unittest.TestCase):
    def test_wallet_net_pnl_and_full_close(self):
        fields = {"time": 10, "coin": "LINK", "side": "A", "sz": "10", "px": "10",
                  "closedPnl": "15", "fee": ".2", "startPosition": "10", "dir": "Close Long"}
        result = summarize([fields], [{"time": 11, "delta": {"usdc": "-.3"}}])
        self.assertEqual(result["net_pnl"], 14.5)
        self.assertEqual(result["closed"], 1)

    def test_matching_ignores_natural_words(self):
        self.assertEqual(matched_assets({"title": "ON Chainlink and $OP"}, {"LINK", "ON", "OP"}), {"LINK", "OP"})

    def test_paper_cancels_when_market_gaps(self):
        plan = {"side": 1, "trigger": 10.0, "edge": 10.1, "stop": 9.5}
        row = {"state": "waiting", "plan": plan, "valid_until": 500, "max_spread_bps": 35}
        self.assertEqual(paper_step(row, {"ts": 100, "ask": 10.5, "bid": 10.49}, 100)["state"], "missed")

    @unittest.skipUnless(os.getenv("TEST_POSTGRES_URL"), "PostgreSQL service not configured")
    def test_postgres_ledger(self):
        uid = uuid.uuid4().hex
        with Store(os.environ["TEST_POSTGRES_URL"]) as store:
            self.assertTrue(store.put("test", uid, {"alive": True}, replace=False))
            self.assertFalse(store.put("test", uid, {"alive": False}, replace=False))
            self.assertEqual(store.get("test", uid), {"alive": True})
            self.assertTrue(store.reserve(uid, "mock", 80, 1_000_000_000))
            store.settle(uid, 20, 10, 5)
            self.assertTrue(any(row[0] == "mock" and row[1] == "settled" for row in store.usage()))
