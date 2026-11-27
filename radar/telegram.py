import asyncio
import hmac
import logging
import time
from datetime import UTC, datetime

from .sources import UpstreamError, request

LOG = logging.getLogger("radar")

LABELS = {
    "waiting": "ждём условие входа", "open": "условие входа выполнено", "partial": "первая цель достигнута",
    "tp2": "обе цели достигнуты", "stop": "достигнут стоп", "time_exit": "выход по времени",
    "expired": "срок входа истёк", "missed": "цена вышла за диапазон", "invalidated": "сценарий отменён",
    "unpriced": "исход без оценки, котировки были недоступны",
}


def display_price(p):
    return f"{p:.12f}".rstrip("0").rstrip(".")


def card(key, idea):
    plan = idea["plan"]
    market = "спот" if idea["key"].startswith("spot:") else "USDT-перпетуал"
    direction = "ЛОНГ" if plan["side"] > 0 else "ШОРТ"
    entry = " – ".join(display_price(x) for x in sorted([plan["trigger"], plan["edge"]]))
    expiry = datetime.fromtimestamp(idea["valid_until"], UTC).strftime("%H:%M UTC")
    return (
        f"{idea['key'].split(':')[1]} · Bybit {market} · {direction}\n"
        f"Статус: {LABELS[idea['state']]}\n"
        f"Условие: цена исполнимой котировки достигает {display_price(plan['trigger'])}\n"
        f"Зона входа: {entry}\nСтоп: {display_price(plan['stop'])}\n"
        f"Цели: {display_price(plan['tp1'])} / {display_price(plan['tp2'])}\n"
        f"Вход действителен до {expiry}. Оценка прибыль/риск с издержками: {plan['net_r']}\n"
        f"{idea['reason']}\n\nРучная торговля · симуляция · {key}"
        + (f"\nРезультат симуляции: {idea['pnl_pct']:+.2f}%" if "pnl_pct" in idea else "")
    )


class Telegram:
    def __init__(self, client, store, config, report):
        self.client, self.store, self.config, self.report = client, store, config, report
        self.base = "https://api.telegram.org/bot" + config.telegram_token + "/"

    @property
    def chat(self):
        return self.config.chat_id or self.store.get("telegram", "chat")

    async def call(self, method, **params):
        polling = method == "getUpdates"
        data = await request(self.client, "Telegram", "POST", self.base + method,
                             timeout=35 if polling else 15, json=params)
        if not data.get("ok"):
            raise UpstreamError("Telegram", data.get("error_code"))
        return data["result"]

    async def send(self, message, chat=None, **params):
        return await self.call("sendMessage", chat_id=chat or self.chat, text=message[:3950],
                               link_preview_options={"is_disabled": True}, **params)

    async def handle(self, update):
        message = update.get("message") or {}
        callback = update.get("callback_query")
        if callback:
            chat = (callback.get("message") or {}).get("chat", {})
            if chat.get("type") == "private" and str(chat.get("id")) == str(self.chat):
                await self.call("answerCallbackQuery", callback_query_id=callback["id"])
                if str(callback.get("data", "")).startswith("why:"):
                    await self.send(self.report("why", callback["data"].split(":", 1)[1]))
            return
        chat = message.get("chat") or {}
        if chat.get("type") != "private":
            return
        chat_id = str(chat.get("id"))
        words = str(message.get("text", "")).split(maxsplit=1)
        if not words or not words[0].startswith("/"):
            return
        cmd = words[0].split("@")[0][1:]
        arg = words[1].strip() if len(words) > 1 else ""
        if not self.chat:
            if cmd == "start" and self.config.pairing_code and hmac.compare_digest(
                    arg.encode(), self.config.pairing_code.encode()):
                if self.store.put("telegram", "chat", chat_id, replace=False):
                    await self.send(self.report("start", ""), chat_id)
            return
        if chat_id == str(self.chat):
            await self.send(self.report(cmd, arg))

    async def poll(self):
        offset = self.store.get("telegram", "offset") or 0
        updates = await self.call("getUpdates", timeout=24, offset=offset,
                                  allowed_updates=["message", "callback_query"])
        for update in updates:
            try:
                await self.handle(update)
            except Exception:
                LOG.exception("telegram update %s failed", update.get("update_id"))
            self.store.put("telegram", "offset", update["update_id"] + 1)
        if not updates:
            await asyncio.sleep(.25)

    async def publish(self, key, idea):
        if not self.chat:
            return
        entry = self.store.get("telegram_message", key) or {}
        if entry.get("state") == idea["state"]:
            return
        body = card(key, idea)
        buttons = {"inline_keyboard": [[{"text": "Почему?", "callback_data": f"why:{key}"}]]}
        if entry.get("id"):
            try:
                await self.call("editMessageText", chat_id=self.chat, message_id=entry["id"], text=body,
                                reply_markup=buttons, link_preview_options={"is_disabled": True})
            except UpstreamError as exc:
                if exc.code != 400:
                    raise
                # A concurrent retry may already have applied the same edit.
                # Send state transitions separately if the original card no longer exists.
                if time.time() - idea["created"] > 48 * 3600:
                    return
            msg_id = entry["id"]
        elif idea["state"] == "waiting" and time.time() < idea["valid_until"]:
            msg_id = (await self.send(body, reply_markup=buttons))["message_id"]
        else:
            return
        self.store.put("telegram_message", key, {"id": msg_id, "state": idea["state"]})
