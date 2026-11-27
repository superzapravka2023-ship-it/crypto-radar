import os
import re
from dataclasses import dataclass
from decimal import Decimal

ADDRESS = re.compile(r"0x[a-fA-F0-9]{40}\Z")


def items(name, default=""):
    return tuple(item.strip() for item in os.getenv(name, default).split(",") if item.strip())


@dataclass(frozen=True)
class Config:
    database: str
    router_key: str
    telegram_token: str
    chat_id: str
    pairing_code: str
    nansen_key: str
    addresses: tuple
    feeds: tuple
    excluded: frozenset
    turnover: float
    wallet_count: int
    live_wallets: int
    wallet_poll: int
    budget: Decimal
    spread_bps: float
    bybit: str
    news_key: str
    news_api: str
    spot_fee_bps: float
    perp_fee_bps: float
    port: int
    x_key: str
    x_query: str

    @classmethod
    def load(cls):
        addresses = tuple(dict.fromkeys(a.lower() for a in items("HYPERLIQUID_ADDRESSES")))
        secret = os.getenv("TELEGRAM_PAIRING_CODE", "")
        chat = os.getenv("TELEGRAM_CHAT_ID", "")
        token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        count = int(os.getenv("WALLET_COUNT", "100"))
        live = int(os.getenv("LIVE_WALLETS", "10"))
        interval = int(os.getenv("WALLET_POLL_SECONDS", "240"))
        budget = Decimal(os.getenv("DAILY_OPENROUTER_USD", "3"))
        db = os.getenv("DATABASE_URL", "sqlite:radar.db")
        bybit = os.getenv("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
        if not all(ADDRESS.fullmatch(address) for address in addresses):
            raise ValueError("HYPERLIQUID_ADDRESSES must contain public hexadecimal wallet addresses")
        if not 0 <= live <= min(count, 10) or not 1 <= count <= 100 or interval < 180:
            raise ValueError("Expected 1–100 wallets, up to 10 live and a polling interval of at least 180 seconds")
        if not budget.is_finite() or budget < 0:
            raise ValueError("DAILY_OPENROUTER_USD must be non-negative")
        if token and not chat and len(secret) < 24 and not chat_from_db(db):
            raise ValueError("Set TELEGRAM_CHAT_ID or a random TELEGRAM_PAIRING_CODE of at least 24 characters")
        if chat and (not chat.isdecimal() or int(chat) <= 0):
            raise ValueError("TELEGRAM_CHAT_ID must be a private chat ID")
        if os.getenv("RAILWAY_ENVIRONMENT") and not db.startswith(("postgres://", "postgresql://")):
            raise ValueError("Set DATABASE_URL using the Railway PostgreSQL service")
        if not re.fullmatch(r"https://api(?:-[a-z]+)?\.bybit\.(?:com|nl|tr|kz|ge|ae|eu|hk)", bybit):
            raise ValueError("Set BYBIT_API_BASE to an official Bybit API host")
        return cls(
            db, os.getenv("OPENROUTER_API_KEY", ""), token, chat, secret,
            os.getenv("NANSEN_API_KEY", ""), addresses,
            items("NEWS_RSS_URLS", "https://cointelegraph.com/rss,https://www.coindesk.com/arc/outboundfeeds/rss/"),
            frozenset(x.upper() for x in items("EXCLUDED_ASSETS", "BTC,SOL,XRP,DOT")) | {"BTC", "SOL", "XRP", "DOT"},
            float(os.getenv("MIN_TURNOVER_USDT", "10000000")), count, live, interval,
            budget, float(os.getenv("MAX_SPREAD_BPS", "35")), bybit,
            os.getenv("CRYPTOPANIC_API_KEY", ""),
            os.getenv("CRYPTOPANIC_API_URL", "https://cryptopanic.com/api/developer/v2/posts/"),
            float(os.getenv("PAPER_SPOT_FEE_BPS", "10")),
            float(os.getenv("PAPER_PERP_FEE_BPS", "5.5")),
            int(os.getenv("PORT", "8080")),
            os.getenv("X_API_BEARER_TOKEN", ""), os.getenv("X_RECENT_QUERY", ""),
        )


def chat_from_db(database):
    from .store import Store

    with Store(database) as store:
        return store.get("telegram", "chat")
