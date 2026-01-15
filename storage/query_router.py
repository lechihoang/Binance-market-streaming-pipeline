"""Query router with automatic tier selection (Redis/PostgreSQL).

Routes data queries to the appropriate storage tier based on time range:
- Recent data (< CACHE_HOUR): Redis (hot tier, fast access)
- Historical data (>= CACHE_HOUR): PostgreSQL (warm tier, persistent storage)

Supports kline (OHLCV), trade, and alert data types with automatic fallback.
"""

from datetime import datetime, timedelta
from typing import Any

from util.constant import CACHE_HOUR
from util.logging import get_logger

from .postgres import Postgres
from .redis import Redis

logger = get_logger(__name__)


class Router:
    """Smart query router with automatic tier selection and fallback.

    Routes queries to optimal storage tier based on data recency:
    - Hot tier (Redis): Last N hours of data (configurable via CACHE_HOUR)
    - Warm tier (PostgreSQL): Historical data beyond cache window

    Features:
    - Automatic tier selection based on timestamp
    - Fallback to next tier if query fails or returns empty
    - Support for multiple data types (klines, alerts, trades)
    - Multi-timeframe kline queries (1m, 5m, 15m, 1h)

    Terminology:
    - Kline: OHLCV data (Open, High, Low, Close, Volume) in time intervals
    """

    TIER = ["redis", "postgres"]
    KLINE = "klines"
    ALERT = "alerts"
    TRADE = "trades"

    def __init__(self, redis: Redis, postgres: Postgres):
        self.redis = redis
        self.pg = postgres

        self.query_map = {
            "redis": {
                "klines": (lambda s, st, en: self.redis_kline(s), False),
                "trades": (lambda s, st, en: self.redis.get_trade(s, limit=1000), False),
                "alerts": (lambda s, st, en: self.redis.get_alert(limit=1000), False),
            },
            "postgres": {
                "klines": (lambda s, st, en: self.pg.get_kline(s, st, en), True),
                "alerts": (lambda s, st, en: self.pg.get_alert(s, st, en), True),
            },
        }

    def wrap_single(self, result: Any) -> list[dict[str, Any]]:
        return [result] if result else []

    def redis_kline(self, symbol: str, interval: str = "1m") -> list[dict[str, Any]]:
        if interval == "1m":
            r = self.redis.get_agg(symbol, "1m")
            return [r] if r else []
        return self.redis.get_agg_list(symbol, interval)

    def select_tier(self, start: datetime) -> str:
        now = datetime.now()
        start_local = start if start.tzinfo else start
        if start_local > now - timedelta(hours=CACHE_HOUR):
            return "redis"
        return "postgres"

    def query_tier(
        self, tier: str, data_type: str, symbol: str, start: datetime, end: datetime, interval: str = "1m"
    ) -> list[dict[str, Any]]:
        if data_type == self.KLINE:
            return self.query_kline_tier(tier, symbol, start, end, interval)

        tier_map = self.query_map.get(tier, {})
        fn = tier_map.get(data_type)
        if not fn:
            return []
        return fn[0](symbol, start, end)

    def query_kline_tier(
        self, tier: str, symbol: str, start: datetime, end: datetime, interval: str = "1m"
    ) -> list[dict[str, Any]]:
        if tier == "redis":
            # Redis only has latest record - if requesting historical range, return empty to fallback to Postgres
            now = datetime.now()
            time_diff = (now - start).total_seconds() / 60  # minutes
            if time_diff > 5:  # If requesting data older than 5 minutes, use Postgres instead
                return []
            return self.redis_kline(symbol, interval)
        elif tier == "postgres":
            if interval == "1m":
                return self.pg.get_kline(symbol, start, end)
            return self.pg.get_kline_agg(symbol, start, end, interval)
        return []

    def query(
        self, data_type: str, symbol: str, start: datetime, end: datetime, interval: str = "1m"
    ) -> list[dict[str, Any]]:
        tier = self.select_tier(start)
        idx = self.TIER.index(tier)

        for t in self.TIER[idx:]:
            try:
                result = self.query_tier(t, data_type, symbol, start, end, interval)
                if result:
                    logger.debug(f"Query OK on {t}: {data_type}, {symbol}, interval={interval}")
                    return result
                logger.debug(f"{t} empty, trying next")
            except Exception as e:
                logger.warning(f"{t} query failed: {e}, trying next")

        return []
