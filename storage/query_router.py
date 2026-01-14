"""Auto tier selection for queries based on time range."""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from .redis import Redis
from .postgres import Postgres
from util.logging import get_logger
from util.constant import VALID_INTERVAL, CACHE_HOUR

logger = get_logger(__name__)


class Router:
    TIER = ["redis", "postgres"]
    KLINE = "klines"
    ALERT = "alerts"
    TRADE = "trades"

    def __init__(self, redis: Redis, postgres: Postgres):
        self.redis = redis
        self.pg = postgres

        self.query_map = {
            "redis": {
                "klines": (lambda s, st, en: self.redis_candle(s), False),
                "trades": (lambda s, st, en: self.redis.get_trade(s, limit=1000), False),
                "alerts": (lambda s, st, en: self.redis.get_alert(limit=1000), False),
            },
            "postgres": {
                "klines": (lambda s, st, en: self.pg.get_candle(s, st, en), True),
                "alerts": (lambda s, st, en: self.pg.get_alert(s, st, en), True),
            },
        }

    def wrap_single(self, result: Any) -> List[Dict[str, Any]]:
        return [result] if result else []

    def redis_candle(self, symbol: str, interval: str = "1m") -> List[Dict[str, Any]]:
        if interval == "1m":
            r = self.redis.get_agg(symbol, "1m")
            return [r] if r else []
        return self.redis.get_agg_list(symbol, interval)

    def select_tier(self, start: datetime) -> str:
        now = datetime.now(timezone.utc)
        start_utc = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
        if start_utc > now - timedelta(hours=CACHE_HOUR):
            return "redis"
        return "postgres"

    def query_tier(
        self, tier: str, data_type: str, symbol: str, start: datetime, end: datetime,
        interval: str = "1m"
    ) -> List[Dict[str, Any]]:
        if data_type == self.KLINE:
            return self.query_kline_tier(tier, symbol, start, end, interval)

        tier_map = self.query_map.get(tier, {})
        fn = tier_map.get(data_type)
        if not fn:
            return []
        return fn[0](symbol, start, end)

    def query_kline_tier(
        self, tier: str, symbol: str, start: datetime, end: datetime, interval: str = "1m"
    ) -> List[Dict[str, Any]]:
        if tier == "redis":
            return self.redis_candle(symbol, interval)
        elif tier == "postgres":
            if interval == "1m":
                return self.pg.get_candle(symbol, start, end)
            return self.pg.get_candle_agg(symbol, start, end, interval)
        return []

    def query(
        self, data_type: str, symbol: str, start: datetime, end: datetime,
        interval: str = "1m"
    ) -> List[Dict[str, Any]]:
        """Query with auto tier selection and fallback."""
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
