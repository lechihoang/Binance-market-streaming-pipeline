"""QueryRouter - Automatic tier selection for queries based on time range."""

from datetime import datetime, timedelta
from typing import Any, Dict, List

from .redis import RedisStorage
from .postgres import PostgresStorage
from utils.logging import get_logger

logger = get_logger(__name__)


class QueryRouter:
    
    REDIS_THRESHOLD_HOURS = 1
    TIER_ORDER = ["redis", "postgres"]
    
    DATA_TYPE_KLINES = "klines"
    DATA_TYPE_ALERTS = "alerts"
    DATA_TYPE_TRADES = "trades"
    
    VALID_INTERVALS = {"1m", "5m", "15m"}
    
    def __init__(self, redis: RedisStorage, postgres: PostgresStorage):
        self.redis = redis
        self.postgres = postgres
        
        self._query_map = {
            "redis": {
                "klines": (lambda s, st, en: self._get_redis_candles(s), False),
                "trades": (lambda s, st, en: self.redis.get_recent_trades(s, limit=1000), False),
                "alerts": (lambda s, st, en: self.redis.get_recent_alerts(limit=1000), False),
            },
            "postgres": {
                "klines": (lambda s, st, en: self.postgres.query_candles(s, st, en), True),
                "alerts": (lambda s, st, en: self.postgres.query_alerts(s, st, en), True),
            },
        }

    def _wrap_single(self, result: Any) -> List[Dict[str, Any]]:
        return [result] if result else []
    
    def _get_redis_candles(self, symbol: str, interval: str = "1m") -> List[Dict[str, Any]]:
        if interval == "1m":
            result = self.redis.get_aggregation(symbol, "1m")
            return [result] if result else []
        else:
            return self.redis.get_aggregations_multi(symbol, interval)
    
    def _select_tier(self, start: datetime) -> str:
        """Select storage tier based on start time.
        
        < 1 hour: Redis (cache)
        >= 1 hour: PostgreSQL (permanent storage)
        """
        now = datetime.now()
        start_local = start.replace(tzinfo=None) if start.tzinfo else start
        if start_local > now - timedelta(hours=self.REDIS_THRESHOLD_HOURS):
            return "redis"
        return "postgres"
    
    def _query_tier(
        self, tier: str, data_type: str, symbol: str, start: datetime, end: datetime,
        interval: str = "1m"
    ) -> List[Dict[str, Any]]:
        if data_type == self.DATA_TYPE_KLINES:
            return self._query_klines_tier(tier, symbol, start, end, interval)
        
        tier_map = self._query_map.get(tier, {})
        query_fn = tier_map.get(data_type)
        if not query_fn:
            return []
        return query_fn[0](symbol, start, end)
    
    def _query_klines_tier(
        self, tier: str, symbol: str, start: datetime, end: datetime, interval: str = "1m"
    ) -> List[Dict[str, Any]]:
        if tier == "redis":
            return self._get_redis_candles(symbol, interval)
        
        elif tier == "postgres":
            if interval == "1m":
                return self.postgres.query_candles(symbol, start, end)
            else:
                return self.postgres.query_candles_aggregated(symbol, start, end, interval)
        
        return []
    
    def query(
        self, data_type: str, symbol: str, start: datetime, end: datetime,
        interval: str = "1m"
    ) -> List[Dict[str, Any]]:
        """Query data with automatic tier selection and fallback."""
        selected_tier = self._select_tier(start)
        start_idx = self.TIER_ORDER.index(selected_tier)
        
        for tier in self.TIER_ORDER[start_idx:]:
            try:
                result = self._query_tier(tier, data_type, symbol, start, end, interval)
                if result:
                    logger.debug(f"Query succeeded on {tier}: {data_type}, {symbol}, interval={interval}")
                    return result
                logger.debug(f"{tier} returned empty, trying next tier")
            except Exception as e:
                logger.warning(f"{tier} query failed: {e}, trying next tier")
        
        return []
