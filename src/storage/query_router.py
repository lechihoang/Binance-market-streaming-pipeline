"""
QueryRouter - Automatic tier selection for queries.

Routes queries to appropriate storage tier based on time range:
- < 1 hour: Redis (Hot Path)
- < 90 days: PostgreSQL (Warm Path)
- >= 90 days: MinIO (Cold Path)
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

from .redis import RedisStorage
from .postgres import PostgresStorage
from .minio import MinioStorage
from src.utils.logging import get_logger

logger = get_logger(__name__)


class QueryRouter:
    """Routes queries to appropriate storage tier based on time range."""
    
    REDIS_THRESHOLD_HOURS = 1
    POSTGRES_THRESHOLD_DAYS = 90
    TIER_ORDER = ["redis", "postgres", "minio"]
    
    # Data type constants
    DATA_TYPE_KLINES = "klines"
    DATA_TYPE_CANDLES = "candles"
    DATA_TYPE_INDICATORS = "indicators"
    DATA_TYPE_ALERTS = "alerts"
    DATA_TYPE_TRADES = "trades"
    DATA_TYPE_AGGREGATIONS = "aggregations"
    
    def __init__(self, redis: RedisStorage, postgres: PostgresStorage, minio: MinioStorage):
        self.redis = redis
        self.postgres = postgres
        self.minio = minio
        
        # Query method mapping: tier -> data_type -> (method, needs_time_range)
        self._query_map = {
            "redis": {
                "indicators": (lambda s, st, en: self._wrap_single(self.redis.get_indicators(s)), False),
                "candles": (lambda s, st, en: self._get_redis_candles(s), False),
                "klines": (lambda s, st, en: self._get_redis_candles(s), False),
                "aggregations": (lambda s, st, en: self._get_redis_candles(s), False),
                "trades": (lambda s, st, en: self.redis.get_recent_trades(s, limit=1000), False),
                "alerts": (lambda s, st, en: self.redis.get_recent_alerts(limit=1000), False),
            },
            "postgres": {
                "indicators": (lambda s, st, en: self.postgres.query_indicators(s, st, en), True),
                "candles": (lambda s, st, en: self.postgres.query_candles(s, st, en), True),
                "klines": (lambda s, st, en: self.postgres.query_candles(s, st, en), True),
                "aggregations": (lambda s, st, en: self.postgres.query_candles(s, st, en), True),
                "alerts": (lambda s, st, en: self.postgres.query_alerts(s, st, en), True),
            },
            "minio": {
                "indicators": (lambda s, st, en: self.minio.read_indicators(s, st, en), True),
                "candles": (lambda s, st, en: self.minio.read_klines(s, st, en), True),
                "klines": (lambda s, st, en: self.minio.read_klines(s, st, en), True),
                "aggregations": (lambda s, st, en: self.minio.read_klines(s, st, en), True),
                "alerts": (lambda s, st, en: self.minio.read_alerts(s, st, en), True),
            },
        }


    def _wrap_single(self, result: Any) -> List[Dict[str, Any]]:
        """Wrap single result in list."""
        return [result] if result else []
    
    def _get_redis_candles(self, symbol: str) -> List[Dict[str, Any]]:
        """Get candles from Redis, trying multiple intervals."""
        for interval in ["1m", "5m", "15m", "1h"]:
            result = self.redis.get_aggregation(symbol, interval)
            if result:
                return [result]
        return []
    
    def _select_tier(self, start: datetime) -> str:
        """Select storage tier based on start time. Assumes naive UTC datetimes."""
        now = datetime.utcnow()
        start_utc = start.replace(tzinfo=None) if start.tzinfo else start
        if start_utc >= now - timedelta(hours=self.REDIS_THRESHOLD_HOURS):
            return "redis"
        if start_utc >= now - timedelta(days=self.POSTGRES_THRESHOLD_DAYS):
            return "postgres"
        return "minio"
    
    def _query_tier(
        self, tier: str, data_type: str, symbol: str, start: datetime, end: datetime
    ) -> List[Dict[str, Any]]:
        """Query a specific tier."""
        tier_map = self._query_map.get(tier, {})
        query_fn = tier_map.get(data_type)
        if not query_fn:
            return []
        return query_fn[0](symbol, start, end)
    
    def query(
        self, data_type: str, symbol: str, start: datetime, end: datetime
    ) -> List[Dict[str, Any]]:
        """Query data with automatic tier selection and fallback."""
        selected_tier = self._select_tier(start)
        start_idx = self.TIER_ORDER.index(selected_tier)
        
        for tier in self.TIER_ORDER[start_idx:]:
            try:
                result = self._query_tier(tier, data_type, symbol, start, end)
                if result:
                    logger.debug(f"Query succeeded on {tier}: {data_type}, {symbol}")
                    return result
                logger.debug(f"{tier} returned empty, trying next tier")
            except Exception as e:
                logger.warning(f"{tier} query failed: {e}, trying next tier")
        
        return []
