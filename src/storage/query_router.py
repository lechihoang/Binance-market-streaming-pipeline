"""
QueryRouter - Automatic tier selection for queries.

Routes queries to appropriate storage tier based on time range:
- < 1 hour: Redis (Hot Path)
- < 90 days: PostgreSQL (Warm Path)
- >= 90 days: MinIO (Cold Path)

Implements fallback logic when a tier fails.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .redis_storage import RedisStorage
from .postgres_storage import PostgresStorage
from .minio_storage import MinioStorage

logger = logging.getLogger(__name__)


class QueryRouter:
    """Routes queries to appropriate storage tier based on time range.
    
    Tier selection rules (per Requirements 4.1, 4.2):
    - < 1 hour: Redis (Hot Path)
    - < 90 days: PostgreSQL (Warm Path)
    - >= 90 days: MinIO (Cold Path)
    
    Fallback logic:
    - Redis fails -> PostgreSQL
    - PostgreSQL fails -> MinIO
    """
    
    # Time thresholds for tier selection
    REDIS_THRESHOLD_HOURS = 1
    POSTGRES_THRESHOLD_DAYS = 90
    
    # Tier names
    TIER_REDIS = "redis"
    TIER_POSTGRES = "postgres"
    TIER_MINIO = "minio"
    
    # Supported data types
    DATA_TYPE_CANDLES = "candles"
    DATA_TYPE_KLINES = "klines"
    DATA_TYPE_INDICATORS = "indicators"
    DATA_TYPE_ALERTS = "alerts"
    DATA_TYPE_TRADES = "trades"
    DATA_TYPE_AGGREGATIONS = "aggregations"

    
    def __init__(
        self,
        redis: RedisStorage,
        postgres: PostgresStorage,
        minio: MinioStorage,
    ):
        """Initialize QueryRouter with storage tier instances.
        
        Args:
            redis: RedisStorage instance for hot path queries
            postgres: PostgresStorage instance for warm path queries
            minio: MinioStorage instance for cold path queries
        """
        self.redis = redis
        self.postgres = postgres
        self.minio = minio
    
    def _select_tier(self, start: datetime, end: datetime) -> str:
        """Select appropriate storage tier based on time range.
        
        Selection rules:
        - If time range is within last 1 hour -> Redis
        - If time range is within last 90 days -> PostgreSQL
        - If time range exceeds 90 days -> MinIO
        
        Args:
            start: Query start datetime
            end: Query end datetime
            
        Returns:
            Tier name: 'redis', 'postgres', or 'minio'
        """
        now = datetime.now()
        
        # Calculate time range boundaries
        redis_cutoff = now - timedelta(hours=self.REDIS_THRESHOLD_HOURS)
        postgres_cutoff = now - timedelta(days=self.POSTGRES_THRESHOLD_DAYS)
        
        # If start time is within last hour, use Redis
        if start >= redis_cutoff:
            return self.TIER_REDIS
        
        # If start time is within last 90 days, use PostgreSQL
        if start >= postgres_cutoff:
            return self.TIER_POSTGRES
        
        # Otherwise use MinIO for historical data
        return self.TIER_MINIO
    
    def _query_redis(
        self,
        data_type: str,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> List[Dict[str, Any]]:
        """Query data from Redis tier.
        
        Args:
            data_type: Type of data to query
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of data records
        """
        if data_type == self.DATA_TYPE_INDICATORS:
            result = self.redis.get_indicators(symbol)
            if result:
                return [result]
            return []
        
        elif data_type in (self.DATA_TYPE_CANDLES, self.DATA_TYPE_KLINES, 
                           self.DATA_TYPE_AGGREGATIONS):
            # Try to get aggregation for common intervals
            for interval in ["1m", "5m", "15m", "1h"]:
                result = self.redis.get_aggregation(symbol, interval)
                if result:
                    return [result]
            return []
        
        elif data_type == self.DATA_TYPE_TRADES:
            return self.redis.get_recent_trades(symbol, limit=1000)
        
        elif data_type == self.DATA_TYPE_ALERTS:
            return self.redis.get_recent_alerts(limit=1000)
        
        return []

    
    def _query_postgres(
        self,
        data_type: str,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> List[Dict[str, Any]]:
        """Query data from PostgreSQL tier.
        
        Args:
            data_type: Type of data to query
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of data records
        """
        if data_type == self.DATA_TYPE_INDICATORS:
            return self.postgres.query_indicators(symbol, start, end)
        
        elif data_type in (self.DATA_TYPE_CANDLES, self.DATA_TYPE_KLINES,
                           self.DATA_TYPE_AGGREGATIONS):
            return self.postgres.query_candles(symbol, start, end)
        
        elif data_type == self.DATA_TYPE_ALERTS:
            return self.postgres.query_alerts(symbol, start, end)
        
        return []
    
    def _query_minio(
        self,
        data_type: str,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> List[Dict[str, Any]]:
        """Query data from MinIO tier.
        
        Args:
            data_type: Type of data to query
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of data records
        """
        if data_type == self.DATA_TYPE_INDICATORS:
            return self.minio.read_indicators(symbol, start, end)
        
        elif data_type in (self.DATA_TYPE_CANDLES, self.DATA_TYPE_KLINES,
                           self.DATA_TYPE_AGGREGATIONS):
            return self.minio.read_klines(symbol, start, end)
        
        elif data_type == self.DATA_TYPE_ALERTS:
            return self.minio.read_alerts(symbol, start, end)
        
        return []
    
    def query(
        self,
        data_type: str,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> List[Dict[str, Any]]:
        """Query data with automatic tier selection and fallback.
        
        Routes query to appropriate tier based on time range.
        Implements fallback logic if primary tier fails:
        - Redis fails -> PostgreSQL
        - PostgreSQL fails -> MinIO
        
        Args:
            data_type: Type of data to query (candles, indicators, alerts, trades)
            symbol: Trading symbol (e.g., 'BTCUSDT')
            start: Query start datetime
            end: Query end datetime
            
        Returns:
            List of data records from the appropriate tier
        """
        selected_tier = self._select_tier(start, end)
        logger.debug(f"Selected tier '{selected_tier}' for query: {data_type}, {symbol}, {start}-{end}")
        
        # Try primary tier
        if selected_tier == self.TIER_REDIS:
            try:
                result = self._query_redis(data_type, symbol, start, end)
                if result:
                    return result
                # If Redis returns empty, fall through to PostgreSQL
                logger.debug("Redis returned empty, falling back to PostgreSQL")
            except Exception as e:
                logger.warning(f"Redis query failed: {e}, falling back to PostgreSQL")
            
            # Fallback to PostgreSQL
            selected_tier = self.TIER_POSTGRES
        
        if selected_tier == self.TIER_POSTGRES:
            try:
                result = self._query_postgres(data_type, symbol, start, end)
                if result:
                    return result
                # If PostgreSQL returns empty, fall through to MinIO
                logger.debug("PostgreSQL returned empty, falling back to MinIO")
            except Exception as e:
                logger.warning(f"PostgreSQL query failed: {e}, falling back to MinIO")
            
            # Fallback to MinIO
            selected_tier = self.TIER_MINIO
        
        # Final tier: MinIO
        try:
            return self._query_minio(data_type, symbol, start, end)
        except Exception as e:
            logger.error(f"MinIO query failed: {e}")
            return []
