"""
StorageWriter - Multi-tier write coordinator.

Writes data to all 3 storage tiers (Redis, PostgreSQL, MinIO)
with partial failure resilience.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from .redis import RedisStorage
from .postgres import PostgresStorage
from .minio import MinioStorage
from src.utils.logging import get_logger

logger = get_logger(__name__)


class StorageWriter:
    """Coordinates writes to all storage tiers.
    
    Implements multi-tier write strategy:
    - Redis: overwrite latest values (hot path)
    - PostgreSQL: upsert on (symbol, timestamp) (warm path)
    - MinIO: append partitioned files (cold path)
    
    Handles partial failures by logging errors and continuing
    to write to other sinks.
    """
    
    def __init__(
        self,
        redis: RedisStorage,
        postgres: Optional[PostgresStorage] = None,
        minio: Optional[MinioStorage] = None,
    ):
        """Initialize StorageWriter with storage tier instances.
        
        Args:
            redis: RedisStorage instance for hot path
            postgres: PostgresStorage instance for warm path
            minio: MinioStorage instance for cold path
        """
        self.redis = redis
        self._warm_storage: Optional[PostgresStorage] = postgres
        self._cold_storage: Optional[MinioStorage] = minio
        
        if self._warm_storage is None:
            logger.warning("No warm path storage configured (postgres)")
        if self._cold_storage is None:
            logger.warning("No cold path storage configured (minio)")
    
    def write_aggregation(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Write aggregation data to all 3 tiers.
        
        Redis: overwrite aggregations:{symbol}:{interval} hash
        PostgreSQL: upsert into trades_1m table
        MinIO: append to klines partition
        
        Args:
            data: Dict with keys: timestamp, symbol, interval, open, high, low,
                  close, volume, quote_volume, trades_count
                  Timestamp should be a datetime object.
                  
        Returns:
            Dict with success status for each tier: {'redis': bool, 'warm': bool, 'cold': bool}
        """
        results = {'redis': False, 'warm': False, 'cold': False}
        symbol = data.get('symbol', '')
        interval = data.get('interval', '1m')
        
        # Get timestamp as datetime, convert to ISO string for Redis
        timestamp_dt = self._to_datetime(data.get('timestamp'))
        timestamp_iso = timestamp_dt.isoformat() if timestamp_dt else None
        
        # Write to Redis (overwrite) - use ISO string for timestamp
        try:
            ohlcv = {
                'open': data.get('open'),
                'high': data.get('high'),
                'low': data.get('low'),
                'close': data.get('close'),
                'volume': data.get('volume'),
                'timestamp': timestamp_iso,
            }
            self.redis.write_aggregation(symbol, interval, ohlcv)
            results['redis'] = True
        except Exception as e:
            logger.error(f"Redis write_aggregation failed for {symbol}: {e}")
        
        # Write to warm path (PostgreSQL) - use datetime object (driver auto-converts)
        try:
            candle = {
                'timestamp': timestamp_dt,
                'symbol': symbol,
                'open': data.get('open'),
                'high': data.get('high'),
                'low': data.get('low'),
                'close': data.get('close'),
                'volume': data.get('volume'),
                'quote_volume': data.get('quote_volume'),
                'trades_count': data.get('trades_count'),
            }
            if self._warm_storage is not None:
                self._warm_storage.upsert_candle(candle)
                results['warm'] = True
        except Exception as e:
            logger.error(f"PostgreSQL write_aggregation failed for {symbol}: {e}")
        
        # Write to cold path (MinIO) - use datetime object directly
        try:
            kline = {
                'timestamp': timestamp_dt,
                'symbol': symbol,
                'open': data.get('open'),
                'high': data.get('high'),
                'low': data.get('low'),
                'close': data.get('close'),
                'volume': data.get('volume'),
                'quote_volume': data.get('quote_volume', 0),
                'trades_count': data.get('trades_count', 0),
            }
            if self._cold_storage is not None:
                write_date = timestamp_dt or datetime.now()
                self._cold_storage.write_klines(symbol, [kline], write_date)
                results['cold'] = True
        except Exception as e:
            logger.error(f"MinIO write_aggregation failed for {symbol}: {e}")
        
        self._log_write_result('aggregation', symbol, results)
        return results

    def write_alert(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Write alert to all 3 tiers.
        
        Redis: push to alerts:recent list
        PostgreSQL: insert into alerts table
        MinIO: append to alerts partition
        
        Args:
            data: Dict with keys: alert_id, timestamp, symbol, alert_type, 
                  alert_level, created_at, details
                  Timestamp and created_at should be datetime objects.
                  
        Returns:
            Dict with success status for each tier
        """
        results = {'redis': False, 'warm': False, 'cold': False}
        symbol = data.get('symbol', '')
        
        # Convert datetime fields to ISO strings for Redis
        timestamp_dt = self._to_datetime(data.get('timestamp'))
        created_at_dt = self._to_datetime(data.get('created_at'))
        timestamp_iso = timestamp_dt.isoformat() if timestamp_dt else None
        created_at_iso = created_at_dt.isoformat() if created_at_dt else None
        
        # Write to Redis (push to list) - use ISO strings for datetime fields
        try:
            redis_data = {
                **data,
                'timestamp': timestamp_iso,
                'created_at': created_at_iso,
            }
            self.redis.write_alert(redis_data)
            results['redis'] = True
        except Exception as e:
            logger.error(f"Redis write_alert failed for {symbol}: {e}")
        
        # Write to warm path (PostgreSQL) - use datetime object (driver auto-converts)
        # Map alert_level to severity for PostgreSQL schema compatibility
        try:
            alert_record = {
                'timestamp': timestamp_dt,
                'symbol': symbol,
                'alert_type': data.get('alert_type'),
                'severity': data.get('alert_level'),  # Map alert_level to severity
                'message': f"{data.get('alert_type')}: {symbol}",
                'metadata': data.get('details'),
            }
            if self._warm_storage is not None:
                self._warm_storage.insert_alert(alert_record)
                results['warm'] = True
        except Exception as e:
            logger.error(f"PostgreSQL write_alert failed for {symbol}: {e}")
        
        # Write to cold path (MinIO) - use datetime object directly
        try:
            alert_record = {
                'timestamp': timestamp_dt,
                'symbol': symbol,
                'alert_type': data.get('alert_type'),
                'severity': data.get('alert_level'),  # Map alert_level to severity
                'message': f"{data.get('alert_type')}: {symbol}",
                'metadata': data.get('details'),
            }
            if self._cold_storage is not None:
                write_date = timestamp_dt or datetime.now()
                self._cold_storage.write_alerts(symbol, [alert_record], write_date)
                results['cold'] = True
        except Exception as e:
            logger.error(f"MinIO write_alert failed for {symbol}: {e}")
        
        self._log_write_result('alert', symbol, results)
        return results
    
    def _to_datetime(self, timestamp) -> Optional[datetime]:
        """Validate and return datetime object.
        
        Per timestamp standardization convention, streaming jobs should
        pass datetime objects directly. This method validates the input.
        
        Args:
            timestamp: datetime object (expected)
            
        Returns:
            datetime object or None if invalid/None
        """
        if timestamp is None:
            return None
        if isinstance(timestamp, datetime):
            return timestamp
        # Log warning for non-datetime input (should not happen with standardized code)
        logger.warning(f"Expected datetime object, got {type(timestamp).__name__}. Using current time as fallback.")
        return datetime.now()
    
    def _log_write_result(
        self, 
        data_type: str, 
        symbol: str, 
        results: Dict[str, bool]
    ) -> None:
        """Log write results with appropriate level.
        
        Args:
            data_type: Type of data written
            symbol: Trading symbol
            results: Dict with success status for each tier
        """
        success_count = sum(results.values())
        total_count = len(results)
        
        if success_count == total_count:
            logger.debug(f"Write {data_type} for {symbol}: all tiers succeeded")
        elif success_count == 0:
            logger.error(f"Write {data_type} for {symbol}: all tiers failed")
        else:
            failed_tiers = [k for k, v in results.items() if not v]
            logger.warning(
                f"Write {data_type} for {symbol}: partial failure - "
                f"{failed_tiers} failed"
            )
