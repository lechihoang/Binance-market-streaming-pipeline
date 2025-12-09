"""
StorageWriter - Multi-tier write coordinator.

Writes data to all 3 storage tiers (Redis, PostgreSQL/DuckDB, MinIO)
with partial failure resilience.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional, Union

from .redis_storage import RedisStorage
from .duckdb_storage import DuckDBStorage
from .postgres_storage import PostgresStorage
from .minio_storage import MinioStorage

logger = logging.getLogger(__name__)


class StorageWriter:
    """Coordinates writes to all storage tiers.
    
    Implements multi-tier write strategy:
    - Redis: overwrite latest values (hot path)
    - PostgreSQL/DuckDB: upsert on (symbol, timestamp) (warm path)
    - MinIO: append partitioned files (cold path)
    
    Handles partial failures by logging errors and continuing
    to write to other sinks.
    """
    
    def __init__(
        self,
        redis: RedisStorage,
        duckdb: Optional[DuckDBStorage] = None,
        postgres: Optional[PostgresStorage] = None,
        minio: Optional[MinioStorage] = None,
    ):
        """Initialize StorageWriter with storage tier instances.
        
        Args:
            redis: RedisStorage instance for hot path
            duckdb: DuckDBStorage instance for warm path (legacy)
            postgres: PostgresStorage instance for warm path (preferred)
            minio: MinioStorage instance for cold path
        """
        self.redis = redis
        self.duckdb = duckdb
        self.postgres = postgres
        self.minio = minio
        
        # Determine which warm path storage to use (prefer postgres)
        self._warm_storage: Optional[Union[PostgresStorage, DuckDBStorage]] = postgres or duckdb
        # Cold path storage (MinIO)
        self._cold_storage: Optional[MinioStorage] = minio
        
        if self._warm_storage is None:
            logger.warning("No warm path storage configured (postgres or duckdb)")
        if self._cold_storage is None:
            logger.warning("No cold path storage configured (minio)")
    
    def write_aggregation(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Write aggregation data to all 3 tiers.
        
        Redis: overwrite aggregations:{symbol}:{interval} hash
        PostgreSQL/DuckDB: upsert into trades_1m table
        MinIO: append to klines partition
        
        Args:
            data: Dict with keys: timestamp, symbol, interval, open, high, low,
                  close, volume, quote_volume, trades_count
                  
        Returns:
            Dict with success status for each tier: {'redis': bool, 'warm': bool, 'cold': bool}
        """
        results = {'redis': False, 'warm': False, 'cold': False}
        symbol = data.get('symbol', '')
        interval = data.get('interval', '1m')
        
        # Write to Redis (overwrite)
        try:
            ohlcv = {
                'open': data.get('open'),
                'high': data.get('high'),
                'low': data.get('low'),
                'close': data.get('close'),
                'volume': data.get('volume'),
                'timestamp': data.get('timestamp'),
            }
            self.redis.write_aggregation(symbol, interval, ohlcv)
            results['redis'] = True
        except Exception as e:
            logger.error(f"Redis write_aggregation failed for {symbol}: {e}")
        
        # Write to warm path (PostgreSQL or DuckDB)
        timestamp_dt = self._to_datetime(data.get('timestamp'))
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
            storage_name = "PostgreSQL" if self.postgres else "DuckDB"
            logger.error(f"{storage_name} write_aggregation failed for {symbol}: {e}")
        
        # Write to cold path (MinIO)
        try:
            kline = {
                'timestamp': data.get('timestamp'),
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

    def write_indicators(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Write technical indicators to all 3 tiers.
        
        Redis: overwrite indicators:{symbol} hash
        PostgreSQL/DuckDB: upsert into indicators table
        MinIO: append to indicators partition
        
        Args:
            data: Dict with keys: timestamp, symbol, rsi, macd, macd_signal,
                  sma_20, ema_12, ema_26, bb_upper, bb_lower, atr
                  
        Returns:
            Dict with success status for each tier
        """
        results = {'redis': False, 'warm': False, 'cold': False}
        symbol = data.get('symbol', '')
        
        # Write to Redis (overwrite)
        try:
            indicators = {
                'rsi': data.get('rsi'),
                'macd': data.get('macd'),
                'macd_signal': data.get('macd_signal'),
                'sma_20': data.get('sma_20'),
                'ema_12': data.get('ema_12'),
                'ema_26': data.get('ema_26'),
                'bb_upper': data.get('bb_upper'),
                'bb_lower': data.get('bb_lower'),
                'atr': data.get('atr'),
            }
            # Filter out None values
            indicators = {k: v for k, v in indicators.items() if v is not None}
            # Only write to Redis if there are non-None indicators
            if indicators:
                self.redis.write_indicators(symbol, indicators)
                results['redis'] = True
            else:
                # No indicators to write, consider it a success (no-op)
                results['redis'] = True
                logger.debug(f"No non-None indicators to write for {symbol}")
        except Exception as e:
            logger.error(f"Redis write_indicators failed for {symbol}: {e}")
        
        # Write to warm path (PostgreSQL or DuckDB)
        timestamp_dt = self._to_datetime(data.get('timestamp'))
        try:
            indicators_record = {
                'timestamp': timestamp_dt,
                'symbol': symbol,
                'rsi': data.get('rsi'),
                'macd': data.get('macd'),
                'macd_signal': data.get('macd_signal'),
                'sma_20': data.get('sma_20'),
                'ema_12': data.get('ema_12'),
                'ema_26': data.get('ema_26'),
                'bb_upper': data.get('bb_upper'),
                'bb_lower': data.get('bb_lower'),
                'atr': data.get('atr'),
            }
            if self._warm_storage is not None:
                self._warm_storage.upsert_indicators(indicators_record)
                results['warm'] = True
        except Exception as e:
            storage_name = "PostgreSQL" if self.postgres else "DuckDB"
            logger.error(f"{storage_name} write_indicators failed for {symbol}: {e}")
        
        # Write to cold path (MinIO)
        try:
            indicator_record = {
                'timestamp': data.get('timestamp'),
                'symbol': symbol,
                'rsi': data.get('rsi', 0),
                'macd': data.get('macd', 0),
                'macd_signal': data.get('macd_signal', 0),
                'sma_20': data.get('sma_20', 0),
                'ema_12': data.get('ema_12', 0),
                'ema_26': data.get('ema_26', 0),
                'bb_upper': data.get('bb_upper', 0),
                'bb_lower': data.get('bb_lower', 0),
                'atr': data.get('atr', 0),
            }
            if self._cold_storage is not None:
                write_date = timestamp_dt or datetime.now()
                self._cold_storage.write_indicators(symbol, [indicator_record], write_date)
                results['cold'] = True
        except Exception as e:
            logger.error(f"MinIO write_indicators failed for {symbol}: {e}")
        
        self._log_write_result('indicators', symbol, results)
        return results
    
    def write_alert(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Write alert to all 3 tiers.
        
        Redis: push to alerts:recent list
        PostgreSQL/DuckDB: insert into alerts table
        MinIO: append to alerts partition
        
        Args:
            data: Dict with keys: alert_id, timestamp, symbol, alert_type, 
                  alert_level, created_at, details
                  
        Returns:
            Dict with success status for each tier
        """
        results = {'redis': False, 'warm': False, 'cold': False}
        symbol = data.get('symbol', '')
        
        # Write to Redis (push to list) - keep all fields for AnomalyValidator
        try:
            self.redis.write_alert(data)
            results['redis'] = True
        except Exception as e:
            logger.error(f"Redis write_alert failed for {symbol}: {e}")
        
        # Write to warm path (PostgreSQL or DuckDB)
        # Map alert_level to severity for PostgreSQL schema compatibility
        timestamp_dt = self._to_datetime(data.get('timestamp'))
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
            storage_name = "PostgreSQL" if self.postgres else "DuckDB"
            logger.error(f"{storage_name} write_alert failed for {symbol}: {e}")
        
        # Write to cold path (MinIO)
        try:
            alert_record = {
                'timestamp': data.get('timestamp'),
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
    
    def _to_datetime(self, timestamp: Any) -> Optional[datetime]:
        """Convert timestamp to datetime object.
        
        Args:
            timestamp: Unix timestamp (seconds or milliseconds), datetime, or ISO string
            
        Returns:
            datetime object or None
        """
        if timestamp is None:
            return None
        if isinstance(timestamp, datetime):
            return timestamp
        if isinstance(timestamp, (int, float)):
            # Handle milliseconds vs seconds
            if timestamp > 1e12:
                return datetime.fromtimestamp(timestamp / 1000)
            return datetime.fromtimestamp(timestamp)
        if isinstance(timestamp, str):
            # Handle ISO format string
            try:
                return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                logger.warning(f"Failed to parse timestamp string: {timestamp}")
                return None
        return None
    
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
