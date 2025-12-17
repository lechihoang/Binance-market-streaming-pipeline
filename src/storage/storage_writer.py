"""
StorageWriter - Multi-tier write coordinator.

Writes data to all 3 storage tiers (Redis, PostgreSQL, MinIO)
with partial failure resilience.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from .redis import RedisStorage
from .postgres import PostgresStorage
from .minio import MinioStorage
from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class BatchResult:
    """Result of a batch write operation across storage tiers.
    
    Attributes:
        total_records: Total number of records in the batch
        success_count: Number of records successfully written
        failure_count: Number of records that failed to write
        tier_results: Dict with success status for each tier (redis, warm, cold)
        failed_records: List of records that failed to write
        duration_ms: Total duration of the batch write in milliseconds
    """
    total_records: int
    success_count: int
    failure_count: int
    tier_results: Dict[str, bool]
    failed_records: List[Dict[str, Any]] = field(default_factory=list)
    duration_ms: float = 0.0


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

    # =========================================================================
    # Batch Write Methods
    # =========================================================================

    def write_aggregations_batch(
        self, records: List[Dict[str, Any]]
    ) -> BatchResult:
        """Write aggregation data to all 3 tiers in parallel.
        
        Uses ThreadPoolExecutor to call batch methods on all storage tiers
        concurrently. Handles partial failures gracefully - if one tier fails,
        other tiers continue their writes.
        
        Redis: write_aggregations_batch (pipeline HSET)
        PostgreSQL: upsert_candles_batch (executemany with ON CONFLICT)
        MinIO: write_klines_batch (single Parquet file per symbol)
        
        Args:
            records: List of aggregation dicts with keys:
                timestamp (datetime), symbol, interval, open, high, low,
                close, volume, quote_volume, trades_count
                
        Returns:
            BatchResult with success/failure status for each tier
            
        Requirements: 1.2, 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4
        """
        if not records:
            return BatchResult(
                total_records=0,
                success_count=0,
                failure_count=0,
                tier_results={'redis': True, 'warm': True, 'cold': True},
                failed_records=[],
                duration_ms=0.0
            )
        
        start_time = time.time()
        tier_results = {'redis': False, 'warm': False, 'cold': False}
        all_failed_records: List[Dict[str, Any]] = []
        
        # Prepare records for each tier
        redis_records = []
        postgres_records = []
        minio_records = []
        
        for record in records:
            timestamp_dt = self._to_datetime(record.get('timestamp'))
            timestamp_iso = timestamp_dt.isoformat() if timestamp_dt else None
            symbol = record.get('symbol', '')
            interval = record.get('interval', '1m')
            
            # Redis record (uses ISO string for timestamp)
            redis_records.append({
                'symbol': symbol,
                'interval': interval,
                'open': record.get('open'),
                'high': record.get('high'),
                'low': record.get('low'),
                'close': record.get('close'),
                'volume': record.get('volume'),
                'timestamp': timestamp_iso,
            })
            
            # PostgreSQL record (uses datetime object)
            postgres_records.append({
                'timestamp': timestamp_dt,
                'symbol': symbol,
                'open': record.get('open'),
                'high': record.get('high'),
                'low': record.get('low'),
                'close': record.get('close'),
                'volume': record.get('volume'),
                'quote_volume': record.get('quote_volume'),
                'trades_count': record.get('trades_count'),
            })
            
            # MinIO record (uses datetime object)
            minio_records.append({
                'timestamp': timestamp_dt,
                'symbol': symbol,
                'open': record.get('open'),
                'high': record.get('high'),
                'low': record.get('low'),
                'close': record.get('close'),
                'volume': record.get('volume'),
                'quote_volume': record.get('quote_volume', 0),
                'trades_count': record.get('trades_count', 0),
            })
        
        # Define tier write functions
        def write_redis():
            try:
                success_count, failed = self.redis.write_aggregations_batch(redis_records)
                return ('redis', success_count == len(redis_records), failed)
            except Exception as e:
                logger.error(f"Redis batch write failed: {e}")
                return ('redis', False, redis_records)
        
        def write_postgres():
            if self._warm_storage is None:
                return ('warm', True, [])
            try:
                row_count = self._warm_storage.upsert_candles_batch(postgres_records)
                return ('warm', row_count > 0, [])
            except Exception as e:
                logger.error(f"PostgreSQL batch write failed: {e}")
                return ('warm', False, postgres_records)
        
        def write_minio():
            if self._cold_storage is None:
                return ('cold', True, [])
            try:
                # Get write date from first record
                write_date = self._to_datetime(records[0].get('timestamp')) or datetime.now()
                success_count, failed_symbols = self._cold_storage.write_klines_batch(
                    minio_records, write_date
                )
                return ('cold', len(failed_symbols) == 0, [])
            except Exception as e:
                logger.error(f"MinIO batch write failed: {e}")
                return ('cold', False, minio_records)
        
        # Execute writes in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(write_redis): 'redis',
                executor.submit(write_postgres): 'warm',
                executor.submit(write_minio): 'cold',
            }
            
            for future in as_completed(futures, timeout=30):
                try:
                    tier, success, failed = future.result(timeout=30)
                    tier_results[tier] = success
                    if not success and failed:
                        all_failed_records.extend(failed)
                except Exception as e:
                    tier_name = futures[future]
                    logger.error(f"{tier_name} tier write timed out or failed: {e}")
                    tier_results[tier_name] = False
        
        duration_ms = (time.time() - start_time) * 1000
        
        # Calculate success/failure counts
        # Success if at least one tier succeeded
        success_count = len(records) if any(tier_results.values()) else 0
        failure_count = len(records) - success_count
        
        result = BatchResult(
            total_records=len(records),
            success_count=success_count,
            failure_count=failure_count,
            tier_results=tier_results,
            failed_records=all_failed_records,
            duration_ms=duration_ms
        )
        
        self._log_batch_result('aggregations', result)
        return result

    def write_alerts_batch(
        self, alerts: List[Dict[str, Any]]
    ) -> BatchResult:
        """Write alerts to all 3 tiers in parallel.
        
        Uses ThreadPoolExecutor to call batch methods on all storage tiers
        concurrently. Handles partial failures gracefully - if one tier fails,
        other tiers continue their writes.
        
        Redis: write_alerts_batch (pipeline LPUSH)
        PostgreSQL: insert_alerts_batch (executemany)
        MinIO: write_alerts_batch (single Parquet file)
        
        Args:
            alerts: List of alert dicts with keys:
                alert_id, timestamp (datetime), symbol, alert_type,
                alert_level, created_at, details
                
        Returns:
            BatchResult with success/failure status for each tier
            
        Requirements: 1.5, 2.1, 2.2, 2.3, 2.4
        """
        if not alerts:
            return BatchResult(
                total_records=0,
                success_count=0,
                failure_count=0,
                tier_results={'redis': True, 'warm': True, 'cold': True},
                failed_records=[],
                duration_ms=0.0
            )
        
        start_time = time.time()
        tier_results = {'redis': False, 'warm': False, 'cold': False}
        all_failed_records: List[Dict[str, Any]] = []
        
        # Prepare records for each tier
        redis_alerts = []
        postgres_alerts = []
        minio_alerts = []
        
        for alert in alerts:
            timestamp_dt = self._to_datetime(alert.get('timestamp'))
            created_at_dt = self._to_datetime(alert.get('created_at'))
            timestamp_iso = timestamp_dt.isoformat() if timestamp_dt else None
            created_at_iso = created_at_dt.isoformat() if created_at_dt else None
            symbol = alert.get('symbol', '')
            
            # Redis alert (uses ISO strings for datetime fields)
            redis_alerts.append({
                **alert,
                'timestamp': timestamp_iso,
                'created_at': created_at_iso,
            })
            
            # PostgreSQL alert (uses datetime object, maps alert_level to severity)
            postgres_alerts.append({
                'timestamp': timestamp_dt,
                'symbol': symbol,
                'alert_type': alert.get('alert_type'),
                'severity': alert.get('alert_level'),
                'message': f"{alert.get('alert_type')}: {symbol}",
                'metadata': alert.get('details'),
            })
            
            # MinIO alert (uses datetime object)
            minio_alerts.append({
                'timestamp': timestamp_dt,
                'symbol': symbol,
                'alert_type': alert.get('alert_type'),
                'severity': alert.get('alert_level'),
                'message': f"{alert.get('alert_type')}: {symbol}",
                'metadata': alert.get('details'),
            })
        
        # Define tier write functions
        def write_redis():
            try:
                success_count, failed = self.redis.write_alerts_batch(redis_alerts)
                return ('redis', success_count == len(redis_alerts), failed)
            except Exception as e:
                logger.error(f"Redis alerts batch write failed: {e}")
                return ('redis', False, redis_alerts)
        
        def write_postgres():
            if self._warm_storage is None:
                return ('warm', True, [])
            try:
                row_count = self._warm_storage.insert_alerts_batch(postgres_alerts)
                return ('warm', row_count > 0, [])
            except Exception as e:
                logger.error(f"PostgreSQL alerts batch write failed: {e}")
                return ('warm', False, postgres_alerts)
        
        def write_minio():
            if self._cold_storage is None:
                return ('cold', True, [])
            try:
                write_date = self._to_datetime(alerts[0].get('timestamp')) or datetime.now()
                success_count, errors = self._cold_storage.write_alerts_batch(
                    minio_alerts, write_date
                )
                return ('cold', len(errors) == 0, [])
            except Exception as e:
                logger.error(f"MinIO alerts batch write failed: {e}")
                return ('cold', False, minio_alerts)
        
        # Execute writes in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(write_redis): 'redis',
                executor.submit(write_postgres): 'warm',
                executor.submit(write_minio): 'cold',
            }
            
            for future in as_completed(futures, timeout=30):
                try:
                    tier, success, failed = future.result(timeout=30)
                    tier_results[tier] = success
                    if not success and failed:
                        all_failed_records.extend(failed)
                except Exception as e:
                    tier_name = futures[future]
                    logger.error(f"{tier_name} tier alerts write timed out or failed: {e}")
                    tier_results[tier_name] = False
        
        duration_ms = (time.time() - start_time) * 1000
        
        # Calculate success/failure counts
        success_count = len(alerts) if any(tier_results.values()) else 0
        failure_count = len(alerts) - success_count
        
        result = BatchResult(
            total_records=len(alerts),
            success_count=success_count,
            failure_count=failure_count,
            tier_results=tier_results,
            failed_records=all_failed_records,
            duration_ms=duration_ms
        )
        
        self._log_batch_result('alerts', result)
        return result

    def _log_batch_result(self, data_type: str, result: BatchResult) -> None:
        """Log batch write results with appropriate level.
        
        Args:
            data_type: Type of data written (aggregations, alerts)
            result: BatchResult with write statistics
        """
        tier_status = ", ".join(
            f"{tier}={'OK' if success else 'FAIL'}"
            for tier, success in result.tier_results.items()
        )
        
        if all(result.tier_results.values()):
            logger.debug(
                f"Batch write {data_type}: {result.total_records} records, "
                f"all tiers succeeded ({tier_status}), {result.duration_ms:.1f}ms"
            )
        elif not any(result.tier_results.values()):
            logger.error(
                f"Batch write {data_type}: {result.total_records} records, "
                f"all tiers failed ({tier_status}), {result.duration_ms:.1f}ms"
            )
        else:
            logger.warning(
                f"Batch write {data_type}: {result.total_records} records, "
                f"partial failure ({tier_status}), {result.duration_ms:.1f}ms"
            )
