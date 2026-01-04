"""StorageWriter - Two-tier write coordinator (Redis + PostgreSQL)."""

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from .redis import RedisStorage
from .postgres import PostgresStorage
from utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class BatchResult:
    total_records: int
    success_count: int
    failure_count: int
    tier_results: Dict[str, bool]
    failed_records: List[Dict[str, Any]] = field(default_factory=list)
    duration_ms: float = 0.0


class StorageWriter:
    def __init__(
        self,
        redis: RedisStorage,
        postgres: Optional[PostgresStorage] = None,
    ):
        """Initialize StorageWriter with storage tier instances."""
        self.redis = redis
        self._warm_storage: Optional[PostgresStorage] = postgres
        
        if self._warm_storage is None:
            logger.warning("No warm path storage configured (postgres)")

    def _write_to_tier(
        self,
        tier: str,
        write_fn: Callable[[], bool],
        data_type: str,
        symbol: str
    ) -> bool:
        """Execute a write operation for a single tier with error handling.
        
        Wraps a write callable with try/except and logging. This method
        provides consistent error handling across all tier writes.
        """
        try:
            result = write_fn()
            return result
        except Exception as e:
            logger.error(f"{tier} write_{data_type} failed for {symbol}: {e}")
            return False

    def _execute_parallel_writes(
        self,
        tier_write_fns: Dict[str, Callable[[], Tuple[str, bool, List[Dict[str, Any]]]]],
        timeout: int = 30
    ) -> Tuple[Dict[str, bool], List[Dict[str, Any]]]:
        """Execute tier writes sequentially (simplified from parallel).
        
        Each tier write function should return a tuple of:
        (tier_name, success, failed_records)
        """
        tier_results = {tier: False for tier in tier_write_fns.keys()}
        all_failed_records: List[Dict[str, Any]] = []
        
        for tier_name, write_fn in tier_write_fns.items():
            try:
                tier, success, failed = write_fn()
                tier_results[tier] = success
                if not success and failed:
                    all_failed_records.extend(failed)
            except Exception as e:
                logger.error(f"{tier_name} tier write failed: {e}")
                tier_results[tier_name] = False
        
        return tier_results, all_failed_records

    def _transform_alert_for_redis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform alert record for Redis storage.
        
        Converts datetime fields to ISO strings.
        """
        timestamp_dt: Optional[datetime] = data.get('timestamp')
        created_at_dt: Optional[datetime] = data.get('created_at')
        timestamp_iso = timestamp_dt.isoformat() if timestamp_dt else None
        created_at_iso = created_at_dt.isoformat() if created_at_dt else None
        
        return {
            **data,
            'timestamp': timestamp_iso,
            'created_at': created_at_iso,
        }

    def _transform_alert_for_postgres(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform alert record for PostgreSQL storage.
        
        Maps alert_level to severity and generates message from alert_type and symbol.
        """
        symbol = data.get('symbol', '')
        alert_type = data.get('alert_type')
        
        return {
            'timestamp': data.get('timestamp'),
            'symbol': symbol,
            'alert_type': alert_type,
            'severity': data.get('alert_level'),  # Map alert_level to severity
            'message': f"{alert_type}: {symbol}",
            'metadata': data.get('details'),
        }

    def write_aggregation(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Write aggregation data to both tiers.
        
        Redis: overwrite aggregations:{symbol}:{interval} hash
        PostgreSQL: upsert into trades_1m table
        """
        results = {'redis': False, 'warm': False}
        symbol = data.get('symbol', '')
        interval = data.get('interval', '1m')
        
        # Convert timestamp to ISO string for Redis
        ts = data.get('timestamp')
        redis_data = {**data, 'timestamp': ts.isoformat() if ts else None}
        
        def write_redis() -> bool:
            self.redis.write_aggregation(symbol, interval, redis_data)
            return True
        
        results['redis'] = self._write_to_tier('redis', write_redis, 'aggregation', symbol)
        
        def write_postgres() -> bool:
            if self._warm_storage is None:
                return True
            self._warm_storage.upsert_candle(data)
            return True
        
        results['warm'] = self._write_to_tier('warm', write_postgres, 'aggregation', symbol)
        
        self._log_write_result('aggregation', symbol, results)
        return results

    def write_alert(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Write alert to both tiers.
        
        Redis: push to alerts:recent list
        PostgreSQL: insert into alerts table
        """
        results = {'redis': False, 'warm': False}
        symbol = data.get('symbol', '')
        
        # Transform records for each tier
        redis_data = self._transform_alert_for_redis(data)
        postgres_data = self._transform_alert_for_postgres(data)
        
        # Write to Redis using generic helper
        def write_redis() -> bool:
            self.redis.write_alert(redis_data)
            return True
        
        results['redis'] = self._write_to_tier('redis', write_redis, 'alert', symbol)
        
        # Write to warm path (PostgreSQL) using generic helper
        def write_postgres() -> bool:
            if self._warm_storage is None:
                return True  # No warm storage configured, consider success
            self._warm_storage.insert_alert(postgres_data)
            return True
        
        results['warm'] = self._write_to_tier('warm', write_postgres, 'alert', symbol)
        
        self._log_write_result('alert', symbol, results)
        return results
    
    def _log_write_result(
        self, 
        data_type: str, 
        symbol: str, 
        results: Dict[str, bool]
    ) -> None:
        """Log write results with appropriate level."""
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

    def write_aggregations_batch(
        self, records: List[Dict[str, Any]]
    ) -> BatchResult:
        """Write aggregation data to both tiers.
        
        Redis: write_aggregations_batch (pipeline HSET)
        PostgreSQL: upsert_candles_batch (executemany with ON CONFLICT)
        """
        if not records:
            return BatchResult(
                total_records=0,
                success_count=0,
                failure_count=0,
                tier_results={'redis': True, 'warm': True},
                failed_records=[],
                duration_ms=0.0
            )
        
        start_time = time.time()
        
        # Convert timestamp to ISO string for Redis (inline)
        def to_redis_record(r: Dict[str, Any]) -> Dict[str, Any]:
            ts = r.get('timestamp')
            return {**r, 'timestamp': ts.isoformat() if ts else None}
        
        redis_records = [to_redis_record(r) for r in records]
        
        # Define tier write functions that return (tier_name, success, failed_records)
        def write_redis() -> Tuple[str, bool, List[Dict[str, Any]]]:
            try:
                success_count = self.redis.write_aggregations_batch(redis_records)
                return ('redis', success_count == len(redis_records), [])
            except Exception as e:
                logger.error(f"Redis batch write failed: {e}")
                return ('redis', False, redis_records)
        
        def write_postgres() -> Tuple[str, bool, List[Dict[str, Any]]]:
            if self._warm_storage is None:
                return ('warm', True, [])
            try:
                row_count = self._warm_storage.upsert_candles_batch(records)
                return ('warm', row_count > 0, [])
            except Exception as e:
                logger.error(f"PostgreSQL batch write failed: {e}")
                return ('warm', False, records)
        
        # Execute writes
        tier_write_fns = {
            'redis': write_redis,
            'warm': write_postgres,
        }
        tier_results, all_failed_records = self._execute_parallel_writes(tier_write_fns)
        
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
        """Write alerts to both tiers.
        
        Redis: write_alerts_batch (pipeline LPUSH)
        PostgreSQL: insert_alerts_batch (executemany)
        """
        if not alerts:
            return BatchResult(
                total_records=0,
                success_count=0,
                failure_count=0,
                tier_results={'redis': True, 'warm': True},
                failed_records=[],
                duration_ms=0.0
            )
        
        start_time = time.time()
        
        # Use transformer functions for record preparation
        redis_alerts = [self._transform_alert_for_redis(a) for a in alerts]
        postgres_alerts = [self._transform_alert_for_postgres(a) for a in alerts]
        
        # Define tier write functions that return (tier_name, success, failed_records)
        def write_redis() -> Tuple[str, bool, List[Dict[str, Any]]]:
            try:
                # write_alerts_batch returns int (success count), not tuple
                success_count = self.redis.write_alerts_batch(redis_alerts)
                return ('redis', success_count == len(redis_alerts), [])
            except Exception as e:
                logger.error(f"Redis alerts batch write failed: {e}")
                return ('redis', False, redis_alerts)
        
        def write_postgres() -> Tuple[str, bool, List[Dict[str, Any]]]:
            if self._warm_storage is None:
                return ('warm', True, [])
            try:
                row_count = self._warm_storage.insert_alerts_batch(postgres_alerts)
                return ('warm', row_count > 0, [])
            except Exception as e:
                logger.error(f"PostgreSQL alerts batch write failed: {e}")
                return ('warm', False, postgres_alerts)
        
        # Execute writes
        tier_write_fns = {
            'redis': write_redis,
            'warm': write_postgres,
        }
        tier_results, all_failed_records = self._execute_parallel_writes(tier_write_fns)
        
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
        """Log batch write results with appropriate level."""
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
