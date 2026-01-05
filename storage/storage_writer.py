"""Two-tier write coordinator (Redis + PostgreSQL)."""

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from .redis import Redis
from .postgres import Postgres
from util.logging import get_logger

logger = get_logger(__name__)


@dataclass
class BatchResult:
    total: int
    success: int
    failed: int
    tiers: Dict[str, bool]
    failed_records: List[Dict[str, Any]] = field(default_factory=list)
    duration_ms: float = 0.0


class Writer:
    def __init__(
        self,
        redis: Redis,
        postgres: Optional[Postgres] = None,
    ):
        self.redis = redis
        self.pg = postgres
        
        if self.pg is None:
            logger.warning("No warm storage configured (postgres)")

    def write_tier(
        self,
        tier: str,
        fn: Callable[[], bool],
        data_type: str,
        symbol: str
    ) -> bool:
        try:
            return fn()
        except Exception as e:
            logger.error(f"{tier} write_{data_type} failed for {symbol}: {e}")
            return False

    def run_writes(
        self,
        fns: Dict[str, Callable[[], Tuple[str, bool, List[Dict[str, Any]]]]],
        timeout: int = 30
    ) -> Tuple[Dict[str, bool], List[Dict[str, Any]]]:
        results = {tier: False for tier in fns.keys()}
        all_failed: List[Dict[str, Any]] = []
        
        for tier, fn in fns.items():
            try:
                name, ok, failed = fn()
                results[name] = ok
                if not ok and failed:
                    all_failed.extend(failed)
            except Exception as e:
                logger.error(f"{tier} tier write failed: {e}")
                results[tier] = False
        
        return results, all_failed

    def pack_alert_redis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        ts = data.get('timestamp')
        created = data.get('created_at')
        return {
            **data,
            'timestamp': ts.isoformat() if isinstance(ts, datetime) else ts,
            'created_at': created.isoformat() if isinstance(created, datetime) else created,
        }

    def pack_alert_pg(self, data: Dict[str, Any]) -> Dict[str, Any]:
        symbol = data.get('symbol', '')
        alert_type = data.get('alert_type')
        return {
            'timestamp': data.get('timestamp'),
            'symbol': symbol,
            'alert_type': alert_type,
            'severity': data.get('alert_level'),
            'message': f"{alert_type}: {symbol}",
            'metadata': data.get('details'),
        }

    def write_agg(self, data: Dict[str, Any]) -> Dict[str, bool]:
        results = {'redis': False, 'warm': False}
        symbol = data.get('symbol', '')
        interval = data.get('interval', '1m')
        
        ts = data.get('timestamp')
        redis_data = {**data, 'timestamp': ts.isoformat() if ts else None}
        
        def do_redis() -> bool:
            self.redis.write_agg(symbol, interval, redis_data)
            return True
        
        results['redis'] = self.write_tier('redis', do_redis, 'agg', symbol)
        
        def do_pg() -> bool:
            if self.pg is None:
                return True
            self.pg.write_candle(data)
            return True
        
        results['warm'] = self.write_tier('warm', do_pg, 'agg', symbol)
        
        self.log_result('agg', symbol, results)
        return results

    def write_alert(self, data: Dict[str, Any]) -> Dict[str, bool]:
        results = {'redis': False, 'warm': False}
        symbol = data.get('symbol', '')
        
        redis_data = self.pack_alert_redis(data)
        pg_data = self.pack_alert_pg(data)
        
        def do_redis() -> bool:
            self.redis.write_alert(redis_data)
            return True
        
        results['redis'] = self.write_tier('redis', do_redis, 'alert', symbol)
        
        def do_pg() -> bool:
            if self.pg is None:
                return True
            self.pg.write_alert(pg_data)
            return True
        
        results['warm'] = self.write_tier('warm', do_pg, 'alert', symbol)
        
        self.log_result('alert', symbol, results)
        return results
    
    def log_result(self, data_type: str, symbol: str, results: Dict[str, bool]) -> None:
        ok = sum(results.values())
        total = len(results)
        
        if ok == total:
            logger.debug(f"Write {data_type} for {symbol}: all tiers OK")
        elif ok == 0:
            logger.error(f"Write {data_type} for {symbol}: all tiers failed")
        else:
            failed = [k for k, v in results.items() if not v]
            logger.warning(f"Write {data_type} for {symbol}: {failed} failed")

    def write_aggs(self, records: List[Dict[str, Any]]) -> BatchResult:
        if not records:
            return BatchResult(
                total=0, success=0, failed=0,
                tiers={'redis': True, 'warm': True},
                failed_records=[], duration_ms=0.0
            )
        
        start = time.time()
        
        def to_redis(r: Dict[str, Any]) -> Dict[str, Any]:
            ts = r.get('timestamp')
            return {**r, 'timestamp': ts.isoformat() if ts else None}
        
        redis_records = [to_redis(r) for r in records]
        
        def do_redis() -> Tuple[str, bool, List[Dict[str, Any]]]:
            try:
                n = self.redis.write_aggs(redis_records)
                return ('redis', n == len(redis_records), [])
            except Exception as e:
                logger.error(f"Redis batch write failed: {e}")
                return ('redis', False, redis_records)
        
        def do_pg() -> Tuple[str, bool, List[Dict[str, Any]]]:
            if self.pg is None:
                return ('warm', True, [])
            try:
                n = self.pg.write_candles(records)
                return ('warm', n > 0, [])
            except Exception as e:
                logger.error(f"PostgreSQL batch write failed: {e}")
                return ('warm', False, records)
        
        fns = {'redis': do_redis, 'warm': do_pg}
        tier_results, all_failed = self.run_writes(fns)
        
        duration = (time.time() - start) * 1000
        ok = len(records) if any(tier_results.values()) else 0
        
        result = BatchResult(
            total=len(records),
            success=ok,
            failed=len(records) - ok,
            tiers=tier_results,
            failed_records=all_failed,
            duration_ms=duration
        )
        
        self.log_batch('aggs', result)
        return result

    def write_alerts(self, alerts: List[Dict[str, Any]]) -> BatchResult:
        if not alerts:
            return BatchResult(
                total=0, success=0, failed=0,
                tiers={'redis': True, 'warm': True},
                failed_records=[], duration_ms=0.0
            )
        
        start = time.time()
        
        redis_alerts = [self.pack_alert_redis(a) for a in alerts]
        pg_alerts = [self.pack_alert_pg(a) for a in alerts]
        
        def do_redis() -> Tuple[str, bool, List[Dict[str, Any]]]:
            try:
                n = self.redis.write_alerts(redis_alerts)
                return ('redis', n == len(redis_alerts), [])
            except Exception as e:
                logger.error(f"Redis alerts batch write failed: {e}")
                return ('redis', False, redis_alerts)
        
        def do_pg() -> Tuple[str, bool, List[Dict[str, Any]]]:
            if self.pg is None:
                return ('warm', True, [])
            try:
                n = self.pg.write_alerts(pg_alerts)
                return ('warm', n > 0, [])
            except Exception as e:
                logger.error(f"PostgreSQL alerts batch write failed: {e}")
                return ('warm', False, pg_alerts)
        
        fns = {'redis': do_redis, 'warm': do_pg}
        tier_results, all_failed = self.run_writes(fns)
        
        duration = (time.time() - start) * 1000
        ok = len(alerts) if any(tier_results.values()) else 0
        
        result = BatchResult(
            total=len(alerts),
            success=ok,
            failed=len(alerts) - ok,
            tiers=tier_results,
            failed_records=all_failed,
            duration_ms=duration
        )
        
        self.log_batch('alerts', result)
        return result

    def log_batch(self, data_type: str, result: BatchResult) -> None:
        status = ", ".join(
            f"{t}={'OK' if ok else 'FAIL'}"
            for t, ok in result.tiers.items()
        )
        
        msg = f"Batch {data_type}: {result.total} records, {status}, {result.duration_ms:.1f}ms"
        
        if all(result.tiers.values()):
            logger.debug(msg)
        elif not any(result.tiers.values()):
            logger.error(msg)
        else:
            logger.warning(msg)
