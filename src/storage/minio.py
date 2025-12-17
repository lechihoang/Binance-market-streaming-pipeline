"""
MinIO storage module for cold path data access.

Stores historical data in S3-compatible bucket with date-partitioned paths.
Path format: {bucket}/{data_type}/symbol={symbol}/date={YYYY-MM-DD}/data.parquet
"""

import io
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

from src.utils.logging import get_logger
from src.utils.retry import RetryConfig, retry_operation
from src.utils.metrics import track_latency, record_error, record_retry

logger = get_logger(__name__)


class MinioStorage:
    """MinIO/S3 storage for historical data (Cold Path)."""
    
    COMPRESSION = "snappy"
    
    KLINES_SCHEMA = pa.schema([
        ('timestamp', pa.timestamp('ms')), ('symbol', pa.string()),
        ('open', pa.float64()), ('high', pa.float64()), ('low', pa.float64()),
        ('close', pa.float64()), ('volume', pa.float64()),
        ('quote_volume', pa.float64()), ('trades_count', pa.int64()),
    ])
    
    INDICATORS_SCHEMA = pa.schema([
        ('timestamp', pa.timestamp('ms')), ('symbol', pa.string()),
        ('rsi', pa.float64()), ('macd', pa.float64()), ('macd_signal', pa.float64()),
        ('sma_20', pa.float64()), ('ema_12', pa.float64()), ('ema_26', pa.float64()),
        ('bb_upper', pa.float64()), ('bb_lower', pa.float64()), ('atr', pa.float64()),
    ])

    ALERTS_SCHEMA = pa.schema([
        ('timestamp', pa.timestamp('ms')), ('symbol', pa.string()),
        ('alert_type', pa.string()), ('severity', pa.string()),
        ('message', pa.string()), ('metadata', pa.string()),
    ])
    
    def __init__(
        self,
        endpoint: str = "localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        bucket: str = "crypto-data",
        secure: bool = False,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.secure = secure
        
        self._retry_config = RetryConfig(
            max_retries=max_retries,
            initial_delay_ms=int(retry_delay * 1000),
            max_delay_ms=60000,
            multiplier=2.0,
            jitter_factor=0.1,
            retryable_exceptions=(S3Error, Exception),
        )
        
        self._client: Optional[Minio] = None
        self._connect_with_retry()
        self._ensure_bucket()
        logger.info(f"MinioStorage initialized at {endpoint}, bucket={bucket}")


    def _connect_with_retry(self) -> None:
        """Create MinIO client with retry logic."""
        def create_client():
            self._client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure
            )
            self._client.list_buckets()
            return self._client
        
        def on_retry(attempt: int, delay_ms: int, error: Exception):
            record_retry("minio_storage", "connect", "failed")
        
        try:
            retry_operation(
                create_client, config=self._retry_config,
                operation_name="MinIO connection", on_retry=on_retry,
            )
            record_retry("minio_storage", "connect", "success")
        except Exception:
            record_error("minio_storage", "connection_error", "critical")
            raise

    def _ensure_bucket(self) -> None:
        """Create bucket if it doesn't exist."""
        try:
            if not self._client.bucket_exists(self.bucket):
                self._client.make_bucket(self.bucket)
                logger.info(f"Created bucket: {self.bucket}")
        except S3Error as e:
            logger.error(f"Failed to ensure bucket exists: {e}")
            raise

    def _get_object_path(self, data_type: str, symbol: str, date: datetime) -> str:
        """Generate object path for a given data type, symbol, and date."""
        date_str = date.strftime("%Y-%m-%d")
        return f"{data_type}/symbol={symbol}/date={date_str}/data.parquet"

    def _execute_with_retry(self, operation, *args, **kwargs):
        """Execute an operation with retry logic."""
        def execute_op():
            return operation(*args, **kwargs)
        
        def on_retry(attempt: int, delay_ms: int, error: Exception):
            record_retry("minio_storage", "operation", "failed")
        
        try:
            with track_latency("minio_storage", "operation"):
                result = retry_operation(
                    execute_op, config=self._retry_config,
                    operation_name="MinIO operation", on_retry=on_retry,
                )
            record_retry("minio_storage", "operation", "success")
            return result
        except Exception:
            record_error("minio_storage", "operation_error", "error")
            raise

    def _write_parquet_to_minio(self, table: pa.Table, object_path: str) -> bool:
        """Write a PyArrow table to MinIO as Parquet."""
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression=self.COMPRESSION)
        buffer.seek(0)
        
        data_length = buffer.getbuffer().nbytes
        self._execute_with_retry(
            self._client.put_object, self.bucket, object_path,
            buffer, data_length, content_type="application/octet-stream"
        )
        logger.debug(f"Wrote {data_length} bytes to {object_path}")
        return True

    def _read_parquet_from_minio(self, object_path: str) -> Optional[pa.Table]:
        """Read a Parquet file from MinIO."""
        try:
            response = self._execute_with_retry(
                self._client.get_object, self.bucket, object_path
            )
            buffer = io.BytesIO(response.read())
            response.close()
            response.release_conn()
            return pq.read_table(buffer)
        except S3Error as e:
            if e.code == "NoSuchKey":
                logger.debug(f"Object not found: {object_path}")
                return None
            raise




    # ==================== Write Methods ====================

    def write_klines(self, symbol: str, data: List[Dict[str, Any]], date: datetime) -> bool:
        """Write klines data as Parquet to MinIO.
        
        Args:
            symbol: Trading symbol
            data: List of kline dicts with datetime objects for timestamp
            date: Date for partitioning
        """
        if not data:
            return True
        
        arrays = {
            'timestamp': pa.array([d["timestamp"] for d in data], type=pa.timestamp('ms')),
            'symbol': pa.array([d.get("symbol", symbol) for d in data]),
            'open': pa.array([float(d["open"]) for d in data]),
            'high': pa.array([float(d["high"]) for d in data]),
            'low': pa.array([float(d["low"]) for d in data]),
            'close': pa.array([float(d["close"]) for d in data]),
            'volume': pa.array([float(d["volume"]) for d in data]),
            'quote_volume': pa.array([float(d.get("quote_volume", 0)) for d in data]),
            'trades_count': pa.array([int(d.get("trades_count", 0)) for d in data]),
        }
        table = pa.table(arrays, schema=self.KLINES_SCHEMA)
        object_path = self._get_object_path("klines", symbol, date)
        return self._write_parquet_to_minio(table, object_path)

    def write_indicators(self, symbol: str, data: List[Dict[str, Any]], date: datetime) -> bool:
        """Write indicators data as Parquet to MinIO.
        
        Args:
            symbol: Trading symbol
            data: List of indicator dicts with datetime objects for timestamp
            date: Date for partitioning
        """
        if not data:
            return True
        
        def safe_float(value, default=0.0):
            return float(value) if value is not None else default
        
        arrays = {
            'timestamp': pa.array([d["timestamp"] for d in data], type=pa.timestamp('ms')),
            'symbol': pa.array([d.get("symbol", symbol) for d in data]),
            'rsi': pa.array([safe_float(d.get("rsi")) for d in data]),
            'macd': pa.array([safe_float(d.get("macd")) for d in data]),
            'macd_signal': pa.array([safe_float(d.get("macd_signal")) for d in data]),
            'sma_20': pa.array([safe_float(d.get("sma_20")) for d in data]),
            'ema_12': pa.array([safe_float(d.get("ema_12")) for d in data]),
            'ema_26': pa.array([safe_float(d.get("ema_26")) for d in data]),
            'bb_upper': pa.array([safe_float(d.get("bb_upper")) for d in data]),
            'bb_lower': pa.array([safe_float(d.get("bb_lower")) for d in data]),
            'atr': pa.array([safe_float(d.get("atr")) for d in data]),
        }
        table = pa.table(arrays, schema=self.INDICATORS_SCHEMA)
        object_path = self._get_object_path("indicators", symbol, date)
        return self._write_parquet_to_minio(table, object_path)

    def write_alerts(self, symbol: str, data: List[Dict[str, Any]], date: datetime) -> bool:
        """Write alerts data as Parquet to MinIO.
        
        Args:
            symbol: Trading symbol
            data: List of alert dicts with datetime objects for timestamp
            date: Date for partitioning
        """
        if not data:
            return True
        
        arrays = {
            'timestamp': pa.array([d["timestamp"] for d in data], type=pa.timestamp('ms')),
            'symbol': pa.array([d.get("symbol", symbol) for d in data]),
            'alert_type': pa.array([d["alert_type"] for d in data]),
            'severity': pa.array([d["severity"] for d in data]),
            'message': pa.array([d.get("message", "") for d in data]),
            'metadata': pa.array([
                json.dumps(d.get("metadata")) if d.get("metadata") else ""
                for d in data
            ]),
        }
        table = pa.table(arrays, schema=self.ALERTS_SCHEMA)
        object_path = self._get_object_path("alerts", symbol, date)
        return self._write_parquet_to_minio(table, object_path)

    # ==================== Batch Write Methods ====================

    def _get_batch_object_path(self, data_type: str, symbol: str, date: datetime) -> str:
        """Generate batch object path with timestamp for uniqueness.
        
        Path format: {data_type}/symbol={symbol}/year={YYYY}/month={MM}/day={DD}/batch_{timestamp}.parquet
        """
        return (
            f"{data_type}/symbol={symbol}/"
            f"year={date.year}/month={date.month:02d}/day={date.day:02d}/"
            f"batch_{int(time.time() * 1000)}.parquet"
        )

    def write_klines_batch(
        self, records: List[Dict[str, Any]], write_date: Optional[datetime] = None
    ) -> Tuple[int, List[str]]:
        """Write batch of klines as single Parquet file per symbol.
        
        Groups records by symbol and writes one Parquet file per symbol.
        Uses date-based partitioning (year/month/day).
        
        Args:
            records: List of kline dicts with timestamp, symbol, OHLCV data
            write_date: Date for partitioning (defaults to current date)
            
        Returns:
            Tuple of (success_count, list of failed symbols)
            
        Requirements: 1.4, 5.1, 5.3
        """
        if not records:
            return (0, [])
        
        write_date = write_date or datetime.now()
        
        # Group records by symbol
        by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for record in records:
            symbol = record.get("symbol", "UNKNOWN")
            by_symbol[symbol].append(record)
        
        success_count = 0
        failed_symbols: List[str] = []
        
        for symbol, klines in by_symbol.items():
            try:
                arrays = {
                    'timestamp': pa.array(
                        [d["timestamp"] for d in klines], type=pa.timestamp('ms')
                    ),
                    'symbol': pa.array([d.get("symbol", symbol) for d in klines]),
                    'open': pa.array([float(d["open"]) for d in klines]),
                    'high': pa.array([float(d["high"]) for d in klines]),
                    'low': pa.array([float(d["low"]) for d in klines]),
                    'close': pa.array([float(d["close"]) for d in klines]),
                    'volume': pa.array([float(d["volume"]) for d in klines]),
                    'quote_volume': pa.array([float(d.get("quote_volume", 0)) for d in klines]),
                    'trades_count': pa.array([int(d.get("trades_count", 0)) for d in klines]),
                }
                table = pa.table(arrays, schema=self.KLINES_SCHEMA)
                object_path = self._get_batch_object_path("klines", symbol, write_date)
                
                if self._write_parquet_to_minio(table, object_path):
                    success_count += len(klines)
                    logger.debug(f"Wrote {len(klines)} klines for {symbol} to {object_path}")
                else:
                    failed_symbols.append(symbol)
                    logger.error(f"Failed to write klines batch for {symbol}")
            except Exception as e:
                failed_symbols.append(symbol)
                logger.error(f"Error writing klines batch for {symbol}: {e}")
        
        return (success_count, failed_symbols)

    def write_alerts_batch(
        self, alerts: List[Dict[str, Any]], write_date: Optional[datetime] = None
    ) -> Tuple[int, List[str]]:
        """Write batch of alerts as single Parquet file.
        
        Writes all alerts to a single Parquet file with date-based partitioning.
        
        Args:
            alerts: List of alert dicts with timestamp, symbol, alert_type, etc.
            write_date: Date for partitioning (defaults to current date)
            
        Returns:
            Tuple of (success_count, list of error messages)
            
        Requirements: 5.2, 5.3
        """
        if not alerts:
            return (0, [])
        
        write_date = write_date or datetime.now()
        errors: List[str] = []
        
        try:
            arrays = {
                'timestamp': pa.array(
                    [d["timestamp"] for d in alerts], type=pa.timestamp('ms')
                ),
                'symbol': pa.array([d.get("symbol", "UNKNOWN") for d in alerts]),
                'alert_type': pa.array([d["alert_type"] for d in alerts]),
                'severity': pa.array([d["severity"] for d in alerts]),
                'message': pa.array([d.get("message", "") for d in alerts]),
                'metadata': pa.array([
                    json.dumps(d.get("metadata")) if d.get("metadata") else ""
                    for d in alerts
                ]),
            }
            table = pa.table(arrays, schema=self.ALERTS_SCHEMA)
            
            # Use "all" as symbol for batch alerts file
            object_path = self._get_batch_object_path("alerts", "all", write_date)
            
            if self._write_parquet_to_minio(table, object_path):
                logger.debug(f"Wrote {len(alerts)} alerts to {object_path}")
                return (len(alerts), [])
            else:
                errors.append("Failed to write alerts batch to MinIO")
                return (0, errors)
        except Exception as e:
            errors.append(f"Error writing alerts batch: {e}")
            logger.error(f"Error writing alerts batch: {e}")
            return (0, errors)


    # ==================== Read Methods ====================

    def read_klines(self, symbol: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        """Read klines from date-partitioned Parquet files."""
        all_records = []
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        while current <= end_date:
            object_path = self._get_object_path("klines", symbol, current)
            table = self._read_parquet_from_minio(object_path)
            
            if table is not None:
                df = table.to_pandas()
                if df['timestamp'].dt.tz is not None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
                all_records.extend(df.to_dict('records'))
            
            current += timedelta(days=1)
        
        all_records.sort(key=lambda x: x['timestamp'])
        return all_records

    def read_indicators(self, symbol: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        """Read indicators from date-partitioned Parquet files."""
        all_records = []
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        while current <= end_date:
            object_path = self._get_object_path("indicators", symbol, current)
            table = self._read_parquet_from_minio(object_path)
            
            if table is not None:
                df = table.to_pandas()
                if df['timestamp'].dt.tz is not None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
                all_records.extend(df.to_dict('records'))
            
            current += timedelta(days=1)
        
        all_records.sort(key=lambda x: x['timestamp'])
        return all_records

    def read_alerts(self, symbol: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        """Read alerts from date-partitioned Parquet files."""
        all_records = []
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        while current <= end_date:
            object_path = self._get_object_path("alerts", symbol, current)
            table = self._read_parquet_from_minio(object_path)
            
            if table is not None:
                df = table.to_pandas()
                if df['timestamp'].dt.tz is not None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
                records = df.to_dict('records')
                
                for record in records:
                    if record.get('metadata'):
                        try:
                            record['metadata'] = json.loads(record['metadata'])
                        except (json.JSONDecodeError, TypeError):
                            record['metadata'] = None
                    else:
                        record['metadata'] = None
                
                all_records.extend(records)
            
            current += timedelta(days=1)
        
        all_records.sort(key=lambda x: x['timestamp'])
        return all_records

    def close(self) -> None:
        """Close the MinIO client (no-op, client is stateless)."""
        logger.info("MinioStorage closed")


# ============================================================================
# HEALTH CHECK
# ============================================================================

def check_minio_health(
    endpoint: str = "localhost:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
    bucket: str = "crypto-data",
    secure: bool = False,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    **context
) -> Dict[str, Any]:
    """Check MinIO connection health."""
    retry_config = RetryConfig(
        max_retries=max_retries,
        initial_delay_ms=int(retry_delay * 1000),
        max_delay_ms=60000,
        multiplier=2.0,
        jitter_factor=0.1,
    )
    
    attempt_count = [0]
    
    def do_health_check():
        attempt_count[0] += 1
        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        client.list_buckets()
        
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            logger.info(f"Created MinIO bucket: {bucket}")
        
        return {
            'service': 'minio', 'tier': 'cold', 'status': 'healthy',
            'endpoint': endpoint, 'bucket': bucket,
            'attempt': attempt_count[0], 'timestamp': datetime.now().isoformat()
        }
    
    def on_retry(attempt: int, delay_ms: int, error: Exception):
        record_retry("minio_health", "check", "failed")
    
    try:
        with track_latency("minio_health", "check"):
            result = retry_operation(
                do_health_check, config=retry_config,
                operation_name="MinIO health check", on_retry=on_retry,
            )
        logger.info(f"MinIO health check passed: {endpoint}, bucket={bucket}")
        record_retry("minio_health", "check", "success")
        return result
    except Exception as e:
        record_error("minio_health", "health_check_error", "critical")
        raise Exception(f"MinIO health check failed after {max_retries} attempts: {e}")
