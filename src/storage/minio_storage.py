"""
MinioStorage - Cold Path storage for historical archive.

Provides S3-compatible object storage for historical data using MinIO.
Stores data in date-partitioned Parquet files with snappy compression.
"""

import io
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinioStorage:
    """MinIO/S3 storage for historical data (Cold Path).
    
    Stores data in S3-compatible bucket with date-partitioned paths:
    {bucket}/{data_type}/symbol={symbol}/date={YYYY-MM-DD}/data.parquet
    
    Uses snappy compression for efficient storage.
    """
    
    # Parquet configuration
    COMPRESSION = "snappy"
    
    # Schema definitions (matching ParquetStorage)
    KLINES_SCHEMA = pa.schema([
        ('timestamp', pa.timestamp('ms')),
        ('symbol', pa.string()),
        ('open', pa.float64()),
        ('high', pa.float64()),
        ('low', pa.float64()),
        ('close', pa.float64()),
        ('volume', pa.float64()),
        ('quote_volume', pa.float64()),
        ('trades_count', pa.int64()),
    ])
    
    INDICATORS_SCHEMA = pa.schema([
        ('timestamp', pa.timestamp('ms')),
        ('symbol', pa.string()),
        ('rsi', pa.float64()),
        ('macd', pa.float64()),
        ('macd_signal', pa.float64()),
        ('sma_20', pa.float64()),
        ('ema_12', pa.float64()),
        ('ema_26', pa.float64()),
        ('bb_upper', pa.float64()),
        ('bb_lower', pa.float64()),
        ('atr', pa.float64()),
    ])

    ALERTS_SCHEMA = pa.schema([
        ('timestamp', pa.timestamp('ms')),
        ('symbol', pa.string()),
        ('alert_type', pa.string()),
        ('severity', pa.string()),
        ('message', pa.string()),
        ('metadata', pa.string()),  # JSON string
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
        """Initialize MinIO client.
        
        Args:
            endpoint: MinIO server endpoint (host:port)
            access_key: MinIO access key
            secret_key: MinIO secret key
            bucket: Bucket name for storing data
            secure: Use HTTPS if True
            max_retries: Maximum number of retry attempts
            retry_delay: Base delay in seconds between retries (exponential backoff)
        """
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.secure = secure
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        self._client: Optional[Minio] = None
        self._connect_with_retry()
        self._ensure_bucket()
        logger.info(
            f"MinioStorage initialized at {endpoint}, bucket={bucket}"
        )

    def _connect_with_retry(self) -> None:
        """Create MinIO client with retry logic and exponential backoff.
        
        Raises:
            S3Error: If connection fails after all retries
        """
        last_error = None
        for attempt in range(self.max_retries):
            try:
                self._client = Minio(
                    self.endpoint,
                    access_key=self.access_key,
                    secret_key=self.secret_key,
                    secure=self.secure
                )
                # Test connection by listing buckets
                self._client.list_buckets()
                if attempt > 0:
                    logger.info(
                        f"MinIO connection successful on attempt {attempt + 1}"
                    )
                return
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(
                        f"MinIO connection failed (attempt {attempt + 1}/{self.max_retries}), "
                        f"retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"MinIO connection failed after {self.max_retries} attempts"
                    )
                    raise
        
        raise last_error

    def _ensure_bucket(self) -> None:
        """Create bucket if it doesn't exist.
        
        Implements Requirement 2.1: Store files in S3-compatible bucket.
        """
        try:
            if not self._client.bucket_exists(self.bucket):
                self._client.make_bucket(self.bucket)
                logger.info(f"Created bucket: {self.bucket}")
            else:
                logger.debug(f"Bucket already exists: {self.bucket}")
        except S3Error as e:
            logger.error(f"Failed to ensure bucket exists: {e}")
            raise


    def _get_object_path(
        self, 
        data_type: str, 
        symbol: str, 
        date: datetime
    ) -> str:
        """Generate object path for a given data type, symbol, and date.
        
        Path format: {data_type}/symbol={symbol}/date={YYYY-MM-DD}/data.parquet
        
        Args:
            data_type: Type of data (klines, indicators, alerts)
            symbol: Trading symbol
            date: Date for partitioning
            
        Returns:
            Object path within bucket
        """
        date_str = date.strftime("%Y-%m-%d")
        return f"{data_type}/symbol={symbol}/date={date_str}/data.parquet"

    def _execute_with_retry(self, operation, *args, **kwargs):
        """Execute an operation with retry logic.
        
        Args:
            operation: Callable to execute
            *args: Positional arguments for operation
            **kwargs: Keyword arguments for operation
            
        Returns:
            Result of the operation
            
        Raises:
            Exception: If operation fails after all retries
        """
        last_error = None
        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(
                        f"Operation failed (attempt {attempt + 1}/{self.max_retries}), "
                        f"retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
                else:
                    raise
        
        raise last_error

    def _write_parquet_to_minio(
        self, 
        table: pa.Table, 
        object_path: str
    ) -> bool:
        """Write a PyArrow table to MinIO as Parquet.
        
        Args:
            table: PyArrow table to write
            object_path: Object path within bucket
            
        Returns:
            True if successful
        """
        # Write table to in-memory buffer
        buffer = io.BytesIO()
        pq.write_table(
            table,
            buffer,
            compression=self.COMPRESSION
        )
        buffer.seek(0)
        
        # Upload to MinIO
        data_length = buffer.getbuffer().nbytes
        self._execute_with_retry(
            self._client.put_object,
            self.bucket,
            object_path,
            buffer,
            data_length,
            content_type="application/octet-stream"
        )
        
        logger.debug(f"Wrote {data_length} bytes to {object_path}")
        return True

    def _read_parquet_from_minio(self, object_path: str) -> Optional[pa.Table]:
        """Read a Parquet file from MinIO.
        
        Args:
            object_path: Object path within bucket
            
        Returns:
            PyArrow table or None if not found
        """
        try:
            response = self._execute_with_retry(
                self._client.get_object,
                self.bucket,
                object_path
            )
            
            # Read into buffer
            buffer = io.BytesIO(response.read())
            response.close()
            response.release_conn()
            
            # Parse Parquet
            return pq.read_table(buffer)
        except S3Error as e:
            if e.code == "NoSuchKey":
                logger.debug(f"Object not found: {object_path}")
                return None
            raise


    # ==================== Write Methods ====================

    def write_klines(
        self, 
        symbol: str, 
        data: List[Dict[str, Any]], 
        date: datetime
    ) -> bool:
        """Write klines data as Parquet to MinIO.
        
        Implements Requirements 2.1, 2.4: Store in S3-compatible bucket
        with snappy compression and date-partitioned paths.
        
        Args:
            symbol: Trading symbol
            data: List of kline dictionaries
            date: Date for partitioning
            
        Returns:
            True if successful
        """
        if not data:
            return True
        
        # Convert to PyArrow table
        arrays = {
            'timestamp': pa.array([
                d["timestamp"] if isinstance(d["timestamp"], datetime)
                else datetime.fromtimestamp(
                    d["timestamp"] / 1000 if d["timestamp"] > 1e12 else d["timestamp"]
                )
                for d in data
            ], type=pa.timestamp('ms')),
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

    def write_indicators(
        self, 
        symbol: str, 
        data: List[Dict[str, Any]], 
        date: datetime
    ) -> bool:
        """Write indicators data as Parquet to MinIO.
        
        Implements Requirements 2.1, 2.4: Store in S3-compatible bucket
        with snappy compression and date-partitioned paths.
        
        Args:
            symbol: Trading symbol
            data: List of indicator dictionaries
            date: Date for partitioning
            
        Returns:
            True if successful
        """
        if not data:
            return True
        
        # Helper to safely convert to float, handling None values
        def safe_float(value, default=0.0):
            if value is None:
                return default
            return float(value)
        
        # Convert to PyArrow table
        arrays = {
            'timestamp': pa.array([
                d["timestamp"] if isinstance(d["timestamp"], datetime)
                else datetime.fromtimestamp(
                    d["timestamp"] / 1000 if d["timestamp"] > 1e12 else d["timestamp"]
                )
                for d in data
            ], type=pa.timestamp('ms')),
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

    def write_alerts(
        self, 
        symbol: str, 
        data: List[Dict[str, Any]], 
        date: datetime
    ) -> bool:
        """Write alerts data as Parquet to MinIO.
        
        Implements Requirements 2.1, 2.4: Store in S3-compatible bucket
        with snappy compression and date-partitioned paths.
        
        Args:
            symbol: Trading symbol
            data: List of alert dictionaries
            date: Date for partitioning
            
        Returns:
            True if successful
        """
        if not data:
            return True
        
        import json
        
        def parse_timestamp(ts):
            """Parse timestamp from various formats."""
            if isinstance(ts, datetime):
                return ts
            if isinstance(ts, str):
                try:
                    return datetime.fromisoformat(ts.replace('Z', '+00:00'))
                except ValueError:
                    return datetime.now()
            if isinstance(ts, (int, float)):
                if ts > 1e12:
                    return datetime.fromtimestamp(ts / 1000)
                return datetime.fromtimestamp(ts)
            return datetime.now()
        
        # Convert to PyArrow table
        arrays = {
            'timestamp': pa.array([
                parse_timestamp(d["timestamp"])
                for d in data
            ], type=pa.timestamp('ms')),
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


    # ==================== Read Methods ====================

    def read_klines(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Read klines from date-partitioned Parquet files.
        
        Implements Requirement 2.2: Retrieve Parquet files from
        appropriate date partition.
        
        Args:
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of kline dictionaries (empty list if not found)
        """
        all_records = []
        
        # Iterate through each date in range
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        while current <= end_date:
            object_path = self._get_object_path("klines", symbol, current)
            table = self._read_parquet_from_minio(object_path)
            
            if table is not None:
                df = table.to_pandas()
                # Convert timestamp to timezone-naive for comparison
                if df['timestamp'].dt.tz is not None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                # Filter by time range
                df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
                all_records.extend(df.to_dict('records'))
            
            current += timedelta(days=1)
        
        # Sort by timestamp
        all_records.sort(key=lambda x: x['timestamp'])
        return all_records

    def read_indicators(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Read indicators from date-partitioned Parquet files.
        
        Implements Requirement 2.2: Retrieve Parquet files from
        appropriate date partition.
        
        Args:
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of indicator dictionaries (empty list if not found)
        """
        all_records = []
        
        # Iterate through each date in range
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        while current <= end_date:
            object_path = self._get_object_path("indicators", symbol, current)
            table = self._read_parquet_from_minio(object_path)
            
            if table is not None:
                df = table.to_pandas()
                # Convert timestamp to timezone-naive for comparison
                if df['timestamp'].dt.tz is not None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                # Filter by time range
                df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
                all_records.extend(df.to_dict('records'))
            
            current += timedelta(days=1)
        
        # Sort by timestamp
        all_records.sort(key=lambda x: x['timestamp'])
        return all_records

    def read_alerts(
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime
    ) -> List[Dict[str, Any]]:
        """Read alerts from date-partitioned Parquet files.
        
        Implements Requirement 2.2: Retrieve Parquet files from
        appropriate date partition.
        
        Args:
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            
        Returns:
            List of alert dictionaries (empty list if not found)
        """
        import json
        
        all_records = []
        
        # Iterate through each date in range
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        while current <= end_date:
            object_path = self._get_object_path("alerts", symbol, current)
            table = self._read_parquet_from_minio(object_path)
            
            if table is not None:
                df = table.to_pandas()
                # Convert timestamp to timezone-naive for comparison
                if df['timestamp'].dt.tz is not None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                # Filter by time range
                df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
                records = df.to_dict('records')
                
                # Parse metadata JSON
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
        
        # Sort by timestamp
        all_records.sort(key=lambda x: x['timestamp'])
        return all_records

    def close(self) -> None:
        """Close the MinIO client (no-op, client is stateless)."""
        logger.info("MinioStorage closed")
