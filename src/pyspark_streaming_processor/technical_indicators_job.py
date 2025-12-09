"""
Technical Indicators Job

Calculates technical indicators from aggregated candle data.
Reads from processed_aggregations Kafka topic, maintains stateful storage
of candles, computes indicators (SMA, EMA, RSI, MACD, BB, ATR), and writes
to multiple sinks (Kafka, Redis, DuckDB).

This is a self-contained job with inline logging, metrics, and memory monitoring.
"""

import logging
import signal
import sys
import time
import json
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from collections import deque
from dataclasses import dataclass, field

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import (
    col, from_json, to_json, struct, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, TimestampType
)

from .config import Config
from .connectors import RedisConnector, DuckDBConnector
from .graceful_shutdown import GracefulShutdown

# Import storage tier classes for StorageWriter integration
from src.storage.redis_storage import RedisStorage
from src.storage.postgres_storage import PostgresStorage
from src.storage.minio_storage import MinioStorage
from src.storage.storage_writer import StorageWriter


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured logging."""
    
    def __init__(self, job_name: str):
        super().__init__()
        self.job_name = job_name
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with structured information."""
        timestamp = datetime.utcnow().isoformat()
        
        log_parts = [
            f"[{timestamp}]",
            f"[{record.levelname}]",
            f"[{self.job_name}]",
            f"[{record.name}]",
            record.getMessage(),
        ]
        
        if record.exc_info:
            log_parts.append("\n" + self.formatException(record.exc_info))
        
        return " ".join(log_parts)


@dataclass
class CandleState:
    """
    State container for maintaining candle history per symbol.
    
    Stores up to 200 candles for indicator calculations.
    """
    symbol: str
    candles: deque = field(default_factory=lambda: deque(maxlen=200))
    last_update: int = 0
    
    def add_candle(self, candle: Dict[str, Any]) -> None:
        """
        Add a candle to state, automatically removing oldest if > 200.
        
        Args:
            candle: Dictionary with candle data (timestamp, open, high, low, close, volume)
        """
        self.candles.append(candle)
        self.last_update = int(datetime.now().timestamp())


class IndicatorCalculator:
    """
    Calculator for technical indicators.
    
    Implements SMA, EMA, RSI, MACD, Bollinger Bands, and ATR calculations.
    """
    
    @staticmethod
    def sma(prices: List[float], period: int) -> Optional[float]:
        """
        Calculate Simple Moving Average.
        
        Args:
            prices: List of closing prices
            period: Number of periods for SMA
            
        Returns:
            SMA value or None if insufficient data
        """
        if len(prices) < period:
            return None
        return sum(prices[-period:]) / period
    
    @staticmethod
    def ema(prices: List[float], period: int, prev_ema: Optional[float] = None) -> Optional[float]:
        """
        Calculate Exponential Moving Average.
        
        Args:
            prices: List of closing prices
            period: Number of periods for EMA
            prev_ema: Previous EMA value (for recursive calculation)
            
        Returns:
            EMA value or None if insufficient data
        """
        if len(prices) < period:
            return None
        
        k = 2 / (period + 1)
        
        if prev_ema is None:
            # Initialize with SMA
            ema = sum(prices[:period]) / period
            
            # Calculate EMA iteratively for all remaining prices
            for price in prices[period:]:
                ema = (price * k) + (ema * (1 - k))
            
            return ema
        else:
            # Use provided previous EMA and calculate for last price only
            return (prices[-1] * k) + (prev_ema * (1 - k))
    
    @staticmethod
    def rsi(prices: List[float], period: int = 14) -> Optional[float]:
        """
        Calculate Relative Strength Index using Wilder's Smoothed Moving Average.
        
        This is the standard RSI calculation used by most trading platforms.
        Uses SMMA (Smoothed Moving Average) for avg_gain and avg_loss.
        
        Args:
            prices: List of closing prices
            period: Number of periods for RSI (default 14)
            
        Returns:
            RSI value (0-100) or None if insufficient data
        """
        if len(prices) < period + 1:
            return None
        
        # Calculate price changes
        changes = []
        for i in range(1, len(prices)):
            changes.append(prices[i] - prices[i-1])
        
        # Separate gains and losses
        gains = [max(0, c) for c in changes]
        losses = [abs(min(0, c)) for c in changes]
        
        # First avg_gain/avg_loss: use SMA for initial period
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        
        # Apply Wilder's Smoothed Moving Average for remaining periods
        # Formula: SMMA = (prev_SMMA * (period - 1) + current_value) / period
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi_value = 100 - (100 / (1 + rs))
        
        return rsi_value
    
    @staticmethod
    def macd(prices: List[float], ema_12_prev: Optional[float] = None, 
             ema_26_prev: Optional[float] = None, 
             signal_prev: Optional[float] = None) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """
        Calculate MACD (Moving Average Convergence Divergence).
        
        Args:
            prices: List of closing prices
            ema_12_prev: Previous EMA(12) value
            ema_26_prev: Previous EMA(26) value
            signal_prev: Previous signal line value
            
        Returns:
            Tuple of (MACD line, signal line, histogram) or (None, None, None)
        """
        if len(prices) < 26:
            return None, None, None
        
        ema12 = IndicatorCalculator.ema(prices, 12, ema_12_prev)
        ema26 = IndicatorCalculator.ema(prices, 26, ema_26_prev)
        
        if ema12 is None or ema26 is None:
            return None, None, None
        
        macd_line = ema12 - ema26
        
        # For signal line, we need to track MACD history
        # Simplified: use EMA(9) of MACD if we have previous signal
        if signal_prev is not None:
            k = 2 / (9 + 1)
            signal_line = (macd_line * k) + (signal_prev * (1 - k))
        else:
            signal_line = macd_line  # Initialize with MACD value
        
        histogram = macd_line - signal_line
        
        return macd_line, signal_line, histogram
    
    @staticmethod
    def bollinger_bands(prices: List[float], period: int = 20) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """
        Calculate Bollinger Bands.
        
        Args:
            prices: List of closing prices
            period: Number of periods (default 20)
            
        Returns:
            Tuple of (middle band, upper band, lower band) or (None, None, None)
        """
        if len(prices) < period:
            return None, None, None
        
        recent_prices = prices[-period:]
        sma = sum(recent_prices) / period
        
        variance = sum((p - sma) ** 2 for p in recent_prices) / period
        stddev = variance ** 0.5
        
        middle = sma
        upper = sma + (2 * stddev)
        lower = sma - (2 * stddev)
        
        return middle, upper, lower
    
    @staticmethod
    def atr(highs: List[float], lows: List[float], closes: List[float], 
            period: int = 14, prev_atr: Optional[float] = None) -> Optional[float]:
        """
        Calculate Average True Range.
        
        Args:
            highs: List of high prices
            lows: List of low prices
            closes: List of closing prices
            period: Number of periods (default 14)
            prev_atr: Previous ATR value for EMA calculation
            
        Returns:
            ATR value or None if insufficient data
        """
        if len(highs) < period + 1 or len(lows) < period + 1 or len(closes) < period + 1:
            return None
        
        true_ranges = []
        for i in range(1, len(highs)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
            true_ranges.append(tr)
        
        if prev_atr is None:
            # Initialize with simple average
            return sum(true_ranges[-period:]) / period
        else:
            # Use EMA smoothing
            k = 1 / period
            return (true_ranges[-1] * k) + (prev_atr * (1 - k))


class TechnicalIndicatorsJob:
    """
    Spark Structured Streaming job for calculating technical indicators.
    
    Reads candles from Kafka, maintains stateful storage per symbol,
    computes indicators, and writes to multiple sinks.
    
    This is a self-contained job with inline:
    - SparkSession creation
    - Logging setup
    - Batch metrics logging
    - Memory monitoring
    """
    
    def __init__(self, config: Config):
        """
        Initialize Technical Indicators Job.
        
        Args:
            config: Configuration object with Kafka, Spark, Redis, DuckDB settings
        """
        self.config = config
        self.logger = self._setup_logging()
        self.spark: Optional[SparkSession] = None
        self.query: Optional[any] = None
        self.shutdown_requested = False
        self.storage_writer: Optional[StorageWriter] = None
        
        # Micro-batch auto-stop attributes
        self.empty_batch_count: int = 0
        self.empty_batch_threshold: int = 2  # Stop quickly when no new data
        self.max_runtime_seconds: int = 60  # 1 minute per job
        self.start_time: Optional[float] = None
        
        # Initialize GracefulShutdown for controlled shutdown behavior
        self.graceful_shutdown = GracefulShutdown(
            graceful_shutdown_timeout=30,
            shutdown_progress_interval=5,
            logger=self.logger
        )
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _setup_logging(self) -> logging.Logger:
        """
        Set up structured logging for this job.
        
        Returns:
            Configured logger instance
        """
        job_name = "TechnicalIndicatorsJob"
        numeric_level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        
        # Create handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(numeric_level)
        
        # Set formatter
        formatter = StructuredFormatter(job_name)
        handler.setFormatter(formatter)
        
        # Get logger for this module
        logger = logging.getLogger(__name__)
        logger.setLevel(numeric_level)
        logger.handlers.clear()
        logger.addHandler(handler)
        
        return logger
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals with graceful shutdown support."""
        # Delegate to GracefulShutdown for proper shutdown handling
        self.graceful_shutdown.request_shutdown(signum)
        self.shutdown_requested = True
        
        # Stop the query to trigger shutdown sequence
        # The actual termination will wait for batch completion in run()
        if self.query:
            self.query.stop()
    
    def should_stop(self, is_empty_batch: bool) -> bool:
        """
        Check if job should stop based on empty batch count or timeout.
        
        Args:
            is_empty_batch: Whether the current batch is empty
            
        Returns:
            True if job should stop, False otherwise
        """
        # Check timeout
        if self.start_time and (time.time() - self.start_time) > self.max_runtime_seconds:
            self.logger.info(f"Max runtime {self.max_runtime_seconds}s exceeded, stopping")
            return True
        
        # Check empty batches
        if is_empty_batch:
            self.empty_batch_count += 1
            self.logger.info(f"Empty batch detected, count: {self.empty_batch_count}/{self.empty_batch_threshold}")
            if self.empty_batch_count >= self.empty_batch_threshold:
                self.logger.info(f"{self.empty_batch_threshold} consecutive empty batches, stopping")
                return True
        else:
            # Reset counter when data is received
            if self.empty_batch_count > 0:
                self.logger.info(f"Non-empty batch received, resetting empty batch counter from {self.empty_batch_count} to 0")
            self.empty_batch_count = 0
        
        return False
    
    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure SparkSession with resource limits and state store.
        
        Memory config: executor=512m, driver=512m (Spark minimum ~450MB)
        
        Returns:
            Configured SparkSession
        """
        # Spark requires minimum ~450MB for driver memory
        executor_memory = "512m"
        driver_memory = "512m"
        
        self.logger.info("Creating SparkSession with configuration:")
        self.logger.info(f"  Executor memory: {executor_memory}")
        self.logger.info(f"  Driver memory: {driver_memory}")
        self.logger.info(f"  Executor cores: {self.config.spark.executor_cores}")
        self.logger.info(f"  Shuffle partitions: {self.config.spark.shuffle_partitions}")
        
        spark = (SparkSession.builder
                 .appName(self.config.spark.app_name)
                 .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
                 .config("spark.sql.caseSensitive", "true")  # Enable case-sensitive field names
                 .config("spark.executor.memory", executor_memory)
                 .config("spark.driver.memory", driver_memory)
                 .config("spark.executor.cores", str(self.config.spark.executor_cores))
                 .config("spark.sql.shuffle.partitions", str(self.config.spark.shuffle_partitions))
                 .config("spark.streaming.kafka.maxRatePerPartition", 
                        str(self.config.kafka.max_rate_per_partition))
                 .config("spark.streaming.backpressure.enabled", 
                        str(self.config.spark.backpressure_enabled).lower())
                 .config("spark.sql.streaming.checkpointLocation", 
                        self.config.spark.checkpoint_location)
                 .config("spark.sql.streaming.stateStore.providerClass",
                        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
                 # State store reliability settings
                 .config("spark.sql.streaming.minBatchesToRetain", 
                        str(self.config.spark.state_store_min_batches_to_retain))
                 .config("spark.sql.streaming.stateStore.maintenanceInterval",
                        self.config.spark.state_store_maintenance_interval)
                 .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
                 .getOrCreate())
        
        # Set log level
        spark.sparkContext.setLogLevel(self.config.log_level)
        
        self.logger.info(f"SparkSession created: {spark.sparkContext.applicationId}")
        self.logger.info(f"Checkpoint location: {self.config.spark.checkpoint_location}")
        self.logger.info("State store provider: HDFSBackedStateStoreProvider")
        
        return spark
    
    def _get_jvm_memory_metrics(self) -> Dict[str, Any]:
        """
        Get JVM memory metrics from Spark.
        
        Returns:
            Dictionary with memory metrics in MB
        """
        try:
            from pyspark import SparkContext
            
            sc = SparkContext.getOrCreate()
            jvm = sc._jvm
            runtime = jvm.java.lang.Runtime.getRuntime()
            
            max_memory = runtime.maxMemory() / (1024 * 1024)
            total_memory = runtime.totalMemory() / (1024 * 1024)
            free_memory = runtime.freeMemory() / (1024 * 1024)
            used_memory = total_memory - free_memory
            
            usage_pct = (used_memory / max_memory) * 100 if max_memory > 0 else 0
            
            return {
                "heap_used_mb": round(used_memory, 2),
                "heap_max_mb": round(max_memory, 2),
                "heap_usage_pct": round(usage_pct, 2),
                "heap_free_mb": round(free_memory, 2),
                "heap_total_mb": round(total_memory, 2),
            }
        except Exception as e:
            return {
                "heap_used_mb": 0,
                "heap_max_mb": 0,
                "heap_usage_pct": 0,
                "heap_free_mb": 0,
                "heap_total_mb": 0,
                "error": str(e)
            }
    
    def _log_memory_metrics(self, batch_id: Optional[int] = None, alert_threshold_pct: float = 80.0) -> None:
        """
        Log JVM memory metrics and alert if usage is high.
        
        Args:
            batch_id: Optional batch identifier
            alert_threshold_pct: Percentage threshold for high memory alert
        """
        metrics = self._get_jvm_memory_metrics()
        
        if "error" in metrics:
            self.logger.warning(f"Failed to get memory metrics: {metrics['error']}")
            return
        
        batch_str = f"Batch {batch_id}: " if batch_id is not None else ""
        
        self.logger.info(
            f"{batch_str}Memory usage: "
            f"{metrics['heap_used_mb']:.0f}MB / {metrics['heap_max_mb']:.0f}MB "
            f"({metrics['heap_usage_pct']:.1f}%), "
            f"free={metrics['heap_free_mb']:.0f}MB"
        )
        
        if metrics['heap_usage_pct'] >= alert_threshold_pct:
            self.logger.warning(
                f"{batch_str}HIGH MEMORY USAGE ALERT: "
                f"{metrics['heap_usage_pct']:.1f}% "
                f"(threshold: {alert_threshold_pct}%) - "
                f"Used: {metrics['heap_used_mb']:.0f}MB / {metrics['heap_max_mb']:.0f}MB"
            )
    
    def _log_batch_metrics(
        self,
        batch_id: int,
        duration_seconds: float,
        record_count: int,
        watermark: Optional[str] = None
    ) -> None:
        """
        Log batch processing metrics.
        
        Args:
            batch_id: Batch identifier
            duration_seconds: Processing duration in seconds
            record_count: Number of records processed
            watermark: Current watermark timestamp
        """
        watermark_str = f", watermark={watermark}" if watermark else ""
        self.logger.info(
            f"Batch {batch_id} completed: "
            f"duration={duration_seconds:.2f}s, "
            f"records={record_count}{watermark_str}"
        )
    
    def _log_processing_metrics(
        self,
        batch_id: int,
        checkpoint_location: str,
        processing_time_seconds: float,
        record_count: int,
        watermark: Optional[str] = None
    ) -> None:
        """
        Log comprehensive processing metrics including checkpoint location.
        
        Args:
            batch_id: Batch identifier
            checkpoint_location: Path to checkpoint directory
            processing_time_seconds: Processing duration in seconds
            record_count: Number of records processed
            watermark: Current watermark timestamp
        """
        watermark_str = f", watermark={watermark}" if watermark else ""
        
        self.logger.info(
            f"Processing metrics - Batch {batch_id}: "
            f"checkpoint={checkpoint_location}, "
            f"processing_time={processing_time_seconds:.2f}s, "
            f"record_count={record_count}{watermark_str}"
        )

    def _init_storage_writer(self) -> StorageWriter:
        """
        Initialize StorageWriter with all 3 storage tiers.
        
        Uses PostgresStorage for warm path (concurrent write support)
        and MinioStorage for cold path (S3-compatible object storage).
        
        Returns:
            Configured StorageWriter instance
        """
        self.logger.info("Initializing StorageWriter with 3-tier storage (Redis, PostgreSQL, MinIO)")
        
        # Initialize Redis storage (hot path)
        redis_storage = RedisStorage(
            host=self.config.redis.host,
            port=self.config.redis.port,
            db=self.config.redis.db
        )
        
        # Initialize PostgreSQL storage (warm path - supports concurrent writes)
        postgres_storage = PostgresStorage(
            host=self.config.postgres.host,
            port=self.config.postgres.port,
            user=self.config.postgres.user,
            password=self.config.postgres.password,
            database=self.config.postgres.database,
            max_retries=self.config.postgres.max_retries,
            retry_delay=self.config.postgres.retry_delay
        )
        
        # Initialize MinIO storage (cold path - S3-compatible object storage)
        minio_storage = MinioStorage(
            endpoint=self.config.minio.endpoint,
            access_key=self.config.minio.access_key,
            secret_key=self.config.minio.secret_key,
            bucket=self.config.minio.bucket,
            secure=self.config.minio.secure,
            max_retries=self.config.minio.max_retries
        )
        
        storage_writer = StorageWriter(
            redis=redis_storage,
            postgres=postgres_storage,
            minio=minio_storage
        )
        
        self.logger.info("StorageWriter initialized successfully with PostgreSQL and MinIO")
        return storage_writer

    def create_stream_reader(self) -> DataFrame:
        """
        Create Kafka stream reader for processed_aggregations topic.
        Filters for 1-minute window candles only.
        
        Returns:
            DataFrame with parsed candle data
        """
        self.logger.info(f"Creating stream reader for topic: {self.config.kafka.topic_processed_aggregations}")
        self.logger.info(f"Kafka bootstrap servers: {self.config.kafka.bootstrap_servers}")
        
        try:
            # Read from Kafka
            # Use "earliest" to read candles written by TradeAggregationJob in the same DAG run
            # Checkpoint will track actual progress, so we won't reprocess old data
            df = (self.spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers)
                  .option("subscribe", self.config.kafka.topic_processed_aggregations)
                  .option("startingOffsets", "earliest")
                  .option("maxOffsetsPerTrigger", 
                         str(self.config.kafka.max_rate_per_partition * 10))
                  .load())
            
            # Define candle schema
            candle_schema = StructType([
                StructField("window_start", TimestampType(), False),
                StructField("window_end", TimestampType(), False),
                StructField("window_duration", StringType(), False),
                StructField("symbol", StringType(), False),
                StructField("open", DoubleType(), False),
                StructField("high", DoubleType(), False),
                StructField("low", DoubleType(), False),
                StructField("close", DoubleType(), False),
                StructField("volume", DoubleType(), False),
                StructField("quote_volume", DoubleType(), False),
                StructField("trade_count", LongType(), False),
                StructField("vwap", DoubleType(), True),
                StructField("price_change_pct", DoubleType(), True),
                StructField("buy_sell_ratio", DoubleType(), True),
                StructField("large_order_count", LongType(), True),
                StructField("price_stddev", DoubleType(), True)
            ])
            
            # Parse JSON and extract fields
            parsed_df = df.select(
                from_json(col("value").cast("string"), candle_schema).alias("candle")
            ).select("candle.*")
            
            # Filter for 1-minute windows only
            filtered_df = parsed_df.filter(col("window_duration") == "1m")
            
            # Add watermark for late event handling (1 minute)
            watermarked_df = filtered_df.withWatermark("window_start", "1 minute")
            
            self.logger.info("Stream reader created successfully, filtering for 1m windows with 1 minute watermark")
            return watermarked_df
            
        except Exception as e:
            self.logger.error(f"Failed to create stream reader: {str(e)}", 
                            extra={"topic": self.config.kafka.topic_processed_aggregations,
                                   "error": str(e)})
            raise
    
    def _load_historical_candles(self) -> None:
        """
        Load historical candles from PostgreSQL to bootstrap indicator state.
        
        This ensures we have enough data (at least 26 candles for MACD, 15 for RSI)
        to calculate indicators immediately when the job starts.
        
        Loads the last 50 candles per symbol from the warm storage tier.
        """
        from datetime import timedelta
        
        self.logger.info("Loading historical candles from PostgreSQL to bootstrap state...")
        
        try:
            # Get PostgreSQL storage from StorageWriter
            if self.storage_writer is None or self.storage_writer.postgres is None:
                self.logger.warning("PostgreSQL storage not available, skipping historical load")
                return
            
            postgres = self.storage_writer.postgres
            
            # Query last 24 hours of candles for common symbols
            # Using 24 hours to handle timezone differences and data gaps
            symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT']
            now = datetime.now()
            start = now - timedelta(hours=24)
            
            total_loaded = 0
            
            for symbol in symbols:
                try:
                    candles = postgres.query_candles(symbol, start, now, interval='1m')
                    
                    if candles:
                        # Initialize state for symbol
                        if symbol not in self.candle_states:
                            self.candle_states[symbol] = CandleState(symbol=symbol)
                        
                        state = self.candle_states[symbol]
                        
                        # Add candles to state (sorted by timestamp)
                        sorted_candles = sorted(candles, key=lambda x: x.get('timestamp', datetime.min))
                        
                        for candle in sorted_candles[-50:]:  # Keep last 50 candles
                            candle_dict = {
                                'timestamp': candle.get('timestamp'),
                                'open': float(candle.get('open', 0)),
                                'high': float(candle.get('high', 0)),
                                'low': float(candle.get('low', 0)),
                                'close': float(candle.get('close', 0)),
                                'volume': float(candle.get('volume', 0))
                            }
                            state.add_candle(candle_dict)
                        
                        total_loaded += len(state.candles)
                        self.logger.info(
                            f"Loaded {len(state.candles)} historical candles for {symbol}"
                        )
                        
                except Exception as e:
                    self.logger.warning(f"Failed to load historical candles for {symbol}: {e}")
            
            self.logger.info(
                f"Historical candle loading complete: {total_loaded} candles "
                f"across {len(self.candle_states)} symbols"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to load historical candles: {e}")
    
    def process_with_state(self, df: DataFrame) -> callable:
        """
        Process candles with stateful storage using foreachBatch.
        Maintains up to 200 candles per symbol and calculates indicators.
        
        Args:
            df: DataFrame with candle data
            
        Returns:
            Batch processing function
        """
        self.logger.info("Setting up stateful candle processing")
        
        # We'll use foreachBatch to maintain state in memory
        # This is simpler than mapGroupsWithState for this use case
        self.candle_states: Dict[str, CandleState] = {}
        
        # Load historical candles from PostgreSQL to bootstrap state
        # This ensures we have enough data to calculate indicators immediately
        self._load_historical_candles()
        
        def process_batch(batch_df: DataFrame, batch_id: int):
            """Process each batch and maintain state."""
            try:
                # Check if batch should be skipped due to shutdown request (Requirement 5.2)
                if self.graceful_shutdown.should_skip_batch():
                    self.logger.info(f"Batch {batch_id}: Skipping due to shutdown request")
                    return
                
                start_time = time.time()
                
                # Log memory usage at start of batch (inline)
                self._log_memory_metrics(batch_id=batch_id, alert_threshold_pct=80.0)
                
                # Check if batch is empty and determine if we should stop
                is_empty = batch_df.isEmpty()
                
                if self.should_stop(is_empty):
                    self.logger.info(f"Batch {batch_id}: Stopping query due to auto-stop condition")
                    if self.query:
                        self.query.stop()
                    return
                
                if is_empty:
                    self.logger.debug(f"Batch {batch_id} is empty, skipping")
                    return
                
                # Mark batch as started for graceful shutdown tracking (Requirement 5.1)
                self.graceful_shutdown.mark_batch_start(batch_id)
                
                self.logger.info(f"Processing batch {batch_id}")
                
                # Collect candles from batch
                candles = batch_df.collect()
                
                results = []
                
                for candle_row in candles:
                    symbol = candle_row.symbol
                    
                    # Initialize state for symbol if not exists
                    if symbol not in self.candle_states:
                        self.candle_states[symbol] = CandleState(symbol=symbol)
                    
                    state = self.candle_states[symbol]
                    
                    # Add candle to state (automatically limits to 200)
                    candle_dict = {
                        'timestamp': candle_row.window_start,
                        'open': float(candle_row.open),
                        'high': float(candle_row.high),
                        'low': float(candle_row.low),
                        'close': float(candle_row.close),
                        'volume': float(candle_row.volume)
                    }
                    state.add_candle(candle_dict)
                    
                    # Calculate indicators
                    indicators = self._calculate_indicators(state)
                    
                    # Create result row
                    result = {
                        'timestamp': candle_row.window_start,
                        'symbol': symbol,
                        **indicators
                    }
                    results.append(result)
                
                # Write results to sinks
                if results:
                    self._write_indicators_to_sinks(results, batch_id)
                
                # Emit state size metrics
                total_state_size = sum(len(state.candles) for state in self.candle_states.values())
                
                # Log batch metrics (inline)
                duration = time.time() - start_time
                watermark = str(results[0].get('timestamp')) if results else None
                
                self._log_batch_metrics(batch_id, duration, len(results), watermark)
                
                self.logger.info(f"Batch {batch_id} processed: {len(results)} indicators calculated, "
                               f"total state size: {total_state_size} candles across {len(self.candle_states)} symbols")
                
                # Mark batch as completed for graceful shutdown tracking (Requirement 5.1)
                self.graceful_shutdown.mark_batch_end(batch_id)
                
            except Exception as e:
                self.logger.error(f"Error processing batch {batch_id}: {str(e)}", 
                                extra={"batch_id": batch_id, "error": str(e)},
                                exc_info=True)
                # Ensure batch is marked as ended even on error (Requirement 5.3)
                self.graceful_shutdown.mark_batch_end(batch_id)
        
        return process_batch
    
    def _calculate_indicators(self, state: CandleState) -> Dict[str, Optional[float]]:
        """
        Calculate all technical indicators from candle state.
        
        Args:
            state: CandleState with candle history
            
        Returns:
            Dictionary with indicator values
        """
        candles = list(state.candles)
        
        if not candles:
            return self._empty_indicators()
        
        # Extract price lists
        closes = [c['close'] for c in candles]
        highs = [c['high'] for c in candles]
        lows = [c['low'] for c in candles]
        
        # Calculate indicators
        indicators = {
            'sma_5': IndicatorCalculator.sma(closes, 5),
            'sma_10': IndicatorCalculator.sma(closes, 10),
            'sma_20': IndicatorCalculator.sma(closes, 20),
            'sma_50': IndicatorCalculator.sma(closes, 50),
            'ema_12': IndicatorCalculator.ema(closes, 12),
            'ema_26': IndicatorCalculator.ema(closes, 26),
            'rsi_14': IndicatorCalculator.rsi(closes, 14),
        }
        
        # MACD
        macd_line, signal_line, histogram = IndicatorCalculator.macd(closes)
        indicators['macd_line'] = macd_line
        indicators['macd_signal'] = signal_line
        indicators['macd_histogram'] = histogram
        
        # Bollinger Bands
        bb_middle, bb_upper, bb_lower = IndicatorCalculator.bollinger_bands(closes, 20)
        indicators['bb_middle'] = bb_middle
        indicators['bb_upper'] = bb_upper
        indicators['bb_lower'] = bb_lower
        
        # ATR
        indicators['atr_14'] = IndicatorCalculator.atr(highs, lows, closes, 14)
        
        return indicators
    
    def _empty_indicators(self) -> Dict[str, Optional[float]]:
        """Return empty indicators dictionary."""
        return {
            'sma_5': None,
            'sma_10': None,
            'sma_20': None,
            'sma_50': None,
            'ema_12': None,
            'ema_26': None,
            'rsi_14': None,
            'macd_line': None,
            'macd_signal': None,
            'macd_histogram': None,
            'bb_middle': None,
            'bb_upper': None,
            'bb_lower': None,
            'atr_14': None
        }


    def _write_indicators_to_sinks(self, indicators: List[Dict[str, Any]], batch_id: int) -> None:
        """
        Write indicators to multiple sinks using StorageWriter for 3-tier storage.
        
        Writes to:
        - Kafka (for downstream consumers like Anomaly Detection job)
        - Redis, DuckDB, Parquet (via StorageWriter for 3-tier storage)
        
        Args:
            indicators: List of indicator dictionaries
            batch_id: Batch identifier
        """
        start_time = time.time()
        
        self.logger.info(f"Batch {batch_id}: Writing {len(indicators)} indicators to sinks")
        
        # Write to Kafka (for downstream consumers like Anomaly Detection job)
        try:
            from kafka import KafkaProducer
            
            producer = KafkaProducer(
                bootstrap_servers=self.config.kafka.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                compression_type=self.config.kafka.compression_type,
                acks=self.config.kafka.acks
            )
            
            for indicator in indicators:
                # Convert timestamp to string for JSON serialization
                indicator_copy = indicator.copy()
                indicator_copy['timestamp'] = str(indicator_copy['timestamp'])
                
                producer.send(
                    self.config.kafka.topic_processed_indicators,
                    key=indicator['symbol'],
                    value=indicator_copy
                )
            
            producer.flush()
            producer.close()
            
            self.logger.debug(f"Batch {batch_id}: Wrote to Kafka topic {self.config.kafka.topic_processed_indicators}")
        except Exception as e:
            self.logger.error(f"Batch {batch_id}: Failed to write to Kafka: {str(e)}", 
                            extra={"error": str(e)})
        
        # Write to 3-tier storage using StorageWriter (Redis, DuckDB, Parquet)
        # Per Requirements 5.2: Redis with overwrite, DuckDB with upsert, Parquet with append
        success_count = 0
        failure_count = 0
        
        for indicator in indicators:
            try:
                # Convert indicator to storage format
                indicator_data = {
                    'timestamp': indicator.get('timestamp'),
                    'symbol': indicator.get('symbol'),
                    'rsi': indicator.get('rsi_14'),
                    'macd': indicator.get('macd_line'),
                    'macd_signal': indicator.get('macd_signal'),
                    'sma_20': indicator.get('sma_20'),
                    'ema_12': indicator.get('ema_12'),
                    'ema_26': indicator.get('ema_26'),
                    'bb_upper': indicator.get('bb_upper'),
                    'bb_lower': indicator.get('bb_lower'),
                    'atr': indicator.get('atr_14'),
                }
                
                # Write to all 3 tiers via StorageWriter
                results = self.storage_writer.write_indicators(indicator_data)
                
                if all(results.values()):
                    success_count += 1
                else:
                    failure_count += 1
                    failed_tiers = [k for k, v in results.items() if not v]
                    self.logger.warning(
                        f"Batch {batch_id}: Partial write failure for {indicator.get('symbol')} - "
                        f"failed tiers: {failed_tiers}"
                    )
                    
            except Exception as e:
                failure_count += 1
                self.logger.error(f"Batch {batch_id}: Failed to write indicator for {indicator.get('symbol')}: {str(e)}")
        
        self.logger.info(
            f"Batch {batch_id}: StorageWriter completed - "
            f"{success_count} succeeded, {failure_count} failed"
        )
        
        # Log comprehensive processing metrics (inline)
        duration = time.time() - start_time
        
        # Get watermark from indicators if available
        watermark = None
        if indicators:
            watermark = str(indicators[0].get('timestamp'))
        
        self._log_processing_metrics(
            batch_id,
            self.config.spark.checkpoint_location,
            duration,
            len(indicators),
            watermark
        )
    
    def run(self) -> None:
        """
        Run the Technical Indicators streaming job.
        
        Creates Spark session, initializes StorageWriter for 3-tier storage,
        reads from Kafka, processes indicators, and writes to multiple sinks.
        
        The job will automatically stop when:
        - max_runtime_seconds is exceeded (default 5 minutes)
        - empty_batch_threshold consecutive empty batches are received (default 3)
        """
        try:
            # Create Spark session
            self.spark = self._create_spark_session()
            
            # Initialize StorageWriter for 3-tier storage (Redis, DuckDB, Parquet)
            self.storage_writer = self._init_storage_writer()
            
            # Create stream reader
            candles_stream = self.create_stream_reader()
            
            # Set up stateful processing
            process_batch_func = self.process_with_state(candles_stream)
            
            # Track start time for timeout-based auto-stop
            self.start_time = time.time()
            
            self.logger.info("Technical Indicators Job started successfully (micro-batch mode)")
            self.logger.info(f"Reading from topic: {self.config.kafka.topic_processed_aggregations}")
            self.logger.info("Writing to 3-tier storage: Redis (hot), DuckDB (warm), Parquet (cold)")
            self.logger.info("Starting stateful processing with 200 candle limit per symbol")
            self.logger.info(f"Auto-stop config: max_runtime={self.max_runtime_seconds}s, empty_batch_threshold={self.empty_batch_threshold}")
            self.logger.info(f"Graceful shutdown timeout: {self.graceful_shutdown.graceful_shutdown_timeout}s")
            
            # Start streaming query with foreachBatch
            # Checkpoint interval is set to 1 minute for state persistence
            query = (candles_stream
                    .writeStream
                    .foreachBatch(process_batch_func)
                    .outputMode("update")
                    .option("checkpointLocation", self.config.spark.checkpoint_location)
                    .trigger(processingTime='1 minute')  # Checkpoint interval
                    .start())
            
            self.query = query
            
            # Use awaitTermination with timeout for micro-batch processing
            # The query will also stop if should_stop() returns True in process_batch
            query.awaitTermination(timeout=self.max_runtime_seconds)
            
            # Wait for any in-progress batch to complete (Requirement 1.3, 1.4)
            was_graceful = self.graceful_shutdown.wait_for_batch_completion()
            
            # Log whether shutdown was graceful or forced (Requirement 3.3)
            if self.graceful_shutdown.shutdown_requested:
                if was_graceful:
                    self.logger.info("Technical Indicators Job shutdown completed gracefully")
                else:
                    self.logger.warning("Technical Indicators Job shutdown was forced due to timeout")
            else:
                self.logger.info("Technical Indicators Job completed successfully")
            
        except Exception as e:
            self.logger.error(f"Job failed with error: {str(e)}", 
                            extra={"error": str(e)}, 
                            exc_info=True)
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Clean up resources on shutdown."""
        self.logger.info("Starting cleanup...")
        
        if self.query and self.query.isActive:
            self.logger.info("Stopping streaming query and writing final checkpoint...")
            self.query.stop()
            self.logger.info("Streaming query stopped, checkpoint written")
        
        # Log final state statistics
        if hasattr(self, 'candle_states'):
            total_candles = sum(len(state.candles) for state in self.candle_states.values())
            self.logger.info(f"Final state: {len(self.candle_states)} symbols, {total_candles} total candles")
        
        if self.spark:
            self.logger.info("Stopping SparkSession...")
            self.spark.stop()
        
        self.logger.info("Cleanup completed. Shutdown successful.")


def main():
    """Main entry point for Technical Indicators Job."""
    config = Config.from_env("TechnicalIndicatorsJob")
    job = TechnicalIndicatorsJob(config)
    job.run()


if __name__ == "__main__":
    main()
