"""
Anomaly Detection Job (Consolidated)

Detects all 6 anomaly types from multiple data sources:
- Whale alerts (raw_trades): Trade value > $100,000
- Volume spikes (aggregations): Volume > 3x average(20 periods)
- Price spikes (aggregations): Price change > 2% in 1 minute
- RSI extremes (indicators): RSI > 70 or RSI < 30
- BB breakouts (indicators + aggregations): Price outside Bollinger Bands
- MACD crossovers (indicators): MACD crosses signal line

This is a self-contained job with inline logging, metrics, and memory monitoring.
"""

import json
import logging
import signal
import sys
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, lit, struct, current_timestamp, when, expr,
    to_json, abs as spark_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, BooleanType, TimestampType
)

from .config import Config
from .connectors import RedisConnector, DuckDBConnector, KafkaConnector
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



class AnomalyDetectionJob:
    """
    Consolidated Spark Structured Streaming job for detecting all anomaly types.

    Monitors multiple data sources and generates alerts for:
    - Whale alerts: Trade value > $100,000 (HIGH)
    - Volume spikes: Volume > 3x average (MEDIUM)
    - Price spikes: Price change > 2% (HIGH)
    - RSI extremes: RSI > 70 or < 30 (LOW)
    - BB breakouts: Price outside bands (MEDIUM)
    - MACD crossovers: MACD crosses signal (LOW)

    This is a self-contained job with inline:
    - SparkSession creation (512m memory)
    - Logging setup
    - Batch metrics logging
    - Memory monitoring
    """

    # Thresholds per design spec
    WHALE_THRESHOLD = 100000.0  # $100k
    VOLUME_SPIKE_MULTIPLIER = 3.0
    VOLUME_LOOKBACK_PERIODS = 20
    PRICE_SPIKE_THRESHOLD = 2.0  # 2%
    RSI_OVERBOUGHT = 70.0
    RSI_OVERSOLD = 30.0

    def __init__(self, config: Config):
        """
        Initialize Anomaly Detection Job.

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
        job_name = "AnomalyDetectionJob"
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
        Create and configure SparkSession with resource limits.

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

    # Schema definitions
    @staticmethod
    def _get_trade_schema() -> StructType:
        """Get schema for raw trade messages from Binance connector."""
        original_data_schema = StructType([
            StructField("E", LongType(), False),  # Event time
            StructField("s", StringType(), False),  # Symbol
            StructField("p", StringType(), False),  # Price
            StructField("q", StringType(), False),  # Quantity
            StructField("m", BooleanType(), False),  # Is buyer maker
            StructField("t", LongType(), True)  # Trade ID
        ])

        return StructType([
            StructField("original_data", original_data_schema, False),
            StructField("symbol", StringType(), False),
            StructField("ingestion_timestamp", LongType(), False)
        ])

    @staticmethod
    def _get_aggregation_schema() -> StructType:
        """Get schema for aggregation messages."""
        return StructType([
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

    @staticmethod
    def _get_indicator_schema() -> StructType:
        """Get schema for indicator messages."""
        return StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("symbol", StringType(), False),
            StructField("sma_5", DoubleType(), True),
            StructField("sma_10", DoubleType(), True),
            StructField("sma_20", DoubleType(), True),
            StructField("sma_50", DoubleType(), True),
            StructField("ema_12", DoubleType(), True),
            StructField("ema_26", DoubleType(), True),
            StructField("rsi_14", DoubleType(), True),
            StructField("macd_line", DoubleType(), True),
            StructField("macd_signal", DoubleType(), True),
            StructField("macd_histogram", DoubleType(), True),
            StructField("bb_middle", DoubleType(), True),
            StructField("bb_upper", DoubleType(), True),
            StructField("bb_lower", DoubleType(), True),
            StructField("atr_14", DoubleType(), True)
        ])

    def detect_whale_alerts(self, df: DataFrame) -> DataFrame:
        """
        Detect whale alerts from raw trades.

        Whale alert: Trade value > $100,000, level HIGH

        Args:
            df: Raw Kafka DataFrame from raw_trades topic

        Returns:
            DataFrame with whale alerts
        """
        self.logger.info(f"Detecting whale alerts with threshold: ${self.WHALE_THRESHOLD:,.2f}")

        trade_schema = self._get_trade_schema()

        # Parse JSON from Kafka value
        parsed_df = df.select(
            from_json(col("value").cast("string"), trade_schema).alias("trade")
        )

        # Extract fields and calculate trade value
        # Note: is_buyer_maker (m) = true means the buyer is the maker, so the trade is a SELL
        #       is_buyer_maker (m) = false means the seller is the maker, so the trade is a BUY
        trades_df = parsed_df.select(
            (col("trade.original_data.E") / 1000).cast(TimestampType()).alias("timestamp"),
            col("trade.symbol").alias("symbol"),
            col("trade.original_data.p").cast(DoubleType()).alias("price"),
            col("trade.original_data.q").cast(DoubleType()).alias("quantity"),
            when(col("trade.original_data.m") == True, lit("SELL"))
                .otherwise(lit("BUY")).alias("side"),
        ).withColumn(
            "value",
            col("price") * col("quantity")
        )

        # Add watermark for late event handling (1 minute)
        trades_df = trades_df.withWatermark("timestamp", "1 minute")

        # Filter for whale trades (value > $100,000)
        whale_trades = trades_df.filter(col("value") > self.WHALE_THRESHOLD)

        # Create alert structure with details as JSON string for UNION compatibility
        alerts_df = whale_trades.select(
            col("timestamp"),
            col("symbol"),
            lit("WHALE_ALERT").alias("alert_type"),
            lit("HIGH").alias("alert_level"),
            to_json(struct(
                col("price"),
                col("quantity"),
                col("value"),
                col("side")
            )).alias("details"),
            expr("uuid()").alias("alert_id"),
            current_timestamp().alias("created_at")
        )

        self.logger.info("Whale alert detection configured")
        return alerts_df


    def detect_volume_spikes(self, df: DataFrame) -> DataFrame:
        """
        Detect volume spikes from aggregations.

        Volume spike: Volume significantly higher than typical (using quote_volume threshold), level MEDIUM
        Note: Spark Structured Streaming doesn't support row-based window functions,
        so we use a fixed threshold approach based on quote_volume.

        Args:
            df: Raw Kafka DataFrame from processed_aggregations topic

        Returns:
            DataFrame with volume spike alerts
        """
        # Use a high quote_volume threshold to detect unusual activity
        # $1M quote volume in 5 minutes indicates significant trading activity
        QUOTE_VOLUME_THRESHOLD = 1000000.0  # $1M
        
        self.logger.info(
            f"Detecting volume spikes with quote_volume threshold: ${QUOTE_VOLUME_THRESHOLD:,.0f}"
        )

        agg_schema = self._get_aggregation_schema()

        # Parse JSON from Kafka value
        parsed_df = df.select(
            from_json(col("value").cast("string"), agg_schema).alias("agg")
        )

        # Extract fields and filter for 5-minute windows
        aggs_df = parsed_df.select(
            col("agg.window_start").alias("window_start"),
            col("agg.window_end").alias("window_end"),
            col("agg.window_duration").alias("window_duration"),
            col("agg.symbol").alias("symbol"),
            col("agg.volume").alias("volume"),
            col("agg.quote_volume").alias("quote_volume"),
            col("agg.trade_count").alias("trade_count")
        ).filter(col("window_duration") == "5m")

        # Add watermark for late event handling (1 minute)
        aggs_df = aggs_df.withWatermark("window_start", "1 minute")

        # Filter for volume spikes based on quote_volume threshold
        volume_spikes = aggs_df.filter(
            col("quote_volume") > QUOTE_VOLUME_THRESHOLD
        )

        # Create alert structure with details as JSON string for UNION compatibility
        alerts_df = volume_spikes.select(
            col("window_start").alias("timestamp"),
            col("symbol"),
            lit("VOLUME_SPIKE").alias("alert_type"),
            lit("MEDIUM").alias("alert_level"),
            to_json(struct(
                col("volume"),
                col("quote_volume"),
                col("trade_count")
            )).alias("details"),
            expr("uuid()").alias("alert_id"),
            current_timestamp().alias("created_at")
        )

        self.logger.info("Volume spike detection configured")
        return alerts_df

    def detect_price_spikes(self, df: DataFrame) -> DataFrame:
        """
        Detect price spikes from aggregations.

        Price spike: Price change > 2% in 1 minute, level HIGH

        Args:
            df: Raw Kafka DataFrame from processed_aggregations topic

        Returns:
            DataFrame with price spike alerts
        """
        self.logger.info(f"Detecting price spikes with threshold: {self.PRICE_SPIKE_THRESHOLD}%")

        agg_schema = self._get_aggregation_schema()

        # Parse JSON from Kafka value
        parsed_df = df.select(
            from_json(col("value").cast("string"), agg_schema).alias("agg")
        )

        # Extract fields and filter for 1-minute windows
        aggs_df = parsed_df.select(
            col("agg.window_start").alias("window_start"),
            col("agg.window_end").alias("window_end"),
            col("agg.window_duration").alias("window_duration"),
            col("agg.symbol").alias("symbol"),
            col("agg.open").alias("open"),
            col("agg.close").alias("close"),
            col("agg.price_change_pct").alias("price_change_pct")
        ).filter(col("window_duration") == "1m")

        # Add watermark for late event handling (1 minute)
        aggs_df = aggs_df.withWatermark("window_start", "1 minute")

        # Filter for price spikes (|price_change_pct| > 2%)
        price_spikes = aggs_df.filter(
            spark_abs(col("price_change_pct")) > self.PRICE_SPIKE_THRESHOLD
        )

        # Create alert structure with details as JSON string for UNION compatibility
        alerts_df = price_spikes.select(
            col("window_start").alias("timestamp"),
            col("symbol"),
            lit("PRICE_SPIKE").alias("alert_type"),
            lit("HIGH").alias("alert_level"),
            to_json(struct(
                col("open"),
                col("close"),
                col("price_change_pct")
            )).alias("details"),
            expr("uuid()").alias("alert_id"),
            current_timestamp().alias("created_at")
        )

        self.logger.info("Price spike detection configured")
        return alerts_df


    def detect_rsi_extremes(self, df: DataFrame) -> DataFrame:
        """
        Detect RSI extreme conditions from indicators.

        RSI extreme: RSI > 70 (overbought) or RSI < 30 (oversold), level LOW

        Args:
            df: Raw Kafka DataFrame from processed_indicators topic

        Returns:
            DataFrame with RSI extreme alerts
        """
        self.logger.info(
            f"Detecting RSI extremes: overbought > {self.RSI_OVERBOUGHT}, "
            f"oversold < {self.RSI_OVERSOLD}"
        )

        indicator_schema = self._get_indicator_schema()

        # Parse JSON from Kafka value
        parsed_df = df.select(
            from_json(col("value").cast("string"), indicator_schema).alias("ind")
        )

        # Extract fields
        indicators_df = parsed_df.select(
            col("ind.timestamp").alias("timestamp"),
            col("ind.symbol").alias("symbol"),
            col("ind.rsi_14").alias("rsi_14")
        )

        # Add watermark for late event handling (1 minute)
        indicators_df = indicators_df.withWatermark("timestamp", "1 minute")

        # Filter for RSI extremes
        rsi_extremes = indicators_df.filter(
            (col("rsi_14").isNotNull()) &
            ((col("rsi_14") > self.RSI_OVERBOUGHT) | (col("rsi_14") < self.RSI_OVERSOLD))
        )

        # Create alert structure with details as JSON string for UNION compatibility
        alerts_df = rsi_extremes.select(
            col("timestamp"),
            col("symbol"),
            lit("RSI_EXTREME").alias("alert_type"),
            lit("LOW").alias("alert_level"),
            to_json(struct(
                col("rsi_14"),
                when(col("rsi_14") > self.RSI_OVERBOUGHT, "OVERBOUGHT")
                    .otherwise("OVERSOLD").alias("condition")
            )).alias("details"),
            expr("uuid()").alias("alert_id"),
            current_timestamp().alias("created_at")
        )

        self.logger.info("RSI extreme detection configured")
        return alerts_df

    def detect_bb_breakouts(self, indicators_df: DataFrame, aggs_df: DataFrame) -> DataFrame:
        """
        Detect Bollinger Band breakouts from indicators and aggregations.

        BB breakout: Price > upper_band or < lower_band, level MEDIUM

        Args:
            indicators_df: Raw Kafka DataFrame from processed_indicators topic
            aggs_df: Raw Kafka DataFrame from processed_aggregations topic

        Returns:
            DataFrame with BB breakout alerts
        """
        self.logger.info("Detecting Bollinger Band breakouts")

        indicator_schema = self._get_indicator_schema()
        agg_schema = self._get_aggregation_schema()

        # Parse indicators
        parsed_indicators = indicators_df.select(
            from_json(col("value").cast("string"), indicator_schema).alias("ind")
        ).select(
            col("ind.timestamp").alias("timestamp"),
            col("ind.symbol").alias("symbol"),
            col("ind.bb_upper").alias("bb_upper"),
            col("ind.bb_lower").alias("bb_lower")
        )

        # Parse aggregations (1m windows for latest price)
        parsed_aggs = aggs_df.select(
            from_json(col("value").cast("string"), agg_schema).alias("agg")
        ).select(
            col("agg.window_start").alias("timestamp"),
            col("agg.symbol").alias("symbol"),
            col("agg.close").alias("price"),
            col("agg.window_duration").alias("window_duration")
        ).filter(col("window_duration") == "1m")

        # Join indicators with latest prices using watermark for stream-stream join
        indicators_with_watermark = parsed_indicators.withWatermark("timestamp", "1 minute")
        aggs_with_watermark = parsed_aggs.withWatermark("timestamp", "1 minute")

        joined_df = indicators_with_watermark.join(
            aggs_with_watermark,
            (indicators_with_watermark.symbol == aggs_with_watermark.symbol) &
            (indicators_with_watermark.timestamp == aggs_with_watermark.timestamp),
            "inner"
        ).select(
            indicators_with_watermark.timestamp,
            indicators_with_watermark.symbol,
            aggs_with_watermark.price,
            indicators_with_watermark.bb_upper,
            indicators_with_watermark.bb_lower
        )

        # Filter for breakouts
        breakouts = joined_df.filter(
            (col("bb_upper").isNotNull()) &
            (col("bb_lower").isNotNull()) &
            ((col("price") > col("bb_upper")) | (col("price") < col("bb_lower")))
        )

        # Create alert structure with details as JSON string for UNION compatibility
        alerts_df = breakouts.select(
            col("timestamp"),
            col("symbol"),
            lit("BB_BREAKOUT").alias("alert_type"),
            lit("MEDIUM").alias("alert_level"),
            to_json(struct(
                col("price"),
                col("bb_upper"),
                col("bb_lower"),
                when(col("price") > col("bb_upper"), "UPPER")
                    .otherwise("LOWER").alias("breakout_direction")
            )).alias("details"),
            expr("uuid()").alias("alert_id"),
            current_timestamp().alias("created_at")
        )

        self.logger.info("BB breakout detection configured")
        return alerts_df

    def detect_macd_crossovers(self, df: DataFrame) -> DataFrame:
        """
        Detect MACD crossovers from indicators.

        MACD crossover: Detected when histogram is very close to zero (recent crossover), level LOW
        Note: Spark Structured Streaming doesn't support lag() window function,
        so we detect crossovers by checking when histogram is near zero with significant MACD values.

        Args:
            df: Raw Kafka DataFrame from processed_indicators topic

        Returns:
            DataFrame with MACD crossover alerts
        """
        self.logger.info("Detecting MACD crossovers using histogram approach")

        indicator_schema = self._get_indicator_schema()

        # Parse JSON from Kafka value
        parsed_df = df.select(
            from_json(col("value").cast("string"), indicator_schema).alias("ind")
        )

        # Extract fields including histogram
        indicators_df = parsed_df.select(
            col("ind.timestamp").alias("timestamp"),
            col("ind.symbol").alias("symbol"),
            col("ind.macd_line").alias("macd_line"),
            col("ind.macd_signal").alias("macd_signal"),
            col("ind.macd_histogram").alias("macd_histogram")
        )

        # Add watermark for late event handling (1 minute)
        indicators_df = indicators_df.withWatermark("timestamp", "1 minute")

        # Detect crossovers by checking when histogram is very close to zero
        # This indicates a recent crossover. Threshold of 0.0001 catches recent crosses.
        HISTOGRAM_THRESHOLD = 0.0001
        
        crossovers = indicators_df.filter(
            (col("macd_line").isNotNull()) &
            (col("macd_signal").isNotNull()) &
            (col("macd_histogram").isNotNull()) &
            (spark_abs(col("macd_histogram")) < HISTOGRAM_THRESHOLD) &
            (spark_abs(col("macd_line")) > 0.001)  # Ensure meaningful MACD values
        )

        # Create alert structure with details as JSON string for UNION compatibility
        # Direction based on MACD line position relative to signal
        alerts_df = crossovers.select(
            col("timestamp"),
            col("symbol"),
            lit("MACD_CROSSOVER").alias("alert_type"),
            lit("LOW").alias("alert_level"),
            to_json(struct(
                col("macd_line"),
                col("macd_signal"),
                col("macd_histogram"),
                when(col("macd_line") > col("macd_signal"), "BULLISH")
                    .otherwise("BEARISH").alias("direction")
            )).alias("details"),
            expr("uuid()").alias("alert_id"),
            current_timestamp().alias("created_at")
        )

        self.logger.info("MACD crossover detection configured")
        return alerts_df


    def _create_alert(
        self,
        timestamp: datetime,
        symbol: str,
        alert_type: str,
        alert_level: str,
        details: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create an alert dictionary with all required fields.

        Args:
            timestamp: Event timestamp
            symbol: Trading symbol (e.g., "BTCUSDT")
            alert_type: Type of alert (WHALE_ALERT, VOLUME_SPIKE, etc.)
            alert_level: Severity level (HIGH, MEDIUM, LOW)
            details: Type-specific details dictionary

        Returns:
            Alert dictionary with UUID, timestamp, symbol, alert_type, alert_level, details
        """
        return {
            "alert_id": str(uuid.uuid4()),
            "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else str(timestamp),
            "symbol": symbol,
            "alert_type": alert_type,
            "alert_level": alert_level,
            "details": details,
            "created_at": datetime.utcnow().isoformat()
        }

    def _write_alerts_to_sinks(self, alerts: List[Dict[str, Any]], batch_id: int) -> None:
        """
        Write alerts to multiple sinks using StorageWriter for 3-tier storage.

        Writes to:
        - Kafka (for downstream consumers)
        - Redis, DuckDB, Parquet (via StorageWriter for 3-tier storage)

        Args:
            alerts: List of alert dictionaries
            batch_id: Batch identifier
        """
        if not alerts:
            self.logger.debug(f"Batch {batch_id}: No alerts to write")
            return

        self.logger.info(f"Batch {batch_id}: Writing {len(alerts)} alerts to sinks")

        # Write to Kafka (for downstream consumers)
        try:
            kafka_conn = KafkaConnector(
                bootstrap_servers=self.config.kafka.bootstrap_servers,
                client_id="anomaly_detection_job"
            )

            for alert in alerts:
                kafka_conn.send(
                    topic=self.config.kafka.topic_alerts,
                    value=alert,
                    key=alert["symbol"]
                )

            kafka_conn.close()
            self.logger.debug(f"Batch {batch_id}: Wrote {len(alerts)} alerts to Kafka")
        except Exception as e:
            self.logger.error(f"Batch {batch_id}: Failed to write to Kafka: {str(e)}")

        # Write to 3-tier storage using StorageWriter (Redis, DuckDB, Parquet)
        # Per Requirements 5.3: Redis list, DuckDB table, Parquet files
        success_count = 0
        failure_count = 0

        for alert in alerts:
            try:
                # Convert alert to storage format
                # Keep required fields for AnomalyValidator: alert_id, alert_level, created_at
                alert_data = {
                    'alert_id': alert.get('alert_id'),
                    'timestamp': alert.get('timestamp'),
                    'symbol': alert.get('symbol'),
                    'alert_type': alert.get('alert_type'),
                    'alert_level': alert.get('alert_level'),
                    'created_at': alert.get('created_at'),
                    'details': alert.get('details', '{}'),
                }

                # Write to all 3 tiers via StorageWriter
                results = self.storage_writer.write_alert(alert_data)

                if all(results.values()):
                    success_count += 1
                else:
                    failure_count += 1
                    failed_tiers = [k for k, v in results.items() if not v]
                    self.logger.warning(
                        f"Batch {batch_id}: Partial write failure for alert {alert.get('symbol')} - "
                        f"failed tiers: {failed_tiers}"
                    )

            except Exception as e:
                failure_count += 1
                self.logger.error(f"Batch {batch_id}: Failed to write alert for {alert.get('symbol')}: {str(e)}")

        self.logger.info(
            f"Batch {batch_id}: StorageWriter completed - "
            f"{success_count} succeeded, {failure_count} failed"
        )

    def _process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Process a batch of alerts and write to sinks.
        
        Supports graceful shutdown:
        - Checks should_skip_batch() at start to skip if shutdown requested
        - Marks batch start/end for tracking
        - Completes all writes even if shutdown requested mid-batch

        Args:
            batch_df: Batch DataFrame with alerts
            batch_id: Batch identifier
        """
        try:
            # Check if batch should be skipped due to shutdown request (Requirement 5.2)
            if self.graceful_shutdown.should_skip_batch():
                self.logger.info(f"Batch {batch_id}: Skipping due to shutdown request")
                return
            
            start_time = time.time()

            # Log memory usage at start of batch
            self._log_memory_metrics(batch_id=batch_id, alert_threshold_pct=80.0)

            # Check if batch is empty and determine if we should stop
            is_empty = batch_df.isEmpty()

            if self.should_stop(is_empty):
                self.logger.info(f"Batch {batch_id}: Stopping query due to auto-stop condition")
                if self.query:
                    self.query.stop()
                return

            if is_empty:
                self.logger.info(f"Batch {batch_id} is EMPTY, skipping writes")
                return

            # Mark batch as started for graceful shutdown tracking (Requirement 5.1)
            self.graceful_shutdown.mark_batch_start(batch_id)

            # Collect alerts
            records = batch_df.collect()
            record_count = len(records)

            self.logger.info(f"Batch {batch_id}: Processing {record_count} alerts")

            # Convert to alert dictionaries
            alerts = []
            for row in records:
                # details is a JSON string from to_json(struct(...)), parse it
                details_dict = json.loads(row.details) if row.details else {}

                alert = self._create_alert(
                    timestamp=row.timestamp,
                    symbol=row.symbol,
                    alert_type=row.alert_type,
                    alert_level=row.alert_level,
                    details=details_dict
                )
                alerts.append(alert)

            # Write to sinks
            self._write_alerts_to_sinks(alerts, batch_id)

            # Log batch metrics
            duration = time.time() - start_time
            watermark = str(records[0].timestamp) if records else None
            self._log_batch_metrics(batch_id, duration, record_count, watermark)
            
            # Mark batch as completed for graceful shutdown tracking (Requirement 5.1)
            self.graceful_shutdown.mark_batch_end(batch_id)
            
        except Exception as e:
            self.logger.error(f"Batch {batch_id}: Error in _process_batch: {str(e)}", exc_info=True)
            # Ensure batch is marked as ended even on error (Requirement 5.3)
            self.graceful_shutdown.mark_batch_end(batch_id)


    def _create_stream_reader(self, topic: str) -> DataFrame:
        """
        Create Kafka stream reader for a specific topic.

        Args:
            topic: Kafka topic name

        Returns:
            DataFrame with raw Kafka messages
        """
        self.logger.info(f"Creating stream reader for topic: {topic}")

        try:
            # Use "earliest" to read data written by upstream jobs in the same DAG run
            # Checkpoint will track actual progress, so we won't reprocess old data
            df = (self.spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers)
                  .option("subscribe", topic)
                  .option("startingOffsets", "earliest")
                  .option("maxOffsetsPerTrigger",
                         str(self.config.kafka.max_rate_per_partition * 10))
                  .load())

            self.logger.info(f"Stream reader created for topic: {topic}")
            return df

        except Exception as e:
            self.logger.error(f"Failed to create stream reader for {topic}: {str(e)}")
            raise

    def run(self) -> None:
        """
        Run the Anomaly Detection streaming job.

        Creates Spark session, initializes StorageWriter for 3-tier storage,
        reads from multiple Kafka topics, detects all 6 anomaly types,
        and writes alerts to multiple sinks.

        Multi-source streaming approach:
        - raw_trades -> whale alerts
        - processed_aggregations -> volume spikes, price spikes
        - processed_indicators -> RSI extremes, MACD crossovers
        - processed_indicators + processed_aggregations -> BB breakouts
        """
        try:
            # Create Spark session
            self.spark = self._create_spark_session()

            # Initialize StorageWriter for 3-tier storage (Redis, DuckDB, Parquet)
            self.storage_writer = self._init_storage_writer()

            self.logger.info("Anomaly Detection Job starting...")
            self.logger.info("Detecting 6 anomaly types:")
            self.logger.info(f"  - Whale alerts (threshold: ${self.WHALE_THRESHOLD:,.0f})")
            self.logger.info(f"  - Volume spikes ({self.VOLUME_SPIKE_MULTIPLIER}x average)")
            self.logger.info(f"  - Price spikes ({self.PRICE_SPIKE_THRESHOLD}% change)")
            self.logger.info(f"  - RSI extremes (>{self.RSI_OVERBOUGHT} or <{self.RSI_OVERSOLD})")
            self.logger.info("  - BB breakouts (price outside bands)")
            self.logger.info("  - MACD crossovers (MACD crosses signal)")
            self.logger.info("Writing to 3-tier storage: Redis (hot), DuckDB (warm), Parquet (cold)")

            # Create stream readers for each topic
            raw_trades_stream = self._create_stream_reader(self.config.kafka.topic_raw_trades)
            aggregations_stream = self._create_stream_reader(self.config.kafka.topic_processed_aggregations)
            indicators_stream = self._create_stream_reader(self.config.kafka.topic_processed_indicators)

            # For BB breakouts, we need a second aggregations stream
            aggregations_stream_2 = self._create_stream_reader(self.config.kafka.topic_processed_aggregations)

            # Detect anomalies from each source
            whale_alerts = self.detect_whale_alerts(raw_trades_stream)
            volume_spikes = self.detect_volume_spikes(aggregations_stream)
            price_spikes = self.detect_price_spikes(aggregations_stream)
            rsi_extremes = self.detect_rsi_extremes(indicators_stream)
            macd_crossovers = self.detect_macd_crossovers(indicators_stream)
            bb_breakouts = self.detect_bb_breakouts(indicators_stream, aggregations_stream_2)

            # Union all alerts into a single stream
            # Note: All DataFrames have the same schema:
            # timestamp, symbol, alert_type, alert_level, details, alert_id, created_at
            all_alerts = (whale_alerts
                         .union(volume_spikes)
                         .union(price_spikes)
                         .union(rsi_extremes)
                         .union(macd_crossovers)
                         .union(bb_breakouts))

            # Track start time for timeout-based auto-stop
            self.start_time = time.time()

            self.logger.info("Anomaly Detection Job started successfully (micro-batch mode)")
            self.logger.info(f"Auto-stop config: max_runtime={self.max_runtime_seconds}s, empty_batch_threshold={self.empty_batch_threshold}")
            self.logger.info(f"Graceful shutdown timeout: {self.graceful_shutdown.graceful_shutdown_timeout}s")

            # Write to multiple sinks using foreachBatch
            query = (all_alerts
                    .writeStream
                    .foreachBatch(self._process_batch)
                    .outputMode("append")
                    .trigger(processingTime='10 seconds')
                    .option("checkpointLocation", self.config.spark.checkpoint_location)
                    .start())

            self.query = query

            # Use awaitTermination with timeout for micro-batch processing
            # The query will also stop if should_stop() returns True in _process_batch
            query.awaitTermination(timeout=self.max_runtime_seconds)

            # Wait for any in-progress batch to complete (Requirement 1.3, 1.4)
            was_graceful = self.graceful_shutdown.wait_for_batch_completion()
            
            # Log whether shutdown was graceful or forced (Requirement 3.3)
            if self.graceful_shutdown.shutdown_requested:
                if was_graceful:
                    self.logger.info("Anomaly Detection Job shutdown completed gracefully")
                else:
                    self.logger.warning("Anomaly Detection Job shutdown was forced due to timeout")
            else:
                self.logger.info("Anomaly Detection Job completed successfully")

        except Exception as e:
            self.logger.error(f"Job failed with error: {str(e)}", exc_info=True)
            raise
        finally:
            self._cleanup()

    def _cleanup(self):
        """Clean up resources on shutdown."""
        self.logger.info("Starting cleanup...")

        if self.query and self.query.isActive:
            self.logger.info("Stopping streaming query...")
            self.query.stop()

        if self.spark:
            self.logger.info("Stopping SparkSession...")
            self.spark.stop()

        self.logger.info("Cleanup completed. Shutdown successful.")


def main():
    """Main entry point for Anomaly Detection Job."""
    config = Config.from_env("AnomalyDetectionJob")
    job = AnomalyDetectionJob(config)
    job.run()


if __name__ == "__main__":
    main()
