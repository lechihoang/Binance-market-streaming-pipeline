"""
Trade Aggregation Job

Aggregates raw trade data into OHLCV candles with multiple time windows.
Reads from raw_trades Kafka topic, computes aggregations, and writes to
multiple sinks (Kafka, Redis, Parquet).

This is a self-contained job with inline logging, metrics, and memory monitoring.
"""

import logging
import signal
import sys
import time
from datetime import datetime
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, window, count, sum as spark_sum, avg, min as spark_min, 
    max as spark_max, first, last, stddev, when, from_json, to_json, 
    struct, date_format, lit, expr, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, BooleanType, TimestampType
)

from src.utils.config import Config
from src.storage.redis import RedisConnector
from src.utils.shutdown import GracefulShutdown
from src.utils.logging import StructuredFormatter

# Import storage tier classes for StorageWriter integration
from src.storage.redis import RedisStorage
from src.storage.postgres import PostgresStorage
from src.storage.minio import MinioStorage
from src.storage.storage_writer import StorageWriter

# Import metrics utilities for production monitoring
from src.utils.metrics import (
    record_error,
    record_message_processed,
    track_latency,
)


class TradeAggregationJob:
    """
    Spark Structured Streaming job for aggregating raw trades into OHLCV candles.
    
    Processes trades from Kafka, creates tumbling windows (1m, 5m, 15m),
    computes aggregations and derived metrics, and writes to multiple sinks.
    
    This is a self-contained job with inline:
    - SparkSession creation
    - Logging setup
    - Batch metrics logging
    - Memory monitoring
    """
    
    def __init__(self, config: Config):
        """
        Initialize Trade Aggregation Job.
        
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
        self.empty_batch_threshold: int = 3  # Stop after 3 consecutive empty batches
        self.max_runtime_seconds: int = 180  # 3 minutes per job (accounts for ~60s Spark startup)
        self.start_time: Optional[float] = None
        
        # Initialize GracefulShutdown for controlled shutdown behavior
        # Increased timeout to 60s to allow checkpoint completion before forced termination
        self.graceful_shutdown = GracefulShutdown(
            graceful_shutdown_timeout=60,
            shutdown_progress_interval=10,
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
        job_name = "TradeAggregationJob"
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
        """Handle shutdown signals with graceful shutdown support.
        
        IMPORTANT: We do NOT call query.stop() here immediately.
        Instead, we set a flag and let the current batch complete naturally.
        The query will be stopped after the batch finishes and checkpoint is committed.
        This prevents offset loss due to incomplete checkpoint writes.
        """
        # Delegate to GracefulShutdown for proper shutdown handling
        self.graceful_shutdown.request_shutdown(signum)
        self.shutdown_requested = True
        
        # NOTE: Do NOT call query.stop() here!
        # Let the current batch complete and checkpoint commit.
        # The should_stop() method in write_to_sinks will stop the query
        # after the batch completes, ensuring checkpoint is written.
        self.logger.info("Shutdown signal received - will stop after current batch completes and checkpoint commits")
    
    def should_stop(self, is_empty_batch: bool) -> bool:
        """
        Check if job should stop based on shutdown signal, empty batch count or timeout.
        
        IMPORTANT: This is called INSIDE foreachBatch, so returning True here
        and calling query.stop() will allow Spark to commit the checkpoint
        for the current batch before stopping.
        
        Args:
            is_empty_batch: Whether the current batch is empty
            
        Returns:
            True if job should stop, False otherwise
        """
        # Check if shutdown was requested via signal (SIGTERM/SIGINT)
        # This ensures we stop gracefully after the current batch completes
        if self.shutdown_requested:
            self.logger.info("Shutdown signal detected, stopping after this batch completes")
            return True
        
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
                 .config("spark.sql.caseSensitive", "true")  # Enable case-sensitive field names (e vs E, t vs T)
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
        Create Kafka stream reader for raw_trades topic.
        
        Returns:
            DataFrame with raw Kafka messages
        """
        self.logger.info(f"Creating stream reader for topic: {self.config.kafka.topic_raw_trades}")
        self.logger.info(f"Kafka bootstrap servers: {self.config.kafka.bootstrap_servers}")
        
        try:
            # Use "earliest" so checkpoint can track progress
            # When job restarts, it continues from last checkpoint offset (not from beginning)
            # This ensures no data is lost when job is temporarily stopped
            df = (self.spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers)
                  .option("subscribe", self.config.kafka.topic_raw_trades)
                  .option("startingOffsets", "earliest")
                  .option("maxOffsetsPerTrigger", 
                         str(self.config.kafka.max_rate_per_partition * 10))
                  .load())
            
            self.logger.info("Stream reader created successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to create stream reader: {str(e)}", 
                            extra={"topic": self.config.kafka.topic_raw_trades,
                                   "error": str(e)})
            raise
    
    def parse_trades(self, df: DataFrame) -> DataFrame:
        """
        Parse JSON trade messages and extract fields.
        
        Args:
            df: Raw Kafka DataFrame
            
        Returns:
            DataFrame with parsed trade fields and watermark
        """
        # Define schema for Binance connector EnrichedMessage format
        # Must match fields from EnrichedMessage in binance_kafka_connector/models.py
        # Note: spark.sql.caseSensitive=true is enabled to distinguish e/E and t/T fields
        original_data_schema = StructType([
            StructField("e", StringType(), True),   # Event type ("trade")
            StructField("E", LongType(), True),     # Event time (milliseconds)
            StructField("s", StringType(), True),   # Symbol
            StructField("t", LongType(), True),     # Trade ID
            StructField("p", StringType(), True),   # Price
            StructField("q", StringType(), True),   # Quantity
            StructField("T", LongType(), True),     # Trade time (milliseconds)
            StructField("m", BooleanType(), True),  # Is buyer maker
        ])
        
        # Full EnrichedMessage schema matching binance_kafka_connector/models.py
        trade_schema = StructType([
            StructField("original_data", original_data_schema, True),
            StructField("ingestion_timestamp", LongType(), True),
            StructField("source", StringType(), True),
            StructField("data_version", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("stream_type", StringType(), True),
            StructField("topic", StringType(), True)
        ])
        
        self.logger.info("Parsing trade messages with Binance connector schema")
        
        try:
            # Parse JSON from Kafka value
            parsed_df = df.select(
                from_json(col("value").cast("string"), trade_schema).alias("trade"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            )
            
            # Extract fields and convert event_time to timestamp
            # Use getField() to avoid case-insensitive ambiguity between 'E' and 'e'
            original_data_col = col("trade").getField("original_data")
            extracted_df = parsed_df.select(
                (original_data_col.getField("E") / 1000).cast(TimestampType()).alias("event_time"),
                col("trade").getField("symbol").alias("symbol"),
                original_data_col.getField("p").cast(DoubleType()).alias("price"),
                original_data_col.getField("q").cast(DoubleType()).alias("quantity"),
                original_data_col.getField("m").alias("is_buyer_maker"),
                original_data_col.getField("t").alias("trade_id"),
                col("topic"),
                col("partition"),
                col("offset")
            )
            
            # Add watermark for late event handling (1 minute)
            watermarked_df = extracted_df.withWatermark("event_time", "1 minute")
            
            self.logger.info("Trade parsing configured with 1 minute watermark")
            return watermarked_df
            
        except Exception as e:
            self.logger.error(f"Failed to parse trades: {str(e)}", 
                            extra={"error": str(e)})
            raise
    
    def aggregate_windows(self, df: DataFrame) -> DataFrame:
        """
        Create tumbling window aggregations for multiple time windows.
        
        Args:
            df: Parsed trades DataFrame with watermark
            
        Returns:
            DataFrame with OHLCV aggregations for 1m, 5m, 15m windows
        """
        self.logger.info("Creating window aggregations: 1m, 5m, 15m")
        
        # Define window durations
        window_durations = ["1 minute", "5 minutes", "15 minutes"]
        window_labels = ["1m", "5m", "15m"]
        
        aggregated_dfs = []
        
        for duration, label in zip(window_durations, window_labels):
            # Create tumbling window and group by symbol
            windowed_df = (df
                          .groupBy(
                              window(col("event_time"), duration).alias("window"),
                              col("symbol")
                          )
                          .agg(
                              # Basic aggregations
                              count("*").alias("trade_count"),
                              spark_sum("quantity").alias("volume"),
                              spark_sum(col("price") * col("quantity")).alias("quote_volume"),
                              avg("price").alias("avg_price"),
                              spark_min("price").alias("low"),
                              spark_max("price").alias("high"),
                              first("price").alias("open"),
                              last("price").alias("close"),
                              stddev("price").alias("price_stddev"),
                              # Buy/sell counts
                              spark_sum(when(col("is_buyer_maker") == False, 1).otherwise(0)).alias("buy_count"),
                              spark_sum(when(col("is_buyer_maker") == True, 1).otherwise(0)).alias("sell_count")
                          ))
            
            # Extract window start and end, add window duration label
            windowed_df = windowed_df.select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                lit(label).alias("window_duration"),
                col("symbol"),
                col("open"),
                col("high"),
                col("low"),
                col("close"),
                col("volume"),
                col("quote_volume"),
                col("trade_count"),
                col("avg_price"),
                col("price_stddev"),
                col("buy_count"),
                col("sell_count")
            )
            
            aggregated_dfs.append(windowed_df)
        
        # Union all window aggregations
        result_df = aggregated_dfs[0]
        for agg_df in aggregated_dfs[1:]:
            result_df = result_df.union(agg_df)
        
        self.logger.info("Window aggregations created successfully")
        return result_df
    
    def compute_derived_metrics(self, df: DataFrame) -> DataFrame:
        """
        Compute derived metrics from aggregated data.
        
        Args:
            df: Aggregated OHLCV DataFrame
            
        Returns:
            DataFrame with additional derived metrics
        """
        self.logger.info("Computing derived metrics: VWAP, price_change_pct, buy_sell_ratio, large_order_count")
        
        # Define large order threshold (can be made configurable)
        large_order_threshold = 10.0
        
        result_df = df.withColumn(
            "vwap",
            col("quote_volume") / col("volume")
        ).withColumn(
            "price_change_pct",
            ((col("close") - col("open")) / col("open")) * 100
        ).withColumn(
            "buy_sell_ratio",
            when(col("sell_count") > 0, col("buy_count") / col("sell_count")).otherwise(lit(None))
        ).withColumn(
            "large_order_count",
            lit(0)  # Placeholder - would need per-trade data to compute accurately
        )
        
        self.logger.info("Derived metrics computed successfully")
        return result_df

    
    def write_to_sinks(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Write batch to multiple sinks using StorageWriter for 3-tier storage.
        
        Writes to:
        - Kafka (for downstream consumers)
        - Redis, DuckDB, Parquet (via StorageWriter for 3-tier storage)
        
        Supports graceful shutdown:
        - Checks should_skip_batch() at start to skip if shutdown requested
        - Marks batch start/end for tracking
        - Completes all writes even if shutdown requested mid-batch
        
        Args:
            batch_df: Batch DataFrame to write
            batch_id: Batch identifier
        """
        try:
            # Check if batch should be skipped due to shutdown request (Requirement 5.2)
            if self.graceful_shutdown.should_skip_batch():
                self.logger.info(f"Batch {batch_id}: Skipping due to shutdown request")
                return
            
            # Track batch processing latency using utils metrics
            with track_latency("spark_trade_aggregation", "batch_processing"):
                self._write_to_sinks_impl(batch_df, batch_id)
                
        except Exception as e:
            # Record error metric for batch processing failure
            record_error(
                service="spark_trade_aggregation",
                error_type="batch_processing_error",
                severity="error"
            )
            self.logger.error(f"Batch {batch_id}: Error in write_to_sinks: {str(e)}", exc_info=True)
            # Ensure batch is marked as ended even on error (Requirement 5.3)
            self.graceful_shutdown.mark_batch_end(batch_id)
    
    def _write_to_sinks_impl(self, batch_df: DataFrame, batch_id: int) -> None:
        """Internal implementation of write_to_sinks with metrics tracking."""
        start_time = time.time()
        
        self.logger.info(f"write_to_sinks called for batch {batch_id}")
        
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
            self.logger.info(f"Batch {batch_id} is EMPTY, skipping writes")
            return
        
        # Mark batch as started for graceful shutdown tracking (Requirement 5.1)
        self.graceful_shutdown.mark_batch_start(batch_id)
        
        # Collect data for writing
        records = batch_df.collect()
        record_count = len(records)
        
        self.logger.info(f"Writing batch {batch_id} with {record_count} records to sinks")
        
        # Write to Kafka (for downstream consumers like Technical Indicators job)
        try:
            kafka_df = batch_df.select(
                col("symbol").cast("string").alias("key"),
                to_json(struct(*[col(c) for c in batch_df.columns])).alias("value")
            )
            
            kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers) \
                .option("topic", self.config.kafka.topic_processed_aggregations) \
                .option("kafka.enable.idempotence", str(self.config.kafka.enable_idempotence).lower()) \
                .option("kafka.acks", self.config.kafka.acks) \
                .option("kafka.max.in.flight.requests.per.connection", 
                       str(self.config.kafka.max_in_flight_requests_per_connection)) \
                .save()
            
            self.logger.debug(f"Batch {batch_id}: Wrote to Kafka topic {self.config.kafka.topic_processed_aggregations}")
        except Exception as e:
            self.logger.error(f"Batch {batch_id}: Failed to write to Kafka: {str(e)}")
        
        # Write to 3-tier storage using StorageWriter (Redis, DuckDB, Parquet)
        # Per Requirements 5.1: Redis with overwrite, DuckDB with upsert, Parquet with append
        success_count = 0
        failure_count = 0
        
        for row in records:
            try:
                # Convert row to aggregation data dict
                aggregation_data = {
                    'timestamp': row.window_start,
                    'symbol': row.symbol,
                    'interval': row.window_duration,
                    'open': float(row.open) if row.open else None,
                    'high': float(row.high) if row.high else None,
                    'low': float(row.low) if row.low else None,
                    'close': float(row.close) if row.close else None,
                    'volume': float(row.volume) if row.volume else None,
                    'quote_volume': float(row.quote_volume) if hasattr(row, 'quote_volume') and row.quote_volume else 0,
                    'trades_count': int(row.trade_count) if hasattr(row, 'trade_count') and row.trade_count else 0,
                }
                
                # Write to all 3 tiers via StorageWriter
                results = self.storage_writer.write_aggregation(aggregation_data)
                
                if all(results.values()):
                    success_count += 1
                else:
                    failure_count += 1
                    failed_tiers = [k for k, v in results.items() if not v]
                    self.logger.warning(
                        f"Batch {batch_id}: Partial write failure for {row.symbol} - "
                        f"failed tiers: {failed_tiers}"
                    )
                    
            except Exception as e:
                failure_count += 1
                self.logger.error(f"Batch {batch_id}: Failed to write aggregation for {row.symbol}: {str(e)}")
        
        # Record message processed metrics for successful writes
        for _ in range(success_count):
            record_message_processed(
                service="spark_trade_aggregation",
                topic="processed_aggregations",
                status="success"
            )
        
        self.logger.info(
            f"Batch {batch_id}: StorageWriter completed - "
            f"{success_count} succeeded, {failure_count} failed"
        )
        
        # Log batch metrics (inline)
        duration = time.time() - start_time
        
        # Get watermark from batch if available
        watermark = None
        try:
            if records:
                watermark = str(records[0].window_start) if hasattr(records[0], 'window_start') else None
        except:
            pass
        
        # Log comprehensive processing metrics (inline)
        self._log_processing_metrics(
            batch_id,
            self.config.spark.checkpoint_location,
            duration,
            record_count,
            watermark
        )
        
        # Mark batch as completed for graceful shutdown tracking (Requirement 5.1)
        self.graceful_shutdown.mark_batch_end(batch_id)
    
    def run(self) -> None:
        """
        Run the Trade Aggregation streaming job.
        
        Creates Spark session, initializes StorageWriter for 3-tier storage,
        reads from Kafka, processes trades, and writes to multiple sinks.
        
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
            raw_stream = self.create_stream_reader()
            
            # Parse trades
            trades_df = self.parse_trades(raw_stream)
            
            # Aggregate windows
            aggregated_df = self.aggregate_windows(trades_df)
            
            # Compute derived metrics
            enriched_df = self.compute_derived_metrics(aggregated_df)
            
            # Track start time for timeout-based auto-stop
            self.start_time = time.time()
            
            self.logger.info("Trade Aggregation Job started successfully (micro-batch mode)")
            self.logger.info(f"Reading from topic: {self.config.kafka.topic_raw_trades}")
            self.logger.info("Writing to 3-tier storage: Redis (hot), PostgreSQL (warm), MinIO (cold)")
            self.logger.info(f"Auto-stop config: max_runtime={self.max_runtime_seconds}s, empty_batch_threshold={self.empty_batch_threshold}")
            self.logger.info(f"Graceful shutdown timeout: {self.graceful_shutdown.graceful_shutdown_timeout}s")
            
            # Write to multiple sinks using foreachBatch
            # Trigger interval increased to 60s to match actual processing time (~40-50s per batch)
            # This prevents "falling behind" warnings and allows proper batch completion
            query = (enriched_df
                    .writeStream
                    .foreachBatch(self.write_to_sinks)
                    .outputMode("update")
                    .trigger(processingTime='60 seconds')
                    .option("checkpointLocation", self.config.spark.checkpoint_location)
                    .start())
            
            self.query = query
            
            # Use awaitTermination with timeout for micro-batch processing
            # The query will also stop if should_stop() returns True in write_to_sinks
            # IMPORTANT: awaitTermination returns when query stops, but checkpoint
            # may still be committing. We need to wait for that to complete.
            query.awaitTermination(timeout=self.max_runtime_seconds)
            
            # Wait for any in-progress batch to complete (Requirement 1.3, 1.4)
            was_graceful = self.graceful_shutdown.wait_for_batch_completion()
            
            # CRITICAL: Wait a bit for Spark to commit checkpoint after query stops
            # Spark commits checkpoint AFTER foreachBatch returns, so we need to
            # give it time to write the checkpoint before we stop SparkSession
            if self.query and not self.query.isActive:
                self.logger.info("Query stopped, waiting for checkpoint commit...")
                time.sleep(5)  # Give Spark time to commit checkpoint
                self.logger.info("Checkpoint commit wait completed")
            
            # Log whether shutdown was graceful or forced (Requirement 3.3)
            if self.graceful_shutdown.shutdown_requested:
                if was_graceful:
                    self.logger.info("Trade Aggregation Job shutdown completed gracefully")
                else:
                    self.logger.warning("Trade Aggregation Job shutdown was forced due to timeout")
            else:
                self.logger.info("Trade Aggregation Job completed successfully")
            
        except Exception as e:
            # Record error metric for job failure
            record_error(
                service="spark_trade_aggregation",
                error_type="job_failure",
                severity="critical"
            )
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
            self.logger.info("Stopping streaming query...")
            self.query.stop()
        
        if self.spark:
            self.logger.info("Stopping SparkSession...")
            self.spark.stop()
        
        self.logger.info("Cleanup completed. Shutdown successful.")


def main():
    """Main entry point for Trade Aggregation Job."""
    config = Config.from_env("TradeAggregationJob")
    job = TradeAggregationJob(config)
    job.run()


if __name__ == "__main__":
    main()
