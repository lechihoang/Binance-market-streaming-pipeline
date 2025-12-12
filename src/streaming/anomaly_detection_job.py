"""
Anomaly Detection Job Module for PySpark Streaming Processor.

This module contains the AnomalyDetectionJob class for detecting various
anomaly types in cryptocurrency market data.

Anomaly Types Detected:
- Whale alerts: Trade value > $100,000 (HIGH)
- Volume spikes: Volume > 3x average (MEDIUM)
- Price spikes: Price change > 2% (HIGH)
- RSI extremes: RSI > 70 or < 30 (LOW)
- BB breakouts: Price outside bands (MEDIUM)
- MACD crossovers: MACD crosses signal (LOW)

Requirements Coverage:
---------------------
    - Requirement 1.2: AnomalyDetectionJob in its own file
"""

import json
import logging
import signal
import sys
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    current_timestamp,
    expr,
    from_json,
    lit,
    struct,
    to_json,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .core import Config, KafkaConnector
from src.utils.logging import StructuredFormatter
from src.utils.shutdown import GracefulShutdown

# Import storage tier classes for StorageWriter integration
from src.storage import MinioStorage, PostgresStorage, RedisStorage, StorageWriter

# Import metrics utilities for production monitoring
from src.utils.metrics import (
    record_error,
    record_message_processed,
    track_latency,
)


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
    """

    # Thresholds per design spec
    WHALE_THRESHOLD = 100000.0  # $100k
    VOLUME_SPIKE_MULTIPLIER = 3.0
    VOLUME_LOOKBACK_PERIODS = 20
    PRICE_SPIKE_THRESHOLD = 2.0  # 2%
    RSI_OVERBOUGHT = 70.0
    RSI_OVERSOLD = 30.0

    def __init__(self, config: Config):
        """Initialize Anomaly Detection Job."""
        self.config = config
        self.logger = self._setup_logging()
        self.spark: Optional[SparkSession] = None
        self.query: Optional[any] = None
        self.shutdown_requested = False
        self.storage_writer: Optional[StorageWriter] = None

        self.empty_batch_count: int = 0
        self.empty_batch_threshold: int = 2
        self.max_runtime_seconds: int = 60
        self.start_time: Optional[float] = None

        self.graceful_shutdown = GracefulShutdown(
            graceful_shutdown_timeout=30,
            shutdown_progress_interval=5,
            logger=self.logger
        )

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _setup_logging(self) -> logging.Logger:
        """Set up structured logging for this job."""
        job_name = "AnomalyDetectionJob"
        numeric_level = getattr(logging, self.config.log_level.upper(), logging.INFO)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(numeric_level)
        formatter = StructuredFormatter(job_name)
        handler.setFormatter(formatter)

        logger = logging.getLogger(__name__ + ".AnomalyDetectionJob")
        logger.setLevel(numeric_level)
        logger.handlers.clear()
        logger.addHandler(handler)

        return logger

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals with graceful shutdown support."""
        self.graceful_shutdown.request_shutdown(signum)
        self.shutdown_requested = True
        if self.query:
            self.query.stop()

    def should_stop(self, is_empty_batch: bool) -> bool:
        """Check if job should stop based on empty batch count or timeout."""
        if self.start_time and (time.time() - self.start_time) > self.max_runtime_seconds:
            self.logger.info(f"Max runtime {self.max_runtime_seconds}s exceeded, stopping")
            return True

        if is_empty_batch:
            self.empty_batch_count += 1
            self.logger.info(f"Empty batch detected, count: {self.empty_batch_count}/{self.empty_batch_threshold}")
            if self.empty_batch_count >= self.empty_batch_threshold:
                self.logger.info(f"{self.empty_batch_threshold} consecutive empty batches, stopping")
                return True
        else:
            if self.empty_batch_count > 0:
                self.logger.info(f"Non-empty batch received, resetting empty batch counter from {self.empty_batch_count} to 0")
            self.empty_batch_count = 0

        return False

    def _create_spark_session(self) -> SparkSession:
        """Create and configure SparkSession with resource limits."""
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
                 .config("spark.sql.caseSensitive", "true")
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
                 .config("spark.sql.streaming.minBatchesToRetain",
                        str(self.config.spark.state_store_min_batches_to_retain))
                 .config("spark.sql.streaming.stateStore.maintenanceInterval",
                        self.config.spark.state_store_maintenance_interval)
                 .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
                 .getOrCreate())

        spark.sparkContext.setLogLevel(self.config.log_level)

        self.logger.info(f"SparkSession created: {spark.sparkContext.applicationId}")
        self.logger.info(f"Checkpoint location: {self.config.spark.checkpoint_location}")

        return spark

    def _get_jvm_memory_metrics(self) -> Dict[str, Any]:
        """Get JVM memory metrics from Spark."""
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
            }
        except Exception as e:
            return {"heap_used_mb": 0, "heap_max_mb": 0, "heap_usage_pct": 0, "error": str(e)}

    def _log_memory_metrics(self, batch_id: Optional[int] = None, alert_threshold_pct: float = 80.0) -> None:
        """Log JVM memory metrics and alert if usage is high."""
        metrics = self._get_jvm_memory_metrics()
        if "error" in metrics:
            self.logger.warning(f"Failed to get memory metrics: {metrics['error']}")
            return
        batch_str = f"Batch {batch_id}: " if batch_id is not None else ""
        self.logger.info(f"{batch_str}Memory usage: {metrics['heap_used_mb']:.0f}MB / {metrics['heap_max_mb']:.0f}MB ({metrics['heap_usage_pct']:.1f}%)")
        if metrics['heap_usage_pct'] >= alert_threshold_pct:
            self.logger.warning(f"{batch_str}HIGH MEMORY USAGE ALERT: {metrics['heap_usage_pct']:.1f}%")

    def _log_batch_metrics(self, batch_id: int, duration_seconds: float, record_count: int, watermark: Optional[str] = None) -> None:
        """Log batch processing metrics."""
        watermark_str = f", watermark={watermark}" if watermark else ""
        self.logger.info(f"Batch {batch_id} completed: duration={duration_seconds:.2f}s, records={record_count}{watermark_str}")

    def _init_storage_writer(self) -> StorageWriter:
        """Initialize StorageWriter with all 3 storage tiers."""
        self.logger.info("Initializing StorageWriter with 3-tier storage (Redis, PostgreSQL, MinIO)")

        redis_storage = RedisStorage(host=self.config.redis.host, port=self.config.redis.port, db=self.config.redis.db)
        postgres_storage = PostgresStorage(
            host=self.config.postgres.host, port=self.config.postgres.port,
            user=self.config.postgres.user, password=self.config.postgres.password,
            database=self.config.postgres.database, max_retries=self.config.postgres.max_retries,
            retry_delay=self.config.postgres.retry_delay
        )
        minio_storage = MinioStorage(
            endpoint=self.config.minio.endpoint, access_key=self.config.minio.access_key,
            secret_key=self.config.minio.secret_key, bucket=self.config.minio.bucket,
            secure=self.config.minio.secure, max_retries=self.config.minio.max_retries
        )

        storage_writer = StorageWriter(redis=redis_storage, postgres=postgres_storage, minio=minio_storage)
        self.logger.info("StorageWriter initialized successfully with PostgreSQL and MinIO")
        return storage_writer

    @staticmethod
    def _get_trade_schema() -> StructType:
        """Get schema for raw trade messages from Binance connector."""
        original_data_schema = StructType([
            StructField("E", LongType(), False),
            StructField("s", StringType(), False),
            StructField("p", StringType(), False),
            StructField("q", StringType(), False),
            StructField("m", BooleanType(), False),
            StructField("t", LongType(), True)
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
        """Detect whale alerts from raw trades. Whale alert: Trade value > $100,000, level HIGH"""
        self.logger.info(f"Detecting whale alerts with threshold: ${self.WHALE_THRESHOLD:,.2f}")
        trade_schema = self._get_trade_schema()
        parsed_df = df.select(from_json(col("value").cast("string"), trade_schema).alias("trade"))
        trades_df = parsed_df.select(
            (col("trade.original_data.E") / 1000).cast(TimestampType()).alias("timestamp"),
            col("trade.symbol").alias("symbol"),
            col("trade.original_data.p").cast(DoubleType()).alias("price"),
            col("trade.original_data.q").cast(DoubleType()).alias("quantity"),
            when(col("trade.original_data.m") == True, lit("SELL")).otherwise(lit("BUY")).alias("side"),
        ).withColumn("value", col("price") * col("quantity"))
        trades_df = trades_df.withWatermark("timestamp", "1 minute")
        whale_trades = trades_df.filter(col("value") > self.WHALE_THRESHOLD)
        alerts_df = whale_trades.select(
            col("timestamp"), col("symbol"),
            lit("WHALE_ALERT").alias("alert_type"), lit("HIGH").alias("alert_level"),
            to_json(struct(col("price"), col("quantity"), col("value"), col("side"))).alias("details"),
            expr("uuid()").alias("alert_id"), current_timestamp().alias("created_at")
        )
        self.logger.info("Whale alert detection configured")
        return alerts_df

    def detect_volume_spikes(self, df: DataFrame) -> DataFrame:
        """Detect volume spikes from aggregations. Volume spike: Quote volume > $1M, level MEDIUM"""
        QUOTE_VOLUME_THRESHOLD = 1000000.0
        self.logger.info(f"Detecting volume spikes with quote_volume threshold: ${QUOTE_VOLUME_THRESHOLD:,.0f}")
        agg_schema = self._get_aggregation_schema()
        parsed_df = df.select(from_json(col("value").cast("string"), agg_schema).alias("agg"))
        aggs_df = parsed_df.select(
            col("agg.window_start").alias("window_start"), col("agg.window_end").alias("window_end"),
            col("agg.window_duration").alias("window_duration"), col("agg.symbol").alias("symbol"),
            col("agg.volume").alias("volume"), col("agg.quote_volume").alias("quote_volume"),
            col("agg.trade_count").alias("trade_count")
        ).filter(col("window_duration") == "5m")
        aggs_df = aggs_df.withWatermark("window_start", "1 minute")
        volume_spikes = aggs_df.filter(col("quote_volume") > QUOTE_VOLUME_THRESHOLD)
        alerts_df = volume_spikes.select(
            col("window_start").alias("timestamp"), col("symbol"),
            lit("VOLUME_SPIKE").alias("alert_type"), lit("MEDIUM").alias("alert_level"),
            to_json(struct(col("volume"), col("quote_volume"), col("trade_count"))).alias("details"),
            expr("uuid()").alias("alert_id"), current_timestamp().alias("created_at")
        )
        self.logger.info("Volume spike detection configured")
        return alerts_df

    def detect_price_spikes(self, df: DataFrame) -> DataFrame:
        """Detect price spikes from aggregations. Price spike: Price change > 2% in 1 minute, level HIGH"""
        self.logger.info(f"Detecting price spikes with threshold: {self.PRICE_SPIKE_THRESHOLD}%")
        agg_schema = self._get_aggregation_schema()
        parsed_df = df.select(from_json(col("value").cast("string"), agg_schema).alias("agg"))
        aggs_df = parsed_df.select(
            col("agg.window_start").alias("window_start"), col("agg.window_end").alias("window_end"),
            col("agg.window_duration").alias("window_duration"), col("agg.symbol").alias("symbol"),
            col("agg.open").alias("open"), col("agg.close").alias("close"),
            col("agg.price_change_pct").alias("price_change_pct")
        ).filter(col("window_duration") == "1m")
        aggs_df = aggs_df.withWatermark("window_start", "1 minute")
        price_spikes = aggs_df.filter(spark_abs(col("price_change_pct")) > self.PRICE_SPIKE_THRESHOLD)
        alerts_df = price_spikes.select(
            col("window_start").alias("timestamp"), col("symbol"),
            lit("PRICE_SPIKE").alias("alert_type"), lit("HIGH").alias("alert_level"),
            to_json(struct(col("open"), col("close"), col("price_change_pct"))).alias("details"),
            expr("uuid()").alias("alert_id"), current_timestamp().alias("created_at")
        )
        self.logger.info("Price spike detection configured")
        return alerts_df

    def detect_rsi_extremes(self, df: DataFrame) -> DataFrame:
        """Detect RSI extreme conditions from indicators. RSI extreme: RSI > 70 or RSI < 30, level LOW"""
        self.logger.info(f"Detecting RSI extremes: overbought > {self.RSI_OVERBOUGHT}, oversold < {self.RSI_OVERSOLD}")
        indicator_schema = self._get_indicator_schema()
        parsed_df = df.select(from_json(col("value").cast("string"), indicator_schema).alias("ind"))
        indicators_df = parsed_df.select(
            col("ind.timestamp").alias("timestamp"), col("ind.symbol").alias("symbol"),
            col("ind.rsi_14").alias("rsi_14")
        )
        indicators_df = indicators_df.withWatermark("timestamp", "1 minute")
        rsi_extremes = indicators_df.filter(
            (col("rsi_14").isNotNull()) &
            ((col("rsi_14") > self.RSI_OVERBOUGHT) | (col("rsi_14") < self.RSI_OVERSOLD))
        )
        alerts_df = rsi_extremes.select(
            col("timestamp"), col("symbol"),
            lit("RSI_EXTREME").alias("alert_type"), lit("LOW").alias("alert_level"),
            to_json(struct(col("rsi_14"),
                when(col("rsi_14") > self.RSI_OVERBOUGHT, "OVERBOUGHT").otherwise("OVERSOLD").alias("condition")
            )).alias("details"),
            expr("uuid()").alias("alert_id"), current_timestamp().alias("created_at")
        )
        self.logger.info("RSI extreme detection configured")
        return alerts_df

    def detect_bb_breakouts(self, indicators_df: DataFrame, aggs_df: DataFrame) -> DataFrame:
        """Detect Bollinger Band breakouts from indicators and aggregations. BB breakout: Price outside bands, level MEDIUM"""
        self.logger.info("Detecting Bollinger Band breakouts")
        indicator_schema = self._get_indicator_schema()
        agg_schema = self._get_aggregation_schema()
        parsed_indicators = indicators_df.select(
            from_json(col("value").cast("string"), indicator_schema).alias("ind")
        ).select(
            col("ind.timestamp").alias("timestamp"), col("ind.symbol").alias("symbol"),
            col("ind.bb_upper").alias("bb_upper"), col("ind.bb_lower").alias("bb_lower")
        )
        parsed_aggs = aggs_df.select(
            from_json(col("value").cast("string"), agg_schema).alias("agg")
        ).select(
            col("agg.window_start").alias("timestamp"), col("agg.symbol").alias("symbol"),
            col("agg.close").alias("price"), col("agg.window_duration").alias("window_duration")
        ).filter(col("window_duration") == "1m")
        indicators_with_watermark = parsed_indicators.withWatermark("timestamp", "1 minute")
        aggs_with_watermark = parsed_aggs.withWatermark("timestamp", "1 minute")
        joined_df = indicators_with_watermark.join(
            aggs_with_watermark,
            (indicators_with_watermark.symbol == aggs_with_watermark.symbol) &
            (indicators_with_watermark.timestamp == aggs_with_watermark.timestamp),
            "inner"
        ).select(
            indicators_with_watermark.timestamp, indicators_with_watermark.symbol,
            aggs_with_watermark.price, indicators_with_watermark.bb_upper, indicators_with_watermark.bb_lower
        )
        breakouts = joined_df.filter(
            (col("bb_upper").isNotNull()) & (col("bb_lower").isNotNull()) &
            ((col("price") > col("bb_upper")) | (col("price") < col("bb_lower")))
        )
        alerts_df = breakouts.select(
            col("timestamp"), col("symbol"),
            lit("BB_BREAKOUT").alias("alert_type"), lit("MEDIUM").alias("alert_level"),
            to_json(struct(col("price"), col("bb_upper"), col("bb_lower"),
                when(col("price") > col("bb_upper"), "UPPER").otherwise("LOWER").alias("breakout_direction")
            )).alias("details"),
            expr("uuid()").alias("alert_id"), current_timestamp().alias("created_at")
        )
        self.logger.info("BB breakout detection configured")
        return alerts_df

    def detect_macd_crossovers(self, df: DataFrame) -> DataFrame:
        """Detect MACD crossovers from indicators. MACD crossover: Histogram near zero, level LOW"""
        self.logger.info("Detecting MACD crossovers using histogram approach")
        indicator_schema = self._get_indicator_schema()
        parsed_df = df.select(from_json(col("value").cast("string"), indicator_schema).alias("ind"))
        indicators_df = parsed_df.select(
            col("ind.timestamp").alias("timestamp"), col("ind.symbol").alias("symbol"),
            col("ind.macd_line").alias("macd_line"), col("ind.macd_signal").alias("macd_signal"),
            col("ind.macd_histogram").alias("macd_histogram")
        )
        indicators_df = indicators_df.withWatermark("timestamp", "1 minute")
        HISTOGRAM_THRESHOLD = 0.0001
        crossovers = indicators_df.filter(
            (col("macd_line").isNotNull()) & (col("macd_signal").isNotNull()) &
            (col("macd_histogram").isNotNull()) &
            (spark_abs(col("macd_histogram")) < HISTOGRAM_THRESHOLD) &
            (spark_abs(col("macd_line")) > 0.001)
        )
        alerts_df = crossovers.select(
            col("timestamp"), col("symbol"),
            lit("MACD_CROSSOVER").alias("alert_type"), lit("LOW").alias("alert_level"),
            to_json(struct(col("macd_line"), col("macd_signal"), col("macd_histogram"),
                when(col("macd_line") > col("macd_signal"), "BULLISH").otherwise("BEARISH").alias("direction")
            )).alias("details"),
            expr("uuid()").alias("alert_id"), current_timestamp().alias("created_at")
        )
        self.logger.info("MACD crossover detection configured")
        return alerts_df


    def _create_alert(self, timestamp: datetime, symbol: str, alert_type: str, alert_level: str, details: Dict[str, Any]) -> Dict[str, Any]:
        """Create an alert dictionary with all required fields."""
        return {
            "alert_id": str(uuid.uuid4()),
            "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else str(timestamp),
            "symbol": symbol, "alert_type": alert_type, "alert_level": alert_level,
            "details": details, "created_at": datetime.utcnow().isoformat()
        }

    def _write_alerts_to_sinks(self, alerts: List[Dict[str, Any]], batch_id: int) -> None:
        """Write alerts to multiple sinks using StorageWriter for 3-tier storage."""
        if not alerts:
            self.logger.debug(f"Batch {batch_id}: No alerts to write")
            return
        self.logger.info(f"Batch {batch_id}: Writing {len(alerts)} alerts to sinks")

        try:
            kafka_conn = KafkaConnector(bootstrap_servers=self.config.kafka.bootstrap_servers, client_id="anomaly_detection_job")
            for alert in alerts:
                kafka_conn.send(topic=self.config.kafka.topic_alerts, value=alert, key=alert["symbol"])
            kafka_conn.close()
            self.logger.debug(f"Batch {batch_id}: Wrote {len(alerts)} alerts to Kafka")
        except Exception as e:
            self.logger.error(f"Batch {batch_id}: Failed to write to Kafka: {str(e)}")

        success_count = 0
        failure_count = 0
        for alert in alerts:
            try:
                alert_data = {
                    'alert_id': alert.get('alert_id'), 'timestamp': alert.get('timestamp'),
                    'symbol': alert.get('symbol'), 'alert_type': alert.get('alert_type'),
                    'alert_level': alert.get('alert_level'), 'created_at': alert.get('created_at'),
                    'details': alert.get('details', '{}'),
                }
                results = self.storage_writer.write_alert(alert_data)
                if all(results.values()):
                    success_count += 1
                else:
                    failure_count += 1
                    failed_tiers = [k for k, v in results.items() if not v]
                    self.logger.warning(f"Batch {batch_id}: Partial write failure for alert {alert.get('symbol')} - failed tiers: {failed_tiers}")
            except Exception as e:
                failure_count += 1
                self.logger.error(f"Batch {batch_id}: Failed to write alert for {alert.get('symbol')}: {str(e)}")
        self.logger.info(f"Batch {batch_id}: StorageWriter completed - {success_count} succeeded, {failure_count} failed")

    def _process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Process a batch of alerts and write to sinks."""
        try:
            if self.graceful_shutdown.should_skip_batch():
                self.logger.info(f"Batch {batch_id}: Skipping due to shutdown request")
                return
            
            # Track batch processing latency using utils metrics
            with track_latency("spark_anomaly_detection", "batch_processing"):
                self._log_memory_metrics(batch_id=batch_id, alert_threshold_pct=80.0)
                is_empty = batch_df.isEmpty()
                if self.should_stop(is_empty):
                    self.logger.info(f"Batch {batch_id}: Stopping query due to auto-stop condition")
                    if self.query:
                        self.query.stop()
                    return
                if is_empty:
                    self.logger.info(f"Batch {batch_id} is EMPTY, skipping writes")
                    return
                self.graceful_shutdown.mark_batch_start(batch_id)
                start_time = time.time()
                records = batch_df.collect()
                record_count = len(records)
                self.logger.info(f"Batch {batch_id}: Processing {record_count} alerts")
                alerts = []
                for row in records:
                    details_dict = json.loads(row.details) if row.details else {}
                    alert = self._create_alert(timestamp=row.timestamp, symbol=row.symbol, alert_type=row.alert_type, alert_level=row.alert_level, details=details_dict)
                    alerts.append(alert)
                    
                    # Record message processed metric for each alert
                    record_message_processed(
                        service="spark_anomaly_detection",
                        topic="alerts",
                        status="success"
                    )
                
                self._write_alerts_to_sinks(alerts, batch_id)
                duration = time.time() - start_time
                watermark = str(records[0].timestamp) if records else None
                self._log_batch_metrics(batch_id, duration, record_count, watermark)
                self.graceful_shutdown.mark_batch_end(batch_id)
        except Exception as e:
            # Record error metric for batch processing failure
            record_error(
                service="spark_anomaly_detection",
                error_type="batch_processing_error",
                severity="error"
            )
            self.logger.error(f"Batch {batch_id}: Error in _process_batch: {str(e)}", exc_info=True)
            self.graceful_shutdown.mark_batch_end(batch_id)

    def _create_stream_reader(self, topic: str) -> DataFrame:
        """Create Kafka stream reader for a specific topic."""
        self.logger.info(f"Creating stream reader for topic: {topic}")
        try:
            df = (self.spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers)
                  .option("subscribe", topic)
                  .option("startingOffsets", "earliest")
                  .option("maxOffsetsPerTrigger", str(self.config.kafka.max_rate_per_partition * 10))
                  .load())
            self.logger.info(f"Stream reader created for topic: {topic}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to create stream reader for {topic}: {str(e)}")
            raise

    def run(self) -> None:
        """Run the Anomaly Detection streaming job."""
        try:
            self.spark = self._create_spark_session()
            self.storage_writer = self._init_storage_writer()

            self.logger.info("Anomaly Detection Job starting...")
            self.logger.info("Detecting 6 anomaly types:")
            self.logger.info(f"  - Whale alerts (threshold: ${self.WHALE_THRESHOLD:,.0f})")
            self.logger.info(f"  - Volume spikes ({self.VOLUME_SPIKE_MULTIPLIER}x average)")
            self.logger.info(f"  - Price spikes ({self.PRICE_SPIKE_THRESHOLD}% change)")
            self.logger.info(f"  - RSI extremes (>{self.RSI_OVERBOUGHT} or <{self.RSI_OVERSOLD})")
            self.logger.info("  - BB breakouts (price outside bands)")
            self.logger.info("  - MACD crossovers (MACD crosses signal)")
            self.logger.info("Writing to 3-tier storage: Redis (hot), PostgreSQL (warm), MinIO (cold)")

            raw_trades_stream = self._create_stream_reader(self.config.kafka.topic_raw_trades)
            aggregations_stream = self._create_stream_reader(self.config.kafka.topic_processed_aggregations)
            indicators_stream = self._create_stream_reader(self.config.kafka.topic_processed_indicators)
            aggregations_stream_2 = self._create_stream_reader(self.config.kafka.topic_processed_aggregations)

            whale_alerts = self.detect_whale_alerts(raw_trades_stream)
            volume_spikes = self.detect_volume_spikes(aggregations_stream)
            price_spikes = self.detect_price_spikes(aggregations_stream)
            rsi_extremes = self.detect_rsi_extremes(indicators_stream)
            macd_crossovers = self.detect_macd_crossovers(indicators_stream)
            bb_breakouts = self.detect_bb_breakouts(indicators_stream, aggregations_stream_2)

            all_alerts = (whale_alerts.union(volume_spikes).union(price_spikes)
                         .union(rsi_extremes).union(macd_crossovers).union(bb_breakouts))

            self.start_time = time.time()
            self.logger.info("Anomaly Detection Job started successfully (micro-batch mode)")
            self.logger.info(f"Auto-stop config: max_runtime={self.max_runtime_seconds}s, empty_batch_threshold={self.empty_batch_threshold}")
            self.logger.info(f"Graceful shutdown timeout: {self.graceful_shutdown.graceful_shutdown_timeout}s")

            query = (all_alerts.writeStream.foreachBatch(self._process_batch).outputMode("append")
                    .trigger(processingTime='10 seconds')
                    .option("checkpointLocation", self.config.spark.checkpoint_location).start())
            self.query = query
            query.awaitTermination(timeout=self.max_runtime_seconds)

            was_graceful = self.graceful_shutdown.wait_for_batch_completion()
            if self.graceful_shutdown.shutdown_requested:
                if was_graceful:
                    self.logger.info("Anomaly Detection Job shutdown completed gracefully")
                else:
                    self.logger.warning("Anomaly Detection Job shutdown was forced due to timeout")
            else:
                self.logger.info("Anomaly Detection Job completed successfully")
        except Exception as e:
            # Record error metric for job failure
            record_error(
                service="spark_anomaly_detection",
                error_type="job_failure",
                severity="critical"
            )
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


def run_anomaly_detection_job():
    """Main entry point for Anomaly Detection Job."""
    config = Config.from_env("AnomalyDetectionJob")
    job = AnomalyDetectionJob(config)
    job.run()
