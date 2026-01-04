"""Base Spark Job Module."""

import gc
import logging
import signal
import sys
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

import os

from utils.logging import StructuredFormatter
from utils.shutdown import GracefulShutdown
from storage.redis import RedisStorage
from storage.postgres import PostgresStorage
from storage.storage_writer import StorageWriter


class BaseSparkJob(ABC):
    DEFAULT_EMPTY_BATCH_THRESHOLD = 3
    DEFAULT_MAX_RUNTIME_SECONDS = 180
    DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT = 15
    
    def __init__(self, job_name: str):
        self.job_name = job_name
        self._load_config()
        self.logger = self._setup_logging()
        self.spark: Optional[SparkSession] = None
        self.query: Optional[Any] = None
        self.shutdown_requested = False
        self.storage_writer: Optional[StorageWriter] = None
        
        # Micro-batch auto-stop attributes
        self.empty_batch_count: int = 0
        self.empty_batch_threshold: int = self.DEFAULT_EMPTY_BATCH_THRESHOLD
        self.max_runtime_seconds: int = self.DEFAULT_MAX_RUNTIME_SECONDS
        self.start_time: Optional[float] = None
        
        # Initialize GracefulShutdown for controlled shutdown behavior
        self.graceful_shutdown = GracefulShutdown(
            graceful_shutdown_timeout=self.DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT,
            shutdown_progress_interval=10,
            logger=self.logger
        )
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _load_config(self) -> None:
        """Load configuration from environment variables."""
        # Kafka config
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.kafka_topic_raw_trades = os.getenv("TOPIC_RAW_TRADES", "raw_trades")
        self.kafka_topic_processed_aggregations = os.getenv("TOPIC_PROCESSED_AGGREGATIONS", "processed_aggregations")
        self.kafka_topic_alerts = os.getenv("TOPIC_ALERTS", "alerts")
        self.kafka_max_rate_per_partition = int(os.getenv("KAFKA_MAX_RATE_PER_PARTITION", "50000"))
        self.kafka_enable_idempotence = os.getenv("KAFKA_ENABLE_IDEMPOTENCE", "true").lower() in ("true", "1", "yes")
        self.kafka_acks = os.getenv("KAFKA_ACKS", "all")
        self.kafka_max_in_flight_requests = int(os.getenv("KAFKA_MAX_IN_FLIGHT_REQUESTS", "5"))
        
        # Spark config
        self.spark_app_name = os.getenv("SPARK_APP_NAME", self.job_name)
        self.spark_executor_cores = int(os.getenv("SPARK_EXECUTOR_CORES", "1"))
        self.spark_shuffle_partitions = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "2"))
        default_checkpoint = f"/opt/airflow/data/spark-checkpoints/{self.job_name}"
        self.spark_checkpoint_location = os.getenv("SPARK_CHECKPOINT_LOCATION", default_checkpoint)
        self.spark_state_store_min_batches = int(os.getenv("SPARK_STATE_STORE_MIN_BATCHES", "50"))
        self.spark_state_store_maintenance_interval = os.getenv("SPARK_STATE_STORE_MAINTENANCE_INTERVAL", "30s")
        self.spark_backpressure_enabled = os.getenv("SPARK_BACKPRESSURE_ENABLED", "true").lower() in ("true", "1", "yes")
        
        # Redis config
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_db = int(os.getenv("REDIS_DB", "0"))
        
        # Postgres config
        self.postgres_host = os.getenv("POSTGRES_HOST", "localhost")
        self.postgres_port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.postgres_user = os.getenv("POSTGRES_USER", "crypto")
        self.postgres_password = os.getenv("POSTGRES_PASSWORD", "crypto")
        self.postgres_database = os.getenv("POSTGRES_DB", "crypto_data")
        self.postgres_max_retries = int(os.getenv("POSTGRES_MAX_RETRIES", "3"))
        self.postgres_retry_delay = float(os.getenv("POSTGRES_RETRY_DELAY", "1.0"))
        
        # Log level
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

    def _setup_logging(self) -> logging.Logger:
        numeric_level = getattr(logging, self.log_level.upper(), logging.INFO)
        
        # Create handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(numeric_level)
        
        # Set formatter with job name
        formatter = StructuredFormatter(self.job_name)
        handler.setFormatter(formatter)
        
        # Get logger for this module with job-specific name
        logger = logging.getLogger(f"{__name__}.{self.job_name}")
        logger.setLevel(numeric_level)
        logger.handlers.clear()
        logger.addHandler(handler)
        
        return logger
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals with graceful shutdown support."""
        # Delegate to GracefulShutdown for proper shutdown handling
        self.graceful_shutdown.request_shutdown(signum)
        self.shutdown_requested = True
        
        # NOTE: Do NOT call query.stop() here!
        # Let the current batch complete and checkpoint commit.
        # The should_stop() method will stop the query after the batch completes.
        self.logger.info(
            "Shutdown signal received - will stop after current batch completes "
            "and checkpoint commits"
        )
    
    def should_stop(self, is_empty_batch: bool) -> bool:
        """Check if job should stop based on shutdown signal, empty batch count or timeout."""
        if self.shutdown_requested:
            self.logger.info("Shutdown signal detected, stopping after batch completes")
            return True
        
        if self.start_time and (time.time() - self.start_time) > self.max_runtime_seconds:
            self.logger.info(f"Max runtime {self.max_runtime_seconds}s exceeded, stopping")
            return True
        
        if is_empty_batch:
            self.empty_batch_count += 1
            if self.empty_batch_count >= self.empty_batch_threshold:
                self.logger.info(f"{self.empty_batch_threshold} consecutive empty batches, stopping")
                return True
        else:
            self.empty_batch_count = 0
        
        return False

    def _create_spark_session(self) -> SparkSession:
        """Create and configure SparkSession with resource limits."""
        executor_memory = "512m"
        driver_memory = "512m"
        
        self.logger.info(
            f"Creating SparkSession: executor={executor_memory}, driver={driver_memory}, "
            f"cores={self.spark_executor_cores}, partitions={self.spark_shuffle_partitions}"
        )
        
        spark = (SparkSession.builder
                 .appName(self.spark_app_name)
                 .config("spark.jars.packages", 
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
                 .config("spark.sql.caseSensitive", "true")
                 .config("spark.executor.memory", executor_memory)
                 .config("spark.driver.memory", driver_memory)
                 .config("spark.executor.cores", str(self.spark_executor_cores))
                 .config("spark.sql.shuffle.partitions", 
                        str(self.spark_shuffle_partitions))
                 .config("spark.streaming.kafka.maxRatePerPartition",
                        str(self.kafka_max_rate_per_partition))
                 .config("spark.streaming.backpressure.enabled",
                        str(self.spark_backpressure_enabled).lower())
                 .config("spark.sql.streaming.checkpointLocation",
                        self.spark_checkpoint_location)
                 .config("spark.sql.streaming.minBatchesToRetain",
                        str(self.spark_state_store_min_batches))
                 .config("spark.sql.streaming.stateStore.maintenanceInterval",
                        self.spark_state_store_maintenance_interval)
                 .config("spark.sql.streaming.stateStore.minDeltasForSnapshot", "5")
                 .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
                 .getOrCreate())
        
        spark.sparkContext.setLogLevel(self.log_level)
        self.logger.info(f"SparkSession created: {spark.sparkContext.applicationId}")
        
        return spark
    
    def _get_jvm_memory_metrics(self) -> Dict[str, Any]:
        """Get JVM memory metrics from Spark."""
        try:
            from pyspark import SparkContext
            sc = SparkContext.getOrCreate()
            runtime = sc._jvm.java.lang.Runtime.getRuntime()
            
            max_mb = runtime.maxMemory() / (1024 * 1024)
            total_mb = runtime.totalMemory() / (1024 * 1024)
            free_mb = runtime.freeMemory() / (1024 * 1024)
            used_mb = total_mb - free_mb
            
            return {
                "heap_used_mb": round(used_mb, 2),
                "heap_max_mb": round(max_mb, 2),
                "heap_usage_pct": round((used_mb / max_mb) * 100, 2) if max_mb > 0 else 0,
            }
        except Exception as e:
            return {"heap_used_mb": 0, "heap_max_mb": 0, "heap_usage_pct": 0, "error": str(e)}
    
    def _log_memory_metrics(self, batch_id: Optional[int] = None, alert_threshold_pct: float = 80.0) -> None:
        """Log JVM memory metrics and alert if usage is high."""
        metrics = self._get_jvm_memory_metrics()
        if "error" in metrics:
            return
        
        prefix = f"Batch {batch_id}: " if batch_id is not None else ""
        self.logger.info(
            f"{prefix}Memory: {metrics['heap_used_mb']:.0f}MB / {metrics['heap_max_mb']:.0f}MB "
            f"({metrics['heap_usage_pct']:.1f}%)"
        )
        
        if metrics['heap_usage_pct'] >= alert_threshold_pct:
            self.logger.warning(f"{prefix}HIGH MEMORY: {metrics['heap_usage_pct']:.1f}%")
    
    def _log_batch_metrics(
        self,
        batch_id: int,
        duration_seconds: float,
        record_count: int,
        watermark: Optional[str] = None
    ) -> None:
        """Log batch processing metrics."""
        watermark_str = f", watermark={watermark}" if watermark else ""
        self.logger.info(
            f"Batch {batch_id} completed: "
            f"duration={duration_seconds:.2f}s, "
            f"records={record_count}{watermark_str}"
        )

    def _init_storage_writer(self) -> StorageWriter:
        """Initialize StorageWriter with 2-tier storage (Redis, PostgreSQL)."""
        self.logger.info("Initializing StorageWriter (Redis + PostgreSQL)")
        
        redis_storage = RedisStorage(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        postgres_storage = PostgresStorage(
            host=self.postgres_host, port=self.postgres_port,
            user=self.postgres_user, password=self.postgres_password,
            database=self.postgres_database,
            max_retries=self.postgres_max_retries, retry_delay=self.postgres_retry_delay
        )
        
        return StorageWriter(redis=redis_storage, postgres=postgres_storage)
    
    def _cleanup(self) -> None:
        """Clean up resources."""
        self.logger.info("Starting cleanup...")
        cleanup_start = time.time()
        
        if self.query and self.query.isActive:
            self.query.stop()
            self.query = None
        
        if self.spark:
            try:
                self.spark.catalog.clearCache()
            except Exception:
                pass
            self.spark.stop()
            self.spark = None
        
        gc.collect()
        self.logger.info(f"Cleanup completed in {time.time() - cleanup_start:.2f}s")
    
    @abstractmethod
    def run(self) -> None:
        """Run the streaming job. Must be implemented by subclasses."""
        pass
