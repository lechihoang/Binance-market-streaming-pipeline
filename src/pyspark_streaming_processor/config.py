"""
Configuration module for PySpark streaming jobs.

Manages configuration for Kafka connections, Spark settings, Redis, DuckDB,
and job-specific parameters.
"""

import os
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class KafkaConfig:
    """Kafka connection configuration."""
    
    bootstrap_servers: str = "localhost:9092"
    
    # Topics
    topic_raw_trades: str = "raw_trades"
    topic_raw_klines: str = "raw_klines"
    topic_raw_tickers: str = "raw_tickers"
    topic_processed_aggregations: str = "processed_aggregations"
    topic_processed_indicators: str = "processed_indicators"
    topic_alerts: str = "alerts"
    
    # Consumer settings
    max_rate_per_partition: int = 50000  # High throughput for catching up
    starting_offsets: str = "latest"  # Start from latest to avoid processing old backlog
    
    # Producer settings
    compression_type: str = "snappy"
    enable_idempotence: bool = True  # For exactly-once semantics
    acks: str = "all"  # Wait for all replicas to acknowledge
    max_in_flight_requests_per_connection: int = 5  # Max for idempotent producer
    
    @classmethod
    def from_env(cls) -> "KafkaConfig":
        """Create configuration from environment variables."""
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic_raw_trades=os.getenv("TOPIC_RAW_TRADES", "raw_trades"),
            topic_raw_klines=os.getenv("TOPIC_RAW_KLINES", "raw_klines"),
            topic_raw_tickers=os.getenv("TOPIC_RAW_TICKERS", "raw_tickers"),
            topic_processed_aggregations=os.getenv("TOPIC_PROCESSED_AGGREGATIONS", "processed_aggregations"),
            topic_processed_indicators=os.getenv("TOPIC_PROCESSED_INDICATORS", "processed_indicators"),
            topic_alerts=os.getenv("TOPIC_ALERTS", "alerts"),
            max_rate_per_partition=int(os.getenv("KAFKA_MAX_RATE_PER_PARTITION", "50000")),
            starting_offsets=os.getenv("KAFKA_STARTING_OFFSETS", "latest"),
            compression_type=os.getenv("KAFKA_COMPRESSION_TYPE", "snappy"),
            enable_idempotence=os.getenv("KAFKA_ENABLE_IDEMPOTENCE", "true").lower() == "true",
            acks=os.getenv("KAFKA_ACKS", "all"),
            max_in_flight_requests_per_connection=int(os.getenv("KAFKA_MAX_IN_FLIGHT_REQUESTS", "5")),
        )


@dataclass
class SparkConfig:
    """Spark configuration for streaming jobs."""
    
    app_name: str = "PySpark Streaming Processor"
    executor_memory: str = "512m"
    driver_memory: str = "512m"
    executor_cores: int = 1
    shuffle_partitions: int = 2
    
    # Streaming settings - use persistent location instead of /tmp
    checkpoint_location: str = "/opt/airflow/data/spark-checkpoints"
    checkpoint_interval: str = "30 seconds"
    
    # State store settings for reliability
    state_store_min_batches_to_retain: int = 10
    state_store_maintenance_interval: str = "30s"
    
    # Backpressure
    backpressure_enabled: bool = True
    
    @classmethod
    def from_env(cls, job_name: str) -> "SparkConfig":
        """Create configuration from environment variables."""
        # Use persistent checkpoint location (not /tmp which can be cleared)
        default_checkpoint = f"/opt/airflow/data/spark-checkpoints/{job_name}"
        
        return cls(
            app_name=os.getenv("SPARK_APP_NAME", job_name),
            executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "512m"),
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "512m"),
            executor_cores=int(os.getenv("SPARK_EXECUTOR_CORES", "1")),
            shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "2")),
            checkpoint_location=os.getenv("SPARK_CHECKPOINT_LOCATION", default_checkpoint),
            checkpoint_interval=os.getenv("SPARK_CHECKPOINT_INTERVAL", "30 seconds"),
            state_store_min_batches_to_retain=int(os.getenv("SPARK_STATE_STORE_MIN_BATCHES", "10")),
            state_store_maintenance_interval=os.getenv("SPARK_STATE_STORE_MAINTENANCE_INTERVAL", "30s"),
            backpressure_enabled=os.getenv("SPARK_BACKPRESSURE_ENABLED", "true").lower() == "true",
        )


@dataclass
class RedisConfig:
    """Redis connection configuration."""
    
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    
    # Connection pool settings
    max_connections: int = 10
    socket_timeout: int = 5
    socket_connect_timeout: int = 5
    
    # TTL settings
    candle_ttl_seconds: int = 3600  # 1 hour
    indicator_ttl_seconds: int = 3600  # 1 hour
    alert_list_max_size: int = 1000
    
    @classmethod
    def from_env(cls) -> "RedisConfig":
        """Create configuration from environment variables."""
        return cls(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            password=os.getenv("REDIS_PASSWORD"),
            max_connections=int(os.getenv("REDIS_MAX_CONNECTIONS", "10")),
            socket_timeout=int(os.getenv("REDIS_SOCKET_TIMEOUT", "5")),
            socket_connect_timeout=int(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "5")),
            candle_ttl_seconds=int(os.getenv("REDIS_CANDLE_TTL_SECONDS", "3600")),
            indicator_ttl_seconds=int(os.getenv("REDIS_INDICATOR_TTL_SECONDS", "3600")),
            alert_list_max_size=int(os.getenv("REDIS_ALERT_LIST_MAX_SIZE", "1000")),
        )


@dataclass
class DuckDBConfig:
    """DuckDB configuration."""
    
    database_path: str = "/tmp/streaming_data.duckdb"
    
    # Table names
    table_candles: str = "candles"
    table_indicators: str = "indicators"
    table_alerts: str = "alerts"
    
    @classmethod
    def from_env(cls) -> "DuckDBConfig":
        """Create configuration from environment variables."""
        return cls(
            database_path=os.getenv("DUCKDB_DATABASE_PATH", "/tmp/streaming_data.duckdb"),
            table_candles=os.getenv("DUCKDB_TABLE_CANDLES", "candles"),
            table_indicators=os.getenv("DUCKDB_TABLE_INDICATORS", "indicators"),
            table_alerts=os.getenv("DUCKDB_TABLE_ALERTS", "alerts"),
        )


@dataclass
class PostgresConfig:
    """PostgreSQL connection configuration for warm path storage."""
    
    host: str = "localhost"
    port: int = 5432
    user: str = "crypto"
    password: str = "crypto"
    database: str = "crypto_data"
    
    # Connection pool settings
    max_connections: int = 10
    
    # Retry settings
    max_retries: int = 3
    retry_delay: float = 1.0
    
    @classmethod
    def from_env(cls) -> "PostgresConfig":
        """Create configuration from environment variables."""
        return cls(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "crypto"),
            password=os.getenv("POSTGRES_PASSWORD", "crypto"),
            database=os.getenv("POSTGRES_DB", "crypto_data"),
            max_connections=int(os.getenv("POSTGRES_MAX_CONNECTIONS", "10")),
            max_retries=int(os.getenv("POSTGRES_MAX_RETRIES", "3")),
            retry_delay=float(os.getenv("POSTGRES_RETRY_DELAY", "1.0")),
        )


@dataclass
class MinioConfig:
    """MinIO/S3 connection configuration for cold path storage."""
    
    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    bucket: str = "crypto-data"
    secure: bool = False
    
    # Retry settings
    max_retries: int = 3
    
    @classmethod
    def from_env(cls) -> "MinioConfig":
        """Create configuration from environment variables."""
        return cls(
            endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            bucket=os.getenv("MINIO_BUCKET", "crypto-data"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
            max_retries=int(os.getenv("MINIO_MAX_RETRIES", "3")),
        )


@dataclass
class Config:
    """Main configuration container for streaming jobs.
    
    Storage Architecture:
    - Hot Path: Redis (real-time queries)
    - Warm Path: PostgreSQL (90-day analytics)
    - Cold Path: MinIO (historical archive)
    """
    
    kafka: KafkaConfig
    spark: SparkConfig
    redis: RedisConfig
    postgres: PostgresConfig
    minio: MinioConfig
    
    # Logging
    log_level: str = "INFO"
    
    # Keep duckdb for backward compatibility (optional)
    duckdb: Optional[DuckDBConfig] = None
    
    @classmethod
    def from_env(cls, job_name: str) -> "Config":
        """Create complete configuration from environment variables."""
        return cls(
            kafka=KafkaConfig.from_env(),
            spark=SparkConfig.from_env(job_name),
            redis=RedisConfig.from_env(),
            postgres=PostgresConfig.from_env(),
            minio=MinioConfig.from_env(),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            duckdb=DuckDBConfig.from_env(),  # Keep for backward compatibility
        )
