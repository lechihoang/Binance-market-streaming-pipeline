"""
Configuration utilities for environment variable loading.

Provides type-safe environment variable loading with defaults.

Utilities provided:
- get_env_str(): Get string from environment variable
- get_env_int(): Get integer from environment variable
- get_env_float(): Get float from environment variable
- get_env_bool(): Get boolean from environment variable
- get_env_list(): Get list from environment variable
- get_env_optional(): Get optional string from environment variable

Configuration Classes (from streaming/core.py):
- KafkaConfig: Kafka connection configuration
- SparkConfig: Spark configuration for streaming jobs
- RedisConfig: Redis connection configuration
- PostgresConfig: PostgreSQL connection configuration
- MinioConfig: MinIO/S3 connection configuration
- Config: Main configuration container for streaming jobs

Extracted from multiple `from_env()` methods across the codebase to provide
a centralized, reusable configuration loading mechanism.

Requirements:
- 2.1: Provide helper functions for loading typed values from environment variables
- 2.2: Support types: str, int, float, bool, and list
- 2.3: Use provided default value when environment variable is missing
- 2.4: Accept "true", "false", "1", "0" (case-insensitive) for boolean
- 2.5: Split by comma and strip whitespace for list values
"""

import os
from dataclasses import dataclass
from typing import List, Optional


def get_env_str(key: str, default: str = "") -> str:
    """
    Get string from environment variable.
    
    Args:
        key: Environment variable name
        default: Default value if not set (default: "")
        
    Returns:
        Environment variable value or default
        
    Example:
        host = get_env_str("DATABASE_HOST", "localhost")
        
    Requirement 2.1, 2.2, 2.3
    """
    return os.getenv(key, default)


def get_env_int(key: str, default: int = 0) -> int:
    """
    Get integer from environment variable.
    
    Args:
        key: Environment variable name
        default: Default value if not set (default: 0)
        
    Returns:
        Parsed integer value or default
        
    Raises:
        ValueError: If the environment variable value cannot be parsed as int
        
    Example:
        port = get_env_int("DATABASE_PORT", 5432)
        
    Requirement 2.1, 2.2, 2.3
    """
    value = os.getenv(key)
    if value is None:
        return default
    return int(value)


def get_env_float(key: str, default: float = 0.0) -> float:
    """
    Get float from environment variable.
    
    Args:
        key: Environment variable name
        default: Default value if not set (default: 0.0)
        
    Returns:
        Parsed float value or default
        
    Raises:
        ValueError: If the environment variable value cannot be parsed as float
        
    Example:
        timeout = get_env_float("REQUEST_TIMEOUT", 30.0)
        
    Requirement 2.1, 2.2, 2.3
    """
    value = os.getenv(key)
    if value is None:
        return default
    return float(value)


def get_env_bool(key: str, default: bool = False) -> bool:
    """
    Get boolean from environment variable.
    
    Accepts: "true", "false", "1", "0", "yes", "no" (case-insensitive)
    
    Args:
        key: Environment variable name
        default: Default value if not set (default: False)
        
    Returns:
        Parsed boolean value or default
        
    Example:
        debug = get_env_bool("DEBUG_MODE", False)
        enabled = get_env_bool("FEATURE_ENABLED", True)
        
    Requirement 2.1, 2.2, 2.3, 2.4
    """
    value = os.getenv(key)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes")


def get_env_list(
    key: str,
    default: Optional[List[str]] = None,
    separator: str = ","
) -> List[str]:
    """
    Get list from environment variable.
    
    Splits by separator and strips whitespace from each item.
    Empty items are filtered out.
    
    Args:
        key: Environment variable name
        default: Default value if not set (default: empty list)
        separator: Separator character (default: ",")
        
    Returns:
        List of strings or default
        
    Example:
        hosts = get_env_list("KAFKA_BROKERS", ["localhost:9092"])
        # With env KAFKA_BROKERS="host1:9092, host2:9092, host3:9092"
        # Returns: ["host1:9092", "host2:9092", "host3:9092"]
        
    Requirement 2.1, 2.2, 2.3, 2.5
    """
    if default is None:
        default = []
    
    value = os.getenv(key)
    if value is None or value.strip() == "":
        return default
    
    return [item.strip() for item in value.split(separator) if item.strip()]


def get_env_optional(key: str) -> Optional[str]:
    """
    Get optional string from environment variable.
    
    Returns None if the environment variable is not set,
    unlike get_env_str which returns an empty string by default.
    
    Args:
        key: Environment variable name
        
    Returns:
        Environment variable value or None if not set
        
    Example:
        api_key = get_env_optional("API_KEY")
        if api_key is None:
            raise ValueError("API_KEY is required")
            
    Requirement 2.1, 2.2
    """
    return os.getenv(key)


# ============================================================================
# CONFIGURATION CLASSES
# ============================================================================


@dataclass
class KafkaConfig:
    """Kafka connection configuration."""
    
    bootstrap_servers: str = "localhost:9092"
    
    # Topics
    topic_raw_trades: str = "raw_trades"
    topic_raw_klines: str = "raw_klines"
    topic_raw_tickers: str = "raw_tickers"
    topic_processed_aggregations: str = "processed_aggregations"
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
            bootstrap_servers=get_env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic_raw_trades=get_env_str("TOPIC_RAW_TRADES", "raw_trades"),
            topic_raw_klines=get_env_str("TOPIC_RAW_KLINES", "raw_klines"),
            topic_raw_tickers=get_env_str("TOPIC_RAW_TICKERS", "raw_tickers"),
            topic_processed_aggregations=get_env_str("TOPIC_PROCESSED_AGGREGATIONS", "processed_aggregations"),
            topic_alerts=get_env_str("TOPIC_ALERTS", "alerts"),
            max_rate_per_partition=get_env_int("KAFKA_MAX_RATE_PER_PARTITION", 50000),
            starting_offsets=get_env_str("KAFKA_STARTING_OFFSETS", "latest"),
            compression_type=get_env_str("KAFKA_COMPRESSION_TYPE", "snappy"),
            enable_idempotence=get_env_bool("KAFKA_ENABLE_IDEMPOTENCE", True),
            acks=get_env_str("KAFKA_ACKS", "all"),
            max_in_flight_requests_per_connection=get_env_int("KAFKA_MAX_IN_FLIGHT_REQUESTS", 5),
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
    # Increased from 10 to 50 to prevent state file deletion during job restarts
    # This ensures state files are retained across multiple DAG runs
    state_store_min_batches_to_retain: int = 50
    state_store_maintenance_interval: str = "60s"
    
    # Backpressure
    backpressure_enabled: bool = True
    
    @classmethod
    def from_env(cls, job_name: str) -> "SparkConfig":
        """Create configuration from environment variables."""
        # Use persistent checkpoint location (not /tmp which can be cleared)
        default_checkpoint = f"/opt/airflow/data/spark-checkpoints/{job_name}"
        
        return cls(
            app_name=get_env_str("SPARK_APP_NAME", job_name),
            executor_memory=get_env_str("SPARK_EXECUTOR_MEMORY", "512m"),
            driver_memory=get_env_str("SPARK_DRIVER_MEMORY", "512m"),
            executor_cores=get_env_int("SPARK_EXECUTOR_CORES", 1),
            shuffle_partitions=get_env_int("SPARK_SHUFFLE_PARTITIONS", 2),
            checkpoint_location=get_env_str("SPARK_CHECKPOINT_LOCATION", default_checkpoint),
            checkpoint_interval=get_env_str("SPARK_CHECKPOINT_INTERVAL", "30 seconds"),
            state_store_min_batches_to_retain=get_env_int("SPARK_STATE_STORE_MIN_BATCHES", 50),
            state_store_maintenance_interval=get_env_str("SPARK_STATE_STORE_MAINTENANCE_INTERVAL", "30s"),
            backpressure_enabled=get_env_bool("SPARK_BACKPRESSURE_ENABLED", True),
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
            host=get_env_str("REDIS_HOST", "localhost"),
            port=get_env_int("REDIS_PORT", 6379),
            db=get_env_int("REDIS_DB", 0),
            password=get_env_optional("REDIS_PASSWORD"),
            max_connections=get_env_int("REDIS_MAX_CONNECTIONS", 10),
            socket_timeout=get_env_int("REDIS_SOCKET_TIMEOUT", 5),
            socket_connect_timeout=get_env_int("REDIS_SOCKET_CONNECT_TIMEOUT", 5),
            candle_ttl_seconds=get_env_int("REDIS_CANDLE_TTL_SECONDS", 3600),
            indicator_ttl_seconds=get_env_int("REDIS_INDICATOR_TTL_SECONDS", 3600),
            alert_list_max_size=get_env_int("REDIS_ALERT_LIST_MAX_SIZE", 1000),
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
            host=get_env_str("POSTGRES_HOST", "localhost"),
            port=get_env_int("POSTGRES_PORT", 5432),
            user=get_env_str("POSTGRES_USER", "crypto"),
            password=get_env_str("POSTGRES_PASSWORD", "crypto"),
            database=get_env_str("POSTGRES_DB", "crypto_data"),
            max_connections=get_env_int("POSTGRES_MAX_CONNECTIONS", 10),
            max_retries=get_env_int("POSTGRES_MAX_RETRIES", 3),
            retry_delay=get_env_float("POSTGRES_RETRY_DELAY", 1.0),
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
            endpoint=get_env_str("MINIO_ENDPOINT", "localhost:9000"),
            access_key=get_env_str("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=get_env_str("MINIO_SECRET_KEY", "minioadmin"),
            bucket=get_env_str("MINIO_BUCKET", "crypto-data"),
            secure=get_env_bool("MINIO_SECURE", False),
            max_retries=get_env_int("MINIO_MAX_RETRIES", 3),
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
    
    @classmethod
    def from_env(cls, job_name: str) -> "Config":
        """Create complete configuration from environment variables."""
        return cls(
            kafka=KafkaConfig.from_env(),
            spark=SparkConfig.from_env(job_name),
            redis=RedisConfig.from_env(),
            postgres=PostgresConfig.from_env(),
            minio=MinioConfig.from_env(),
            log_level=get_env_str("LOG_LEVEL", "INFO"),
        )
