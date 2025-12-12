"""
Core module for PySpark Streaming Processor.

Contains configuration classes and connector utilities for Kafka, Spark, Redis,
PostgreSQL, and MinIO connections.

Table of Contents:
- Configuration Classes (line ~30)
  - KafkaConfig
  - SparkConfig
  - RedisConfig
  - PostgresConfig
  - MinioConfig
  - Config (main container)
- Connectors (line ~250)
  - KafkaConnector
  - RedisConnector
"""

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Use centralized config utilities from utils module
from src.utils.config import (
    get_env_str,
    get_env_int,
    get_env_float,
    get_env_bool,
    get_env_optional,
)


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
            bootstrap_servers=get_env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic_raw_trades=get_env_str("TOPIC_RAW_TRADES", "raw_trades"),
            topic_raw_klines=get_env_str("TOPIC_RAW_KLINES", "raw_klines"),
            topic_raw_tickers=get_env_str("TOPIC_RAW_TICKERS", "raw_tickers"),
            topic_processed_aggregations=get_env_str("TOPIC_PROCESSED_AGGREGATIONS", "processed_aggregations"),
            topic_processed_indicators=get_env_str("TOPIC_PROCESSED_INDICATORS", "processed_indicators"),
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
            app_name=get_env_str("SPARK_APP_NAME", job_name),
            executor_memory=get_env_str("SPARK_EXECUTOR_MEMORY", "512m"),
            driver_memory=get_env_str("SPARK_DRIVER_MEMORY", "512m"),
            executor_cores=get_env_int("SPARK_EXECUTOR_CORES", 1),
            shuffle_partitions=get_env_int("SPARK_SHUFFLE_PARTITIONS", 2),
            checkpoint_location=get_env_str("SPARK_CHECKPOINT_LOCATION", default_checkpoint),
            checkpoint_interval=get_env_str("SPARK_CHECKPOINT_INTERVAL", "30 seconds"),
            state_store_min_batches_to_retain=get_env_int("SPARK_STATE_STORE_MIN_BATCHES", 10),
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


# ============================================================================
# CONNECTORS
# ============================================================================


class KafkaConnector:
    """
    Kafka connector for writing data to Kafka topics.
    
    Note: For Spark Structured Streaming, Kafka writes are typically done
    through the DataFrame API. This class is for non-Spark Kafka operations.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        client_id: Optional[str] = None
    ):
        """
        Initialize Kafka connector.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            client_id: Optional client ID
        """
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self._producer = None
    
    @property
    def producer(self):
        """Get Kafka producer, creating if needed."""
        if self._producer is None:
            from kafka import KafkaProducer
            
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
        
        return self._producer
    
    def send(self, topic: str, value: Dict[str, Any], key: Optional[str] = None) -> None:
        """
        Send message to Kafka topic.
        
        Args:
            topic: Kafka topic name
            value: Message value (dictionary)
            key: Optional message key
        """
        self.producer.send(topic, value=value, key=key)
        self.producer.flush()
    
    def close(self) -> None:
        """Close Kafka producer."""
        if self._producer:
            self._producer.close()
            self._producer = None


class RedisConnector:
    """
    Redis connector for writing data to Redis.
    
    Supports writing to hash, list, and string data types with TTL support.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None
    ):
        """
        Initialize Redis connector.
        
        Args:
            host: Redis host
            port: Redis port
            db: Redis database number
            password: Optional Redis password
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self._client = None
        self._pool = None
    
    @property
    def client(self):
        """Get Redis client, creating connection if needed."""
        if self._client is None:
            import redis
            
            self._pool = redis.ConnectionPool(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password
            )
            self._client = redis.Redis(connection_pool=self._pool)
            # Test connection
            self._client.ping()
        
        return self._client
    
    def write_to_redis(
        self,
        key: str,
        value: Dict[str, Any],
        ttl_seconds: Optional[int] = None,
        value_type: str = "hash",
        list_max_size: Optional[int] = None
    ) -> None:
        """
        Write data to Redis.
        
        Args:
            key: Redis key
            value: Data to write (dictionary)
            ttl_seconds: Optional TTL in seconds
            value_type: Type of Redis data structure ("hash", "list", "string")
            list_max_size: Maximum list size (for list type only)
        """
        if value_type == "hash":
            self.client.hset(key, mapping=value)
            if ttl_seconds:
                self.client.expire(key, ttl_seconds)
        
        elif value_type == "list":
            self.client.lpush(key, json.dumps(value))
            if list_max_size:
                self.client.ltrim(key, 0, list_max_size - 1)
        
        elif value_type == "string":
            self.client.set(key, json.dumps(value))
            if ttl_seconds:
                self.client.expire(key, ttl_seconds)
        
        else:
            raise ValueError(f"Unsupported value_type: {value_type}")
    
    def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            self._client.close()
            self._client = None
        if self._pool:
            self._pool.disconnect()
            self._pool = None
