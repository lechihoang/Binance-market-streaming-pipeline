"""
Connector utilities for PySpark Streaming Processor.

Provides RedisConnector, DuckDBConnector, and KafkaConnector
classes for writing data to various sinks.
"""

import json
from typing import Any, Dict, List, Optional


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


class DuckDBConnector:
    """
    DuckDB connector for writing data to DuckDB tables.
    
    Supports creating tables and writing data for candles, indicators, and alerts.
    """
    
    def __init__(self, database_path: str = ":memory:"):
        """
        Initialize DuckDB connector.
        
        Args:
            database_path: Path to DuckDB database file or ":memory:" for in-memory
        """
        import duckdb
        
        self.database_path = database_path
        self.connection = duckdb.connect(database_path)
    
    def create_candles_table(self) -> None:
        """Create candles table if not exists."""
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                window_duration VARCHAR,
                symbol VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                quote_volume DOUBLE,
                trade_count INTEGER,
                vwap DOUBLE,
                price_change_pct DOUBLE,
                buy_sell_ratio DOUBLE,
                large_order_count INTEGER,
                price_stddev DOUBLE
            )
        """)
    
    def create_indicators_table(self) -> None:
        """Create indicators table if not exists."""
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS indicators (
                timestamp TIMESTAMP,
                symbol VARCHAR,
                sma_5 DOUBLE,
                sma_10 DOUBLE,
                sma_20 DOUBLE,
                sma_50 DOUBLE,
                ema_12 DOUBLE,
                ema_26 DOUBLE,
                rsi_14 DOUBLE,
                macd_line DOUBLE,
                macd_signal DOUBLE,
                macd_histogram DOUBLE,
                bb_middle DOUBLE,
                bb_upper DOUBLE,
                bb_lower DOUBLE,
                atr_14 DOUBLE
            )
        """)
    
    def create_alerts_table(self) -> None:
        """Create alerts table if not exists."""
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                timestamp TIMESTAMP,
                symbol VARCHAR,
                alert_type VARCHAR,
                alert_level VARCHAR,
                details VARCHAR,
                alert_id VARCHAR,
                created_at TIMESTAMP
            )
        """)
    
    def write_to_duckdb(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        create_table: bool = True
    ) -> None:
        """
        Write data to DuckDB table.
        
        Args:
            table_name: Name of the table to write to
            data: List of dictionaries containing data to write
            create_table: Whether to create table if not exists
        """
        if not data:
            return
        
        if create_table:
            if table_name == "candles":
                self.create_candles_table()
            elif table_name == "indicators":
                self.create_indicators_table()
            elif table_name == "alerts":
                self.create_alerts_table()
        
        # Get columns from first record
        columns = list(data[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        column_names = ", ".join(columns)
        
        # Insert data
        for record in data:
            values = [record.get(col) for col in columns]
            self.connection.execute(
                f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})",
                values
            )
    
    def close(self) -> None:
        """Close DuckDB connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
