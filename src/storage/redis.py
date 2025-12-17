"""
Redis storage module for hot path data access.

Combines RedisStorage and RedisTickerStorage for real-time data access.
Provides sub-10ms latency access to latest market data using Redis
data structures (hashes, sorted sets, lists).

Table of Contents:
- RedisStorage (line ~50)
- RedisTickerStorage (line ~350)
- Health Check Functions (line ~600)
"""

import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import redis
from redis.exceptions import ConnectionError, TimeoutError

from src.utils.logging import get_logger

logger = get_logger(__name__)


# ============================================================================
# REDIS STORAGE - Hot Path
# ============================================================================

class RedisStorage:
    """Redis storage for real-time data access.
    
    Implements hot path storage with:
    - Hash structures for latest prices, tickers, indicators, aggregations
    - Sorted sets for recent trades (max 1000, 1 hour TTL)
    - Lists for alerts (max 1000, 24 hour TTL)
    """
    
    # TTL constants in seconds
    TTL_1_HOUR = 3600
    TTL_24_HOURS = 86400
    
    # Collection size limits
    MAX_TRADES = 1000
    MAX_ALERTS = 1000
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """Initialize Redis connection with retry logic.
        
        Args:
            host: Redis server hostname
            port: Redis server port
            db: Redis database number
            max_retries: Maximum connection retry attempts
            retry_delay: Base delay between retries (exponential backoff)
        """
        self.host = host
        self.port = port
        self.db = db
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._client: Optional[redis.Redis] = None
        self._connect()

    def _connect(self) -> None:
        """Establish Redis connection with exponential backoff retry."""
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                self._client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    decode_responses=True,
                    socket_timeout=5.0,
                    socket_connect_timeout=5.0,
                )
                # Test connection
                self._client.ping()
                logger.info(f"Connected to Redis at {self.host}:{self.port}")
                return
            except (ConnectionError, TimeoutError) as e:
                last_error = e
                delay = self.retry_delay * (2 ** attempt)
                logger.warning(
                    f"Redis connection attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                    f"Retrying in {delay}s..."
                )
                if attempt < self.max_retries - 1:
                    time.sleep(delay)
        
        raise ConnectionError(
            f"Failed to connect to Redis after {self.max_retries} attempts: {last_error}"
        )
    
    @property
    def client(self) -> redis.Redis:
        """Get Redis client, reconnecting if necessary."""
        if self._client is None:
            self._connect()
        return self._client
    
    def ping(self) -> bool:
        """Check if Redis connection is alive."""
        try:
            return self.client.ping()
        except (ConnectionError, TimeoutError):
            return False

    # =========================================================================
    # Hash Operations - Latest Price, Ticker, Indicators, Aggregations
    # =========================================================================
    
    def get_latest_price(self, symbol: str) -> Optional[dict]:
        """Get latest price from Redis hash.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Dict with price, volume, timestamp or None if not found
        """
        key = f"latest_price:{symbol}"
        data = self.client.hgetall(key)
        if not data:
            return None
        return {
            "price": float(data["price"]),
            "volume": float(data["volume"]),
            "timestamp": int(data["timestamp"]),
        }
    
    def get_latest_ticker(self, symbol: str) -> Optional[dict]:
        """Get latest ticker from Redis hash.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Dict with ticker stats or None if not found
        """
        key = f"latest_ticker:{symbol}"
        data = self.client.hgetall(key)
        if not data:
            return None
        # Convert numeric fields back to floats
        return {k: float(v) for k, v in data.items()}

    def write_latest_price(self, symbol: str, price: float, volume: float, timestamp: int) -> None:
        """Write latest price to Redis hash with TTL.
        
        Args:
            symbol: Trading pair symbol
            price: Current price
            volume: Trading volume
            timestamp: Unix timestamp in milliseconds
        """
        key = f"latest_price:{symbol}"
        mapping = {
            "price": str(price),
            "volume": str(volume),
            "timestamp": str(timestamp),
        }
        pipe = self.client.pipeline()
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, self.TTL_1_HOUR)
        pipe.execute()

    def write_latest_ticker(self, symbol: str, stats: dict) -> None:
        """Write latest ticker stats to Redis hash with TTL.
        
        Args:
            symbol: Trading pair symbol
            stats: Dict with ticker statistics (open, high, low, close, volume, etc.)
        """
        key = f"latest_ticker:{symbol}"
        # Convert all values to strings for Redis
        mapping = {k: str(v) for k, v in stats.items()}
        pipe = self.client.pipeline()
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, self.TTL_1_HOUR)
        pipe.execute()

    def write_recent_trade(self, symbol: str, trade: dict) -> None:
        """Write a trade to Redis sorted set with TTL.
        
        Args:
            symbol: Trading pair symbol
            trade: Dict with trade data (price, quantity, timestamp, is_buyer_maker)
        """
        key = f"recent_trades:{symbol}"
        trade_json = json.dumps(trade)
        timestamp = trade.get("timestamp", int(time.time() * 1000))
        
        pipe = self.client.pipeline()
        # Add to sorted set with timestamp as score
        pipe.zadd(key, {trade_json: timestamp})
        # Trim to keep only the most recent trades
        pipe.zremrangebyrank(key, 0, -(self.MAX_TRADES + 1))
        # Set TTL
        pipe.expire(key, self.TTL_1_HOUR)
        pipe.execute()

    def write_indicators(self, symbol: str, indicators: dict) -> None:
        """Write technical indicators to Redis hash.
        
        Key: indicators:{symbol}
        Fields: rsi, macd, macd_signal, sma_20, ema_12, ema_26, bb_upper, bb_lower, atr
        
        Args:
            symbol: Trading pair symbol
            indicators: Dict with indicator values
        """
        key = f"indicators:{symbol}"
        # Convert all values to strings for Redis
        mapping = {k: str(v) for k, v in indicators.items()}
        self.client.hset(key, mapping=mapping)
    
    def get_indicators(self, symbol: str) -> Optional[dict]:
        """Get technical indicators from Redis hash.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Dict with indicator values or None if not found
        """
        key = f"indicators:{symbol}"
        data = self.client.hgetall(key)
        if not data:
            return None
        # Convert numeric fields back to floats
        return {k: float(v) for k, v in data.items()}
    
    def write_aggregation(
        self, symbol: str, interval: str, ohlcv: dict
    ) -> None:
        """Write OHLCV aggregation to Redis hash with TTL.
        
        Key: market:{symbol}:{interval}
        Fields: price, open, high, low, close, volume, timestamp
        TTL: 1 hour
        
        Also writes to market:{symbol} (without interval) for Grafana dashboard.
        
        Args:
            symbol: Trading pair symbol
            interval: Time interval (e.g., '1m', '5m', '1h')
            ohlcv: Dict with OHLCV data
        """
        pipe = self.client.pipeline()
        
        # Write to market:{symbol}:{interval}
        key = f"market:{symbol}:{interval}"
        mapping = {
            "price": str(ohlcv.get("close", 0)),
            "open": str(ohlcv.get("open", 0)),
            "high": str(ohlcv.get("high", 0)),
            "low": str(ohlcv.get("low", 0)),
            "close": str(ohlcv.get("close", 0)),
            "volume": str(ohlcv.get("volume", 0)),
            "timestamp": str(ohlcv.get("timestamp", "")),
        }
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, self.TTL_1_HOUR)
        
        # Write to market:{symbol} for Grafana dashboard (only for 1m interval)
        if interval == "1m":
            market_key = f"market:{symbol}"
            pipe.hset(market_key, mapping=mapping)
            pipe.expire(market_key, self.TTL_1_HOUR)
            
            # Update total volume
            if ohlcv.get("volume"):
                pipe.incrbyfloat("market:total_volume", float(ohlcv.get("volume", 0)))
                pipe.expire("market:total_volume", self.TTL_1_HOUR)
                
                # Update accumulated volumes hash for bar chart (per symbol)
                pipe.hincrbyfloat("market:volumes", symbol, float(ohlcv.get("volume", 0)))
                pipe.expire("market:volumes", self.TTL_1_HOUR)
        
        pipe.execute()
    
    def get_aggregation(self, symbol: str, interval: str) -> Optional[dict]:
        """Get OHLCV aggregation from Redis hash.
        
        Args:
            symbol: Trading pair symbol
            interval: Time interval
            
        Returns:
            Dict with OHLCV data or None if not found
        """
        key = f"market:{symbol}:{interval}"
        data = self.client.hgetall(key)
        if not data:
            return None
        return data

    def write_aggregations_batch(
        self, records: List[Dict[str, Any]]
    ) -> tuple[int, List[Dict[str, Any]]]:
        """Write multiple aggregations using Redis pipeline.
        
        Uses Redis pipeline to batch all HSET operations for improved performance.
        Each record should contain: symbol, interval, and OHLCV data.
        
        Args:
            records: List of aggregation records, each containing:
                - symbol: Trading pair symbol
                - interval: Time interval (e.g., '1m', '5m', '1h')
                - open, high, low, close, volume: OHLCV data
                - timestamp: Unix timestamp
                
        Returns:
            Tuple of (success_count, failed_records)
            
        Requirements: 1.2, 3.1, 3.3
        Property 3: Redis pipeline atomicity
        """
        if not records:
            return 0, []
        
        failed_records: List[Dict[str, Any]] = []
        success_count = 0
        
        try:
            pipe = self.client.pipeline()
            valid_records: List[Dict[str, Any]] = []
            
            for record in records:
                try:
                    symbol = record.get("symbol")
                    interval = record.get("interval", "1m")
                    
                    if not symbol:
                        failed_records.append(record)
                        continue
                    
                    # Build key and mapping
                    key = f"market:{symbol}:{interval}"
                    mapping = {
                        "price": str(record.get("close", 0)),
                        "open": str(record.get("open", 0)),
                        "high": str(record.get("high", 0)),
                        "low": str(record.get("low", 0)),
                        "close": str(record.get("close", 0)),
                        "volume": str(record.get("volume", 0)),
                        "timestamp": str(record.get("timestamp", "")),
                    }
                    
                    pipe.hset(key, mapping=mapping)
                    pipe.expire(key, self.TTL_1_HOUR)
                    
                    # Also write to market:{symbol} for Grafana (1m interval only)
                    if interval == "1m":
                        market_key = f"market:{symbol}"
                        pipe.hset(market_key, mapping=mapping)
                        pipe.expire(market_key, self.TTL_1_HOUR)
                    
                    valid_records.append(record)
                    
                except Exception as e:
                    logger.warning(f"Failed to prepare record for pipeline: {e}")
                    failed_records.append(record)
            
            # Execute pipeline
            if valid_records:
                pipe.execute()
                success_count = len(valid_records)
                logger.debug(f"Batch wrote {success_count} aggregations to Redis")
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Redis pipeline execution failed: {e}")
            # All records failed
            failed_records = records
            success_count = 0
        except Exception as e:
            logger.error(f"Unexpected error in batch write: {e}")
            failed_records = records
            success_count = 0
        
        return success_count, failed_records

    # =========================================================================
    # Collection Operations - Recent Trades, Alerts
    # =========================================================================
    
    def get_recent_trades(self, symbol: str, limit: int = 100) -> list:
        """Get recent trades from Redis sorted set.
        
        Args:
            symbol: Trading pair symbol
            limit: Maximum number of trades to return
            
        Returns:
            List of trade dicts, most recent first
        """
        key = f"recent_trades:{symbol}"
        # Get trades in reverse order (most recent first)
        trades_json = self.client.zrevrange(key, 0, limit - 1)
        return [json.loads(t) for t in trades_json]

    def write_alert(self, alert: dict, max_alerts: int = MAX_ALERTS) -> None:
        """Write alert to Redis list with TTL.
        
        Key: alerts:recent
        TTL: 24 hours
        Max size: 1000 alerts
        
        Args:
            alert: Dict with alert data
            max_alerts: Maximum number of alerts to keep
        """
        key = "alerts:recent"
        alert_json = json.dumps(alert)
        
        pipe = self.client.pipeline()
        # Push to front of list
        pipe.lpush(key, alert_json)
        # Trim to keep only the most recent alerts
        pipe.ltrim(key, 0, max_alerts - 1)
        # Set TTL
        pipe.expire(key, self.TTL_24_HOURS)
        pipe.execute()
    
    def get_recent_alerts(self, limit: int = 100) -> list:
        """Get recent alerts from Redis list.
        
        Args:
            limit: Maximum number of alerts to return
            
        Returns:
            List of alert dicts, most recent first
        """
        key = "alerts:recent"
        alerts_json = self.client.lrange(key, 0, limit - 1)
        return [json.loads(a) for a in alerts_json]

    def write_alerts_batch(
        self, alerts: List[Dict[str, Any]], max_alerts: int = MAX_ALERTS
    ) -> tuple[int, List[Dict[str, Any]]]:
        """Write multiple alerts using Redis pipeline.
        
        Uses Redis pipeline to batch all LPUSH operations for improved performance.
        Maintains list size limit with LTRIM after all pushes.
        
        Args:
            alerts: List of alert dictionaries to write
            max_alerts: Maximum number of alerts to keep in the list
                
        Returns:
            Tuple of (success_count, failed_records)
            
        Requirements: 3.2, 3.3
        """
        if not alerts:
            return 0, []
        
        failed_records: List[Dict[str, Any]] = []
        success_count = 0
        key = "alerts:recent"
        
        try:
            pipe = self.client.pipeline()
            valid_alerts: List[Dict[str, Any]] = []
            
            for alert in alerts:
                try:
                    alert_json = json.dumps(alert)
                    pipe.lpush(key, alert_json)
                    valid_alerts.append(alert)
                except (TypeError, ValueError) as e:
                    logger.warning(f"Failed to serialize alert: {e}")
                    failed_records.append(alert)
            
            # Trim list to maintain size limit and set TTL
            if valid_alerts:
                pipe.ltrim(key, 0, max_alerts - 1)
                pipe.expire(key, self.TTL_24_HOURS)
                
                # Execute pipeline
                pipe.execute()
                success_count = len(valid_alerts)
                logger.debug(f"Batch wrote {success_count} alerts to Redis")
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Redis pipeline execution failed for alerts: {e}")
            failed_records = alerts
            success_count = 0
        except Exception as e:
            logger.error(f"Unexpected error in alerts batch write: {e}")
            failed_records = alerts
            success_count = 0
        
        return success_count, failed_records


# ============================================================================
# REDIS TICKER STORAGE
# ============================================================================

class RedisTickerStorage:
    """
    Redis storage for real-time ticker data.
    
    Implements hot path storage with:
    - Hash structures for ticker data (key: ticker:{symbol})
    - TTL of 60 seconds for auto-expiration
    - String serialization for numeric precision
    
    Properties validated:
    - Property 1: Redis key pattern consistency (ticker:{symbol})
    - Property 2: Redis Hash field completeness
    - Property 3: TTL is always set (1-60 seconds)
    - Property 4: Numeric precision preservation
    """
    
    # Default TTL in seconds
    DEFAULT_TTL_SECONDS = 60
    
    # Key prefix for ticker data
    KEY_PREFIX = "ticker"
    
    # Required fields in Redis Hash (Requirement 3.2)
    REQUIRED_FIELDS = {
        "last_price",
        "price_change",
        "price_change_pct",
        "open",
        "high",
        "low",
        "volume",
        "quote_volume",
        "trades_count",
        "updated_at",
    }
    
    # Field mapping from Binance format to storage format
    FIELD_MAPPING = {
        "c": "last_price",
        "p": "price_change",
        "P": "price_change_pct",
        "o": "open",
        "h": "high",
        "l": "low",
        "v": "volume",
        "q": "quote_volume",
        "n": "trades_count",
        "E": "event_time",
    }
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """
        Initialize Redis connection for ticker storage.
        
        Args:
            host: Redis server hostname
            port: Redis server port
            db: Redis database number
            ttl_seconds: TTL for ticker keys (default 60s, Requirement 3.3)
            max_retries: Maximum connection retry attempts
            retry_delay: Base delay between retries (exponential backoff)
        """
        self.host = host
        self.port = port
        self.db = db
        self.ttl_seconds = min(max(ttl_seconds, 1), 60)  # Clamp to 1-60 seconds
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._client: Optional[redis.Redis] = None
        self._connect()
    
    def _connect(self) -> None:
        """Establish Redis connection with exponential backoff retry."""
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                self._client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    decode_responses=True,
                    socket_timeout=5.0,
                    socket_connect_timeout=5.0,
                )
                # Test connection
                self._client.ping()
                logger.info(f"Connected to Redis at {self.host}:{self.port}")
                return
            except (ConnectionError, TimeoutError) as e:
                last_error = e
                delay = self.retry_delay * (2 ** attempt)
                logger.warning(
                    f"Redis connection attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                    f"Retrying in {delay}s..."
                )
                if attempt < self.max_retries - 1:
                    time.sleep(delay)
        
        raise ConnectionError(
            f"Failed to connect to Redis after {self.max_retries} attempts: {last_error}"
        )
    
    @property
    def client(self) -> redis.Redis:
        """Get Redis client, reconnecting if necessary."""
        if self._client is None:
            self._connect()
        return self._client
    
    def ping(self) -> bool:
        """Check if Redis connection is alive."""
        try:
            return self.client.ping()
        except (ConnectionError, TimeoutError):
            return False
    
    def _make_key(self, symbol: str) -> str:
        """
        Generate Redis key for a symbol.
        
        Key pattern: ticker:{symbol} (uppercase)
        
        Args:
            symbol: Trading pair symbol (e.g., BTCUSDT)
            
        Returns:
            Redis key string
            
        Property 1: Redis key pattern consistency
        """
        return f"{self.KEY_PREFIX}:{symbol.upper()}"
    
    def _transform_to_storage_format(self, data: Dict[str, Any]) -> Dict[str, str]:
        """
        Transform Binance ticker data to storage format.
        
        Converts field names and serializes all values as strings.
        
        Args:
            data: Raw ticker data from Binance
            
        Returns:
            Dictionary with storage field names and string values
            
        Property 4: Numeric precision preservation
        """
        result: Dict[str, str] = {}
        
        # Map Binance fields to storage fields
        for binance_field, storage_field in self.FIELD_MAPPING.items():
            if binance_field in data and data[binance_field] is not None:
                result[storage_field] = str(data[binance_field])
        
        # Add updated_at timestamp (current time in ms)
        result["updated_at"] = str(int(time.time() * 1000))
        
        return result
    
    def _transform_from_storage_format(self, data: Dict[str, str]) -> Dict[str, Any]:
        """Transform storage format back to API response format."""
        if not data:
            return {}
        
        result: Dict[str, Any] = {}
        
        # String fields (preserve precision)
        string_fields = {
            "last_price", "price_change", "price_change_pct",
            "open", "high", "low", "volume", "quote_volume"
        }
        
        for field in string_fields:
            if field in data:
                result[field] = data[field]
        
        # Integer fields - data already validated by DAG
        int_fields = {"trades_count", "updated_at", "event_time"}
        for field in int_fields:
            if field in data:
                result[field] = int(data[field])
        
        return result
    
    def write_ticker(self, symbol: str, data: Dict[str, Any]) -> bool:
        """
        Write ticker data to Redis Hash with TTL.
        
        Args:
            symbol: Trading pair symbol (e.g., BTCUSDT)
            data: Ticker data dictionary (Binance format)
            
        Returns:
            True if write was successful, False otherwise
            
        Requirements: 3.1, 3.2, 3.3, 3.4
        Properties: 1, 2, 3, 4
        """
        try:
            key = self._make_key(symbol)
            storage_data = self._transform_to_storage_format(data)
            
            # Use pipeline for atomic operation
            pipe = self.client.pipeline()
            pipe.hset(key, mapping=storage_data)
            pipe.expire(key, self.ttl_seconds)
            pipe.execute()
            
            logger.debug(f"Wrote ticker data for {symbol} with TTL {self.ttl_seconds}s")
            return True
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to write ticker for {symbol}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error writing ticker for {symbol}: {e}")
            return False
    
    def get_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get ticker data for a symbol.
        
        Args:
            symbol: Trading pair symbol (e.g., BTCUSDT)
            
        Returns:
            Ticker data dictionary or None if not found
            
        Requirement: 4.1
        Property 5: API returns matching data
        """
        try:
            key = self._make_key(symbol)
            data = self.client.hgetall(key)
            
            if not data:
                return None
            
            result = self._transform_from_storage_format(data)
            result["symbol"] = symbol.upper()
            
            return result
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to get ticker for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting ticker for {symbol}: {e}")
            return None
    
    def get_all_tickers(self) -> List[Dict[str, Any]]:
        """
        Get all ticker data.
        
        Returns:
            List of ticker data dictionaries
            
        Requirement: 4.2
        Property 6: All tickers returned
        """
        try:
            # Find all ticker keys
            pattern = f"{self.KEY_PREFIX}:*"
            keys = self.client.keys(pattern)
            
            if not keys:
                return []
            
            tickers = []
            for key in keys:
                # Extract symbol from key
                symbol = key.replace(f"{self.KEY_PREFIX}:", "")
                ticker = self.get_ticker(symbol)
                if ticker:
                    tickers.append(ticker)
            
            return tickers
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to get all tickers: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error getting all tickers: {e}")
            return []
    


# ============================================================================
# HEALTH CHECKS
# ============================================================================

# ============================================================================
# REDIS CONNECTOR - Simple connector for streaming jobs
# ============================================================================

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


# ============================================================================
# HEALTH CHECKS
# ============================================================================

def check_redis_health(
    host: str = "localhost",
    port: int = 6379,
    db: int = 0,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    **context
) -> Dict[str, Any]:
    """
    Check Redis connection health (Hot Path).
    
    Args:
        host: Redis host
        port: Redis port
        db: Redis database number
        max_retries: Maximum retry attempts
        retry_delay: Base delay between retries (exponential backoff)
        context: Optional Airflow context
        
    Returns:
        Dict with health check status
        
    Raises:
        Exception: If health check fails after all retries
    """
    last_error = None
    
    for attempt in range(max_retries):
        try:
            client = redis.Redis(
                host=host,
                port=port,
                db=db,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
            # Test connection
            client.ping()
            client.close()
            
            result = {
                'service': 'redis',
                'tier': 'hot',
                'status': 'healthy',
                'host': host,
                'port': port,
                'attempt': attempt + 1,
                'timestamp': datetime.now().isoformat()
            }
            logger.info(f"Redis health check passed: {host}:{port}")
            return result
            
        except (ConnectionError, TimeoutError, Exception) as e:
            last_error = e
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)
                logger.warning(
                    f"Redis health check failed (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {delay}s: {e}"
                )
                time.sleep(delay)
    
    raise Exception(f"Redis health check failed after {max_retries} attempts: {last_error}")
