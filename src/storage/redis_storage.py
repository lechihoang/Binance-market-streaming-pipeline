"""
RedisStorage - Hot Path storage for real-time queries.

Provides sub-10ms latency access to latest market data using Redis
data structures (hashes, sorted sets, lists).
"""

import json
import logging
import time
from typing import Any, Optional

import redis
from redis.exceptions import ConnectionError, TimeoutError

logger = logging.getLogger(__name__)


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
    
    def write_latest_price(
        self, symbol: str, price: float, volume: float, timestamp: int
    ) -> None:
        """Write latest price to Redis hash.
        
        Key: latest_price:{symbol}
        Fields: price, volume, timestamp
        
        Args:
            symbol: Trading pair symbol (e.g., BTCUSDT)
            price: Current price
            volume: Current volume
            timestamp: Unix timestamp in milliseconds
        """
        key = f"latest_price:{symbol}"
        self.client.hset(key, mapping={
            "price": str(price),
            "volume": str(volume),
            "timestamp": str(timestamp),
        })
    
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
    
    def write_latest_ticker(self, symbol: str, stats: dict) -> None:
        """Write latest ticker (24h stats) to Redis hash.
        
        Key: latest_ticker:{symbol}
        Fields: open, high, low, close, volume, quote_volume
        
        Args:
            symbol: Trading pair symbol
            stats: Dict with 24h statistics
        """
        key = f"latest_ticker:{symbol}"
        # Convert all values to strings for Redis
        mapping = {k: str(v) for k, v in stats.items()}
        self.client.hset(key, mapping=mapping)
    
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

    # =========================================================================
    # Collection Operations - Recent Trades, Alerts
    # =========================================================================
    
    def write_recent_trade(
        self, symbol: str, trade: dict, max_trades: int = MAX_TRADES
    ) -> None:
        """Write trade to Redis sorted set with TTL.
        
        Key: recent_trades:{symbol}
        Score: timestamp
        Member: JSON-encoded trade data
        TTL: 1 hour
        Max size: 1000 trades
        
        Args:
            symbol: Trading pair symbol
            trade: Dict with trade data (must include 'timestamp')
            max_trades: Maximum number of trades to keep
        """
        key = f"recent_trades:{symbol}"
        timestamp = trade.get("timestamp", int(time.time() * 1000))
        trade_json = json.dumps(trade)
        
        pipe = self.client.pipeline()
        # Add trade with timestamp as score
        pipe.zadd(key, {trade_json: timestamp})
        # Trim to keep only the most recent trades
        pipe.zremrangebyrank(key, 0, -(max_trades + 1))
        # Set TTL
        pipe.expire(key, self.TTL_1_HOUR)
        pipe.execute()
    
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
    
    def get_trades_count(self, symbol: str) -> int:
        """Get count of trades in sorted set.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Number of trades stored
        """
        key = f"recent_trades:{symbol}"
        return self.client.zcard(key)
    
    def get_alerts_count(self) -> int:
        """Get count of alerts in list.
        
        Returns:
            Number of alerts stored
        """
        key = "alerts:recent"
        return self.client.llen(key)
    
    def flush_db(self) -> None:
        """Flush all keys in current database. Use with caution."""
        self.client.flushdb()
