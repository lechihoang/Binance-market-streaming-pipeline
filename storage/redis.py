"""Unified Redis Storage Module."""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import redis
from redis.exceptions import ConnectionError, TimeoutError

logger = logging.getLogger(__name__)


def check_redis_health(
    host: str = "localhost",
    port: int = 6379,
    db: int = 0,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> Dict[str, Any]:
    """Check Redis connection health with retry logic."""
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        try:
            client = redis.Redis(
                host=host,
                port=port,
                db=db,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            client.ping()
            client.close()
            
            return {
                "service": "redis",
                "tier": "hot",
                "status": "healthy",
                "host": host,
                "port": port,
                "attempt": attempt,
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                time.sleep(retry_delay)
    
    raise Exception(f"Redis health check failed after {max_retries} attempts: {last_error}")


class RedisStorage:
    PREFIX_AGGREGATION = "agg"
    PREFIX_TICKER = "ticker"
    PREFIX_TRADE = "trade"
    PREFIX_ALERT = "alert"
    PREFIX_PRICE = "price"
    
    DEFAULT_AGGREGATION_TTL = 3600
    DEFAULT_TICKER_TTL = 60
    DEFAULT_TRADE_TTL = 300
    DEFAULT_ALERT_TTL = 86400
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        ttl_seconds: Optional[int] = None,
        ticker_ttl_seconds: Optional[int] = None,
    ):
        """Initialize Redis storage."""
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # Support both ttl_seconds (backward compat) and ticker_ttl_seconds
        self.ticker_ttl_seconds = ticker_ttl_seconds or ttl_seconds or self.DEFAULT_TICKER_TTL
        
        self._client: Optional[redis.Redis] = None
        self._connect()
    
    def _connect(self) -> None:
        """Connect to Redis with exponential backoff retry logic."""
        last_error = None
        delay = self.retry_delay
        
        for attempt in range(1, self.max_retries + 1):
            try:
                self._client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    password=self.password,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    decode_responses=True,
                )
                self._client.ping()
                logger.info(f"Connected to Redis at {self.host}:{self.port}/{self.db}")
                return
            except (ConnectionError, TimeoutError) as e:
                last_error = e
                logger.warning(f"Redis connection attempt {attempt} failed: {e}")
                if attempt < self.max_retries:
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
        
        raise ConnectionError(f"Failed to connect to Redis after {self.max_retries} attempts: {last_error}")
    
    @property
    def client(self) -> redis.Redis:
        """Get Redis client, reconnecting if necessary."""
        if self._client is None:
            self._connect()
        try:
            self._client.ping()
        except (ConnectionError, TimeoutError):
            logger.warning("Redis connection lost, reconnecting...")
            self._connect()
        return self._client
    
    def ping(self) -> bool:
        """Check if Redis is reachable."""
        try:
            return self.client.ping()
        except Exception:
            return False
    
    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
            logger.info("Redis connection closed")

    def _make_aggregation_key(self, symbol: str, interval: str = "1m") -> str:
        """Create Redis key for aggregation data."""
        return f"{self.PREFIX_AGGREGATION}:{symbol}:{interval}"
    
    def write_aggregation(self, symbol: str, interval: str, data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Write aggregation (OHLCV) data to Redis."""
        try:
            key = self._make_aggregation_key(symbol, interval)
            # Convert all values to strings for Redis hash
            hash_data = {k: str(v) if v is not None else "" for k, v in data.items()}
            self.client.hset(key, mapping=hash_data)
            
            if ttl is None:
                ttl = self.DEFAULT_AGGREGATION_TTL
            self.client.expire(key, ttl)
            
            return True
        except Exception as e:
            logger.error(f"Failed to write aggregation for {symbol}: {e}")
            return False
    
    def get_aggregation(self, symbol: str, interval: str = "1m") -> Optional[Dict[str, Any]]:
        """Get aggregation data from Redis."""
        try:
            key = self._make_aggregation_key(symbol, interval)
            data = self.client.hgetall(key)
            
            if not data:
                return None
            
            # Convert numeric strings back to floats/ints
            result = {}
            for k, v in data.items():
                if v == "":
                    result[k] = None
                elif k in ("trade_count", "buy_count", "sell_count"):
                    result[k] = int(float(v)) if v else 0
                else:
                    try:
                        result[k] = float(v)
                    except ValueError:
                        result[k] = v
            
            return result
        except Exception as e:
            logger.error(f"Failed to get aggregation for {symbol}: {e}")
            return None
    
    def write_aggregations_batch(self, aggregations: List[Dict[str, Any]], ttl: Optional[int] = None) -> int:
        """Write multiple aggregations in a batch."""
        success_count = 0
        pipe = self.client.pipeline()
        
        try:
            for agg in aggregations:
                symbol = agg.get("symbol")
                interval = agg.get("interval", "1m")
                if not symbol:
                    continue
                
                key = self._make_aggregation_key(symbol, interval)
                hash_data = {k: str(v) if v is not None else "" for k, v in agg.items()}
                pipe.hset(key, mapping=hash_data)
                pipe.expire(key, ttl or self.DEFAULT_AGGREGATION_TTL)
                success_count += 1
            
            pipe.execute()
            return success_count
        except Exception as e:
            logger.error(f"Failed to write aggregations batch: {e}")
            return 0

    def get_aggregations_multi(
        self, 
        symbol: str, 
        interval: str = "5m"
    ) -> List[Dict[str, Any]]:
        """Get recent 1m aggregations and combine into higher timeframe.
        
        Retrieves recent 1-minute aggregation data for a symbol and aggregates
        it into higher timeframe candles (5m, 15m) using in-memory Python logic.
        
        Note: Redis only stores the latest candle per symbol/interval in this
        implementation, so this returns at most one aggregated candle from
        recent data when multiple 1m candles are available.
        """
        # For 1m interval, just return the single aggregation if it exists
        if interval == "1m":
            agg = self.get_aggregation(symbol, "1m")
            return [agg] if agg else []
        
        # Validate interval
        valid_intervals = {"5m", "15m"}
        if interval not in valid_intervals:
            logger.warning(f"Invalid interval '{interval}', returning empty list")
            return []
        
        # Get the 1m aggregation data
        # In this implementation, Redis stores only the latest candle per symbol
        # For proper multi-candle aggregation, we would need historical 1m data
        agg_1m = self.get_aggregation(symbol, "1m")
        
        if not agg_1m:
            return []
        
        # Since Redis only stores the latest 1m candle, we return it as-is
        # but with the requested interval. In a production system with
        # time-series data in Redis, we would aggregate multiple 1m candles.
        # 
        # The aggregation logic follows OHLCV rules:
        # - open: first candle's open
        # - high: max of all highs
        # - low: min of all lows  
        # - close: last candle's close
        # - volume: sum of all volumes
        # - quote_volume: sum of all quote_volumes
        # - trades_count: sum of all trades_counts
        
        # For now, return the single candle with the requested interval
        # This is consistent with the design doc note that Redis returns
        # "at most one aggregated candle from recent data"
        result = dict(agg_1m)
        result["interval"] = interval
        
        return [result]

    def get_aggregations_multi_from_list(
        self,
        candles: List[Dict[str, Any]],
        interval: str = "5m"
    ) -> List[Dict[str, Any]]:
        """Aggregate a list of 1m candles into higher timeframe candles.
        
        This is a utility method that performs in-memory aggregation of
        1-minute candles into higher timeframe candles (5m, 15m).
        Window boundaries align to clock time.
        """
        if not candles or interval == "1m":
            return candles if candles else []
        
        # Validate interval
        interval_minutes = {"5m": 5, "15m": 15}.get(interval)
        if interval_minutes is None:
            logger.warning(f"Invalid interval '{interval}', returning original candles")
            return candles
        
        # Sort candles by timestamp
        sorted_candles = sorted(candles, key=lambda x: x.get("timestamp", 0))
        
        # Group candles by aggregation window
        windows: Dict[datetime, List[Dict[str, Any]]] = {}
        
        for candle in sorted_candles:
            ts = candle.get("timestamp")
            if ts is None:
                continue
            
            # Convert timestamp to datetime if needed
            if isinstance(ts, (int, float)):
                # Assume milliseconds timestamp
                ts = datetime.fromtimestamp(ts / 1000)
            elif isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                except ValueError:
                    continue
            
            # Calculate window start (aligned to clock time)
            # e.g., for 5m: 10:03 -> 10:00, 10:07 -> 10:05
            minute = ts.minute
            window_minute = (minute // interval_minutes) * interval_minutes
            window_start = ts.replace(minute=window_minute, second=0, microsecond=0)
            
            if window_start not in windows:
                windows[window_start] = []
            windows[window_start].append(candle)
        
        # Aggregate each window
        aggregated = []
        for window_start, window_candles in sorted(windows.items()):
            if not window_candles:
                continue
            
            # Sort by timestamp within window
            window_candles.sort(key=lambda x: x.get("timestamp", 0))
            
            # OHLCV aggregation rules:
            # - open: first candle's open
            # - high: max of all highs
            # - low: min of all lows
            # - close: last candle's close
            # - volume/quote_volume/trade_count: sum
            agg_candle = {
                "timestamp": window_start,
                "symbol": window_candles[0].get("symbol", ""),
                "interval": interval,
                "open": window_candles[0].get("open", 0),
                "high": max(c.get("high", 0) for c in window_candles),
                "low": min(c.get("low", float("inf")) for c in window_candles),
                "close": window_candles[-1].get("close", 0),
                "volume": sum(c.get("volume", 0) for c in window_candles),
                "quote_volume": sum(c.get("quote_volume", 0) for c in window_candles),
                "trade_count": sum(c.get("trade_count", 0) for c in window_candles),
            }
            
            # Handle edge case where all lows were 0 or missing
            if agg_candle["low"] == float("inf"):
                agg_candle["low"] = 0
            
            aggregated.append(agg_candle)
        
        return aggregated
    
    def _make_price_key(self, symbol: str) -> str:
        """Create Redis key for latest price."""
        return f"{self.PREFIX_PRICE}:{symbol}"
    
    def write_latest_price(self, symbol: str, price: float, timestamp: Optional[int] = None) -> bool:
        """Write latest price for a symbol."""
        try:
            key = self._make_price_key(symbol)
            data = {
                "price": str(price),
                "timestamp": str(timestamp or int(time.time() * 1000)),
            }
            self.client.hset(key, mapping=data)
            self.client.expire(key, self.DEFAULT_AGGREGATION_TTL)
            return True
        except Exception as e:
            logger.error(f"Failed to write latest price for {symbol}: {e}")
            return False
    
    def get_latest_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get latest price for a symbol."""
        try:
            key = self._make_price_key(symbol)
            data = self.client.hgetall(key)
            if not data:
                return None
            return {
                "price": float(data.get("price", 0)),
                "timestamp": int(data.get("timestamp", 0)),
            }
        except Exception as e:
            logger.error(f"Failed to get latest price for {symbol}: {e}")
            return None
    
    def write_latest_ticker(self, symbol: str, data: Dict[str, Any]) -> bool:
        """Write latest ticker data (alias for write_ticker for backward compatibility)."""
        return self.write_ticker(symbol, data)
    
    def get_latest_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get latest ticker data (alias for get_ticker for backward compatibility)."""
        return self.get_ticker(symbol)

    def _make_ticker_key(self, symbol: str) -> str:
        """Create Redis key for ticker data."""
        return f"{self.PREFIX_TICKER}:{symbol.upper()}"
    
    def _transform_ticker_to_storage(self, symbol: str, data: Dict[str, Any]) -> Dict[str, str]:
        """
        Transform ticker data for Redis storage.
        
        Maps Binance ticker fields to storage format:
        - s: symbol
        - c: last_price (close)
        - p: price_change
        - P: price_change_pct
        - o: open
        - h: high
        - l: low
        - v: volume
        - q: quote_volume
        - n: trade_count (if available)
        - E: event_time -> updated_at
        """
        return {
            "symbol": symbol.upper(),
            "last_price": str(data.get("c", data.get("last_price", "0"))),
            "price_change": str(data.get("p", data.get("price_change", "0"))),
            "price_change_pct": str(data.get("P", data.get("price_change_pct", "0"))),
            "open": str(data.get("o", data.get("open", "0"))),
            "high": str(data.get("h", data.get("high", "0"))),
            "low": str(data.get("l", data.get("low", "0"))),
            "volume": str(data.get("v", data.get("volume", "0"))),
            "quote_volume": str(data.get("q", data.get("quote_volume", "0"))),
            "trade_count": str(data.get("n", data.get("trade_count", "0"))),
            "updated_at": str(data.get("E", data.get("updated_at", int(time.time() * 1000)))),
        }
    
    def _transform_ticker_from_storage(self, data: Dict[str, str]) -> Dict[str, Any]:
        """Transform ticker data from Redis storage format."""
        if not data:
            return {}
        
        return {
            "symbol": data.get("symbol", ""),
            "last_price": data.get("last_price", "0"),
            "price_change": data.get("price_change", "0"),
            "price_change_pct": data.get("price_change_pct", "0"),
            "open": data.get("open", "0"),
            "high": data.get("high", "0"),
            "low": data.get("low", "0"),
            "volume": data.get("volume", "0"),
            "quote_volume": data.get("quote_volume", "0"),
            "trade_count": int(data.get("trade_count", 0) or 0),
            "updated_at": int(data.get("updated_at", 0) or 0),
        }
    
    def write_ticker(self, symbol: str, data: Dict[str, Any]) -> bool:
        """Write ticker data to Redis."""
        try:
            key = self._make_ticker_key(symbol)
            storage_data = self._transform_ticker_to_storage(symbol, data)
            self.client.hset(key, mapping=storage_data)
            self.client.expire(key, self.ticker_ttl_seconds)
            return True
        except Exception as e:
            logger.error(f"Failed to write ticker for {symbol}: {e}")
            return False
    
    def get_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get ticker data from Redis."""
        try:
            key = self._make_ticker_key(symbol)
            data = self.client.hgetall(key)
            if not data:
                return None
            return self._transform_ticker_from_storage(data)
        except Exception as e:
            logger.error(f"Failed to get ticker for {symbol}: {e}")
            return None
    
    def get_all_tickers(self) -> List[Dict[str, Any]]:
        """Get all ticker data from Redis."""
        try:
            pattern = f"{self.PREFIX_TICKER}:*"
            keys = self.client.keys(pattern)
            
            if not keys:
                return []
            
            tickers = []
            pipe = self.client.pipeline()
            for key in keys:
                pipe.hgetall(key)
            
            results = pipe.execute()
            for data in results:
                if data:
                    tickers.append(self._transform_ticker_from_storage(data))
            
            return tickers
        except Exception as e:
            logger.error(f"Failed to get all tickers: {e}")
            return []

    def _make_trade_key(self, symbol: str) -> str:
        """Create Redis key for recent trades."""
        return f"{self.PREFIX_TRADE}:{symbol}"
    
    def write_recent_trade(self, symbol: str, trade: Dict[str, Any], max_trades: int = 100) -> bool:
        """Write a recent trade to Redis list."""
        try:
            key = self._make_trade_key(symbol)
            trade_json = json.dumps(trade)
            
            pipe = self.client.pipeline()
            pipe.lpush(key, trade_json)
            pipe.ltrim(key, 0, max_trades - 1)
            pipe.expire(key, self.DEFAULT_TRADE_TTL)
            pipe.execute()
            
            return True
        except Exception as e:
            logger.error(f"Failed to write trade for {symbol}: {e}")
            return False
    
    def get_recent_trades(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent trades from Redis."""
        try:
            key = self._make_trade_key(symbol)
            trades_json = self.client.lrange(key, 0, limit - 1)
            
            trades = []
            for trade_str in trades_json:
                try:
                    trades.append(json.loads(trade_str))
                except json.JSONDecodeError:
                    continue
            
            return trades
        except Exception as e:
            logger.error(f"Failed to get trades for {symbol}: {e}")
            return []
    
    def _make_alert_key(self) -> str:
        """Create Redis key for alerts list."""
        return f"{self.PREFIX_ALERT}:recent"
    
    def write_alert(self, alert: Dict[str, Any], max_alerts: int = 1000) -> bool:
        """Write an alert to Redis."""
        try:
            key = self._make_alert_key()
            
            # Ensure timestamp is present
            if "timestamp" not in alert:
                alert["timestamp"] = datetime.now().isoformat()
            elif isinstance(alert["timestamp"], datetime):
                alert["timestamp"] = alert["timestamp"].isoformat()
            
            alert_json = json.dumps(alert)
            
            pipe = self.client.pipeline()
            pipe.lpush(key, alert_json)
            pipe.ltrim(key, 0, max_alerts - 1)
            pipe.expire(key, self.DEFAULT_ALERT_TTL)
            pipe.execute()
            
            return True
        except Exception as e:
            logger.error(f"Failed to write alert: {e}")
            return False
    
    def get_recent_alerts(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent alerts from Redis."""
        try:
            key = self._make_alert_key()
            alerts_json = self.client.lrange(key, 0, limit - 1)
            
            alerts = []
            for alert_str in alerts_json:
                try:
                    alert = json.loads(alert_str)
                    # Convert timestamp string back to datetime if needed
                    if "timestamp" in alert and isinstance(alert["timestamp"], str):
                        try:
                            alert["timestamp"] = datetime.fromisoformat(alert["timestamp"].replace("Z", "+00:00"))
                        except ValueError:
                            pass
                    alerts.append(alert)
                except json.JSONDecodeError:
                    continue
            
            return alerts
        except Exception as e:
            logger.error(f"Failed to get alerts: {e}")
            return []
    
    def write_alerts_batch(self, alerts: List[Dict[str, Any]], max_alerts: int = 1000) -> int:
        """Write multiple alerts in a batch."""
        if not alerts:
            return 0
        
        try:
            key = self._make_alert_key()
            pipe = self.client.pipeline()
            
            for alert in alerts:
                if "timestamp" not in alert:
                    alert["timestamp"] = datetime.now().isoformat()
                elif isinstance(alert["timestamp"], datetime):
                    alert["timestamp"] = alert["timestamp"].isoformat()
                
                alert_json = json.dumps(alert)
                pipe.lpush(key, alert_json)
            
            pipe.ltrim(key, 0, max_alerts - 1)
            pipe.expire(key, self.DEFAULT_ALERT_TTL)
            pipe.execute()
            
            return len(alerts)
        except Exception as e:
            logger.error(f"Failed to write alerts batch: {e}")
            return 0

    def write_to_redis(
        self,
        key: str,
        value: Union[Dict[str, Any], List[Any], str],
        data_type: str = "hash",
        ttl: Optional[int] = None,
    ) -> bool:
        """Generic write method supporting multiple Redis data types."""
        try:
            if data_type == "hash":
                if not isinstance(value, dict):
                    raise ValueError("Value must be a dict for hash type")
                hash_data = {k: str(v) if v is not None else "" for k, v in value.items()}
                self.client.hset(key, mapping=hash_data)
            
            elif data_type == "list":
                if not isinstance(value, list):
                    raise ValueError("Value must be a list for list type")
                # Clear existing list and add new items
                pipe = self.client.pipeline()
                pipe.delete(key)
                for item in value:
                    if isinstance(item, (dict, list)):
                        pipe.rpush(key, json.dumps(item))
                    else:
                        pipe.rpush(key, str(item))
                pipe.execute()
            
            elif data_type == "string":
                if isinstance(value, (dict, list)):
                    self.client.set(key, json.dumps(value))
                else:
                    self.client.set(key, str(value))
            
            else:
                raise ValueError(f"Unsupported data type: {data_type}")
            
            if ttl:
                self.client.expire(key, ttl)
            
            return True
        except Exception as e:
            logger.error(f"Failed to write to Redis key {key}: {e}")
            return False
