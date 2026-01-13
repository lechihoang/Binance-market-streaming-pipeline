"""Redis storage module."""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import redis
from redis.exceptions import ConnectionError, TimeoutError

logger = logging.getLogger(__name__)


def parse_candle_timestamp(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def get_window_start(dt: datetime, window_minutes: int) -> datetime:
    window_min = (dt.minute // window_minutes) * window_minutes
    return dt.replace(minute=window_min, second=0, microsecond=0)


def aggregate_candles(candles: List[Dict[str, Any]], window_start: datetime, interval: str) -> Dict[str, Any]:
    candles.sort(key=lambda x: x.get("timestamp", 0))
    agg = {
        "timestamp": window_start,
        "symbol": candles[0].get("symbol", ""),
        "interval": interval,
        "open": candles[0].get("open", 0),
        "high": max(c.get("high", 0) for c in candles),
        "low": min(c.get("low", float("inf")) for c in candles),
        "close": candles[-1].get("close", 0),
        "volume": sum(c.get("volume", 0) for c in candles),
        "quote_volume": sum(c.get("quote_volume", 0) for c in candles),
        "trade_count": sum(c.get("trade_count", 0) for c in candles),
    }
    if agg["low"] == float("inf"):
        agg["low"] = 0
    return agg


def check_health(
    host: str = "localhost",
    port: int = 6379,
    db: int = 0,
    retries: int = 3,
    delay: float = 1.0,
    max_retries: Optional[int] = None,
    retry_delay: Optional[float] = None,
) -> Dict[str, Any]:
    """Check Redis connection health."""
    retries = max_retries if max_retries is not None else retries
    delay = retry_delay if retry_delay is not None else delay
    last_err = None
    
    for attempt in range(1, retries + 1):
        try:
            client = redis.Redis(
                host=host, port=port, db=db,
                socket_connect_timeout=5, socket_timeout=5,
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
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(delay)
    
    raise Exception(f"Redis health check failed after {retries} attempts: {last_err}")


class Redis:
    # Key prefixes
    AGG = "agg"
    TICKER = "ticker"
    TRADE = "trade"
    ALERT = "alert"
    PRICE = "price"
    
    # TTLs in seconds
    AGG_TTL = 3600
    TICKER_TTL = 60
    TRADE_TTL = 300
    ALERT_TTL = 86400
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        ttl_seconds: Optional[int] = None,
        ticker_ttl: Optional[int] = None,
    ):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.ticker_ttl = ticker_ttl or ttl_seconds or self.TICKER_TTL
        
        self.client: Optional[redis.Redis] = None
        self.connect()
    
    def connect(self) -> None:
        """Connect to Redis with exponential backoff."""
        last_err = None
        delay = self.retry_delay
        
        for attempt in range(1, self.max_retries + 1):
            try:
                self.client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    password=self.password,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    decode_responses=True,
                )
                self.client.ping()
                logger.info(f"Connected to Redis at {self.host}:{self.port}/{self.db}")
                return
            except (ConnectionError, TimeoutError) as e:
                last_err = e
                logger.warning(f"Redis connection attempt {attempt} failed: {e}")
                if attempt < self.max_retries:
                    time.sleep(delay)
                    delay *= 2
        
        raise ConnectionError(f"Failed to connect to Redis after {self.max_retries} attempts: {last_err}")
    
    def ensure_connected(self) -> Any:
        if self.client is None:
            self.connect()
        try:
            if self.client:
                self.client.ping()
        except (ConnectionError, TimeoutError):
            logger.warning("Redis connection lost, reconnecting...")
            self.connect()
        return self.client
    
    def ping(self) -> bool:
        try:
            result = self.ensure_connected().ping()
            return bool(result)
        except Exception:
            return False
    
    def close(self) -> None:
        if self.client:
            self.client.close()
            self.client = None
            logger.info("Redis connection closed")

    # ========== Aggregation (OHLCV) ==========
    
    def agg_key(self, symbol: str, interval: str = "1m") -> str:
        return f"{self.AGG}:{symbol}:{interval}"
    
    def write_agg(self, symbol: str, interval: str, data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Write aggregation (OHLCV) data."""
        try:
            key = self.agg_key(symbol, interval)
            hash_data = {k: str(v) if v is not None else "" for k, v in data.items()}
            self.ensure_connected().hset(key, mapping=hash_data)
            self.ensure_connected().expire(key, ttl or self.AGG_TTL)
            return True
        except Exception as e:
            logger.error(f"Failed to write agg for {symbol}: {e}")
            return False
    
    def get_agg(self, symbol: str, interval: str = "1m") -> Optional[Dict[str, Any]]:
        """Get aggregation data."""
        try:
            key = self.agg_key(symbol, interval)
            data = self.ensure_connected().hgetall(key)
            if not data:
                return None
            
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
            logger.error(f"Failed to get agg for {symbol}: {e}")
            return None
    
    def write_aggs(self, aggs: List[Dict[str, Any]], ttl: Optional[int] = None) -> int:
        """Batch write aggregations."""
        count = 0
        pipe = self.ensure_connected().pipeline()
        
        try:
            for agg in aggs:
                symbol = agg.get("symbol")
                interval = agg.get("interval", "1m")
                if not symbol:
                    continue
                
                key = self.agg_key(symbol, interval)
                hash_data = {k: str(v) if v is not None else "" for k, v in agg.items()}
                pipe.hset(key, mapping=hash_data)
                pipe.expire(key, ttl or self.AGG_TTL)
                count += 1
            
            pipe.execute()
            return count
        except Exception as e:
            logger.error(f"Failed to write aggs batch: {e}")
            return 0

    def get_aggs(self, symbol: str, interval: str = "5m") -> List[Dict[str, Any]]:
        """Get aggregations, combining into higher timeframe if needed."""
        if interval == "1m":
            agg = self.get_agg(symbol, "1m")
            return [agg] if agg else []
        
        if interval not in {"5m", "15m"}:
            logger.warning(f"Invalid interval '{interval}'")
            return []
        
        agg_1m = self.get_agg(symbol, "1m")
        if not agg_1m:
            return []
        
        result = dict(agg_1m)
        result["interval"] = interval
        return [result]

    def combine_aggs(self, candles: List[Dict[str, Any]], interval: str = "5m") -> List[Dict[str, Any]]:
        if not candles or interval == "1m":
            return candles or []
        
        mins = {"5m": 5, "15m": 15}.get(interval)
        if mins is None:
            logger.warning(f"Invalid interval '{interval}'")
            return candles
        
        windows: Dict[datetime, List[Dict[str, Any]]] = {}
        for candle in candles:
            ts = parse_candle_timestamp(candle.get("timestamp"))
            if ts is None:
                continue
            window_start = get_window_start(ts, mins)
            if window_start not in windows:
                windows[window_start] = []
            windows[window_start].append(candle)
        
        return [
            aggregate_candles(wc, ws, interval)
            for ws, wc in sorted(windows.items())
            if wc
        ]
    
    # ========== Price ==========
    
    def price_key(self, symbol: str) -> str:
        return f"{self.PRICE}:{symbol}"
    
    def write_price(self, symbol: str, price: float, ts: Optional[int] = None) -> bool:
        """Write latest price."""
        try:
            key = self.price_key(symbol)
            data = {
                "price": str(price),
                "timestamp": str(ts or int(time.time() * 1000)),
            }
            self.ensure_connected().hset(key, mapping=data)
            self.ensure_connected().expire(key, self.AGG_TTL)
            return True
        except Exception as e:
            logger.error(f"Failed to write price for {symbol}: {e}")
            return False
    
    def get_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get latest price."""
        try:
            key = self.price_key(symbol)
            data = self.ensure_connected().hgetall(key)
            if not data:
                return None
            return {
                "price": float(data.get("price", 0)),
                "timestamp": int(data.get("timestamp", 0)),
            }
        except Exception as e:
            logger.error(f"Failed to get price for {symbol}: {e}")
            return None

    # ========== Ticker ==========
    
    def ticker_key(self, symbol: str) -> str:
        return f"{self.TICKER}:{symbol.upper()}"
    
    def pack_ticker(self, symbol: str, data: Dict[str, Any]) -> Dict[str, str]:
        """Transform ticker for storage.
        
        Supports multiple key formats:
        - Raw Binance API keys (o, h, l, c, p, P, etc.)
        - Normalized keys from connector (open_price, high_price, etc.)
        - Redis storage keys (open, high, low, etc.)
        """
        return {
            "symbol": symbol.upper(),
            "last_price": str(data.get("c", data.get("last_price", "0"))),
            "price_change": str(data.get("p", data.get("price_change", "0"))),
            "price_change_pct": str(data.get("P", data.get("price_change_pct", data.get("price_change_percent", "0")))),
            "open": str(data.get("o", data.get("open", data.get("open_price", "0")))),
            "high": str(data.get("h", data.get("high", data.get("high_price", "0")))),
            "low": str(data.get("l", data.get("low", data.get("low_price", "0")))),
            "volume": str(data.get("v", data.get("volume", "0"))),
            "quote_volume": str(data.get("q", data.get("quote_volume", "0"))),
            "trade_count": str(data.get("n", data.get("trade_count", "0"))),
            "updated_at": str(data.get("E", data.get("updated_at", data.get("event_time", int(time.time() * 1000))))),
        }
    
    def unpack_ticker(self, data: Dict[str, str]) -> Dict[str, Any]:
        """Transform ticker from storage."""
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
        """Write ticker data."""
        try:
            key = self.ticker_key(symbol)
            storage_data = self.pack_ticker(symbol, data)
            self.ensure_connected().hset(key, mapping=storage_data)
            self.ensure_connected().expire(key, self.ticker_ttl)
            return True
        except Exception as e:
            logger.error(f"Failed to write ticker for {symbol}: {e}")
            return False
    
    def get_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get ticker data."""
        try:
            key = self.ticker_key(symbol)
            data = self.ensure_connected().hgetall(key)
            if not data:
                return None
            return self.unpack_ticker(data)
        except Exception as e:
            logger.error(f"Failed to get ticker for {symbol}: {e}")
            return None
    
    def get_tickers(self) -> List[Dict[str, Any]]:
        """Get all tickers."""
        try:
            pattern = f"{self.TICKER}:*"
            keys = self.ensure_connected().keys(pattern)
            if not keys:
                return []
            
            tickers = []
            pipe = self.ensure_connected().pipeline()
            for key in keys:
                pipe.hgetall(key)
            
            for data in pipe.execute():
                if data:
                    tickers.append(self.unpack_ticker(data))
            return tickers
        except Exception as e:
            logger.error(f"Failed to get tickers: {e}")
            return []

    # ========== Trades ==========
    
    def trade_key(self, symbol: str) -> str:
        return f"{self.TRADE}:{symbol}"
    
    def write_trade(self, symbol: str, trade: Dict[str, Any], max_trades: int = 100) -> bool:
        """Write recent trade."""
        try:
            key = self.trade_key(symbol)
            trade_json = json.dumps(trade)
            
            pipe = self.ensure_connected().pipeline()
            pipe.lpush(key, trade_json)
            pipe.ltrim(key, 0, max_trades - 1)
            pipe.expire(key, self.TRADE_TTL)
            pipe.execute()
            return True
        except Exception as e:
            logger.error(f"Failed to write trade for {symbol}: {e}")
            return False
    
    def get_trades(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent trades."""
        try:
            key = self.trade_key(symbol)
            trades_json = self.ensure_connected().lrange(key, 0, limit - 1)
            
            trades = []
            for t in trades_json:
                try:
                    trades.append(json.loads(t))
                except json.JSONDecodeError:
                    continue
            return trades
        except Exception as e:
            logger.error(f"Failed to get trades for {symbol}: {e}")
            return []
    
    # ========== Alerts ==========
    
    def alert_key(self) -> str:
        return f"{self.ALERT}:recent"
    
    def write_alert(self, alert: Dict[str, Any], max_alerts: int = 1000) -> bool:
        """Write an alert."""
        try:
            key = self.alert_key()
            
            if "timestamp" not in alert:
                alert["timestamp"] = datetime.now(timezone.utc).isoformat()
            elif isinstance(alert["timestamp"], datetime):
                alert["timestamp"] = alert["timestamp"].isoformat()
            
            alert_json = json.dumps(alert)
            
            pipe = self.ensure_connected().pipeline()
            pipe.lpush(key, alert_json)
            pipe.ltrim(key, 0, max_alerts - 1)
            pipe.expire(key, self.ALERT_TTL)
            pipe.execute()
            return True
        except Exception as e:
            logger.error(f"Failed to write alert: {e}")
            return False
    
    def get_alerts(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent alerts."""
        try:
            key = self.alert_key()
            alerts_json = self.ensure_connected().lrange(key, 0, limit - 1)
            
            alerts = []
            for a in alerts_json:
                try:
                    alert = json.loads(a)
                    if "timestamp" in alert and isinstance(alert["timestamp"], str):
                        try:
                            dt = datetime.fromisoformat(alert["timestamp"].replace("Z", "+00:00"))
                            alert["timestamp"] = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
                        except ValueError:
                            pass
                    alerts.append(alert)
                except json.JSONDecodeError:
                    continue
            return alerts
        except Exception as e:
            logger.error(f"Failed to get alerts: {e}")
            return []
    
    def write_alerts(self, alerts: List[Dict[str, Any]], max_alerts: int = 1000) -> int:
        """Batch write alerts."""
        if not alerts:
            return 0
        
        try:
            key = self.alert_key()
            pipe = self.ensure_connected().pipeline()
            
            for alert in alerts:
                if "timestamp" not in alert:
                    alert["timestamp"] = datetime.now(timezone.utc).isoformat()
                elif isinstance(alert["timestamp"], datetime):
                    alert["timestamp"] = alert["timestamp"].isoformat()
                pipe.lpush(key, json.dumps(alert))
            
            pipe.ltrim(key, 0, max_alerts - 1)
            pipe.expire(key, self.ALERT_TTL)
            pipe.execute()
            return len(alerts)
        except Exception as e:
            logger.error(f"Failed to write alerts batch: {e}")
            return 0

    # ========== Generic ==========
    
    def write(
        self,
        key: str,
        value: Union[Dict[str, Any], List[Any], str],
        dtype: str = "hash",
        ttl: Optional[int] = None,
    ) -> bool:
        """Generic write supporting multiple Redis data types."""
        try:
            if dtype == "hash":
                if not isinstance(value, dict):
                    raise ValueError("Value must be dict for hash")
                hash_data = {k: str(v) if v is not None else "" for k, v in value.items()}
                self.ensure_connected().hset(key, mapping=hash_data)
            
            elif dtype == "list":
                if not isinstance(value, list):
                    raise ValueError("Value must be list for list type")
                pipe = self.ensure_connected().pipeline()
                pipe.delete(key)
                for item in value:
                    if isinstance(item, (dict, list)):
                        pipe.rpush(key, json.dumps(item))
                    else:
                        pipe.rpush(key, str(item))
                pipe.execute()
            
            elif dtype == "string":
                if isinstance(value, (dict, list)):
                    self.ensure_connected().set(key, json.dumps(value))
                else:
                    self.ensure_connected().set(key, str(value))
            
            else:
                raise ValueError(f"Unsupported dtype: {dtype}")
            
            if ttl:
                self.ensure_connected().expire(key, ttl)
            return True
        except Exception as e:
            logger.error(f"Failed to write key {key}: {e}")
            return False
