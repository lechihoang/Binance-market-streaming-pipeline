"""Redis storage module for real-time market data caching.

Provides fast in-memory caching for:
- OHLCV klines (Open, High, Low, Close, Volume aggregations)
- Real-time ticker data (24h price/volume stats)
- Recent trades and alerts

Acts as hot tier for recent data with automatic TTL-based expiration.
"""

import json
import time
from datetime import datetime
from typing import Any

import redis
from redis.exceptions import ConnectionError, TimeoutError

from util.constant import (
    REDIS_DB,
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_PORT,
    TICKER_MAP,
    RedisKey,
    RedisLimit,
    RedisTTL,
)
from util.logging import get_logger

logger = get_logger(__name__)


class Redis:
    """Redis storage operations for real-time data caching.

    Manages Redis connections and operations for hot-tier data storage:
    - Kline aggregations: 1m/5m/15m/1h OHLCV data with configurable TTL
    - Ticker data: 24h rolling statistics per symbol
    - Trades/Alerts: Recent data with list trimming

    Data automatically expires based on configured TTLs to maintain cache size.
    """

    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        db: int = REDIS_DB,
        password: str | None = REDIS_PASSWORD,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        ttl_seconds: int | None = None,
        ticker_ttl: int | None = None,
    ):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.ticker_ttl = ticker_ttl or ttl_seconds or RedisTTL.TICKER

        self.client: redis.Redis | None = None
        self.connect()

    def connect(self) -> None:
        """Connect to Redis with exponential backoff."""
        last_err, delay = None, self.retry_delay

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

    def ensure(self) -> redis.Redis:
        """Ensure connected, reconnect if needed."""
        if self.client is None:
            self.connect()
        try:
            self.client.ping()  # type: ignore
        except (ConnectionError, TimeoutError):
            logger.warning("Redis connection lost, reconnecting...")
            self.connect()
        return self.client  # type: ignore

    def ping(self) -> bool:
        try:
            return bool(self.ensure().ping())
        except Exception:
            return False

    def close(self) -> None:
        if self.client:
            self.client.close()
            self.client = None
            logger.info("Redis connection closed")

    # Helper Methods

    def to_hash(self, data: dict[str, Any]) -> dict[str, str]:
        """Convert dict to Redis hash format."""
        return {k: str(v) if v is not None else "" for k, v in data.items()}

    def parse_value(self, key: str, value: str) -> Any:
        """Parse Redis hash value to appropriate type."""
        if value == "":
            return None
        if key in ("trade_count", "buy_count", "sell_count"):
            return int(float(value)) if value else 0
        try:
            return float(value)
        except ValueError:
            return value

    def write_to_list(self, key: str, item: dict[str, Any], max_items: int, ttl: int) -> bool:
        """Write item to list with trimming and expiry."""
        try:
            pipe = self.ensure().pipeline()
            pipe.lpush(key, json.dumps(item))
            pipe.ltrim(key, 0, max_items - 1)
            pipe.expire(key, ttl)
            pipe.execute()
            return True
        except Exception as e:
            logger.error(f"Failed to write to {key}: {e}")
            return False

    def read_from_list(self, key: str, limit: int) -> list[dict[str, Any]]:
        """Read list items as dicts."""
        try:
            items = self.ensure().lrange(key, 0, limit - 1)
            result = []
            for item in items:
                try:
                    result.append(json.loads(item))
                except json.JSONDecodeError:
                    continue
            return result
        except Exception as e:
            logger.error(f"Failed to read {key}: {e}")
            return []

    @staticmethod
    def parse_ts(ts: Any) -> datetime | None:
        """Parse timestamp to datetime."""
        if ts is None:
            return None
        if isinstance(ts, datetime):
            return ts
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(ts / 1000)
        if isinstance(ts, str):
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return dt.replace(tzinfo=None) if dt.tzinfo else dt
            except ValueError:
                return None
        return None

    # Aggregation (OHLCV klines)

    def agg_key(self, symbol: str, interval: str = "1m") -> str:
        return f"{RedisKey.AGG}:{symbol}:{interval}"

    def write_agg(self, symbol: str, interval: str, data: dict[str, Any], ttl: int | None = None) -> bool:
        try:
            key = self.agg_key(symbol, interval)
            self.ensure().hset(key, mapping=self.to_hash(data))
            self.ensure().expire(key, ttl or RedisTTL.AGG)
            return True
        except Exception as e:
            logger.error(f"Failed to write agg for {symbol}: {e}")
            return False

    def get_agg(self, symbol: str, interval: str = "1m") -> dict[str, Any] | None:
        try:
            data = self.ensure().hgetall(self.agg_key(symbol, interval))
            return {k: self.parse_value(k, v) for k, v in data.items()} if data else None
        except Exception as e:
            logger.error(f"Failed to get agg for {symbol}: {e}")
            return None

    def write_agg_batch(self, aggs: list[dict[str, Any]], ttl: int | None = None) -> int:
        count = 0
        try:
            pipe = self.ensure().pipeline()
            for agg in aggs:
                symbol, interval = agg.get("symbol"), agg.get("interval", "1m")
                if not symbol:
                    continue
                key = self.agg_key(symbol, interval)
                pipe.hset(key, mapping=self.to_hash(agg))
                pipe.expire(key, ttl or RedisTTL.AGG)
                count += 1
            pipe.execute()
            return count
        except Exception as e:
            logger.error(f"Failed to write agg batch: {e}")
            return 0

    def get_agg_list(self, symbol: str, interval: str = "5m") -> list[dict[str, Any]]:
        """Get aggregations for a symbol."""
        agg = self.get_agg(symbol, "1m")
        if not agg:
            return []
        if interval != "1m":
            agg["interval"] = interval
        return [agg]

    # Price

    def price_key(self, symbol: str) -> str:
        return f"{RedisKey.PRICE}:{symbol}"

    def write_price(self, symbol: str, price: float, ts: int | None = None) -> bool:
        """Write latest price."""
        try:
            key = self.price_key(symbol)
            data = {"price": str(price), "timestamp": str(ts or int(time.time() * 1000))}
            self.ensure().hset(key, mapping=data)
            self.ensure().expire(key, RedisTTL.AGG)
            return True
        except Exception as e:
            logger.error(f"Failed to write price for {symbol}: {e}")
            return False

    def get_price(self, symbol: str) -> dict[str, Any] | None:
        """Get latest price."""
        try:
            data = self.ensure().hgetall(self.price_key(symbol))
            return {"price": float(data.get("price", 0)), "timestamp": int(data.get("timestamp", 0))} if data else None
        except Exception as e:
            logger.error(f"Failed to get price for {symbol}: {e}")
            return None

    # Ticker

    def ticker_key(self, symbol: str) -> str:
        return f"{RedisKey.TICKER}:{symbol.upper()}"

    def pack_ticker(self, symbol: str, data: dict[str, Any]) -> dict[str, str]:
        """Transform ticker for storage."""
        packed = {"symbol": symbol.upper()}

        for binance_key, storage_key in TICKER_MAP.items():
            packed[storage_key] = str(data.get(binance_key, data.get(storage_key, "0")))

        # Fallback for alternative key names
        alt_keys = {
            "open_price": "open",
            "high_price": "high",
            "low_price": "low",
            "price_change_percent": "price_change_pct",
            "event_time": "updated_at",
        }
        for alt, target in alt_keys.items():
            if alt in data:
                packed[target] = str(data[alt])

        return packed

    def unpack_ticker(self, data: dict[str, str]) -> dict[str, Any]:
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

    def write_ticker(self, symbol: str, data: dict[str, Any]) -> bool:
        """Write ticker data."""
        try:
            key = self.ticker_key(symbol)
            self.ensure().hset(key, mapping=self.pack_ticker(symbol, data))
            self.ensure().expire(key, self.ticker_ttl)
            return True
        except Exception as e:
            logger.error(f"Failed to write ticker for {symbol}: {e}")
            return False

    def get_ticker(self, symbol: str) -> dict[str, Any] | None:
        """Get ticker data."""
        try:
            data = self.ensure().hgetall(self.ticker_key(symbol))
            return self.unpack_ticker(data) if data else None
        except Exception as e:
            logger.error(f"Failed to get ticker for {symbol}: {e}")
            return None

    def get_ticker_all(self) -> list[dict[str, Any]]:
        """Get all tickers."""
        try:
            keys = self.ensure().keys(f"{RedisKey.TICKER}:*")
            if not keys:
                return []

            pipe = self.ensure().pipeline()
            for key in keys:
                pipe.hgetall(key)

            return [self.unpack_ticker(data) for data in pipe.execute() if data]
        except Exception as e:
            logger.error(f"Failed to get all tickers: {e}")
            return []

    # Trades

    def trade_key(self, symbol: str) -> str:
        return f"{RedisKey.TRADE}:{symbol}"

    def write_trade(self, symbol: str, trade: dict[str, Any], max_trades: int = RedisLimit.MAX_TRADE) -> bool:
        """Write recent trade."""
        return self.write_to_list(self.trade_key(symbol), trade, max_trades, RedisTTL.TRADE)

    def get_trade(self, symbol: str, limit: int = RedisLimit.MAX_TRADE) -> list[dict[str, Any]]:
        """Get recent trades."""
        return self.read_from_list(self.trade_key(symbol), limit)

    # Alerts

    def alert_key(self) -> str:
        return f"{RedisKey.ALERT}:recent"

    def normalize_alert_ts(self, alert: dict[str, Any]) -> None:
        """Normalize alert timestamp to ISO format."""
        if "timestamp" not in alert:
            alert["timestamp"] = datetime.now().isoformat()
        elif isinstance(alert["timestamp"], datetime):
            alert["timestamp"] = alert["timestamp"].isoformat()

    def write_alert(self, alert: dict[str, Any], max_alerts: int = RedisLimit.MAX_ALERT) -> bool:
        """Write an alert."""
        self.normalize_alert_ts(alert)
        return self.write_to_list(self.alert_key(), alert, max_alerts, RedisTTL.ALERT)

    def get_alert(self, limit: int = RedisLimit.MAX_ALERT) -> list[dict[str, Any]]:
        """Get recent alerts."""
        alerts = self.read_from_list(self.alert_key(), limit)

        for alert in alerts:
            if "timestamp" in alert and isinstance(alert["timestamp"], str):
                parsed = self.parse_ts(alert["timestamp"])
                if parsed:
                    alert["timestamp"] = parsed
        return alerts

    def write_alert_batch(self, alerts: list[dict[str, Any]], max_alerts: int = RedisLimit.MAX_ALERT) -> int:
        """Batch write alerts."""
        if not alerts:
            return 0

        try:
            key = self.alert_key()
            pipe = self.ensure().pipeline()

            for alert in alerts:
                self.normalize_alert_ts(alert)
                pipe.lpush(key, json.dumps(alert))

            pipe.ltrim(key, 0, max_alerts - 1)
            pipe.expire(key, RedisTTL.ALERT)
            pipe.execute()
            return len(alerts)
        except Exception as e:
            logger.error(f"Failed to write alert batch: {e}")
            return 0

    # Generic

    def write(
        self,
        key: str,
        value: dict[str, Any] | list[Any] | str,
        dtype: str = "hash",
        ttl: int | None = None,
    ) -> bool:
        """Generic write supporting multiple Redis data types."""
        try:
            if dtype == "hash":
                if not isinstance(value, dict):
                    raise ValueError("Value must be dict for hash")
                self.ensure().hset(key, mapping=self.to_hash(value))

            elif dtype == "list":
                if not isinstance(value, list):
                    raise ValueError("Value must be list for list type")
                pipe = self.ensure().pipeline()
                pipe.delete(key)
                for item in value:
                    pipe.rpush(key, json.dumps(item) if isinstance(item, (dict, list)) else str(item))
                pipe.execute()

            elif dtype == "string":
                val = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
                self.ensure().set(key, val)

            else:
                raise ValueError(f"Unsupported dtype: {dtype}")

            if ttl:
                self.ensure().expire(key, ttl)
            return True
        except Exception as e:
            logger.error(f"Failed to write key {key}: {e}")
            return False


def check_health(
    host: str = REDIS_HOST,
    port: int = REDIS_PORT,
    db: int = REDIS_DB,
    retries: int = 3,
    delay: float = 1.0,
    max_retries: int | None = None,
    retry_delay: float | None = None,
) -> dict[str, Any]:
    """Check Redis connection health."""
    retries = max_retries if max_retries is not None else retries
    delay = retry_delay if retry_delay is not None else delay
    last_err = None

    for attempt in range(1, retries + 1):
        try:
            client = redis.Redis(host=host, port=port, db=db, socket_connect_timeout=5, socket_timeout=5)
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
            last_err = e
            if attempt < retries:
                time.sleep(delay)

    raise Exception(f"Redis health check failed after {retries} attempts: {last_err}")
