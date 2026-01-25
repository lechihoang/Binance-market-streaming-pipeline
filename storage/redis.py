"""Redis storage module for real-time market data caching."""

import json
from datetime import datetime
from typing import Any

import redis
from loguru import logger

from schema.market import Alert, Kline, Ticker, Trade
from util.constant import (
    REDIS_DB,
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_PORT,
    RedisKey,
    RedisLimit,
    RedisTTL,
)


class Redis:
    def __init__(
        self,
        host: str = REDIS_HOST,
        port: int = REDIS_PORT,
        db: int = REDIS_DB,
        password: str | None = REDIS_PASSWORD,
        ticker_ttl: int | None = None,
        # Legacy params for compatibility
        max_retries: int = 3,
        retry_delay: float = 1.0,
        ttl_seconds: int | None = None,
    ):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.ticker_ttl = ticker_ttl or ttl_seconds or RedisTTL.TICKER

        self.client = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
            health_check_interval=30,
        )

        try:
            self.client.ping()
            logger.info(f"Connected to Redis at {self.host}:{self.port}/{self.db}")
        except Exception as e:
            logger.warning(f"Initial Redis ping failed (will retry on usage): {e}")

    def close(self) -> None:
        self.client.close()
        logger.info("Redis connection closed")

    def ping(self) -> bool:
        try:
            return bool(self.client.ping())
        except Exception:
            return False

    # ==========================================
    # Kline/Aggregation
    # ==========================================

    def _kline_key(self, symbol: str, interval: str = "1m") -> str:
        return f"{RedisKey.AGG}:{symbol}:{interval}"

    def write_kline(self, kline: Kline, ttl: int | None = None) -> bool:
        """Write single kline to Redis."""
        try:
            key = self._kline_key(kline.symbol, kline.interval)
            self.client.hset(key, mapping=kline.to_redis_dict())
            self.client.expire(key, ttl or RedisTTL.AGG)
            return True
        except Exception as e:
            logger.error(f"Failed to write kline {kline.symbol}: {e}")
            return False

    def write_klines(self, klines: list[Kline | dict], ttl: int | None = None) -> int:
        """Batch write klines to Redis."""
        if not klines:
            return 0
        try:
            pipe = self.client.pipeline()
            for item in klines:
                kline = item if isinstance(item, Kline) else Kline.model_validate(item)
                key = self._kline_key(kline.symbol, kline.interval)
                pipe.hset(key, mapping=kline.to_redis_dict())
                pipe.expire(key, ttl or RedisTTL.AGG)
            pipe.execute()
            return len(klines)
        except Exception as e:
            logger.error(f"Failed to write kline batch: {e}")
            return 0

    def get_kline(self, symbol: str, interval: str = "1m") -> Kline | None:
        """Get single kline for symbol."""
        try:
            key = self._kline_key(symbol, interval)
            data = self.client.hgetall(key)
            if not data:
                return None
            data["interval"] = interval
            return Kline.from_redis_dict(data)
        except Exception as e:
            logger.error(f"Failed to get kline {symbol}: {e}")
            return None

    def get_klines(self, symbol: str, interval: str = "1m") -> list[Kline]:
        """Get klines for symbol (returns list for consistency)."""
        kline = self.get_kline(symbol, interval)
        return [kline] if kline else []

    # Legacy aliases for backward compatibility
    def write_agg_batch(self, aggs: list[dict], ttl: int | None = None) -> int:
        return self.write_klines(aggs, ttl)

    def get_agg(self, symbol: str, interval: str = "1m") -> dict | None:
        kline = self.get_kline(symbol, interval)
        return kline.model_dump() if kline else None

    def get_agg_list(self, symbol: str, interval: str = "1m") -> list[dict]:
        return [k.model_dump() for k in self.get_klines(symbol, interval)]

    # ==========================================
    # Ticker
    # ==========================================

    def _ticker_key(self, symbol: str) -> str:
        return f"{RedisKey.TICKER}:{symbol.upper()}"

    def write_ticker(self, symbol: str, data: dict[str, Any]) -> bool:
        """Write ticker data to Redis."""
        try:
            # Ticker model handles Binance field mapping automatically
            ticker = Ticker.model_validate({**data, "symbol": symbol.upper()})
            key = self._ticker_key(symbol)
            self.client.hset(key, mapping=ticker.to_redis_dict())
            self.client.expire(key, self.ticker_ttl)
            return True
        except Exception as e:
            logger.error(f"Failed to write ticker {symbol}: {e}")
            return False

    def get_ticker(self, symbol: str) -> Ticker | None:
        """Get ticker for symbol."""
        try:
            key = self._ticker_key(symbol)
            data = self.client.hgetall(key)
            return Ticker.from_redis_dict(data) if data else None
        except Exception as e:
            logger.error(f"Failed to get ticker {symbol}: {e}")
            return None

    def get_ticker_all(self) -> list[dict[str, Any]]:
        """Get all tickers (returns dicts for API compatibility)."""
        try:
            keys = self.client.keys(f"{RedisKey.TICKER}:*")
            if not keys:
                return []
            pipe = self.client.pipeline()
            for key in keys:
                pipe.hgetall(key)
            results = []
            for data in pipe.execute():
                if data:
                    try:
                        ticker = Ticker.from_redis_dict(data)
                        results.append(ticker.model_dump())
                    except Exception:
                        results.append(data)
            return results
        except Exception as e:
            logger.error(f"Failed to get all tickers: {e}")
            return []

    # ==========================================
    # Trade
    # ==========================================

    def _trade_key(self, symbol: str) -> str:
        return f"{RedisKey.TRADE}:{symbol}"

    def write_trade(self, symbol: str, data: dict[str, Any], max_trades: int = RedisLimit.MAX_TRADE) -> bool:
        """Write trade to Redis list."""
        try:
            trade = Trade.model_validate({**data, "symbol": symbol})
            key = self._trade_key(symbol)
            pipe = self.client.pipeline()
            pipe.lpush(key, json.dumps(trade.to_redis_dict()))
            pipe.ltrim(key, 0, max_trades - 1)
            pipe.expire(key, RedisTTL.TRADE)
            pipe.execute()
            return True
        except Exception as e:
            logger.error(f"Failed to write trade {symbol}: {e}")
            return False

    def get_trade(self, symbol: str, limit: int = RedisLimit.MAX_TRADE) -> list[dict[str, Any]]:
        """Get recent trades for symbol."""
        try:
            key = self._trade_key(symbol)
            items = self.client.lrange(key, 0, limit - 1)
            results = []
            for item in items:
                data = json.loads(item)
                trade = Trade.from_redis_dict(data)
                results.append(trade.model_dump())
            return results
        except Exception as e:
            logger.error(f"Failed to get trades {symbol}: {e}")
            return []

    # ==========================================
    # Alert
    # ==========================================

    def _alert_key(self) -> str:
        return f"{RedisKey.ALERT}:recent"

    def write_alerts(self, alerts: list[Alert | dict]) -> int:
        """Write alerts to Redis list."""
        if not alerts:
            return 0
        try:
            pipe = self.client.pipeline()
            key = self._alert_key()
            for item in alerts:
                alert = item if isinstance(item, Alert) else Alert.model_validate(item)
                pipe.lpush(key, json.dumps(alert.to_redis_dict()))
            pipe.ltrim(key, 0, RedisLimit.MAX_ALERT - 1)
            pipe.expire(key, RedisTTL.ALERT)
            pipe.execute()
            return len(alerts)
        except Exception as e:
            logger.error(f"Failed to write alerts: {e}")
            return 0

    def get_alerts(self, limit: int = RedisLimit.MAX_ALERT) -> list[Alert]:
        """Get recent alerts as Alert objects."""
        try:
            key = self._alert_key()
            items = self.client.lrange(key, 0, limit - 1)
            return [Alert.from_redis_dict(json.loads(item)) for item in items]
        except Exception as e:
            logger.error(f"Failed to get alerts: {e}")
            return []

    # Legacy aliases for backward compatibility
    def save_alerts(self, records: list[dict]) -> int:
        return self.write_alerts(records)

    def get_alert(self, limit: int = RedisLimit.MAX_ALERT) -> list[dict]:
        return [a.model_dump() for a in self.get_alerts(limit)]

    # ==========================================
    # Price
    # ==========================================

    def write_price(self, symbol: str, price: float, ts: datetime | None = None) -> bool:
        """Write latest price for symbol."""
        key = f"{RedisKey.PRICE}:{symbol}"
        try:
            ts_val = ts or datetime.now()
            self.client.hset(key, mapping={
                "price": str(price),
                "timestamp": ts_val.isoformat(),
            })
            self.client.expire(key, RedisTTL.AGG)
            return True
        except Exception as e:
            logger.error(f"Failed to write price {symbol}: {e}")
            return False

    def get_price(self, symbol: str) -> dict[str, Any] | None:
        """Get latest price for symbol."""
        key = f"{RedisKey.PRICE}:{symbol}"
        try:
            data = self.client.hgetall(key)
            return data if data else None
        except Exception as e:
            logger.error(f"Failed to get price {symbol}: {e}")
            return None


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
    try:
        client = redis.Redis(host=host, port=port, db=db, socket_connect_timeout=2)
        client.ping()
        client.close()
        return {
            "service": "redis",
            "tier": "hot",
            "status": "healthy",
            "host": host,
            "port": port,
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.warning(f"Health check failed: {e}")
        raise Exception(f"Redis health check failed: {e}")
