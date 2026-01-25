"""Pydantic models for market data - single source of truth for data format.

All storage (Redis, Postgres) and API layers should use these models
to ensure consistent data format across the system.
"""

from datetime import datetime
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class Kline(BaseModel):
    """1-minute OHLCV candlestick data (aggregated from trades)."""

    model_config = ConfigDict(from_attributes=True)

    timestamp: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trade_count: int
    buy_count: int
    sell_count: int
    # Optional fields
    interval: str = "1m"
    volume_weighted_avg_price: float | None = None
    price_change_percent: float | None = None
    buy_sell_ratio: float | None = None
    average_price: float | None = None
    price_volatility: float | None = None

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> datetime:
        if isinstance(v, datetime):
            return v.replace(tzinfo=None) if v.tzinfo else v
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00")).replace(tzinfo=None)
        if isinstance(v, (int, float)):
            # Assume epoch seconds if < 1e12, else milliseconds
            ts = v / 1000 if v > 1e12 else v
            return datetime.fromtimestamp(ts)
        raise ValueError(f"Cannot parse timestamp: {v}")

    @field_validator("open", "high", "low", "close", "volume", "quote_volume", mode="before")
    @classmethod
    def parse_float(cls, v: Any) -> float:
        return float(v) if v is not None else 0.0

    @field_validator("trade_count", "buy_count", "sell_count", mode="before")
    @classmethod
    def parse_int(cls, v: Any) -> int:
        return int(float(v)) if v is not None else 0

    def to_redis_dict(self) -> dict[str, str]:
        """Serialize to Redis hash (all string values)."""
        data = self.model_dump(exclude_none=True)
        data["timestamp"] = self.timestamp.isoformat()
        return {k: str(v) for k, v in data.items()}

    @classmethod
    def from_redis_dict(cls, data: dict[str, str]) -> Self:
        """Deserialize from Redis hash."""
        return cls.model_validate(data)

    def to_pg_dict(self) -> dict[str, Any]:
        """Serialize for Postgres (native types)."""
        return self.model_dump(exclude_none=True)

    @classmethod
    def db_fields(cls) -> list[str]:
        """Fields for database operations."""
        return [
            "timestamp", "symbol", "open", "high", "low", "close",
            "volume", "quote_volume", "trade_count", "buy_count", "sell_count",
            "volume_weighted_avg_price", "price_change_percent", "buy_sell_ratio",
            "average_price", "price_volatility",
        ]


class Alert(BaseModel):
    """Anomaly alert (volume spike, price spike, etc.)."""

    model_config = ConfigDict(from_attributes=True)

    timestamp: datetime
    symbol: str
    alert_type: str  # VOLUME_SPIKE, PRICE_SPIKE, TRADE_COUNT_SPIKE, BUY_SELL_IMBALANCE
    alert_level: str  # HIGH, MEDIUM, LOW
    message: str = ""
    details: dict[str, Any] = {}
    # Optional
    alert_id: str | None = None
    created_at: datetime | None = None

    @field_validator("timestamp", "created_at", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> datetime | None:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v.replace(tzinfo=None) if v.tzinfo else v
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00")).replace(tzinfo=None)
        if isinstance(v, (int, float)):
            ts = v / 1000 if v > 1e12 else v
            return datetime.fromtimestamp(ts)
        raise ValueError(f"Cannot parse timestamp: {v}")

    @field_validator("details", mode="before")
    @classmethod
    def parse_details(cls, v: Any) -> dict[str, Any]:
        if isinstance(v, dict):
            return v
        if isinstance(v, str):
            import json
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                return {}
        return {}

    @model_validator(mode="after")
    def set_defaults(self) -> Self:
        if not self.message:
            self.message = f"{self.alert_type}: {self.symbol}"
        if not self.created_at:
            self.created_at = datetime.now()
        return self

    def to_redis_dict(self) -> dict[str, str]:
        """Serialize to Redis (all string values, details as JSON)."""
        import json
        data = {
            "timestamp": self.timestamp.isoformat(),
            "symbol": self.symbol,
            "alert_type": self.alert_type,
            "alert_level": self.alert_level,
            "message": self.message,
            "details": json.dumps(self.details),
        }
        if self.created_at:
            data["created_at"] = self.created_at.isoformat()
        return data

    @classmethod
    def from_redis_dict(cls, data: dict[str, Any]) -> Self:
        """Deserialize from Redis."""
        return cls.model_validate(data)

    def to_pg_dict(self) -> dict[str, Any]:
        """Serialize for Postgres."""
        import json
        return {
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "alert_type": self.alert_type,
            "severity": self.alert_level,  # Postgres uses 'severity' column
            "message": self.message,
            "metadata": json.dumps(self.details),  # Postgres uses 'metadata' column
        }

    @classmethod
    def from_pg_dict(cls, data: dict[str, Any]) -> Self:
        """Deserialize from Postgres row."""
        return cls.model_validate({
            "timestamp": data["timestamp"],
            "symbol": data["symbol"],
            "alert_type": data["alert_type"],
            "alert_level": data.get("severity", data.get("alert_level", "MEDIUM")),
            "message": data.get("message", ""),
            "details": data.get("metadata", data.get("details", {})),
        })

    @classmethod
    def db_fields(cls) -> list[str]:
        """Fields for database operations (Postgres column names)."""
        return ["timestamp", "symbol", "alert_type", "severity", "message", "metadata"]


class Ticker(BaseModel):
    """Real-time ticker data from Binance WebSocket."""

    model_config = ConfigDict(from_attributes=True)

    symbol: str
    last_price: float
    price_change: float
    price_change_pct: float
    open: float
    high: float
    low: float
    volume: float
    quote_volume: float
    trade_count: int
    updated_at: datetime

    # Binance short names -> our field names
    _BINANCE_MAP = {
        "c": "last_price",
        "p": "price_change",
        "P": "price_change_pct",
        "o": "open",
        "h": "high",
        "l": "low",
        "v": "volume",
        "q": "quote_volume",
        "n": "trade_count",
        "E": "updated_at",
    }

    @model_validator(mode="before")
    @classmethod
    def normalize_binance_fields(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Map Binance short field names to our standard names."""
        if not isinstance(data, dict):
            return data
        result = {}
        for key, value in data.items():
            # Map short name to long name if exists
            new_key = cls._BINANCE_MAP.get(key, key)
            # Don't overwrite if long name already set
            if new_key not in result:
                result[new_key] = value
        return result

    @field_validator("updated_at", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> datetime:
        if isinstance(v, datetime):
            return v.replace(tzinfo=None) if v.tzinfo else v
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00")).replace(tzinfo=None)
            except ValueError:
                # Try parsing as epoch
                return datetime.fromtimestamp(float(v) / 1000 if float(v) > 1e12 else float(v))
        if isinstance(v, (int, float)):
            ts = v / 1000 if v > 1e12 else v
            return datetime.fromtimestamp(ts)
        raise ValueError(f"Cannot parse timestamp: {v}")

    @field_validator(
        "last_price", "price_change", "price_change_pct",
        "open", "high", "low", "volume", "quote_volume",
        mode="before"
    )
    @classmethod
    def parse_float(cls, v: Any) -> float:
        return float(v) if v is not None else 0.0

    @field_validator("trade_count", mode="before")
    @classmethod
    def parse_int(cls, v: Any) -> int:
        return int(float(v)) if v is not None else 0

    def to_redis_dict(self) -> dict[str, str]:
        """Serialize to Redis hash."""
        return {
            "symbol": self.symbol,
            "last_price": str(self.last_price),
            "price_change": str(self.price_change),
            "price_change_pct": str(self.price_change_pct),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "volume": str(self.volume),
            "quote_volume": str(self.quote_volume),
            "trade_count": str(self.trade_count),
            "updated_at": str(int(self.updated_at.timestamp() * 1000)),
        }

    @classmethod
    def from_redis_dict(cls, data: dict[str, str]) -> Self:
        """Deserialize from Redis hash."""
        return cls.model_validate(data)

    @property
    def is_complete(self) -> bool:
        """Check if ticker has all required data."""
        return self.trade_count > 0 and self.quote_volume > 0


class Trade(BaseModel):
    """Individual trade from Binance."""

    model_config = ConfigDict(from_attributes=True)

    symbol: str
    trade_id: int
    price: float
    quantity: float
    timestamp: datetime
    side: str  # BUY or SELL

    @model_validator(mode="before")
    @classmethod
    def normalize_kafka_fields(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Map Kafka/Binance field names to our standard names."""
        if not isinstance(data, dict):
            return data
        result = dict(data)
        # Map trade_time -> timestamp
        if "trade_time" in result and "timestamp" not in result:
            result["timestamp"] = result.pop("trade_time")
        # Map is_buyer_maker -> side
        if "is_buyer_maker" in result and "side" not in result:
            result["side"] = "SELL" if result.pop("is_buyer_maker") else "BUY"
        return result

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> datetime:
        if isinstance(v, datetime):
            return v.replace(tzinfo=None) if v.tzinfo else v
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00")).replace(tzinfo=None)
        if isinstance(v, (int, float)):
            ts = v / 1000 if v > 1e12 else v
            return datetime.fromtimestamp(ts)
        raise ValueError(f"Cannot parse timestamp: {v}")

    @field_validator("price", "quantity", mode="before")
    @classmethod
    def parse_float(cls, v: Any) -> float:
        return float(v) if v is not None else 0.0

    @field_validator("trade_id", mode="before")
    @classmethod
    def parse_int(cls, v: Any) -> int:
        return int(v) if v is not None else 0

    @property
    def total(self) -> float:
        """Total value of trade (price * quantity)."""
        return self.price * self.quantity

    def to_redis_dict(self) -> dict[str, str]:
        """Serialize to Redis."""
        return {
            "symbol": self.symbol,
            "trade_id": str(self.trade_id),
            "price": str(self.price),
            "quantity": str(self.quantity),
            "timestamp": self.timestamp.isoformat(),
            "side": self.side,
        }

    @classmethod
    def from_redis_dict(cls, data: dict[str, str]) -> Self:
        """Deserialize from Redis."""
        return cls.model_validate(data)
