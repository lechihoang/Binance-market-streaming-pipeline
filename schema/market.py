from datetime import datetime
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from util.constant import TICKER_BINANCE_MAP


class Kline(BaseModel):
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
            return v
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "").replace("+00:00", ""))
        if isinstance(v, (int, float)):
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
        data = self.model_dump(exclude_none=True)
        data["timestamp"] = self.timestamp.isoformat()
        return {k: str(v) for k, v in data.items()}

    @classmethod
    def from_redis_dict(cls, data: dict[str, str]) -> Self:
        return cls.model_validate(data)

    def to_pg_dict(self) -> dict[str, Any]:
        return self.model_dump(exclude_none=True)

    @classmethod
    def db_fields(cls) -> list[str]:
        return [
            "timestamp",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "trade_count",
            "buy_count",
            "sell_count",
            "volume_weighted_avg_price",
            "price_change_percent",
            "buy_sell_ratio",
            "average_price",
            "price_volatility",
        ]


class Alert(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    timestamp: datetime
    symbol: str
    alert_type: str
    alert_level: str
    message: str = ""
    details: dict[str, Any] = {}
    alert_id: str | None = None
    created_at: datetime | None = None

    @field_validator("timestamp", "created_at", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> datetime | None:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "").replace("+00:00", ""))
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
    def from_redis_dict(cls, data: dict[str, str]) -> Self:
        return cls.model_validate(data)

    @property
    def is_complete(self) -> bool:
        return self.trade_count > 0 and self.quote_volume > 0


class Trade(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    symbol: str
    trade_id: int
    price: float
    quantity: float
    timestamp: datetime
    side: str

    @model_validator(mode="before")
    @classmethod
    def normalize_kafka_fields(cls, data: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(data, dict):
            return data
        result = dict(data)
        if "trade_time" in result and "timestamp" not in result:
            result["timestamp"] = result.pop("trade_time")
        if "is_buyer_maker" in result and "side" not in result:
            result["side"] = "SELL" if result.pop("is_buyer_maker") else "BUY"
        return result

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> datetime:
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "").replace("+00:00", ""))
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
        return self.price * self.quantity

    def to_redis_dict(self) -> dict[str, str]:
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
        return cls.model_validate(data)
