from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from schema.market import Alert, Kline, Ticker

# ==========================================
# Ticker Responses
# ==========================================


class TickerDataResponse(BaseModel):
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
    complete: bool = False

    @field_validator(
        "last_price", "price_change", "price_change_pct", "open", "high", "low", "volume", "quote_volume", mode="before"
    )
    @classmethod
    def ensure_float(cls, v: Any) -> float:
        return float(v) if v is not None else 0.0

    @model_validator(mode="before")
    @classmethod
    def check_complete(cls, values: dict[str, Any]) -> dict[str, Any]:
        required = {"trade_count", "quote_volume"}
        is_complete = all(values.get(f) not in (None, "", "0", 0, 0.0) for f in required)
        values["complete"] = is_complete
        return values

    @classmethod
    def from_ticker(cls, ticker: Ticker) -> "TickerDataResponse":
        """Create from base Ticker model."""
        return cls(
            symbol=ticker.symbol,
            last_price=ticker.last_price,
            price_change=ticker.price_change,
            price_change_pct=ticker.price_change_pct,
            open=ticker.open,
            high=ticker.high,
            low=ticker.low,
            volume=ticker.volume,
            quote_volume=ticker.quote_volume,
            trade_count=ticker.trade_count,
            updated_at=ticker.updated_at,
            complete=ticker.is_complete,
        )


class TickerListResponse(BaseModel):
    tickers: list[TickerDataResponse]
    count: int
    timestamp: datetime


class TickerHealthResponse(BaseModel):
    status: str
    redis_connected: bool
    ticker_count: int
    latency_ms: float
    timestamp: datetime


class MarketSummaryResponse(BaseModel):
    total_symbols: int
    total_trades: int
    total_quote_volume: float
    avg_trade_value: float
    timestamp: datetime


class TopTradingResponse(BaseModel):
    symbol: str
    last_price: float
    trade_count: int
    quote_volume: float


class RecentTradeResponse(BaseModel):

    trade_id: int
    timestamp: datetime
    price: float
    quantity: float
    side: str
    total: float

    @model_validator(mode="before")
    @classmethod
    def calculate_total(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "total" not in values:
            price = float(values.get("price", 0))
            qty = float(values.get("quantity", 0))
            values["total"] = price * qty
        return values


# ==========================================
# Kline Responses
# ==========================================


class KlineResponse(BaseModel):
    timestamp: datetime = Field(description="Kline interval start time")
    open: float = Field(description="Opening price")
    high: float = Field(description="Highest price")
    low: float = Field(description="Lowest price")
    close: float = Field(description="Closing price")
    volume: float = Field(description="Total volume traded")
    quote_volume: float | None = Field(default=None, description="Total value traded")
    trade_count: int | None = Field(default=None, description="Number of trades")
    buy_count: int | None = Field(default=None, description="Buyer-initiated trades")
    sell_count: int | None = Field(default=None, description="Seller-initiated trades")

    @classmethod
    def from_kline(cls, kline: Kline) -> "KlineResponse":
        """Create from base Kline model."""
        return cls(
            timestamp=kline.timestamp,
            open=kline.open,
            high=kline.high,
            low=kline.low,
            close=kline.close,
            volume=kline.volume,
            quote_volume=kline.quote_volume,
            trade_count=kline.trade_count,
            buy_count=kline.buy_count,
            sell_count=kline.sell_count,
        )


class TradesCountResponse(BaseModel):
    timestamp: datetime
    trade_count: int
    interval: str


# ==========================================
# Alert Responses
# ==========================================


class PriceSpikeResponse(BaseModel):

    timestamp: datetime
    symbol: str
    open_price: float
    close_price: float
    price_change_pct: float

    @classmethod
    def from_alert(cls, alert: Alert) -> "PriceSpikeResponse":
        """Create from base Alert model."""
        return cls(
            timestamp=alert.timestamp,
            symbol=alert.symbol,
            open_price=float(alert.details.get("open", 0)),
            close_price=float(alert.details.get("close", 0)),
            price_change_pct=float(alert.details.get("price_change_pct", 0)),
        )


class VolumeSpikeResponse(BaseModel):

    timestamp: datetime
    symbol: str
    volume: float
    quote_volume: float
    trade_count: int

    @classmethod
    def from_alert(cls, alert: Alert) -> "VolumeSpikeResponse":
        return cls(
            timestamp=alert.timestamp,
            symbol=alert.symbol,
            volume=float(alert.details.get("volume", 0)),
            quote_volume=float(alert.details.get("quote_volume", 0)),
            trade_count=int(alert.details.get("trade_count", 0)),
        )


class TradeCountSpikeResponse(BaseModel):
    """Trade count spike alert response."""

    timestamp: datetime
    symbol: str
    trade_count: int
    buy_count: int
    sell_count: int

    @classmethod
    def from_alert(cls, alert: Alert) -> "TradeCountSpikeResponse":
        """Create from base Alert model."""
        return cls(
            timestamp=alert.timestamp,
            symbol=alert.symbol,
            trade_count=int(alert.details.get("trade_count", 0)),
            buy_count=int(alert.details.get("buy_count", 0)),
            sell_count=int(alert.details.get("sell_count", 0)),
        )


class BuySellImbalanceResponse(BaseModel):

    timestamp: datetime
    symbol: str
    buy_count: int
    sell_count: int
    buy_sell_ratio: float
    imbalance_direction: str

    @classmethod
    def from_alert(cls, alert: Alert) -> "BuySellImbalanceResponse":
        ratio = float(alert.details.get("buy_sell_ratio", 0))
        return cls(
            timestamp=alert.timestamp,
            symbol=alert.symbol,
            buy_count=int(alert.details.get("buy_count", 0)),
            sell_count=int(alert.details.get("sell_count", 0)),
            buy_sell_ratio=ratio,
            imbalance_direction="BUY_HEAVY" if ratio > 1 else "SELL_HEAVY",
        )


# ==========================================
# System Responses
# ==========================================


class ServiceHealth(BaseModel):
    name: str
    healthy: bool
    latency_ms: float | None = None
    error: str | None = None


class HealthResponse(BaseModel):
    status: str
    redis: bool
    postgres: bool
    timestamp: datetime
    services: list[ServiceHealth] | None = None


class TierStatusResponse(BaseModel):
    tier: str
    last_run: str | None = None
    success: bool
    records_affected: int = 0
    bytes_reclaimed: int = 0
    error: str | None = None


class LifecycleHealthResponse(BaseModel):
    last_run: str | None = None
    overall_success: bool
    tiers: list[TierStatusResponse] = []


# ==========================================
# ML Responses
# ==========================================


class MLPredictionResponse(BaseModel):
    symbol: str
    timestamp: str
    current_volatility: float
    predicted_volatility_5m: float
    volatility_level: str  # LOW / MEDIUM / HIGH


class MLStatusResponse(BaseModel):
    model_loaded: bool
    model_info: dict | None = None
    error: str | None = None
