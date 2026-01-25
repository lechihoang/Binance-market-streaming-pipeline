"""API response schemas - extends base models from schema.market."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, computed_field, field_validator, model_validator

from schema.market import Alert, Kline, Ticker, Trade


def _to_ms(v: Any) -> int:
    """Convert various timestamp formats to milliseconds."""
    if isinstance(v, datetime):
        return int(v.timestamp() * 1000)
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str):
        try:
            return int(float(v))
        except ValueError:
            pass
    return 0


# ==========================================
# Ticker Responses
# ==========================================


class TickerDataResponse(BaseModel):
    """API response for ticker data (extends base Ticker model)."""

    symbol: str
    last_price: str
    price_change: str
    price_change_pct: str
    open: str
    high: str
    low: str
    volume: str
    quote_volume: str
    trade_count: int
    updated_at: int
    complete: bool = False

    @field_validator("updated_at", mode="before")
    @classmethod
    def parse_updated_at(cls, v: Any) -> int:
        return _to_ms(v)

    @model_validator(mode="before")
    @classmethod
    def check_complete(cls, values: dict[str, Any]) -> dict[str, Any]:
        required = {"trade_count", "quote_volume"}
        is_complete = all(
            values.get(f) not in (None, "", "0", 0)
            for f in required
        )
        values["complete"] = is_complete
        return values

    @classmethod
    def from_ticker(cls, ticker: Ticker) -> "TickerDataResponse":
        """Create from base Ticker model."""
        return cls(
            symbol=ticker.symbol,
            last_price=str(ticker.last_price),
            price_change=str(ticker.price_change),
            price_change_pct=str(ticker.price_change_pct),
            open=str(ticker.open),
            high=str(ticker.high),
            low=str(ticker.low),
            volume=str(ticker.volume),
            quote_volume=str(ticker.quote_volume),
            trade_count=ticker.trade_count,
            updated_at=int(ticker.updated_at.timestamp() * 1000),
            complete=ticker.is_complete,
        )


class TickerListResponse(BaseModel):
    tickers: list[TickerDataResponse]
    count: int
    timestamp: int


class TickerHealthResponse(BaseModel):
    status: str
    redis_connected: bool
    ticker_count: int
    latency_ms: float
    timestamp: int


class MarketSummaryResponse(BaseModel):
    total_symbols: int
    total_trades: int
    total_quote_volume: float
    avg_trade_value: float
    timestamp: int


class TopTradingResponse(BaseModel):
    symbol: str
    last_price: float
    trade_count: int
    quote_volume: float


# ==========================================
# Trade Responses
# ==========================================


class RecentTradeResponse(BaseModel):
    """API response for recent trades."""

    trade_id: int
    timestamp: int
    price: float
    quantity: float
    side: str
    total: float

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Any) -> int:
        return _to_ms(v)

    @model_validator(mode="before")
    @classmethod
    def calculate_total(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "total" not in values:
            price = float(values.get("price", 0))
            qty = float(values.get("quantity", 0))
            values["total"] = price * qty
        return values

    @classmethod
    def from_trade(cls, trade: Trade) -> "RecentTradeResponse":
        """Create from base Trade model."""
        return cls(
            trade_id=trade.trade_id,
            timestamp=int(trade.timestamp.timestamp() * 1000),
            price=trade.price,
            quantity=trade.quantity,
            side=trade.side,
            total=trade.total,
        )


# ==========================================
# Kline Responses
# ==========================================


class KlineResponse(BaseModel):
    """OHLCV kline response - mirrors base Kline model."""

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
    """Price spike alert response."""

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
    """Volume spike alert response."""

    timestamp: datetime
    symbol: str
    volume: float
    quote_volume: float
    trade_count: int

    @classmethod
    def from_alert(cls, alert: Alert) -> "VolumeSpikeResponse":
        """Create from base Alert model."""
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
    """Buy/sell imbalance alert response."""

    timestamp: datetime
    symbol: str
    buy_count: int
    sell_count: int
    buy_sell_ratio: float
    imbalance_direction: str

    @classmethod
    def from_alert(cls, alert: Alert) -> "BuySellImbalanceResponse":
        """Create from base Alert model."""
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
