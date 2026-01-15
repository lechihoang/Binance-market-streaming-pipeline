from datetime import datetime

from pydantic import BaseModel, Field


class TickerDataResponse(BaseModel):
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
    complete: bool


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


class RecentTradeResponse(BaseModel):
    trade_id: int
    timestamp: int
    price: float
    quantity: float
    side: str  # BUY or SELL
    total: float


class KlineResponse(BaseModel):
    """OHLCV kline data for a time interval.

    Kline: Binance term for candlestick/OHLCV data.
    OHLCV: Open, High, Low, Close, Volume - standard price format.
    """

    timestamp: datetime = Field(description="Kline interval start time")
    open: float = Field(description="Opening price (OHLCV)")
    high: float = Field(description="Highest price during interval (OHLCV)")
    low: float = Field(description="Lowest price during interval (OHLCV)")
    close: float = Field(description="Closing price (OHLCV)")
    volume: float = Field(description="Total volume traded (OHLCV)")
    quote_volume: float | None = Field(default=None, description="Total value traded (price × quantity sum)")
    trade_count: int | None = Field(default=None, description="Number of trades in interval")
    buy_count: int | None = Field(default=None, description="Number of buyer-initiated trades")
    sell_count: int | None = Field(default=None, description="Number of seller-initiated trades")


class TradesCountResponse(BaseModel):
    timestamp: datetime
    trade_count: int
    interval: str


class PriceSpikeResponse(BaseModel):
    timestamp: datetime
    symbol: str
    open_price: float
    close_price: float
    price_change_pct: float


class VolumeSpikeResponse(BaseModel):
    timestamp: datetime
    symbol: str
    volume: float
    quote_volume: float
    trade_count: int


class TradeCountSpikeResponse(BaseModel):
    timestamp: datetime
    symbol: str
    trade_count: int
    buy_count: int
    sell_count: int


class BuySellImbalanceResponse(BaseModel):
    timestamp: datetime
    symbol: str
    buy_count: int
    sell_count: int
    buy_sell_ratio: float
    imbalance_direction: str  # BUY_HEAVY or SELL_HEAVY


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
