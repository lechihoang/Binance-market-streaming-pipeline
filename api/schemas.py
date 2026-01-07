from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


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
    tickers: List[TickerDataResponse]
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


class KlineResponse(BaseModel):
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: Optional[float] = None
    trade_count: Optional[int] = None
    buy_count: Optional[int] = None
    sell_count: Optional[int] = None


class TradesCountResponse(BaseModel):
    timestamp: datetime
    trade_count: int
    interval: str


class WhaleAlertResponse(BaseModel):
    timestamp: datetime
    symbol: str
    side: str
    amount: float
    price: float
    total_value: float


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


class ServiceHealth(BaseModel):
    name: str
    healthy: bool
    latency_ms: Optional[float] = None
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    redis: bool
    postgres: bool
    timestamp: datetime
    services: Optional[List[ServiceHealth]] = None


class TierStatusResponse(BaseModel):
    tier: str
    last_run: Optional[str] = None
    success: bool
    records_affected: int = 0
    bytes_reclaimed: int = 0
    error: Optional[str] = None


class LifecycleHealthResponse(BaseModel):
    last_run: Optional[str] = None
    overall_success: bool
    tiers: List[TierStatusResponse] = []


class MLPredictionResponse(BaseModel):
    symbol: str
    timestamp: str
    current_volatility: float
    predicted_volatility_5m: float
    volatility_level: str  # LOW / MEDIUM / HIGH


class MLStatusResponse(BaseModel):
    model_loaded: bool
    model_info: Optional[dict] = None
    error: Optional[str] = None
