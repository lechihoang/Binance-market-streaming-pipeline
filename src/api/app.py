"""
FastAPI Application - Crypto Data API (Simplified).

Consolidated REST API for cryptocurrency data from Three-Tier Storage.
All endpoints, models, and dependencies in a single file.
"""

import json as json_module
import os
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from threading import Lock
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Depends, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from prometheus_fastapi_instrumentator import Instrumentator

from src.storage.redis import RedisStorage, RedisTickerStorage
from src.storage.postgres import PostgresStorage
from src.storage.minio import MinioStorage
from src.storage.query_router import QueryRouter
from src.utils.logging import get_logger


logger = get_logger(__name__)


# ============================================================================
# RATE LIMITING CONFIGURATION
# ============================================================================

RATE_LIMIT_PER_MINUTE = 100
RATE_LIMIT_WINDOW_SECONDS = 60

limiter = Limiter(key_func=get_remote_address, default_limits=["100/minute"])


class RateLimitTracker:
    """Track rate limits per IP for accurate header reporting."""
    
    def __init__(self, limit: int = 100, window_seconds: int = 60):
        self.limit = limit
        self.window_seconds = window_seconds
        self._requests: dict[str, list[float]] = defaultdict(list)
        self._lock = Lock()
    
    def record_request(self, ip: str) -> tuple[int, int, int]:
        """Record a request and return (limit, remaining, reset_time)."""
        now = time.time()
        window_start = now - self.window_seconds
        
        with self._lock:
            self._requests[ip] = [
                ts for ts in self._requests[ip] if ts > window_start
            ]
            self._requests[ip].append(now)
            count = len(self._requests[ip])
            remaining = max(0, self.limit - count)
            
            if self._requests[ip]:
                oldest_in_window = min(self._requests[ip])
                reset_time = int(oldest_in_window + self.window_seconds)
            else:
                reset_time = int(now + self.window_seconds)
            
            return self.limit, remaining, reset_time
    
    def is_rate_limited(self, ip: str) -> bool:
        """Check if IP is currently rate limited."""
        now = time.time()
        window_start = now - self.window_seconds
        
        with self._lock:
            self._requests[ip] = [
                ts for ts in self._requests[ip] if ts > window_start
            ]
            return len(self._requests[ip]) >= self.limit
    
    def get_retry_after(self, ip: str) -> int:
        """Get seconds until rate limit resets for an IP."""
        now = time.time()
        window_start = now - self.window_seconds
        
        with self._lock:
            self._requests[ip] = [
                ts for ts in self._requests[ip] if ts > window_start
            ]
            if self._requests[ip]:
                oldest = min(self._requests[ip])
                return max(1, int((oldest + self.window_seconds) - now))
            return 0


rate_tracker = RateLimitTracker(
    limit=RATE_LIMIT_PER_MINUTE, 
    window_seconds=RATE_LIMIT_WINDOW_SECONDS
)


def custom_rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    """Custom handler for rate limit exceeded errors."""
    ip = get_remote_address(request)
    retry_after = rate_tracker.get_retry_after(ip)
    limit, remaining, reset_time = rate_tracker.record_request(ip)
    
    return JSONResponse(
        status_code=429,
        content={
            "detail": "Rate limit exceeded",
            "retry_after": retry_after,
        },
        headers={
            "Retry-After": str(retry_after),
            "X-RateLimit-Limit": str(limit),
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": str(reset_time),
        },
    )


# ============================================================================
# FASTAPI APPLICATION SETUP
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown."""
    app.state.start_time = time.time()
    yield


app = FastAPI(
    title="Crypto Data API",
    description="""
REST API for cryptocurrency market data from Three-Tier Storage.

## Features

* **Market Data** - Real-time ticker, price, and trade data from Redis
* **Analytics** - Historical klines from PostgreSQL/MinIO
* **Alerts** - Whale alerts and anomaly detection results
* **System Monitoring** - Health checks and Prometheus metrics

## Data Sources

The API automatically routes queries to the appropriate storage tier:
- **Redis**: Real-time data (last 1 hour)
- **PostgreSQL**: Interactive analytics (last 90 days)
- **MinIO**: Historical data (older than 90 days)
""",
    version="1.0.0",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "market", "description": "Real-time market data endpoints"},
        {"name": "analytics", "description": "Historical analytics - klines, trades count"},
        {"name": "alerts", "description": "Whale alerts"},
        {"name": "system", "description": "System health and status"},
    ],
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, custom_rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Rate limiting middleware with accurate header tracking."""
    ip = get_remote_address(request)
    
    path = request.url.path
    if path in ["/", "/docs", "/redoc", "/openapi.json", "/api/v1/system/health"]:
        response = await call_next(request)
        return response
    
    if rate_tracker.is_rate_limited(ip):
        retry_after = rate_tracker.get_retry_after(ip)
        _, _, reset_time = rate_tracker.record_request(ip)
        return JSONResponse(
            status_code=429,
            content={
                "detail": "Rate limit exceeded",
                "retry_after": retry_after,
            },
            headers={
                "Retry-After": str(retry_after),
                "X-RateLimit-Limit": str(RATE_LIMIT_PER_MINUTE),
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(reset_time),
            },
        )
    
    limit, remaining, reset_time = rate_tracker.record_request(ip)
    response: Response = await call_next(request)
    response.headers["X-RateLimit-Limit"] = str(limit)
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    response.headers["X-RateLimit-Reset"] = str(reset_time)
    
    return response


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Crypto Data API", "version": "1.0.0"}


# Prometheus metrics instrumentation
Instrumentator().instrument(app).expose(app, endpoint="/metrics")


# ============================================================================
# RESPONSE MODELS (Inline)
# ============================================================================

class TickerDataResponse(BaseModel):
    """Real-time ticker response model."""
    symbol: str
    last_price: str
    price_change: str
    price_change_pct: str
    open: str
    high: str
    low: str
    volume: str
    quote_volume: str
    trades_count: int
    updated_at: int
    complete: bool


class TickerListResponse(BaseModel):
    """Response model for multiple tickers."""
    tickers: List[TickerDataResponse]
    count: int
    timestamp: int


class TickerHealthResponse(BaseModel):
    """Health check response for ticker service."""
    status: str
    redis_connected: bool
    ticker_count: int
    latency_ms: float
    timestamp: int


class MarketSummaryResponse(BaseModel):
    """Response model for market summary statistics."""
    total_symbols: int
    total_trades: int
    total_quote_volume: float
    avg_trade_value: float
    timestamp: int


class TopTradingResponse(BaseModel):
    """Response model for top trading endpoints."""
    symbol: str
    last_price: float
    trades_count: int
    quote_volume: float


class KlineResponse(BaseModel):
    """OHLCV candlestick data."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: Optional[float] = None
    trades_count: Optional[int] = None


class TradesCountResponse(BaseModel):
    """Trades count aggregated by time interval."""
    timestamp: datetime
    trades_count: int
    interval: str


class WhaleAlertResponse(BaseModel):
    """Response model for whale alerts."""
    timestamp: datetime
    symbol: str
    side: str
    amount: float
    price: float
    total_value: float


class ServiceHealth(BaseModel):
    """Individual service health status."""
    name: str
    healthy: bool
    latency_ms: Optional[float] = None
    error: Optional[str] = None


class HealthResponse(BaseModel):
    """System health status."""
    status: str
    redis: bool
    postgres: bool
    timestamp: datetime
    services: Optional[List[ServiceHealth]] = None


# ============================================================================
# STORAGE DEPENDENCIES (Inline)
# ============================================================================

@lru_cache()
def get_redis() -> RedisStorage:
    """Get singleton RedisStorage instance."""
    return RedisStorage(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
    )


@lru_cache()
def get_postgres() -> PostgresStorage:
    """Get singleton PostgresStorage instance."""
    return PostgresStorage(
        host=os.getenv("POSTGRES_DATA_HOST", "postgres-data"),
        port=int(os.getenv("POSTGRES_DATA_PORT", "5432")),
        user=os.getenv("POSTGRES_DATA_USER", "crypto"),
        password=os.getenv("POSTGRES_DATA_PASSWORD", "crypto"),
        database=os.getenv("POSTGRES_DATA_DB", "crypto_data"),
    )


@lru_cache()
def get_minio() -> MinioStorage:
    """Get singleton MinioStorage instance."""
    return MinioStorage(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        bucket=os.getenv("MINIO_BUCKET", "crypto-data"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


@lru_cache()
def get_query_router() -> QueryRouter:
    """Get singleton QueryRouter instance."""
    return QueryRouter(
        redis=get_redis(),
        postgres=get_postgres(),
        minio=get_minio(),
    )


@lru_cache()
def get_ticker_storage() -> RedisTickerStorage:
    """Get singleton RedisTickerStorage instance."""
    return RedisTickerStorage(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        ttl_seconds=int(os.getenv("TICKER_TTL_SECONDS", "60")),
    )


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

OPTIONAL_FIELDS = {"trades_count", "quote_volume"}


def is_ticker_complete(data: dict) -> bool:
    """Check if ticker has all optional fields present and valid."""
    for field in OPTIONAL_FIELDS:
        if field not in data:
            return False
        value = data[field]
        if value is None or value == "" or value == "0":
            return False
    return True


def format_ticker_response(data: dict) -> TickerDataResponse:
    """Format ticker data to API response model."""
    updated_at = data.get("updated_at", 0)
    if not updated_at or updated_at == 0:
        updated_at = int(time.time() * 1000)
    
    return TickerDataResponse(
        symbol=data.get("symbol", ""),
        last_price=str(data.get("last_price", "0")),
        price_change=str(data.get("price_change", "0")),
        price_change_pct=str(data.get("price_change_pct", "0")),
        open=str(data.get("open", "0")),
        high=str(data.get("high", "0")),
        low=str(data.get("low", "0")),
        volume=str(data.get("volume", "0")),
        quote_volume=str(data.get("quote_volume", "0")),
        trades_count=int(data.get("trades_count", 0)),
        updated_at=int(updated_at),
        complete=is_ticker_complete(data),
    )


def check_redis_health(redis: RedisStorage) -> ServiceHealth:
    """Check Redis connection health."""
    start = time.time()
    try:
        healthy = redis.ping()
        latency_ms = (time.time() - start) * 1000
        return ServiceHealth(
            name="redis",
            healthy=healthy,
            latency_ms=round(latency_ms, 2),
            error=None if healthy else "Ping failed"
        )
    except Exception as e:
        latency_ms = (time.time() - start) * 1000
        return ServiceHealth(
            name="redis",
            healthy=False,
            latency_ms=round(latency_ms, 2),
            error=str(e)
        )


def check_postgres_health(postgres: PostgresStorage) -> ServiceHealth:
    """Check PostgreSQL connection health."""
    start = time.time()
    try:
        postgres._execute_with_retry("SELECT 1", fetch=True)
        latency_ms = (time.time() - start) * 1000
        return ServiceHealth(
            name="postgres",
            healthy=True,
            latency_ms=round(latency_ms, 2),
            error=None
        )
    except Exception as e:
        latency_ms = (time.time() - start) * 1000
        return ServiceHealth(
            name="postgres",
            healthy=False,
            latency_ms=round(latency_ms, 2),
            error=str(e)
        )


def determine_overall_status(redis_healthy: bool, postgres_healthy: bool, kafka_healthy: bool) -> str:
    """Determine overall system health status.
    
    Args:
        redis_healthy: Redis connection status
        postgres_healthy: PostgreSQL connection status
        kafka_healthy: Kafka connection status (kept for backward compatibility)
        
    Returns:
        "healthy" if all services up, "degraded" if some down, "unhealthy" if all down
    """
    services = [redis_healthy, postgres_healthy, kafka_healthy]
    healthy_count = sum(services)
    
    if healthy_count == len(services):
        return "healthy"
    elif healthy_count == 0:
        return "unhealthy"
    else:
        return "degraded"


# Klines helper functions
VALID_INTERVALS = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
MAX_TIME_RANGE_DAYS = 365


def validate_time_range(start: datetime, end: datetime) -> None:
    """Validate that time range does not exceed 1 year."""
    if (end - start).days > MAX_TIME_RANGE_DAYS:
        raise HTTPException(
            status_code=400,
            detail="Time range exceeds maximum allowed (1 year)"
        )


def query_klines_with_fallback(
    query_router: QueryRouter,
    minio: MinioStorage,
    symbol: str,
    start: datetime,
    end: datetime,
) -> tuple[List[dict], str]:
    """Query klines with fallback chain."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    redis_cutoff = now - timedelta(hours=1)
    postgres_cutoff = now - timedelta(days=90)
    
    if start >= redis_cutoff:
        primary_tier = "redis"
    elif start >= postgres_cutoff:
        primary_tier = "postgres"
    else:
        primary_tier = "minio"
    
    try:
        data = query_router.query(
            data_type=QueryRouter.DATA_TYPE_KLINES,
            symbol=symbol,
            start=start,
            end=end,
        )
        if data:
            return data, primary_tier
    except Exception as e:
        logger.warning(f"QueryRouter failed for klines/{symbol}: {e}")
    
    try:
        data = minio.read_klines(symbol, start, end)
        if data:
            return data, "minio"
    except Exception as e:
        logger.warning(f"MinIO fallback failed for klines/{symbol}: {e}")
    
    return [], "none"


# Trades count intervals
VALID_TRADES_COUNT_INTERVALS = {"1m", "1h", "1d"}


# ============================================================================
# MARKET ENDPOINTS
# ============================================================================

@app.get("/api/v1/market/ticker-health", response_model=TickerHealthResponse, tags=["market"])
async def get_ticker_health(
    storage: RedisTickerStorage = Depends(get_ticker_storage),
) -> TickerHealthResponse:
    """Get ticker service health status."""
    start_time = time.time()
    
    redis_connected = storage.ping()
    ticker_count = len(storage.get_all_tickers()) if redis_connected else 0
    latency_ms = (time.time() - start_time) * 1000
    
    if redis_connected and ticker_count > 0:
        status = "healthy"
    elif redis_connected:
        status = "degraded"
    else:
        status = "unhealthy"
    
    return TickerHealthResponse(
        status=status,
        redis_connected=redis_connected,
        ticker_count=ticker_count,
        latency_ms=round(latency_ms, 2),
        timestamp=int(time.time() * 1000),
    )


@app.get("/api/v1/market/summary", response_model=MarketSummaryResponse, tags=["market"])
async def get_market_summary(
    response: Response,
    storage: RedisTickerStorage = Depends(get_ticker_storage),
) -> MarketSummaryResponse:
    """Get real-time market summary statistics."""
    start_time = time.time()
    
    all_tickers = storage.get_all_tickers()
    
    total_symbols = len(all_tickers)
    total_trades = 0
    total_quote_volume = 0.0
    
    for ticker in all_tickers:
        try:
            total_trades += int(ticker.get("trades_count", 0))
            total_quote_volume += float(ticker.get("quote_volume", 0))
        except (ValueError, TypeError):
            continue
    
    avg_trade_value = total_quote_volume / total_trades if total_trades > 0 else 0.0
    
    response_time_ms = (time.time() - start_time) * 1000
    response.headers["X-Response-Time-Ms"] = f"{response_time_ms:.2f}"
    response.headers["X-Data-Source"] = "redis"
    
    return MarketSummaryResponse(
        total_symbols=total_symbols,
        total_trades=total_trades,
        total_quote_volume=round(total_quote_volume, 2),
        avg_trade_value=round(avg_trade_value, 2),
        timestamp=int(time.time() * 1000),
    )


@app.get("/api/v1/market/realtime", response_model=TickerListResponse, tags=["market"])
async def get_all_realtime_tickers(
    response: Response,
    storage: RedisTickerStorage = Depends(get_ticker_storage),
) -> TickerListResponse:
    """Get all available ticker data."""
    start_time = time.time()
    
    tickers_data = storage.get_all_tickers()
    tickers = [format_ticker_response(data) for data in tickers_data]
    
    response_time_ms = (time.time() - start_time) * 1000
    response.headers["X-Response-Time-Ms"] = f"{response_time_ms:.2f}"
    response.headers["X-Data-Source"] = "redis"
    
    return TickerListResponse(
        tickers=tickers,
        count=len(tickers),
        timestamp=int(time.time() * 1000),
    )


@app.get("/api/v1/market/top-by-trades", response_model=List[TopTradingResponse], tags=["market"])
async def get_top_by_trades(
    response: Response,
    limit: int = Query(default=5, ge=1, le=50),
    storage: RedisTickerStorage = Depends(get_ticker_storage),
) -> List[TopTradingResponse]:
    """Get top symbols by trades count."""
    start_time = time.time()
    
    all_tickers = storage.get_all_tickers()
    
    results = []
    for ticker in all_tickers:
        try:
            symbol = ticker.get("symbol", "")
            if not symbol:
                continue
            
            last_price = float(ticker.get("last_price", 0))
            trades_count = int(ticker.get("trades_count", 0))
            quote_volume = float(ticker.get("quote_volume", 0))
            
            results.append(TopTradingResponse(
                symbol=symbol,
                last_price=last_price,
                trades_count=trades_count,
                quote_volume=quote_volume,
            ))
        except Exception as e:
            logger.warning(f"Failed to parse ticker data for top-by-trades: {e}")
            continue
    
    results.sort(key=lambda x: (x.trades_count, x.quote_volume), reverse=True)
    
    response_time_ms = (time.time() - start_time) * 1000
    response.headers["X-Response-Time-Ms"] = f"{response_time_ms:.2f}"
    
    return results[:limit]


@app.get("/api/v1/market/top-by-volume", response_model=List[TopTradingResponse], tags=["market"])
async def get_top_by_volume(
    response: Response,
    limit: int = Query(default=5, ge=1, le=50),
    storage: RedisTickerStorage = Depends(get_ticker_storage),
) -> List[TopTradingResponse]:
    """Get top symbols by quote volume."""
    start_time = time.time()
    
    all_tickers = storage.get_all_tickers()
    
    results = []
    for ticker in all_tickers:
        try:
            symbol = ticker.get("symbol", "")
            if not symbol:
                continue
            
            last_price = float(ticker.get("last_price", 0))
            trades_count = int(ticker.get("trades_count", 0))
            quote_volume = float(ticker.get("quote_volume", 0))
            
            results.append(TopTradingResponse(
                symbol=symbol,
                last_price=last_price,
                trades_count=trades_count,
                quote_volume=quote_volume,
            ))
        except Exception as e:
            logger.warning(f"Failed to parse ticker data for top-by-volume: {e}")
            continue
    
    results.sort(key=lambda x: (x.quote_volume, x.trades_count), reverse=True)
    
    response_time_ms = (time.time() - start_time) * 1000
    response.headers["X-Response-Time-Ms"] = f"{response_time_ms:.2f}"
    
    return results[:limit]


# ============================================================================
# ANALYTICS ENDPOINTS
# ============================================================================

@app.get("/api/v1/analytics/klines/{symbol}", response_model=List[KlineResponse], tags=["analytics"])
async def get_klines(
    symbol: str,
    response: Response,
    interval: str = Query(default="1m", description="Candle interval"),
    start: Optional[datetime] = Query(default=None, description="Start time"),
    end: Optional[datetime] = Query(default=None, description="End time"),
    query_router: QueryRouter = Depends(get_query_router),
    minio: MinioStorage = Depends(get_minio),
) -> List[KlineResponse]:
    """Get OHLCV klines for a symbol with multi-tier fallback."""
    if interval not in VALID_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid interval. Valid values: {', '.join(sorted(VALID_INTERVALS))}"
        )
    
    # Normalize timezone-aware datetimes to naive (UTC)
    start = (start.replace(tzinfo=None) if start and start.tzinfo else start)
    end = (end.replace(tzinfo=None) if end and end.tzinfo else end)
    
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if end is None:
        end = now
    if start is None:
        start = end - timedelta(hours=24)
    
    validate_time_range(start, end)
    
    data, data_source = query_klines_with_fallback(
        query_router, minio, symbol, start, end
    )
    
    response.headers["X-Data-Source"] = data_source
    
    return [
        KlineResponse(
            timestamp=record.get("timestamp").replace(tzinfo=timezone.utc),
            open=record.get("open", 0.0),
            high=record.get("high", 0.0),
            low=record.get("low", 0.0),
            close=record.get("close", 0.0),
            volume=record.get("volume", 0.0),
            quote_volume=record.get("quote_volume"),
            trades_count=record.get("trades_count"),
        )
        for record in data
    ]


@app.get("/api/v1/analytics/trades-count", response_model=List[TradesCountResponse], tags=["analytics"])
async def get_trades_count(
    response: Response,
    symbol: str = Query(description="Trading pair symbol (e.g., BTCUSDT)"),
    interval: str = Query(default="1h", description="Time interval (1m, 1h, 1d)"),
    limit: int = Query(default=24, ge=1, le=1000, description="Number of data points"),
    postgres: PostgresStorage = Depends(get_postgres),
) -> List[TradesCountResponse]:
    """Get trades count aggregated by time interval."""
    if interval not in VALID_TRADES_COUNT_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid interval. Valid values: {', '.join(sorted(VALID_TRADES_COUNT_INTERVALS))}"
        )
    
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    interval_durations = {
        "1m": timedelta(minutes=1),
        "1h": timedelta(hours=1),
        "1d": timedelta(days=1),
    }
    
    duration = interval_durations[interval]
    start = now - (duration * limit)
    end = now
    
    try:
        data = postgres.query_trades_count(symbol.upper(), start, end, interval)
        response.headers["X-Data-Source"] = "postgres"
        
        if len(data) > limit:
            data = data[-limit:]
        
        return [
            TradesCountResponse(
                timestamp=record["timestamp"].replace(tzinfo=timezone.utc),
                trades_count=record["trades_count"],
                interval=record["interval"],
            )
            for record in data
        ]
    except Exception as e:
        logger.warning(f"Failed to query trades count for {symbol}: {e}")
        response.headers["X-Data-Source"] = "none"
        return []


@app.get("/api/v1/analytics/alerts/whale-alerts", response_model=List[WhaleAlertResponse], tags=["alerts"])
async def get_whale_alerts(
    limit: int = Query(default=50, ge=1, le=500),
    redis: RedisStorage = Depends(get_redis),
    postgres: PostgresStorage = Depends(get_postgres),
) -> List[WhaleAlertResponse]:
    """Get whale alerts (large trades)."""
    effective_limit = min(limit, 500)
    whale_alerts = []
    
    # Try Redis first for recent alerts
    try:
        alerts = redis.get_recent_alerts(limit=effective_limit * 2)
        for alert in alerts:
            if alert.get("alert_type", "").upper() in ("WHALE", "WHALE_ALERT"):
                details = alert.get("details", alert.get("metadata", {}))
                if isinstance(details, str):
                    try:
                        details = json_module.loads(details)
                    except:
                        details = {}
                
                whale_alerts.append(WhaleAlertResponse(
                    timestamp=alert.get("timestamp").replace(tzinfo=timezone.utc),
                    symbol=alert.get("symbol", "UNKNOWN"),
                    side=details.get("side", "BUY"),
                    amount=float(details.get("quantity", details.get("amount", 0))),
                    price=float(details.get("price", 0)),
                    total_value=float(details.get("value", details.get("total_value", 0))),
                ))
    except Exception:
        pass
    
    # If not enough from Redis, try PostgreSQL
    if len(whale_alerts) < effective_limit:
        try:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            start = now - timedelta(days=7)
            
            for symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]:
                pg_alerts = postgres.query_alerts(symbol, start, now)
                for alert in pg_alerts:
                    if alert.get("alert_type", "").upper() in ("WHALE", "WHALE_ALERT"):
                        details = alert.get("metadata", alert.get("details", {}))
                        if isinstance(details, str):
                            try:
                                details = json_module.loads(details)
                            except:
                                details = {}
                        
                        whale_alerts.append(WhaleAlertResponse(
                            timestamp=alert.get("timestamp").replace(tzinfo=timezone.utc),
                            symbol=alert.get("symbol", "UNKNOWN"),
                            side=details.get("side", "BUY"),
                            amount=float(details.get("quantity", details.get("amount", 0))),
                            price=float(details.get("price", 0)),
                            total_value=float(details.get("value", details.get("total_value", 0))),
                        ))
        except Exception:
            pass
    
    whale_alerts.sort(key=lambda x: x.timestamp, reverse=True)
    return whale_alerts[:effective_limit]


# ============================================================================
# SYSTEM ENDPOINTS
# ============================================================================

@app.get("/api/v1/system/health", response_model=HealthResponse, tags=["system"])
async def get_health(
    redis: RedisStorage = Depends(get_redis),
    postgres: PostgresStorage = Depends(get_postgres),
) -> HealthResponse:
    """Get system health status."""
    redis_health = check_redis_health(redis)
    postgres_health = check_postgres_health(postgres)
    
    if redis_health.healthy and postgres_health.healthy:
        overall_status = "healthy"
    elif redis_health.healthy or postgres_health.healthy:
        overall_status = "degraded"
    else:
        overall_status = "unhealthy"
    
    return HealthResponse(
        status=overall_status,
        redis=redis_health.healthy,
        postgres=postgres_health.healthy,
        timestamp=datetime.now(timezone.utc),
        services=[redis_health, postgres_health]
    )
