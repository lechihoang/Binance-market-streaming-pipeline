import json as json_module
import os
import sys
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from functools import lru_cache
from threading import Lock
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from api.schema import (
    BuySellImbalanceResponse,
    HealthResponse,
    KlineResponse,
    MarketSummaryResponse,
    MLPredictionResponse,
    MLStatusResponse,
    PriceSpikeResponse,
    RecentTradeResponse,
    ServiceHealth,
    TickerDataResponse,
    TickerHealthResponse,
    TickerListResponse,
    TopTradingResponse,
    TradeCountSpikeResponse,
    TradesCountResponse,
    VolumeSpikeResponse,
)
from schema.market import Alert, Kline, Ticker
from pydantic import ValidationError
from storage.postgres import Postgres as PostgresStorage
from storage.query_router import Router as QueryRouter
from storage.redis import Redis as RedisStorage
from util.constant import (
    DEFAULT_SYMBOL,
    MAX_TIME_RANGE_DAY,
    RATE_LIMIT,
    RATE_LIMIT_WINDOW,
    VALID_INTERVAL,
    VALID_TRADE_COUNT_INTERVAL,
)
from loguru import logger

# logger = get_logger(__name__)

ALERT_SYMBOLS = DEFAULT_SYMBOL

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
            self._requests[ip] = [ts for ts in self._requests[ip] if ts > window_start]
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
            self._requests[ip] = [ts for ts in self._requests[ip] if ts > window_start]
            return len(self._requests[ip]) >= self.limit

    def get_retry_after(self, ip: str) -> int:
        """Get seconds until rate limit resets for an IP."""
        now = time.time()
        window_start = now - self.window_seconds

        with self._lock:
            self._requests[ip] = [ts for ts in self._requests[ip] if ts > window_start]
            if self._requests[ip]:
                oldest = min(self._requests[ip])
                return max(1, int((oldest + self.window_seconds) - now))
            return 0


rate_tracker = RateLimitTracker(limit=RATE_LIMIT, window_seconds=RATE_LIMIT_WINDOW)


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown."""
    app.state.start_time = time.time()
    logger.add(sys.stderr, level="INFO", serialize=True)
    yield


app = FastAPI(
    title="Crypto Data API",
    description="""
REST API for cryptocurrency market data from Two-Tier Storage.

## Features

* **Market Data** - Real-time ticker, price, and trade data from RedisStorage
* **Analytics** - Historical klines from PostgreSQL
* **Alerts** - Whale alerts and anomaly detection results
* **System Monitoring** - Health checks and Prometheus metrics

## Data Sources

The API automatically routes queries to the appropriate storage tier:
- **RedisStorage**: Real-time data (last 1 hour, cache)
- **PostgreSQL**: Historical data (permanent storage)
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


async def interval_validation_error_handler(request: Request, exc: RequestValidationError):
    """Custom handler for interval validation errors."""
    for error in exc.errors():
        if error.get("loc") == ("query", "interval"):
            return JSONResponse(
                status_code=400,
                content={"detail": f"Invalid interval. Valid values: {', '.join(sorted(VALID_INTERVAL))}"},
            )
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


app.add_exception_handler(RequestValidationError, interval_validation_error_handler)

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
                "X-RateLimit-Limit": str(RATE_LIMIT),
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


Instrumentator().instrument(app).expose(app, endpoint="/metrics")


@lru_cache
def get_redis() -> RedisStorage:
    """Get singleton RedisStorage instance."""
    return RedisStorage(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
    )


@lru_cache
def get_postgres() -> PostgresStorage:
    """Get singleton PostgresStorage instance."""
    return PostgresStorage(
        host=os.getenv("POSTGRES_DATA_HOST", "postgres-data"),
        port=int(os.getenv("POSTGRES_DATA_PORT", "5432")),
        user=os.getenv("POSTGRES_DATA_USER", "crypto"),
        password=os.getenv("POSTGRES_DATA_PASSWORD", "crypto"),
        database=os.getenv("POSTGRES_DATA_DB", "crypto_data"),
    )


@lru_cache
def get_query_router() -> QueryRouter:
    """Get singleton QueryRouter instance."""
    return QueryRouter(
        redis=get_redis(),
        postgres=get_postgres(),
    )


@lru_cache
def get_ticker_storage() -> RedisStorage:
    """Get singleton RedisStorage instance for ticker data."""
    return RedisStorage(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        ttl_seconds=int(os.getenv("TICKER_TTL_SECONDS", "60")),
    )


def check_redis_health(redis: RedisStorage) -> ServiceHealth:
    """Check RedisStorage connection health."""
    start = time.time()
    try:
        healthy = redis.ping()
        latency_ms = (time.time() - start) * 1000
        return ServiceHealth(
            name="redis", healthy=healthy, latency_ms=round(latency_ms, 2), error=None if healthy else "Ping failed"
        )
    except Exception as e:
        latency_ms = (time.time() - start) * 1000
        return ServiceHealth(name="redis", healthy=False, latency_ms=round(latency_ms, 2), error=str(e))


def check_postgres_health(postgres: PostgresStorage) -> ServiceHealth:
    """Check PostgreSQL connection health."""
    start = time.time()
    try:
        postgres.run("SELECT 1", fetch=True)
        latency_ms = (time.time() - start) * 1000
        return ServiceHealth(name="postgres", healthy=True, latency_ms=round(latency_ms, 2), error=None)
    except Exception as e:
        latency_ms = (time.time() - start) * 1000
        return ServiceHealth(name="postgres", healthy=False, latency_ms=round(latency_ms, 2), error=str(e))


def determine_overall_status(redis_healthy: bool, postgres_healthy: bool, kafka_healthy: bool) -> str:
    """Determine overall system health status."""
    services = [redis_healthy, postgres_healthy, kafka_healthy]
    healthy_count = sum(services)

    if healthy_count == len(services):
        return "healthy"
    elif healthy_count == 0:
        return "unhealthy"
    else:
        return "degraded"


def normalize_time_range(
    start: datetime | None, end: datetime | None, default_hours: int = 1
) -> tuple[datetime, datetime]:
    now = datetime.now()
    if not end:
        end = now
    if not start:
        start = now - timedelta(hours=default_hours)
    return start, end


def query_alerts_by_type(
    query_router: QueryRouter,
    alert_types: list[str],
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    alerts = []
    for symbol in ALERT_SYMBOLS:
        try:
            result = query_router.query(
                data_type=QueryRouter.ALERT,
                symbol=symbol,
                start=start,
                end=end,
            )
            for alert in result:
                if alert.get("alert_type", "").upper() in alert_types:
                    alerts.append(alert)
        except Exception:
            pass
    return alerts


def validate_time_range(start: datetime, end: datetime) -> None:
    if (end - start).days > MAX_TIME_RANGE_DAY:
        raise HTTPException(status_code=400, detail="Time range exceeds maximum allowed (1 year)")


def validate_interval(interval: str) -> None:
    if interval not in VALID_INTERVAL:
        raise HTTPException(
            status_code=400, detail=f"Invalid interval. Valid values: {', '.join(sorted(VALID_INTERVAL))}"
        )


def query_klines_with_fallback(
    query_router: QueryRouter,
    symbol: str,
    start: datetime,
    end: datetime,
    interval: str = "1m",
) -> tuple[list[dict], str]:
    """Query klines with automatic tier selection (RedisStorage cache or PostgreSQL)."""
    now = datetime.now()
    redis_cutoff = now - timedelta(hours=1)

    if start >= redis_cutoff:
        primary_tier = "redis"
    else:
        primary_tier = "postgres"

    try:
        data = query_router.query(
            data_type=QueryRouter.KLINE,
            symbol=symbol,
            start=start,
            end=end,
            interval=interval,
        )
        if data:
            return data, primary_tier
    except Exception as e:
        logger.warning(f"QueryRouter failed for klines/{symbol} interval={interval}: {e}")

    return [], "none"


@app.get("/api/v1/market/ticker-health", response_model=TickerHealthResponse, tags=["market"])
async def get_ticker_health(
    storage: RedisStorage = Depends(get_ticker_storage),
) -> TickerHealthResponse:
    """Get ticker service health status."""
    start_time = time.time()

    redis_connected = storage.ping()
    ticker_count = len(storage.get_ticker_all()) if redis_connected else 0
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
        timestamp=datetime.now(),
    )


@app.get("/api/v1/market/summary", response_model=MarketSummaryResponse, tags=["market"])
async def get_market_summary(
    response: Response,
    storage: RedisStorage = Depends(get_ticker_storage),
) -> MarketSummaryResponse:
    """Get real-time market summary statistics."""
    start_time = time.time()

    all_tickers = storage.get_ticker_all()

    total_symbols = len(all_tickers)
    total_trades = 0
    total_quote_volume = 0.0

    for data in all_tickers:
        try:
            ticker = Ticker.model_validate(data)
            total_trades += ticker.trade_count
            total_quote_volume += ticker.quote_volume
        except ValidationError:
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
        timestamp=datetime.now(),
    )


@app.get("/api/v1/market/realtime", response_model=TickerListResponse, tags=["market"])
async def get_all_realtime_tickers(
    response: Response,
    storage: RedisStorage = Depends(get_ticker_storage),
) -> TickerListResponse:
    """Get all available ticker data."""
    start_time = time.time()

    tickers_data = storage.get_ticker_all()
    tickers = [TickerDataResponse(**data) for data in tickers_data]

    response_time_ms = (time.time() - start_time) * 1000
    response.headers["X-Response-Time-Ms"] = f"{response_time_ms:.2f}"
    response.headers["X-Data-Source"] = "redis"

    return TickerListResponse(
        tickers=tickers,
        count=len(tickers),
        timestamp=datetime.now(),
    )


@app.get("/api/v1/market/top-by-trades", response_model=list[TopTradingResponse], tags=["market"])
async def get_top_by_trades(
    response: Response,
    limit: int = Query(default=5, ge=1, le=50),
    storage: RedisStorage = Depends(get_ticker_storage),
) -> list[TopTradingResponse]:
    """Get top symbols by trades count."""
    start_time = time.time()

    all_tickers = storage.get_ticker_all()

    results = []
    for data in all_tickers:
        try:
            ticker = Ticker.model_validate(data)
            if not ticker.symbol:
                continue

            results.append(
                TopTradingResponse(
                    symbol=ticker.symbol,
                    last_price=ticker.last_price,
                    trade_count=ticker.trade_count,
                    quote_volume=ticker.quote_volume,
                )
            )
        except ValidationError as e:
            logger.warning(f"Failed to parse ticker data for top-by-trades: {e}")
            continue

    results.sort(key=lambda x: (x.trade_count, x.quote_volume), reverse=True)

    response_time_ms = (time.time() - start_time) * 1000
    response.headers["X-Response-Time-Ms"] = f"{response_time_ms:.2f}"

    return results[:limit]


@app.get("/api/v1/market/top-by-volume", response_model=list[TopTradingResponse], tags=["market"])
async def get_top_by_volume(
    response: Response,
    limit: int = Query(default=5, ge=1, le=50),
    storage: RedisStorage = Depends(get_ticker_storage),
) -> list[TopTradingResponse]:
    """Get top symbols by quote volume."""
    start_time = time.time()

    all_tickers = storage.get_ticker_all()

    results = []
    for data in all_tickers:
        try:
            ticker = Ticker.model_validate(data)
            if not ticker.symbol:
                continue

            results.append(
                TopTradingResponse(
                    symbol=ticker.symbol,
                    last_price=ticker.last_price,
                    trade_count=ticker.trade_count,
                    quote_volume=ticker.quote_volume,
                )
            )
        except ValidationError as e:
            logger.warning(f"Failed to parse ticker data for top-by-volume: {e}")
            continue

    results.sort(key=lambda x: (x.quote_volume, x.trade_count), reverse=True)

    response_time_ms = (time.time() - start_time) * 1000
    response.headers["X-Response-Time-Ms"] = f"{response_time_ms:.2f}"

    return results[:limit]


@app.get("/api/v1/market/recent-trades", response_model=list[RecentTradeResponse], tags=["market"])
async def get_recent_trades(
    response: Response,
    symbol: str = Query(description="Trading pair symbol (e.g., BTCUSDT)"),
    limit: int = Query(default=20, ge=1, le=100, description="Number of trades to return"),
    storage: RedisStorage = Depends(get_ticker_storage),
) -> list[RecentTradeResponse]:
    """Get recent trades for a symbol from Redis cache."""
    start_time = time.time()

    trades = storage.get_trade(symbol.upper(), limit)

    results = []
    for trade in trades:
        try:
            results.append(RecentTradeResponse(**trade))
        except Exception as e:
            logger.warning(f"Failed to parse trade data for {symbol}: {e}")
            continue

    response_time_ms = (time.time() - start_time) * 1000
    response.headers["X-Response-Time-Ms"] = f"{response_time_ms:.2f}"
    response.headers["X-Data-Source"] = "redis"

    return results


@app.get(
    "/api/v1/analytics/klines/{symbol}",
    response_model=list[KlineResponse],
    tags=["analytics"],
    summary="Get OHLCV klines for a symbol(Open, High, Low, Close, Volume)",
)
async def get_klines(
    symbol: str,
    response: Response,
    interval: str = Query(default="1m", pattern="^(1m|5m|15m|1h)$", description="Time interval (1m, 5m, 15m, 1h)"),
    minutes: int = Query(default=60, description="Number of minutes to fetch (only used if start is not provided)"),
    start: datetime | None = Query(default=None, description="Start time (ISO 8601 format)"),
    end: datetime | None = Query(default=None, description="End time (ISO 8601 format, defaults to now)"),
    query_router: QueryRouter = Depends(get_query_router),
) -> list[KlineResponse]:
    """Get OHLCV klines for a symbol with automatic tier selection (Redis/PostgreSQL)."""
    validate_interval(interval)

    now = datetime.now()
    if end is None:
        end = now
    if start is None:
        start = end - timedelta(minutes=minutes)

    validate_time_range(start, end)

    data, data_source = query_klines_with_fallback(query_router, symbol, start, end, interval)

    response.headers["X-Data-Source"] = data_source
    response.headers["X-Interval"] = interval

    results = []
    for record in data:
        try:
            kline = Kline.model_validate(record)
            results.append(KlineResponse.from_kline(kline))
        except ValidationError as e:
            logger.warning(f"Failed to parse kline data for {symbol}: {e}")
            continue
    return results


@app.get("/api/v1/analytics/trades-count", response_model=list[TradesCountResponse], tags=["analytics"])
async def get_trades_count(
    response: Response,
    symbol: str = Query(description="Trading pair symbol (e.g., BTCUSDT)"),
    interval: str = Query(default="1h", description="Time interval (1m, 1h, 1d)"),
    limit: int = Query(default=24, ge=1, le=1000, description="Number of data points"),
    postgres: PostgresStorage = Depends(get_postgres),
) -> list[TradesCountResponse]:
    """Get trades count aggregated by time interval."""
    if interval not in VALID_TRADE_COUNT_INTERVAL:
        raise HTTPException(
            status_code=400, detail=f"Invalid interval. Valid values: {', '.join(sorted(VALID_TRADE_COUNT_INTERVAL))}"
        )

    now = datetime.now()
    interval_durations = {
        "1m": timedelta(minutes=1),
        "1h": timedelta(hours=1),
        "1d": timedelta(days=1),
    }

    duration = interval_durations[interval]
    start = now - (duration * limit)
    end = now

    try:
        data = postgres.get_trades_count(symbol.upper(), start, end, interval)
        response.headers["X-Data-Source"] = "postgres"

        if len(data) > limit:
            data = data[-limit:]

        return [
            TradesCountResponse(
                timestamp=record["timestamp"],
                trade_count=record.get("trade_count", 0),
                interval=record["interval"],
            )
            for record in data
        ]
    except Exception as e:
        logger.warning(f"Failed to query trades count for {symbol}: {e}")
        response.headers["X-Data-Source"] = "none"
        return []


@app.get("/api/v1/analytics/alerts/price-spikes", response_model=list[PriceSpikeResponse], tags=["alerts"])
async def get_price_spikes(
    response: Response,
    limit: int = Query(default=50, ge=1, le=500),
    start: datetime | None = Query(default=None, description="Start time (default: 1 hour ago)"),
    end: datetime | None = Query(default=None, description="End time (default: now)"),
    query_router: QueryRouter = Depends(get_query_router),
) -> list[PriceSpikeResponse]:
    start, end = normalize_time_range(start, end)
    raw_alerts = query_alerts_by_type(query_router, ["PRICE_SPIKE"], start, end)

    price_spikes = []
    for raw_alert in raw_alerts:
        try:
            alert = Alert.model_validate(raw_alert)
            price_spikes.append(PriceSpikeResponse.from_alert(alert))
        except ValidationError as e:
            logger.warning(f"Failed to parse price spike alert: {e}")
            continue

    price_spikes.sort(key=lambda x: x.timestamp, reverse=True)
    return price_spikes[: min(limit, 500)]


@app.get("/api/v1/analytics/alerts/volume-spikes", response_model=list[VolumeSpikeResponse], tags=["alerts"])
async def get_volume_spikes(
    response: Response,
    limit: int = Query(default=50, ge=1, le=500),
    start: datetime | None = Query(default=None, description="Start time (default: 1 hour ago)"),
    end: datetime | None = Query(default=None, description="End time (default: now)"),
    query_router: QueryRouter = Depends(get_query_router),
) -> list[VolumeSpikeResponse]:
    start, end = normalize_time_range(start, end)
    raw_alerts = query_alerts_by_type(query_router, ["VOLUME_SPIKE"], start, end)

    volume_spikes = []
    for raw_alert in raw_alerts:
        try:
            alert = Alert.model_validate(raw_alert)
            volume_spikes.append(VolumeSpikeResponse.from_alert(alert))
        except ValidationError as e:
            logger.warning(f"Failed to parse volume spike alert: {e}")
            continue

    volume_spikes.sort(key=lambda x: x.timestamp, reverse=True)
    return volume_spikes[: min(limit, 500)]


@app.get("/api/v1/analytics/alerts/trade-count-spikes", response_model=list[TradeCountSpikeResponse], tags=["alerts"])
async def get_trade_count_spikes(
    response: Response,
    limit: int = Query(default=50, ge=1, le=500),
    start: datetime | None = Query(default=None, description="Start time (default: 1 hour ago)"),
    end: datetime | None = Query(default=None, description="End time (default: now)"),
    query_router: QueryRouter = Depends(get_query_router),
) -> list[TradeCountSpikeResponse]:
    start, end = normalize_time_range(start, end)
    raw_alerts = query_alerts_by_type(query_router, ["TRADE_COUNT_SPIKE"], start, end)

    trade_spikes = []
    for raw_alert in raw_alerts:
        try:
            alert = Alert.model_validate(raw_alert)
            trade_spikes.append(TradeCountSpikeResponse.from_alert(alert))
        except ValidationError as e:
            logger.warning(f"Failed to parse trade count spike alert: {e}")
            continue

    trade_spikes.sort(key=lambda x: x.timestamp, reverse=True)
    return trade_spikes[: min(limit, 500)]


@app.get("/api/v1/analytics/alerts/buy-sell-imbalance", response_model=list[BuySellImbalanceResponse], tags=["alerts"])
async def get_buy_sell_imbalance(
    response: Response,
    limit: int = Query(default=50, ge=1, le=500),
    start: datetime | None = Query(default=None, description="Start time (default: 1 hour ago)"),
    end: datetime | None = Query(default=None, description="End time (default: now)"),
    query_router: QueryRouter = Depends(get_query_router),
) -> list[BuySellImbalanceResponse]:
    start, end = normalize_time_range(start, end)
    raw_alerts = query_alerts_by_type(query_router, ["BUY_SELL_IMBALANCE"], start, end)

    imbalances = []
    for raw_alert in raw_alerts:
        try:
            alert = Alert.model_validate(raw_alert)
            imbalances.append(BuySellImbalanceResponse.from_alert(alert))
        except ValidationError as e:
            logger.warning(f"Failed to parse buy/sell imbalance alert: {e}")
            continue

    imbalances.sort(key=lambda x: x.timestamp, reverse=True)
    return imbalances[: min(limit, 500)]


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
        timestamp=datetime.now(),
        services=[redis_health, postgres_health],
    )


def classify_volatility_level(volatility: float) -> str:
    """Classify volatility into LOW/MEDIUM/HIGH levels.

    Args:
        volatility: Volatility value in decimal format (e.g., 0.02 = 2%)

    Returns:
        LOW: < 0.5% (< 0.005)
        MEDIUM: 0.5% - 1.5% (0.005 - 0.015)
        HIGH: > 1.5% (> 0.015)
    """
    if volatility < 0.005:
        return "LOW"
    elif volatility <= 0.015:
        return "MEDIUM"
    else:
        return "HIGH"


@app.get("/api/v1/ml/predict/{symbol}", response_model=MLPredictionResponse, tags=["ml"])
async def get_ml_prediction(
    symbol: str,
    response: Response,
    postgres: PostgresStorage = Depends(get_postgres),
) -> MLPredictionResponse:
    prediction = postgres.get_latest_volatility_prediction(symbol.upper())

    if not prediction:
        raise HTTPException(status_code=404, detail=f"No volatility prediction available for {symbol}")

    response.headers["X-Data-Source"] = "postgres"

    predicted_volatility = prediction["predicted_volatility_5m"]

    return MLPredictionResponse(
        symbol=prediction["symbol"],
        timestamp=str(prediction["timestamp"]),
        current_volatility=prediction["current_volatility"],
        predicted_volatility_5m=predicted_volatility,
        volatility_level=classify_volatility_level(predicted_volatility),
    )


@app.get("/api/v1/ml/status", response_model=MLStatusResponse, tags=["ml"])
async def get_ml_status() -> MLStatusResponse:
    import json
    from pathlib import Path

    model_dir = Path(os.getenv("MODEL_DIR", "model"))
    model_path = model_dir / "volatility_predictor.json"
    meta_path = model_dir / "volatility_predictor_meta.json"

    if not model_path.exists():
        return MLStatusResponse(
            model_loaded=False,
            error="Volatility model file not found",
        )

    model_info = {}
    if meta_path.exists():
        with open(meta_path) as f:
            model_info = json.load(f)

    return MLStatusResponse(
        model_loaded=True,
        model_info=model_info,
    )
