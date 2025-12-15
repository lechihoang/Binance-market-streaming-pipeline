"""
FastAPI Application - Crypto Data API.

Provides REST API and WebSocket interface for cryptocurrency data
from Three-Tier Storage (Redis, DuckDB, Parquet).
"""

import time
from collections import defaultdict
from contextlib import asynccontextmanager
from threading import Lock

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from prometheus_fastapi_instrumentator import Instrumentator

from src.api.routers import market_routes
from src.api.routers import analytics_routes
from src.api.routers import system


# Rate limit configuration
RATE_LIMIT_PER_MINUTE = 100
RATE_LIMIT_WINDOW_SECONDS = 60

# Rate limiter configuration: 100 requests per minute per IP
limiter = Limiter(key_func=get_remote_address, default_limits=["100/minute"])


# In-memory rate limit tracking for accurate headers
class RateLimitTracker:
    """Track rate limits per IP for accurate header reporting."""
    
    def __init__(self, limit: int = 100, window_seconds: int = 60):
        self.limit = limit
        self.window_seconds = window_seconds
        self._requests: dict[str, list[float]] = defaultdict(list)
        self._lock = Lock()
    
    def record_request(self, ip: str) -> tuple[int, int, int]:
        """Record a request and return (limit, remaining, reset_time).
        
        Args:
            ip: Client IP address
            
        Returns:
            Tuple of (limit, remaining, reset_timestamp)
        """
        now = time.time()
        window_start = now - self.window_seconds
        
        with self._lock:
            # Clean old requests outside the window
            self._requests[ip] = [
                ts for ts in self._requests[ip] if ts > window_start
            ]
            
            # Record this request
            self._requests[ip].append(now)
            
            # Calculate remaining
            count = len(self._requests[ip])
            remaining = max(0, self.limit - count)
            
            # Calculate reset time (end of current window)
            if self._requests[ip]:
                oldest_in_window = min(self._requests[ip])
                reset_time = int(oldest_in_window + self.window_seconds)
            else:
                reset_time = int(now + self.window_seconds)
            
            return self.limit, remaining, reset_time
    
    def is_rate_limited(self, ip: str) -> bool:
        """Check if IP is currently rate limited.
        
        Args:
            ip: Client IP address
            
        Returns:
            True if rate limited, False otherwise
        """
        now = time.time()
        window_start = now - self.window_seconds
        
        with self._lock:
            # Clean old requests
            self._requests[ip] = [
                ts for ts in self._requests[ip] if ts > window_start
            ]
            return len(self._requests[ip]) >= self.limit
    
    def get_retry_after(self, ip: str) -> int:
        """Get seconds until rate limit resets for an IP.
        
        Args:
            ip: Client IP address
            
        Returns:
            Seconds until oldest request expires from window
        """
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


# Global rate limit tracker
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown."""
    # Startup
    app.state.start_time = time.time()
    yield
    # Shutdown - cleanup if needed


app = FastAPI(
    title="Crypto Data API",
    description="""
REST API and WebSocket service for cryptocurrency market data from Three-Tier Storage.

## Features

* **Market Data** - Real-time ticker, price, and trade data from Redis
* **Analytics** - Historical klines, technical indicators, volume analysis from DuckDB/Parquet
* **Alerts** - Whale alerts and anomaly detection results
* **Real-time Streaming** - WebSocket endpoints for live data updates
* **System Monitoring** - Health checks and Prometheus metrics

## Data Sources

The API automatically routes queries to the appropriate storage tier:
- **Redis**: Real-time data (last 1 hour)
- **DuckDB**: Interactive analytics (last 90 days)
- **Parquet**: Historical data (older than 90 days)
""",
    version="1.0.0",
    lifespan=lifespan,
    openapi_tags=[
        {
            "name": "market",
            "description": "Real-time market data endpoints - ticker, price, trades, top movers, and WebSocket streaming",
        },
        {
            "name": "ticker",
            "description": "Real-time ticker data from hot path - low latency (~50ms)",
        },
        {
            "name": "analytics",
            "description": "Historical analytics - klines, technical indicators, volume analysis, volatility",
        },
        {
            "name": "alerts",
            "description": "Whale alerts and anomaly detection results",
        },
        {
            "name": "system",
            "description": "System health, metrics, and status endpoints",
        },
    ],
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# Attach limiter to app state
app.state.limiter = limiter

# Add custom rate limit exceeded handler
app.add_exception_handler(RateLimitExceeded, custom_rate_limit_exceeded_handler)

# CORS middleware configuration
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
    
    # Skip rate limiting for docs and health endpoints
    path = request.url.path
    if path in ["/", "/docs", "/redoc", "/openapi.json", "/api/v1/system/health"]:
        response = await call_next(request)
        return response
    
    # Check if rate limited before processing
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
    
    # Record request and get rate limit info
    limit, remaining, reset_time = rate_tracker.record_request(ip)
    
    # Process request
    response: Response = await call_next(request)
    
    # Add rate limit headers to response
    response.headers["X-RateLimit-Limit"] = str(limit)
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    response.headers["X-RateLimit-Reset"] = str(reset_time)
    
    return response


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Crypto Data API", "version": "1.0.0"}


# Include routers with tags for OpenAPI documentation
# Market routes: market data, ticker, and realtime WebSocket endpoints
app.include_router(market_routes.router, prefix="/api/v1/market", tags=["market"])
# Analytics routes: klines, indicators, volume analysis, volatility, and alerts
app.include_router(analytics_routes.router, prefix="/api/v1/analytics", tags=["analytics"])
# System routes: health, metrics, and status
app.include_router(system.router, prefix="/api/v1/system", tags=["system"])

# Prometheus metrics instrumentation
# Exposes /metrics endpoint for Prometheus scraping
Instrumentator().instrument(app).expose(app, endpoint="/metrics")
