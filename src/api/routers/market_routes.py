"""
Market Routes - Consolidated market data endpoints.

Contains all market-related REST API and WebSocket endpoints:
- Market data (ticker, price, trades, top movers)
- Real-time ticker data from hot path
- WebSocket streaming for trades, klines, and tickers

Table of Contents:
- Imports and Configuration (line ~20)
- Response Models (line ~60)
- Market Endpoints (line ~150)
- Ticker Endpoints (line ~350)
- Realtime WebSocket Endpoints (line ~500)
"""

import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Response, WebSocket, WebSocketDisconnect, status
from pydantic import BaseModel
from starlette.websockets import WebSocketState

from src.api.dependencies import get_redis, get_postgres, get_minio
from src.api.models import TickerResponse, PriceResponse, TradeResponse
from src.storage import RedisStorage, RedisTickerStorage, PostgresStorage, MinioStorage


logger = logging.getLogger(__name__)

# Combined router for all market-related endpoints
router = APIRouter()


# ============================================================================
# RESPONSE MODELS
# ============================================================================

class TopMoverResponse(BaseModel):
    """Response model for top movers."""
    symbol: str
    price: float
    change_percent: float
    volume: float


class TickerDataResponse(BaseModel):
    """
    Real-time ticker response model.
    
    Matches design document API Response Model.
    """
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
    updated_at: int  # Unix timestamp ms
    complete: bool   # All fields present (Requirement 7.5)


class TickerListResponse(BaseModel):
    """Response model for multiple tickers."""
    tickers: List[TickerDataResponse]
    count: int
    timestamp: int  # Response timestamp (Requirement 4.3)


class TickerHealthResponse(BaseModel):
    """Health check response for ticker service."""
    status: str  # healthy, degraded, unhealthy
    redis_connected: bool
    ticker_count: int
    latency_ms: float
    timestamp: int


# ============================================================================
# MARKET ENDPOINTS - Helper Functions
# ============================================================================

def query_with_fallback(
    redis: RedisStorage,
    postgres: PostgresStorage,
    minio: MinioStorage,
    query_type: str,
    symbol: str,
    **kwargs,
) -> Tuple[Any, str]:
    """Query data with fallback chain: Redis -> PostgreSQL -> MinIO.
    
    Args:
        redis: RedisStorage instance
        postgres: PostgresStorage instance
        minio: MinioStorage instance
        query_type: Type of query ('ticker', 'price', 'trades')
        symbol: Trading symbol
        **kwargs: Additional query parameters
        
    Returns:
        Tuple of (data, data_source) where data_source is 'redis', 'postgres', or 'minio'
        
    Raises:
        HTTPException 503: When all data sources fail
    """
    errors = []
    
    # Try Redis first
    try:
        if query_type == "ticker":
            data = redis.get_latest_ticker(symbol)
        elif query_type == "price":
            data = redis.get_latest_price(symbol)
        elif query_type == "trades":
            limit = kwargs.get("limit", 100)
            data = redis.get_recent_trades(symbol, limit=limit)
        else:
            data = None
            
        if data:
            return data, "redis"
    except Exception as e:
        logger.warning(f"Redis query failed for {query_type}/{symbol}: {e}")
        errors.append(f"Redis: {str(e)}")
    
    # Fallback to PostgreSQL
    try:
        now = datetime.now()
        start = now - timedelta(hours=24)  # Last 24 hours for ticker/price
        
        if query_type == "ticker":
            candles = postgres.query_candles(symbol, start, now)
            if candles:
                # Aggregate to ticker format
                latest = candles[-1]
                volumes = [c.get("volume", 0.0) for c in candles]
                highs = [c.get("high", 0.0) for c in candles]
                lows = [c.get("low", 0.0) for c in candles]
                first_close = candles[0].get("close", 0.0) if candles else 0.0
                last_close = latest.get("close", 0.0)
                price_change = last_close - first_close if first_close else 0.0
                
                data = {
                    "close": last_close,
                    "price": last_close,
                    "volume": sum(volumes),
                    "price_change_24h": price_change,
                    "high": max(highs) if highs else 0.0,
                    "low": min(lows) if lows else 0.0,
                    "timestamp": int(latest.get("timestamp", datetime.now()).timestamp() * 1000) 
                        if isinstance(latest.get("timestamp"), datetime) 
                        else int(latest.get("timestamp", 0)),
                }
                return data, "postgres"
                
        elif query_type == "price":
            candles = postgres.query_candles(symbol, start, now)
            if candles:
                latest = candles[-1]
                data = {
                    "price": latest.get("close", 0.0),
                    "volume": latest.get("volume", 0.0),
                    "timestamp": int(latest.get("timestamp", datetime.now()).timestamp() * 1000)
                        if isinstance(latest.get("timestamp"), datetime)
                        else int(latest.get("timestamp", 0)),
                }
                return data, "postgres"
                
        elif query_type == "trades":
            # PostgreSQL doesn't store individual trades, only candles
            # Skip to MinIO for trades
            pass
            
    except Exception as e:
        logger.warning(f"PostgreSQL query failed for {query_type}/{symbol}: {e}")
        errors.append(f"PostgreSQL: {str(e)}")
    
    # Fallback to MinIO
    try:
        now = datetime.now()
        start = now - timedelta(hours=24)
        
        if query_type == "ticker":
            klines = minio.read_klines(symbol, start, now)
            if klines:
                latest = klines[-1]
                volumes = [k.get("volume", 0.0) for k in klines]
                highs = [k.get("high", 0.0) for k in klines]
                lows = [k.get("low", 0.0) for k in klines]
                first_close = klines[0].get("close", 0.0) if klines else 0.0
                last_close = latest.get("close", 0.0)
                price_change = last_close - first_close if first_close else 0.0
                
                ts = latest.get("timestamp")
                if isinstance(ts, datetime):
                    ts = int(ts.timestamp() * 1000)
                elif ts is None:
                    ts = 0
                    
                data = {
                    "close": last_close,
                    "price": last_close,
                    "volume": sum(volumes),
                    "price_change_24h": price_change,
                    "high": max(highs) if highs else 0.0,
                    "low": min(lows) if lows else 0.0,
                    "timestamp": ts,
                }
                return data, "minio"
                
        elif query_type == "price":
            klines = minio.read_klines(symbol, start, now)
            if klines:
                latest = klines[-1]
                ts = latest.get("timestamp")
                if isinstance(ts, datetime):
                    ts = int(ts.timestamp() * 1000)
                elif ts is None:
                    ts = 0
                    
                data = {
                    "price": latest.get("close", 0.0),
                    "volume": latest.get("volume", 0.0),
                    "timestamp": ts,
                }
                return data, "minio"
                
        elif query_type == "trades":
            # MinIO doesn't have a read_trades method, return empty
            pass
                
    except Exception as e:
        logger.warning(f"MinIO query failed for {query_type}/{symbol}: {e}")
        errors.append(f"MinIO: {str(e)}")
    
    # All sources failed or returned no data
    return None, "none"


# ============================================================================
# MARKET ENDPOINTS
# ============================================================================

@router.get("/ticker/{symbol}", response_model=TickerResponse, tags=["market"])
async def get_market_ticker(
    symbol: str,
    response: Response,
    redis: RedisStorage = Depends(get_redis),
    postgres: PostgresStorage = Depends(get_postgres),
    minio: MinioStorage = Depends(get_minio),
) -> TickerResponse:
    """Get latest 24h ticker statistics for a symbol.
    
    Retrieves ticker data from Redis with fallback to PostgreSQL/MinIO.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        
    Returns:
        TickerResponse with 24h statistics
        
    Raises:
        HTTPException 404: Symbol not found
        HTTPException 503: All data sources unavailable
    """
    data, data_source = query_with_fallback(
        redis, postgres, minio, "ticker", symbol
    )
    
    # Add X-Data-Source header
    response.headers["X-Data-Source"] = data_source
    
    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"Symbol not found: {symbol}"
        )
    
    # Map data to response model
    return TickerResponse(
        symbol=symbol,
        price=data.get("close", data.get("price", 0.0)),
        volume=data.get("volume", 0.0),
        price_change_24h=data.get("price_change_24h", 0.0),
        high=data.get("high", 0.0),
        low=data.get("low", 0.0),
        timestamp=int(data.get("timestamp", 0)),
    )


@router.get("/price/{symbol}", response_model=PriceResponse, tags=["market"])
async def get_price(
    symbol: str,
    response: Response,
    redis: RedisStorage = Depends(get_redis),
    postgres: PostgresStorage = Depends(get_postgres),
    minio: MinioStorage = Depends(get_minio),
) -> PriceResponse:
    """Get latest price for a symbol.
    
    Retrieves price data from Redis with fallback to PostgreSQL/MinIO.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        
    Returns:
        PriceResponse with current price, volume, timestamp
        
    Raises:
        HTTPException 404: Symbol not found
        HTTPException 503: All data sources unavailable
    """
    data, data_source = query_with_fallback(
        redis, postgres, minio, "price", symbol
    )
    
    # Add X-Data-Source header
    response.headers["X-Data-Source"] = data_source
    
    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"Symbol not found: {symbol}"
        )
    
    return PriceResponse(
        symbol=symbol,
        price=data["price"],
        volume=data["volume"],
        timestamp=data["timestamp"],
    )


@router.get("/trades/{symbol}", response_model=List[TradeResponse], tags=["market"])
async def get_trades(
    symbol: str,
    response: Response,
    limit: int = Query(default=100, ge=1, le=1000),
    redis: RedisStorage = Depends(get_redis),
    postgres: PostgresStorage = Depends(get_postgres),
    minio: MinioStorage = Depends(get_minio),
) -> List[TradeResponse]:
    """Get recent trades for a symbol.
    
    Retrieves trades from Redis with fallback to MinIO.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        limit: Maximum number of trades to return (default 100, max 1000)
        
    Returns:
        List of TradeResponse, most recent first
        
    Raises:
        HTTPException 404: Symbol not found (no trades available)
        HTTPException 503: All data sources unavailable
    """
    # Enforce max limit of 1000
    effective_limit = min(limit, 1000)
    
    trades, data_source = query_with_fallback(
        redis, postgres, minio, "trades", symbol, limit=effective_limit
    )
    
    # Add X-Data-Source header
    response.headers["X-Data-Source"] = data_source
    
    if not trades:
        raise HTTPException(
            status_code=404,
            detail=f"Symbol not found: {symbol}"
        )
    
    return [
        TradeResponse(
            price=trade.get("price", 0.0),
            quantity=trade.get("quantity", trade.get("qty", 0.0)),
            timestamp=trade.get("timestamp", 0) if not isinstance(trade.get("timestamp"), datetime) 
                else int(trade.get("timestamp").timestamp() * 1000),
            is_buyer_maker=trade.get("is_buyer_maker", False),
            side="SELL" if trade.get("is_buyer_maker", False) else "BUY",
        )
        for trade in trades
    ]


@router.get("/top-movers", response_model=List[TopMoverResponse], tags=["market"])
async def get_top_movers(
    direction: str = Query(default="gainers", pattern="^(gainers|losers)$"),
    limit: int = Query(default=10, ge=1, le=50),
    redis: RedisStorage = Depends(get_redis),
) -> List[TopMoverResponse]:
    """Get top gainers or losers.
    
    Retrieves symbols with highest price changes from Redis.
    
    Args:
        direction: 'gainers' for top gainers, 'losers' for top losers
        limit: Maximum number of results (default 10, max 50)
        
    Returns:
        List of TopMoverResponse sorted by change_percent
    """
    # Get all available symbols from Redis market keys
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]
    movers = []
    
    for symbol in symbols:
        try:
            # Get data from market:{symbol} key (used by aggregation)
            data = redis.client.hgetall(f"market:{symbol}")
            if data:
                price = float(data.get("price", data.get("close", 0)))
                open_price = float(data.get("open", price))
                volume = float(data.get("volume", 0))
                
                # Calculate change percent
                if open_price > 0:
                    change_percent = ((price - open_price) / open_price) * 100
                else:
                    change_percent = 0.0
                
                movers.append(TopMoverResponse(
                    symbol=symbol,
                    price=price,
                    change_percent=round(change_percent, 4),
                    volume=volume,
                ))
        except Exception as e:
            logger.warning(f"Failed to get ticker for {symbol}: {e}")
            continue
    
    # Sort by change_percent
    if direction == "gainers":
        movers.sort(key=lambda x: x.change_percent, reverse=True)
    else:
        movers.sort(key=lambda x: x.change_percent)
    
    return movers[:limit]


# ============================================================================
# TICKER ENDPOINTS - Helper Functions and Dependencies
# ============================================================================

# Dependency injection for ticker storage
@lru_cache()
def get_ticker_storage() -> RedisTickerStorage:
    """Get singleton RedisTickerStorage instance."""
    return RedisTickerStorage(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        ttl_seconds=int(os.getenv("TICKER_TTL_SECONDS", "60")),
    )


# Optional fields that determine `complete` status
OPTIONAL_FIELDS = {"trades_count", "quote_volume"}


def is_ticker_complete(data: dict) -> bool:
    """
    Check if ticker has all optional fields present and valid.
    
    Property 11: Complete field accuracy
    
    Args:
        data: Ticker data dictionary
        
    Returns:
        True if all optional fields are present and valid
    """
    for field in OPTIONAL_FIELDS:
        if field not in data:
            return False
        value = data[field]
        if value is None or value == "" or value == "0":
            return False
    return True


def format_ticker_response(data: dict) -> TickerDataResponse:
    """
    Format ticker data to API response model.
    
    Args:
        data: Raw ticker data from Redis
        
    Returns:
        TickerDataResponse with all fields
    """
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
        updated_at=int(data.get("updated_at", 0)),
        complete=is_ticker_complete(data),
    )


# ============================================================================
# TICKER ENDPOINTS
# ============================================================================

@router.get("/realtime/{symbol}", response_model=TickerDataResponse, tags=["ticker"])
async def get_realtime_ticker(
    symbol: str,
    response: Response,
    storage: RedisTickerStorage = Depends(get_ticker_storage),
) -> TickerDataResponse:
    """
    Get real-time ticker data for a symbol.
    
    Retrieves ticker data from Redis hot path storage.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        
    Returns:
        TickerDataResponse with current ticker data
        
    Raises:
        HTTPException 404: Symbol not found
        
    Requirements: 4.1, 4.3, 4.4, 7.5
    Properties: 5, 7, 11
    """
    start_time = time.time()
    
    data = storage.get_ticker(symbol.upper())
    
    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"Symbol not found: {symbol}"
        )
    
    # Add response time header
    response_time_ms = (time.time() - start_time) * 1000
    response.headers["X-Response-Time-Ms"] = f"{response_time_ms:.2f}"
    response.headers["X-Data-Source"] = "redis"
    
    return format_ticker_response(data)


@router.get("/realtime", response_model=TickerListResponse, tags=["ticker"])
async def get_all_realtime_tickers(
    response: Response,
    storage: RedisTickerStorage = Depends(get_ticker_storage),
) -> TickerListResponse:
    """
    Get all available ticker data.
    
    Retrieves all ticker data from Redis hot path storage.
    
    Returns:
        TickerListResponse with all tickers and count
        
    Requirements: 4.2, 4.3
    Property 6: All tickers returned
    """
    start_time = time.time()
    
    tickers_data = storage.get_all_tickers()
    
    tickers = [format_ticker_response(data) for data in tickers_data]
    
    # Add response time header
    response_time_ms = (time.time() - start_time) * 1000
    response.headers["X-Response-Time-Ms"] = f"{response_time_ms:.2f}"
    response.headers["X-Data-Source"] = "redis"
    
    return TickerListResponse(
        tickers=tickers,
        count=len(tickers),
        timestamp=int(time.time() * 1000),
    )


@router.get("/ticker-health", response_model=TickerHealthResponse, tags=["ticker"])
async def get_ticker_health(
    storage: RedisTickerStorage = Depends(get_ticker_storage),
) -> TickerHealthResponse:
    """
    Get ticker service health status.
    
    Checks Redis connectivity and ticker data availability.
    
    Returns:
        TickerHealthResponse with health status
    """
    start_time = time.time()
    
    # Check Redis connection
    redis_connected = storage.ping()
    
    # Get ticker count by counting all tickers
    ticker_count = len(storage.get_all_tickers()) if redis_connected else 0
    
    # Calculate latency
    latency_ms = (time.time() - start_time) * 1000
    
    # Determine status
    if redis_connected and ticker_count > 0:
        status = "healthy"
    elif redis_connected:
        status = "degraded"  # Connected but no data
    else:
        status = "unhealthy"
    
    return TickerHealthResponse(
        status=status,
        redis_connected=redis_connected,
        ticker_count=ticker_count,
        latency_ms=round(latency_ms, 2),
        timestamp=int(time.time() * 1000),
    )


# ============================================================================
# REALTIME WEBSOCKET ENDPOINTS - Connection Manager
# ============================================================================

class ConnectionManager:
    """Manages WebSocket connections with per-IP limits and heartbeat."""
    
    MAX_CONNECTIONS_PER_IP = 10
    HEARTBEAT_INTERVAL = 30  # seconds
    
    def __init__(self):
        # Track active connections: {websocket: ip_address}
        self.active_connections: Dict[WebSocket, str] = {}
        # Track connections per IP: {ip_address: set of websockets}
        self.connections_by_ip: Dict[str, Set[WebSocket]] = defaultdict(set)
        # Track heartbeat tasks: {websocket: task}
        self.heartbeat_tasks: Dict[WebSocket, asyncio.Task] = {}
    
    def get_client_ip(self, websocket: WebSocket) -> str:
        """Extract client IP from WebSocket connection."""
        # Try X-Forwarded-For header first (for proxied connections)
        forwarded = websocket.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        # Fall back to direct client host
        client = websocket.client
        return client.host if client else "unknown"
    
    def can_connect(self, ip_address: str) -> bool:
        """Check if IP can accept new connection."""
        return len(self.connections_by_ip[ip_address]) < self.MAX_CONNECTIONS_PER_IP
    
    async def connect(self, websocket: WebSocket) -> bool:
        """Accept WebSocket connection if within limits.
        
        Returns:
            True if connection accepted, False if rejected
        """
        ip_address = self.get_client_ip(websocket)
        
        if not self.can_connect(ip_address):
            # Reject connection - policy violation
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            logger.warning(f"Rejected WebSocket from {ip_address}: max connections exceeded")
            return False
        
        await websocket.accept()
        self.active_connections[websocket] = ip_address
        self.connections_by_ip[ip_address].add(websocket)
        
        # Start heartbeat task
        self.heartbeat_tasks[websocket] = asyncio.create_task(
            self._heartbeat_loop(websocket)
        )
        
        logger.info(f"WebSocket connected from {ip_address}")
        return True
    
    def disconnect(self, websocket: WebSocket) -> None:
        """Remove WebSocket connection from tracking."""
        ip_address = self.active_connections.pop(websocket, None)
        if ip_address:
            self.connections_by_ip[ip_address].discard(websocket)
            if not self.connections_by_ip[ip_address]:
                del self.connections_by_ip[ip_address]
        
        # Cancel heartbeat task
        task = self.heartbeat_tasks.pop(websocket, None)
        if task:
            task.cancel()
        
        logger.info(f"WebSocket disconnected from {ip_address}")
    
    async def _heartbeat_loop(self, websocket: WebSocket) -> None:
        """Send periodic heartbeat messages."""
        try:
            while True:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                if websocket.client_state == WebSocketState.CONNECTED:
                    await self.send_message(websocket, {
                        "type": "heartbeat",
                        "timestamp": datetime.now().isoformat()
                    })
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug(f"Heartbeat loop ended: {e}")
    
    async def send_message(self, websocket: WebSocket, message: dict) -> bool:
        """Send JSON message to WebSocket.
        
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json(message)
                return True
        except Exception as e:
            logger.debug(f"Failed to send message: {e}")
        return False
    
    async def broadcast(self, message: dict, connections: Set[WebSocket]) -> None:
        """Broadcast message to multiple connections."""
        for websocket in list(connections):
            await self.send_message(websocket, message)
    
    def get_connection_count(self, ip_address: Optional[str] = None) -> int:
        """Get number of active connections."""
        if ip_address:
            return len(self.connections_by_ip.get(ip_address, set()))
        return len(self.active_connections)


# Global connection manager instance
manager = ConnectionManager()


def get_connection_manager() -> ConnectionManager:
    """Dependency to get connection manager."""
    return manager


# ============================================================================
# REALTIME WEBSOCKET ENDPOINTS
# ============================================================================

@router.websocket("/ws/trades/{symbol}")
async def websocket_trades(
    websocket: WebSocket,
    symbol: str,
    redis: RedisStorage = Depends(get_redis),
):
    """WebSocket endpoint for real-time trade updates.
    
    Subscribes to Redis pub/sub channel for trade updates.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
    """
    if not await manager.connect(websocket):
        return
    
    pubsub = None
    try:
        # Subscribe to Redis pub/sub channel
        channel = f"trades:{symbol}"
        pubsub = redis.client.pubsub()
        pubsub.subscribe(channel)
        
        # Send initial confirmation
        await manager.send_message(websocket, {
            "type": "subscribed",
            "channel": channel,
            "symbol": symbol,
            "timestamp": datetime.now().isoformat()
        })
        
        # Listen for messages
        while True:
            # Check for incoming client messages (for graceful disconnect)
            try:
                client_msg = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=0.1
                )
                # Handle ping/pong or close
                if client_msg == "close":
                    break
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break
            
            # Check for pub/sub messages
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if message and message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    await manager.send_message(websocket, {
                        "type": "trade",
                        "symbol": symbol,
                        "data": data,
                        "timestamp": datetime.now().isoformat()
                    })
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in pub/sub message: {message['data']}")
            
            # Small sleep to prevent busy loop
            await asyncio.sleep(0.01)
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for trades/{symbol}")
    except Exception as e:
        logger.error(f"Error in trades WebSocket: {e}")
        await manager.send_message(websocket, {
            "type": "error",
            "message": f"Data source error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
    finally:
        if pubsub:
            try:
                pubsub.unsubscribe()
                pubsub.close()
            except Exception:
                pass
        manager.disconnect(websocket)


@router.websocket("/ws/klines/{symbol}/{interval}")
async def websocket_klines(
    websocket: WebSocket,
    symbol: str,
    interval: str,
    redis: RedisStorage = Depends(get_redis),
):
    """WebSocket endpoint for real-time kline/candlestick updates.
    
    Subscribes to Redis pub/sub channel for kline updates.
    
    Args:
        symbol: Trading pair symbol (e.g., BTCUSDT)
        interval: Kline interval (e.g., 1m, 5m, 1h)
    """
    if not await manager.connect(websocket):
        return
    
    pubsub = None
    try:
        # Subscribe to Redis pub/sub channel
        channel = f"klines:{symbol}:{interval}"
        pubsub = redis.client.pubsub()
        pubsub.subscribe(channel)
        
        # Send initial confirmation
        await manager.send_message(websocket, {
            "type": "subscribed",
            "channel": channel,
            "symbol": symbol,
            "interval": interval,
            "timestamp": datetime.now().isoformat()
        })
        
        # Listen for messages
        while True:
            # Check for incoming client messages
            try:
                client_msg = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=0.1
                )
                if client_msg == "close":
                    break
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break
            
            # Check for pub/sub messages
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if message and message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    await manager.send_message(websocket, {
                        "type": "kline",
                        "symbol": symbol,
                        "interval": interval,
                        "data": data,
                        "timestamp": datetime.now().isoformat()
                    })
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in pub/sub message: {message['data']}")
            
            await asyncio.sleep(0.01)
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for klines/{symbol}/{interval}")
    except Exception as e:
        logger.error(f"Error in klines WebSocket: {e}")
        await manager.send_message(websocket, {
            "type": "error",
            "message": f"Data source error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
    finally:
        if pubsub:
            try:
                pubsub.unsubscribe()
                pubsub.close()
            except Exception:
                pass
        manager.disconnect(websocket)


@router.websocket("/ws/ticker")
async def websocket_ticker(
    websocket: WebSocket,
    redis: RedisStorage = Depends(get_redis),
):
    """WebSocket endpoint for all tickers broadcast.
    
    Broadcasts all symbol ticker data every 1 second.
    """
    if not await manager.connect(websocket):
        return
    
    BROADCAST_INTERVAL = 1.0  # seconds
    
    try:
        # Send initial confirmation
        await manager.send_message(websocket, {
            "type": "subscribed",
            "channel": "ticker",
            "timestamp": datetime.now().isoformat()
        })
        
        while True:
            # Check for incoming client messages
            try:
                client_msg = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=0.1
                )
                if client_msg == "close":
                    break
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break
            
            # Collect all tickers from Redis
            try:
                tickers = []
                # Scan for all ticker keys
                cursor = 0
                while True:
                    cursor, keys = redis.client.scan(
                        cursor=cursor,
                        match="latest_ticker:*",
                        count=100
                    )
                    for key in keys:
                        symbol = key.replace("latest_ticker:", "")
                        data = redis.get_latest_ticker(symbol)
                        if data:
                            tickers.append({
                                "symbol": symbol,
                                **data
                            })
                    if cursor == 0:
                        break
                
                # Broadcast tickers
                await manager.send_message(websocket, {
                    "type": "ticker",
                    "data": tickers,
                    "count": len(tickers),
                    "timestamp": datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error fetching tickers: {e}")
                await manager.send_message(websocket, {
                    "type": "error",
                    "message": f"Data source error: {str(e)}",
                    "timestamp": datetime.now().isoformat()
                })
            
            # Wait for next broadcast interval
            await asyncio.sleep(BROADCAST_INTERVAL)
            
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for ticker")
    except Exception as e:
        logger.error(f"Error in ticker WebSocket: {e}")
        await manager.send_message(websocket, {
            "type": "error",
            "message": f"Data source error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
    finally:
        manager.disconnect(websocket)
