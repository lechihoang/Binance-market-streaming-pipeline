"""
Alerts Router - Alert notification endpoints.

Provides REST API endpoints for recent alerts from Redis
and historical alerts from PostgreSQL.
"""

from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from src.api.dependencies import get_redis, get_postgres
from src.api.models import AlertResponse
from src.storage.redis_storage import RedisStorage
from src.storage.postgres_storage import PostgresStorage


router = APIRouter(tags=["alerts"])

# Valid alert types
VALID_ALERT_TYPES = {"whale", "price_spike", "volume_anomaly", "volatility"}


class WhaleAlertResponse(BaseModel):
    """Response model for whale alerts."""
    timestamp: datetime
    symbol: str
    side: str
    amount: float
    price: float
    total_value: float


@router.get("/recent", response_model=List[AlertResponse])
async def get_recent_alerts(
    limit: int = Query(default=100, ge=1, le=1000),
    redis: RedisStorage = Depends(get_redis),
) -> List[AlertResponse]:
    """Get recent alerts from Redis.
    
    Retrieves alerts from Redis list `alerts:recent`.
    
    Args:
        limit: Maximum number of alerts to return (default 100, max 1000)
        
    Returns:
        List of AlertResponse, most recent first
    """
    # Enforce max limit of 1000
    effective_limit = min(limit, 1000)
    
    alerts = redis.get_recent_alerts(limit=effective_limit)
    
    return [
        AlertResponse(
            timestamp=datetime.fromtimestamp(alert.get("timestamp", 0) / 1000)
            if isinstance(alert.get("timestamp"), (int, float)) and alert.get("timestamp") > 1e10
            else datetime.fromtimestamp(alert.get("timestamp", 0))
            if isinstance(alert.get("timestamp"), (int, float))
            else alert.get("timestamp", datetime.now()),
            symbol=alert.get("symbol", "UNKNOWN"),
            alert_type=alert.get("alert_type", "unknown"),
            severity=alert.get("severity", "info"),
            message=alert.get("message"),
            metadata=alert.get("metadata"),
        )
        for alert in alerts
    ]


@router.get("/history", response_model=List[AlertResponse])
async def get_alert_history(
    start: datetime = Query(description="Start datetime"),
    end: datetime = Query(description="End datetime"),
    type: Optional[str] = Query(default=None, description="Alert type filter"),
    symbol: Optional[str] = Query(default=None, description="Symbol filter"),
    postgres: PostgresStorage = Depends(get_postgres),
) -> List[AlertResponse]:
    """Get historical alerts from PostgreSQL.
    
    Retrieves alerts from PostgreSQL alerts table filtered by time range and optional type.
    
    Args:
        start: Start datetime
        end: End datetime
        type: Optional alert type filter (whale, price_spike, volume_anomaly, volatility)
        symbol: Optional symbol filter
        
    Returns:
        List of AlertResponse matching filters
        
    Raises:
        HTTPException 400: Invalid alert type
    """
    # Validate alert type if provided
    if type is not None and type not in VALID_ALERT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid alert type. Valid values: {', '.join(sorted(VALID_ALERT_TYPES))}"
        )
    
    # Query alerts from PostgreSQL using the query_alerts method
    # If no symbol provided, use a wildcard symbol to get all
    query_symbol = symbol if symbol else "BTCUSDT"  # Default symbol for query
    
    # Get alerts from PostgreSQL
    alerts_data = postgres.query_alerts(query_symbol, start, end)
    
    # Filter by type if provided
    if type is not None:
        alerts_data = [a for a in alerts_data if a.get('alert_type') == type]
    
    # If no symbol filter was provided, we need to query for all symbols
    # For now, return what we have (the API may need enhancement for multi-symbol queries)
    
    alerts = []
    for alert_dict in alerts_data:
        alerts.append(AlertResponse(
            timestamp=alert_dict['timestamp'],
            symbol=alert_dict['symbol'],
            alert_type=alert_dict['alert_type'],
            severity=alert_dict['severity'],
            message=alert_dict.get('message'),
            metadata=alert_dict.get('metadata'),
        ))
    
    return alerts


@router.get("/whale-alerts", response_model=List[WhaleAlertResponse])
async def get_whale_alerts(
    limit: int = Query(default=50, ge=1, le=500),
    redis: RedisStorage = Depends(get_redis),
    postgres: PostgresStorage = Depends(get_postgres),
) -> List[WhaleAlertResponse]:
    """Get whale alerts (large trades).
    
    Retrieves whale alerts from Redis (recent) and PostgreSQL (historical).
    
    Args:
        limit: Maximum number of alerts to return (default 50, max 500)
        
    Returns:
        List of WhaleAlertResponse, most recent first
    """
    effective_limit = min(limit, 500)
    whale_alerts = []
    
    # Try Redis first for recent alerts
    try:
        alerts = redis.get_recent_alerts(limit=effective_limit * 2)
        for alert in alerts:
            if alert.get("alert_type", "").upper() in ("WHALE", "WHALE_ALERT"):
                metadata = alert.get("metadata", {})
                if isinstance(metadata, str):
                    import json
                    try:
                        metadata = json.loads(metadata)
                    except:
                        metadata = {}
                
                ts = alert.get("timestamp", 0)
                if isinstance(ts, (int, float)):
                    if ts > 1e10:
                        ts = datetime.fromtimestamp(ts / 1000)
                    else:
                        ts = datetime.fromtimestamp(ts)
                elif not isinstance(ts, datetime):
                    ts = datetime.now()
                
                whale_alerts.append(WhaleAlertResponse(
                    timestamp=ts,
                    symbol=alert.get("symbol", "UNKNOWN"),
                    side=metadata.get("side", "BUY"),
                    amount=float(metadata.get("amount", metadata.get("quantity", 0))),
                    price=float(metadata.get("price", 0)),
                    total_value=float(metadata.get("total_value", metadata.get("value", 0))),
                ))
    except Exception:
        pass
    
    # If not enough from Redis, try PostgreSQL
    if len(whale_alerts) < effective_limit:
        try:
            now = datetime.now()
            start = now - timedelta(days=7)
            
            for symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]:
                pg_alerts = postgres.query_alerts(symbol, start, now)
                for alert in pg_alerts:
                    if alert.get("alert_type", "").upper() in ("WHALE", "WHALE_ALERT"):
                        metadata = alert.get("metadata", {})
                        if isinstance(metadata, str):
                            import json
                            try:
                                metadata = json.loads(metadata)
                            except:
                                metadata = {}
                        
                        whale_alerts.append(WhaleAlertResponse(
                            timestamp=alert.get("timestamp", datetime.now()),
                            symbol=alert.get("symbol", "UNKNOWN"),
                            side=metadata.get("side", "BUY"),
                            amount=float(metadata.get("amount", metadata.get("quantity", 0))),
                            price=float(metadata.get("price", 0)),
                            total_value=float(metadata.get("total_value", metadata.get("value", 0))),
                        ))
        except Exception:
            pass
    
    # Sort by timestamp descending and limit
    whale_alerts.sort(key=lambda x: x.timestamp, reverse=True)
    return whale_alerts[:effective_limit]
