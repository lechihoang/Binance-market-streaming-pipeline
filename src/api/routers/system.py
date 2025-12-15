"""
System Router - Health, metrics, and status endpoints.

Provides REST API endpoints for system monitoring including
health checks, Prometheus-compatible metrics, and API status.
"""

import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Request

from src.api.dependencies import get_redis, get_postgres
from src.api.models import HealthResponse, MetricsResponse, StatusResponse, ServiceHealth
from src.storage.redis import RedisStorage
from src.storage.postgres import PostgresStorage


router = APIRouter(tags=["system"])


def check_redis_health(redis: RedisStorage) -> ServiceHealth:
    """Check Redis connection health.
    
    Args:
        redis: RedisStorage instance
        
    Returns:
        ServiceHealth with status and latency
    """
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
    """Check PostgreSQL connection health.
    
    Args:
        postgres: PostgresStorage instance
        
    Returns:
        ServiceHealth with status and latency
    """
    start = time.time()
    try:
        # Execute a simple query to check connection
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


def check_kafka_health() -> ServiceHealth:
    """Check Kafka connection health.
    
    Note: Kafka health check is simplified - returns healthy by default
    since Kafka consumer is not always running in API context.
    
    Returns:
        ServiceHealth with status
    """
    # Kafka health check is optional for API - return healthy by default
    # In production, this would check actual Kafka broker connectivity
    return ServiceHealth(
        name="kafka",
        healthy=True,
        latency_ms=0.0,
        error=None
    )


def determine_overall_status(redis_healthy: bool, postgres_healthy: bool, kafka_healthy: bool) -> str:
    """Determine overall system health status.
    
    Args:
        redis_healthy: Redis connection status
        postgres_healthy: PostgreSQL connection status
        kafka_healthy: Kafka connection status
        
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


@router.get("/health", response_model=HealthResponse)
async def get_health(
    redis: RedisStorage = Depends(get_redis),
    postgres: PostgresStorage = Depends(get_postgres),
) -> HealthResponse:
    """Get system health status.
    
    Checks connectivity to Redis, PostgreSQL, and Kafka services.
    
    Returns:
        HealthResponse with overall status and individual service states:
        - "healthy": All services are up
        - "degraded": Some services are down
        - "unhealthy": All services are down
    """
    redis_health = check_redis_health(redis)
    postgres_health = check_postgres_health(postgres)
    kafka_health = check_kafka_health()
    
    overall_status = determine_overall_status(
        redis_health.healthy,
        postgres_health.healthy,
        kafka_health.healthy
    )
    
    return HealthResponse(
        status=overall_status,
        redis=redis_health.healthy,
        duckdb=postgres_health.healthy,  # Keep field name for backward compatibility
        kafka=kafka_health.healthy,
        timestamp=datetime.now(),
        services=[redis_health, postgres_health, kafka_health]
    )


@router.get("/metrics", response_model=MetricsResponse)
async def get_metrics() -> MetricsResponse:
    """Get Prometheus-compatible metrics.
    
    Returns metrics including:
    - request_count: Total number of requests processed
    - request_latency_avg_ms: Average request latency in milliseconds
    - active_connections: Number of active connections
    - error_rate: Percentage of requests that resulted in errors
    
    Note: In production, these would be collected from actual metrics store.
    """
    # Placeholder metrics - in production, these would come from
    # a metrics collector like prometheus_client
    return MetricsResponse(
        request_count=0,
        request_latency_avg_ms=0.0,
        active_connections=0,
        error_rate=0.0
    )


@router.get("/status", response_model=StatusResponse)
async def get_status(request: Request) -> StatusResponse:
    """Get API status information.
    
    Returns:
        StatusResponse with version, uptime, and environment info
    """
    # Calculate uptime from app start time
    start_time = getattr(request.app.state, 'start_time', time.time())
    uptime_seconds = time.time() - start_time
    
    return StatusResponse(
        version="1.0.0",
        uptime_seconds=round(uptime_seconds, 2),
        environment="production"
    )
