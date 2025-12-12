"""
Three-Tier Storage Architecture for cryptocurrency streaming pipeline.

This module provides a layered storage system:
- Hot Path (Redis): Real-time queries with < 10ms latency
- Warm Path (PostgreSQL): Interactive analytics with < 1s latency  
- Cold Path (MinIO): S3-compatible object storage for historical archive

Components:
- RedisStorage: In-memory storage for latest data
- RedisTickerStorage: Specialized Redis storage for ticker data
- PostgresStorage: PostgreSQL storage for warm path
- MinioStorage: S3-compatible object storage for cold path
- StorageWriter: Multi-tier write coordinator
- QueryRouter: Automatic tier selection for queries
"""

from .redis import RedisStorage, RedisTickerStorage, check_redis_health
from .postgres import PostgresStorage, check_postgres_health
from .minio import MinioStorage, check_minio_health
from .storage_writer import StorageWriter
from .query_router import QueryRouter


def check_all_storage_health(
    redis_config=None, postgres_config=None, minio_config=None
):
    """Check health of all 3 storage tiers."""
    results = {}
    results['redis'] = check_redis_health(**(redis_config or {}))
    results['postgresql'] = check_postgres_health(**(postgres_config or {}))
    results['minio'] = check_minio_health(**(minio_config or {}))
    return results

__all__ = [
    # Redis (Hot Path)
    "RedisStorage",
    "RedisTickerStorage",
    # PostgreSQL (Warm Path)
    "PostgresStorage",
    # MinIO (Cold Path)
    "MinioStorage",
    # Coordinators
    "StorageWriter",
    "QueryRouter",
    # Health checks
    "check_redis_health",
    "check_postgres_health",
    "check_minio_health",
    "check_all_storage_health",
]
