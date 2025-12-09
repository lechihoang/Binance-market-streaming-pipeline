"""
Three-Tier Storage Architecture for cryptocurrency streaming pipeline.

This module provides a layered storage system:
- Hot Path (Redis): Real-time queries with < 10ms latency
- Warm Path (PostgreSQL/DuckDB): Interactive analytics with < 1s latency  
- Cold Path (MinIO): S3-compatible object storage for historical archive

Components:
- RedisStorage: In-memory storage for latest data
- PostgresStorage: PostgreSQL storage for warm path (concurrent writes)
- DuckDBStorage: DuckDB OLAP storage for warm path (legacy)
- MinioStorage: S3-compatible object storage for cold path
- StorageWriter: Multi-tier write coordinator
- QueryRouter: Automatic tier selection for queries
"""

from .redis_storage import RedisStorage
from .duckdb_storage import DuckDBStorage
from .postgres_storage import PostgresStorage
from .minio_storage import MinioStorage
from .storage_writer import StorageWriter
from .query_router import QueryRouter
from .health_checks import (
    check_redis_health,
    check_postgres_health,
    check_minio_health,
    check_all_storage_health,
)

__all__ = [
    "RedisStorage",
    "DuckDBStorage", 
    "PostgresStorage",
    "MinioStorage",
    "StorageWriter",
    "QueryRouter",
    # Health checks
    "check_redis_health",
    "check_postgres_health",
    "check_minio_health",
    "check_all_storage_health",
]
