"""
Dependency injection for FastAPI endpoints.

Provides singleton instances of storage classes using @lru_cache
for connection reuse across requests.
"""

import os
from functools import lru_cache

from src.storage.redis import RedisStorage
from src.storage.postgres import PostgresStorage
from src.storage.minio import MinioStorage
from src.storage.query_router import QueryRouter


@lru_cache()
def get_redis() -> RedisStorage:
    """Get singleton RedisStorage instance.
    
    Returns:
        RedisStorage instance connected to Redis server
    """
    return RedisStorage(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
    )


@lru_cache()
def get_postgres() -> PostgresStorage:
    """Get singleton PostgresStorage instance.
    
    Returns:
        PostgresStorage instance with database connection
        
    Environment variables:
        POSTGRES_DATA_HOST: PostgreSQL host (default: postgres-data)
        POSTGRES_DATA_PORT: PostgreSQL port (default: 5432)
        POSTGRES_DATA_USER: Database user (default: crypto)
        POSTGRES_DATA_PASSWORD: Database password (default: crypto)
        POSTGRES_DATA_DB: Database name (default: crypto_data)
    """
    return PostgresStorage(
        host=os.getenv("POSTGRES_DATA_HOST", "postgres-data"),
        port=int(os.getenv("POSTGRES_DATA_PORT", "5432")),
        user=os.getenv("POSTGRES_DATA_USER", "crypto"),
        password=os.getenv("POSTGRES_DATA_PASSWORD", "crypto"),
        database=os.getenv("POSTGRES_DATA_DB", "crypto_data"),
    )


@lru_cache()
def get_minio() -> MinioStorage:
    """Get singleton MinioStorage instance.
    
    Returns:
        MinioStorage instance for cold path queries
        
    Environment variables:
        MINIO_ENDPOINT: MinIO endpoint (default: localhost:9000)
        MINIO_ACCESS_KEY: MinIO access key (default: minioadmin)
        MINIO_SECRET_KEY: MinIO secret key (default: minioadmin)
        MINIO_BUCKET: MinIO bucket name (default: crypto-data)
        MINIO_SECURE: Use HTTPS (default: false)
    """
    return MinioStorage(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        bucket=os.getenv("MINIO_BUCKET", "crypto-data"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


@lru_cache()
def get_query_router() -> QueryRouter:
    """Get singleton QueryRouter instance.
    
    Returns:
        QueryRouter instance with all storage tiers
    """
    return QueryRouter(
        redis=get_redis(),
        postgres=get_postgres(),
        minio=get_minio(),
    )
