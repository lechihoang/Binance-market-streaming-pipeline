"""
Health Check Functions for 3-Tier Storage Architecture.

Provides health check functions for each storage tier:
- Redis (Hot Path): Real-time queries
- PostgreSQL (Warm Path): 90-day analytics
- MinIO (Cold Path): Historical archive

These functions can be used standalone or with Airflow PythonOperator.
"""

import logging
import socket
import time
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def check_redis_health(
    host: str = "localhost",
    port: int = 6379,
    db: int = 0,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    **context
) -> Dict[str, Any]:
    """
    Check Redis connection health (Hot Path).
    
    Args:
        host: Redis host
        port: Redis port
        db: Redis database number
        max_retries: Maximum retry attempts
        retry_delay: Base delay between retries (exponential backoff)
        context: Optional Airflow context
        
    Returns:
        Dict with health check status
        
    Raises:
        Exception: If health check fails after all retries
    """
    import redis
    from redis.exceptions import ConnectionError, TimeoutError
    
    last_error = None
    
    for attempt in range(max_retries):
        try:
            client = redis.Redis(
                host=host,
                port=port,
                db=db,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
            # Test connection
            client.ping()
            client.close()
            
            result = {
                'service': 'redis',
                'tier': 'hot',
                'status': 'healthy',
                'host': host,
                'port': port,
                'attempt': attempt + 1,
                'timestamp': datetime.now().isoformat()
            }
            logger.info(f"Redis health check passed: {host}:{port}")
            return result
            
        except (ConnectionError, TimeoutError, Exception) as e:
            last_error = e
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)
                logger.warning(
                    f"Redis health check failed (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {delay}s: {e}"
                )
                time.sleep(delay)
    
    raise Exception(f"Redis health check failed after {max_retries} attempts: {last_error}")


def check_postgres_health(
    host: str = "localhost",
    port: int = 5432,
    user: str = "crypto",
    password: str = "crypto",
    database: str = "crypto_data",
    max_retries: int = 3,
    retry_delay: float = 1.0,
    **context
) -> Dict[str, Any]:
    """
    Check PostgreSQL connection health (Warm Path).
    
    Args:
        host: PostgreSQL host
        port: PostgreSQL port
        user: Database user
        password: Database password
        database: Database name
        max_retries: Maximum retry attempts
        retry_delay: Base delay between retries (exponential backoff)
        context: Optional Airflow context
        
    Returns:
        Dict with health check status
        
    Raises:
        Exception: If health check fails after all retries
    """
    import psycopg2
    
    last_error = None
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                connect_timeout=10
            )
            # Test connection with simple query
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            conn.close()
            
            result = {
                'service': 'postgresql',
                'tier': 'warm',
                'status': 'healthy',
                'host': host,
                'port': port,
                'database': database,
                'attempt': attempt + 1,
                'timestamp': datetime.now().isoformat()
            }
            logger.info(f"PostgreSQL health check passed: {host}:{port}/{database}")
            return result
            
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)
                logger.warning(
                    f"PostgreSQL health check failed (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {delay}s: {e}"
                )
                time.sleep(delay)
    
    raise Exception(f"PostgreSQL health check failed after {max_retries} attempts: {last_error}")


def check_minio_health(
    endpoint: str = "localhost:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
    bucket: str = "crypto-data",
    secure: bool = False,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    **context
) -> Dict[str, Any]:
    """
    Check MinIO connection health (Cold Path).
    
    Args:
        endpoint: MinIO endpoint (host:port)
        access_key: MinIO access key
        secret_key: MinIO secret key
        bucket: Bucket name to check/create
        secure: Use HTTPS if True
        max_retries: Maximum retry attempts
        retry_delay: Base delay between retries (exponential backoff)
        context: Optional Airflow context
        
    Returns:
        Dict with health check status
        
    Raises:
        Exception: If health check fails after all retries
    """
    from minio import Minio
    from minio.error import S3Error
    
    last_error = None
    
    for attempt in range(max_retries):
        try:
            client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
            # Test connection by listing buckets
            client.list_buckets()
            
            # Ensure bucket exists
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                logger.info(f"Created MinIO bucket: {bucket}")
            
            result = {
                'service': 'minio',
                'tier': 'cold',
                'status': 'healthy',
                'endpoint': endpoint,
                'bucket': bucket,
                'attempt': attempt + 1,
                'timestamp': datetime.now().isoformat()
            }
            logger.info(f"MinIO health check passed: {endpoint}, bucket={bucket}")
            return result
            
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)
                logger.warning(
                    f"MinIO health check failed (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {delay}s: {e}"
                )
                time.sleep(delay)
    
    raise Exception(f"MinIO health check failed after {max_retries} attempts: {last_error}")


def check_all_storage_health(
    redis_config: Optional[Dict[str, Any]] = None,
    postgres_config: Optional[Dict[str, Any]] = None,
    minio_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Check health of all 3 storage tiers.
    
    Args:
        redis_config: Redis connection config
        postgres_config: PostgreSQL connection config
        minio_config: MinIO connection config
        
    Returns:
        Dict with health status for each tier
        
    Raises:
        Exception: If any health check fails
    """
    results = {}
    
    # Check Redis (Hot Path)
    redis_cfg = redis_config or {}
    results['redis'] = check_redis_health(**redis_cfg)
    
    # Check PostgreSQL (Warm Path)
    postgres_cfg = postgres_config or {}
    results['postgresql'] = check_postgres_health(**postgres_cfg)
    
    # Check MinIO (Cold Path)
    minio_cfg = minio_config or {}
    results['minio'] = check_minio_health(**minio_cfg)
    
    return results
