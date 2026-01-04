"""
Cleanup utilities for Airflow DAGs.
"""

import glob
import logging
import os
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def cleanup_connector_resources(**context) -> Dict[str, Any]:
    """
    Cleanup for Binance connector DAG.
    Kills orphan processes and clears temp files.
    """
    errors: List[str] = []
    
    # Kill orphan connector processes
    try:
        subprocess.run(['pkill', '-f', 'binance_kafka_connector'], 
                      capture_output=True, text=True, timeout=30)
        logger.info("Killed orphan connector processes")
    except Exception as e:
        errors.append(f"Failed to kill processes: {e}")
        logger.error(f"Failed to kill processes: {e}")
    
    # Clear temp files
    for pattern in ['/tmp/binance_connector_*', '/tmp/kafka_producer_*']:
        for f in glob.glob(pattern):
            try:
                os.remove(f)
            except OSError as e:
                logger.warning(f"Failed to remove {f}: {e}")
    
    logger.info("Connector cleanup completed")
    return {'status': 'completed', 'errors': errors, 'timestamp': datetime.now().isoformat()}


def cleanup_streaming_resources(
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    **context
) -> Dict[str, Any]:
    """
    Cleanup for streaming processing DAG.
    Flushes Redis and clears temp files. Does NOT kill processes (they auto-stop).
    """
    errors: List[str] = []
    
    redis_host = redis_host or os.getenv('REDIS_HOST', 'redis')
    redis_port = redis_port or int(os.getenv('REDIS_PORT', '6379'))
    
    # Flush Redis
    try:
        import redis
        client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        client.bgsave()
        client.close()
        logger.info(f"Flushed Redis data ({redis_host}:{redis_port})")
    except Exception as e:
        errors.append(f"Failed to flush Redis: {e}")
        logger.error(f"Failed to flush Redis: {e}")
    
    # Clear temp files (NOT checkpoint files)
    for pattern in ['/tmp/spark_streaming_*', '/tmp/pyspark_*', '/tmp/streaming_processor_*']:
        for f in glob.glob(pattern):
            try:
                os.remove(f)
            except OSError as e:
                logger.warning(f"Failed to remove {f}: {e}")
    
    logger.info("Streaming cleanup completed")
    return {'status': 'completed', 'errors': errors, 'timestamp': datetime.now().isoformat()}
