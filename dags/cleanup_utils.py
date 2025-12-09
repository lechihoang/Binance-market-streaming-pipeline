"""
Cleanup Utility Module for Airflow DAGs.

This module provides cleanup functions for the Binance Connector DAG
and Streaming Processing DAG. Each cleanup function wraps individual
cleanup steps in try-except blocks to continue on partial errors.

Functions:
- cleanup_connector_resources: Cleanup for Binance connector DAG
- cleanup_streaming_resources: Cleanup for streaming processing DAG
"""

import glob
import logging
import os
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional

# Import log_with_context from data_quality if available
try:
    from data_quality import log_with_context
except ImportError:
    # Fallback for testing without Airflow
    def log_with_context(task_instance, level: str, message: str, **extra_context) -> None:
        """Fallback logging function when data_quality is not available."""
        logger = logging.getLogger(__name__)
        context_str = ' | '.join([f"{k}={v}" for k, v in extra_context.items() if v is not None])
        formatted_message = f"[{context_str}] {message}" if context_str else message
        getattr(logger, level, logger.info)(formatted_message)


def _log_message(
    task_instance: Optional[Any],
    logger: logging.Logger,
    level: str,
    message: str,
    **extra_context
) -> None:
    """
    Helper function to log messages with or without task instance.
    
    Args:
        task_instance: Airflow task instance (can be None)
        logger: Python logger instance
        level: Log level ('info', 'warning', 'error', 'debug')
        message: Log message
        extra_context: Additional context to include in log
    """
    if task_instance:
        log_with_context(task_instance, level, message, **extra_context)
    else:
        context_str = ' | '.join([f"{k}={v}" for k, v in extra_context.items() if v is not None])
        formatted_message = f"[{context_str}] {message}" if context_str else message
        getattr(logger, level, logger.info)(formatted_message)


def _kill_processes(
    process_pattern: str,
    task_instance: Optional[Any],
    logger: logging.Logger,
    cleanup_errors: List[str]
) -> None:
    """
    Kill processes matching the given pattern.
    
    Args:
        process_pattern: Pattern to match processes (used with pkill -f)
        task_instance: Airflow task instance (can be None)
        logger: Python logger instance
        cleanup_errors: List to append errors to
    """
    try:
        result = subprocess.run(
            ['pkill', '-f', process_pattern],
            capture_output=True,
            text=True,
            timeout=30
        )
        _log_message(
            task_instance, logger, 'info',
            f"Killed processes matching '{process_pattern}'",
            return_code=result.returncode
        )
    except subprocess.TimeoutExpired as e:
        error_msg = f"Timeout killing processes matching '{process_pattern}': {str(e)}"
        cleanup_errors.append(error_msg)
        _log_message(task_instance, logger, 'error', error_msg)
    except Exception as e:
        error_msg = f"Failed to kill processes matching '{process_pattern}': {str(e)}"
        cleanup_errors.append(error_msg)
        _log_message(task_instance, logger, 'error', error_msg)


def _clear_temp_files(
    temp_patterns: List[str],
    task_instance: Optional[Any],
    logger: logging.Logger,
    cleanup_errors: List[str]
) -> int:
    """
    Clear temporary files matching the given patterns.
    
    Args:
        temp_patterns: List of glob patterns for temp files
        task_instance: Airflow task instance (can be None)
        logger: Python logger instance
        cleanup_errors: List to append errors to
        
    Returns:
        Number of files removed
    """
    files_removed = 0
    try:
        for pattern in temp_patterns:
            for filepath in glob.glob(pattern):
                try:
                    os.remove(filepath)
                    files_removed += 1
                except OSError as e:
                    # Log individual file removal errors but continue
                    _log_message(
                        task_instance, logger, 'warning',
                        f"Failed to remove file: {filepath}",
                        error=str(e)
                    )
        
        _log_message(
            task_instance, logger, 'info',
            "Cleared temporary files",
            files_removed=files_removed
        )
    except Exception as e:
        error_msg = f"Failed to clear temporary files: {str(e)}"
        cleanup_errors.append(error_msg)
        _log_message(task_instance, logger, 'error', error_msg)
    
    return files_removed


def cleanup_connector_resources(**context) -> Dict[str, Any]:
    """
    Cleanup for Binance connector DAG.
    
    Performs cleanup actions:
    1. Kill any orphan connector processes
    2. Clear temporary files
    3. Log cleanup actions
    
    Uses try-except for each step to continue on partial errors.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        Dictionary containing cleanup status:
        - status: 'completed'
        - errors: List of error messages (empty if no errors)
        - timestamp: ISO format timestamp
        
    Example:
        >>> cleanup_connector_resources(**context)
        {'status': 'completed', 'errors': [], 'timestamp': '2024-01-01T12:00:00'}
    """
    task_instance = context.get('task_instance')
    logger = logging.getLogger(__name__)
    cleanup_errors: List[str] = []
    
    # Step 1: Kill any orphan connector processes
    _kill_processes('binance_kafka_connector', task_instance, logger, cleanup_errors)
    
    # Step 2: Clear temporary files
    temp_patterns = [
        '/tmp/binance_connector_*',
        '/tmp/kafka_producer_*',
    ]
    _clear_temp_files(temp_patterns, task_instance, logger, cleanup_errors)
    
    # Step 3: Log cleanup summary
    _log_message(
        task_instance, logger, 'info',
        "Connector cleanup completed",
        errors_count=len(cleanup_errors),
        errors=cleanup_errors if cleanup_errors else None
    )
    
    return {
        'status': 'completed',
        'errors': cleanup_errors,
        'timestamp': datetime.now().isoformat()
    }


def _flush_redis_data(
    redis_host: str,
    redis_port: int,
    task_instance: Optional[Any],
    logger: logging.Logger,
    cleanup_errors: List[str]
) -> None:
    """
    Flush pending data to Redis storage.
    
    Args:
        redis_host: Redis hostname
        redis_port: Redis port number
        task_instance: Airflow task instance (can be None)
        logger: Python logger instance
        cleanup_errors: List to append errors to
    """
    try:
        import redis
        redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        # Trigger background save
        redis_client.bgsave()
        redis_client.close()
        
        _log_message(
            task_instance, logger, 'info',
            "Flushed pending data to Redis",
            redis_host=redis_host, redis_port=redis_port
        )
    except ImportError:
        error_msg = "Redis library not installed, skipping Redis flush"
        cleanup_errors.append(error_msg)
        _log_message(task_instance, logger, 'warning', error_msg)
    except Exception as e:
        error_msg = f"Failed to flush Redis data: {str(e)}"
        cleanup_errors.append(error_msg)
        _log_message(task_instance, logger, 'error', error_msg)


def cleanup_streaming_resources(
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    **context
) -> Dict[str, Any]:
    """
    Cleanup for streaming processing DAG.
    
    Performs cleanup actions:
    1. Flush any pending data to storage (Redis)
    2. Clear temporary files (NOT checkpoint files - those are needed for state recovery)
    3. Log cleanup actions
    
    NOTE: We do NOT kill streaming job processes here because:
    - Jobs have auto-stop mechanism (empty_batch_threshold, max_runtime_seconds)
    - Killing processes while writing state files causes checkpoint corruption
    - State file corruption leads to "delta file does not exist" errors
    
    Uses try-except for each step to continue on partial errors.
    
    Args:
        redis_host: Redis hostname (defaults to REDIS_HOST env var or 'redis')
        redis_port: Redis port number (defaults to REDIS_PORT env var or 6379)
        context: Airflow context dictionary
        
    Returns:
        Dictionary containing cleanup status:
        - status: 'completed'
        - errors: List of error messages (empty if no errors)
        - timestamp: ISO format timestamp
        
    Example:
        >>> cleanup_streaming_resources(redis_host='localhost', redis_port=6379, **context)
        {'status': 'completed', 'errors': [], 'timestamp': '2024-01-01T12:00:00'}
    """
    task_instance = context.get('task_instance')
    logger = logging.getLogger(__name__)
    cleanup_errors: List[str] = []
    
    # Get Redis config from parameters or environment
    if redis_host is None:
        redis_host = os.getenv('REDIS_HOST', 'redis')
    if redis_port is None:
        redis_port = int(os.getenv('REDIS_PORT', '6379'))
    
    # Step 1: Flush pending data to storage (Redis)
    _flush_redis_data(redis_host, redis_port, task_instance, logger, cleanup_errors)
    
    # Step 2: DO NOT kill streaming job processes
    # Jobs have auto-stop mechanism and killing them causes checkpoint corruption
    _log_message(
        task_instance, logger, 'info',
        "Skipping process kill - jobs have auto-stop mechanism",
    )
    
    # Step 3: Clear temporary files (NOT checkpoint files)
    temp_patterns = [
        '/tmp/spark_streaming_*',
        '/tmp/pyspark_*',
        '/tmp/streaming_processor_*',
    ]
    _clear_temp_files(temp_patterns, task_instance, logger, cleanup_errors)
    
    # Step 4: Log cleanup summary
    _log_message(
        task_instance, logger, 'info',
        "Streaming cleanup completed",
        errors_count=len(cleanup_errors),
        errors=cleanup_errors if cleanup_errors else None
    )
    
    return {
        'status': 'completed',
        'errors': cleanup_errors,
        'timestamp': datetime.now().isoformat()
    }
