"""
Ticker Monitor DAG

Monitors the real-time ticker consumer service health and data freshness.
This DAG runs independently from the streaming processing DAG.

TaskGroup Structure:
1. health_checks: test_redis_health, test_kafka_health (parallel)
2. data_validation: validate_ticker_freshness

Requirements:
- Requirement 1.1: Ticker data should be written to Redis within 100ms
- Requirement 4.3: Response should include timestamp indicating data freshness
- Design doc: Health Checks section

Note: This DAG monitors the ticker-consumer Docker service which runs
independently and writes directly to Redis (bypassing Spark).
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
import sys
import time

# Add parent directory to path so 'from src.xxx' imports work
sys.path.insert(0, '/opt/airflow')

from src.storage.redis import check_redis_health
from src.utils.logging import get_logger

logger = get_logger(__name__)

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Configuration from environment
redis_host = os.getenv('REDIS_HOST', 'redis')
redis_port = int(os.getenv('REDIS_PORT', '6379'))
kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')

# Ticker configuration
TICKER_SYMBOLS = os.getenv(
    'TICKER_SYMBOLS',
    'BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,AVAXUSDT,'
    'DOTUSDT,MATICUSDT,LINKUSDT,LTCUSDT,UNIUSDT,ATOMUSDT,SHIBUSDT'
).split(',')

# Freshness thresholds (in seconds)
TICKER_MAX_AGE_SECONDS = 60  # TTL is 60 seconds
TICKER_WARNING_AGE_SECONDS = 30  # Warn if data is older than 30 seconds


def check_kafka_health(
    bootstrap_servers: str,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    **context
) -> dict:
    """
    Check Kafka connectivity for ticker consumer.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        max_retries: Maximum retry attempts
        retry_delay: Base delay between retries
        context: Airflow context
        
    Returns:
        Dict with health check status
        
    Raises:
        Exception: If health check fails after all retries
    """
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable, KafkaError
    
    last_error = None
    
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=10000,
                api_version_auto_timeout_ms=10000,
            )
            # Check if raw_tickers topic exists
            topics = consumer.topics()
            consumer.close()
            
            has_ticker_topic = 'raw_tickers' in topics
            
            result = {
                'service': 'kafka',
                'status': 'healthy',
                'bootstrap_servers': bootstrap_servers,
                'raw_tickers_topic_exists': has_ticker_topic,
                'attempt': attempt + 1,
                'timestamp': datetime.now().isoformat()
            }
            
            if not has_ticker_topic:
                logger.warning("raw_tickers topic does not exist yet")
            
            logger.info(f"Kafka health check passed: {bootstrap_servers}")
            return result
            
        except (NoBrokersAvailable, KafkaError) as e:
            last_error = e
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)
                logger.warning(
                    f"Kafka health check failed (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {delay}s: {e}"
                )
                time.sleep(delay)
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)
                logger.warning(
                    f"Kafka health check failed (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {delay}s: {e}"
                )
                time.sleep(delay)
    
    raise Exception(f"Kafka health check failed after {max_retries} attempts: {last_error}")


def validate_ticker_freshness(
    redis_host: str,
    redis_port: int,
    symbols: list,
    max_age_seconds: int = 60,
    warning_age_seconds: int = 30,
    **context
) -> dict:
    """
    Validate that ticker data in Redis is fresh.
    
    Checks each configured symbol for:
    1. Data exists in Redis
    2. Data is not stale (within TTL)
    3. Data freshness (warning if older than threshold)
    
    Args:
        redis_host: Redis server hostname
        redis_port: Redis server port
        symbols: List of symbols to check
        max_age_seconds: Maximum acceptable age for data (error threshold)
        warning_age_seconds: Age threshold for warnings
        context: Airflow context
        
    Returns:
        Dict with validation results
        
    Raises:
        ValueError: If validation fails (too many stale/missing tickers)
    """
    import redis
    
    client = redis.Redis(
        host=redis_host,
        port=redis_port,
        decode_responses=True,
        socket_timeout=5.0,
        socket_connect_timeout=5.0,
    )
    
    current_time_ms = int(time.time() * 1000)
    results = {
        'total_symbols': len(symbols),
        'found_count': 0,
        'missing_count': 0,
        'fresh_count': 0,
        'stale_count': 0,
        'warning_count': 0,
        'missing_symbols': [],
        'stale_symbols': [],
        'warning_symbols': [],
        'freshness_details': {},
        'timestamp': datetime.now().isoformat(),
    }
    
    for symbol in symbols:
        key = f"ticker:{symbol.upper()}"
        data = client.hgetall(key)
        
        if not data:
            results['missing_count'] += 1
            results['missing_symbols'].append(symbol)
            results['freshness_details'][symbol] = {
                'status': 'missing',
                'age_seconds': None,
            }
            continue
        
        results['found_count'] += 1
        
        # Check freshness
        updated_at = data.get('updated_at')
        if updated_at:
            try:
                updated_at_ms = int(updated_at)
                age_ms = current_time_ms - updated_at_ms
                age_seconds = age_ms / 1000
                
                results['freshness_details'][symbol] = {
                    'status': 'ok',
                    'age_seconds': round(age_seconds, 2),
                    'updated_at': updated_at_ms,
                }
                
                if age_seconds > max_age_seconds:
                    results['stale_count'] += 1
                    results['stale_symbols'].append(symbol)
                    results['freshness_details'][symbol]['status'] = 'stale'
                elif age_seconds > warning_age_seconds:
                    results['warning_count'] += 1
                    results['warning_symbols'].append(symbol)
                    results['freshness_details'][symbol]['status'] = 'warning'
                else:
                    results['fresh_count'] += 1
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid updated_at for {symbol}: {updated_at}")
                results['stale_count'] += 1
                results['stale_symbols'].append(symbol)
                results['freshness_details'][symbol] = {
                    'status': 'invalid_timestamp',
                    'age_seconds': None,
                }
        else:
            # No updated_at field - consider stale
            results['stale_count'] += 1
            results['stale_symbols'].append(symbol)
            results['freshness_details'][symbol] = {
                'status': 'no_timestamp',
                'age_seconds': None,
            }
    
    client.close()
    
    # Log summary
    logger.info(
        f"Ticker freshness validation: "
        f"{results['found_count']}/{results['total_symbols']} found, "
        f"{results['fresh_count']} fresh, "
        f"{results['warning_count']} warnings, "
        f"{results['stale_count']} stale, "
        f"{results['missing_count']} missing"
    )
    
    if results['missing_symbols']:
        logger.warning(f"Missing tickers: {', '.join(results['missing_symbols'])}")
    
    if results['stale_symbols']:
        logger.warning(f"Stale tickers: {', '.join(results['stale_symbols'])}")
    
    if results['warning_symbols']:
        logger.warning(f"Warning tickers (aging): {', '.join(results['warning_symbols'])}")
    
    # Determine if validation passes
    # Allow some missing/stale tickers during startup or network issues
    # Fail if more than 50% of tickers are missing or stale
    failure_threshold = results['total_symbols'] * 0.5
    total_failures = results['missing_count'] + results['stale_count']
    
    if total_failures > failure_threshold:
        raise ValueError(
            f"Ticker freshness validation failed: "
            f"{total_failures}/{results['total_symbols']} tickers are missing or stale. "
            f"Missing: {results['missing_symbols']}, Stale: {results['stale_symbols']}"
        )
    
    return results


with DAG(
    dag_id='ticker_monitor_dag',
    default_args=default_args,
    description='Monitor real-time ticker consumer health and data freshness',
    schedule_interval='*/2 * * * *',  # Run every 2 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['monitoring', 'ticker', 'realtime'],
) as dag:
    
    # ==========================================================================
    # TaskGroup 1: Health Checks
    # ==========================================================================
    with TaskGroup("health_checks") as health_checks:
        test_redis_health_task = PythonOperator(
            task_id='test_redis_health',
            python_callable=check_redis_health,
            op_kwargs={
                'host': redis_host,
                'port': redis_port,
                'max_retries': 3,
            },
        )
        
        test_kafka_health_task = PythonOperator(
            task_id='test_kafka_health',
            python_callable=check_kafka_health,
            op_kwargs={
                'bootstrap_servers': kafka_bootstrap_servers,
                'max_retries': 3,
            },
        )
    
    # ==========================================================================
    # TaskGroup 2: Data Validation
    # ==========================================================================
    with TaskGroup("data_validation") as data_validation:
        validate_freshness_task = PythonOperator(
            task_id='validate_ticker_freshness',
            python_callable=validate_ticker_freshness,
            op_kwargs={
                'redis_host': redis_host,
                'redis_port': redis_port,
                'symbols': TICKER_SYMBOLS,
                'max_age_seconds': TICKER_MAX_AGE_SECONDS,
                'warning_age_seconds': TICKER_WARNING_AGE_SECONDS,
            },
        )
    
    # ==========================================================================
    # Task Dependencies
    # ==========================================================================
    health_checks >> data_validation
