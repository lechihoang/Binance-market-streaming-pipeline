"""
Streaming Processing DAG

Orchestrates the Spark streaming jobs that process trade data from Kafka.
This DAG runs independently from the Binance Connector DAG.

TaskGroup Structure:
1. health_checks: test_redis_health, test_postgres_health, test_minio_health (parallel)
2. trade_aggregation: run_trade_aggregation_job >> validate_aggregation_output
3. technical_indicators: run_technical_indicators_job >> validate_indicators_output
4. anomaly_detection: run_anomaly_detection_job >> validate_anomaly_output
5. cleanup: cleanup_streaming

Storage Architecture:
- Hot Path: Redis (real-time queries)
- Warm Path: PostgreSQL (90-day analytics)
- Cold Path: MinIO (historical archive)

Note: anomaly_detection_job runs AFTER technical_indicators_job because it depends on
the processed_indicators Kafka topic which is created by technical_indicators_job.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os
import sys

# Add src directory to path for imports
sys.path.insert(0, '/opt/airflow/src')

from data_quality import on_failure_callback
from cleanup_utils import cleanup_streaming_resources

# Import health check functions from storage module
from storage.health_checks import (
    check_redis_health,
    check_postgres_health,
    check_minio_health,
)

# Import validation functions
from validators.job_validators import (
    validate_aggregation_output,
    validate_indicators_output,
    validate_anomaly_output,
)

# Import RedisStorage for validation tasks
from storage.redis_storage import RedisStorage

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': on_failure_callback,
}

# Storage config from environment
redis_host = os.getenv('REDIS_HOST', 'redis')
redis_port = int(os.getenv('REDIS_PORT', '6379'))

postgres_host = os.getenv('POSTGRES_HOST', 'postgres-data')
postgres_port = int(os.getenv('POSTGRES_PORT', '5432'))
postgres_user = os.getenv('POSTGRES_USER', 'crypto')
postgres_password = os.getenv('POSTGRES_PASSWORD', 'crypto')
postgres_db = os.getenv('POSTGRES_DB', 'crypto_data')

minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
minio_bucket = os.getenv('MINIO_BUCKET', 'crypto-data')
minio_secure = os.getenv('MINIO_SECURE', 'false').lower() == 'true'


# Validation wrapper functions for PythonOperator
def run_aggregation_validation(**kwargs):
    """Wrapper function to run aggregation validation with RedisStorage."""
    redis_storage = RedisStorage(host=redis_host, port=redis_port)
    result = validate_aggregation_output(redis_storage)
    if not result.is_valid:
        raise ValueError(result.message)
    return result.message


def run_indicators_validation(**kwargs):
    """Wrapper function to run indicators validation with RedisStorage."""
    redis_storage = RedisStorage(host=redis_host, port=redis_port)
    result = validate_indicators_output(redis_storage)
    if not result.is_valid:
        raise ValueError(result.message)
    return result.message


def run_anomaly_validation(**kwargs):
    """Wrapper function to run anomaly validation with RedisStorage."""
    redis_storage = RedisStorage(host=redis_host, port=redis_port)
    result = validate_anomaly_output(redis_storage)
    if not result.is_valid:
        raise ValueError(result.message)
    return result.message


# Common environment variables for Spark jobs
spark_job_env = {
    'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
    'REDIS_HOST': redis_host,
    'REDIS_PORT': str(redis_port),
    # PostgreSQL Data Storage (Warm Path)
    'POSTGRES_HOST': postgres_host,
    'POSTGRES_PORT': str(postgres_port),
    'POSTGRES_USER': postgres_user,
    'POSTGRES_PASSWORD': postgres_password,
    'POSTGRES_DB': postgres_db,
    # MinIO Object Storage (Cold Path)
    'MINIO_ENDPOINT': minio_endpoint,
    'MINIO_ACCESS_KEY': minio_access_key,
    'MINIO_SECRET_KEY': minio_secret_key,
    'MINIO_BUCKET': minio_bucket,
    'MINIO_SECURE': str(minio_secure).lower(),
}


with DAG(
    dag_id='streaming_processing_dag',
    default_args=default_args,
    description='Spark streaming jobs for processing trade data from Kafka',
    schedule_interval='* * * * *',  # Run every 1 minute
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=['streaming', 'spark', 'processing'],
) as dag:
    
    # ==========================================================================
    # TaskGroup 1: Health Checks
    # ==========================================================================
    with TaskGroup("health_checks") as health_checks:
        test_redis_health = PythonOperator(
            task_id='test_redis_health',
            python_callable=check_redis_health,
            op_kwargs={
                'host': redis_host,
                'port': redis_port,
                'max_retries': 3,
            },
        )
        
        test_postgres_health = PythonOperator(
            task_id='test_postgres_health',
            python_callable=check_postgres_health,
            op_kwargs={
                'host': postgres_host,
                'port': postgres_port,
                'user': postgres_user,
                'password': postgres_password,
                'database': postgres_db,
                'max_retries': 3,
            },
        )
        
        test_minio_health = PythonOperator(
            task_id='test_minio_health',
            python_callable=check_minio_health,
            op_kwargs={
                'endpoint': minio_endpoint,
                'access_key': minio_access_key,
                'secret_key': minio_secret_key,
                'bucket': minio_bucket,
                'secure': minio_secure,
                'max_retries': 3,
            },
        )
    
    # ==========================================================================
    # TaskGroup 2: Trade Aggregation
    # ==========================================================================
    with TaskGroup("trade_aggregation") as trade_aggregation:
        run_trade_aggregation_job = BashOperator(
            task_id='run_trade_aggregation_job',
            bash_command='PYTHONPATH=/opt/airflow/src:$PYTHONPATH /usr/local/bin/python -m pyspark_streaming_processor.trade_aggregation_job',
            cwd='/opt/airflow',
            env=spark_job_env,
        )
        
        validate_aggregation = PythonOperator(
            task_id='validate_aggregation_output',
            python_callable=run_aggregation_validation,
        )
        
        # Internal dependency: run job then validate
        run_trade_aggregation_job >> validate_aggregation
    
    # ==========================================================================
    # TaskGroup 3: Technical Indicators
    # ==========================================================================
    with TaskGroup("technical_indicators") as technical_indicators:
        run_technical_indicators_job = BashOperator(
            task_id='run_technical_indicators_job',
            bash_command='PYTHONPATH=/opt/airflow/src:$PYTHONPATH /usr/local/bin/python -m pyspark_streaming_processor.technical_indicators_job',
            cwd='/opt/airflow',
            env=spark_job_env,
        )
        
        validate_indicators = PythonOperator(
            task_id='validate_indicators_output',
            python_callable=run_indicators_validation,
        )
        
        # Internal dependency: run job then validate
        run_technical_indicators_job >> validate_indicators
    
    # ==========================================================================
    # TaskGroup 4: Anomaly Detection
    # ==========================================================================
    with TaskGroup("anomaly_detection") as anomaly_detection:
        run_anomaly_detection_job = BashOperator(
            task_id='run_anomaly_detection_job',
            bash_command='PYTHONPATH=/opt/airflow/src:$PYTHONPATH /usr/local/bin/python -m pyspark_streaming_processor.anomaly_detection_job',
            cwd='/opt/airflow',
            env=spark_job_env,
        )
        
        validate_anomaly = PythonOperator(
            task_id='validate_anomaly_output',
            python_callable=run_anomaly_validation,
        )
        
        # Internal dependency: run job then validate
        run_anomaly_detection_job >> validate_anomaly
    
    # ==========================================================================
    # TaskGroup 5: Cleanup
    # ==========================================================================
    with TaskGroup("cleanup") as cleanup:
        cleanup_streaming_task = PythonOperator(
            task_id='cleanup_streaming',
            python_callable=cleanup_streaming_resources,
            op_kwargs={
                'redis_host': redis_host,
                'redis_port': redis_port,
            },
            trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of upstream status
        )
    
    # ==========================================================================
    # TaskGroup Dependencies
    # ==========================================================================
    # Maintain dependency order: health_checks >> trade_aggregation >> 
    # technical_indicators >> anomaly_detection >> cleanup
    health_checks >> trade_aggregation >> technical_indicators >> anomaly_detection >> cleanup
