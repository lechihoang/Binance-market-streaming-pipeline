"""Streaming Processing DAG."""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, '/opt/airflow')

from util.cleanup import cleanup_streaming_resources

from storage.redis import check_health as check_redis_health
from storage.postgres import check_health as check_postgres_health

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

redis_host = os.getenv('REDIS_HOST', 'redis')
redis_port = int(os.getenv('REDIS_PORT', '6379'))

postgres_host = os.getenv('POSTGRES_HOST', 'postgres-data')
postgres_port = int(os.getenv('POSTGRES_PORT', '5432'))
postgres_user = os.getenv('POSTGRES_USER', 'crypto')
postgres_password = os.getenv('POSTGRES_PASSWORD', 'crypto')
postgres_db = os.getenv('POSTGRES_DB', 'crypto_data')

spark_job_env = {
    'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
    'REDIS_HOST': redis_host,
    'REDIS_PORT': str(redis_port),
    'POSTGRES_HOST': postgres_host,
    'POSTGRES_PORT': str(postgres_port),
    'POSTGRES_USER': postgres_user,
    'POSTGRES_PASSWORD': postgres_password,
    'POSTGRES_DB': postgres_db,
}


with DAG(
    dag_id='streaming_processing_dag',
    default_args=default_args,
    description='Spark streaming jobs for processing trade data from Kafka',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes (allows time for Spark startup + processing)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['streaming', 'spark', 'processing'],
) as dag:
    
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
    
    with TaskGroup("trade_aggregation") as trade_aggregation:
        run_trade_aggregation_job = BashOperator(
            task_id='run_trade_aggregation_job',
            bash_command='PYTHONPATH=/app:$PYTHONPATH /usr/local/bin/python /app/processing/trade_aggregation_job.py',
            cwd='/opt/airflow',
            env=spark_job_env,
        )
    
    with TaskGroup("anomaly_detection") as anomaly_detection:
        run_anomaly_detection_job = BashOperator(
            task_id='run_anomaly_detection_job',
            bash_command='PYTHONPATH=/app:$PYTHONPATH /usr/local/bin/python /app/processing/anomaly_detection_job.py',
            cwd='/opt/airflow',
            env=spark_job_env,
        )
    
    cleanup_streaming_task = PythonOperator(
        task_id='cleanup_streaming',
        python_callable=cleanup_streaming_resources,
        op_kwargs={
            'redis_host': redis_host,
            'redis_port': redis_port,
        },
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    health_checks >> trade_aggregation >> anomaly_detection >> cleanup_streaming_task
