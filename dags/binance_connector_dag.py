"""
Binance Connector DAG

Orchestrates the Binance WebSocket connector that ingests trade data into Kafka.
This DAG runs independently from the streaming processing DAG.

Tasks:
- test_kafka_health: Check Kafka connectivity before starting connector
- run_binance_connector: Run Binance WebSocket connector
- cleanup_connector: Cleanup resources when connector finishes or fails
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, '/opt/airflow')

from cleanup_utils import cleanup_connector_resources

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


with DAG(
    dag_id='binance_connector_dag',
    default_args=default_args,
    description='Binance WebSocket connector for ingesting trade data into Kafka',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['streaming', 'binance', 'connector'],
) as dag:
    
    # Task 1: Test Kafka health before starting connector
    def check_kafka_health(**context):
        """Simple Kafka health check using socket connection."""
        import socket
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        host, port = kafka_servers.split(':')[0], int(kafka_servers.split(':')[1])
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((host, port))
        sock.close()
        if result != 0:
            raise Exception(f"Kafka not reachable at {host}:{port}")
        return {'status': 'healthy', 'host': host, 'port': port}
    
    test_kafka_health = PythonOperator(
        task_id='test_kafka_health',
        python_callable=check_kafka_health,
    )
    
    # Task 2: Run Binance connector
    # Streams config is loaded from binance_kafka_connector.config module
    # Only override BINANCE_STREAMS env var if explicitly set, otherwise use module defaults
    connector_env = {
        'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
    }
    # Only pass BINANCE_STREAMS if explicitly set in environment
    if os.getenv('BINANCE_STREAMS'):
        connector_env['BINANCE_STREAMS'] = os.getenv('BINANCE_STREAMS')
    
    run_binance_connector = BashOperator(
        task_id='run_binance_connector',
        bash_command='PYTHONPATH=/opt/airflow:$PYTHONPATH /usr/local/bin/python src/binance_kafka_connector/connector.py',
        cwd='/opt/airflow',
        env=connector_env,
    )
    
    # Task 3: Cleanup connector resources
    cleanup_connector_task = PythonOperator(
        task_id='cleanup_connector',
        python_callable=cleanup_connector_resources,
        trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of upstream status
    )
    
    # Set dependencies
    test_kafka_health >> run_binance_connector >> cleanup_connector_task
