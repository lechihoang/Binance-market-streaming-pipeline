"""Binance Connector DAG."""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, '/opt/airflow')

from util.cleanup import cleanup_connector_resources

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
    
    def check_kafka_health(**context):
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
    
    connector_env = {
        'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
        'SCHEMA_REGISTRY_URL': os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081'),
        'TICKER_SYMBOLS': os.getenv('TICKER_SYMBOLS', 'BTCUSDT,ETHUSDT,BNBUSDT,XRPUSDT,SOLUSDT,ADAUSDT,DOGEUSDT,TRXUSDT,AVAXUSDT,LINKUSDT,DOTUSDT,MATICUSDT,SHIBUSDT,LTCUSDT,ATOMUSDT'),
    }
    if os.getenv('BINANCE_STREAMS'):
        connector_env['BINANCE_STREAMS'] = os.getenv('BINANCE_STREAMS')
    
    run_binance_connector = BashOperator(
        task_id='run_binance_connector',
        bash_command='PYTHONPATH=/app:$PYTHONPATH /usr/local/bin/python /app/ingestion/connector.py',
        cwd='/opt/airflow',
        env=connector_env,
        append_env=True,
    )
    
    cleanup_connector_task = PythonOperator(
        task_id='cleanup_connector',
        python_callable=cleanup_connector_resources,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    test_kafka_health >> run_binance_connector >> cleanup_connector_task
