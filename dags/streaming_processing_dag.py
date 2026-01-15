"""Runs Spark jobs every 5min - trade aggregation, anomaly detection, volatility prediction."""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow")

from storage.postgres import check_health as check_postgres_health
from storage.redis import check_health as check_redis_health
from util.cleanup import cleanup_streaming_resources
from util.constant import (
    KAFKA_SERVER,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    PYSPARK_PYTHON,
    REDIS_HOST,
    REDIS_PORT,
)

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

spark_job_env = {
    "KAFKA_BOOTSTRAP_SERVERS": KAFKA_SERVER,
    "REDIS_HOST": REDIS_HOST,
    "REDIS_PORT": str(REDIS_PORT),
    "POSTGRES_HOST": POSTGRES_HOST,
    "POSTGRES_PORT": str(POSTGRES_PORT),
    "POSTGRES_USER": POSTGRES_USER,
    "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
    "POSTGRES_DB": POSTGRES_DB,
    "PYSPARK_PYTHON": PYSPARK_PYTHON,
    "PYSPARK_DRIVER_PYTHON": PYSPARK_PYTHON,
}


with DAG(
    dag_id="streaming_processing_dag",
    default_args=default_args,
    description="Spark streaming jobs for processing trade data from Kafka",
    schedule_interval="*/5 * * * *",  # Run every 5 minutes (allows time for Spark startup + processing)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "spark", "processing"],
) as dag:
    with TaskGroup("health_checks") as health_checks:
        test_redis_health = PythonOperator(
            task_id="test_redis_health",
            python_callable=check_redis_health,
            op_kwargs={
                "host": REDIS_HOST,
                "port": REDIS_PORT,
                "max_retries": 3,
            },
        )

        test_postgres_health = PythonOperator(
            task_id="test_postgres_health",
            python_callable=check_postgres_health,
            op_kwargs={
                "host": POSTGRES_HOST,
                "port": POSTGRES_PORT,
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "database": POSTGRES_DB,
                "max_retries": 3,
            },
        )

    with TaskGroup("trade_aggregation") as trade_aggregation:
        run_trade_aggregation_job = BashOperator(
            task_id="run_trade_aggregation_job",
            bash_command="PYTHONPATH=/app:$PYTHONPATH /usr/local/bin/python /app/processing/trade_aggregation_job.py",
            cwd="/opt/airflow",
            env=spark_job_env,
        )

    with TaskGroup("volatility_prediction") as volatility_prediction:
        run_volatility_prediction_job = BashOperator(
            task_id="run_volatility_prediction_job",
            bash_command="PYTHONPATH=/app:$PYTHONPATH /usr/local/bin/python /app/processing/volatility_prediction_job.py",
            cwd="/opt/airflow",
            env=spark_job_env,
        )

    with TaskGroup("anomaly_detection") as anomaly_detection:
        run_anomaly_detection_job = BashOperator(
            task_id="run_anomaly_detection_job",
            bash_command="PYTHONPATH=/app:$PYTHONPATH /usr/local/bin/python /app/processing/anomaly_detection_job.py",
            cwd="/opt/airflow",
            env=spark_job_env,
        )

    cleanup_streaming_task = PythonOperator(
        task_id="cleanup_streaming",
        python_callable=cleanup_streaming_resources,
        op_kwargs={
            "redis_host": REDIS_HOST,
            "redis_port": REDIS_PORT,
        },
        trigger_rule=TriggerRule.ALL_DONE,
    )

    health_checks >> trade_aggregation
    trade_aggregation >> volatility_prediction >> cleanup_streaming_task
    trade_aggregation >> anomaly_detection >> cleanup_streaming_task
