FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ curl libpq-dev libsnappy-dev libsnappy1v5 librdkafka-dev \
    openjdk-21-jre-headless procps && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV _JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"

COPY docker/airflow/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir "apache-airflow==2.10.4"

COPY api/ ./api/
COPY ingestion/ ./ingestion/
COPY storage/ ./storage/
COPY processing/ ./processing/
COPY util/ ./util/
COPY models/ ./models/

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/app:/opt/airflow
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=/usr/local/bin/python3.11
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11

RUN mkdir -p /app/data /opt/airflow/dags /opt/airflow/logs /tmp/spark-checkpoints
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app /opt/airflow

CMD ["airflow", "version"]
