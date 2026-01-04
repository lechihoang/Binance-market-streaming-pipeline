# Unified Dockerfile for Crypto Data Pipeline
# Supports: API, Binance Connector, Ticker Consumer, Airflow + PySpark

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including Java for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    libpq-dev \
    libsnappy-dev \
    libsnappy1v5 \
    openjdk-21-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Python dependencies
COPY requirements.txt ./

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir "apache-airflow==2.10.4"

# Copy application modules
COPY api/ ./api/
COPY binance_kafka_connector/ ./binance_kafka_connector/
COPY storage/ ./storage/
COPY streaming/ ./streaming/
COPY ticker_consumer/ ./ticker_consumer/
COPY utils/ ./utils/
COPY validators/ ./validators/

# Set Airflow home
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/app:/opt/airflow
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Create directories
RUN mkdir -p /app/data /opt/airflow/dags /opt/airflow/logs /tmp/spark-checkpoints

# Create non-root user
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app /opt/airflow

# Default command (overridden in docker-compose)
CMD ["python", "-m", "binance_kafka_connector"]
