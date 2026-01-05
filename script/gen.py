"""Generate Architecture Diagram for Real-Time Crypto Pipeline."""

from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.queue import Kafka
from diagrams.onprem.analytics import Spark
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.monitoring import Grafana
from diagrams.onprem.network import Internet
from diagrams.programming.framework import Fastapi
from diagrams.aws.storage import S3  # Use S3 icon for MinIO (S3-compatible)

graph_attr = {
    "fontsize": "16",
    "bgcolor": "white",
    "pad": "0.5",
    "splines": "line",  # Straight lines
    "nodesep": "0.8",
    "ranksep": "1.0",
}

with Diagram(
    "Real-time cryptocurrency data pipeline",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    filename="real_time_crypto_pipeline",
    outformat="png",
):
    # Source - Binance WebSocket (standalone, left side)
    source = Internet("Binance\nWebSocket")

    # Ingestion Layer - Kafka Topics
    with Cluster("Kafka"):
        kafka_trades = Kafka("raw_trades")
        kafka_tickers = Kafka("raw_tickers")
        kafka_aggs = Kafka("processed_aggs")

    # Processing Layer
    with Cluster("Stream Processing"):
        spark_agg = Spark("Trade\nAggregation")
        spark_anomaly = Spark("Anomaly\nDetection")

    # Storage Layer
    with Cluster("Storage"):
        redis = Redis("Redis\n(Hot)")
        postgres = PostgreSQL("PostgreSQL\n(Warm)")
        minio = S3("MinIO\n(Cold)")

    # Serving Layer
    with Cluster("Serving"):
        api = Fastapi("FastAPI")
        dashboard = Grafana("Grafana")

    # === Data Flow (all edges same color) ===

    # Source to Kafka
    source >> kafka_trades
    source >> kafka_tickers

    # Trades processing path
    kafka_trades >> spark_agg

    # Spark Agg outputs
    spark_agg >> kafka_aggs
    spark_agg >> redis
    spark_agg >> postgres
    spark_agg >> minio

    # Anomaly Detection inputs
    kafka_trades >> Edge(style="dashed") >> spark_anomaly
    kafka_aggs >> spark_anomaly

    # Anomaly outputs
    spark_anomaly >> redis
    spark_anomaly >> postgres
    spark_anomaly >> minio

    # Tickers to Redis
    kafka_tickers >> redis

    # Storage to API
    redis >> api
    postgres >> api
    minio >> api

    # API to Dashboard
    api >> dashboard


if __name__ == "__main__":
    print("Diagram generated: real_time_crypto_pipeline.png")
