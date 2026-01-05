"""Anomaly Detection Job - whale trades, volume spikes, price spikes."""

import json
import os
import signal
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    abs as spark_abs, col, current_timestamp, expr, from_json,
    lit, struct, to_json, when,
)
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType,
    StructField, StructType, TimestampType,
)

from storage.redis import Redis
from storage.postgres import Postgres
from storage.storage_writer import Writer
from util.shutdown import GracefulShutdown
from util.metrics import record_error, record_message_processed
from util.kafka import KafkaProducer
from validator.anomaly_validator import validate_alert_records

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC_TRADES = os.getenv("TOPIC_RAW_TRADES", "raw_trades")
TOPIC_AGGS = os.getenv("TOPIC_PROCESSED_AGGREGATIONS", "processed_aggregations")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "alerts")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT_LOCATION", "/opt/airflow/data/spark-checkpoints/anomaly")
MAX_RUNTIME = int(os.getenv("SPARK_MAX_RUNTIME", "180"))

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "crypto")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "crypto")
PG_DB = os.getenv("POSTGRES_DB", "crypto_data")

WHALE_MIN = 100000.0
VOLUME_MIN = 1000000.0
PRICE_PCT = 2.0

TRADE_SCHEMA = StructType([
    StructField("E", LongType(), False),
    StructField("s", StringType(), False),
    StructField("p", StringType(), False),
    StructField("q", StringType(), False),
    StructField("m", BooleanType(), False),
])

AGG_SCHEMA = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("symbol", StringType(), False),
    StructField("open", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", DoubleType(), False),
    StructField("quote_volume", DoubleType(), False),
    StructField("trade_count", LongType(), False),
    StructField("price_change_percent", DoubleType(), True),
    StructField("interval", StringType(), False),
])


def build_alert_df(df: DataFrame, alert_type: str, alert_level: str, detail_cols: List[str]) -> DataFrame:
    return df.select(
        col("timestamp"), col("symbol"),
        lit(alert_type).alias("alert_type"),
        lit(alert_level).alias("alert_level"),
        to_json(struct(*[col(c) for c in detail_cols])).alias("details"),
        expr("uuid()").alias("alert_id"),
        current_timestamp().alias("created_at"),
    )


class AnomalyJob:
    """Detect whale trades, volume spikes, price spikes."""

    def __init__(self):
        self.shutdown = GracefulShutdown(graceful_shutdown_timeout=90)
        self.spark: Optional[SparkSession] = None
        self.query: Any = None
        self.writer: Optional[Writer] = None
        self.producer: Optional[KafkaProducer] = None
        signal.signal(signal.SIGTERM, lambda sig, _: self.shutdown.request_shutdown(sig))
        signal.signal(signal.SIGINT, lambda sig, _: self.shutdown.request_shutdown(sig))

    def whale_alerts(self, df: DataFrame) -> DataFrame:
        parsed = df.select(from_json(col("value").cast("string"), TRADE_SCHEMA).alias("t"))
        trades = parsed.select(
            (col("t.E") / 1000).cast(TimestampType()).alias("timestamp"),
            col("t.s").alias("symbol"),
            col("t.p").cast(DoubleType()).alias("price"),
            col("t.q").cast(DoubleType()).alias("quantity"),
            when(col("t.m"), lit("SELL")).otherwise(lit("BUY")).alias("side"),
        ).withColumn("value", col("price") * col("quantity"))
        trades = trades.withWatermark("timestamp", "1 minute")
        whales = trades.filter(col("value") > WHALE_MIN)
        return build_alert_df(whales, "WHALE_ALERT", "HIGH", ["price", "quantity", "value", "side"])

    def volume_alerts(self, df: DataFrame) -> DataFrame:
        parsed = df.select(from_json(col("value").cast("string"), AGG_SCHEMA).alias("a"))
        aggs = parsed.select(
            col("a.timestamp").alias("timestamp"),
            col("a.symbol").alias("symbol"),
            col("a.volume").alias("volume"),
            col("a.quote_volume").alias("quote_volume"),
            col("a.trade_count").alias("trade_count"),
            col("a.interval").alias("interval"),
        ).filter(col("interval") == "1m")
        aggs = aggs.withWatermark("timestamp", "1 minute")
        spikes = aggs.filter(col("quote_volume") > VOLUME_MIN)
        return build_alert_df(spikes, "VOLUME_SPIKE", "MEDIUM", ["volume", "quote_volume", "trade_count"])

    def price_alerts(self, df: DataFrame) -> DataFrame:
        parsed = df.select(from_json(col("value").cast("string"), AGG_SCHEMA).alias("a"))
        aggs = parsed.select(
            col("a.timestamp").alias("timestamp"),
            col("a.symbol").alias("symbol"),
            col("a.open").alias("open"),
            col("a.close").alias("close"),
            col("a.price_change_percent").alias("price_change_percent"),
            col("a.interval").alias("interval"),
        ).filter(col("interval") == "1m")
        aggs = aggs.withWatermark("timestamp", "1 minute")
        spikes = aggs.filter(spark_abs(col("price_change_percent")) > PRICE_PCT)
        return build_alert_df(spikes, "PRICE_SPIKE", "HIGH", ["open", "close", "price_change_percent"])

    def to_avro(self, alert: Dict[str, Any]) -> Dict[str, Any]:
        ts = alert.get("timestamp")
        created = alert.get("created_at")
        details = alert.get("details", {})
        ts_ms = int(ts.timestamp() * 1000) if isinstance(ts, datetime) else (int(ts) if ts else 0)
        created_ms = int(created.timestamp() * 1000) if isinstance(created, datetime) else (int(created) if created else 0)
        return {
            "alert_id": alert["alert_id"],
            "timestamp": ts_ms,
            "symbol": alert["symbol"],
            "alert_type": alert["alert_type"],
            "alert_level": alert["alert_level"],
            "details": json.dumps(details) if isinstance(details, dict) else str(details),
            "created_at": created_ms,
        }

    def send_alerts(self, alerts: List[Dict[str, Any]]) -> None:
        if not alerts or not self.producer:
            return
        for a in alerts:
            self.producer.send(value=self.to_avro(a), key=a["symbol"])
        self.producer.flush()

        records = [{
            "alert_id": a.get("alert_id"),
            "timestamp": a.get("timestamp"),
            "symbol": a.get("symbol"),
            "alert_type": a.get("alert_type"),
            "alert_level": a.get("alert_level"),
            "created_at": a.get("created_at"),
            "details": a.get("details", "{}"),
        } for a in alerts]

        if self.writer:
            self.writer.write_alerts(records)

    def write_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        if self.shutdown.shutdown_requested:
            raise InterruptedError("Shutdown requested")
        if batch_df.isEmpty():
            return

        rows = batch_df.collect()
        alerts = []
        for row in rows:
            details = json.loads(row.details) if row.details else {}
            alerts.append({
                "alert_id": row.alert_id,
                "timestamp": row.timestamp,
                "symbol": row.symbol,
                "alert_type": row.alert_type,
                "alert_level": row.alert_level,
                "details": details,
                "created_at": datetime.now(timezone.utc),
            })

        valid, invalid, _ = validate_alert_records(alerts, self.writer.pg if self.writer else None)
        for _ in valid:
            record_message_processed("spark_anomaly_detection", "alerts", "success")
        self.send_alerts(valid)

    def run(self) -> None:
        try:
            self.spark = (SparkSession.builder
                .appName("AnomalyJob")
                .config("spark.jars.packages",
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                        "org.apache.spark:spark-avro_2.12:3.5.3")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.streaming.checkpointLocation", CHECKPOINT)
                .getOrCreate())

            self.writer = Writer(
                redis=Redis(host=REDIS_HOST, port=REDIS_PORT),
                postgres=Postgres(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, database=PG_DB),
            )
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                schema_registry_url=REGISTRY_URL,
                topic=TOPIC_ALERTS,
            )

            def read_stream(topic: str) -> DataFrame:
                assert self.spark is not None
                return (self.spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                    .option("subscribe", topic)
                    .option("startingOffsets", "earliest")
                    .load())

            trades = read_stream(TOPIC_TRADES)
            aggs = read_stream(TOPIC_AGGS)

            all_alerts = (self.whale_alerts(trades)
                .union(self.volume_alerts(aggs))
                .union(self.price_alerts(aggs)))

            self.query = (all_alerts.writeStream
                .foreachBatch(self.write_batch)
                .outputMode("append")
                .trigger(processingTime="60 seconds")
                .option("checkpointLocation", CHECKPOINT)
                .start())

            self.query.awaitTermination(timeout=MAX_RUNTIME)
        except Exception:
            record_error("spark_anomaly_detection", "job_failure", "critical")
            raise
        finally:
            if self.producer:
                self.producer.close()
            if self.query and self.query.isActive:
                self.query.stop()
            if self.spark:
                self.spark.stop()


if __name__ == "__main__":
    AnomalyJob().run()
