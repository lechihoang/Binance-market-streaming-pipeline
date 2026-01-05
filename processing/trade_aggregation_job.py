"""Trade Aggregation Job - 1-minute OHLCV from trades."""

import os
import signal
from typing import Any, Optional

import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, window, count, sum as spark_sum, avg, min as spark_min,
    max as spark_max, first, last, stddev, when, to_json, struct, lit, expr,
)
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import DoubleType, TimestampType

from storage.redis import Redis
from storage.postgres import Postgres
from storage.storage_writer import Writer
from util.shutdown import GracefulShutdown
from util.metrics import record_error, record_message_processed
from validator.aggregation_validator import validate_aggregation_records

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC_TRADES = os.getenv("TOPIC_RAW_TRADES", "raw_trades")
TOPIC_AGGS = os.getenv("TOPIC_PROCESSED_AGGREGATIONS", "processed_aggregations")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT_LOCATION", "/opt/airflow/data/spark-checkpoints/trade-agg")
MAX_RUNTIME = int(os.getenv("SPARK_MAX_RUNTIME", "180"))

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "crypto")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "crypto")
PG_DB = os.getenv("POSTGRES_DB", "crypto_data")


class TradeAggJob:
    """1-minute OHLCV aggregation from trades."""

    def __init__(self):
        self.shutdown = GracefulShutdown(graceful_shutdown_timeout=15)
        self.spark: Optional[SparkSession] = None
        self.query: Any = None
        self.writer: Optional[Writer] = None
        self.schema_cache = ""
        signal.signal(signal.SIGTERM, lambda sig, _: self.shutdown.request_shutdown(sig))
        signal.signal(signal.SIGINT, lambda sig, _: self.shutdown.request_shutdown(sig))

    def get_schema(self) -> str:
        if self.schema_cache:
            return self.schema_cache
        resp = requests.get(f"{REGISTRY_URL}/subjects/raw_trades-value/versions/latest", timeout=10)
        resp.raise_for_status()
        self.schema_cache = resp.json()["schema"]
        return self.schema_cache

    def parse(self, df: DataFrame) -> DataFrame:
        schema = self.get_schema()
        parsed = df.select(
            from_avro(expr("substring(value, 6)"), schema).alias("trade"),
            col("timestamp").alias("kafka_timestamp"),
        )
        return parsed.select(
            (col("trade.event_time") / 1000).cast(TimestampType()).alias("event_time"),
            col("trade.symbol").alias("symbol"),
            col("trade.price").cast(DoubleType()).alias("price"),
            col("trade.quantity").cast(DoubleType()).alias("quantity"),
            col("trade.is_buyer_maker").alias("is_buyer_maker"),
        ).withWatermark("event_time", "1 minute")

    def aggregate(self, df: DataFrame) -> DataFrame:
        return (df
            .groupBy(window(col("event_time"), "1 minute"), col("symbol"))
            .agg(
                count("*").alias("trade_count"),
                spark_sum("quantity").alias("volume"),
                spark_sum(col("price") * col("quantity")).alias("quote_volume"),
                avg("price").alias("average_price"),
                spark_min("price").alias("low"),
                spark_max("price").alias("high"),
                first("price").alias("open"),
                last("price").alias("close"),
                stddev("price").alias("price_volatility"),
                spark_sum(when(~col("is_buyer_maker"), 1).otherwise(0)).alias("buy_count"),
                spark_sum(when(col("is_buyer_maker"), 1).otherwise(0)).alias("sell_count"),
            )
            .select(
                col("window.start").alias("timestamp"),
                col("window.end").alias("window_end"),
                lit("1m").alias("interval"),
                "symbol", "open", "high", "low", "close", "volume", "quote_volume",
                "trade_count", "average_price", "price_volatility", "buy_count", "sell_count",
            )
            .withColumn("volume_weighted_avg_price", col("quote_volume") / col("volume"))
            .withColumn("price_change_percent", ((col("close") - col("open")) / col("open")) * 100)
            .withColumn("buy_sell_ratio", when(col("sell_count") > 0, col("buy_count") / col("sell_count"))))

    def write_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        if self.shutdown.shutdown_requested:
            raise InterruptedError("Shutdown requested")
        if batch_df.isEmpty():
            return

        records = [row.asDict() for row in batch_df.collect()]

        try:
            kafka_df = batch_df.select(
                col("symbol").cast("string").alias("key"),
                to_json(struct(*[col(c) for c in batch_df.columns])).alias("value"),
            )
            (kafka_df.write.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("topic", TOPIC_AGGS)
                .save())
        except Exception:
            pass

        valid, invalid, _ = validate_aggregation_records(records, self.writer.pg if self.writer else None)
        if valid and self.writer:
            self.writer.write_aggs(valid)
            for _ in valid:
                record_message_processed("spark_trade_aggregation", "processed_aggregations", "success")

    def run(self) -> None:
        try:
            self.spark = (SparkSession.builder
                .appName("TradeAggJob")
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

            raw = (self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("subscribe", TOPIC_TRADES)
                .option("startingOffsets", "earliest")
                .load())

            aggs = self.aggregate(self.parse(raw))

            self.query = (aggs.writeStream
                .foreachBatch(self.write_batch)
                .outputMode("update")
                .trigger(processingTime="60 seconds")
                .option("checkpointLocation", CHECKPOINT)
                .start())

            self.query.awaitTermination(timeout=MAX_RUNTIME)
        except Exception as e:
            record_error("spark_trade_aggregation", "job_failure", "critical")
            raise
        finally:
            if self.query and self.query.isActive:
                self.query.stop()
            if self.spark:
                self.spark.stop()


if __name__ == "__main__":
    TradeAggJob().run()
