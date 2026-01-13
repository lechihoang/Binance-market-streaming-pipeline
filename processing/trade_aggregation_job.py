"""Trade Aggregation Job - 1-minute OHLCV from trades."""

import os
import signal
from typing import Any, List, Optional

import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, window, count, sum as spark_sum, avg, min as spark_min,
    max as spark_max, first, last, stddev, when, lit, expr,
)
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import DoubleType, TimestampType

from storage.redis import Redis
from storage.postgres import Postgres
from util.shutdown import GracefulShutdown
from util.metrics import record_error, record_message_processed
from util.logging import get_logger
from processing.validators.aggregation_validator import validate_aggregation_records

logger = get_logger(__name__)

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC_TRADES = os.getenv("TOPIC_RAW_TRADES", "raw_trades")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT_LOCATION", "/opt/airflow/data/spark-checkpoints/trade-agg")
MAX_RUNTIME = int(os.getenv("SPARK_MAX_RUNTIME", "180"))

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "crypto")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "crypto")
PG_DB = os.getenv("POSTGRES_DB", "crypto_data")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# Columns to write to PostgreSQL
PG_COLUMNS = [
    "timestamp", "symbol", "open", "high", "low", "close", "volume", "quote_volume",
    "trade_count", "buy_count", "sell_count", "volume_weighted_avg_price",
    "price_change_percent", "buy_sell_ratio", "average_price", "price_volatility",
]


class TradeAggJob:
    """1-minute OHLCV aggregation from trades."""

    def __init__(self):
        self.shutdown = GracefulShutdown(graceful_shutdown_timeout=15)
        self.spark: Optional[SparkSession] = None
        self.query: Any = None
        self.redis: Optional[Redis] = None
        self.pg: Optional[Postgres] = None
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

    def write_to_postgres(self, df: DataFrame) -> None:
        df.select(*PG_COLUMNS).write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "staging_trades_1m") \
            .option("user", PG_USER) \
            .option("password", PG_PASS) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        if self.pg:
            self.pg.merge_staging_to_trades()

    def write_to_redis(self, records: List[dict]) -> int:
        """Write records to Redis cache."""
        if not self.redis or not records:
            return 0
        
        # Convert timestamp to ISO string for Redis
        redis_records = []
        for r in records:
            record = dict(r)
            ts = record.get("timestamp")
            if ts and hasattr(ts, "isoformat"):
                record["timestamp"] = ts.isoformat()
            redis_records.append(record)
        
        return self.redis.write_aggs(redis_records)

    def write_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        if self.shutdown.shutdown_requested:
            raise InterruptedError("Shutdown requested")
        if batch_df.isEmpty():
            return

        # Validate records
        records = [row.asDict() for row in batch_df.collect()]
        valid, invalid, _ = validate_aggregation_records(records, None)
        
        if not valid:
            logger.warning(f"Batch {batch_id}: No valid records to write")
            return

        # Filter DataFrame to only valid records (by symbol+timestamp)
        valid_keys = {(r["symbol"], r["timestamp"]) for r in valid}
        valid_df = batch_df.filter(
            expr("concat(symbol, '|', timestamp)").isin(
                [f"{k[0]}|{k[1]}" for k in valid_keys]
            )
        )

        # 1. Write to PostgreSQL via staging + MERGE (primary storage)
        try:
            self.write_to_postgres(valid_df)
            logger.info(f"Batch {batch_id}: Wrote {len(valid)} records to PostgreSQL")
        except Exception as e:
            logger.error(f"Batch {batch_id}: Failed to write to PostgreSQL: {e}")
            record_error("spark_trade_aggregation", "postgres_write_error", "error")

        # 2. Write to Redis cache (for real-time queries)
        try:
            redis_count = self.write_to_redis(valid)
            logger.info(f"Batch {batch_id}: Wrote {redis_count} records to Redis")
        except Exception as e:
            logger.error(f"Batch {batch_id}: Failed to write to Redis: {e}")
            record_error("spark_trade_aggregation", "redis_write_error", "error")

        # Record metrics
        for _ in valid:
            record_message_processed("spark_trade_aggregation", "processed_aggregations", "success")

    def run(self) -> None:
        try:
            self.spark = (SparkSession.builder
                .appName("TradeAggJob")
                .config("spark.jars.packages",
                        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
                        "org.apache.spark:spark-avro_2.13:4.1.0,"
                        "org.postgresql:postgresql:42.7.4")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.streaming.checkpointLocation", CHECKPOINT)
                .getOrCreate())

            self.redis = Redis(host=REDIS_HOST, port=REDIS_PORT)
            self.pg = Postgres(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, database=PG_DB)

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
            logger.error(f"Trade aggregation job failed: {e}")
            raise
        finally:
            if self.pg:
                self.pg.close()
            if self.redis:
                self.redis.close()
            if self.query and self.query.isActive:
                self.query.stop()
            if self.spark:
                self.spark.stop()


if __name__ == "__main__":
    TradeAggJob().run()
