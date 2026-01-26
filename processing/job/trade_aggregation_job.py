"""
Trade Aggregation Job

Spark Structured Streaming: Kafka raw trades -> 1-minute OHLCV -> PostgreSQL + Redis.
Validates data quality with Great Expectations before writing.
"""

import signal
from typing import Any

import requests
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import broadcast, col, count, expr, lit, stddev, struct, when, window
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from processing.validator.aggregation_validator import validate_aggregation_records
from storage.postgres import Postgres
from storage.redis import Redis
from util.constant import (
    KAFKA_SERVER,
    MAX_RUNTIME,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    REDIS_HOST,
    REDIS_PORT,
    SCHEMA_REGISTRY_URL,
    SPARK_CHECKPOINT,
    TOPIC_TRADE,
)
from util.metric import record_error, record_message_processed

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

VALIDATION_FILTER_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
])

shutdown_requested = False
shutdown_signal = "Unknown"


def handle_shutdown(sig: int, frame: Any) -> None:
    """Set global shutdown flag on SIGTERM/SIGINT."""
    global shutdown_requested, shutdown_signal
    shutdown_requested = True
    shutdown_signal = {2: "SIGINT", 15: "SIGTERM"}.get(sig, f"Signal-{sig}")
    logger.warning(f"Shutdown requested: {shutdown_signal}")


class TradeAggregationJob:
    """Kafka raw trades -> 1m OHLCV candles -> PostgreSQL (staging/merge) + Redis cache."""

    def __init__(self):
        self.spark: SparkSession | None = None
        self.query: Any = None
        self.postgres: Postgres | None = None
        self.redis: Redis | None = None
        self.avro_schema_cache: str = ""
        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)

    def fetch_avro_schema(self) -> str:
        """Fetch Avro schema from Schema Registry, cached after first call."""
        if self.avro_schema_cache:
            return self.avro_schema_cache
        url = f"{SCHEMA_REGISTRY_URL}/subjects/raw_trades-value/versions/latest"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        self.avro_schema_cache = response.json()["schema"]
        return self.avro_schema_cache

    def parse_trades(self, raw_df: DataFrame) -> DataFrame:
        """Decode Avro from Kafka (skip 5-byte Confluent header), return structured trades with watermark."""
        avro_schema = self.fetch_avro_schema()
        parsed = raw_df.select(
            from_avro(expr("substring(value, 6)"), avro_schema).alias("trade"),
            col("timestamp").alias("kafka_timestamp"),
        )
        return (
            parsed.select(
                (col("trade.event_time") / 1000).cast(TimestampType()).alias("event_time"),
                col("trade.symbol").alias("symbol"),
                col("trade.price").cast(DoubleType()).alias("price"),
                col("trade.quantity").cast(DoubleType()).alias("quantity"),
                col("trade.is_buyer_maker").alias("is_buyer_maker"),
            )
            .withWatermark("event_time", "1 minute")
        )

    def aggregate_ohlcv(self, trades_df: DataFrame) -> DataFrame:
        """Group trades into 1-minute windows with OHLCV, trade counts, volatility, and derived metrics."""
        return (
            trades_df.groupBy(window(col("event_time"), "1 minute"), col("symbol"))
            .agg(
                count("*").alias("trade_count"),
                spark_sum("quantity").alias("volume"),
                spark_sum(col("price") * col("quantity")).alias("quote_volume"),
                spark_min("price").alias("low"),
                spark_max("price").alias("high"),
                spark_min(struct("event_time", "price")).alias("first_trade"),
                spark_max(struct("event_time", "price")).alias("last_trade"),
                stddev("price").alias("price_volatility"),
                spark_sum(when(~col("is_buyer_maker"), 1).otherwise(0)).alias("buy_count"),
                spark_sum(when(col("is_buyer_maker"), 1).otherwise(0)).alias("sell_count"),
            )
            .select(
                col("window.start").alias("timestamp"),
                col("window.end").alias("window_end"),
                lit("1m").alias("interval"),
                col("symbol"),
                col("first_trade.price").alias("open"),
                col("high"),
                col("low"),
                col("last_trade.price").alias("close"),
                col("volume"),
                col("quote_volume"),
                col("trade_count"),
                col("price_volatility"),
                col("buy_count"),
                col("sell_count"),
            )
            .withColumn("volume_weighted_avg_price", col("quote_volume") / col("volume"))
            .withColumn("average_price", col("volume_weighted_avg_price"))
            .withColumn(
                "price_change_percent",
                when(col("open") > 0, ((col("close") - col("open")) / col("open")) * 100).otherwise(0.0),
            )
            .withColumn(
                "buy_sell_ratio",
                when(col("sell_count") > 0, col("buy_count") / col("sell_count")).otherwise(lit(None)),
            )
        )

    def filter_valid_records(self, batch_df: DataFrame, batch_id: int) -> DataFrame | None:
        """Validate with Great Expectations, return only valid records joined back to full DataFrame."""
        records = [row.asDict() for row in batch_df.collect()]
        valid_records, invalid_records, _ = validate_aggregation_records(records, None)

        if not valid_records:
            logger.warning(f"Batch {batch_id}: All {len(records)} records failed validation")
            return None

        if invalid_records:
            logger.warning(f"Batch {batch_id}: {len(invalid_records)}/{len(records)} records failed validation")

        valid_keys = [(r["symbol"], r["timestamp"]) for r in valid_records]
        valid_keys_df = self.spark.createDataFrame(valid_keys, VALIDATION_FILTER_SCHEMA)
        valid_df = batch_df.join(broadcast(valid_keys_df), on=["symbol", "timestamp"], how="inner")

        logger.info(f"Batch {batch_id}: {len(valid_records)}/{len(records)} records passed validation")
        return valid_df

    def write_to_postgres(self, df: DataFrame) -> None:
        """Write to staging_trades_1m via JDBC, then merge into trades_1m."""
        (
            df.write.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "staging_trades_1m")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )
        if self.postgres:
            self.postgres.merge_staging_to_trade()

    def write_to_redis(self, records: list[dict]) -> int:
        """Write aggregated records to Redis cache."""
        if not self.redis or not records:
            return 0
        return self.redis.write_agg_batch(records)

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Callback for each Spark Structured Streaming micro-batch (60s trigger)."""
        if shutdown_requested:
            logger.warning(f"Batch {batch_id}: Shutdown signal ({shutdown_signal}), skipping")
            if self.query and self.query.isActive:
                self.query.stop()
            return

        if batch_df.isEmpty():
            logger.debug(f"Batch {batch_id}: Empty, skipping")
            return

        valid_df = self.filter_valid_records(batch_df, batch_id)
        if valid_df is None:
            return

        valid_records = [row.asDict() for row in valid_df.collect()]

        try:
            self.write_to_postgres(valid_df)
            logger.info(f"Batch {batch_id}: Wrote {len(valid_records)} records to PostgreSQL")
        except Exception as e:
            logger.error(f"Batch {batch_id}: PostgreSQL write failed: {e}")
            record_error("spark_trade_aggregation", "postgres_write_error", "error")

        try:
            redis_count = self.write_to_redis(valid_records)
            logger.info(f"Batch {batch_id}: Wrote {redis_count} records to Redis")
        except Exception as e:
            logger.error(f"Batch {batch_id}: Redis write failed: {e}")
            record_error("spark_trade_aggregation", "redis_write_error", "error")

        for _ in valid_records:
            record_message_processed("spark_trade_aggregation", "processed_aggregations", "success")

    def run(self) -> None:
        """Initialize Spark + storage, start streaming query, run until shutdown or MAX_RUNTIME."""
        try:
            self.spark = (
                SparkSession.builder.appName("TradeAggregationJob")
                .config(
                    "spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
                    "org.apache.spark:spark-avro_2.13:4.1.0,"
                    "org.postgresql:postgresql:42.7.4",
                )
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate()
            )
            logger.info("Spark session initialized")

            self.postgres = Postgres(
                host=POSTGRES_HOST, port=POSTGRES_PORT,
                user=POSTGRES_USER, password=POSTGRES_PASSWORD, database=POSTGRES_DB,
            )
            self.redis = Redis(host=REDIS_HOST, port=REDIS_PORT)
            logger.info("Storage connections initialized")

            raw_trades = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_SERVER)
                .option("subscribe", TOPIC_TRADE)
                .option("startingOffsets", "earliest")
                .load()
            )

            parsed_trades = self.parse_trades(raw_trades)
            aggregated_ohlcv = self.aggregate_ohlcv(parsed_trades)

            self.query = (
                aggregated_ohlcv.writeStream
                .foreachBatch(self.process_batch)
                .outputMode("update")
                .trigger(processingTime="60 seconds")
                .option("checkpointLocation", SPARK_CHECKPOINT)
                .start()
            )
            logger.info("Streaming query started")

            self.query.awaitTermination(timeout=MAX_RUNTIME)

        except Exception as e:
            logger.error(f"Job failed: {e}", exc_info=True)
            record_error("spark_trade_aggregation", "job_failure", "critical")
            raise

        finally:
            logger.info("Shutting down...")
            if self.postgres:
                self.postgres.close()
            if self.redis:
                self.redis.close()
            if self.query and self.query.isActive:
                self.query.stop()
            if self.spark:
                self.spark.stop()
            logger.info("Shutdown complete")


if __name__ == "__main__":
    TradeAggregationJob().run()
