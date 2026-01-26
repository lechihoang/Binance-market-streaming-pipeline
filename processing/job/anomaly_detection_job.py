"""
Reads aggregated trades from PostgreSQL, detects 4 anomaly types:
volume spikes, price spikes, trade count spikes, buy/sell imbalance.
Writes alerts to PostgreSQL (staging/merge) + Redis cache.
"""

import signal
from datetime import datetime, timedelta

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from processing.validator.anomaly_validator import validate_alert_records
from schema.market import Alert
from storage.postgres import Postgres
from storage.redis import Redis
from util.constant import (
    BATCH_SIZE,
    BUY_RATIO_HIGH,
    BUY_RATIO_LOW,
    JOB_ANOMALY,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    PRICE_CHANGE_THRESHOLD,
    PYSPARK_PYTHON,
    REDIS_HOST,
    REDIS_PORT,
    TRADE_COUNT_MULTIPLIER,
    VOLUME_THRESHOLD,
)
from util.metric import record_error, record_message_processed

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

shutdown_requested = False
shutdown_signal = "Unknown"


def handle_shutdown(sig, frame):
    global shutdown_requested, shutdown_signal
    shutdown_requested = True
    shutdown_signal = {2: "SIGINT", 15: "SIGTERM"}.get(sig, f"Signal {sig}")


class AnomalyJob:
    def __init__(self):
        self.spark: SparkSession | None = None
        self.redis: Redis | None = None
        self.pg: Postgres | None = None
        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)

    def read_trades(self, start: datetime, end: datetime, lookback_minutes: int = 0) -> DataFrame:
        query_start = start - timedelta(minutes=lookback_minutes)
        return (
            self.spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option(
                "query",
                f"""
                SELECT timestamp, symbol, open, close, volume, quote_volume,
                       trade_count, buy_count, sell_count, buy_sell_ratio, price_change_percent
                FROM trades_1m
                WHERE timestamp > '{query_start}' AND timestamp <= '{end}'
                """,
            )
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

    def detect_anomalies(self, df: DataFrame, start: datetime) -> DataFrame:
        current = df.filter(F.col("timestamp") > start)

        volume_spike = current.filter(F.col("quote_volume") > VOLUME_THRESHOLD).select(
            F.col("timestamp"),
            F.col("symbol"),
            F.lit("VOLUME_SPIKE").alias("alert_type"),
            F.lit("MEDIUM").alias("alert_level"),
            F.to_json(F.struct("volume", "quote_volume", "trade_count")).alias("details"),
        )

        price_spike = current.filter(F.abs(F.col("price_change_percent")) > PRICE_CHANGE_THRESHOLD).select(
            F.col("timestamp"),
            F.col("symbol"),
            F.lit("PRICE_SPIKE").alias("alert_type"),
            F.lit("HIGH").alias("alert_level"),
            F.to_json(F.struct("open", "close", "price_change_percent")).alias("details"),
        )

        symbol_avg = (
            df.groupBy("symbol")
            .agg(F.count("*").alias("cnt"), F.avg("trade_count").alias("avg_tc"))
            .filter(F.col("cnt") >= 60)
        )

        trade_spike = (
            current.join(symbol_avg, "symbol")
            .filter(F.col("trade_count") > F.col("avg_tc") * TRADE_COUNT_MULTIPLIER)
            .select(
                F.col("timestamp"),
                F.col("symbol"),
                F.lit("TRADE_COUNT_SPIKE").alias("alert_type"),
                F.lit("MEDIUM").alias("alert_level"),
                F.to_json(
                    F.struct(
                        F.col("trade_count"),
                        F.col("buy_count"),
                        F.col("sell_count"),
                        F.round(F.col("avg_tc"), 2).alias("avg_trade_count"),
                        F.round(F.col("trade_count") / F.col("avg_tc"), 2).alias("multiplier"),
                    )
                ).alias("details"),
            )
        )

        imbalance = (
            current.filter(
                F.col("buy_sell_ratio").isNotNull()
                & ((F.col("buy_sell_ratio") < BUY_RATIO_LOW) | (F.col("buy_sell_ratio") > BUY_RATIO_HIGH))
            )
            .withColumn("pressure", F.when(F.col("buy_sell_ratio") < BUY_RATIO_LOW, "More Sell").otherwise("More Buy"))
            .select(
                F.col("timestamp"),
                F.col("symbol"),
                F.lit("BUY_SELL_IMBALANCE").alias("alert_type"),
                F.lit("MEDIUM").alias("alert_level"),
                F.to_json(F.struct("buy_sell_ratio", "buy_count", "sell_count", "trade_count", "pressure")).alias(
                    "details"
                ),
            )
        )

        return (
            volume_spike.union(price_spike)
            .union(trade_spike)
            .union(imbalance)
            .withColumn("alert_id", F.expr("uuid()"))
            .withColumn("created_at", F.current_timestamp())
        )

    def write_alerts_to_postgres(self, records: list) -> int:
        if not records:
            return 0
        pg_records = [Alert.model_validate(r).to_pg_dict() for r in records]
        staging_df = self.spark.createDataFrame(pg_records)
        (
            staging_df.write.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "staging_alerts")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )
        if self.pg:
            return self.pg.merge_staging_to_alert()
        return 0

    def process_batch(self, records: list, is_final: bool = False) -> tuple[int, int, datetime | None]:
        """Validate and write a batch of alert records. Returns (valid_count, invalid_count, max_timestamp)."""
        if not records:
            return 0, 0, None

        valid, invalid, _ = validate_alert_records(records, self.pg)
        max_ts = None

        if valid:
            self.write_alerts_to_postgres(valid)
            self.redis.save_alerts(valid)
            max_ts = max(r["timestamp"] for r in valid)

        label = "Final batch" if is_final else "Batch"
        logger.info(f"{label} processed: {len(valid)} valid, {len(invalid)} invalid")
        return len(valid), len(invalid), max_ts

    def run(self) -> None:
        """Read trades, detect anomalies, validate, write alerts, update checkpoint."""
        logger.info("Starting AnomalyJob")
        try:
            self.spark = (
                SparkSession.builder.appName("AnomalyJob")
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.pyspark.python", PYSPARK_PYTHON)
                .config("spark.pyspark.driver.python", PYSPARK_PYTHON)
                .config("spark.executorEnv.PYSPARK_PYTHON", PYSPARK_PYTHON)
                .getOrCreate()
            )

            self.pg = Postgres(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                database=POSTGRES_DB,
            )
            self.redis = Redis(host=REDIS_HOST, port=REDIS_PORT)

            checkpoint = self.pg.get_checkpoint(JOB_ANOMALY)
            start = checkpoint["last_processed_timestamp"] if checkpoint else datetime.now() - timedelta(hours=1)
            end = datetime.now()
            logger.info(f"Processing: {start} to {end} (local time)")

            df = self.read_trades(start, end, lookback_minutes=60)
            df.cache()

            row_count = df.count()
            logger.info(f"Read {row_count} rows")
            if row_count == 0:
                self.pg.update_checkpoint(JOB_ANOMALY, end, 0)
                return

            alerts_df = self.detect_anomalies(df, start)
            alerts_df = alerts_df.cache()
            alert_count = alerts_df.count()
            df.unpersist()

            logger.info(f"Found {alert_count} alerts")
            if alert_count == 0:
                alerts_df.unpersist()
                self.pg.update_checkpoint(JOB_ANOMALY, end, 0)
                return

            total_valid = 0
            total_invalid = 0
            max_ts = None
            batch = []

            for row in alerts_df.toLocalIterator():
                batch.append(row.asDict())
                if len(batch) >= BATCH_SIZE:
                    valid_n, invalid_n, batch_max_ts = self.process_batch(batch)
                    total_valid += valid_n
                    total_invalid += invalid_n
                    if batch_max_ts:
                        max_ts = batch_max_ts if max_ts is None else max(max_ts, batch_max_ts)
                    batch = []

            valid_n, invalid_n, batch_max_ts = self.process_batch(batch, is_final=True)
            total_valid += valid_n
            total_invalid += invalid_n
            if batch_max_ts:
                max_ts = batch_max_ts if max_ts is None else max(max_ts, batch_max_ts)

            alerts_df.unpersist()

            for _ in range(total_valid):
                record_message_processed("spark_anomaly_detection", "alerts", "success")

            self.pg.update_checkpoint(JOB_ANOMALY, max_ts if max_ts else end, total_valid)
            logger.info(f"AnomalyJob completed: {total_valid} valid, {total_invalid} invalid")

        except Exception:
            record_error("spark_anomaly_detection", "job_failure", "critical")
            raise
        finally:
            if self.redis:
                self.redis.close()
            if self.pg:
                self.pg.close()
            if self.spark:
                self.spark.stop()


if __name__ == "__main__":
    AnomalyJob().run()
