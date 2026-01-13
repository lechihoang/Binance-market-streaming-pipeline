"""Anomaly Detection Job - batch mode từ trades_1m."""

import json
import os
import signal
from datetime import datetime, timezone, timedelta
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from storage.redis import Redis
from storage.postgres import Postgres
from util.shutdown import GracefulShutdown
from util.metrics import record_error, record_message_processed
from util.logging import get_logger
from processing.validators.anomaly_validator import validate_alert_records

logger = get_logger(__name__)

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "crypto")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "crypto")
PG_DB = os.getenv("POSTGRES_DB", "crypto_data")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# Thresholds
VOLUME_THRESHOLD = 1_000_000.0
PRICE_CHANGE_THRESHOLD = 2.0
TRADE_COUNT_MULTIPLIER = 3.0
BUY_RATIO_LOW = 0.3
BUY_RATIO_HIGH = 0.7

BATCH_SIZE = 100
JOB_NAME = "anomaly_detection"


class AnomalyJob:

    def __init__(self):
        self.shutdown = GracefulShutdown(graceful_shutdown_timeout=30)
        self.spark: Optional[SparkSession] = None
        self.redis: Optional[Redis] = None
        self.pg: Optional[Postgres] = None
        signal.signal(signal.SIGTERM, lambda s, _: self.shutdown.request_shutdown(s))
        signal.signal(signal.SIGINT, lambda s, _: self.shutdown.request_shutdown(s))

    def read_trades(self, start: datetime, end: datetime, lookback_minutes: int = 0) -> DataFrame:
        """Read trades_1m via Spark JDBC."""
        query_start = start - timedelta(minutes=lookback_minutes)
        return (self.spark.read
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("query", f"""
                SELECT timestamp, symbol, open, close, volume, quote_volume, 
                       trade_count, buy_count, sell_count, buy_sell_ratio, price_change_percent
                FROM trades_1m
                WHERE timestamp > '{query_start}' AND timestamp <= '{end}'
            """)
            .option("user", PG_USER)
            .option("password", PG_PASS)
            .option("driver", "org.postgresql.Driver")
            .load())

    def detect_anomalies(self, df: DataFrame, start: datetime) -> DataFrame:
        """Detect all anomalies in one pass and return DataFrame ready for writing."""
        current = df.filter(F.col("timestamp") > start)
        
        # Volume spike
        volume_spike = (current
            .filter(F.col("quote_volume") > VOLUME_THRESHOLD)
            .select(
                F.col("timestamp"), F.col("symbol"),
                F.lit("VOLUME_SPIKE").alias("alert_type"),
                F.lit("MEDIUM").alias("alert_level"),
                F.to_json(F.struct("volume", "quote_volume", "trade_count")).alias("details")
            ))
        
        # Price spike
        price_spike = (current
            .filter(F.abs(F.col("price_change_percent")) > PRICE_CHANGE_THRESHOLD)
            .select(
                F.col("timestamp"), F.col("symbol"),
                F.lit("PRICE_SPIKE").alias("alert_type"),
                F.lit("HIGH").alias("alert_level"),
                F.to_json(F.struct("open", "close", "price_change_percent")).alias("details")
            ))
        
        # Trade count spike (needs 60-min avg, skip if < 60 records)
        symbol_avg = (df
            .groupBy("symbol")
            .agg(F.count("*").alias("cnt"), F.avg("trade_count").alias("avg_tc"))
            .filter(F.col("cnt") >= 60))
        
        trade_spike = (current
            .join(symbol_avg, "symbol")
            .filter(F.col("trade_count") > F.col("avg_tc") * TRADE_COUNT_MULTIPLIER)
            .select(
                F.col("timestamp"), F.col("symbol"),
                F.lit("TRADE_COUNT_SPIKE").alias("alert_type"),
                F.lit("MEDIUM").alias("alert_level"),
                F.to_json(F.struct(
                    F.col("trade_count"),
                    F.round(F.col("avg_tc"), 2).alias("avg_trade_count"),
                    F.round(F.col("trade_count") / F.col("avg_tc"), 2).alias("multiplier")
                )).alias("details")
            ))
        
        # Buy/sell imbalance
        imbalance = (current
            .filter(
                F.col("buy_sell_ratio").isNotNull() &
                ((F.col("buy_sell_ratio") < BUY_RATIO_LOW) | (F.col("buy_sell_ratio") > BUY_RATIO_HIGH))
            )
            .withColumn("pressure", 
                F.when(F.col("buy_sell_ratio") < BUY_RATIO_LOW, "SELL_PRESSURE")
                 .otherwise("BUY_PRESSURE"))
            .select(
                F.col("timestamp"), F.col("symbol"),
                F.lit("BUY_SELL_IMBALANCE").alias("alert_type"),
                F.lit("MEDIUM").alias("alert_level"),
                F.to_json(F.struct("buy_sell_ratio", "buy_count", "sell_count", "trade_count", "pressure")).alias("details")
            ))
        
        # Union all and add alert_id, created_at for direct write
        all_alerts = volume_spike.union(price_spike).union(trade_spike).union(imbalance)
        
        return all_alerts.withColumn(
            "alert_id", F.expr("uuid()")
        ).withColumn(
            "created_at", F.current_timestamp()
        )

    def write_alerts_to_postgres(self, records: list) -> int:
        if not records:
            return 0
        
        pg_records = []
        for r in records:
            pg_records.append({
                "timestamp": r["timestamp"],
                "symbol": r["symbol"],
                "alert_type": r["alert_type"],
                "severity": r["alert_level"],
                "message": f"{r['alert_type']}: {r['symbol']}",
                "metadata": r["details"],
            })
        
        staging_df = self.spark.createDataFrame(pg_records)
        
        staging_df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "staging_alerts") \
            .option("user", PG_USER) \
            .option("password", PG_PASS) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        if self.pg:
            return self.pg.merge_staging_to_alerts()
        return 0

    def write_alerts_to_redis(self, records: list) -> None:
        if not self.redis or not records:
            return
        
        redis_records = []
        for r in records:
            redis_records.append({
                "alert_id": r["alert_id"],
                "timestamp": r["timestamp"].isoformat() if hasattr(r["timestamp"], 'isoformat') else str(r["timestamp"]),
                "symbol": r["symbol"],
                "alert_type": r["alert_type"],
                "alert_level": r["alert_level"],
                "created_at": r["created_at"].isoformat() if hasattr(r["created_at"], 'isoformat') else str(r["created_at"]),
                "details": json.loads(r["details"]) if isinstance(r["details"], str) else r["details"],
            })
        
        self.redis.write_alerts(redis_records)

    def run(self) -> None:
        logger.info("Starting AnomalyJob")
        try:
            self.spark = (SparkSession.builder
                .appName("AnomalyJob")
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.pyspark.python", "/usr/local/bin/python3.11")
                .config("spark.pyspark.driver.python", "/usr/local/bin/python3.11")
                .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/local/bin/python3.11")
                .getOrCreate())

            self.pg = Postgres(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, database=PG_DB)
            self.redis = Redis(host=REDIS_HOST, port=REDIS_PORT)

            # Time range
            checkpoint = self.pg.get_checkpoint(JOB_NAME)
            start = checkpoint["last_processed_timestamp"] if checkpoint else datetime.now(timezone.utc) - timedelta(hours=1)
            end = datetime.now(timezone.utc)
            logger.info(f"Processing: {start} to {end}")

            # Read with 60-min lookback for trade count avg
            df = self.read_trades(start, end, lookback_minutes=60)
            df.cache()
            
            count = df.count()
            logger.info(f"Read {count} rows")
            if count == 0:
                self.pg.update_checkpoint(JOB_NAME, end, 0)
                return

            # Detect anomalies - returns DataFrame with alert_id, created_at columns
            alerts_df = self.detect_anomalies(df, start)
            alerts_df = alerts_df.cache()
            alert_count = alerts_df.count()
            df.unpersist()
            
            logger.info(f"Found {alert_count} alerts")
            if alert_count == 0:
                alerts_df.unpersist()
                self.pg.update_checkpoint(JOB_NAME, end, 0)
                return

            # Process in batches using toLocalIterator (memory efficient)
            total_valid = 0
            total_invalid = 0
            max_ts = None
            batch = []
            
            for row in alerts_df.toLocalIterator():
                batch.append(row.asDict())
                
                if len(batch) >= BATCH_SIZE:
                    valid, invalid, _ = validate_alert_records(batch, self.pg)
                    
                    if valid:
                        self.write_alerts_to_postgres(valid)
                        self.write_alerts_to_redis(valid)
                        batch_max_ts = max(r["timestamp"] for r in valid)
                        max_ts = batch_max_ts if max_ts is None else max(max_ts, batch_max_ts)
                        total_valid += len(valid)
                    
                    total_invalid += len(invalid)
                    logger.info(f"Batch processed: {len(valid)} valid, {len(invalid)} invalid")
                    batch = []
            
            # Process remaining batch
            if batch:
                valid, invalid, _ = validate_alert_records(batch, self.pg)
                
                if valid:
                    self.write_alerts_to_postgres(valid)
                    self.write_alerts_to_redis(valid)
                    batch_max_ts = max(r["timestamp"] for r in valid)
                    max_ts = batch_max_ts if max_ts is None else max(max_ts, batch_max_ts)
                    total_valid += len(valid)
                
                total_invalid += len(invalid)
                logger.info(f"Final batch processed: {len(valid)} valid, {len(invalid)} invalid")
            
            alerts_df.unpersist()
            
            for _ in range(total_valid):
                record_message_processed("spark_anomaly_detection", "alerts", "success")

            if max_ts:
                self.pg.update_checkpoint(JOB_NAME, max_ts, total_valid)
            else:
                self.pg.update_checkpoint(JOB_NAME, end, 0)
            
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
