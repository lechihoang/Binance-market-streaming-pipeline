"""
Volatility Prediction Job

Reads aggregated trades from PostgreSQL, computes 24 rolling features,
predicts next 5-minute volatility using a LightGBM model.
Writes features and predictions to PostgreSQL (staging/merge).
"""

import signal
from datetime import datetime, timedelta
from pathlib import Path

import lightgbm as lgb
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window

from storage.postgres import Postgres
from util.constant import (
    JOB_VOLATILITY,
    LOOKBACK_HOUR,
    MODEL_DIR,
    MODEL_FILE,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    PYSPARK_PYTHON,
)
from util.metric import record_error, record_message_processed

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

FEATURE_COLUMNS = [
    "return_1m",
    "volatility_15m",
    "volatility_30m",
    "volatility_60m",
    "price_range_pct",
    "price_body_pct",
    "volume_ratio_15m",
    "volume_ratio_60m",
    "buy_ratio",
    "buy_sell_imbalance",
    "price_vs_ma_15m",
    "price_vs_ma_60m",
    "hour",
    "symbol_encoded",
]

SYMBOL_ENCODING: dict[str, int] = {
    "AAVEUSDT": 0,
    "ADAUSDT": 1,
    "ALGOUSDT": 2,
    "APTUSDT": 3,
    "ARBUSDT": 4,
    "ATOMUSDT": 5,
    "AVAXUSDT": 6,
    "BCHUSDT": 7,
    "BNBUSDT": 8,
    "BONKUSDT": 9,
    "BTCUSDT": 10,
    "DOGEUSDT": 11,
    "DOTUSDT": 12,
    "ENAUSDT": 13,
    "ETCUSDT": 14,
    "ETHUSDT": 15,
    "FILUSDT": 16,
    "HBARUSDT": 17,
    "ICPUSDT": 18,
    "LINKUSDT": 19,
    "LTCUSDT": 20,
    "NEARUSDT": 21,
    "ONDOUSDT": 22,
    "OPUSDT": 23,
    "PEPEUSDT": 24,
    "RENDERUSDT": 25,
    "SHIBUSDT": 26,
    "SOLUSDT": 27,
    "SUIUSDT": 28,
    "TAOUSDT": 29,
    "TONUSDT": 30,
    "TRUMPUSDT": 31,
    "TRXUSDT": 32,
    "UNIUSDT": 33,
    "VETUSDT": 34,
    "WLDUSDT": 35,
    "WLFIUSDT": 36,
    "XLMUSDT": 37,
    "XRPUSDT": 38,
    "ZECUSDT": 39,
}

shutdown_requested = False
shutdown_signal = "Unknown"


def handle_shutdown(sig, frame):
    """Set global shutdown flag on SIGTERM/SIGINT."""
    global shutdown_requested, shutdown_signal
    shutdown_requested = True
    shutdown_signal = {2: "SIGINT", 15: "SIGTERM"}.get(sig, f"Signal {sig}")


class VolatilityPredictionJob:
    def __init__(self):
        self.spark: SparkSession | None = None
        self.pg: Postgres | None = None
        self.model: lgb.Booster | None = None
        self.max_ts: datetime | None = None
        self.records_processed: int = 0
        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)

    def load_model(self) -> bool:
        model_path = Path(MODEL_DIR) / MODEL_FILE
        if not model_path.exists():
            logger.warning(f"Volatility model not found at {model_path}")
            return False
        self.model = lgb.Booster(model_file=str(model_path))
        logger.info(f"Volatility model loaded from {model_path}")
        return True

    def read_trades(self, start_time: datetime) -> DataFrame:
        if not self.spark:
            raise RuntimeError("SparkSession not initialized")
        return (
            self.spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option(
                "dbtable",
                f"""(
                SELECT timestamp, symbol, open, high, low, close, volume,
                       quote_volume, trade_count, buy_count, sell_count
                FROM trades_1m
                WHERE timestamp > '{start_time.isoformat()}'
            ) AS trades""",
            )
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

    def compute_features(self, df: DataFrame) -> DataFrame:
        w_symbol = Window.partitionBy("symbol").orderBy("timestamp")
        w5 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-4, 0)
        w15 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-14, 0)
        w30 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-29, 0)
        w60 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-59, 0)

        features = df.select(
            F.col("timestamp"),
            F.col("symbol"),
            F.col("open").cast(DoubleType()),
            F.col("high").cast(DoubleType()),
            F.col("low").cast(DoubleType()),
            F.col("close").cast(DoubleType()),
            F.col("volume").cast(DoubleType()),
            F.col("quote_volume").cast(DoubleType()),
            F.col("trade_count"),
            F.col("buy_count"),
            F.col("sell_count"),
        )

        for lag, name in [(1, "return_1m"), (5, "return_5m"), (15, "return_15m")]:
            lag_col = f"close_{lag}"
            features = features.withColumn(lag_col, F.lag("close", lag).over(w_symbol)).withColumn(
                name,
                F.when(
                    F.col(lag_col).isNotNull() & (F.col(lag_col) > 0),
                    ((F.col("close") - F.col(lag_col)) / F.col(lag_col)) * 100,
                ).otherwise(0.0),
            )

        features = (
            features.withColumn("volatility_5m", F.coalesce(F.stddev("return_1m").over(w5), F.lit(0.0)))
            .withColumn("volatility_15m", F.coalesce(F.stddev("return_1m").over(w15), F.lit(0.0)))
            .withColumn("volatility_30m", F.coalesce(F.stddev("return_1m").over(w30), F.lit(0.0)))
            .withColumn("volatility_60m", F.coalesce(F.stddev("return_1m").over(w60), F.lit(0.0)))
            .withColumn("volatility_ratio", F.col("volatility_5m") / (F.col("volatility_30m") + 1e-8))
        )

        features = features.withColumn(
            "price_range_pct", ((F.col("high") - F.col("low")) / (F.col("close") + 1e-8)) * 100
        ).withColumn("price_body_pct", (F.abs(F.col("close") - F.col("open")) / (F.col("close") + 1e-8)) * 100)

        features = (
            features.withColumn("avg_volume_60", F.avg("volume").over(w60))
            .withColumn(
                "volume_ratio_60m",
                F.when(F.col("avg_volume_60") > 0, F.col("volume") / F.col("avg_volume_60")).otherwise(1.0),
            )
            .withColumn("avg_volume_15", F.avg("volume").over(w15))
            .withColumn(
                "volume_ratio_15m",
                F.when(F.col("avg_volume_15") > 0, F.col("volume") / F.col("avg_volume_15")).otherwise(1.0),
            )
        )

        features = features.withColumn(
            "buy_ratio",
            F.when(
                F.col("trade_count") > 0,
                F.col("buy_count").cast(DoubleType()) / F.col("trade_count").cast(DoubleType()),
            ).otherwise(0.5),
        ).withColumn(
            "buy_sell_imbalance",
            F.when(
                F.col("trade_count") > 0,
                (2.0 * F.col("buy_count").cast(DoubleType()) - F.col("trade_count").cast(DoubleType()))
                / F.col("trade_count").cast(DoubleType()),
            ).otherwise(0.0),
        )

        features = (
            features.withColumn("price_ma_15", F.avg("close").over(w15))
            .withColumn(
                "price_vs_ma_15m", ((F.col("close") - F.col("price_ma_15")) / (F.col("price_ma_15") + 1e-8)) * 100
            )
            .withColumn("price_ma_60", F.avg("close").over(w60))
            .withColumn(
                "price_vs_ma_60m", ((F.col("close") - F.col("price_ma_60")) / (F.col("price_ma_60") + 1e-8)) * 100
            )
        )

        symbol_map = F.create_map([F.lit(x) for kv in SYMBOL_ENCODING.items() for x in kv])
        features = features.withColumn("hour", F.hour("timestamp").cast(IntegerType())).withColumn(
            "symbol_encoded", F.coalesce(symbol_map[F.col("symbol")], F.lit(99)).cast(IntegerType())
        )

        return features.select(
            "timestamp",
            "symbol",
            "close",
            "volume",
            "quote_volume",
            "trade_count",
            "return_1m",
            "return_5m",
            "return_15m",
            "volatility_5m",
            "volatility_15m",
            "volatility_30m",
            "volatility_60m",
            "volatility_ratio",
            "price_range_pct",
            "price_body_pct",
            "volume_ratio_15m",
            "volume_ratio_60m",
            "buy_ratio",
            "buy_sell_imbalance",
            "price_vs_ma_15m",
            "price_vs_ma_60m",
            "hour",
            "symbol_encoded",
        ).filter(F.col("return_15m").isNotNull())

    def predict_volatility(self, features_df: DataFrame) -> DataFrame:
        if self.model is None:
            logger.warning("Model not loaded, skipping predictions")
            return features_df.withColumn("predicted_volatility_5m", F.lit(None).cast(DoubleType()))

        broadcasted_model = self.spark.sparkContext.broadcast(self.model)
        predict_udf = F.udf(
            lambda *features: float(broadcasted_model.value.predict([[*features]])[0]),
            DoubleType(),
        )
        return features_df.withColumn("predicted_volatility_5m", predict_udf(*[F.col(c) for c in FEATURE_COLUMNS]))

    def write_features(self, df: DataFrame, count: int | None = None) -> int:
        if df.isEmpty():
            logger.info("No features to write")
            return 0

        feature_cols = [
            "timestamp",
            "symbol",
            "close",
            "volume",
            "quote_volume",
            "trade_count",
            "return_1m",
            "return_5m",
            "return_15m",
            "volatility_5m",
            "volatility_15m",
            "volatility_30m",
            "volatility_60m",
            "volatility_ratio",
            "price_range_pct",
            "price_body_pct",
            "volume_ratio_15m",
            "volume_ratio_60m",
            "buy_ratio",
            "buy_sell_imbalance",
            "price_vs_ma_15m",
            "price_vs_ma_60m",
            "hour",
            "symbol_encoded",
        ]

        record_count = count if count is not None else df.count()

        (
            df.select(*feature_cols)
            .write.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "staging_ml_features")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )

        if self.pg:
            self.pg.merge_staging_to_ml_features()

        logger.info(f"Wrote {record_count} feature records to ml_features")
        return record_count

    def write_predictions(self, df: DataFrame, count: int | None = None) -> int:
        if df.isEmpty():
            logger.info("No volatility predictions to write")
            return 0

        predictions_df = df.select(
            F.col("timestamp"),
            F.col("symbol"),
            F.col("volatility_5m").alias("current_volatility"),
            F.col("predicted_volatility_5m"),
        )

        record_count = count if count is not None else predictions_df.count()

        (
            predictions_df.write.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "staging_volatility_predictions")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )

        if self.pg:
            self.pg.merge_staging_to_volatility_predictions()

        logger.info(f"Wrote {record_count} volatility predictions")
        return record_count

    def save_checkpoint(self) -> None:
        if not self.pg or not self.max_ts:
            return
        try:
            self.pg.update_checkpoint(JOB_VOLATILITY, self.max_ts, self.records_processed)
            logger.info(f"Checkpoint saved: {self.max_ts}, records: {self.records_processed}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def run(self) -> None:
        try:
            self.spark = (
                SparkSession.builder.appName("VolatilityPredictionJob")
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

            checkpoint = self.pg.get_checkpoint(JOB_VOLATILITY)
            if checkpoint:
                start_time = checkpoint["last_processed_timestamp"]
                logger.info(f"Resuming from checkpoint: {start_time}")
            else:
                start_time = datetime.now() - timedelta(hours=LOOKBACK_HOUR)
                logger.info(f"No checkpoint, starting from {LOOKBACK_HOUR}h ago: {start_time} (local time)")

            model_loaded = self.load_model()
            if not model_loaded:
                logger.warning("Running without model - will compute features only")

            trades = self.read_trades(start_time)
            trades = trades.cache()
            row_count = trades.count()
            logger.info(f"Read {row_count} rows from trades_1m")

            if row_count == 0:
                logger.warning("No new trades found, skipping feature computation")
                trades.unpersist()
                return

            max_ts_row = trades.agg(F.max("timestamp").alias("max_ts")).collect()[0]
            self.max_ts = max_ts_row["max_ts"]
            self.records_processed = row_count

            features = self.compute_features(trades)
            trades.unpersist()

            features = features.cache()
            feature_count = features.count()
            logger.info(f"Computed {feature_count} feature rows")

            written_features = self.write_features(features, count=feature_count)
            for _ in range(written_features):
                record_message_processed("spark_volatility_prediction", "ml_features", "success")

            if model_loaded:
                predictions = self.predict_volatility(features)
                written_predictions = self.write_predictions(predictions, count=feature_count)

                for _ in range(written_predictions):
                    record_message_processed("spark_volatility_prediction", "volatility_predictions", "success")

                if written_predictions > 0:
                    self.save_checkpoint()

                logger.info(f"Job completed: {written_features} features, {written_predictions} predictions")
            else:
                logger.info(f"Feature-only mode: {written_features} features (no model, checkpoint not updated)")

            features.unpersist()

        except Exception as e:
            record_error("spark_volatility_prediction", "job_failure", "critical")
            logger.error(f"Volatility prediction job failed: {e}")
            raise
        finally:
            if self.pg:
                self.pg.close()
            if self.spark:
                self.spark.stop()


if __name__ == "__main__":
    VolatilityPredictionJob().run()
