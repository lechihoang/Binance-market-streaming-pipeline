import os
import signal
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict

import lightgbm as lgb
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType

from storage.postgres import Postgres
from util.shutdown import GracefulShutdown
from util.metrics import record_error, record_message_processed
from util.logging import get_logger

logger = get_logger(__name__)

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "crypto")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "crypto")
PG_DB = os.getenv("POSTGRES_DB", "crypto_data")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

LOOKBACK_HOURS = int(os.getenv("FEATURE_LOOKBACK_HOURS", "2"))
MODEL_DIR = os.getenv("MODEL_DIR", "models")
MODEL_FILE = "volatility_predictor.json"

JOB_NAME = "volatility_prediction"

FEATURE_COLUMNS = [
    "return_5m",
    "return_15m",
    "volatility_5m",
    "volatility_15m",
    "volatility_30m",
    "volatility_60m",
    "volatility_ratio",
    "candle_range",
    "volume_ratio_60m",
    "buy_ratio",
    "buy_sell_imbalance",
    "price_vs_ma_15m",
    "price_vs_ma_60m",
    "hour",
    "symbol_encoded",
]

SYMBOL_ENCODING: Dict[str, int] = {
    "BTCUSDT": 0, "ETHUSDT": 1, "BNBUSDT": 2, "SOLUSDT": 3, "XRPUSDT": 4,
    "DOGEUSDT": 5, "ADAUSDT": 6, "AVAXUSDT": 7, "SHIBUSDT": 8, "DOTUSDT": 9,
    "LINKUSDT": 10, "TRXUSDT": 11, "MATICUSDT": 12, "UNIUSDT": 13,
    "ATOMUSDT": 14, "LTCUSDT": 15, "ETCUSDT": 16, "XLMUSDT": 17,
    "NEARUSDT": 18, "APTUSDT": 19, "SUIUSDT": 20,
}


class VolatilityPredictionJob:

    def __init__(self):
        self.shutdown = GracefulShutdown(graceful_shutdown_timeout=15)
        self.spark: Optional[SparkSession] = None
        self.pg: Optional[Postgres] = None
        self.model: Optional[lgb.Booster] = None
        self._max_ts: Optional[datetime] = None
        self._records_processed: int = 0
        signal.signal(signal.SIGTERM, lambda sig, _: self.shutdown.request_shutdown(sig))
        signal.signal(signal.SIGINT, lambda sig, _: self.shutdown.request_shutdown(sig))

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
        
        start_str = start_time.isoformat()
        
        return (self.spark.read
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", f"""(
                SELECT timestamp, symbol, open, high, low, close, volume, 
                       quote_volume, trade_count
                FROM trades_1m
                WHERE timestamp > '{start_str}'
            ) AS trades""")
            .option("user", PG_USER)
            .option("password", PG_PASS)
            .option("driver", "org.postgresql.Driver")
            .load())

    def compute_features(self, df: DataFrame) -> DataFrame:
        w_symbol = Window.partitionBy("symbol").orderBy("timestamp")
        w_5 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-4, 0)
        w_15 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-14, 0)
        w_30 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-29, 0)
        w_60 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-59, 0)

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
        )

        features = features.withColumn(
            "close_1", F.lag("close", 1).over(w_symbol)
        ).withColumn(
            "return_1m",
            F.when(F.col("close_1").isNotNull() & (F.col("close_1") > 0),
                 ((F.col("close") - F.col("close_1")) / F.col("close_1")) * 100)
                .otherwise(0.0)
        )

        features = features.withColumn(
            "close_5", F.lag("close", 5).over(w_symbol)
        ).withColumn(
            "return_5m",
            F.when(F.col("close_5").isNotNull() & (F.col("close_5") > 0),
                 ((F.col("close") - F.col("close_5")) / F.col("close_5")) * 100)
                .otherwise(0.0)
        )

        features = features.withColumn(
            "close_15", F.lag("close", 15).over(w_symbol)
        ).withColumn(
            "return_15m",
            F.when(F.col("close_15").isNotNull() & (F.col("close_15") > 0),
                 ((F.col("close") - F.col("close_15")) / F.col("close_15")) * 100)
                .otherwise(0.0)
        )

        features = features.withColumn(
            "volatility_5m",
            F.coalesce(F.stddev("return_1m").over(w_5), F.lit(0.0))
        ).withColumn(
            "volatility_15m",
            F.coalesce(F.stddev("return_1m").over(w_15), F.lit(0.0))
        ).withColumn(
            "volatility_30m",
            F.coalesce(F.stddev("return_1m").over(w_30), F.lit(0.0))
        ).withColumn(
            "volatility_60m",
            F.coalesce(F.stddev("return_1m").over(w_60), F.lit(0.0))
        )

        features = features.withColumn(
            "volatility_ratio",
            F.col("volatility_5m") / (F.col("volatility_30m") + 1e-8)
        )

        features = features.withColumn(
            "candle_range",
            ((F.col("high") - F.col("low")) / (F.col("close") + 1e-8)) * 100
        )

        features = features.withColumn(
            "avg_volume_60", F.avg("volume").over(w_60)
        ).withColumn(
            "volume_ratio_60m",
            F.when(F.col("avg_volume_60") > 0, F.col("volume") / F.col("avg_volume_60"))
                .otherwise(1.0)
        )

        features = features.withColumn(
            "buy_ratio", F.lit(0.5)
        ).withColumn(
            "buy_sell_imbalance", F.lit(0.0)
        )

        features = features.withColumn(
            "price_ma_15", F.avg("close").over(w_15)
        ).withColumn(
            "price_vs_ma_15m",
            ((F.col("close") - F.col("price_ma_15")) / (F.col("price_ma_15") + 1e-8)) * 100
        ).withColumn(
            "price_ma_60", F.avg("close").over(w_60)
        ).withColumn(
            "price_vs_ma_60m",
            ((F.col("close") - F.col("price_ma_60")) / (F.col("price_ma_60") + 1e-8)) * 100
        )

        features = features.withColumn(
            "hour", F.hour("timestamp").cast(IntegerType())
        )

        symbol_map = F.create_map([F.lit(x) for kv in SYMBOL_ENCODING.items() for x in kv])
        features = features.withColumn(
            "symbol_encoded",
            F.coalesce(symbol_map[F.col("symbol")], F.lit(99)).cast(IntegerType())
        )

        return features.select(
            "timestamp", "symbol", "close", "volume", "quote_volume", "trade_count",
            "return_5m", "return_15m",
            "volatility_5m", "volatility_15m", "volatility_30m", "volatility_60m",
            "volatility_ratio", "candle_range", "volume_ratio_60m",
            "buy_ratio", "buy_sell_imbalance",
            "price_vs_ma_15m", "price_vs_ma_60m",
            "hour", "symbol_encoded",
        ).filter(
            F.col("return_15m").isNotNull()
        )

    def predict_volatility(self, features_df: DataFrame) -> DataFrame:
        """Predict volatility using broadcast model + UDF."""
        if self.model is None:
            logger.warning("Model not loaded, skipping predictions")
            return features_df.withColumn("predicted_volatility_5m", F.lit(None).cast(DoubleType()))

        # Broadcast model once to all executors
        broadcasted_model = self.spark.sparkContext.broadcast(self.model)
        feature_cols = FEATURE_COLUMNS

        # UDF that uses broadcasted model - predicts row by row
        predict_udf = F.udf(
            lambda *features: float(
                broadcasted_model.value.predict([[*features]])[0]
            ),
            DoubleType(),
        )

        return features_df.withColumn(
            "predicted_volatility_5m",
            predict_udf(*[F.col(c) for c in feature_cols])
        )

    def write_features(self, df: DataFrame, count: Optional[int] = None) -> int:
        """Write ML features to PostgreSQL using Spark JDBC (distributed)."""
        if df.isEmpty():
            logger.info("No features to write")
            return 0

        feature_cols = [
            "timestamp", "symbol", "close", "volume", "quote_volume", "trade_count",
            "return_5m", "return_15m",
            "volatility_5m", "volatility_15m", "volatility_30m", "volatility_60m",
            "volatility_ratio", "candle_range", "volume_ratio_60m",
            "buy_ratio", "buy_sell_imbalance",
            "price_vs_ma_15m", "price_vs_ma_60m",
            "hour", "symbol_encoded",
        ]

        # Use provided count if available (from cached df), otherwise count
        record_count = count if count is not None else df.count()
        
        df.select(*feature_cols).write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "ml_features") \
            .option("user", PG_USER) \
            .option("password", PG_PASS) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        logger.info(f"Wrote {record_count} feature records to ml_features via Spark JDBC")
        return record_count

    def write_volatility_predictions(self, df: DataFrame, count: Optional[int] = None) -> int:
        """Write volatility predictions to PostgreSQL using Spark JDBC (distributed)."""
        if df.isEmpty():
            logger.info("No volatility predictions to write")
            return 0

        # Select and rename columns to match table schema
        # Table schema: timestamp, symbol, current_volatility, predicted_volatility_5m
        predictions_df = df.select(
            F.col("timestamp"),
            F.col("symbol"),
            F.col("volatility_5m").alias("current_volatility"),
            F.col("predicted_volatility_5m"),
        )
        
        # Use provided count if available (from cached df), otherwise count
        record_count = count if count is not None else predictions_df.count()
        
        predictions_df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "volatility_predictions") \
            .option("user", PG_USER) \
            .option("password", PG_PASS) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        logger.info(f"Wrote {record_count} volatility prediction records via Spark JDBC")
        return record_count

    def _save_checkpoint(self) -> None:
        """Save checkpoint once before shutdown."""
        if self.pg and self._max_ts:
            try:
                self.pg.update_checkpoint(JOB_NAME, self._max_ts, self._records_processed)
                logger.info(f"Checkpoint saved: {self._max_ts}, records: {self._records_processed}")
            except Exception as e:
                logger.error(f"Failed to save checkpoint: {e}")

    def run(self) -> None:
        try:
            self.spark = (SparkSession.builder
                .appName("VolatilityPredictionJob")
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.pyspark.python", "/usr/local/bin/python3.11")
                .config("spark.pyspark.driver.python", "/usr/local/bin/python3.11")
                .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/local/bin/python3.11")
                .getOrCreate())

            self.pg = Postgres(
                host=PG_HOST, port=PG_PORT,
                user=PG_USER, password=PG_PASS, database=PG_DB
            )

            checkpoint = self.pg.get_checkpoint(JOB_NAME)
            if checkpoint:
                start_time = checkpoint["last_processed_timestamp"]
                logger.info(f"Resuming from checkpoint: {start_time}")
            else:
                start_time = datetime.now() - timedelta(hours=LOOKBACK_HOURS)
                logger.info(f"No checkpoint, starting from {LOOKBACK_HOURS} hours ago: {start_time}")

            model_loaded = self.load_model()
            if not model_loaded:
                logger.warning("Running without model - will compute features only")

            trades = self.read_trades(start_time)
            
            # Cache trades since we use it multiple times
            trades = trades.cache()
            row_count = trades.count()
            logger.info(f"Read {row_count} rows from trades_1m")
            
            if row_count == 0:
                logger.warning("No new trades found, skipping feature computation")
                trades.unpersist()
                return

            # Track max timestamp for checkpoint
            max_ts_row = trades.agg(F.max("timestamp").alias("max_ts")).collect()[0]
            self._max_ts = max_ts_row["max_ts"]
            self._records_processed = row_count

            features = self.compute_features(trades)
            trades.unpersist()  # Done with trades, free memory
            
            # Cache features since we use it multiple times (write_features, predict, write_predictions)
            features = features.cache()
            feature_count = features.count()  # Single count, triggers cache
            logger.info(f"Computed {feature_count} feature rows")

            written_features = self.write_features(features, count=feature_count)
            
            for _ in range(written_features):
                record_message_processed("spark_volatility_prediction", "ml_features", "success")

            if model_loaded:
                predictions = self.predict_volatility(features)
                written_predictions = self.write_volatility_predictions(predictions, count=feature_count)
                
                for _ in range(written_predictions):
                    record_message_processed("spark_volatility_prediction", "volatility_predictions", "success")
                
                if written_predictions > 0:
                    self._save_checkpoint()
                
                logger.info(f"Volatility prediction job completed: {written_features} features, {written_predictions} predictions")
            else:
                logger.info(f"Feature computation completed: {written_features} features (no model, checkpoint not updated)")
            
            # Unpersist features after done
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
