"""Anomaly Detection Job."""

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    current_timestamp,
    expr,
    from_json,
    lit,
    struct,
    to_json,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.base_spark_job import BaseSparkJob

# Import metrics utilities for production monitoring
from utils.metrics import (
    record_error,
    record_message_processed,
    track_latency,
)

# Import KafkaConnector from shared module
from utils.kafka import KafkaConnector


class AnomalyDetectionJob(BaseSparkJob):
    WHALE_THRESHOLD = 100000.0
    VOLUME_SPIKE_THRESHOLD = 1000000.0
    PRICE_SPIKE_THRESHOLD = 2.0

    def __init__(self):
        super().__init__(job_name="AnomalyDetectionJob")
        
        # Override default graceful shutdown timeout to 90s
        # to allow checkpoint completion before forced termination
        self.graceful_shutdown.graceful_shutdown_timeout = 90

    @staticmethod
    def _get_trade_schema() -> StructType:
        metadata_schema = StructType([
            StructField("ingestion_timestamp", LongType(), True),
            StructField("stream_type", StringType(), True),
            StructField("topic", StringType(), True),
        ])
        return StructType([
            StructField("E", LongType(), False),
            StructField("s", StringType(), False),
            StructField("p", StringType(), False),
            StructField("q", StringType(), False),
            StructField("m", BooleanType(), False),
            StructField("t", LongType(), True),
            StructField("metadata", metadata_schema, True),
        ])

    @staticmethod
    def _get_aggregation_schema() -> StructType:
        return StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("interval", StringType(), False),
            StructField("symbol", StringType(), False),
            StructField("open", DoubleType(), False),
            StructField("high", DoubleType(), False),
            StructField("low", DoubleType(), False),
            StructField("close", DoubleType(), False),
            StructField("volume", DoubleType(), False),
            StructField("quote_volume", DoubleType(), False),
            StructField("trade_count", LongType(), False),
            StructField("volume_weighted_avg_price", DoubleType(), True),
            StructField("price_change_percent", DoubleType(), True),
            StructField("buy_sell_ratio", DoubleType(), True),
            StructField("average_price", DoubleType(), True),
            StructField("price_volatility", DoubleType(), True),
            StructField("buy_count", LongType(), True),
            StructField("sell_count", LongType(), True),
        ])

    def detect_whale_alerts(self, df: DataFrame) -> DataFrame:
        self.logger.info(f"Detecting whale alerts with threshold: ${self.WHALE_THRESHOLD:,.2f}")
        trade_schema = self._get_trade_schema()
        parsed_df = df.select(from_json(col("value").cast("string"), trade_schema).alias("trade"))
        trades_df = parsed_df.select(
            (col("trade.E") / 1000).cast(TimestampType()).alias("timestamp"),
            col("trade.s").alias("symbol"),
            col("trade.p").cast(DoubleType()).alias("price"),
            col("trade.q").cast(DoubleType()).alias("quantity"),
            when(col("trade.m") == True, lit("SELL")).otherwise(lit("BUY")).alias("side"),
        ).withColumn("value", col("price") * col("quantity"))
        trades_df = trades_df.withWatermark("timestamp", "1 minute")
        whale_trades = trades_df.filter(col("value") > self.WHALE_THRESHOLD)
        alerts_df = whale_trades.select(
            col("timestamp"), col("symbol"),
            lit("WHALE_ALERT").alias("alert_type"), lit("HIGH").alias("alert_level"),
            to_json(struct(col("price"), col("quantity"), col("value"), col("side"))).alias("details"),
            expr("uuid()").alias("alert_id"), current_timestamp().alias("created_at")
        )
        self.logger.info("Whale alert detection configured")
        return alerts_df

    def detect_volume_spikes(self, df: DataFrame) -> DataFrame:
        self.logger.info(f"Detecting volume spikes with quote_volume threshold: ${self.VOLUME_SPIKE_THRESHOLD:,.0f}")
        agg_schema = self._get_aggregation_schema()
        parsed_df = df.select(from_json(col("value").cast("string"), agg_schema).alias("agg"))
        aggs_df = parsed_df.select(
            col("agg.timestamp").alias("timestamp"), col("agg.window_end").alias("window_end"),
            col("agg.interval").alias("interval"), col("agg.symbol").alias("symbol"),
            col("agg.volume").alias("volume"), col("agg.quote_volume").alias("quote_volume"),
            col("agg.trade_count").alias("trade_count")
        ).filter(col("interval") == "1m")
        aggs_df = aggs_df.withWatermark("timestamp", "1 minute")
        volume_spikes = aggs_df.filter(col("quote_volume") > self.VOLUME_SPIKE_THRESHOLD)
        alerts_df = volume_spikes.select(
            col("timestamp"), col("symbol"),
            lit("VOLUME_SPIKE").alias("alert_type"), lit("MEDIUM").alias("alert_level"),
            to_json(struct(col("volume"), col("quote_volume"), col("trade_count"))).alias("details"),
            expr("uuid()").alias("alert_id"), current_timestamp().alias("created_at")
        )
        self.logger.info("Volume spike detection configured")
        return alerts_df

    def detect_price_spikes(self, df: DataFrame) -> DataFrame:
        self.logger.info(f"Detecting price spikes with threshold: {self.PRICE_SPIKE_THRESHOLD}%")
        agg_schema = self._get_aggregation_schema()
        parsed_df = df.select(from_json(col("value").cast("string"), agg_schema).alias("agg"))
        aggs_df = parsed_df.select(
            col("agg.timestamp").alias("timestamp"), col("agg.window_end").alias("window_end"),
            col("agg.interval").alias("interval"), col("agg.symbol").alias("symbol"),
            col("agg.open").alias("open"), col("agg.close").alias("close"),
            col("agg.price_change_percent").alias("price_change_percent")
        ).filter(col("interval") == "1m")
        aggs_df = aggs_df.withWatermark("timestamp", "1 minute")
        price_spikes = aggs_df.filter(spark_abs(col("price_change_percent")) > self.PRICE_SPIKE_THRESHOLD)
        alerts_df = price_spikes.select(
            col("timestamp"), col("symbol"),
            lit("PRICE_SPIKE").alias("alert_type"), lit("HIGH").alias("alert_level"),
            to_json(struct(col("open"), col("close"), col("price_change_percent"))).alias("details"),
            expr("uuid()").alias("alert_id"), current_timestamp().alias("created_at")
        )
        self.logger.info("Price spike detection configured")
        return alerts_df

    def _row_to_alert(self, row) -> Dict[str, Any]:
        details_dict = json.loads(row.details) if row.details else {}
        return {
            "alert_id": row.alert_id,
            "timestamp": row.timestamp,
            "symbol": row.symbol,
            "alert_type": row.alert_type,
            "alert_level": row.alert_level,
            "details": details_dict,
            "created_at": datetime.now(timezone.utc)
        }

    def _write_alerts_to_sinks(self, alerts: List[Dict[str, Any]], batch_id: int) -> None:
        if not alerts:
            return
        
        try:
            kafka_conn = KafkaConnector(bootstrap_servers=self.kafka_bootstrap_servers, client_id="anomaly_detection_job")
            for alert in alerts:
                kafka_conn.send(topic=self.kafka_topic_alerts, value=alert, key=alert["symbol"])
            kafka_conn.close()
        except Exception as e:
            self.logger.error(f"Batch {batch_id}: Kafka write failed: {e}")

        alert_records = [{
            'alert_id': a.get('alert_id'), 'timestamp': a.get('timestamp'),
            'symbol': a.get('symbol'), 'alert_type': a.get('alert_type'),
            'alert_level': a.get('alert_level'), 'created_at': a.get('created_at'),
            'details': a.get('details', '{}'),
        } for a in alerts]
        
        batch_result = self.storage_writer.write_alerts_batch(alert_records)
        for tier, succeeded in batch_result.tier_results.items():
            if not succeeded:
                self.logger.warning(f"Batch {batch_id}: {tier} tier write failed")

    def _should_abort_batch(self, batch_df: DataFrame, batch_id: int) -> bool:
        if self.graceful_shutdown.shutdown_requested:
            self.logger.info(f"Batch {batch_id}: Aborting due to shutdown")
            raise InterruptedError("Shutdown requested")
        
        is_empty = batch_df.isEmpty()
        if self.should_stop(is_empty):
            if self.query:
                self.query.stop()
            return True
        return is_empty

    def _process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        try:
            with track_latency("spark_anomaly_detection", "batch_processing"):
                if self._should_abort_batch(batch_df, batch_id):
                    return
                
                self._log_memory_metrics(batch_id=batch_id)
                start_time = time.time()
                records = batch_df.collect()
                
                if self.graceful_shutdown.shutdown_requested:
                    raise InterruptedError("Shutdown requested after collect")
                
                alerts = [self._row_to_alert(row) for row in records]
                for _ in alerts:
                    record_message_processed(service="spark_anomaly_detection", topic="alerts", status="success")
                
                self._write_alerts_to_sinks(alerts, batch_id)
                self._log_batch_metrics(batch_id, time.time() - start_time, len(records), 
                                       str(records[0].timestamp) if records else None)
        except InterruptedError:
            raise
        except Exception as e:
            record_error(service="spark_anomaly_detection", error_type="batch_processing_error", severity="error")
            self.logger.error(f"Batch {batch_id}: Error: {e}", exc_info=True)

    def _create_stream_reader(self, topic: str) -> DataFrame:
        self.logger.info(f"Creating stream reader for topic: {topic}")
        try:
            df = (self.spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                  .option("subscribe", topic)
                  .option("startingOffsets", "earliest")
                  .option("maxOffsetsPerTrigger", str(self.kafka_max_rate_per_partition * 10))
                  .load())
            self.logger.info(f"Stream reader created for topic: {topic}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to create stream reader for {topic}: {str(e)}")
            raise

    def run(self) -> None:
        try:
            self.spark = self._create_spark_session()
            self.storage_writer = self._init_storage_writer()

            self.logger.info(
                f"AnomalyDetectionJob started: whale>${self.WHALE_THRESHOLD:,.0f}, "
                f"volume>${self.VOLUME_SPIKE_THRESHOLD:,.0f}, price>{self.PRICE_SPIKE_THRESHOLD}%"
            )

            raw_trades_stream = self._create_stream_reader(self.kafka_topic_raw_trades)
            aggregations_stream = self._create_stream_reader(self.kafka_topic_processed_aggregations)

            all_alerts = (self.detect_whale_alerts(raw_trades_stream)
                         .union(self.detect_volume_spikes(aggregations_stream))
                         .union(self.detect_price_spikes(aggregations_stream)))

            self.start_time = time.time()

            query = (all_alerts.writeStream.foreachBatch(self._process_batch).outputMode("append")
                    .trigger(processingTime='60 seconds')
                    .option("checkpointLocation", self.spark_checkpoint_location).start())
            self.query = query
            query.awaitTermination(timeout=self.max_runtime_seconds)

            self.logger.info("AnomalyDetectionJob completed")
        except Exception as e:
            record_error(service="spark_anomaly_detection", error_type="job_failure", severity="critical")
            self.logger.error(f"Job failed: {e}", exc_info=True)
            raise
        finally:
            self._cleanup()


def run_anomaly_detection_job():
    job = AnomalyDetectionJob()
    job.run()


if __name__ == "__main__":
    run_anomaly_detection_job()
