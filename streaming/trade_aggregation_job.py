"""Trade Aggregation Job."""

import time

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, count, sum as spark_sum, avg, min as spark_min, 
    max as spark_max, first, last, stddev, when, from_json, to_json, 
    struct, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, BooleanType, TimestampType
)

from streaming.base_spark_job import BaseSparkJob

# Import metrics utilities for production monitoring
from utils.metrics import record_error, record_message_processed


class TradeAggregationJob(BaseSparkJob):
    def __init__(self):
        super().__init__(job_name="TradeAggregationJob")

    def create_stream_reader(self) -> DataFrame:
        """Create Kafka stream reader for raw_trades topic."""
        self.logger.info(f"Creating stream reader for topic: {self.kafka_topic_raw_trades}")
        self.logger.info(f"Kafka bootstrap servers: {self.kafka_bootstrap_servers}")
        
        try:
            # Use "earliest" so checkpoint can track progress
            # When job restarts, it continues from last checkpoint offset (not from beginning)
            # This ensures no data is lost when job is temporarily stopped
            df = (self.spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                  .option("subscribe", self.kafka_topic_raw_trades)
                  .option("startingOffsets", "earliest")
                  .option("maxOffsetsPerTrigger", 
                         str(self.kafka_max_rate_per_partition * 10))
                  .load())
            
            self.logger.info("Stream reader created successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to create stream reader: {str(e)}", 
                            extra={"topic": self.kafka_topic_raw_trades,
                                   "error": str(e)})
            raise
    
    def parse_trades(self, df: DataFrame) -> DataFrame:
        """Parse JSON trade messages and extract fields."""
        # Schema for flattened message format from connector:
        # Binance fields at top-level + metadata object
        # Note: spark.sql.caseSensitive=true is enabled to distinguish e/E and t/T fields
        metadata_schema = StructType([
            StructField("ingestion_timestamp", LongType(), True),
            StructField("stream_type", StringType(), True),
            StructField("topic", StringType(), True),
        ])
        
        trade_schema = StructType([
            StructField("e", StringType(), True),   # Event type ("trade")
            StructField("E", LongType(), True),     # Event time (milliseconds)
            StructField("s", StringType(), True),   # Symbol
            StructField("t", LongType(), True),     # Trade ID
            StructField("p", StringType(), True),   # Price
            StructField("q", StringType(), True),   # Quantity
            StructField("T", LongType(), True),     # Trade time (milliseconds)
            StructField("m", BooleanType(), True),  # Is buyer maker
            StructField("metadata", metadata_schema, True),
        ])
        
        self.logger.info("Parsing trade messages with flattened schema")
        
        try:
            # Parse JSON from Kafka value
            parsed_df = df.select(
                from_json(col("value").cast("string"), trade_schema).alias("trade"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            )
            
            # Extract fields directly from top-level (no original_data wrapper)
            # Use getField() to avoid case-insensitive ambiguity between 'E' and 'e'
            extracted_df = parsed_df.select(
                (col("trade").getField("E") / 1000).cast(TimestampType()).alias("event_time"),
                col("trade").getField("s").alias("symbol"),
                col("trade").getField("p").cast(DoubleType()).alias("price"),
                col("trade").getField("q").cast(DoubleType()).alias("quantity"),
                col("trade").getField("m").alias("is_buyer_maker"),
                col("trade").getField("t").alias("trade_id"),
                col("topic"),
                col("partition"),
                col("offset")
            ).withWatermark("event_time", "1 minute")
            
            self.logger.info("Trade parsing configured with 1 minute watermark")
            return extracted_df
            
        except Exception as e:
            self.logger.error(f"Failed to parse trades: {str(e)}", 
                            extra={"error": str(e)})
            raise

    def aggregate_trades(self, df: DataFrame) -> DataFrame:
        return (df
            .groupBy(
                window(col("event_time"), "1 minute").alias("window"),
                col("symbol")
            )
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
                spark_sum(when(col("is_buyer_maker") == False, 1).otherwise(0)).alias("buy_count"),
                spark_sum(when(col("is_buyer_maker") == True, 1).otherwise(0)).alias("sell_count")
            )
            .select(
                col("window.start").alias("timestamp"),
                col("window.end").alias("window_end"),
                lit("1m").alias("interval"),
                col("symbol"),
                col("open"),
                col("high"),
                col("low"),
                col("close"),
                col("volume"),
                col("quote_volume"),
                col("trade_count"),
                col("average_price"),
                col("price_volatility"),
                col("buy_count"),
                col("sell_count")
            )
            .withColumn("volume_weighted_avg_price", col("quote_volume") / col("volume"))
            .withColumn("price_change_percent", ((col("close") - col("open")) / col("open")) * 100)
            .withColumn("buy_sell_ratio", when(col("sell_count") > 0, col("buy_count") / col("sell_count")).otherwise(lit(None)))
        )

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

    def write_to_sinks(self, batch_df: DataFrame, batch_id: int) -> None:
        start_time = time.time()
        
        if self._should_abort_batch(batch_df, batch_id):
            return
        
        self._log_memory_metrics(batch_id=batch_id)
        records = batch_df.collect()
        
        if self.graceful_shutdown.shutdown_requested:
            raise InterruptedError("Shutdown requested after collect")
        
        # Write to Kafka
        try:
            kafka_df = batch_df.select(
                col("symbol").cast("string").alias("key"),
                to_json(struct(*[col(c) for c in batch_df.columns])).alias("value")
            )
            kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("topic", self.kafka_topic_processed_aggregations) \
                .option("kafka.enable.idempotence", str(self.kafka_enable_idempotence).lower()) \
                .option("kafka.acks", self.kafka_acks) \
                .option("kafka.max.in.flight.requests.per.connection", str(self.kafka_max_in_flight_requests)) \
                .save()
        except Exception as e:
            self.logger.error(f"Batch {batch_id}: Kafka write failed: {e}")
        
        aggregation_records = [row.asDict() for row in records]
        batch_result = self.storage_writer.write_aggregations_batch(aggregation_records)
        
        for tier, succeeded in batch_result.tier_results.items():
            if not succeeded:
                self.logger.warning(f"Batch {batch_id}: {tier} tier write failed")
        
        for _ in range(batch_result.success_count):
            record_message_processed(service="spark_trade_aggregation", topic="processed_aggregations", status="success")
        
        duration = time.time() - start_time
        self.logger.info(f"Batch {batch_id}: {len(records)} records in {duration:.2f}s")

    def run(self) -> None:
        try:
            self.spark = self._create_spark_session()
            self.storage_writer = self._init_storage_writer()
            
            raw_stream = self.create_stream_reader()
            trades_df = self.parse_trades(raw_stream)
            enriched_df = self.aggregate_trades(trades_df)
            
            self.start_time = time.time()
            self.logger.info(
                f"TradeAggregationJob started: topic={self.kafka_topic_raw_trades}, "
                f"max_runtime={self.max_runtime_seconds}s"
            )
            
            query = (enriched_df
                    .writeStream
                    .foreachBatch(self.write_to_sinks)
                    .outputMode("update")
                    .trigger(processingTime='60 seconds')
                    .option("checkpointLocation", self.spark_checkpoint_location)
                    .start())
            
            self.query = query
            query.awaitTermination(timeout=self.max_runtime_seconds)
            
            self.logger.info("TradeAggregationJob completed")
            
        except Exception as e:
            record_error(service="spark_trade_aggregation", error_type="job_failure", severity="critical")
            self.logger.error(f"Job failed: {e}", exc_info=True)
            raise
        finally:
            self._cleanup()


def main():
    job = TradeAggregationJob()
    job.run()


if __name__ == "__main__":
    main()
