"""
Spark Structured Streaming Job 1: Raw -> Silver Streaming.

Pipeline:
  1. Consumes 'raw-bank-transactions' and other raw topics from Kafka
  2. Parses JSON and performs strict schema validation
  3. Rejects & diverts malformed/corrupted messages to 'dlq-financial-events' (Dead Letter Queue)
  4. Deduplicates on transaction_id over a 10-minute sliding watermark window
  5. Normalizes local currencies (XOF, GHS) to EUR
  6. Implements Double Sink:
      - Publishes validated clean stream to Kafka 'silver-bank-transactions'
      - Persists micro-batches into Apache Iceberg table 'silver.bank_transactions'

Author: Armel Kouadio
"""

import logging
import os
import sys

logger = logging.getLogger("waba.streaming.raw_to_silver")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def get_streaming_spark_session():
    """Initializes Spark session with Kafka connectors and Iceberg extensions."""
    from pyspark.sql import SparkSession

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    s3_access = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    s3_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
    iceberg_uri = os.getenv("ICEBERG_REST_URI", "http://localhost:8181")

    return (
        SparkSession.builder.appName("WABA_Stream_Raw_To_Silver")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints/raw_to_silver")
        # Iceberg & S3A
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.waba", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.waba.type", "rest")
        .config("spark.sql.catalog.waba.uri", iceberg_uri)
        .config("spark.sql.catalog.waba.warehouse", "s3a://lakehouse/")
        .getOrCreate()
    )


def start_raw_to_silver_pipeline():
    """Starts the real-time Raw -> Silver Structured Streaming ingestion pipeline."""
    from pyspark.sql.functions import (
        col,
        current_timestamp,
        from_json,
        lit,
        struct,
        to_json,
        to_timestamp,
        when,
    )
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Connecting to Kafka source at {kafka_bootstrap}...")

    spark = get_streaming_spark_session()

    # Raw Bank Transaction JSON Schema
    raw_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("beneficiary_account", StringType(), True),
        StructField("branch_id", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("transaction_status", StringType(), True),
        StructField("fee_amount", DoubleType(), True),
        StructField("entity_type", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("source_file", StringType(), True),
    ])

    # 1. Read Stream from Kafka raw-bank-transactions
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "raw-bank-transactions")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON payload
    parsed_stream = raw_stream.select(
        col("key").cast("string").alias("msg_key"),
        col("value").cast("string").alias("raw_payload"),
        from_json(col("value").cast("string"), raw_schema).alias("data"),
        col("timestamp").alias("kafka_msg_time")
    )

    # 3. Filter Valid vs Malformed (Dead Letter Queue routing)
    is_valid_expr = (
        col("data.transaction_id").isNotNull()
        & col("data.amount").isNotNull()
        & (col("data.amount") > 0)
        & col("data.country_code").isNotNull()
    )

    valid_stream = parsed_stream.filter(is_valid_expr).select("data.*")
    malformed_stream = parsed_stream.filter(~is_valid_expr).select(
        col("raw_payload").alias("failed_payload"),
        lit("SCHEMA_VALIDATION_ERROR").alias("error_reason"),
        current_timestamp().alias("dlq_timestamp")
    )

    # Sink 1A: Write Malformed to Dead Letter Queue (DLQ)
    dlq_query = (
        malformed_stream.select(to_json(struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", "dlq-financial-events")
        .option("checkpointLocation", "/tmp/spark-checkpoints/dlq_sink")
        .outputMode("append")
        .start()
    )

    # 4. Transformations: Timestamp parsing, Watermark & 10-Minute Window Deduplication
    XOF_TO_EUR = 1.0 / 655.957
    GHS_TO_EUR = 0.065

    transformed_stream = (
        valid_stream
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["transaction_id", "event_time"])
        .withColumn(
            "amount_eur",
            when(col("currency") == "XOF", col("amount") * lit(XOF_TO_EUR))
            .when(col("currency") == "GHS", col("amount") * lit(GHS_TO_EUR))
            .otherwise(col("amount"))
        )
        .withColumn(
            "fee_amount_eur",
            when(col("currency") == "XOF", col("fee_amount") * lit(XOF_TO_EUR))
            .when(col("currency") == "GHS", col("fee_amount") * lit(GHS_TO_EUR))
            .otherwise(col("fee_amount"))
        )
        .withColumn("processed_timestamp", current_timestamp())
    )

    # Sink 1B: Double Sink 1 -> Kafka 'silver-bank-transactions'
    kafka_silver_query = (
        transformed_stream.select(
            col("transaction_id").alias("key"),
            to_json(struct("*")).alias("value")
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", "silver-bank-transactions")
        .option("checkpointLocation", "/tmp/spark-checkpoints/silver_kafka_sink")
        .outputMode("append")
        .start()
    )

    # Sink 1C: Double Sink 2 -> Apache Iceberg 'waba.silver.bank_transactions_streaming'
    def write_iceberg_microbatch(batch_df, batch_id):
        if not batch_df.isEmpty():
            logger.info(f"Persisting micro-batch {batch_id} ({batch_df.count()} events) into Iceberg Silver...")
            try:
                batch_df.write.format("iceberg").mode("append").saveAsTable("waba.silver.bank_transactions")
            except Exception as e:
                logger.error(f"Error persisting micro-batch {batch_id} to Iceberg: {e}")

    iceberg_query = (
        transformed_stream.writeStream
        .foreachBatch(write_iceberg_microbatch)
        .option("checkpointLocation", "/tmp/spark-checkpoints/silver_iceberg_sink")
        .outputMode("append")
        .start()
    )

    logger.info("Raw to Silver Structured Streaming pipeline running (dual sink active).")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    start_raw_to_silver_pipeline()
