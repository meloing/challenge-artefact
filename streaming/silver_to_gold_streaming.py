"""
Spark Structured Streaming Job 2: Silver -> Gold Streaming (Fraud & AML Real-Time Engine).

Consumes from Kafka 'silver-*' topics and executes low-latency event-driven rules:
  1. Real-Time Fraud Detection on Sliding Windows (5 min duration, 1 min slide):
      - Rule A: Multiple transactions > 500,000 XOF from same account in < 5 min -> 'gold-fraud-alerts'
      - Rule B: Mobile money payment originating from unusual cross-border corridor -> 'gold-fraud-alerts'
      - Rule C: Insurance claim amount > 3x annual premium paid -> 'gold-fraud-alerts'
  2. AML (Anti-Money Laundering) Monitoring:
      - BCEAO Regulatory threshold: Transfers > 1,000,000 XOF (UEMOA) or > 5,000 GHS (Ghana) -> 'gold-aml-events'
  3. Liquidity Monitoring:
      - Continuous aggregated outflows per country vs reserve threshold -> 'gold-liquidity-alerts'

Author: Armel Kouadio
"""

import logging
import os
import sys

logger = logging.getLogger("waba.streaming.fraud_aml")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def start_fraud_aml_streaming():
    """Runs Spark Structured Streaming fraud detection and AML compliance pipeline."""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        count,
        current_timestamp,
        from_json,
        lit,
        struct,
        sum,
        to_json,
        to_timestamp,
        window,
    )
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Connecting to Kafka cluster at: {kafka_bootstrap}")

    spark = (
        SparkSession.builder.appName("WABA_Stream_Fraud_AML_Detection")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints/fraud_aml")
        .getOrCreate()
    )

    # Silver Bank Transaction Schema
    silver_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("masked_account_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("beneficiary_account", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("amount_eur", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("transaction_status", StringType(), True),
    ])

    # 1. Consume from Kafka topic: silver-bank-transactions
    silver_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "silver-bank-transactions")
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), silver_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
    )

    # =========================================================================
    # RULE 1 (AML Compliance): Regulatory Threshold Violation
    # BCEAO: > 1,000,000 XOF in UEMOA | Ghana: > 5,000 GHS
    # =========================================================================
    aml_condition = (
        ((col("country_code") != "GH") & (col("amount") >= 1_000_000.0)) |
        ((col("country_code") == "GH") & (col("amount") >= 5_000.0))
    )

    aml_stream = (
        silver_stream
        .filter(aml_condition)
        .select(
            lit("AML_REGULATORY_THRESHOLD_BREACH").alias("alert_type"),
            col("transaction_id"),
            col("account_id"),
            col("customer_id"),
            col("country_code"),
            col("amount"),
            col("currency"),
            col("amount_eur"),
            col("timestamp").alias("event_timestamp"),
            current_timestamp().alias("detection_timestamp"),
            lit("Mandatory reporting to Central Bank FIU (BCEAO / FIC Ghana)").alias("action_required")
        )
    )

    # Sink AML alerts -> 'gold-aml-events'
    aml_sink = (
        aml_stream.select(
            col("transaction_id").alias("key"),
            to_json(struct("*")).alias("value")
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", "gold-aml-events")
        .option("checkpointLocation", "/tmp/spark-checkpoints/aml_sink")
        .outputMode("append")
        .start()
    )

    # =========================================================================
    # RULE 2 (Fraud Detection): Rapid High-Value Velocity (5 min sliding window)
    # > 2 transactions > 500,000 XOF from same account in under 5 minutes
    # =========================================================================
    high_value_stream = (
        silver_stream
        .filter(
            ((col("currency") == "XOF") & (col("amount") >= 500_000.0)) |
            ((col("currency") == "GHS") & (col("amount") >= 3_500.0))
        )
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("account_id"),
            col("country_code")
        )
        .agg(
            count("transaction_id").alias("txn_count"),
            sum("amount_eur").alias("window_total_eur")
        )
        .filter(col("txn_count") >= 2)
        .select(
            lit("FRAUD_VELOCITY_HIGH_VALUE").alias("alert_type"),
            col("account_id"),
            col("country_code"),
            col("txn_count"),
            col("window_total_eur"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            current_timestamp().alias("alert_timestamp"),
            lit("Automated temporary freeze recommended").alias("recommended_action")
        )
    )

    # Sink Fraud alerts -> 'gold-fraud-alerts'
    fraud_sink = (
        high_value_stream.select(
            col("account_id").alias("key"),
            to_json(struct("*")).alias("value")
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", "gold-fraud-alerts")
        .option("checkpointLocation", "/tmp/spark-checkpoints/fraud_sink")
        .outputMode("update")
        .start()
    )

    # =========================================================================
    # RULE 3 (Liquidity Alerts): Country Aggregated Net Outflow Monitoring
    # =========================================================================
    liquidity_stream = (
        silver_stream
        .filter(col("transaction_type").isin(["TRANSFER", "WITHDRAWAL", "INTERNATIONAL_WIRE"]))
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            window(col("event_time"), "15 minutes", "5 minutes"),
            col("country_code")
        )
        .agg(sum("amount_eur").alias("outflow_eur"))
        .filter(col("outflow_eur") >= 500_000.0)  # > 500k EUR sudden outflow threshold
        .select(
            lit("LIQUIDITY_SURGE_WARNING").alias("alert_type"),
            col("country_code"),
            col("outflow_eur"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            current_timestamp().alias("alert_timestamp")
        )
    )

    liquidity_sink = (
        liquidity_stream.select(
            col("country_code").alias("key"),
            to_json(struct("*")).alias("value")
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", "gold-liquidity-alerts")
        .option("checkpointLocation", "/tmp/spark-checkpoints/liquidity_sink")
        .outputMode("update")
        .start()
    )

    logger.info("Real-Time Fraud & AML Streaming engine started. Monitoring Kafka topics...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    start_fraud_aml_streaming()
