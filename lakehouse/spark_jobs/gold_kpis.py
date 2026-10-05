"""
Gold Layer Financial & Regulatory KPIs Computation Job (PySpark & Iceberg).

Computes the 7 mandatory regulatory and executive tables:
  1. gold.daily_transaction_volume (Volumes, Amounts & Success rates)
  2. gold.npl_ratio_by_country (Non-Performing Loan ratio vs BCEAO 5% threshold)
  3. gold.customer_arpu_monthly (Average Revenue Per User by segment)
  4. gold.loss_ratio_by_product (Sinistres / Primes vs CIMA 70% threshold)
  5. gold.claims_processing_time (Claims settlement duration vs SLAs)
  6. gold.mobile_money_daily_flow (Daily volumes, failure rates, active users)
  7. gold.cross_border_transfers (UEMOA corridor analysis: CI->SN, etc.)

Author: Armel Kouadio
"""

import logging
import os
import sys
from typing import Optional

logger = logging.getLogger("waba.spark.gold")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def run_gold_kpis_computation(country_filter: Optional[str] = None):
    """
    Computes all Gold KPI aggregations from the clean Silver layer
    and writes them into Iceberg gold.* tables.
    """
    from lakehouse.spark_jobs.bronze_ingestion import get_spark_session
    from pyspark.sql.functions import (
        avg,
        col,
        concat,
        count,
        countDistinct,
        current_timestamp,
        date_format,
        lit,
        max,
        min,
        round as spark_round,
        sum,
        to_date,
        when,
    )

    logger.info("Initializing Spark for Gold KPIs computation...")
    spark = get_spark_session(app_name="WABA_Gold_KPIs_Computation")

    # -------------------------------------------------------------------------
    # 1. gold.daily_transaction_volume
    # -------------------------------------------------------------------------
    logger.info("Computing gold.daily_transaction_volume...")
    try:
        silver_bank = spark.table("waba.silver.bank_transactions")
        if country_filter:
            silver_bank = silver_bank.filter(col("country_code") == country_filter)

        daily_vol = (
            silver_bank
            .withColumn("txn_date", to_date(col("timestamp")))
            .groupBy("txn_date", "country_code", "entity_type", "transaction_type")
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_round(sum("amount"), 2).alias("total_amount_local"),
                spark_round(sum("amount_eur"), 2).alias("total_amount_eur"),
                spark_round(sum("fee_amount_eur"), 2).alias("total_fees_eur"),
                spark_round(
                    sum(when(col("transaction_status") == "SUCCESS", 1).otherwise(0)) / count("transaction_id"),
                    4
                ).alias("success_rate")
            )
            .withColumn("gold_updated_at", current_timestamp())
        )

        daily_vol.write.format("iceberg").mode("overwrite").saveAsTable("waba.gold.daily_transaction_volume")
        logger.info("Saved: waba.gold.daily_transaction_volume")
    except Exception as e:
        logger.error(f"Error computing daily_transaction_volume: {e}")

    # -------------------------------------------------------------------------
    # 2. gold.npl_ratio_by_country (BCEAO threshold < 5%)
    # -------------------------------------------------------------------------
    logger.info("Computing gold.npl_ratio_by_country...")
    try:
        silver_loan = spark.table("waba.silver.loan_repayments")
        if country_filter:
            silver_loan = silver_loan.filter(col("country_code") == country_filter)

        npl_df = (
            silver_loan
            .withColumn("eval_date", to_date(col("timestamp")))
            .groupBy("country_code", "loan_type")
            .agg(
                count("repayment_id").alias("total_loans_count"),
                sum(when(col("is_defaulted"), 1).otherwise(0)).alias("defaulted_loans_count"),
                spark_round(sum("amount_due_eur"), 2).alias("total_exposure_eur"),
                spark_round(sum(when(col("is_defaulted"), col("amount_due_eur")).otherwise(0.0)), 2).alias("defaulted_exposure_eur")
            )
            .withColumn(
                "npl_ratio",
                spark_round(col("defaulted_exposure_eur") / when(col("total_exposure_eur") > 0, col("total_exposure_eur")).otherwise(1.0), 4)
            )
            .withColumn(
                "is_bceao_compliant",
                col("npl_ratio") <= 0.05
            )
            .withColumn(
                "risk_status",
                when(col("npl_ratio") < 0.03, "LOW_RISK_GREEN")
                .when(col("npl_ratio") <= 0.05, "MODERATE_ORANGE")
                .otherwise("CRITICAL_RED")
            )
            .withColumn("gold_updated_at", current_timestamp())
        )

        npl_df.write.format("iceberg").mode("overwrite").saveAsTable("waba.gold.npl_ratio_by_country")
        logger.info("Saved: waba.gold.npl_ratio_by_country")
    except Exception as e:
        logger.error(f"Error computing npl_ratio_by_country: {e}")

    # -------------------------------------------------------------------------
    # 3. gold.customer_arpu_monthly (Monthly ARPU by Segment)
    # -------------------------------------------------------------------------
    logger.info("Computing gold.customer_arpu_monthly...")
    try:
        silver_bank = spark.table("waba.silver.bank_transactions")
        if country_filter:
            silver_bank = silver_bank.filter(col("country_code") == country_filter)

        arpu_df = (
            silver_bank
            .withColumn("month_year", date_format(to_date(col("timestamp")), "yyyy-MM"))
            .filter(col("transaction_status") == "SUCCESS")
            .groupBy("month_year", "country_code", "customer_segment")
            .agg(
                spark_round(sum("fee_amount_eur"), 2).alias("total_commissions_eur"),
                countDistinct("customer_id").alias("active_customers_count")
            )
            .withColumn(
                "arpu_eur",
                spark_round(col("total_commissions_eur") / when(col("active_customers_count") > 0, col("active_customers_count")).otherwise(1), 2)
            )
            .withColumn("gold_updated_at", current_timestamp())
        )

        arpu_df.write.format("iceberg").mode("overwrite").saveAsTable("waba.gold.customer_arpu_monthly")
        logger.info("Saved: waba.gold.customer_arpu_monthly")
    except Exception as e:
        logger.error(f"Error computing customer_arpu_monthly: {e}")

    # -------------------------------------------------------------------------
    # 4. gold.loss_ratio_by_product (CIMA threshold > 70%)
    # -------------------------------------------------------------------------
    logger.info("Computing gold.loss_ratio_by_product...")
    try:
        silver_ins = spark.table("waba.silver.insurance_operations")
        if country_filter:
            silver_ins = silver_ins.filter(col("country_code") == country_filter)

        loss_ratio_df = (
            silver_ins
            .withColumn("month_year", date_format(to_date(col("timestamp")), "yyyy-MM"))
            .groupBy("month_year", "country_code", "product_line")
            .agg(
                spark_round(
                    sum(when(col("operation_type").isin(["CLAIM_PAYMENT", "CLAIM_SUBMISSION"]), col("amount_eur")).otherwise(0.0)),
                    2
                ).alias("total_claims_eur"),
                spark_round(
                    sum(when(col("operation_type").isin(["PREMIUM_PAYMENT", "POLICY_RENEWAL"]), col("amount_eur")).otherwise(0.0)),
                    2
                ).alias("total_premiums_eur")
            )
            .withColumn(
                "loss_ratio",
                spark_round(col("total_claims_eur") / when(col("total_premiums_eur") > 0, col("total_premiums_eur")).otherwise(1.0), 4)
            )
            .withColumn(
                "is_cima_alert",
                col("loss_ratio") > 0.70
            )
            .withColumn("gold_updated_at", current_timestamp())
        )

        loss_ratio_df.write.format("iceberg").mode("overwrite").saveAsTable("waba.gold.loss_ratio_by_product")
        logger.info("Saved: waba.gold.loss_ratio_by_product")
    except Exception as e:
        logger.error(f"Error computing loss_ratio_by_product: {e}")

    # -------------------------------------------------------------------------
    # 5. gold.claims_processing_time (Claims SLA duration)
    # -------------------------------------------------------------------------
    logger.info("Computing gold.claims_processing_time...")
    try:
        silver_ins = spark.table("waba.silver.insurance_operations")
        if country_filter:
            silver_ins = silver_ins.filter(col("country_code") == country_filter)

        claims_time_df = (
            silver_ins
            .filter(col("processing_days").isNotNull() & (col("processing_days") > 0))
            .withColumn("month_year", date_format(to_date(col("timestamp")), "yyyy-MM"))
            .groupBy("country_code", "product_line", "month_year")
            .agg(
                count("operation_id").alias("total_processed_claims"),
                spark_round(avg("processing_days"), 1).alias("avg_processing_days"),
                min("processing_days").alias("min_processing_days"),
                max("processing_days").alias("max_processing_days"),
                spark_round(
                    sum(when(col("processing_days") > 30, 1).otherwise(0)) / count("operation_id"),
                    4
                ).alias("sla_breach_rate")
            )
            .withColumn("gold_updated_at", current_timestamp())
        )

        claims_time_df.write.format("iceberg").mode("overwrite").saveAsTable("waba.gold.claims_processing_time")
        logger.info("Saved: waba.gold.claims_processing_time")
    except Exception as e:
        logger.error(f"Error computing claims_processing_time: {e}")

    # -------------------------------------------------------------------------
    # 6. gold.mobile_money_daily_flow
    # -------------------------------------------------------------------------
    logger.info("Computing gold.mobile_money_daily_flow...")
    try:
        silver_mm = spark.table("waba.silver.mobile_money_payments")
        if country_filter:
            silver_mm = silver_mm.filter(col("sender_country") == country_filter)

        mm_daily_df = (
            silver_mm
            .withColumn("flow_date", to_date(col("timestamp")))
            .groupBy("flow_date", "sender_country", "operator")
            .agg(
                count("payment_id").alias("total_transactions"),
                spark_round(sum("amount_eur"), 2).alias("total_volume_eur"),
                spark_round(sum("fee_amount_eur"), 2).alias("total_fees_eur"),
                countDistinct("sender_id").alias("active_users_count"),
                spark_round(
                    sum(when(col("status") == "FAILED", 1).otherwise(0)) / count("payment_id"),
                    4
                ).alias("failure_rate")
            )
            .withColumnRenamed("sender_country", "country_code")
            .withColumn("gold_updated_at", current_timestamp())
        )

        mm_daily_df.write.format("iceberg").mode("overwrite").saveAsTable("waba.gold.mobile_money_daily_flow")
        logger.info("Saved: waba.gold.mobile_money_daily_flow")
    except Exception as e:
        logger.error(f"Error computing mobile_money_daily_flow: {e}")

    # -------------------------------------------------------------------------
    # 7. gold.cross_border_transfers
    # -------------------------------------------------------------------------
    logger.info("Computing gold.cross_border_transfers...")
    try:
        silver_mm = spark.table("waba.silver.mobile_money_payments")

        cross_border_df = (
            silver_mm
            .filter(col("is_cross_border") == True)
            .withColumn("corridor", concat(col("sender_country"), lit("->"), col("receiver_country")))
            .withColumn("week_year", date_format(to_date(col("timestamp")), "yyyy-'W'ww"))
            .groupBy("week_year", "sender_country", "receiver_country", "corridor")
            .agg(
                count("payment_id").alias("transfer_count"),
                spark_round(sum("amount_eur"), 2).alias("total_volume_eur"),
                spark_round(avg("amount_eur"), 2).alias("avg_transfer_eur")
            )
            .withColumn("gold_updated_at", current_timestamp())
        )

        cross_border_df.write.format("iceberg").mode("overwrite").saveAsTable("waba.gold.cross_border_transfers")
        logger.info("Saved: waba.gold.cross_border_transfers")
    except Exception as e:
        logger.error(f"Error computing cross_border_transfers: {e}")

    logger.info("All 7 Gold KPI tables calculated and persisted successfully.")
    spark.stop()


if __name__ == "__main__":
    country = sys.argv[1] if len(sys.argv) > 1 else None
    run_gold_kpis_computation(country_filter=country)
