"""
DAG: dag_bronze_to_silver
Level: 2 - Medallion Architecture

Description:
  - Triggered upon completion of dag_ingest_raw
  - Executes cleaning, deduplication, reference entity enrichment, and EUR conversions
  - Pseudonymizes PII customer identifiers
  - Persists into Iceberg silver.* tables
  - Triggers downstream 'dag_silver_to_gold'

Author: Armel Kouadio
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param

try:
    from alert_callbacks import task_failure_alert
except ImportError:
    task_failure_alert = None

DEFAULT_ARGS = {
    "owner": "waba_data_platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="dag_bronze_to_silver",
    default_args=DEFAULT_ARGS,
    description="Transforms raw Bronze data into deduplicated, enriched, and normalized Silver tables",
    schedule_interval=None,  # Triggered by dag_ingest_raw
    catchup=False,
    max_active_runs=1,
    tags=["waba", "level-2", "silver", "medallion", "lakehouse"],
    params={
        "country_code": Param(
            "ALL",
            type="string",
            description="Target country ISO code or ALL"
        )
    }
) as dag:

    run_spark_silver = BashOperator(
        task_id="run_spark_silver_transformation",
        bash_command="""
            echo "Submitting PySpark Silver Transformation Job..."
            TARGET_CC="{{ params.country_code }}"
            if [ "$TARGET_CC" = "ALL" ]; then
                TARGET_CC=""
            fi
            python3 /opt/airflow/lakehouse/spark_jobs/silver_transformation.py $TARGET_CC
        """
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_dag_silver_to_gold",
        trigger_dag_id="dag_silver_to_gold",
        conf={"country_code": "{{ params.country_code }}"},
        wait_for_completion=False
    )

    run_spark_silver >> trigger_gold
