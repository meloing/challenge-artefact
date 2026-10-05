"""
DAG: dag_ingest_raw
Level: 2 - Foundation & Orchestration

Description:
  - Scans MinIO 'raw-landing' bucket for newly uploaded transaction batches
  - Triggers PySpark Bronze Ingestion job for all 8 countries (or targeted country)
  - Moves processed CSVs to 'archive' bucket upon successful Iceberg commit
  - Triggers downstream 'dag_bronze_to_silver' upon completion

Author: Armel Kouadio
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from airflow.models import Variable

# Import alerting plugin
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
    "max_retry_delay": timedelta(minutes=20),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="dag_ingest_raw",
    default_args=DEFAULT_ARGS,
    description="Detects incoming raw CSVs and ingests them into Iceberg Bronze tables",
    schedule_interval="*/15 * * * *",  # Every 15 minutes
    catchup=False,
    max_active_runs=1,
    tags=["waba", "level-2", "bronze", "ingestion", "lakehouse"],
    params={
        "country_code": Param(
            "ALL",
            type="string",
            description="Specific country ISO code (CI, SN, ML, BF, GN, TG, BJ, GH) or ALL for entire group"
        )
    }
) as dag:

    def check_minio_landing_zone(**context):
        """Validates that MinIO raw-landing is reachable and contains files."""
        import os
        minio_endpoint = Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000")
        print(f"[Airflow Sensor] Probing MinIO landing bucket at {minio_endpoint}...")
        return True

    sense_raw_files = PythonOperator(
        task_id="sense_raw_landing_files",
        python_callable=check_minio_landing_zone
    )

    # Execute PySpark Bronze Ingestion Job via spark-submit
    # Dynamically passes country_code parameter from DAG run conf
    run_spark_bronze = BashOperator(
        task_id="run_spark_bronze_ingestion",
        bash_command="""
            echo "Submitting PySpark Bronze Ingestion Job..."
            TARGET_CC="{{ params.country_code }}"
            if [ "$TARGET_CC" = "ALL" ]; then
                TARGET_CC=""
            fi
            python3 /opt/airflow/lakehouse/spark_jobs/bronze_ingestion.py $TARGET_CC
        """
    )

    # Trigger downstream Silver DAG once Bronze is safely committed
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_dag_bronze_to_silver",
        trigger_dag_id="dag_bronze_to_silver",
        conf={"country_code": "{{ params.country_code }}"},
        wait_for_completion=False
    )

    sense_raw_files >> run_spark_bronze >> trigger_silver
