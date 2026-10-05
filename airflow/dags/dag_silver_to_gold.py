"""
DAG: dag_silver_to_gold
Level: 2 - Medallion Architecture & Business KPIs

Description:
  - Triggered after dag_bronze_to_silver commits clean Silver datasets
  - Computes business intelligence aggregates and supervisory indicators
  - Persists all 7 Gold KPI tables into Iceberg gold.*
  - Validates prudential threshold ceilings (BCEAO NPL < 5%, CIMA Loss Ratio < 70%)

Author: Armel Kouadio
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
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
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="dag_silver_to_gold",
    default_args=DEFAULT_ARGS,
    description="Computes consolidated executive and regulatory Gold KPIs from Silver layer",
    schedule_interval=None,  # Triggered by dag_bronze_to_silver
    catchup=False,
    max_active_runs=1,
    tags=["waba", "level-2", "gold", "kpis", "lakehouse"],
    params={
        "country_code": Param(
            "ALL",
            type="string",
            description="Target country ISO code or ALL"
        )
    }
) as dag:

    run_spark_gold_kpis = BashOperator(
        task_id="run_spark_gold_kpis_computation",
        bash_command="""
            echo "Submitting PySpark Gold KPIs Computation Job..."
            TARGET_CC="{{ params.country_code }}"
            if [ "$TARGET_CC" = "ALL" ]; then
                TARGET_CC=""
            fi
            python3 /opt/airflow/lakehouse/spark_jobs/gold_kpis.py $TARGET_CC
        """
    )

    run_spark_gold_kpis
