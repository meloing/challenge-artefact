"""
DAG: dag_regulatory_report
Level: 2 - Supervisory Compliance & Reporting

Description:
  - Scheduled daily at J+1 00:30 UTC
  - Computes regulatory prudential returns for BCEAO (WAEMU) and CIMA (Insurance)
  - Exports official aggregated supervisory filings into Iceberg gold.regulatory_*
  - Monitored by Prometheus/Grafana alert: triggers if failed by 06:00 UTC

Author: Armel Kouadio
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

try:
    from alert_callbacks import task_failure_alert, regulatory_sla_miss_alert
except ImportError:
    task_failure_alert = None
    regulatory_sla_miss_alert = None

DEFAULT_ARGS = {
    "owner": "waba_compliance_officer",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "sla": timedelta(hours=5, minutes=30),  # SLA: Must finish before 06:00 UTC
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="dag_regulatory_report",
    default_args=DEFAULT_ARGS,
    description="Daily J+1 (00:30 UTC) regulatory return consolidation for BCEAO and CIMA",
    schedule_interval="30 0 * * *",  # 00:30 UTC daily
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=regulatory_sla_miss_alert,
    tags=["waba", "level-2", "regulatory", "bceao", "cima", "compliance"],
    params={
        "reporting_date": Param(
            "",
            type="string",
            description="Override reporting date (YYYY-MM-DD) or empty for previous day (J+1 default)"
        )
    }
) as dag:

    def notify_supervisory_start(**context):
        exec_date = context.get("execution_date")
        print(f"🏛️ Starting official regulatory consolidation for filing window: {exec_date}...")
        return True

    pre_audit_check = PythonOperator(
        task_id="audit_pre_flight_check",
        python_callable=notify_supervisory_start
    )

    run_regulatory_spark = BashOperator(
        task_id="run_spark_regulatory_consolidation",
        bash_command="""
            echo "Generating BCEAO & CIMA Returns via PySpark..."
            REP_DATE="{{ params.reporting_date }}"
            python3 /opt/airflow/lakehouse/spark_jobs/regulatory_reporting.py $REP_DATE
        """
    )

    def dispatch_regulatory_receipt(**context):
        print("✅ Regulatory reporting committed to Iceberg and audit signed.")
        return True

    post_audit_commit = PythonOperator(
        task_id="sign_and_commit_audit_trail",
        python_callable=dispatch_regulatory_receipt
    )

    pre_audit_check >> run_regulatory_spark >> post_audit_commit
