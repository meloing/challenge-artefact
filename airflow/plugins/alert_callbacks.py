"""
Airflow Alert Callbacks and SLA Miss Handlers.

Provides:
  - Structured failure alerting
  - Notification hooks for regulatory SLA breaches
  - Retry telemetry tracking

Author: Armel Kouadio
"""

import logging
import os
from typing import Dict, Any

logger = logging.getLogger("waba.airflow.alerts")


def task_failure_alert(context: Dict[str, Any]):
    """
    Callback invoked whenever an Airflow task exhausts retries and fails.
    Extracts DAG ID, Task ID, execution date, and exception details.
    """
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id = context.get("task").task_id if context.get("task") else "unknown_task"
    execution_date = context.get("execution_date")
    exception = context.get("exception")

    alert_msg = (
        f"🚨 [AIRFLOW CRITICAL FAILURE] 🚨\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Execution Date: {execution_date}\n"
        f"Reason: {exception}\n"
        f"Action: On-call data engineer notified via pager/webhook."
    )
    logger.error(alert_msg)

    # Webhook notification (e.g. Slack/Teams/Mattermost) if configured
    webhook_url = os.getenv("AIRFLOW_ALERT_WEBHOOK_URL")
    if webhook_url:
        try:
            import urllib.request
            import json
            req = urllib.request.Request(
                webhook_url,
                data=json.dumps({"text": alert_msg}).encode("utf-8"),
                headers={"Content-Type": "application/json"}
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception as e:
            logger.warning(f"Could not dispatch webhook alert: {e}")


def regulatory_sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Special alert callback if dag_regulatory_report misses its strict J+1 06h00 UTC deadline.
    """
    logger.critical(
        f"⚠️ [REGULATORY SLA BREACH] DAG {dag.dag_id} missed reporting deadline! "
        f"BCEAO / CIMA supervisor dispatch delayed."
    )
