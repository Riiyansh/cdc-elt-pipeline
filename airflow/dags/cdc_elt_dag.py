"""
CDC → ELT orchestration DAG.

Production-hygiene features this DAG demonstrates:
  - Idempotent tasks      : land_raw uses ON CONFLICT; dbt incremental is re-runnable.
                            Re-running the whole DAG for the same interval is safe.
  - Retries + backoff     : transient failures retried with exponential backoff.
  - Data-quality gate     : `dbt test` is a hard gate — if DQ checks fail, the run
                            fails and downstream is blocked (no bad data served).
  - Failure alerting      : on_failure_callback routes to Slack (or logs if no webhook).
  - Clear lineage         : land → run → snapshot → DQ gate → publish.
"""

from __future__ import annotations

import os
import json
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt"
DUCKDB_PATH = f"{PROJECT_DIR}/warehouse.duckdb"
DBT_ENV = f"DUCKDB_PATH={DUCKDB_PATH}"


def alert_on_failure(context):
    """Route task failures to Slack if configured, else log loudly."""
    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    msg = (
        f":rotating_light: *Pipeline failure*\n"
        f"DAG: `{dag_id}`\nTask: `{ti.task_id}`\n"
        f"Run: `{context.get('run_id')}`\n"
        f"Log: {ti.log_url}"
    )
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            req = urllib.request.Request(
                webhook,
                data=json.dumps({"text": msg}).encode(),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:  # never let alerting crash the callback
            print(f"[alert] Slack post failed: {e}\n{msg}")
    else:
        print(f"[alert] (no SLACK_WEBHOOK_URL set)\n{msg}")


default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="cdc_elt_pipeline",
    description="CDC → DuckDB → dbt ELT with data-quality gating",
    default_args=default_args,
    schedule="0 * * * *",          # hourly
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,             # serialize runs → no concurrent writes to DuckDB
    tags=["elt", "dbt", "cdc", "data-quality"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Land raw CDC events (idempotent — ON CONFLICT DO NOTHING)
    land_raw = BashOperator(
        task_id="land_raw_events",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python ingestion/load_raw.py --db {DUCKDB_PATH} --events-dir raw_events"
        ),
    )

    # 2. Install dbt package deps
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir .",
    )

    # 3. Build models: staging → intermediate → marts (incremental)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && {DBT_ENV} dbt run --profiles-dir .",
    )

    # 4. Capture SCD2 history
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && {DBT_ENV} dbt snapshot --profiles-dir .",
    )

    # 5. DATA-QUALITY GATE — no retries; failure must block publish + alert
    dbt_test = BashOperator(
        task_id="dbt_test_quality_gate",
        bash_command=f"cd {DBT_DIR} && {DBT_ENV} dbt test --profiles-dir .",
        retries=0,
    )

    publish = EmptyOperator(task_id="publish_marts")

    start >> land_raw >> dbt_deps >> dbt_run >> dbt_snapshot >> dbt_test >> publish
