"""Daily orchestration for the Northwind Outfitters analytics warehouse."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup, dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import timezone

DAGS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = DAGS_DIR.parent
SRC_DIR = PROJECT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from northwind_workflows.callbacks import notify_failure, notify_success
from northwind_workflows.config import WorkflowConfig
from northwind_workflows.manifest import validate_manifest

CONFIG_PATH = Path(
    os.environ.get("NORTHWIND_WORKFLOW_CONFIG", PROJECT_DIR / "config/pipeline.json")
)
CONFIG = WorkflowConfig.from_file(CONFIG_PATH)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": CONFIG.retries,
    "retry_delay": timedelta(minutes=CONFIG.retry_delay_minutes),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": notify_failure,
}

DBT_ENV = {
    "DBT_PROJECT_DIR": CONFIG.dbt_project_dir,
    "DBT_PROFILES_DIR": CONFIG.dbt_profiles_dir,
    "DBT_TARGET": "{{ var.value.get('dbt_target', 'dev') }}",
}


@dag(
    dag_id=CONFIG.dag_id,
    description="Validate raw deliveries and refresh governed BigQuery BI marts.",
    schedule=CronDataIntervalTimetable(
        cron=CONFIG.schedule,
        timezone=timezone(CONFIG.timezone),
    ),
    start_date=datetime(2026, 1, 1, tzinfo=timezone(CONFIG.timezone)),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    dagrun_timeout=timedelta(hours=2),
    default_args=DEFAULT_ARGS,
    on_success_callback=notify_success,
    tags=["northwind", "warehouse", "daily", "production"],
)
def northwind_daily_warehouse_refresh():
    @task(task_id="validate_raw_delivery")
    def validate_raw_delivery() -> dict[str, object]:
        result = validate_manifest(
            Path(CONFIG.raw_manifest_path),
            CONFIG.expected_sources,
            max_age_hours=CONFIG.max_source_age_hours,
        )
        print(json.dumps(result, sort_keys=True))
        return result

    @task(task_id="publish_run_summary", trigger_rule="all_success")
    def publish_run_summary(manifest_summary: dict[str, object]) -> None:
        print(json.dumps({
            "event": "warehouse_refresh_completed",
            "dag_id": CONFIG.dag_id,
            **manifest_summary,
        }, sort_keys=True))

    raw_ready = validate_raw_delivery()

    install_dbt_packages = BashOperator(
        task_id="install_dbt_packages",
        bash_command=(
            "dbt deps --project-dir \"$DBT_PROJECT_DIR\" "
            "--profiles-dir \"$DBT_PROFILES_DIR\""
        ),
        env=DBT_ENV,
        append_env=True,
    )

    source_freshness = BashOperator(
        task_id="check_source_freshness",
        bash_command=(
            "dbt source freshness --project-dir \"$DBT_PROJECT_DIR\" "
            "--profiles-dir \"$DBT_PROFILES_DIR\" --target \"$DBT_TARGET\""
        ),
        env=DBT_ENV,
        append_env=True,
    )

    with TaskGroup(group_id="transform_warehouse") as transform_warehouse:
        staging = BashOperator(
            task_id="build_staging",
            bash_command=(
                "dbt build --select tag:staging --project-dir \"$DBT_PROJECT_DIR\" "
                "--profiles-dir \"$DBT_PROFILES_DIR\" --target \"$DBT_TARGET\""
            ),
            env=DBT_ENV,
            append_env=True,
        )
        core = BashOperator(
            task_id="build_core",
            bash_command=(
                "dbt build --select path:models/marts/core "
                "--project-dir \"$DBT_PROJECT_DIR\" "
                "--profiles-dir \"$DBT_PROFILES_DIR\" --target \"$DBT_TARGET\""
            ),
            env=DBT_ENV,
            append_env=True,
        )
        staging >> core

    with TaskGroup(group_id="refresh_bi_marts") as refresh_bi_marts:
        marketing = BashOperator(
            task_id="build_customer_360",
            bash_command=(
                "dbt build --select path:models/marts/marketing "
                "--project-dir \"$DBT_PROJECT_DIR\" "
                "--profiles-dir \"$DBT_PROFILES_DIR\" --target \"$DBT_TARGET\""
            ),
            env=DBT_ENV,
            append_env=True,
        )
        finance = BashOperator(
            task_id="build_daily_sales",
            bash_command=(
                "dbt build --select path:models/marts/finance "
                "--project-dir \"$DBT_PROJECT_DIR\" "
                "--profiles-dir \"$DBT_PROFILES_DIR\" --target \"$DBT_TARGET\""
            ),
            env=DBT_ENV,
            append_env=True,
        )

    validate_marts = BashOperator(
        task_id="validate_warehouse",
        bash_command=(
            "dbt test --select tag:marts --project-dir \"$DBT_PROJECT_DIR\" "
            "--profiles-dir \"$DBT_PROFILES_DIR\" --target \"$DBT_TARGET\""
        ),
        env=DBT_ENV,
        append_env=True,
    )

    summary = publish_run_summary(raw_ready)
    (
        raw_ready
        >> install_dbt_packages
        >> source_freshness
        >> transform_warehouse
        >> refresh_bi_marts
        >> validate_marts
        >> summary
    )


northwind_daily_warehouse_refresh_dag = northwind_daily_warehouse_refresh()
