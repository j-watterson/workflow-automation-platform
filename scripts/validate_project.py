#!/usr/bin/env python3
"""Static checks for environments where Airflow is not installed."""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    errors: list[str] = []
    dag_path = ROOT / "dags/northwind_daily_warehouse.py"
    source = dag_path.read_text(encoding="utf-8")
    try:
        ast.parse(source, filename=str(dag_path))
    except SyntaxError as error:
        errors.append(f"DAG syntax error: {error}")

    required_dag_controls = [
        "CronDataIntervalTimetable",
        "catchup=False",
        "max_active_runs=1",
        "retry_exponential_backoff",
        "execution_timeout",
        "on_failure_callback",
        "validate_raw_delivery",
        "check_source_freshness",
        "build_staging",
        "build_core",
        "build_customer_360",
        "build_daily_sales",
        "validate_warehouse",
    ]
    for control in required_dag_controls:
        if control not in source:
            errors.append(f"DAG is missing required control: {control}")

    config_path = ROOT / "config/pipeline.json"
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
        if config["dag_id"] not in source:
            errors.append("Configured DAG id is not represented by the DAG")
        if len(config["expected_sources"]) < 3:
            errors.append("Expected at least three upstream sources")
    except (OSError, KeyError, json.JSONDecodeError) as error:
        errors.append(f"Invalid workflow configuration: {error}")

    required_files = [
        "Dockerfile",
        "compose.yaml",
        "README.md",
        "docs/architecture.md",
        "docs/runbook.md",
        "docs/backfill.md",
        ".github/workflows/ci.yml",
    ]
    for filename in required_files:
        if not (ROOT / filename).is_file():
            errors.append(f"Missing required file: {filename}")

    if errors:
        print("Project validation failed:")
        for error in errors:
            print(f"  - {error}")
        return 1
    print(
        f"Project validation passed: {len(required_dag_controls)} DAG controls, "
        f"{len(config['expected_sources'])} upstream sources"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

