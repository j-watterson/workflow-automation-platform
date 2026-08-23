"""Airflow callbacks with structured, provider-neutral alert payloads."""

from __future__ import annotations

import json
import logging
from typing import Any

LOGGER = logging.getLogger(__name__)


def build_failure_payload(context: dict[str, Any]) -> dict[str, Any]:
    task_instance = context.get("task_instance")
    return {
        "event": "airflow_task_failed",
        "dag_id": getattr(task_instance, "dag_id", "unknown"),
        "task_id": getattr(task_instance, "task_id", "unknown"),
        "run_id": context.get("run_id", "unknown"),
        "try_number": getattr(task_instance, "try_number", None),
        "log_url": getattr(task_instance, "log_url", None),
        "exception": str(context.get("exception", "unknown error")),
    }


def notify_failure(context: dict[str, Any]) -> None:
    """Log a structured alert; production can forward it to Slack/PagerDuty."""
    LOGGER.error("workflow_alert=%s", json.dumps(build_failure_payload(context)))


def notify_success(context: dict[str, Any]) -> None:
    LOGGER.info(
        "workflow_completed dag_id=%s run_id=%s",
        context.get("dag").dag_id if context.get("dag") else "unknown",
        context.get("run_id", "unknown"),
    )

