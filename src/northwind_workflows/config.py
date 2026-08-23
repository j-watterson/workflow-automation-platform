"""Validated workflow configuration."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WorkflowConfig:
    dag_id: str
    schedule: str
    timezone: str
    raw_manifest_path: str
    dbt_project_dir: str
    dbt_profiles_dir: str
    expected_sources: tuple[str, ...]
    max_source_age_hours: int
    retries: int
    retry_delay_minutes: int

    @classmethod
    def from_file(cls, path: Path) -> "WorkflowConfig":
        with path.open(encoding="utf-8") as handle:
            raw = json.load(handle)
        required = {
            "dag_id", "schedule", "timezone", "raw_manifest_path",
            "dbt_project_dir", "dbt_profiles_dir", "expected_sources",
            "max_source_age_hours", "retries", "retry_delay_minutes",
        }
        missing = required - raw.keys()
        if missing:
            raise ValueError(f"Missing configuration keys: {sorted(missing)}")
        if not raw["expected_sources"]:
            raise ValueError("expected_sources cannot be empty")
        if raw["retries"] < 0 or raw["retry_delay_minutes"] <= 0:
            raise ValueError("Retry configuration is invalid")
        return cls(
            dag_id=raw["dag_id"],
            schedule=raw["schedule"],
            timezone=raw["timezone"],
            raw_manifest_path=raw["raw_manifest_path"],
            dbt_project_dir=raw["dbt_project_dir"],
            dbt_profiles_dir=raw["dbt_profiles_dir"],
            expected_sources=tuple(raw["expected_sources"]),
            max_source_age_hours=raw["max_source_age_hours"],
            retries=raw["retries"],
            retry_delay_minutes=raw["retry_delay_minutes"],
        )

