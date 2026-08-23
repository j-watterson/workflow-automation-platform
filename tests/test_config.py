import json
from pathlib import Path
import tempfile
import unittest

from northwind_workflows.config import WorkflowConfig


class WorkflowConfigTests(unittest.TestCase):
    def test_project_configuration_loads(self) -> None:
        path = Path(__file__).resolve().parents[1] / "config/pipeline.json"
        config = WorkflowConfig.from_file(path)
        self.assertEqual(config.dag_id, "northwind_daily_warehouse_refresh")
        self.assertEqual(config.schedule, "0 6 * * *")
        self.assertEqual(config.expected_sources[-1], "raw_orders")

    def test_empty_sources_are_rejected(self) -> None:
        values = {
            "dag_id": "test",
            "schedule": "@daily",
            "timezone": "UTC",
            "raw_manifest_path": "/tmp/manifest.json",
            "dbt_project_dir": "/tmp/dbt",
            "dbt_profiles_dir": "/tmp/dbt",
            "expected_sources": [],
            "max_source_age_hours": 24,
            "retries": 2,
            "retry_delay_minutes": 5,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(values), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "cannot be empty"):
                WorkflowConfig.from_file(path)


if __name__ == "__main__":
    unittest.main()

