import unittest

from northwind_workflows.callbacks import build_failure_payload


class FakeTaskInstance:
    dag_id = "daily_refresh"
    task_id = "build_core"
    try_number = 3
    log_url = "https://airflow.example/log"


class CallbackTests(unittest.TestCase):
    def test_failure_payload_contains_operational_context(self) -> None:
        payload = build_failure_payload({
            "task_instance": FakeTaskInstance(),
            "run_id": "scheduled__2026-07-30",
            "exception": RuntimeError("warehouse unavailable"),
        })
        self.assertEqual(payload["event"], "airflow_task_failed")
        self.assertEqual(payload["task_id"], "build_core")
        self.assertEqual(payload["try_number"], 3)
        self.assertIn("warehouse unavailable", payload["exception"])


if __name__ == "__main__":
    unittest.main()

