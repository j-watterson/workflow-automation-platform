import json
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from northwind_workflows.manifest import ManifestError, validate_manifest


NOW = datetime(2026, 7, 30, 6, 0, tzinfo=timezone.utc)
SOURCES = ("raw_customers", "raw_products", "raw_orders")


class ManifestTests(unittest.TestCase):
    def write_manifest(self, values: dict) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "manifest.json"
        path.write_text(json.dumps(values), encoding="utf-8")
        return path

    def valid_manifest(self) -> dict:
        return {
            "generated_at": "2026-07-30T05:30:00+00:00",
            "business_date": "2026-07-29",
            "sources": {
                name: {"row_count": index + 1, "status": "ready"}
                for index, name in enumerate(SOURCES)
            },
        }

    def test_valid_delivery_returns_summary(self) -> None:
        summary = validate_manifest(
            self.write_manifest(self.valid_manifest()), SOURCES, now=NOW
        )
        self.assertEqual(summary["source_count"], 3)
        self.assertEqual(summary["total_rows"], 6)
        self.assertEqual(summary["business_date"], "2026-07-29")

    def test_missing_source_is_rejected(self) -> None:
        manifest = self.valid_manifest()
        del manifest["sources"]["raw_orders"]
        with self.assertRaisesRegex(ManifestError, "Missing expected sources"):
            validate_manifest(self.write_manifest(manifest), SOURCES, now=NOW)

    def test_stale_delivery_is_rejected(self) -> None:
        manifest = self.valid_manifest()
        manifest["generated_at"] = "2026-07-28T00:00:00+00:00"
        with self.assertRaisesRegex(ManifestError, "exceeds"):
            validate_manifest(
                self.write_manifest(manifest), SOURCES, now=NOW, max_age_hours=26
            )

    def test_non_ready_source_is_rejected(self) -> None:
        manifest = self.valid_manifest()
        manifest["sources"]["raw_orders"]["status"] = "loading"
        with self.assertRaisesRegex(ManifestError, "not ready"):
            validate_manifest(self.write_manifest(manifest), SOURCES, now=NOW)


if __name__ == "__main__":
    unittest.main()

