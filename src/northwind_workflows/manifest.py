"""Raw-data manifest checks that are testable outside Airflow."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class ManifestError(ValueError):
    """The upstream raw-data delivery is incomplete or invalid."""


def validate_manifest(
    path: Path,
    expected_sources: tuple[str, ...],
    *,
    now: datetime | None = None,
    max_age_hours: int = 26,
) -> dict[str, Any]:
    if not path.is_file():
        raise ManifestError(f"Raw manifest not found: {path}")
    with path.open(encoding="utf-8") as handle:
        manifest = json.load(handle)

    try:
        generated_at = datetime.fromisoformat(manifest["generated_at"])
        sources = manifest["sources"]
        business_date = manifest["business_date"]
    except (KeyError, TypeError, ValueError) as error:
        raise ManifestError(f"Malformed raw manifest: {error}") from error

    if generated_at.tzinfo is None:
        raise ManifestError("generated_at must include a timezone")
    current_time = now or datetime.now(timezone.utc)
    age_hours = (current_time - generated_at).total_seconds() / 3600
    if age_hours < 0 or age_hours > max_age_hours:
        raise ManifestError(
            f"Raw manifest age {age_hours:.1f}h exceeds {max_age_hours}h"
        )

    missing = set(expected_sources) - set(sources)
    if missing:
        raise ManifestError(f"Missing expected sources: {sorted(missing)}")
    for source_name in expected_sources:
        source = sources[source_name]
        if source.get("status") != "ready":
            raise ManifestError(f"{source_name} is not ready")
        if not isinstance(source.get("row_count"), int) or source["row_count"] < 0:
            raise ManifestError(f"{source_name} has an invalid row_count")

    return {
        "business_date": business_date,
        "generated_at": generated_at.isoformat(),
        "total_rows": sum(sources[name]["row_count"] for name in expected_sources),
        "source_count": len(expected_sources),
    }

