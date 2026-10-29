"""Collect observed results and publish versioned forecast evaluations."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import requests

from tennis_match_prediction.forecast_evaluation.reconciliation import evaluate_batches, timestamp
from tennis_match_prediction.forecast_evaluation.reporting import markdown_report
from tennis_match_prediction.providers.live_tennis import BASE_URL
from tennis_match_prediction.storage.json_io import write_json


def load_batches(directory: Path) -> list[dict[str, Any]]:
    """Read immutable batch records, never latest pointers or manual forecasts."""
    batches = []
    for path in sorted(directory.glob("*/batch.json")):
        batch = json.loads(path.read_text(encoding="utf-8"))
        if batch["id"] != path.parent.name or batch.get("source") != "Live Tennis API":
            raise ValueError(f"Invalid saved batch: {path}")
        batches.append(batch)
    return batches


def update_performance(
    predictions_dir: Path, output_dir: Path, *, api_key: str | None, max_requests: int = 20
) -> dict[str, Any]:
    """Collect bounded per-ID results; publish report and observations as one version."""
    if not 0 <= max_requests <= 20:
        raise ValueError("max_requests must be between 0 and 20")
    batches = load_batches(predictions_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    lock = output_dir / ".performance.lock"
    with lock.open("x", encoding="utf-8") as handle:
        handle.write(datetime.now(UTC).isoformat())
    try:
        return _update_performance(batches, output_dir, api_key, max_requests)
    finally:
        lock.unlink()


def _update_performance(
    batches: list[dict[str, Any]], output_dir: Path, api_key: str | None, max_requests: int
) -> dict[str, Any]:
    now = datetime.now(UTC)
    latest_path = output_dir / "latest.json"
    observations = {}
    if latest_path.exists():
        previous = json.loads(latest_path.read_text(encoding="utf-8"))["id"]
        observations = json.loads(
            (output_dir / "runs" / previous / "results.json").read_text(encoding="utf-8")
        )
    due = {
        row["match"]["id"]
        for batch in batches
        for row in batch["predictions"]
        if (start := timestamp(row["match"].get("scheduled_time"))) is not None and start <= now
    }
    # Round-robin oldest observations, including terminal results to catch corrections.
    ordered = sorted(
        due, key=lambda mid: (observations.get(str(mid), {}).get("observed_at", ""), mid)
    )
    requests_made = 0
    if max_requests and ordered and not api_key:
        raise ValueError("Configure LIVE_TENNIS_API_KEY or use --offline")
    for match_id in ordered[:max_requests]:
        try:
            response = requests.get(
                f"{BASE_URL}/matches/{match_id}",
                headers={"X-API-Key": api_key},
                timeout=30,
                allow_redirects=False,
            )
        except requests.RequestException:
            raise RuntimeError(
                "Result provider is unreachable; previous report preserved"
            ) from None
        requests_made += 1
        if response.status_code in (404, 410):
            # Merged IDs require verified mapping; never follow an arbitrary URL or infer a winner.
            payload = None
        elif response.status_code != 200:
            raise RuntimeError(
                f"Result provider returned HTTP {response.status_code}; previous report preserved"
            )
        else:
            payload = response.json()
            if not isinstance(payload, dict) or type(payload.get("id")) is not int:
                raise ValueError("Result provider returned an invalid match")
            if payload["id"] != match_id:
                raise ValueError("Result provider returned a different match ID")
        observations[str(match_id)] = {
            "observed_at": datetime.now(UTC).isoformat(),
            "http_status": response.status_code,
            "sha256": hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
            "match": payload,
        }
    report = evaluate_batches(batches, observations)
    report.update(
        {
            "generated_at": datetime.now(UTC).isoformat(),
            "requests_made": requests_made,
            "due_match_count": len(due),
            "not_refreshed_count": max(0, len(due) - requests_made),
            "offline": max_requests == 0,
        }
    )
    version = uuid4().hex
    destination = output_dir / "runs" / version
    destination.mkdir(parents=True)
    write_json(destination / "results.json", observations)
    write_json(destination / "report.json", report)
    (destination / "report.md").write_text(markdown_report(report), encoding="utf-8")
    write_json(latest_path, {"id": version})
    return {"id": version, "report_path": str(destination / "report.md"), **report}
