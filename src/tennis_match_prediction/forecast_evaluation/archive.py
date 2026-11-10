"""Read saved prediction batches and report snapshots from local archives."""

import json
from pathlib import Path
from typing import Any

from tennis_match_prediction.forecast_evaluation.details import prediction_details


def load_batches(directory: Path) -> list[dict[str, Any]]:
    """Read immutable batch records, never latest pointers or manual forecasts."""
    batches = []
    for path in sorted(directory.glob("*/batch.json")):
        batch = json.loads(path.read_text(encoding="utf-8"))
        if batch["id"] != path.parent.name or batch.get("source") != "Live Tennis API":
            raise ValueError(f"Invalid saved batch: {path}")
        batches.append(batch)
    return batches


def load_report(directory: Path, predictions_dir: Path) -> dict[str, Any]:
    """Enrich older reports without network access, rescoring, or changing saved files."""
    report = json.loads((directory / "report.json").read_text(encoding="utf-8"))
    missing = [row for row in report.get("decisions", []) if "forecast" not in row]
    if not missing:
        return report
    results_path = directory / "results.json"
    observations = (
        json.loads(results_path.read_text(encoding="utf-8")) if results_path.is_file() else {}
    )
    forecasts = {}
    for batch_id in {row["batch_id"] for row in missing}:
        # Batch IDs originate in saved JSON, not in a trusted filesystem path.
        if not batch_id or len(batch_id) > 64 or any(c not in "0123456789abcdef" for c in batch_id):
            continue
        path = predictions_dir / batch_id / "batch.json"
        if path.is_file():
            batch = json.loads(path.read_text(encoding="utf-8"))
            if batch.get("id") == batch_id:
                for forecast in batch.get("predictions", []):
                    forecasts[(batch_id, forecast["match"]["id"])] = forecast
    for row in missing:
        forecast = forecasts.get((row["batch_id"], row["match_id"]))
        if forecast is not None:
            row.update(prediction_details(forecast, observations.get(str(row["match_id"]))))
            row["prediction_correct"] = (
                row["predicted_player_index"] == row["target"]
                if row.get("reason") is None and row.get("predicted_player_index") is not None
                else None
            )
    return report
