"""Eligibility policy and outcome reconciliation for archived forecasts."""

import math
from collections import Counter, defaultdict
from datetime import UTC, datetime
from typing import Any

from tennis_match_prediction.forecast_evaluation.details import prediction_details
from tennis_match_prediction.forecast_evaluation.metrics import forecast_metrics

POLICY = {
    "version": 1,
    "selection": "last eligible forecast per provider match and exact model version",
    "outcomes": "completed only; exclude retired, walkover, default, abandoned and unresolved",
    "time": "forecast and source collection before original schedule, final schedule and live_at",
    "caveat": "live_at is a provider observation, may be restamped, and is not an official start",
    "history": "history source date strictly before the UTC cutoff date",
}


def timestamp(value: Any) -> datetime | None:
    """Reject absent, malformed and naive timestamps instead of assuming a timezone."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.utcoffset() is not None else None


def _resolve(
    batch: dict[str, Any], forecast: dict[str, Any], observation: dict[str, Any] | None
) -> tuple[str | None, dict[str, Any]]:
    if observation is None:
        return "result_not_collected", {}
    result = observation.get("match")
    if result is None:
        return "result_unavailable", {}
    match = forecast["match"]
    if type(result.get("id")) is not int or result["id"] != match["id"]:
        return "match_id_conflict", {}
    players = result.get("players") or {}
    ids = [(players.get(side) or {}).get("id") for side in ("p1", "p2")]
    expected = [match["provider_player0_id"], match["provider_player1_id"]]
    if (
        any(type(value) is not int for value in [*ids, *expected])
        or len(set(ids)) != 2
        or len(set(expected)) != 2
        or set(ids) != set(expected)
    ):
        return "participant_conflict", {}
    if result.get("status") != "completed":
        return "cancelled" if result.get("status") == "cancelled" else "not_completed", {}
    if result.get("outcome") != "completed":
        return "excluded_outcome", {}
    winner = result.get("winner")
    if type(winner) is not int or winner not in (1, 2):
        return "unknown_winner", {}
    times = [
        timestamp(match.get("scheduled_time")),
        timestamp(result.get("scheduled_time")),
        timestamp(result.get("live_at")),
    ]
    created = timestamp(forecast.get("created_at"))
    fetched = timestamp(batch.get("source_fetched_at"))
    observed = timestamp(observation.get("observed_at"))
    if any(value is None for value in [*times, created, fetched, observed]):
        return "unproven_timing", {}
    cutoff = min(times)
    if not fetched <= created < cutoff or observed < max(times) or observed < created:
        return "not_pre_match", {}
    history_date = timestamp(
        f"{forecast.get('history_source_date', batch.get('history_source_date'))}T00:00:00Z"
    )
    if history_date is None or history_date.date() >= cutoff.date():
        return "incompatible_history_date", {}
    probability = forecast.get("player1_win_probability")
    complement = forecast.get("player0_win_probability")
    if (
        type(probability) not in (int, float)
        or type(complement) not in (int, float)
        or not math.isfinite(probability)
        or not math.isfinite(complement)
        or not 0 <= probability <= 1
        or not math.isclose(probability + complement, 1, abs_tol=1e-9)
    ):
        return "invalid_probability", {}
    return None, {
        "target": int(ids[winner - 1] == expected[1]),
        "probability": probability,
        "cutoff": cutoff.isoformat(),
        "result_version": result.get("result_version"),
        "result_observed_at": observation["observed_at"],
        "result_sha256": observation.get("sha256"),
        "history_lag_days": (cutoff.date() - history_date.date()).days,
    }


def evaluate_batches(
    batches: list[dict[str, Any]], observations: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Keep every decision, and count a match once for each exact model version."""
    decisions = []
    groups = defaultdict(list)
    for batch in batches:
        for forecast in batch["predictions"]:
            match_id = forecast["match"]["id"]
            reason, resolved = _resolve(batch, forecast, observations.get(str(match_id)))
            row = {
                "batch_id": batch["id"],
                "match_id": match_id,
                "model_id": batch["model_id"],
                "model_version": batch["model_version"],
                "model_name": batch.get("model_name"),
                "run_id": batch.get("run_id"),
                "history_version": batch.get("history_version"),
                "surface": forecast["match"].get("surface"),
                "created_at": forecast.get("created_at"),
                "reason": reason,
                **prediction_details(forecast, observations.get(str(match_id))),
                "prediction_correct": None,
                **resolved,
            }
            decisions.append(row)
            groups[(batch["model_version"], match_id)].append(row)
    selected = []
    for rows in groups.values():
        eligible = [row for row in rows if row["reason"] is None]
        if eligible:
            chosen = max(eligible, key=lambda row: (timestamp(row["created_at"]), row["batch_id"]))
            selected.append(chosen)
            for row in eligible:
                if row is not chosen:
                    row["reason"] = "superseded_forecast"
    models = []
    for row in selected:
        row["prediction_correct"] = row["predicted_player_index"] == row["target"]
    for version in sorted({key[0] for key in groups}):
        rows = [row for row in selected if row["model_version"] == version]
        candidates = [row for row in decisions if row["model_version"] == version]
        denominator = len({row["match_id"] for row in candidates})
        models.append(
            {
                "model_version": version,
                "model_name": candidates[0]["model_name"],
                "forecast_matches": denominator,
                "evaluated_fraction": len(rows) / denominator,
                **forecast_metrics(rows),
                "by_surface": {
                    surface: forecast_metrics([row for row in rows if row["surface"] == surface])
                    for surface in sorted({row["surface"] for row in rows}, key=str)
                },
            }
        )
    source_matches = {row["match"]["id"] for batch in batches for row in batch["predictions"]}
    skipped_matches = {row["match_id"] for batch in batches for row in batch.get("skipped", [])}
    source_total = len(source_matches | skipped_matches)
    return {
        "policy": POLICY,
        "batch_count": len(batches),
        "forecast_count": len(decisions),
        "forecast_match_count": len(source_matches),
        "source_match_count": source_total,
        "prediction_coverage": len(source_matches) / source_total if source_total else None,
        "evaluated_count": len(selected),
        "evaluated_match_count": len({row["match_id"] for row in selected}),
        "reason_counts": dict(Counter(row["reason"] for row in decisions if row["reason"])),
        "models": models,
        "decisions": decisions,
    }
