"""Reconcile archived forecasts with provider results and report prospective metrics."""

import argparse
import hashlib
import json
import math
import os
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd
import requests

from tennis_match_prediction.live_tennis_probe import BASE_URL
from tennis_match_prediction.ml.evaluation import probability_metrics
from tennis_match_prediction.paths import DATA_DIR
from tennis_match_prediction.web.upcoming import _write_json

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


def load_batches(directory: Path) -> list[dict[str, Any]]:
    """Read immutable batch records, never latest pointers or manual forecasts."""
    batches = []
    for path in sorted(directory.glob("*/batch.json")):
        batch = json.loads(path.read_text(encoding="utf-8"))
        if batch["id"] != path.parent.name or batch.get("source") != "Live Tennis API":
            raise ValueError(f"Invalid saved batch: {path}")
        batches.append(batch)
    return batches


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


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"count": 0, "accuracy": None, "brier": None, "log_loss": None}
    target = pd.Series([row["target"] for row in rows])
    probabilities = np.array([row["probability"] for row in rows])
    metrics = probability_metrics(target, probabilities, "observed")
    return {
        "count": len(rows),
        **{name.removeprefix("observed_"): value for name, value in metrics.items()},
        "mean_history_lag_days": sum(row["history_lag_days"] for row in rows) / len(rows),
        "neutral_baseline_brier": 0.25,
        "neutral_baseline_log_loss": math.log(2),
        "calibration": [
            {
                "lower": lower / 10,
                "upper": (lower + 1) / 10,
                "count": len(bucket),
                "mean_probability": sum(row["probability"] for row in bucket) / len(bucket),
                "observed_frequency": sum(row["target"] for row in bucket) / len(bucket),
            }
            for lower in range(10)
            if (bucket := [row for row in rows if min(int(row["probability"] * 10), 9) == lower])
        ],
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
                **_metrics(rows),
                "by_surface": {
                    surface: _metrics([row for row in rows if row["surface"] == surface])
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


def markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# Desempenho das previsões salvas",
        "",
        f"Gerado em UTC: {report['generated_at']}",
        "",
        f"Lotes: {report['batch_count']}. Partidas previstas: {report['forecast_match_count']}. "
        f"Partidas avaliadas: {report['evaluated_match_count']}.",
        "",
        "Última previsão elegível por partida e versão exata do modelo. "
        "Somente partidas concluídas normalmente; abandonos, walkovers e cancelamentos excluídos.",
        "A previsão deve anteceder os horários agendados original/final e live_at. "
        "live_at é uma observação do provedor, pode ser regravado e não comprova o início oficial.",
        "O histórico deve ter data de fonte anterior ao dia UTC do corte. "
        "Cobertura avaliada inclui pendências no denominador; "
        "modelos podem ter amostras diferentes.",
        "",
        "| Modelo / versão | Avaliadas / previstas | Accuracy | Brier | Log loss |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for model in report["models"]:
        values = [
            f"{model[name]:.4f}" if model[name] is not None else "—"
            for name in ("accuracy", "brier", "log_loss")
        ]
        label = str(model["model_name"]).replace("|", " ").replace("\n", " ")
        lines.append(
            f"| {label} / {model['model_version'][:12]} | "
            f"{model['count']} / {model['forecast_matches']} | {' | '.join(values)} |"
        )
    if report["evaluated_count"] == 0:
        lines.extend(["", "Ainda não há partidas elegíveis para calcular desempenho."])
    lines.extend(["", "## Motivos de exclusão ou pendência", ""])
    lines.extend(
        f"- {reason}: {count}" for reason, count in sorted(report["reason_counts"].items())
    )
    lines.extend(
        [
            "",
            "Referência neutra (50%): Brier 0,25; log loss 0,6931.",
            "O JSON contém decisões por previsão, versões de resultado, defasagem do histórico, "
            "métricas por superfície e frequências de calibração. "
            "Amostras pequenas não estabelecem calibração.",
            "",
            "Contrato do provedor: https://docs.livetennisapi.com/live-scores.html",
            "",
        ]
    )
    return "\n".join(lines)


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


def _update_performance(batches, output_dir, api_key, max_requests):
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
    _write_json(destination / "results.json", observations)
    _write_json(destination / "report.json", report)
    (destination / "report.md").write_text(markdown_report(report), encoding="utf-8")
    _write_json(latest_path, {"id": version})
    return {"id": version, "report_path": str(destination / "report.md"), **report}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--predictions-dir", type=Path, default=DATA_DIR / "predictions")
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR / "performance")
    parser.add_argument("--max-requests", type=int, default=20)
    parser.add_argument("--offline", action="store_true", help="reuse saved results without HTTP")
    args = parser.parse_args()
    try:
        report = update_performance(
            args.predictions_dir,
            args.output_dir,
            api_key=os.getenv("LIVE_TENNIS_API_KEY"),
            max_requests=0 if args.offline else args.max_requests,
        )
    except (OSError, ValueError, RuntimeError) as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps({k: v for k, v in report.items() if k != "decisions"}, indent=2))


if __name__ == "__main__":
    main()
