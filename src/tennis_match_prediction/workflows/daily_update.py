"""Daily history refresh and prospective evaluation, without retraining models."""

import argparse
import json
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import requests

from tennis_match_prediction.forecast_evaluation.service import update_performance
from tennis_match_prediction.paths import CURATED_DIR, DATA_DIR, DEFAULT_HISTORICAL_MATCHES_CSV
from tennis_match_prediction.pipelines.historical_matches.config import RawMatchesCsv
from tennis_match_prediction.pipelines.historical_matches.refresh_history import refresh_history
from tennis_match_prediction.storage.json_io import write_json


@dataclass
class DailyUpdateSettings:
    csv_path: Path = DEFAULT_HISTORICAL_MATCHES_CSV
    curated_dir: Path = CURATED_DIR
    predictions_dir: Path = DATA_DIR / "predictions"
    performance_dir: Path = DATA_DIR / "performance"
    years: list[int] | None = None
    local_only: bool = False
    include_ongoing: bool = True
    max_requests: int = 20
    reports_dir: Path = field(default_factory=lambda: DATA_DIR / "maintenance")


def run_daily_update(settings: DailyUpdateSettings) -> dict[str, Any]:
    """Run independent stages even if one fails; preserve a dated operational report."""
    if not 0 <= settings.max_requests <= 20:
        raise ValueError("max_requests must be between 0 and 20")
    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    lock = settings.reports_dir / ".maintenance.lock"
    with lock.open("x", encoding="utf-8") as handle:
        handle.write(datetime.now(UTC).isoformat())
    report: dict[str, Any] = {
        "id": uuid4().hex,
        "started_at": datetime.now(UTC).isoformat(),
        "stages": {},
    }
    try:
        year = datetime.now(ZoneInfo("America/Sao_Paulo")).year
        source = RawMatchesCsv(
            csv_path=str(settings.csv_path),
            refresh=not settings.local_only,
            years=settings.years if settings.years is not None else [year - 1, year],
            include_ongoing=settings.include_ongoing,
        )
        try:
            history = refresh_history(source, output_dir=settings.curated_dir)
            report["stages"]["history"] = {
                key: history[key] for key in ("status", "report_path", "raw_sha256")
            }
        except (OSError, ValueError, RuntimeError, requests.RequestException) as error:
            report["stages"]["history"] = {"status": "failed", "error": str(error)}
        try:
            performance = update_performance(
                settings.predictions_dir,
                settings.performance_dir,
                api_key=os.getenv("LIVE_TENNIS_API_KEY"),
                max_requests=settings.max_requests,
            )
            report["stages"]["performance"] = {
                "status": "success",
                "report_path": performance["report_path"],
                "evaluated_count": performance["evaluated_count"],
            }
        except (OSError, ValueError, RuntimeError) as error:
            report["stages"]["performance"] = {"status": "failed", "error": str(error)}
        report["status"] = (
            "failed"
            if any(s["status"] == "failed" for s in report["stages"].values())
            else "success"
        )
        report["finished_at"] = datetime.now(UTC).isoformat()
        path = settings.reports_dir / f"{report['id']}.json"
        write_json(path, report)
        write_json(settings.reports_dir / "latest.json", report)
        if report["status"] == "failed":
            failures = "; ".join(
                f"{stage}: {result['error']}"
                for stage, result in report["stages"].items()
                if result["status"] == "failed"
            )
            raise RuntimeError(f"Daily update had failures: {failures}; see {path.resolve()}")
        return report
    finally:
        lock.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", nargs="+", type=int, help="default: current and previous year")
    parser.add_argument("--csv-path", type=Path, default=DEFAULT_HISTORICAL_MATCHES_CSV)
    parser.add_argument("--curated-dir", type=Path, default=CURATED_DIR)
    parser.add_argument("--predictions-dir", type=Path, default=DATA_DIR / "predictions")
    parser.add_argument("--performance-dir", type=Path, default=DATA_DIR / "performance")
    parser.add_argument("--local-only", action="store_true")
    parser.add_argument("--offline-results", action="store_true")
    parser.add_argument("--annual-only", action="store_true")
    parser.add_argument("--max-requests", type=int, default=20)
    args = parser.parse_args()
    try:
        report = run_daily_update(
            DailyUpdateSettings(
                csv_path=args.csv_path,
                curated_dir=args.curated_dir,
                predictions_dir=args.predictions_dir,
                performance_dir=args.performance_dir,
                years=args.years,
                local_only=args.local_only,
                include_ongoing=not args.annual_only,
                max_requests=0 if args.offline_results else args.max_requests,
            )
        )
    except (OSError, ValueError, RuntimeError) as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
