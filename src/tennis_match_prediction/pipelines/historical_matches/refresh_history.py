"""Refresh annual ATP results and rebuild all historical features and player state."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

from dagster import materialize

from tennis_match_prediction.paths import (
    CURATED_DIR,
    CURATED_MATCHES_CSV_NAME,
    TRAINING_MATCHES_CSV_NAME,
)
from tennis_match_prediction.pipelines.historical_matches.config import (
    RawMatchesCsv,
    _atomic_output,
)
from tennis_match_prediction.pipelines.historical_matches.defs import ASSET_CHECKS, ASSETS
from tennis_match_prediction.pipelines.shared.parquet_io import ParquetDataFrameIOManager


def refresh_history(source: RawMatchesCsv, *, output_dir: Path = CURATED_DIR) -> dict[str, Any]:
    """Fully replay local history after refresh, publishing only after checks pass.

    Attempts retain isolated snapshots and a report for cost comparison and debugging.
    Publication is atomic per file, not a transaction across the three consumer files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    lock_path = output_dir / ".refresh.lock"
    # Exclusive creation prevents two refresh commands from publishing concurrently.
    with lock_path.open("x", encoding="utf-8") as lock:
        lock.write(datetime.now(UTC).isoformat())
    try:
        return _refresh_history(source, output_dir)
    finally:
        lock_path.unlink()


def _refresh_history(source: RawMatchesCsv, output_dir: Path) -> dict[str, Any]:
    started = perf_counter()
    attempt = output_dir / ".refreshes" / uuid4().hex
    attempt.mkdir(parents=True)
    staged_curated = attempt / "curated"
    report_path = attempt / "report.json"
    report: dict[str, Any] = {
        "started_at": datetime.now(UTC).isoformat(),
        "status": "running",
        "mode": "full_rebuild",
        "source": source.model_dump(mode="json"),
        "report_path": str(report_path.resolve()),
    }
    try:
        result = materialize(
            [*ASSETS, *ASSET_CHECKS],
            resources={
                "raw_matches_csv": source,
                "io_manager": ParquetDataFrameIOManager(
                    base_dir=str(attempt / "staging"), curated_base_dir=str(staged_curated)
                ),
            },
            run_config={
                "ops": {
                    name: {"config": {"output_dir": str(staged_curated)}}
                    for name in ("curated_atp_matches_csv", "training_atp_matches_csv")
                }
            },
            raise_on_error=False,
        )
        report["run_id"] = result.run_id
        report["step_seconds"] = {
            event.step_key: event.event_specific_data.duration_ms / 1000
            for event in result.all_events
            if event.is_step_success
        }
        evaluations = result.get_asset_check_evaluations()
        report["failed_checks"] = [
            f"{check.asset_key.to_user_string()}:{check.check_name}"
            for check in evaluations
            if not check.passed
        ]
        if not result.success or report["failed_checks"]:
            raise RuntimeError("Historical dataset execution or quality checks failed")

        # Keep exact inputs alongside the generated snapshots for subsequent comparisons.
        raw_path = Path(source.csv_path)
        shutil.copyfile(raw_path, attempt / "source.csv")
        manifest_path = raw_path.parent / "manifest.json"
        if manifest_path.exists():
            shutil.copyfile(manifest_path, attempt / "source_manifest.json")
        report["raw_sha256"] = _sha256(attempt / "source.csv")
        report["artifacts"] = {}
        for name in (
            CURATED_MATCHES_CSV_NAME,
            TRAINING_MATCHES_CSV_NAME,
            "player_history.parquet",
        ):
            snapshot = staged_curated / name
            destination = output_dir / name
            with _atomic_output(destination) as temporary:
                shutil.copyfile(snapshot, temporary)
            report["artifacts"][name] = {
                "snapshot_path": str(snapshot.resolve()),
                "published_path": str(destination.resolve()),
                "sha256": _sha256(snapshot),
                "bytes": snapshot.stat().st_size,
            }
        report["status"] = "success"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = str(error)
        raise RuntimeError(f"Refresh failed; see {report_path.resolve()}: {error}") from error
    finally:
        report["finished_at"] = datetime.now(UTC).isoformat()
        report["total_seconds"] = perf_counter() - started
        with _atomic_output(report_path) as temporary:
            temporary.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def main() -> None:
    """Refresh selected seasons, then rebuild history from all available local seasons."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("years", nargs="*", type=int, help="seasons to download; default: all")
    parser.add_argument("--csv-path", type=Path)
    parser.add_argument("--output-dir", type=Path, default=CURATED_DIR)
    parser.add_argument("--local-only", action="store_true", help="rebuild without downloading")
    arguments = parser.parse_args()
    if arguments.local_only and arguments.years:
        parser.error("years selects downloads and cannot be combined with --local-only")
    options: dict[str, Any] = {
        "refresh": not arguments.local_only,
        "years": arguments.years or None,
    }
    if arguments.csv_path is not None:
        options["csv_path"] = str(arguments.csv_path)
    source = RawMatchesCsv(**options)
    csv_path = Path(source.csv_path)
    if (
        arguments.local_only
        and not csv_path.exists()
        and not any(csv_path.parent.glob("[0-9][0-9][0-9][0-9].csv"))
    ):
        parser.error("--local-only requires a consolidated CSV or local annual files")
    try:
        report = refresh_history(source, output_dir=arguments.output_dir)
    except (OSError, ValueError, RuntimeError) as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
