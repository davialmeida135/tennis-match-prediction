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
    PROJECT_ROOT,
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
    artifact_names = (CURATED_MATCHES_CSV_NAME, TRAINING_MATCHES_CSV_NAME, "player_history.parquet")
    try:
        raw_path = Path(source.csv_path)
        if source.refresh:
            years = set(source.years) if source.years is not None else None
            source.refresh_history(raw_path.parent, years)
        elif not raw_path.exists():
            source.consolidate_seasons(raw_path.parent)
        shutil.copyfile(raw_path, attempt / "source.csv")
        report["raw_sha256"] = _sha256(attempt / "source.csv")
        report["pipeline_sha256"] = _pipeline_sha256()
        latest_path = output_dir / "latest_refresh.json"
        if latest_path.is_file():
            latest = json.loads(latest_path.read_text(encoding="utf-8"))
            if (
                latest.get("raw_sha256") == report["raw_sha256"]
                and latest.get("pipeline_sha256") == report["pipeline_sha256"]
                and set(latest.get("artifacts", {})) == set(artifact_names)
                and all(
                    (output_dir / name).is_file()
                    and _sha256(output_dir / name) == item["sha256"]
                    and Path(item["snapshot_path"]).is_file()
                    and _sha256(Path(item["snapshot_path"])) == item["sha256"]
                    for name, item in latest["artifacts"].items()
                )
            ):
                report["status"] = "unchanged"
                report["artifacts"] = latest["artifacts"]
                return report
        # ConfigurableResource caches its resolved config; model_copy(update=...)
        # does not replace that Dagster config. Construct a fresh resource.
        frozen_source = RawMatchesCsv(
            **{
                **source.model_dump(),
                "csv_path": str(attempt / "source.csv"),
                "refresh": False,
            }
        )
        result = materialize(
            [*ASSETS, *ASSET_CHECKS],
            resources={
                "raw_matches_csv": frozen_source,
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
        manifest_path = raw_path.parent / "manifest.json"
        if manifest_path.exists():
            shutil.copyfile(manifest_path, attempt / "source_manifest.json")
        report["raw_sha256"] = _sha256(attempt / "source.csv")
        report["artifacts"] = {}
        for name in artifact_names:
            snapshot = staged_curated / name
            destination = output_dir / name
            report["artifacts"][name] = {
                "snapshot_path": str(snapshot.resolve()),
                "published_path": str(destination.resolve()),
                "sha256": _sha256(snapshot),
                "bytes": snapshot.stat().st_size,
            }
        _publish_exports(attempt, output_dir, report)
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


def _publish_exports(attempt: Path, output_dir: Path, report: dict[str, Any]) -> None:
    backups = attempt / "previous"
    backups.mkdir()
    for name in report["artifacts"]:
        if (output_dir / name).exists():
            shutil.copyfile(output_dir / name, backups / name)
    try:
        for name in report["artifacts"]:
            with _atomic_output(output_dir / name) as temporary:
                shutil.copyfile(attempt / "curated" / name, temporary)
        report["status"] = "success"
        # Web readers resolve this pointer to immutable validated snapshots.
        with _atomic_output(output_dir / "latest_refresh.json") as temporary:
            temporary.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    except OSError:
        # Restore compatibility exports after a handled publication failure.
        # Web readers keep their old immutable snapshot until the pointer changes.
        for name in report["artifacts"]:
            if (backups / name).exists():
                with _atomic_output(output_dir / name) as temporary:
                    shutil.copyfile(backups / name, temporary)
            else:
                (output_dir / name).unlink(missing_ok=True)
        raise


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _pipeline_sha256() -> str:
    """Invalidate the unchanged shortcut when implementation or dependencies change."""
    digest = hashlib.sha256()
    files = sorted((PROJECT_ROOT / "src" / "tennis_match_prediction").rglob("*.py"))
    for path in [*files, PROJECT_ROOT / "pyproject.toml", PROJECT_ROOT / "uv.lock"]:
        digest.update(str(path.relative_to(PROJECT_ROOT)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def main() -> None:
    """Refresh selected seasons, then rebuild history from all available local seasons."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("years", nargs="*", type=int, help="seasons to download; default: all")
    parser.add_argument("--csv-path", type=Path)
    parser.add_argument("--output-dir", type=Path, default=CURATED_DIR)
    parser.add_argument("--local-only", action="store_true", help="rebuild without downloading")
    parser.add_argument(
        "--include-ongoing", action="store_true", help="retain recent partial results"
    )
    arguments = parser.parse_args()
    if arguments.local_only and arguments.years:
        parser.error("years selects downloads and cannot be combined with --local-only")
    options: dict[str, Any] = {
        "refresh": not arguments.local_only,
        "years": arguments.years or None,
        "include_ongoing": arguments.include_ongoing,
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
