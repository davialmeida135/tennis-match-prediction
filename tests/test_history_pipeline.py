import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
from dagster import AssetCheckResult, DagsterInstance, Definitions, asset_check, materialize

from tennis_match_prediction import paths
from tennis_match_prediction.contracts.features import PLAYER_COMPARISON_FEATURE_COLUMNS
from tennis_match_prediction.contracts.player_history import PlayerHistory
from tennis_match_prediction.pipelines.defs import defs
from tennis_match_prediction.pipelines.historical_matches import assets
from tennis_match_prediction.pipelines.historical_matches import refresh_history as refresh_module
from tennis_match_prediction.pipelines.historical_matches.config import RawMatchesCsv
from tennis_match_prediction.pipelines.historical_matches.defs import ASSET_CHECKS, ASSETS
from tennis_match_prediction.pipelines.shared.parquet_io import ParquetDataFrameIOManager
from tennis_match_prediction.transforms.match_preparation import prepare_history
from tennis_match_prediction.transforms.temporal_features import (
    attach_temporal_features,
)
from tennis_match_prediction.transforms.training_rows import (
    FINAL_COLUMN_ORDER,
)


def _history(count: int = 4) -> pd.DataFrame:
    rows = []
    for index in range(count):
        winner, loser = ("Alice", "Bob") if index % 2 == 0 else ("Bob", "Alice")
        played_on = date(2026, 1, 1) + timedelta(days=index)
        rows.append(
            {
                "tourney_date": played_on.isoformat()
                if index % 2
                else played_on.strftime("%Y%m%d"),
                "tourney_id": "2026-1",
                "match_num": index + 1,
                "winner_id": winner,
                "loser_id": loser,
                "winner_name": winner,
                "loser_name": loser,
                "surface": None if index % 2 else " hard ",
                "score": "6-4 6-4",
                "tourney_level": "A",
                "round": "F",
                "draw_size": 32,
                "best_of": 3,
                "minutes": 80,
                **{
                    f"{side}_{stem}": value
                    for side in ("winner", "loser")
                    for stem, value in {
                        "ht": None,
                        "age": None,
                        "rank": None,
                        "rank_points": None,
                        "seed": None,
                        "entry": None,
                        "hand": "R",
                    }.items()
                },
            }
        )
    return pd.DataFrame(rows)


def test_refresh_rebuilds_new_corrected_and_deleted_results(tmp_path, monkeypatch):
    """Repeated refreshes replay the full corrected input without reapplying results."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    output = tmp_path / "curated"
    downloaded = _history(200)
    calls = []

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"files": [{"name": "2026.csv", "url": "https://example.test/2026.csv"}]}

        @property
        def content(self):
            return downloaded.to_csv(index=False).encode()

    def get(*args, **kwargs):
        calls.append(args[0])
        return Response()

    monkeypatch.setattr(
        "tennis_match_prediction.pipelines.historical_matches.config.requests.get", get
    )
    source = RawMatchesCsv(
        csv_path=str(raw_dir / "all_atp_matches.csv"),
        catalog_url="https://example.test/catalog",
        years=[2026],
    )
    initial = refresh_module.refresh_history(source, output_dir=output)
    assert len(calls) == 2  # catalog and annual file, never a second fetch inside Dagster
    downloaded = _history(220).drop(index=10).reset_index(drop=True)
    # Correct the winner of an earlier match without creating another match.
    for stem in ("id", "name"):
        downloaded.loc[0, [f"winner_{stem}", f"loser_{stem}"]] = ["Bob", "Alice"]
    updated = refresh_module.refresh_history(source, output_dir=output)
    assert initial["raw_sha256"] != updated["raw_sha256"]
    assert updated["status"] == "success"
    assert updated["total_seconds"] > 0
    assert updated["step_seconds"]["player_comparison_atp_matches"] > 0
    attempt = Path(updated["report_path"]).parent
    assert json.loads(Path(updated["report_path"]).read_text()) == updated
    normalized = pd.read_parquet(attempt / "staging" / "normalized_atp_matches.parquet")
    _, expected_features, expected_history = attach_temporal_features(normalized)
    actual_features = pd.read_parquet(attempt / "staging" / "player_comparison_atp_matches.parquet")
    columns = list(PLAYER_COMPARISON_FEATURE_COLUMNS)
    pd.testing.assert_frame_equal(actual_features[columns], expected_features[columns])
    actual_history = PlayerHistory.model_validate_json(
        pd.read_parquet(output / "player_history.parquet").loc[0, "history"]
    )
    assert actual_history == expected_history
    assert sum(player.wins for player in actual_history.players.values()) == 219
    assert len(pd.read_csv(output / paths.TRAINING_MATCHES_CSV_NAME)) == 219

    def unexpected_rebuild(*args, **kwargs):
        pytest.fail("Unchanged data and implementation must not rebuild the history")

    with monkeypatch.context() as unchanged:
        unchanged.setattr(refresh_module, "materialize", unexpected_rebuild)
        repeated = refresh_module.refresh_history(source, output_dir=output)
    assert repeated["status"] == "unchanged"
    assert updated["raw_sha256"] == repeated["raw_sha256"]
    for name in updated["artifacts"]:
        assert updated["artifacts"][name]["sha256"] == repeated["artifacts"][name]["sha256"]


def test_publication_failure_restores_exports_and_keeps_previous_pointer(tmp_path, monkeypatch):
    attempt = tmp_path / "attempt"
    output = tmp_path / "published"
    stage = attempt / "curated"
    stage.mkdir(parents=True)
    output.mkdir()
    names = ("curated.csv", "training.csv", "history.parquet")
    for name in names:
        (output / name).write_bytes(b"old")
        (stage / name).write_bytes(b"new")
    (output / "latest_refresh.json").write_bytes(b"old pointer")
    copyfile = refresh_module.shutil.copyfile

    def fail_second_export(source, destination):
        if source == stage / "training.csv":
            raise OSError("publication interrupted")
        return copyfile(source, destination)

    monkeypatch.setattr(refresh_module.shutil, "copyfile", fail_second_export)
    with pytest.raises(OSError, match="publication interrupted"):
        refresh_module._publish_exports(attempt, output, {"artifacts": dict.fromkeys(names)})
    assert all((output / name).read_bytes() == b"old" for name in names)
    assert (output / "latest_refresh.json").read_bytes() == b"old pointer"


def test_failed_checks_preserve_published_history_and_exports(tmp_path, monkeypatch):
    raw = tmp_path / "raw.csv"
    _history(200).to_csv(raw, index=False)
    output = tmp_path / "curated"
    output.mkdir()
    names = [
        "player_history.parquet",
        paths.TRAINING_MATCHES_CSV_NAME,
        paths.CURATED_MATCHES_CSV_NAME,
    ]
    for name in names:
        (output / name).write_bytes(b"previous published version")

    @asset_check(asset=assets.training_atp_matches, name="reject_refresh")
    def reject_refresh():
        return AssetCheckResult(passed=False)

    monkeypatch.setattr(refresh_module, "ASSET_CHECKS", [*ASSET_CHECKS, reject_refresh])
    with pytest.raises(RuntimeError, match="quality checks failed"):
        refresh_module.refresh_history(
            RawMatchesCsv(csv_path=str(raw), refresh=False), output_dir=output
        )
    for name in names:
        assert (output / name).read_bytes() == b"previous published version"
    (report_path,) = (output / ".refreshes").glob("*/report.json")
    report = json.loads(report_path.read_text())
    assert report["status"] == "failed"
    assert "training_atp_matches:reject_refresh" in report["failed_checks"]
    assert not (output / ".refresh.lock").exists()


def test_history_paths_share_order_filtering_surface_and_elo() -> None:
    raw = _history()
    raw["tourney_date"] = "2026-01-01"
    raw["tourney_id"] = ["b", "a", "a", "b"]
    raw["match_num"] = [1, None, 1, 1]
    walkover = raw.iloc[[0]].assign(score="W/O", tourney_date="2025-12-01")
    missing_id = raw.iloc[[0]].assign(winner_id=None)
    raw = pd.concat([walkover, raw, missing_id], ignore_index=True)

    prepared = prepare_history(raw)
    _, temporal, history = attach_temporal_features(prepared)

    assert prepared["match_num"].tolist()[:1] == [1]
    assert pd.isna(prepared.loc[1, "match_num"])
    assert prepared["surface"].tolist() == ["Hard"] * 4
    assert len(temporal) == 4
    assert temporal["overall_elo_diff"].eq(0.0).all()
    assert sum(player.wins for player in history.players.values()) == 4


def test_registered_pipeline_materializes_snapshots_and_exports(tmp_path, monkeypatch) -> None:
    Definitions.validate_loadable(defs)
    source = _history(200)
    walkover = source.iloc[[0]].assign(score="W/O", tourney_date="2025-12-01")
    raw = pd.concat([walkover, source], ignore_index=True)
    csv_path = tmp_path / "raw.csv"
    raw.to_csv(csv_path, index=False)
    monkeypatch.setattr(paths, "CURATED_DIR", tmp_path / "curated")
    staging = tmp_path / "staging"

    instance_dir = tmp_path / "instance"
    instance_dir.mkdir()
    with DagsterInstance.ephemeral(
        tempdir=str(instance_dir), settings={"telemetry": {"enabled": False}}
    ) as instance:
        result = materialize(
            [*ASSETS, *ASSET_CHECKS],
            instance=instance,
            resources={
                "io_manager": ParquetDataFrameIOManager(
                    base_dir=str(staging), curated_base_dir=str(tmp_path / "curated")
                ),
                "raw_matches_csv": RawMatchesCsv(csv_path=str(csv_path), refresh=False),
            },
        )

    assert result.success
    assert all(event.passed for event in result.get_asset_check_evaluations())
    assert assets.training_atp_matches.dependency_keys == {assets.curated_atp_matches.key}
    normalized = pd.read_parquet(staging / "normalized_atp_matches.parquet")
    comparisons = pd.read_parquet(staging / "player_comparison_atp_matches.parquet")
    training = pd.read_csv(tmp_path / "curated" / paths.TRAINING_MATCHES_CSV_NAME)
    assert len(normalized) == len(training) == 200
    assert normalized["surface"].eq("Hard").all()
    assert list(training.columns) == FINAL_COLUMN_ORDER
    for removed in (
        "winrate_featured_atp_matches",
        "elo_featured_atp_matches",
        "imputed_atp_matches",
    ):
        assert not (staging / f"{removed}.parquet").exists()
    assert len(training.columns) == 14
    assert training[list(PLAYER_COMPARISON_FEATURE_COLUMNS)].notna().all().all()
    _, expected, history = attach_temporal_features(normalized)
    pd.testing.assert_frame_equal(
        comparisons[list(PLAYER_COMPARISON_FEATURE_COLUMNS)],
        expected[list(PLAYER_COMPARISON_FEATURE_COLUMNS)],
    )
    snapshot = pd.read_parquet(tmp_path / "curated" / "player_history.parquet")
    assert PlayerHistory.model_validate_json(snapshot.loc[0, "history"]) == history

    # Training consumes only the published rows; prediction consumes only the asset snapshot.
    from tennis_match_prediction.contracts.prediction import FutureMatchRequest
    from tennis_match_prediction.ml.predict import predict
    from tennis_match_prediction.ml.train import train

    model_path = tmp_path / "model.pkl"
    experiment = train(
        tmp_path / "curated" / paths.TRAINING_MATCHES_CSV_NAME,
        model_path,
        train_start=date(2026, 1, 11),
        validation_start=date(2026, 5, 1),
        test_start=date(2026, 6, 1),
        tracking_mode="disabled",
        final_evaluation=True,
    )
    assert 0 <= experiment.metrics["test_accuracy"] <= 1
    prediction = predict(
        FutureMatchRequest(
            player0_name="Alice", player1_name="Bob", match_date="2027-01-01", surface="Hard"
        ),
        model_path,
        tmp_path / "curated" / "player_history.parquet",
    )
    assert prediction.player0_win_probability + prediction.player1_win_probability == 1
    assert prediction.history_source_date == history.last_source_date
    assert len(pd.read_csv(tmp_path / "curated" / paths.CURATED_MATCHES_CSV_NAME)) == 200
