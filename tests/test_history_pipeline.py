from datetime import date, timedelta

import pandas as pd
import pytest
from dagster import DagsterInstance, Definitions, materialize

from pipelines.historical_matches import assets
from pipelines.historical_matches.config import RawMatchesCsv
from pipelines.historical_matches.defs import ASSET_CHECKS, ASSETS, defs
from pipelines.historical_matches.transforms.elo import calcular_elo
from pipelines.historical_matches.transforms.head_to_head import calcular_h2h
from pipelines.historical_matches.transforms.history import prepare_history
from pipelines.historical_matches.transforms.imputation import (
    DEFAULT_AGE,
    DEFAULT_HEIGHT,
    DEFAULT_RANK,
    DEFAULT_RANK_POINTS,
    fill_null_age,
    fill_null_height,
    fill_null_rank,
)
from pipelines.historical_matches.transforms.player_history import attach_temporal_features
from pipelines.historical_matches.transforms.training_rows import FINAL_COLUMN_ORDER
from pipelines.historical_matches.transforms.winrate import calcular_winrate_total
from pipelines.shared import paths
from pipelines.shared.contracts import FEATURE_COLUMNS, PlayerHistory
from pipelines.shared.parquet_io import ParquetDataFrameIOManager


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


def test_history_paths_share_order_filtering_surface_and_elo() -> None:
    raw = _history()
    raw["tourney_date"] = "2026-01-01"
    raw["tourney_id"] = ["b", "a", "a", "b"]
    raw["match_num"] = [1, None, 1, 1]
    walkover = raw.iloc[[0]].assign(score="W/O", tourney_date="2025-12-01")
    missing_id = raw.iloc[[0]].assign(winner_id=None)
    raw = pd.concat([walkover, raw, missing_id], ignore_index=True)

    prepared = prepare_history(raw)
    elo = calcular_elo(raw)
    h2h = calcular_h2h(raw.dropna(subset=["winner_id"]))
    winrates = calcular_winrate_total(raw)
    _, temporal, history = attach_temporal_features(prepared)
    expected_differences = temporal["overall_elo_diff"].tolist()

    assert prepared["match_num"].tolist()[:1] == [1]
    assert pd.isna(prepared.loc[1, "match_num"])
    assert prepared["surface"].tolist() == ["Hard"] * 4
    for frame in (elo, h2h, winrates):
        assert frame["tourney_id"].tolist() == prepared["tourney_id"].tolist()
        assert frame["match_num"].fillna(-1).tolist() == prepared["match_num"].fillna(-1).tolist()
        assert len(frame) == 4
    assert elo["elo_diff"].tolist() == pytest.approx(expected_differences)
    assert sum(player.wins for player in history.players.values()) == 4


@pytest.mark.parametrize("fill", [fill_null_height, fill_null_age, fill_null_rank])
def test_imputation_is_causal_and_independent_of_winner_assignment(fill) -> None:
    source = _history(3)
    for stem in ("ht", "age", "rank", "rank_points"):
        source[f"winner_{stem}"] = [None, 10.0, None]
        source[f"loser_{stem}"] = [None, 30.0, None]
    prefix = fill(source.iloc[:2])
    future = source.iloc[[0]].copy()
    for stem in ("ht", "age", "rank", "rank_points"):
        future[f"winner_{stem}"] = 999999.0
        future[f"loser_{stem}"] = 0.0
    full = fill(pd.concat([source, future], ignore_index=True))
    pd.testing.assert_frame_equal(full.iloc[:2], prefix)
    swapped = source.copy()
    for stem in ("ht", "age", "rank", "rank_points"):
        swapped[f"winner_{stem}"] = source[f"loser_{stem}"]
        swapped[f"loser_{stem}"] = source[f"winner_{stem}"]
    swapped_result = fill(swapped)
    filled = fill(source)
    for stem in ("ht", "age", "rank", "rank_points"):
        pd.testing.assert_series_equal(
            filled[f"winner_{stem}"], swapped_result[f"loser_{stem}"], check_names=False
        )
    if fill is fill_null_height:
        assert filled["winner_ht"].tolist() == [DEFAULT_HEIGHT, 10.0, 20.0]
    elif fill is fill_null_age:
        assert filled["winner_age"].tolist() == [DEFAULT_AGE, 10.0, 20.0]
    else:
        assert filled["winner_rank"].tolist() == [DEFAULT_RANK, 10.0, 30.0]
        assert filled["winner_rank_points"].tolist() == [DEFAULT_RANK_POINTS, 10.0, 10.0]


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
                "io_manager": ParquetDataFrameIOManager(base_dir=str(staging)),
                "raw_matches_csv": RawMatchesCsv(csv_path=str(csv_path)),
            },
        )

    assert result.success
    assert all(event.passed for event in result.get_asset_check_evaluations())
    assert assets.training_atp_matches.dependency_keys == {assets.curated_atp_matches.key}
    normalized = pd.read_parquet(staging / "normalized_atp_matches.parquet")
    comparisons = pd.read_parquet(staging / "player_comparison_atp_matches.parquet")
    elo = pd.read_parquet(staging / "elo_featured_atp_matches.parquet")
    training = pd.read_csv(tmp_path / "curated" / paths.TRAINING_MATCHES_CSV_NAME)
    assert len(normalized) == len(training) == 200
    assert normalized["surface"].eq("Hard").all()
    assert list(training.columns) == FINAL_COLUMN_ORDER
    assert training.filter(regex="_(ht|age|rank|rank_points)$").notna().all().all()
    assert elo["elo_diff"].tolist() == pytest.approx(comparisons["overall_elo_diff"].tolist())
    _, expected, history = attach_temporal_features(normalized)
    pd.testing.assert_frame_equal(
        comparisons[list(FEATURE_COLUMNS)], expected[list(FEATURE_COLUMNS)]
    )
    snapshot = pd.read_parquet(staging / "player_history.parquet")
    assert PlayerHistory.model_validate_json(snapshot.loc[0, "history"]) == history

    # Training consumes only the published rows; prediction consumes only the asset snapshot.
    from ml.predict import predict
    from ml.train import train
    from pipelines.shared.contracts import FutureMatchRequest

    monkeypatch.setattr("ml.train._log_mlflow", lambda *_: None)
    model_path = tmp_path / "model.pkl"
    metrics = train(
        tmp_path / "curated" / paths.TRAINING_MATCHES_CSV_NAME,
        model_path,
        train_start=date(2026, 1, 11),
        validation_start=date(2026, 5, 1),
        test_start=date(2026, 6, 1),
    )
    assert 0 <= metrics["test_accuracy"] <= 1
    prediction = predict(
        FutureMatchRequest(
            player0_name="Alice", player1_name="Bob", match_date="2027-01-01", surface="Hard"
        ),
        model_path,
        staging / "player_history.parquet",
    )
    assert prediction.player0_win_probability + prediction.player1_win_probability == 1
    assert prediction.history_source_date == history.last_source_date
    for name in (paths.CURATED_MATCHES_CSV_NAME, paths.PLAYER_COMPARISON_CSV_NAME):
        assert len(pd.read_csv(tmp_path / "curated" / name)) == 200
