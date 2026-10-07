import pandas as pd
import pytest

from pipelines.historical_matches.anonymization import FINAL_COLUMN_ORDER, anonymize
from pipelines.historical_matches.assets import (
    curated_atp_matches,
    elo_featured_atp_matches,
    h2h_featured_atp_matches,
    imputed_atp_matches,
    normalized_atp_matches,
    winrate_featured_atp_matches,
)
from pipelines.historical_matches.feature_state import attach_temporal_features


def _matches() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "tourney_id": ["2026-1", "2026-1"],
            "match_num": [1, 2],
            "tourney_date": [20260101, 20260102],
            "winner_id": [1, 2],
            "loser_id": [2, 1],
            "winner_name": ["Alice", "Bob"],
            "loser_name": ["Bob", "Alice"],
            "score": ["6-4 6-4", "6-3 6-3"],
            "surface": ["Hard", "Hard"],
            "round": ["SF", "F"],
            "tourney_level": ["A", "A"],
            "winner_hand": ["R", "L"],
            "loser_hand": ["L", "R"],
        }
    )


def test_old_temporal_snapshot_reports_how_to_rebuild() -> None:
    old_snapshot = pd.DataFrame(
        {"overall_elo_diff": [0.0], "winner": [1], "match_date": ["2026-01-01"]}
    )

    with pytest.raises(ValueError, match="Materialize temporal_training_matches"):
        curated_atp_matches(_matches().assign(**old_snapshot.iloc[0].to_dict()))


def test_curated_matches_keep_attached_features_when_reordered() -> None:
    matches = _matches()
    featured, temporal, _ = attach_temporal_features(matches)

    curated = curated_atp_matches(featured.iloc[::-1]).reset_index(drop=True)

    assert curated["match_num"].tolist() == [2, 1]
    assert curated.loc[0, "overall_elo_diff"] == temporal.loc[1, "overall_elo_diff"]
    assert curated.loc[1, "overall_elo_diff"] == 0.0
    assert curated["surface_Hard"].tolist() == [1.0, 1.0]


def test_curated_matches_reject_missing_temporal_results() -> None:
    matches = _matches()
    featured, _, _ = attach_temporal_features(matches)
    featured.loc[1, "overall_elo_diff"] = float("nan")

    with pytest.raises(ValueError, match="Missing temporal features"):
        curated_atp_matches(featured)


@pytest.mark.parametrize("missing_number", [float("nan"), pd.NA])
def test_missing_match_numbers_survive_temporal_snapshot_and_curated_join(
    missing_number: object, tmp_path
) -> None:
    matches = _matches()
    matches["match_num"] = pd.Series([1, missing_number], dtype="Int64")
    featured, temporal, state = attach_temporal_features(matches)
    snapshot = tmp_path / "temporal.parquet"
    featured.to_parquet(snapshot, index=False)
    restored = pd.read_parquet(snapshot)

    curated = curated_atp_matches(restored)

    assert str(restored["match_num"].dtype) == "Int64"
    assert pd.isna(curated.loc[1, "match_num"])
    assert len(curated) == 2
    assert curated.loc[1, "overall_elo_diff"] == temporal.loc[1, "overall_elo_diff"]
    assert sum(player.wins for player in state.players.values()) == 2


def test_duplicate_keys_and_walkovers_preserve_source_alignment() -> None:
    matches = pd.concat([_matches().iloc[[1]], _matches()], ignore_index=True)
    matches.loc[1, "score"] = "W/O"
    matches.index = [7, 7, 7]

    featured, temporal, state = attach_temporal_features(matches)
    curated = curated_atp_matches(featured)

    assert len(featured) == 3
    assert len(temporal) == len(curated) == 2
    assert pd.isna(featured.loc[1, "overall_elo_diff"])
    assert curated["overall_elo_diff"].tolist() == temporal["overall_elo_diff"].tolist()
    assert sum(player.wins for player in state.players.values()) == 2


def test_feature_chain_preserves_matches_through_anonymization() -> None:
    raw = pd.concat([_matches(), _matches().iloc[[0]]], ignore_index=True)
    raw.loc[2, "tourney_id"] = "other-tournament"
    raw.loc[1, "match_num"] = float("nan")
    raw = raw.assign(draw_size=32, best_of=3)
    for side in ("winner", "loser"):
        for column, value in {
            "ht": 180,
            "age": 25,
            "rank": 20,
            "rank_points": 1000,
            "seed": None,
            "entry": None,
        }.items():
            raw[f"{side}_{column}"] = value
    normalized = normalized_atp_matches(raw)
    featured, _, _ = attach_temporal_features(normalized)
    imputed = imputed_atp_matches(featured)
    winrates = winrate_featured_atp_matches(imputed)
    h2h = h2h_featured_atp_matches(winrates)
    elo = elo_featured_atp_matches(h2h)
    curated = curated_atp_matches(elo)
    final = anonymize(curated, random_seed=42)

    assert all(len(frame) == len(raw) for frame in (featured, imputed, winrates, h2h, elo, final))
    assert list(final.columns) == FINAL_COLUMN_ORDER
    assert final[["player0_elo", "player1_elo", "overall_elo_diff"]].notna().all().all()
