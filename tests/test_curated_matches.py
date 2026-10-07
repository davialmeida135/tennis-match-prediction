import pandas as pd
import pytest

from pipelines.historical_matches.assets import curated_atp_matches
from pipelines.historical_matches.feature_state import build_temporal_match_features


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
        curated_atp_matches(_matches(), old_snapshot)


def test_curated_matches_join_temporal_features_by_match_keys() -> None:
    matches = _matches()
    temporal, _ = build_temporal_match_features(matches)

    curated = curated_atp_matches(matches.iloc[::-1], temporal)

    assert curated["match_num"].tolist() == [2, 1]
    assert curated.loc[0, "overall_elo_diff"] == temporal.loc[1, "overall_elo_diff"]
    assert curated.loc[1, "overall_elo_diff"] == 0.0
    assert curated["surface_Hard"].tolist() == [1.0, 1.0]


def test_curated_matches_reject_missing_temporal_results() -> None:
    matches = _matches()
    temporal, _ = build_temporal_match_features(matches)

    with pytest.raises(ValueError, match="did not match every curated result"):
        curated_atp_matches(matches, temporal.iloc[:1])
