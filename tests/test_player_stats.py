import pandas as pd

from tennis_match_prediction.transforms.elo import calcular_elo
from tennis_match_prediction.transforms.history import prepare_history
from tennis_match_prediction.transforms.normalization import (
    remove_matches_without_player_ids,
)
from tennis_match_prediction.transforms.player_history import attach_temporal_features


def test_normalization_drops_matches_without_both_player_ids() -> None:
    matches = pd.DataFrame(
        {
            "winner_id": ["A", None, "C", " "],
            "loser_id": ["B", "B", None, "D"],
        }
    )

    normalized = remove_matches_without_player_ids(matches)

    assert normalized["winner_id"].tolist() == ["A"]
    assert normalized["loser_id"].tolist() == ["B"]


def test_h2h_does_not_compare_missing_player_ids() -> None:
    matches = pd.DataFrame(
        {
            "tourney_date": [pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-02")],
            "match_num": [1, 2],
            "winner_id": [None, "A"],
            "loser_id": ["B", "B"],
        }
    )

    _, featured, _ = attach_temporal_features(
        remove_matches_without_player_ids(matches).assign(
            tourney_id="2026-1", winner_name="Alice", loser_name="Bob", surface="Hard"
        )
    )

    assert featured["h2h_log_odds"].tolist() == [0.0]


def test_elo_keeps_rows_with_colliding_or_missing_match_keys() -> None:
    matches = pd.DataFrame(
        {
            "tourney_id": ["one", "two", "three"],
            "tourney_date": [pd.Timestamp("2026-01-01")] * 3,
            "match_num": [1, 1, None],
            "tourney_level": ["D"] * 3,
            "winner_id": ["A"] * 3,
            "loser_id": ["B"] * 3,
            "winner_name": ["Alice"] * 3,
            "loser_name": ["Bob"] * 3,
            "overall_elo_diff": [0.0, 10.0, 20.0],
        }
    )

    featured = calcular_elo(matches)

    pd.testing.assert_frame_equal(featured[matches.columns], prepare_history(matches))
    assert featured[["winner_elo", "loser_elo"]].notna().all().all()
