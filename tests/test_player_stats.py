import pandas as pd

from pipelines.historical_matches.transforms.normalization import (
    remove_matches_without_player_ids,
)
from pipelines.historical_matches.transforms.player_stats import calcular_h2h


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

    featured = calcular_h2h(matches)

    assert featured["h2h"].tolist() == [0, 0]
