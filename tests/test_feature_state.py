from datetime import date

import pandas as pd
import pytest

from pipelines.historical_matches.feature_state import FeatureState, build_training_frame
from pipelines.shared.contracts import FutureMatchRequest


def _match(
    match_num: int,
    winner_id: str,
    winner_name: str,
    loser_id: str,
    loser_name: str,
) -> dict[str, object]:
    return {
        "tourney_date": 20260101,
        "tourney_id": "2026-1",
        "match_num": match_num,
        "winner_id": winner_id,
        "winner_name": winner_name,
        "loser_id": loser_id,
        "loser_name": loser_name,
        "surface": "Hard",
        "score": "6-4 6-4",
        "minutes": 80,
        "winner_rank": 10,
        "loser_rank": 20,
        "winner_rank_points": 3_000,
        "loser_rank_points": 2_000,
        "winner_age": 25,
        "loser_age": 27,
        "w_svpt": 60,
        "w_ace": 8,
        "w_df": 2,
        "w_1stWon": 30,
        "w_2ndWon": 15,
        "l_svpt": 55,
        "l_ace": 4,
        "l_df": 4,
        "l_1stWon": 25,
        "l_2ndWon": 10,
    }


def test_feature_is_captured_before_its_result() -> None:
    state = FeatureState()
    first = pd.Series(_match(1, "a", "Alice", "b", "Bob"))
    state.apply_result(first)
    second = pd.Series(_match(2, "b", "Bob", "a", "Alice"))

    features = state.apply_result(second)

    assert features["overall_elo_diff"] < 0
    assert features["h2h_log_odds"] < 0


def test_checkpoint_continues_incremental_calculation(tmp_path) -> None:
    first = pd.Series(_match(1, "a", "Alice", "b", "Bob"))
    second = pd.Series(_match(2, "b", "Bob", "a", "Alice"))
    uninterrupted = FeatureState()
    uninterrupted.apply_result(first)
    expected = uninterrupted.apply_result(second)

    resumed = FeatureState()
    resumed.apply_result(first)
    checkpoint = tmp_path / "state.json"
    resumed.save(checkpoint)
    actual = FeatureState.load(checkpoint).apply_result(second)

    assert actual == expected


def test_future_prediction_cannot_use_same_day_results() -> None:
    state = FeatureState()
    state.apply_result(pd.Series(_match(1, "a", "Alice", "b", "Bob")))
    request = FutureMatchRequest(
        player0_name="Alice",
        player1_name="Bob",
        match_date=date(2026, 1, 1),
        surface="Hard",
    )

    with pytest.raises(ValueError, match="after the persisted"):
        state.features_for(request)


def test_training_frame_is_balanced_and_has_no_future_features() -> None:
    matches = pd.DataFrame(
        [
            _match(1, "a", "Alice", "b", "Bob"),
            _match(2, "b", "Bob", "a", "Alice"),
        ]
    )

    frame, _ = build_training_frame(matches, random_seed=1)

    assert set(frame["winner"]) == {0, 1}
    assert frame.loc[0, "overall_elo_diff"] == 0


def test_mixed_source_date_formats_are_sorted_chronologically() -> None:
    earlier = _match(1, "a", "Alice", "b", "Bob")
    earlier["tourney_date"] = "2025-12-29"
    later = _match(1, "b", "Bob", "a", "Alice")
    later["tourney_date"] = 20260102

    frame, state = build_training_frame(pd.DataFrame([later, earlier]), random_seed=1)

    assert list(frame["match_date"]) == ["2025-12-29", "2026-01-02"]
    assert state.last_result_date == date(2026, 1, 2)
