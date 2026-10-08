from datetime import date

import pandas as pd
import pytest

from tennis_match_prediction.contracts import FutureMatchRequest, PlayerHistory
from tennis_match_prediction.ml.predict import features_for
from tennis_match_prediction.transforms.history import (
    parse_source_date,
)
from tennis_match_prediction.transforms.player_history import (
    attach_temporal_features,
)


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


def test_historical_and_future_features_agree_and_exclude_current_result():
    first = _match(1, "a", "Alice", "b", "Bob")
    second = _match(2, "b", "Bob", "a", "Alice")
    second["tourney_date"] = 20260102
    _, _, history = attach_temporal_features(pd.DataFrame([first]))
    request = FutureMatchRequest(
        player0_name="Alice", player1_name="Bob", match_date="2026-01-02", surface="Hard"
    )
    expected = features_for(history, request)
    _, features, _ = attach_temporal_features(pd.DataFrame([first, second]))
    assert features.loc[1, list(expected)].to_dict() == expected
    assert expected["overall_elo_diff"] < 0
    assert features.loc[0, "overall_elo_diff"] == 0


def test_player_history_parquet_round_trip(tmp_path):
    _, _, history = attach_temporal_features(pd.DataFrame([_match(1, "a", "Alice", "b", "Bob")]))
    path = tmp_path / "player_history.parquet"
    pd.DataFrame({"history": [history.model_dump_json()]}).to_parquet(path)
    loaded = PlayerHistory.model_validate_json(pd.read_parquet(path).loc[0, "history"])
    assert loaded == history
    request = FutureMatchRequest(
        player0_name="Alice", player1_name="Bob", match_date="2026-01-01", surface="Hard"
    )
    with pytest.raises(ValueError, match="after the persisted"):
        features_for(loaded, request)


@pytest.mark.parametrize(("field", "value"), [("wins", -1), ("rating", float("inf")), ("rank", 0)])
def test_player_history_rejects_invalid_values(field, value):
    _, _, history = attach_temporal_features(pd.DataFrame([_match(1, "a", "Alice", "b", "Bob")]))
    payload = history.model_dump()
    payload["players"]["a"][field] = value
    with pytest.raises(ValueError):
        PlayerHistory.model_validate(payload)


def test_player_history_rejects_future_results():
    _, _, history = attach_temporal_features(pd.DataFrame([_match(1, "a", "Alice", "b", "Bob")]))
    payload = history.model_dump()
    payload["players"]["a"]["history"][0]["tourney_date"] = "2026-01-02"
    with pytest.raises(ValueError, match="cutoff"):
        PlayerHistory.model_validate(payload)


@pytest.mark.parametrize("invalid", [-13.689, float("inf"), float("-inf"), float("nan")])
def test_invalid_source_attributes_preserve_valid_prior_history(invalid):
    first = _match(1, "a", "Alice", "b", "Bob")
    second = _match(2, "a", "Alice", "b", "Bob")
    second["tourney_date"] = 20260102
    for side in ("winner", "loser"):
        for attribute in ("age", "rank", "rank_points"):
            second[f"{side}_{attribute}"] = invalid
    _, _, history = attach_temporal_features(pd.DataFrame([first, second]))
    assert history.players["a"].age == 25
    assert history.players["b"].age == 27
    assert history.players["a"].rank == 10
    assert history.players["a"].rank_points == 3000


def test_negative_initial_age_is_unknown_and_zero_points_are_valid():
    first = _match(1, "a", "Alice", "b", "Bob")
    first["winner_age"] = -13.689
    second = _match(2, "a", "Alice", "b", "Bob")
    second["tourney_date"] = 20260102
    second["winner_age"] = -5.013
    second["winner_rank"] = 0
    second["winner_rank_points"] = 0
    _, _, history = attach_temporal_features(pd.DataFrame([first, second]))
    assert history.players["a"].age is None
    assert history.players["a"].rank == 10
    assert history.players["a"].rank_points == 0


@pytest.mark.parametrize(
    "value",
    [
        "2024-02-29",
        "20240229",
        20240229,
        date(2024, 2, 29),
        pd.Timestamp("2024-02-29T12:00:00"),
        "2024-02-29T12:00:00",
    ],
)
def test_match_date_formats_preserve_calendar_date(value: object) -> None:
    assert parse_source_date(value) == date(2024, 2, 29)


@pytest.mark.parametrize("value", ["2023-02-29", "20230229", "20241301", "2024-00-01"])
def test_match_date_rejects_invalid_calendar_dates(value: object) -> None:
    with pytest.raises(ValueError):
        parse_source_date(value)


def test_training_rejects_raw_history_and_unordered_rows(tmp_path):
    from tennis_match_prediction.contracts import PLAYER_COMPARISON_FEATURE_COLUMNS
    from tennis_match_prediction.ml.train import train

    path = tmp_path / "training.csv"
    pd.DataFrame([_match(1, "a", "Alice", "b", "Bob")]).to_csv(path, index=False)
    with pytest.raises(ValueError, match="Expected Dagster training rows"):
        train(
            path,
            tmp_path / "model.pkl",
            train_start=date(2026, 1, 1),
            validation_start=date(2026, 2, 1),
            test_start=date(2026, 3, 1),
        )
    frame = pd.DataFrame({column: [0.0, 0.0] for column in PLAYER_COMPARISON_FEATURE_COLUMNS})
    frame["winner"] = [0, 1]
    frame["tourney_date"] = ["2026-01-02", "2026-01-01"]
    frame.to_csv(path, index=False)
    with pytest.raises(ValueError, match="chronological"):
        train(
            path,
            tmp_path / "model.pkl",
            train_start=date(2026, 1, 1),
            validation_start=date(2026, 2, 1),
            test_start=date(2026, 3, 1),
        )
