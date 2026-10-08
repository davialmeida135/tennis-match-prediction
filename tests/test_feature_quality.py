"""Feature quality regressions: partial source records and independent surface windows."""

import pandas as pd
import pytest

from tennis_match_prediction.contracts import FutureMatchRequest, PlayerState
from tennis_match_prediction.ml.feature_quality import coverage_report
from tennis_match_prediction.ml.predict import features_for
from tennis_match_prediction.transforms.player_comparison import calculate_player_comparison
from tennis_match_prediction.transforms.player_history import attach_temporal_features


def match(number, **updates):
    return {
        "tourney_date": 20260101,
        "tourney_id": "2026-1",
        "match_num": number,
        "winner_id": "a",
        "loser_id": "b",
        "winner_name": "Alice",
        "loser_name": "Bob",
        "surface": "Hard",
        "w_svpt": 100,
        "w_ace": 10,
        "w_df": 0,
        "w_1stWon": 50,
        "w_2ndWon": 20,
        **updates,
    }


def test_partial_serve_records_do_not_bias_rates():
    source = pd.DataFrame(
        [
            match(1),
            match(2, w_ace=None, w_df=5, w_2ndWon=None),
            match(3, w_svpt=None, w_ace=100),
            match(4, w_ace=200, w_df=-1, w_1stWon=200),
        ]
    )
    _, _, history = attach_temporal_features(source)
    player = history.players["a"]
    assert player.serve_points == 300
    assert (player.aces, player.ace_serve_points) == (10, 100)
    assert (player.double_faults, player.double_fault_serve_points) == (5, 200)
    assert (player.service_points_won, player.service_won_serve_points) == (70, 100)


def test_unknown_values_are_neutral_and_valid_zero_rates_are_preserved():
    unknown = PlayerState(name="Unknown")
    zero = PlayerState(name="Zero", rank_points=0, aces=0, ace_serve_points=100)
    known = PlayerState(name="Known", age=25, rank_points=100, aces=10, ace_serve_points=100)
    missing = calculate_player_comparison(unknown, known, "Hard")
    observed = calculate_player_comparison(zero, known, "Hard")
    assert missing["age_diff"] == 0
    assert missing["ace_rate_diff"] == 0
    assert observed["ace_rate_diff"] == pytest.approx(0.1)


def test_surface_window_survives_more_than_fifty_other_matches_and_matches_prediction():
    rows = [match(i, surface="Grass") for i in range(1, 13)]
    rows += [match(i) for i in range(13, 70)]
    _, _, history = attach_temporal_features(pd.DataFrame(rows))
    assert len(history.players["a"].history) == 50
    assert len(history.players["a"].surface_history["Grass"]) == 10
    assert all(item.surface == "Hard" for item in history.players["a"].history)
    request = FutureMatchRequest(
        player0_name="Bob", player1_name="Alice", match_date="2026-01-02", surface="Grass"
    )
    expected = features_for(history, request)
    assert expected["surface_form_10_diff"] == 1
    rows.append(match(70, tourney_date=20260102, surface="Grass"))
    _, features, _ = attach_temporal_features(pd.DataFrame(rows))
    assert features.iloc[-1][list(expected)].to_dict() == expected


def test_coverage_separates_missing_invalid_and_valid_zero():
    source = pd.DataFrame(
        [
            match(1, winner_rank=0, winner_rank_points=0, winner_age=-1),
            match(2, winner_rank=None, winner_rank_points=None, winner_age=25, w_ace=None),
        ]
    )
    report = coverage_report(source)
    fields = {row["field"]: row for row in report["coverage"]}
    assert fields["winner_rank"]["invalid"] == 1
    assert fields["winner_rank"]["missing"] == 1
    assert fields["winner_rank_points"]["valid_zero"] == 1
    assert fields["winner_age"]["invalid"] == 1
    assert fields["w_ace_rate"]["available_fraction"] == 0.5
