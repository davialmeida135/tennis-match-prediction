import math

import pytest

from pipelines.historical_matches.transforms.player_comparison import calculate_player_comparison
from pipelines.shared.contracts import FEATURE_COLUMNS, PlayedMatch, PlayerState


def test_features_use_snapshots_without_mutating_them() -> None:
    player0 = PlayerState(name="Alice")
    player1 = PlayerState(
        name="Bob",
        rating=1600,
        surface_ratings={"Hard": 1550},
        wins=3,
        losses=1,
        h2h_wins={"Alice": 2},
        rank=9,
        rank_points=99,
        age=25,
        serve_points=100,
        aces=10,
        double_faults=5,
        service_points_won=70,
        history=[
            PlayedMatch(tourney_date="2026-01-08", surface="Hard", won=True),
            PlayedMatch(tourney_date="2026-01-01", surface="Clay", won=False),
            PlayedMatch(tourney_date="2025-12-31", surface="Hard", won=True),
        ],
    )
    original = (player0.model_dump(), player1.model_dump())

    features = calculate_player_comparison(player0, player1, "Hard")

    assert tuple(features) == FEATURE_COLUMNS
    assert features == pytest.approx(
        {
            "overall_elo_diff": 100,
            "surface_elo_diff": 50,
            "rank_log_advantage": -math.log(10),
            "points_log_diff": math.log(100),
            "form_10_diff": 2 / 3,
            "surface_form_10_diff": 1,
            "age_diff": 25,
            "h2h_log_odds": math.log(3),
            "experience_log_diff": math.log(5),
            "ace_rate_diff": 0.1,
            "double_fault_rate_diff": 0.05,
            "service_points_won_diff": 0.7,
        }
    )
    reversed_features = calculate_player_comparison(player1, player0, "Hard")
    assert reversed_features == pytest.approx({key: -value for key, value in features.items()})
    assert (player0.model_dump(), player1.model_dump()) == original


def test_unknown_surface_ratings_and_empty_histories_have_neutral_features() -> None:
    features = calculate_player_comparison(
        PlayerState(name="Alice"), PlayerState(name="Bob"), "Clay"
    )
    assert features == dict.fromkeys(FEATURE_COLUMNS, 0.0)
