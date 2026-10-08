"""Pure pre-match feature formulas shared by historical processing and prediction.

Inputs are player snapshots and match context. History updates belong to the player-history transform; these functions never change the supplied snapshots.
"""

import math

from pipelines.shared.contracts import INITIAL_ELO, PlayedMatch, PlayerState


def calculate_player_comparison(
    player0: PlayerState, player1: PlayerState, surface: str
) -> dict[str, float]:
    """Calculate FEATURE_COLUMNS as player1 minus player0 without mutating history."""
    player0_recent = player0.history
    player1_recent = player1.history
    meetings_10 = player1.h2h_wins.get(player0.name, 0)
    meetings_01 = player0.h2h_wins.get(player1.name, 0)
    return {
        "overall_elo_diff": player1.rating - player0.rating,
        "surface_elo_diff": player1.surface_ratings.get(surface, INITIAL_ELO)
        - player0.surface_ratings.get(surface, INITIAL_ELO),
        "rank_log_advantage": _log_value(player0.rank) - _log_value(player1.rank),
        "points_log_diff": _log_value(player1.rank_points) - _log_value(player0.rank_points),
        "form_10_diff": _win_rate(player1_recent[:10]) - _win_rate(player0_recent[:10]),
        "surface_form_10_diff": _win_rate(_on_surface(player1_recent, surface)[:10])
        - _win_rate(_on_surface(player0_recent, surface)[:10]),
        "age_diff": _value(player1.age) - _value(player0.age),
        "h2h_log_odds": math.log((meetings_10 + 1) / (meetings_01 + 1)),
        "experience_log_diff": math.log1p(player1.wins + player1.losses)
        - math.log1p(player0.wins + player0.losses),
        "ace_rate_diff": _rate(player1.aces, player1.serve_points)
        - _rate(player0.aces, player0.serve_points),
        "double_fault_rate_diff": _rate(player1.double_faults, player1.serve_points)
        - _rate(player0.double_faults, player0.serve_points),
        "service_points_won_diff": _rate(player1.service_points_won, player1.serve_points)
        - _rate(player0.service_points_won, player0.serve_points),
    }


def _on_surface(matches: list[PlayedMatch], surface: str) -> list[PlayedMatch]:
    return [item for item in matches if item.surface == surface]


def _win_rate(matches: list[PlayedMatch]) -> float:
    return sum(item.won for item in matches) / len(matches) if matches else 0.0


def _value(value: float | None) -> float:
    return value if value is not None else 0.0


def _log_value(value: float | None) -> float:
    return math.log1p(_value(value))


def _rate(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0
