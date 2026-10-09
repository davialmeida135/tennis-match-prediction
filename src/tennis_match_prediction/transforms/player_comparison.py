"""Pre-match formulas shared by historical processing and future prediction."""

import math

from tennis_match_prediction.contracts import INITIAL_ELO, PlayedMatch, PlayerState


def calculate_player_comparison(
    player0: PlayerState, player1: PlayerState, surface: str
) -> dict[str, float]:
    """Calculate player1-minus-player0 differences without mutating history."""
    return {
        "overall_elo_diff": player1.rating - player0.rating,
        "surface_elo_diff": player1.surface_ratings.get(surface, INITIAL_ELO)
        - player0.surface_ratings.get(surface, INITIAL_ELO),
        "rank_log_advantage": -_difference(player0.rank, player1.rank, logarithmic=True),
        "points_log_diff": _difference(player0.rank_points, player1.rank_points, logarithmic=True),
        "form_10_diff": _difference(
            _win_rate(player0.history[:10]), _win_rate(player1.history[:10])
        ),
        "surface_form_10_diff": _difference(
            _win_rate(player0.surface_history.get(surface, [])),
            _win_rate(player1.surface_history.get(surface, [])),
        ),
        "age_diff": _difference(player0.age, player1.age),
        "h2h_log_odds": math.log(
            (player1.h2h_wins.get(player0.name, 0) + 1)
            / (player0.h2h_wins.get(player1.name, 0) + 1)
        ),
        "experience_log_diff": math.log1p(player1.wins + player1.losses)
        - math.log1p(player0.wins + player0.losses),
        "ace_rate_diff": _difference(
            _rate(player0.aces, player0.ace_serve_points),
            _rate(player1.aces, player1.ace_serve_points),
        ),
        "double_fault_rate_diff": _difference(
            _rate(player0.double_faults, player0.double_fault_serve_points),
            _rate(player1.double_faults, player1.double_fault_serve_points),
        ),
        "service_points_won_diff": _difference(
            _rate(player0.service_points_won, player0.service_won_serve_points),
            _rate(player1.service_points_won, player1.service_won_serve_points),
        ),
    }


def _difference(left: float | None, right: float | None, *, logarithmic: bool = False) -> float:
    if left is None or right is None:
        return 0.0
    return math.log1p(right) - math.log1p(left) if logarithmic else right - left


def _win_rate(matches: list[PlayedMatch]) -> float | None:
    return sum(item.won for item in matches) / len(matches) if matches else None


def _rate(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator > 0 else None
