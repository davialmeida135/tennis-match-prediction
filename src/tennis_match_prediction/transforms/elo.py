"""Overall and surface Elo updates for chronological player history."""

from tennis_match_prediction.contracts.player_history import INITIAL_ELO, PlayerState


def elo_delta(rating: float, opponent_rating: float, result: float, matches: int) -> float:
    """Rating change using pre-match ratings and the player's completed-match count."""
    expected = 1 / (1 + 10 ** ((opponent_rating - rating) / 400))
    k_factor = 250 / ((matches + 5) ** 0.4)
    return k_factor * (result - expected)


def update_match_ratings(
    winner: PlayerState,
    loser: PlayerState,
    surface: str,
    *,
    prior_winner: PlayerState,
    prior_loser: PlayerState,
) -> None:
    """Accumulate overall and surface Elo changes from pre-batch player snapshots.

    Only ratings are mutated. Completed-match counts remain the caller's responsibility.
    Surface Elo uses the player's overall completed-match count, as does overall Elo.
    """
    winner_rating = prior_winner.rating
    loser_rating = prior_loser.rating
    winner_surface_rating = prior_winner.surface_ratings.get(surface, INITIAL_ELO)
    loser_surface_rating = prior_loser.surface_ratings.get(surface, INITIAL_ELO)
    winner.rating += elo_delta(
        winner_rating, loser_rating, 1.0, prior_winner.wins + prior_winner.losses
    )
    loser.rating += elo_delta(
        loser_rating, winner_rating, 0.0, prior_loser.wins + prior_loser.losses
    )
    winner.surface_ratings[surface] = winner.surface_ratings.get(surface, INITIAL_ELO) + elo_delta(
        winner_surface_rating,
        loser_surface_rating,
        1.0,
        prior_winner.wins + prior_winner.losses,
    )
    loser.surface_ratings[surface] = loser.surface_ratings.get(surface, INITIAL_ELO) + elo_delta(
        loser_surface_rating,
        winner_surface_rating,
        0.0,
        prior_loser.wins + prior_loser.losses,
    )
