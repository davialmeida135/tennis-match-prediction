"""Pre-match Elo ratings and the update formula shared with player history."""

import pandas as pd
import polars as pl

from tennis_match_prediction.contracts import INITIAL_ELO, PlayerState
from tennis_match_prediction.transforms.history import (
    history_batches,
    prepare_history,
)


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


def calcular_elo(df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame:
    """Attach pre-match ratings, updating both players from the same prior ratings."""
    source = df.to_pandas() if isinstance(df, pl.DataFrame) else df
    result = prepare_history(source)
    ratings: dict[str, float] = {}
    counts: dict[str, int] = {}
    winner_ratings = [INITIAL_ELO] * len(result)
    loser_ratings = [INITIAL_ELO] * len(result)
    for batch in history_batches(result):
        updates: dict[str, float] = {}
        played: dict[str, int] = {}
        for index, match in batch.iterrows():
            winner, loser = str(match["winner_id"]), str(match["loser_id"])
            winner_rating = ratings.get(winner, INITIAL_ELO)
            loser_rating = ratings.get(loser, INITIAL_ELO)
            winner_ratings[index] = winner_rating
            loser_ratings[index] = loser_rating
            for player, rating, opponent, outcome in (
                (winner, winner_rating, loser_rating, 1.0),
                (loser, loser_rating, winner_rating, 0.0),
            ):
                updates[player] = updates.get(player, 0.0) + elo_delta(
                    rating, opponent, outcome, counts.get(player, 0)
                )
                played[player] = played.get(player, 0) + 1
        for player, delta in updates.items():
            ratings[player] = ratings.get(player, INITIAL_ELO) + delta
            counts[player] = counts.get(player, 0) + played[player]
    result["winner_elo"] = winner_ratings
    result["loser_elo"] = loser_ratings
    return result
