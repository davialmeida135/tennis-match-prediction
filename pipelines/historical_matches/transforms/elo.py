"""Pre-match Elo ratings and the update formula shared with player history."""

import pandas as pd
import polars as pl

from pipelines.shared.contracts import INITIAL_ELO

from .history import history_batches, prepare_history


def elo_delta(rating: float, opponent_rating: float, result: float, matches: int) -> float:
    """Rating change using pre-match ratings and the player's completed-match count."""
    expected = 1 / (1 + 10 ** ((opponent_rating - rating) / 400))
    k_factor = 250 / ((matches + 5) ** 0.4)
    return k_factor * (result - expected)


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
    result["elo_diff"] = result["winner_elo"] - result["loser_elo"]
    return result
