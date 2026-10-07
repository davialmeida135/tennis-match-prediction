"""Pre-match Elo ratings and the update formula shared with player history."""

import pandas as pd
import polars as pl

from pipelines.shared.contracts import INITIAL_ELO

from .history import prepare_history


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
    winner_ratings: list[float] = []
    loser_ratings: list[float] = []
    for match in result.itertuples(index=False):
        winner, loser = str(match.winner_id), str(match.loser_id)
        winner_rating = ratings.get(winner, INITIAL_ELO)
        loser_rating = ratings.get(loser, INITIAL_ELO)
        winner_ratings.append(winner_rating)
        loser_ratings.append(loser_rating)
        ratings[winner] = winner_rating + elo_delta(
            winner_rating, loser_rating, 1.0, counts.get(winner, 0)
        )
        ratings[loser] = loser_rating + elo_delta(
            loser_rating, winner_rating, 0.0, counts.get(loser, 0)
        )
        counts[winner] = counts.get(winner, 0) + 1
        counts[loser] = counts.get(loser, 0) + 1
    result["winner_elo"] = pd.Series(winner_ratings, index=result.index, dtype=float)
    result["loser_elo"] = pd.Series(loser_ratings, index=result.index, dtype=float)
    result["elo_diff"] = result["winner_elo"] - result["loser_elo"]
    return result
