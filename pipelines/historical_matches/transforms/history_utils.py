"""Lookback helpers shared by the feature engineering modules.

All of them assume the DataFrame is sorted chronologically and only look at
matches that happened strictly before the match being processed. That
restriction is what keeps the features free of future information.
"""

import datetime

import polars as pl


def _before_current_match(frame: pl.DataFrame, date, match_num: int) -> pl.DataFrame:
    """Rows that happened before (date, match_num) in chronological order."""
    return frame.filter(
        (pl.col("tourney_date") < date)
        | ((pl.col("tourney_date") == date) & (pl.col("match_num") < match_num))
    )


def _get_previous_matches(
    frame: pl.DataFrame, winner_id: int, loser_id: int, date, match_num: int
) -> list[pl.DataFrame]:
    """Every earlier match of the winner and of the loser."""
    earlier = _before_current_match(frame, date, match_num)
    winner_matches = earlier.filter(
        (pl.col("winner_id") == winner_id) | (pl.col("loser_id") == winner_id)
    )
    loser_matches = earlier.filter(
        (pl.col("winner_id") == loser_id) | (pl.col("loser_id") == loser_id)
    )
    return [winner_matches, loser_matches]


def _get_previous_encounters(
    frame: pl.DataFrame, player1_id: int, player2_id: int, date: datetime.date, match_num: int
) -> pl.DataFrame:
    """Every earlier match between the two players, oldest first."""
    encounters = frame.filter(
        ((pl.col("winner_id") == player1_id) & (pl.col("loser_id") == player2_id))
        | ((pl.col("winner_id") == player2_id) & (pl.col("loser_id") == player1_id))
    )
    encounters = _before_current_match(encounters, date, match_num)
    return encounters.sort("tourney_date", "match_num")
