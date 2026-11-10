"""Orient pre-match comparisons and the target with reproducible player positions."""

import numpy as np
import pandas as pd
import polars as pl

from tennis_match_prediction.contracts.features import PLAYER_COMPARISON_FEATURE_COLUMNS

TARGET_COLUMN = "winner"
MATCH_COLUMNS = ["tourney_date", *PLAYER_COMPARISON_FEATURE_COLUMNS]
FINAL_COLUMN_ORDER = [*MATCH_COLUMNS, TARGET_COLUMN]


def build_training_rows(df: pd.DataFrame, *, random_seed: int | None = None) -> pd.DataFrame:
    """Randomize player positions without exposing winner/loser identities.

    Input comparisons use winner-minus-loser orientation. The same random swap
    changes their signs and sets the target to 1 when player1 is the winner.
    Only the shared comparison features, source date and target are published.
    """
    missing = [column for column in MATCH_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Input DataFrame is missing required columns: {missing}")
    source = df.loc[:, MATCH_COLUMNS].copy()
    source["tourney_date"] = pd.to_datetime(source["tourney_date"]).dt.strftime("%Y-%m-%d")
    frame = pl.from_pandas(source)
    generator = np.random.default_rng(random_seed)
    frame = frame.with_columns(swap=pl.Series(generator.integers(0, 2, size=frame.height)))
    player1_is_winner = pl.col("swap") == 0
    expressions = [
        pl.when(player1_is_winner).then(pl.col(column)).otherwise(-pl.col(column)).alias(column)
        for column in PLAYER_COMPARISON_FEATURE_COLUMNS
    ]
    expressions.append(pl.when(player1_is_winner).then(1).otherwise(0).alias(TARGET_COLUMN))
    return frame.with_columns(expressions).select(FINAL_COLUMN_ORDER).to_pandas()
