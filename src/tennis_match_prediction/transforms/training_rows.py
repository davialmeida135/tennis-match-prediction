"""Build player0/player1 training rows from winner/loser match records.

Random player positions prevent the input column roles from revealing the
winner. Player names and IDs are omitted from the output:

* player0 / player1 receive the winner/loser attributes in random order
* `winner` becomes the binary target: 1 when player1 won the match, 0 otherwise
"""

import numpy as np
import pandas as pd
import polars as pl

from tennis_match_prediction.contracts import PLAYER_COMPARISON_FEATURE_COLUMNS

TARGET_COLUMN = "winner"
SIGNED_MATCH_COLUMNS = ["h2h", "elo_diff", *PLAYER_COMPARISON_FEATURE_COLUMNS]

# Match level columns: identical no matter which player is player0 or player1.
MATCH_COLUMNS = [
    "tourney_date",
    "draw_size",
    "tourney_level",
    "week",
    "year",
    "match_num",
    "best_of",
    "round",
    "h2h",
    "elo_diff",
    "surface_Hard",
    "surface_Clay",
    "surface_Grass",
    *PLAYER_COMPARISON_FEATURE_COLUMNS,
]

# Every `winner_<stem>` / `loser_<stem>` pair becomes `player0_<stem>` / `player1_<stem>`.
PLAYER_ATTRIBUTE_STEMS = [
    "hand",
    "ht",
    "age",
    "rank",
    "rank_points",
    "seed_value",
    "seeded",
    "unseeded",
    "qualifier",
    "lucky_loser",
    "special_exempt",
    "alternate",
    "wildcard",
    "protected_ranking",
    "winrate",
    "winrate_last_10",
    "winrate_last_50",
    "winrate_surface",
    "winrate_surface_last_10",
    "winrate_surface_last_50",
    "elo",
]

FINAL_COLUMN_ORDER = [
    "tourney_date",
    "draw_size",
    "tourney_level",
    "week",
    "year",
    "match_num",
    "player0_hand",
    "player0_ht",
    "player0_age",
    "player0_rank",
    "player0_rank_points",
    "player1_hand",
    "player1_ht",
    "player1_age",
    "player1_rank",
    "player1_rank_points",
    "best_of",
    "round",
    "player0_seed_value",
    "player1_seed_value",
    "player0_seeded",
    "player1_seeded",
    "player0_unseeded",
    "player1_unseeded",
    "player0_qualifier",
    "player1_qualifier",
    "player0_lucky_loser",
    "player1_lucky_loser",
    "player0_special_exempt",
    "player1_special_exempt",
    "player0_alternate",
    "player1_alternate",
    "player0_wildcard",
    "player1_wildcard",
    "player0_protected_ranking",
    "player1_protected_ranking",
    "surface_Hard",
    "surface_Clay",
    "surface_Grass",
    "h2h",
    "elo_diff",
    *PLAYER_COMPARISON_FEATURE_COLUMNS,
    "player0_winrate",
    "player1_winrate",
    "player0_winrate_last_10",
    "player1_winrate_last_10",
    "player0_winrate_last_50",
    "player1_winrate_last_50",
    "player0_winrate_surface",
    "player1_winrate_surface",
    "player0_winrate_surface_last_10",
    "player1_winrate_surface_last_10",
    "player0_winrate_surface_last_50",
    "player1_winrate_surface_last_50",
    "player0_elo",
    "player1_elo",
    TARGET_COLUMN,
]


def build_training_rows(df: pd.DataFrame, *, random_seed: int | None = None) -> pd.DataFrame:
    """Shuffle winner/loser into player0/player1 and add the binary target column.

    Args:
        df: Match level dataset with `winner_*` / `loser_*` columns.
        random_seed: Optional seed, useful to make tests reproducible.

    Returns:
        A new pandas DataFrame with the player0_/player1_ columns and the target.
    """
    df = df.copy()
    df["tourney_date"] = pd.to_datetime(df["tourney_date"]).dt.strftime("%Y-%m-%d")
    frame = df if isinstance(df, pl.DataFrame) else pl.from_pandas(df)

    required_columns = _required_input_columns()
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Input DataFrame is missing required columns: {missing_columns}")

    generator = np.random.default_rng(random_seed)
    frame = frame.with_columns(swap=pl.Series(generator.integers(0, 2, size=frame.height)))

    player1_is_winner = pl.col("swap") == 0
    expressions = [
        pl.when(player1_is_winner)
        .then(pl.col(f"loser_{stem}"))
        .otherwise(pl.col(f"winner_{stem}"))
        .alias(f"player0_{stem}")
        for stem in PLAYER_ATTRIBUTE_STEMS
    ]
    expressions += [
        pl.when(player1_is_winner)
        .then(pl.col(f"winner_{stem}"))
        .otherwise(pl.col(f"loser_{stem}"))
        .alias(f"player1_{stem}")
        for stem in PLAYER_ATTRIBUTE_STEMS
    ]
    # These features are stored from winner-minus-loser perspective. Reorient
    # them to player1-minus-player0 using the same random swap as the target.
    expressions += [
        pl.when(player1_is_winner).then(pl.col(column)).otherwise(-pl.col(column)).alias(column)
        for column in SIGNED_MATCH_COLUMNS
    ]
    expressions.append(pl.when(player1_is_winner).then(1).otherwise(0).alias(TARGET_COLUMN))

    training_rows = frame.with_columns(expressions).select(FINAL_COLUMN_ORDER)
    return training_rows.to_pandas()


def _required_input_columns() -> list[str]:
    columns = list(MATCH_COLUMNS)
    for stem in PLAYER_ATTRIBUTE_STEMS:
        columns.extend([f"winner_{stem}", f"loser_{stem}"])
    return columns
