"""Remove current-match outcome statistics after historical features are calculated."""

import pandas as pd

# Per-match box score of the match being predicted: usable for reporting, but it
# would leak the outcome into a pre-match model, so it is dropped.
PER_MATCH_STAT_COLUMNS = [
    "w_ace",
    "w_df",
    "w_svpt",
    "w_1stIn",
    "w_1stWon",
    "w_2ndWon",
    "w_SvGms",
    "w_bpSaved",
    "w_bpFaced",
    "l_ace",
    "l_df",
    "l_svpt",
    "l_1stIn",
    "l_1stWon",
    "l_2ndWon",
    "l_SvGms",
    "l_bpSaved",
    "l_bpFaced",
]


def remove_match_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the per-match statistics that would leak the result."""
    return df.drop(columns=[column for column in PER_MATCH_STAT_COLUMNS if column in df.columns])
