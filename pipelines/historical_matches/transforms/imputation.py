"""Filling of missing player/match attributes.

Every column is filled with a conservative value so the model never sees a null:

* surface -> "Hard" (the ATP majority surface)
* height / age -> global mean
* rank    -> worst rank in the dataset (an unranked player played no qualifiers)
* rank_points -> lowest value in the dataset
"""

import pandas as pd


def fill_null_surface(df: pd.DataFrame) -> pd.DataFrame:
    """Missing surfaces are treated as hard courts."""
    result = df.copy()
    result["surface"] = result["surface"].fillna("Hard")
    return result


def fill_null_height(df: pd.DataFrame) -> pd.DataFrame:
    """Missing heights are filled with the mean height of winners/losers."""
    result = df.copy()
    for side in ("winner", "loser"):
        result[f"{side}_ht"] = result[f"{side}_ht"].fillna(result[f"{side}_ht"].mean())
    return result


def fill_null_age(df: pd.DataFrame) -> pd.DataFrame:
    """Missing ages are filled with the mean age of winners/losers."""
    result = df.copy()
    for side in ("winner", "loser"):
        result[f"{side}_age"] = result[f"{side}_age"].fillna(result[f"{side}_age"].mean())
    return result


def fill_null_rank(df: pd.DataFrame) -> pd.DataFrame:
    """Missing ranks become the worst observed rank, missing points the lowest total."""
    result = df.copy()
    for side in ("winner", "loser"):
        result[f"{side}_rank"] = result[f"{side}_rank"].fillna(result[f"{side}_rank"].max())
        result[f"{side}_rank_points"] = result[f"{side}_rank_points"].fillna(
            result[f"{side}_rank_points"].min()
        )
    return result
