"""Causal imputation of chronological match rows using only earlier observations.

Both player sides share a reference distribution. Fixed initial defaults keep
empty history and entirely missing columns usable without consulting future rows.
Surface cleanup is deterministic and runs before history features.
"""

import pandas as pd

DEFAULT_HEIGHT = 180.0
DEFAULT_AGE = 25.0
DEFAULT_RANK = 2000.0
DEFAULT_RANK_POINTS = 0.0


def fill_null_surface(df: pd.DataFrame) -> pd.DataFrame:
    """Treat missing/blank surfaces as Hard and normalize source casing."""
    result = df.copy()
    surface = result["surface"].astype("string").str.strip().str.title()
    result["surface"] = surface.replace("", pd.NA).fillna("Hard")
    return result


def _fill_from_prior_rows(
    df: pd.DataFrame, stem: str, statistic: str, default: float
) -> pd.DataFrame:
    """Fill both sides using observed values strictly before the current row."""
    result = df.copy()
    columns = [f"{side}_{stem}" for side in ("winner", "loser")]
    observed = result[columns].apply(pd.to_numeric, errors="coerce")
    if statistic == "mean":
        sums = observed.sum(axis=1).cumsum().shift(1, fill_value=0)
        counts = observed.notna().sum(axis=1).cumsum().shift(1, fill_value=0)
        prior = sums / counts.where(counts > 0)
    elif statistic == "max":
        prior = observed.max(axis=1).cummax().ffill().shift(1)
    elif statistic == "min":
        prior = observed.min(axis=1).cummin().ffill().shift(1)
    else:
        raise ValueError(f"Unsupported imputation statistic: {statistic}")
    prior = prior.fillna(default)
    for column in columns:
        result[column] = observed[column].fillna(prior)
    return result


def fill_null_height(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing height from the pooled mean of previously observed heights."""
    return _fill_from_prior_rows(df, "ht", "mean", DEFAULT_HEIGHT)


def fill_null_age(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing age from the pooled mean of previously observed ages."""
    return _fill_from_prior_rows(df, "age", "mean", DEFAULT_AGE)


def fill_null_rank(df: pd.DataFrame) -> pd.DataFrame:
    """Use the worst prior rank and lowest prior points, with fixed initial defaults."""
    result = _fill_from_prior_rows(df, "rank", "max", DEFAULT_RANK)
    return _fill_from_prior_rows(result, "rank_points", "min", DEFAULT_RANK_POINTS)
