"""Win rates using the same source-date/match-number order as Elo and H2H."""

import pandas as pd
import polars as pl

from pipelines.historical_matches.transforms.history import history_batch_ids, prepare_history


def _attach_winrates(
    df: pd.DataFrame | pl.DataFrame, *, n: int | None, by_surface: bool
) -> pd.DataFrame:
    """Read the prior batch's results, then append outcomes for the next batch."""
    if n is not None and n < 1:
        raise ValueError("Win-rate window must be positive")
    result = prepare_history(df.to_pandas() if isinstance(df, pl.DataFrame) else df)
    suffix = "_surface" if by_surface else ""
    if n is not None:
        suffix += f"_last_{n}"
    batches = history_batch_ids(result)
    long = pl.from_pandas(
        pd.concat(
            [
                pd.DataFrame(
                    {
                        "row": result.index,
                        "batch": batches,
                        "player": result[f"{side}_id"].astype(str),
                        "surface": result["surface"] if by_surface else "",
                        "won": won,
                    }
                )
                for side, won in (("winner", 1), ("loser", 0))
            ],
            ignore_index=True,
        )
    ).sort("row")
    groups = ["player", "surface"]
    previous = pl.col("won").shift(1)
    rate = (
        previous.cum_sum() / (pl.col("won").cum_count() - 1)
        if n is None
        else previous.rolling_mean(n, min_samples=1)
    )
    long = long.with_columns(rate.over(groups).fill_null(0.0).alias("rate"))
    long = long.with_columns(pl.col("rate").first().over(["batch", *groups]).alias("rate"))
    for side, won in (("winner", 1), ("loser", 0)):
        result[f"{side}_winrate{suffix}"] = long.filter(pl.col("won") == won)["rate"].to_numpy()
    return result


def calcular_winrate_total(df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame:
    return _attach_winrates(df, n=None, by_surface=False)


def calcular_winrate_ultimas_n(df: pd.DataFrame | pl.DataFrame, n: int = 50) -> pd.DataFrame:
    return _attach_winrates(df, n=n, by_surface=False)


def calcular_winrate_superficie(df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame:
    return _attach_winrates(df, n=None, by_surface=True)


def calcular_winrate_superficie_ultimas_n(
    df: pd.DataFrame | pl.DataFrame, n: int = 50
) -> pd.DataFrame:
    return _attach_winrates(df, n=n, by_surface=True)
