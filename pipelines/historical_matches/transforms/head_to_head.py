"""Head-to-head history using source dates and tournament match numbers."""

from collections import Counter

import pandas as pd
import polars as pl

from pipelines.historical_matches.transforms.history import history_batches, prepare_history


def calcular_h2h(df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame:
    """Attach winner-minus-loser wins before each unambiguous history batch."""
    result = prepare_history(df.to_pandas() if isinstance(df, pl.DataFrame) else df)
    wins: Counter[tuple[str, str]] = Counter()
    values = [0] * len(result)
    for batch in history_batches(result):
        outcomes = []
        for index, match in batch.iterrows():
            winner, loser = str(match["winner_id"]), str(match["loser_id"])
            values[index] = wins[winner, loser] - wins[loser, winner]
            outcomes.append((winner, loser))
        for winner, loser in outcomes:
            wins[winner, loser] += 1
    result["h2h"] = pd.Series(values, index=result.index, dtype="int64")
    return result
