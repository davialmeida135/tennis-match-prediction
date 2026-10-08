"""Shared calendar-date parsing and deterministic match-history preparation."""

from collections.abc import Iterator
from datetime import date

import pandas as pd

from tennis_match_prediction.transforms.imputation import (
    fill_null_surface,
)


def order_history(matches: pd.DataFrame) -> pd.DataFrame:
    """Order by calendar date, tournament and match number, retaining tied source order."""
    result = matches.copy()
    result["tourney_date"] = pd.to_datetime(result["tourney_date"].map(parse_source_date))
    if "match_num" in result:
        result["match_num"] = pd.to_numeric(result["match_num"], errors="raise").astype("Int64")
    columns = [column for column in ("tourney_date", "tourney_id", "match_num") if column in result]
    return result.sort_values(columns, kind="stable", na_position="last")


def prepare_history(matches: pd.DataFrame) -> pd.DataFrame:
    """Exclude unplayed/unidentifiable matches and clean surface before any lookback."""
    result = matches.copy()
    if "score" in result:
        result = result.loc[result["score"].ne("W/O").fillna(True)]
    valid = result[["winner_id", "loser_id"]].notna().all(axis=1)
    for column in ("winner_id", "loser_id"):
        valid &= result[column].astype("string").str.strip().ne("").fillna(False)
    result = result.loc[valid]
    if "surface" in result:
        result = fill_null_surface(result)
    return order_history(result).reset_index(drop=True)


def history_batch_ids(matches: pd.DataFrame) -> pd.Series:
    """Use match numbers within a tournament; freeze history when order is ambiguous.

    Dates are source dates, which can represent a tournament's start. Tournament
    IDs never establish chronology between events involving the same player.
    """
    batches = pd.Series(range(len(matches)), index=matches.index, dtype="int64")
    if "tourney_id" not in matches:
        return batches.groupby(matches["tourney_date"], sort=False).transform("min")
    keys = [matches["tourney_date"], matches["tourney_id"]]
    ambiguous = matches["match_num"].isna() | matches["match_num"].le(0)
    ambiguous |= matches.duplicated(["tourney_date", "tourney_id", "match_num"], keep=False)
    ambiguous = ambiguous.groupby(keys, dropna=False).transform("any")
    batches = batches.where(~ambiguous, batches.groupby(keys, dropna=False).transform("min"))
    participants = pd.concat(
        [
            matches[["tourney_date", "tourney_id", f"{side}_id"]].rename(
                columns={f"{side}_id": "player_id"}
            )
            for side in ("winner", "loser")
        ],
        ignore_index=True,
    )
    shared = participants.groupby(["tourney_date", "player_id"])["tourney_id"].nunique().gt(1)
    ambiguous_dates = shared[shared].index.get_level_values("tourney_date")
    return batches.where(
        ~matches["tourney_date"].isin(ambiguous_dates),
        batches.groupby(matches["tourney_date"], sort=False).transform("min"),
    )


def history_batches(matches: pd.DataFrame) -> Iterator[pd.DataFrame]:
    """Iterate ordered rows sharing the same prior-history snapshot."""
    for _, batch in matches.groupby(history_batch_ids(matches), sort=False):
        yield batch


def parse_source_date(value: object) -> date:
    """Parse the two date encodings used by the source without relying on string order."""
    if isinstance(value, date):
        return value.date() if isinstance(value, pd.Timestamp) else value
    text = str(value)
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return date.fromisoformat(text)
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:]))
    date_format = "%Y%m%d" if text.isdigit() else "ISO8601"
    return pd.to_datetime(text, format=date_format).date()
