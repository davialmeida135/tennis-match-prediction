"""Shared calendar-date parsing and deterministic match-history preparation."""

from datetime import date

import pandas as pd

from .imputation import fill_null_surface


def order_history(matches: pd.DataFrame) -> pd.DataFrame:
    """Order by calendar date, tournament and match number, retaining tied source order."""
    result = matches.copy()
    result["tourney_date"] = pd.to_datetime(result["tourney_date"].map(parse_match_date))
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


def parse_match_date(value: object) -> date:
    """Parse the two date encodings used by the source without relying on string order."""
    if isinstance(value, date):
        return value.date() if isinstance(value, pd.Timestamp) else value
    text = str(value)
    # Rolling windows repeatedly read historical dates. Avoid constructing pandas
    # datetime arrays/indexes for each of those scalar ISO calendar dates.
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return date.fromisoformat(text)
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:]))
    date_format = "%Y%m%d" if text.isdigit() else "ISO8601"
    return pd.to_datetime(text, format=date_format).date()
