"""Clean match values: surfaces, source dates and unusable-row filtering."""

from datetime import date

import pandas as pd


def remove_wo(df: pd.DataFrame) -> pd.DataFrame:
    """Drop walkovers: no real game was played, so they carry no signal."""
    return df.loc[df["score"].ne("W/O").fillna(True)].copy()


def remove_matches_without_player_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Drop source rows that cannot identify both participants for history features."""
    required = ["winner_id", "loser_id"]
    missing_columns = [column for column in required if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Match data is missing player ID columns: {missing_columns}")
    valid_ids = df[required].notna().all(axis=1)
    valid_ids &= df["winner_id"].astype("string").str.strip().ne("")
    valid_ids &= df["loser_id"].astype("string").str.strip().ne("")
    return df.loc[valid_ids].copy()


def preprocess_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse the source date without generating unused calendar features."""
    result = df.copy()
    result["tourney_date"] = pd.to_datetime(result["tourney_date"].map(parse_source_date))
    return result


def fill_null_surface(df: pd.DataFrame) -> pd.DataFrame:
    """Treat missing/blank surfaces as Hard and normalize source casing."""
    result = df.copy()
    surface = result["surface"].astype("string").str.strip().str.title()
    result["surface"] = surface.replace("", pd.NA).fillna("Hard")
    return result


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
