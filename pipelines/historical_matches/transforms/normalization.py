"""First normalization pass: dates, chronological ordering and seed parsing."""

import pandas as pd

WINNER_ENTRY_METHODS = {
    "S": "winner_seeded",
    "US": "winner_unseeded",
    "WC": "winner_wildcard",
    "Q": "winner_qualifier",
    "LL": "winner_lucky_loser",
    "PR": "winner_protected_ranking",
    "SE": "winner_special_exempt",
    "ALT": "winner_alternate",
}

LOSER_ENTRY_METHODS = {
    "S": "loser_seeded",
    "US": "loser_unseeded",
    "WC": "loser_wildcard",
    "Q": "loser_qualifier",
    "LL": "loser_lucky_loser",
    "PR": "loser_protected_ranking",
    "SE": "loser_special_exempt",
    "ALT": "loser_alternate",
}

ENTRY_METHOD_FLAGS = tuple(dict.fromkeys(list(WINNER_ENTRY_METHODS.values()) + list(LOSER_ENTRY_METHODS.values())))


def preprocess_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Convert the integer `tourney_date` (YYYYMMDD) into a datetime plus week/year."""
    result = df.copy()
    result["tourney_date"] = pd.to_datetime(result["tourney_date"].astype(str), format="%Y%m%d")
    result["week"] = result["tourney_date"].dt.isocalendar().week.astype("int64")
    result["year"] = result["tourney_date"].dt.isocalendar().year.astype("int64")
    return result


def sort_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """Sort chronologically; every lookback feature depends on this ordering."""
    return df.sort_values(by=["tourney_date", "tourney_id", "match_num"]).reset_index(drop=True)


def transform_seed_data(df: pd.DataFrame) -> pd.DataFrame:
    """Turn `winner_seed`/`loser_seed` + `*_entry` into numeric seeds and boolean flags."""
    result = df.copy()

    result["winner_seed_value"] = pd.to_numeric(result["winner_seed"], errors="coerce")
    result["loser_seed_value"] = pd.to_numeric(result["loser_seed"], errors="coerce")
    for flag in ENTRY_METHOD_FLAGS:
        result[flag] = False

    for side in ("winner", "loser"):
        seed_value_column = f"{side}_seed_value"
        entry_column = f"{side}_entry"
        entry_methods = WINNER_ENTRY_METHODS if side == "winner" else LOSER_ENTRY_METHODS

        for index, row in result.iterrows():
            seed = row[seed_value_column]
            if str(seed)[0].isdigit():
                result.at[index, f"{side}_seeded"] = True
                continue

            entry_code = row[entry_column]
            if pd.isna(entry_code):
                result.at[index, f"{side}_unseeded"] = True
            else:
                entry_flag = entry_methods.get(str(entry_code).upper())
                if entry_flag:
                    result.at[index, entry_flag] = True
            # Players without a seed are ranked after everyone else in the draw.
            result.at[index, seed_value_column] = row["draw_size"]

    result = result.drop(columns=["winner_seed", "loser_seed", "winner_entry", "loser_entry"])
    result["winner_seed_value"] = pd.to_numeric(result["winner_seed_value"], errors="coerce").astype("Int64")
    result["loser_seed_value"] = pd.to_numeric(result["loser_seed_value"], errors="coerce").astype("Int64")
    return result
