"""Last pass before anonymization: encode categoricals, drop leaky statistics."""

import pandas as pd
from sklearn.preprocessing import OneHotEncoder

ROUND_CODES = {"F": 0, "SF": 1, "QF": 2, "R16": 3, "R32": 4, "R64": 5, "R128": 6, "RR": 3}
TOURNEY_LEVEL_CODES = {"D": 0, "A": 1, "M": 2, "G": 3, "F": 4}

# Fixed one-hot schema for the surface, kept in sync with `transforms.anonymization`.
SURFACES = ["Hard", "Clay", "Grass"]
SURFACE_COLUMNS = [f"surface_{surface}" for surface in SURFACES]

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


def remove_wo(df: pd.DataFrame) -> pd.DataFrame:
    """Drop walkovers: no real game was played, so they carry no signal."""
    return df[df["score"] != "W/O"]


def encode_surface(df: pd.DataFrame) -> pd.DataFrame:
    """One-hot encode `surface` into surface_Hard / surface_Clay / surface_Grass.

    The categories are pinned to SURFACES instead of being inferred, so a partial
    dataset (a single week without any grass match, for instance) still produces
    the same three columns and downstream expectations never break.
    """
    # Historical ATP data includes carpet matches. They remain represented as
    # all-zero surface indicators because the model schema intentionally only
    # contains Hard, Clay, and Grass, while future/unseen surfaces must not fail.
    encoder = OneHotEncoder(sparse_output=False, categories=[SURFACES], handle_unknown="ignore")
    encoded = encoder.fit_transform(df[["surface"]])
    encoded_frame = pd.DataFrame(encoded, columns=SURFACE_COLUMNS, index=df.index)
    return df.join(encoded_frame)


def transform_round(df: pd.DataFrame) -> pd.DataFrame:
    """Map the round name to an ordered code (final = 0, R128 = 6)."""
    result = df.copy()
    result["round"] = result["round"].map(ROUND_CODES).fillna(0).astype("int64")
    return result


def transform_tourney_level(df: pd.DataFrame) -> pd.DataFrame:
    """Map the tournament level to an ordered code (ATP=0 .. finals=4)."""
    result = df.copy()
    result["tourney_level"] = (
        result["tourney_level"].map(TOURNEY_LEVEL_CODES).fillna(0).astype("int64")
    )
    return result


def transform_handedness(df: pd.DataFrame) -> pd.DataFrame:
    """Encode handedness: 0 = right handed, 1 = left handed."""
    result = df.copy()
    result["winner_hand"] = result["winner_hand"].map({"R": 0, "L": 1}).fillna(0).astype("int64")
    result["loser_hand"] = result["loser_hand"].map({"R": 0, "L": 1}).fillna(0).astype("int64")
    return result


def remove_stat_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the per-match statistics that would leak the result."""
    return df.drop(columns=[column for column in PER_MATCH_STAT_COLUMNS if column in df.columns])
