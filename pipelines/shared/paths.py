"""Canonical filesystem layout for the project.

Every path used by the pipelines is declared here so the data flow stays
predictable and there is a single place to change it.

    data/
      raw/
        historical_matches/   annual TennisMyLife CSVs and consolidated input
      staging/                one parquet snapshot per Dagster asset (IO manager)
      curated/                the datasets handed to the ML step:
                              curated_atp_matches.csv and training_atp_matches.csv

Override the root with the TENNIS_DATA_DIR environment variable (see .env.example).
"""

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# `dagster dev` does not read .env by itself, so load it once, here, before any
# module reads an environment variable.
load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = Path(os.getenv("TENNIS_DATA_DIR") or PROJECT_ROOT / "data")

RAW_DIR = DATA_DIR / "raw"
HISTORICAL_MATCHES_RAW_DIR = RAW_DIR / "historical_matches"

STAGING_DIR = DATA_DIR / "staging"
CURATED_DIR = DATA_DIR / "curated"

# Source history and Dagster-produced prediction history.
DEFAULT_HISTORICAL_MATCHES_CSV = HISTORICAL_MATCHES_RAW_DIR / "all_atp_matches.csv"
PLAYER_HISTORY_PATH = STAGING_DIR / "player_history.parquet"
MODELS_DIR = DATA_DIR / "models"

# Published comparison, readable curated and player-oriented training datasets.
CURATED_MATCHES_CSV_NAME = "curated_atp_matches.csv"
TRAINING_MATCHES_CSV_NAME = "training_atp_matches.csv"
PLAYER_COMPARISON_CSV_NAME = "player_comparison_atp_matches.csv"


def write_curated_csv(frame: pd.DataFrame, file_name: str) -> Path:
    """Write `frame` under data/curated as `file_name` and return the path."""
    path = CURATED_DIR / file_name
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path
