"""Canonical filesystem layout for the project.

Every path used by the pipelines is declared here so the data flow stays
predictable and there is a single place to change it.

    data/
      raw/
        historical_matches/   annual TennisMyLife CSVs and consolidated input
      staging/                one parquet snapshot per Dagster asset (IO manager)
      curated/                the datasets handed to the ML step:
                              pre_anonymized_matches.csv and anonymized_matches.csv

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

# Annual source files and durable state produced by the incremental feature engine.
DEFAULT_HISTORICAL_MATCHES_CSV = HISTORICAL_MATCHES_RAW_DIR / "all_atp_matches.csv"
FEATURE_STATE_DIR = DATA_DIR / "state"
FEATURE_STATE_PATH = FEATURE_STATE_DIR / "feature_state.json"
MODELS_DIR = DATA_DIR / "models"

# The two datasets written under data/curated, in the order the pipeline builds
# them: the curated dataset still names the winner and loser, the anonymized one
# is what a model is trained on.
PRE_ANONYMIZED_CSV_NAME = "pre_anonymized_matches.csv"
ANONYMIZED_CSV_NAME = "anonymized_matches.csv"
TEMPORAL_FEATURES_CSV_NAME = "temporal_features.csv"


def write_curated_csv(frame: pd.DataFrame, file_name: str) -> Path:
    """Write `frame` under data/curated as `file_name` and return the path."""
    path = CURATED_DIR / file_name
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path
