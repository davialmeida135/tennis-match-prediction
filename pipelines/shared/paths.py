"""Canonical filesystem layout for the project.

Every path used by the pipelines is declared here so the data flow stays
predictable and there is a single place to change it.

    data/
      raw/
        historical_matches/   input CSVs downloaded from Kaggle (tracked in git)
        live_matches/         JSON/CSV payloads pulled from the SportDevs API
      staging/                one parquet snapshot per Dagster asset (IO manager)
      curated/                publishable datasets (CSV copies uploaded to W&B)

Override the root with the TENNIS_DATA_DIR environment variable (see .env.example).
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# `dagster dev` does not read .env by itself, so load it once, here, before any
# module reads an environment variable.
load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = Path(os.getenv("TENNIS_DATA_DIR") or PROJECT_ROOT / "data")

RAW_DIR = DATA_DIR / "raw"
HISTORICAL_MATCHES_RAW_DIR = RAW_DIR / "historical_matches"
LIVE_MATCHES_RAW_DIR = RAW_DIR / "live_matches"

STAGING_DIR = DATA_DIR / "staging"
CURATED_DIR = DATA_DIR / "curated"

# Default input of the historical pipeline (the smaller, faster dataset).
DEFAULT_HISTORICAL_MATCHES_CSV = HISTORICAL_MATCHES_RAW_DIR / "atp_matches_2023.csv"

# Name of the CSV written inside data/curated before a dataset is published.
CURATED_CSV_NAME = "pre_anonymized_matches.csv"


def ensure_dir(path: Path) -> Path:
    """Create `path` (including parents) when missing and return it."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def staging_snapshot_path(asset_name: str) -> Path:
    """Parquet snapshot written by the IO manager for a given asset."""
    return STAGING_DIR / f"{asset_name}.parquet"


def curated_dataset_path(file_name: str = CURATED_CSV_NAME) -> Path:
    """CSV copy of a dataset that is ready to be published / shared."""
    return CURATED_DIR / file_name
