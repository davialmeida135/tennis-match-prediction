"""Load published features and preserve source-date boundaries."""

from dataclasses import dataclass
from hashlib import file_digest

import pandas as pd
from pydantic import JsonValue

from tennis_match_prediction.contracts import ExperimentConfig
from tennis_match_prediction.ml.config import TRAINING_MATCHES


@dataclass
class PreparedDataset:
    periods: dict[str, pd.DataFrame]
    split: dict[str, JsonValue]
    sha256: str


def prepare_dataset(config: ExperimentConfig) -> PreparedDataset:
    columns = (*config.feature_columns, "winner", "tourney_date")
    # Hash the same open input used by pandas, before selecting feature columns.
    with config.training_matches_path.open("rb") as source:
        digest = file_digest(source, "sha256").hexdigest()
        source.seek(0)
        matches = pd.read_csv(source, low_memory=False)
    missing = set(columns).difference(matches.columns)
    if missing:
        raise ValueError(
            "Expected Dagster training rows. Materialize materialize_historical_dataset first. "
            f"Missing columns: {sorted(missing)}"
        )
    frame = matches.loc[:, list(columns)]
    TRAINING_MATCHES.model_copy(
        update={"required": columns, "numeric": config.feature_columns}
    ).validate_frame(frame)

    dates = pd.to_datetime(frame["tourney_date"], format="ISO8601").dt.date
    periods = {
        "warmup": frame.loc[dates < config.train_start],
        "training": frame.loc[
            (dates >= config.train_start) & (dates < config.validation_start)
        ],
        "validation": frame.loc[
            (dates >= config.validation_start) & (dates < config.test_start)
        ],
        "test": frame.loc[dates >= config.test_start],
    }
    for name, period in periods.items():
        if period.empty:
            raise ValueError(
                f"The {name} period contains no matches; adjust the split dates"
            )
    if periods["training"]["winner"].nunique() != 2:
        raise ValueError("The training period must contain both target classes")
    split = {
        "date_column": "tourney_date",
        "train_start": config.train_start.isoformat(),
        "validation_start": config.validation_start.isoformat(),
        "test_start": config.test_start.isoformat(),
        "periods": {
            name: {
                "rows": len(period),
                "first_date": str(period["tourney_date"].iloc[0]),
                "last_date": str(period["tourney_date"].iloc[-1]),
            }
            for name, period in periods.items()
        },
    }
    return PreparedDataset(periods, split, digest)
