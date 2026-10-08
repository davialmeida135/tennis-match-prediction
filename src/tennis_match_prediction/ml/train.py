"""Train and register the chronological tennis match prediction model."""

from __future__ import annotations

import argparse
import json
import pickle
from datetime import date
from pathlib import Path

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from tennis_match_prediction.contracts import PLAYER_COMPARISON_FEATURE_COLUMNS
from tennis_match_prediction.ml.config import (
    MAX_ITER,
    MODEL_PATH,
    RANDOM_STATE,
    TEST_START,
    TRAIN_START,
    TRAINING_FEATURE_COLUMNS,
    TRAINING_MATCHES,
    TRAINING_MATCHES_PATH,
    VALIDATION_START,
)


def train(
    training_matches_path: Path = TRAINING_MATCHES_PATH,
    model_path: Path = MODEL_PATH,
    *,
    train_start: date = TRAIN_START,
    validation_start: date = VALIDATION_START,
    test_start: date = TEST_START,
    feature_columns: tuple[str, ...] = TRAINING_FEATURE_COLUMNS,
) -> dict[str, float]:
    """Train using ML configuration defaults or explicit paths, dates, and features."""
    if not feature_columns:
        raise ValueError("Select at least one training feature")
    if len(set(feature_columns)) != len(feature_columns):
        raise ValueError("Training features must not contain duplicates")
    unknown = set(feature_columns).difference(PLAYER_COMPARISON_FEATURE_COLUMNS)
    if unknown:
        raise ValueError(f"Unknown training features: {sorted(unknown)}")
    contract = TRAINING_MATCHES.model_copy(
        update={
            "required": (*feature_columns, "winner", "tourney_date"),
            "numeric": feature_columns,
        }
    )
    matches = pd.read_csv(training_matches_path, low_memory=False)
    missing = set(contract.required).difference(matches.columns)
    if missing:
        raise ValueError(
            "Expected Dagster training rows. Materialize materialize_historical_dataset first. "
            f"Missing columns: {sorted(missing)}"
        )
    frame = matches.loc[:, list(contract.required)]
    contract.validate_frame(frame)
    if len(frame) < 30:
        raise ValueError("At least 30 completed matches are required to train a model")
    if not train_start < validation_start < test_start:
        raise ValueError("Dates must satisfy train_start < validation_start < test_start")
    dates = pd.to_datetime(frame["tourney_date"], format="ISO8601").dt.date
    periods = {
        "warmup": frame.loc[dates < train_start],
        "training": frame.loc[(dates >= train_start) & (dates < validation_start)],
        "validation": frame.loc[(dates >= validation_start) & (dates < test_start)],
        "test": frame.loc[dates >= test_start],
    }
    for name, period in periods.items():
        if period.empty:
            raise ValueError(f"The {name} period contains no matches; adjust the split dates")
    training = periods["training"]
    validation = periods["validation"]
    testing = periods["test"]
    if training["winner"].nunique() != 2:
        raise ValueError("The training period must contain both target classes")
    split_metadata = {
        "date_column": "tourney_date",
        "train_start": train_start.isoformat(),
        "validation_start": validation_start.isoformat(),
        "test_start": test_start.isoformat(),
        "periods": {
            name: {
                "rows": len(period),
                "first_date": str(period["tourney_date"].iloc[0]),
                "last_date": str(period["tourney_date"].iloc[-1]),
            }
            for name, period in periods.items()
        },
    }
    model = make_pipeline(
        StandardScaler(), LogisticRegression(max_iter=MAX_ITER, random_state=RANDOM_STATE)
    )
    model.fit(training.loc[:, list(feature_columns)], training["winner"])
    metrics = _metrics(model, validation, "validation", feature_columns) | _metrics(
        model, testing, "test", feature_columns
    )
    model_path.parent.mkdir(parents=True, exist_ok=True)
    with model_path.open("wb") as file:
        pickle.dump(
            {
                "model": model,
                "feature_columns": feature_columns,
                "history_order": "source_date_match_num",
                "split": split_metadata,
            },
            file,
        )
    metadata_path = model_path.with_suffix(".json")
    metadata_path.write_text(
        json.dumps(
            {
                "feature_columns": feature_columns,
                "history_order": "source_date_match_num",
                "training_matches_path": str(training_matches_path),
                "rows": len(frame),
                "split": split_metadata,
                "metrics": metrics,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    _log_mlflow(model_path, metrics, feature_columns)
    return metrics


def _metrics(
    model: object, frame: pd.DataFrame, prefix: str, feature_columns: tuple[str, ...]
) -> dict[str, float]:
    """Evaluate a fitted scikit-learn classifier on one chronological split."""
    if not hasattr(model, "predict_proba"):
        raise TypeError("Model must provide predict_proba")
    probabilities = model.predict_proba(frame.loc[:, list(feature_columns)])[:, 1]
    return {
        f"{prefix}_accuracy": float(accuracy_score(frame["winner"], probabilities >= 0.5)),
        f"{prefix}_brier": float(brier_score_loss(frame["winner"], probabilities)),
        f"{prefix}_log_loss": float(log_loss(frame["winner"], probabilities, labels=[0, 1])),
    }


def _log_mlflow(
    model_path: Path, metrics: dict[str, float], feature_columns: tuple[str, ...]
) -> None:
    """Log locally when MLflow is installed; training remains usable without a server."""
    try:
        import mlflow
    except ImportError:
        return
    mlflow.set_experiment("tennis-match-prediction")
    with mlflow.start_run():
        mlflow.log_params(
            {
                "model": "logistic_regression",
                "max_iter": MAX_ITER,
                "random_state": RANDOM_STATE,
                "feature_count": len(feature_columns),
                "feature_columns": json.dumps(feature_columns),
                **{
                    key: value
                    for key, value in json.loads(
                        model_path.with_suffix(".json").read_text(encoding="utf-8")
                    )["split"].items()
                    if key != "periods"
                },
            }
        )
        mlflow.log_metrics(metrics)
        mlflow.log_artifact(str(model_path))
        mlflow.log_artifact(str(model_path.with_suffix(".json")))


def main() -> None:
    """Run chronological training from the command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "training_matches", type=Path, nargs="?", default=TRAINING_MATCHES_PATH
    )
    parser.add_argument("--model-path", type=Path, default=MODEL_PATH)
    parser.add_argument(
        "--train-start",
        type=date.fromisoformat,
        default=TRAIN_START,
        help="YYYY-MM-DD; earlier rows are history warmup only",
    )
    parser.add_argument("--validation-start", type=date.fromisoformat, default=VALIDATION_START)
    parser.add_argument("--test-start", type=date.fromisoformat, default=TEST_START)
    parser.add_argument(
        "--features",
        nargs="+",
        default=TRAINING_FEATURE_COLUMNS,
        help="Ordered feature names to train on (defaults to tennis_match_prediction.ml.config)",
    )
    arguments = parser.parse_args()
    metrics = train(
        arguments.training_matches,
        arguments.model_path,
        train_start=arguments.train_start,
        validation_start=arguments.validation_start,
        test_start=arguments.test_start,
        feature_columns=tuple(arguments.features),
    )
    print(json.dumps(metrics, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
