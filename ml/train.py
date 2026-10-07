"""Train and register the chronological tennis match prediction model."""

from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from ml.config import TRAINING_FEATURE_COLUMNS, TRAINING_MATCHES
from pipelines.shared.contracts import FEATURE_COLUMNS
from pipelines.shared.paths import CURATED_DIR, MODELS_DIR, TRAINING_MATCHES_CSV_NAME


def train(
    training_matches_path: Path,
    model_path: Path,
    *,
    feature_columns: tuple[str, ...] = TRAINING_FEATURE_COLUMNS,
) -> dict[str, float]:
    """Train from the Dagster-produced dataset using chronological splits."""
    if not feature_columns:
        raise ValueError("Select at least one training feature")
    if len(set(feature_columns)) != len(feature_columns):
        raise ValueError("Training features must not contain duplicates")
    unknown = set(feature_columns).difference(FEATURE_COLUMNS)
    if unknown:
        raise ValueError(f"Unknown training features: {sorted(unknown)}")
    contract = TRAINING_MATCHES.model_copy(
        update={"required": (*feature_columns, "winner", "match_date"), "numeric": feature_columns}
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
    train_end = int(len(frame) * 0.7)
    validation_end = int(len(frame) * 0.85)
    training = frame.iloc[:train_end]
    validation = frame.iloc[train_end:validation_end]
    testing = frame.iloc[validation_end:]
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1_000, random_state=42))
    model.fit(training.loc[:, list(feature_columns)], training["winner"])
    metrics = _metrics(model, validation, "validation", feature_columns) | _metrics(
        model, testing, "test", feature_columns
    )
    model_path.parent.mkdir(parents=True, exist_ok=True)
    with model_path.open("wb") as file:
        pickle.dump({"model": model, "feature_columns": feature_columns}, file)
    metadata_path = model_path.with_suffix(".json")
    metadata_path.write_text(
        json.dumps(
            {
                "feature_columns": feature_columns,
                "training_matches_path": str(training_matches_path),
                "rows": len(frame),
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
        f"{prefix}_log_loss": float(log_loss(frame["winner"], probabilities)),
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
                "feature_count": len(feature_columns),
                "feature_columns": json.dumps(feature_columns),
            }
        )
        mlflow.log_metrics(metrics)
        mlflow.log_artifact(str(model_path))
        mlflow.log_artifact(str(model_path.with_suffix(".json")))


def main() -> None:
    """Run chronological training from the command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "training_matches", type=Path, nargs="?", default=CURATED_DIR / TRAINING_MATCHES_CSV_NAME
    )
    parser.add_argument("--model-path", type=Path, default=MODELS_DIR / "match_winner.pkl")
    parser.add_argument(
        "--features",
        nargs="+",
        default=TRAINING_FEATURE_COLUMNS,
        help="Ordered feature names to train on (defaults to ml/config.py)",
    )
    arguments = parser.parse_args()
    metrics = train(
        arguments.training_matches,
        arguments.model_path,
        feature_columns=tuple(arguments.features),
    )
    print(json.dumps(metrics, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
