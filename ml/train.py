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

from pipelines.historical_matches.feature_state import FEATURE_COLUMNS, build_training_frame
from pipelines.shared.paths import FEATURE_STATE_PATH, MODELS_DIR


def train(
    historical_matches_path: Path,
    model_path: Path,
    state_path: Path = FEATURE_STATE_PATH,
) -> dict[str, float]:
    """Train with chronological splits and save the model plus feature-state checkpoint."""
    matches = pd.read_csv(
        historical_matches_path,
        dtype={"winner_id": "string", "loser_id": "string"},
        low_memory=False,
    )
    frame, state = build_training_frame(matches)
    if len(frame) < 30:
        raise ValueError("At least 30 completed matches are required to train a model")
    train_end = int(len(frame) * 0.7)
    validation_end = int(len(frame) * 0.85)
    training = frame.iloc[:train_end]
    validation = frame.iloc[train_end:validation_end]
    testing = frame.iloc[validation_end:]
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1_000, random_state=42))
    model.fit(training.loc[:, FEATURE_COLUMNS], training["winner"])
    metrics = _metrics(model, validation, "validation") | _metrics(model, testing, "test")
    model_path.parent.mkdir(parents=True, exist_ok=True)
    with model_path.open("wb") as file:
        pickle.dump({"model": model, "feature_columns": FEATURE_COLUMNS}, file)
    state.save(state_path)
    metadata_path = model_path.with_suffix(".json")
    metadata_path.write_text(
        json.dumps(
            {
                "feature_columns": FEATURE_COLUMNS,
                "historical_matches_path": str(historical_matches_path),
                "rows": len(frame),
                "metrics": metrics,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    _log_mlflow(model_path, metrics)
    return metrics


def _metrics(model: object, frame: pd.DataFrame, prefix: str) -> dict[str, float]:
    """Evaluate a fitted scikit-learn classifier on one chronological split."""
    if not hasattr(model, "predict_proba"):
        raise TypeError("Model must provide predict_proba")
    probabilities = model.predict_proba(frame.loc[:, FEATURE_COLUMNS])[:, 1]
    return {
        f"{prefix}_accuracy": float(accuracy_score(frame["winner"], probabilities >= 0.5)),
        f"{prefix}_brier": float(brier_score_loss(frame["winner"], probabilities)),
        f"{prefix}_log_loss": float(log_loss(frame["winner"], probabilities)),
    }


def _log_mlflow(model_path: Path, metrics: dict[str, float]) -> None:
    """Log locally when MLflow is installed; training remains usable without a server."""
    try:
        import mlflow
    except ImportError:
        return
    mlflow.set_experiment("tennis-match-prediction")
    with mlflow.start_run():
        mlflow.log_params({"model": "logistic_regression", "feature_count": len(FEATURE_COLUMNS)})
        mlflow.log_metrics(metrics)
        mlflow.log_artifact(str(model_path))
        mlflow.log_artifact(str(model_path.with_suffix(".json")))


def main() -> None:
    """Run chronological training from the command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument("historical_matches", type=Path)
    parser.add_argument("--model-path", type=Path, default=MODELS_DIR / "match_winner.pkl")
    parser.add_argument("--state-path", type=Path, default=FEATURE_STATE_PATH)
    arguments = parser.parse_args()
    metrics = train(arguments.historical_matches, arguments.model_path, arguments.state_path)
    print(json.dumps(metrics, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
