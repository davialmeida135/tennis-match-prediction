"""Prediction API and command-line entry point for a minimally specified match."""

from __future__ import annotations

import argparse
import pickle
from pathlib import Path

import pandas as pd

from pipelines.historical_matches.feature_state import FeatureState
from pipelines.shared.contracts import FutureMatchRequest, Prediction
from pipelines.shared.paths import FEATURE_STATE_PATH, MODELS_DIR


def predict(
    request: FutureMatchRequest, model_path: Path, state_path: Path = FEATURE_STATE_PATH
) -> Prediction:
    """Predict a winner probability using names, date, surface, and persisted prior state."""
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")
    if not state_path.exists():
        raise FileNotFoundError(f"Feature state not found: {state_path}")
    with model_path.open("rb") as file:
        artifact: dict[str, object] = pickle.load(file)
    state = FeatureState.load(state_path)
    features = state.features_for(request)
    columns = artifact["feature_columns"]
    model = artifact["model"]
    probability = float(model.predict_proba(pd.DataFrame([features], columns=columns))[:, 1][0])
    return Prediction(
        player0_name=request.player0_name,
        player1_name=request.player1_name,
        player0_win_probability=1 - probability,
        player1_win_probability=probability,
        feature_cutoff=state.last_result_date or request.match_date,
        model_path=str(model_path),
    )


def main() -> None:
    """Predict a match supplied as two names, date, and surface."""
    parser = argparse.ArgumentParser()
    parser.add_argument("player0_name")
    parser.add_argument("player1_name")
    parser.add_argument("match_date")
    parser.add_argument("surface")
    parser.add_argument("--model-path", type=Path, default=MODELS_DIR / "match_winner.pkl")
    parser.add_argument("--state-path", type=Path, default=FEATURE_STATE_PATH)
    arguments = parser.parse_args()
    request = FutureMatchRequest(
        player0_name=arguments.player0_name,
        player1_name=arguments.player1_name,
        match_date=arguments.match_date,
        surface=arguments.surface,
    )
    print(predict(request, arguments.model_path, arguments.state_path).model_dump_json(indent=2))


if __name__ == "__main__":
    main()
