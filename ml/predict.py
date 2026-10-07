"""Prediction API and command-line entry point for a minimally specified match."""

from __future__ import annotations

import argparse
import pickle
from pathlib import Path

import pandas as pd

from pipelines.historical_matches.transforms.player_comparison import calculate_player_comparison
from pipelines.shared.contracts import FutureMatchRequest, PlayerHistory, Prediction
from pipelines.shared.paths import MODELS_DIR, PLAYER_HISTORY_PATH


def predict(
    request: FutureMatchRequest, model_path: Path, history_path: Path = PLAYER_HISTORY_PATH
) -> Prediction:
    """Predict a winner probability using names, date, surface, and persisted prior state."""
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")
    if not history_path.exists():
        raise FileNotFoundError(f"Player history asset not found: {history_path}")
    with model_path.open("rb") as file:
        artifact: dict[str, object] = pickle.load(file)
    snapshot = pd.read_parquet(history_path)
    history = PlayerHistory.model_validate_json(snapshot.loc[0, "history"])
    features = features_for(history, request)
    columns = artifact["feature_columns"]
    model = artifact["model"]
    probability = float(model.predict_proba(pd.DataFrame([features], columns=columns))[:, 1][0])
    return Prediction(
        player0_name=request.player0_name,
        player1_name=request.player1_name,
        player0_win_probability=1 - probability,
        player1_win_probability=probability,
        feature_cutoff=history.last_result_date or request.match_date,
        model_path=str(model_path),
    )


def features_for(history: PlayerHistory, request: FutureMatchRequest) -> dict[str, float]:
    """Calculate a future matchup from the persisted Dagster history asset."""
    if history.last_result_date is None or request.match_date <= history.last_result_date:
        raise ValueError("Prediction date must be after the persisted player-history cutoff")
    players = []
    for name in (request.player0_name, request.player1_name):
        matches = [p for p in history.players.values() if p.name.casefold() == name.casefold()]
        if len(matches) != 1:
            raise ValueError(f"Could not resolve exactly one player named '{name}'")
        players.append(matches[0])
    return calculate_player_comparison(players[0], players[1], request.match_date, request.surface)


def main() -> None:
    """Predict a match supplied as two names, date, and surface."""
    parser = argparse.ArgumentParser()
    parser.add_argument("player0_name")
    parser.add_argument("player1_name")
    parser.add_argument("match_date")
    parser.add_argument("surface")
    parser.add_argument("--model-path", type=Path, default=MODELS_DIR / "match_winner.pkl")
    parser.add_argument("--history-path", type=Path, default=PLAYER_HISTORY_PATH)
    arguments = parser.parse_args()
    request = FutureMatchRequest(
        player0_name=arguments.player0_name,
        player1_name=arguments.player1_name,
        match_date=arguments.match_date,
        surface=arguments.surface,
    )
    print(predict(request, arguments.model_path, arguments.history_path).model_dump_json(indent=2))


if __name__ == "__main__":
    main()
