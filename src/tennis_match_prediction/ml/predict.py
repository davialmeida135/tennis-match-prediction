"""Prediction API and command-line entry point for a minimally specified match."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tennis_match_prediction.contracts import FutureMatchRequest, PlayerHistory, Prediction


def predict(
    request: FutureMatchRequest, model_path: Path, history_path: Path | None = None
) -> Prediction:
    """Predict a winner probability using names, date, surface, and persisted prior state."""
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")
    # Startup imports measured ~94s here; keep CLI help and validation standard-library-only.
    import pandas as pd

    from tennis_match_prediction.contracts import PlayerHistory, Prediction
    from tennis_match_prediction.ml.artifacts import load_artifact
    from tennis_match_prediction.ml.evaluation import validate_probabilities
    from tennis_match_prediction.paths import PLAYER_HISTORY_PATH

    if history_path is None:
        history_path = PLAYER_HISTORY_PATH
    if not history_path.exists():
        raise FileNotFoundError(f"Player history asset not found: {history_path}")
    artifact = load_artifact(model_path)
    snapshot = pd.read_parquet(history_path)
    history = PlayerHistory.model_validate_json(snapshot.loc[0, "history"])
    features = features_for(history, request)
    columns = artifact.metadata.feature_columns
    probability = float(
        validate_probabilities(
            artifact.model.predict_proba(pd.DataFrame([features], columns=columns)), 1
        )[0]
    )
    return Prediction(
        player0_name=request.player0_name,
        player1_name=request.player1_name,
        player0_win_probability=1 - probability,
        player1_win_probability=probability,
        history_source_date=history.last_source_date,
        model_path=str(model_path),
    )


def features_for(history: PlayerHistory, request: FutureMatchRequest) -> dict[str, float]:
    """Calculate a future matchup from the persisted Dagster history asset."""
    from tennis_match_prediction.transforms.player_comparison import calculate_player_comparison

    if history.last_source_date is None or request.match_date <= history.last_source_date:
        raise ValueError("Prediction date must be after the persisted history source date")
    players = []
    for name in (request.player0_name, request.player1_name):
        matches = [p for p in history.players.values() if p.name.casefold() == name.casefold()]
        if len(matches) != 1:
            raise ValueError(f"Could not resolve exactly one player named '{name}'")
        players.append(matches[0])
    return calculate_player_comparison(players[0], players[1], request.surface)


def _match_date(value: str) -> date:
    """Parse an unambiguous calendar date before loading prediction dependencies."""
    message = "match_date must be a valid date in YYYY-MM-DD format (for example, 2026-10-10)"
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(message) from error
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError(message)
    return parsed


def main() -> None:
    """Predict a match supplied as two names, date, and surface."""
    parser = argparse.ArgumentParser()
    parser.add_argument("player0_name")
    parser.add_argument("player1_name")
    parser.add_argument("match_date", type=_match_date, help="match date in YYYY-MM-DD format")
    parser.add_argument(
        "surface",
        type=lambda value: value.strip().title(),
        choices=("Hard", "Clay", "Grass", "Carpet"),
    )
    parser.add_argument("--model-path", type=Path, required=True)
    parser.add_argument(
        "--history-path",
        type=Path,
        help="player history parquet (default: data/curated/player_history.parquet)",
    )
    arguments = parser.parse_args()
    # Argument parsing must finish before importing the contracts' dataframe dependencies.
    from pydantic import ValidationError

    from tennis_match_prediction.contracts import FutureMatchRequest

    try:
        request = FutureMatchRequest(
            player0_name=arguments.player0_name,
            player1_name=arguments.player1_name,
            match_date=arguments.match_date,
            surface=arguments.surface,
        )
    except ValidationError as error:
        parser.error(str(error))
    print(predict(request, arguments.model_path, arguments.history_path).model_dump_json(indent=2))


if __name__ == "__main__":
    main()
