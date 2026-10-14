"""Prediction API and command-line entry point for a minimally specified match."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tennis_match_prediction.contracts import FutureMatchRequest, PlayerHistory, Prediction
    from tennis_match_prediction.ml.artifacts import ModelArtifact


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
    from tennis_match_prediction.paths import PLAYER_HISTORY_PATH

    if history_path is None:
        history_path = PLAYER_HISTORY_PATH
    if not history_path.exists():
        raise FileNotFoundError(f"Player history asset not found: {history_path}")
    artifact = load_artifact(model_path)
    snapshot = pd.read_parquet(history_path)
    history = PlayerHistory.model_validate_json(snapshot.loc[0, "history"])
    features = features_for(history, request)
    probability = probability_for(artifact, features)
    return Prediction(
        player0_name=request.player0_name,
        player1_name=request.player1_name,
        player0_win_probability=1 - probability,
        player1_win_probability=probability,
        history_source_date=history.last_source_date,
        model_path=str(model_path),
        run_id=artifact.metadata.run_id,
    )


def probability_for(artifact: ModelArtifact, features: dict[str, float]) -> float:
    """Score shared features with an already loaded artifact, preserving its input order."""
    import pandas as pd

    from tennis_match_prediction.ml.evaluation import validate_probabilities

    missing = set(artifact.metadata.feature_columns).difference(features)
    if missing:
        raise ValueError(f"Model requires unavailable prediction features: {sorted(missing)}")
    frame = pd.DataFrame([features], columns=artifact.metadata.feature_columns)
    return float(validate_probabilities(artifact.model.predict_proba(frame), 1)[0])


def predict_from_run(
    request: FutureMatchRequest,
    run_id: str,
    history_path: Path | None = None,
    *,
    tracking_uri: str | None = None,
    tracking_mode: str | None = None,
) -> Prediction:
    """Predict with a finished MLflow run's project artifact and current player history."""
    if not run_id.strip():
        raise ValueError("MLflow run ID must not be blank")
    # MLflow is needed only when selecting a tracked model, after CLI validation.
    from mlflow import MlflowClient
    from mlflow.exceptions import MlflowException

    from tennis_match_prediction.ml.config import tracking_settings

    settings = tracking_settings(mode=tracking_mode, uri=tracking_uri, experiment_name=None)
    if settings.mode == "disabled":
        raise ValueError("MLflow model selection requires local or server tracking")
    client = MlflowClient(tracking_uri=settings.uri)
    try:
        run = client.get_run(run_id)
        if run.info.status != "FINISHED":
            raise ValueError(f"MLflow run {run_id} is {run.info.status}; select a FINISHED run")
        models = [
            item
            for item in client.list_artifacts(run_id, "project_model")
            if not item.is_dir and item.path.endswith(".pkl")
        ]
        if len(models) != 1:
            raise ValueError(f"MLflow run {run_id} must contain exactly one project_model/*.pkl")
        artifact_path = models[0].path
        with TemporaryDirectory(prefix="tennis-predict-") as directory:
            downloaded = client.download_artifacts(run_id, artifact_path, directory)
            result = predict(request, Path(downloaded), history_path)
        if result.run_id != run_id:
            raise ValueError("Project artifact run ID does not match the selected MLflow run")
        return result.model_copy(update={"model_path": f"runs:/{run_id}/{artifact_path}"})
    except MlflowException as error:
        raise RuntimeError(
            f"Cannot load MLflow run {run_id} from {settings.uri}: {error}"
        ) from error


def features_for(history: PlayerHistory, request: FutureMatchRequest) -> dict[str, float]:
    """Calculate a future matchup from the persisted Dagster history asset."""
    players = []
    for name in (request.player0_name, request.player1_name):
        matches = [
            player_id
            for player_id, player in history.players.items()
            if player.name.casefold() == name.casefold()
        ]
        if len(matches) != 1:
            raise ValueError(f"Could not resolve exactly one player named '{name}'")
        players.append(matches[0])
    return features_for_players(
        history, players[0], players[1], request.match_date, request.surface
    )


def features_for_players(
    history: PlayerHistory, player0_id: str, player1_id: str, match_date: date, surface: str
) -> dict[str, float]:
    """Calculate a matchup by stable history IDs without ambiguous name resolution."""
    from tennis_match_prediction.transforms.player_comparison import calculate_player_comparison

    if history.last_source_date is None or match_date <= history.last_source_date:
        raise ValueError("Prediction date must be after the persisted history source date")
    if player0_id == player1_id:
        raise ValueError("Choose two different players")
    if player0_id not in history.players or player1_id not in history.players:
        raise ValueError("Player is absent from the persisted history")
    if surface not in {"Hard", "Clay", "Grass", "Carpet"}:
        raise ValueError("surface must be Hard, Clay, Grass, or Carpet")
    return calculate_player_comparison(
        history.players[player0_id], history.players[player1_id], surface
    )


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
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--model-path", type=Path, help="local project model pickle")
    source.add_argument("--run-id", help="MLflow run containing the project model")
    parser.add_argument("--tracking-mode", choices=("server", "local"))
    parser.add_argument("--tracking-uri", help="MLflow server URL or local SQLite URI")
    parser.add_argument(
        "--history-path",
        type=Path,
        help="player history parquet (default: data/curated/player_history.parquet)",
    )
    arguments = parser.parse_args()
    if arguments.run_id is None and (
        arguments.tracking_mode is not None or arguments.tracking_uri is not None
    ):
        parser.error("--tracking-mode and --tracking-uri require --run-id")
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
    try:
        result = (
            predict_from_run(
                request,
                arguments.run_id,
                arguments.history_path,
                tracking_uri=arguments.tracking_uri,
                tracking_mode=arguments.tracking_mode,
            )
            if arguments.run_id is not None
            else predict(request, arguments.model_path, arguments.history_path)
        )
    except (FileNotFoundError, ValueError, RuntimeError) as error:
        parser.error(str(error))
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
