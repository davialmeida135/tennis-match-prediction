"""Cached, read-only player and model access for the local application."""

import hashlib
import json
import logging
import math
import unicodedata
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from threading import Lock

import pandas as pd
from pydantic import JsonValue, ValidationError

from tennis_match_prediction.contracts.experiments import ModelMetadata
from tennis_match_prediction.contracts.features import PLAYER_COMPARISON_FEATURE_COLUMNS
from tennis_match_prediction.contracts.player_history import PlayerHistory, PlayerState
from tennis_match_prediction.ml.artifacts import ModelArtifact, load_artifact
from tennis_match_prediction.ml.predict import features_for_players, probability_for
from tennis_match_prediction.web.contracts import MatchupRequest


def search_key(value: str) -> str:
    """Make searches insensitive to accents, casing and punctuation."""
    normalized = unicodedata.normalize("NFKD", value.casefold())
    return " ".join(
        "".join(
            c if c.isalnum() else " " for c in normalized if not unicodedata.combining(c)
        ).split()
    )


def rate(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator > 0 else None


class PlayerService:
    """One frozen history snapshot per server lifetime; models load on first selection."""

    def __init__(self, history_path: Path, models_dir: Path, model_path: Path | None) -> None:
        publication = history_path.parent / "latest_refresh.json"
        expected_hash = None
        if publication.is_file():
            manifest = json.loads(publication.read_text(encoding="utf-8"))
            item = manifest["artifacts"].get(history_path.name)
            if (
                item is not None
                and Path(item["published_path"]).resolve() == history_path.resolve()
            ):
                snapshot = Path(item["snapshot_path"])
                if not snapshot.resolve().is_relative_to(
                    (history_path.parent / ".refreshes").resolve()
                ):
                    raise ValueError("Published history snapshot is outside the refresh directory")
                history_path = snapshot
                expected_hash = item["sha256"]
        if not history_path.is_file():
            raise FileNotFoundError(
                "Player history is missing. Materialize materialize_historical_dataset first."
            )
        content = history_path.read_bytes()
        self._history_bytes = content
        self.history_version = hashlib.sha256(content).hexdigest()
        if expected_hash is not None and self.history_version != expected_hash:
            raise ValueError("Published history snapshot checksum does not match its manifest")
        snapshot = pd.read_parquet(BytesIO(content))
        self.history = PlayerHistory.model_validate_json(snapshot.loc[0, "history"])
        if self.history.last_source_date is None or not self.history.players:
            raise ValueError("Player history has no completed results")
        self.models: dict[str, tuple[Path, ModelMetadata]] = {}
        paths = (
            [model_path]
            if model_path is not None
            else sorted(
                models_dir.glob("*/model.pkl"), key=lambda path: path.stat().st_mtime, reverse=True
            )
        )
        for path in paths:
            metadata_path = path.with_suffix(".json")
            if not path.is_file() or not metadata_path.is_file():
                if model_path is not None:
                    raise FileNotFoundError("Model pickle and matching JSON metadata are required")
                continue
            try:
                metadata = ModelMetadata.model_validate_json(metadata_path.read_text("utf-8"))
            except (ValidationError, ValueError) as error:
                if model_path is not None:
                    raise ValueError(f"Invalid model metadata: {error}") from error
                logging.getLogger(__name__).warning("Skipping invalid model metadata: %s", path)
                continue
            if not set(metadata.feature_columns).issubset(PLAYER_COMPARISON_FEATURE_COLUMNS):
                if model_path is not None:
                    raise ValueError(
                        "Selected model requires features unavailable at prediction time"
                    )
                continue
            # Registry downloads use temporary paths; identity must survive server restarts.
            model_id = hashlib.sha256(metadata.model_dump_json().encode()).hexdigest()[:16]
            self.models[model_id] = (path, metadata)
        self.default_model_id = next(iter(self.models), None)
        self._artifacts: dict[str, tuple[ModelArtifact, str]] = {}
        self._model_bytes: dict[str, bytes] = {}
        self._model_lock = Lock()
        self._index = {
            player_id: search_key(self.history.players[player_id].name)
            for player_id in sorted(self.history.players, key=self._sort_key)
        }
        if self.default_model_id is not None:
            self._load_model(self.default_model_id)

    def _sort_key(self, player_id: str) -> tuple[int, float, str]:
        player = self.history.players[player_id]
        latest = player.history[0].tourney_date if player.history else date.min
        return (-latest.toordinal(), player.rank or math.inf, player.name)

    def _load_model(self, model_id: str) -> tuple[ModelArtifact, str]:
        if model_id not in self.models:
            raise ValueError("Unknown model. Select one of the available models.")
        with self._model_lock:
            if model_id not in self._artifacts:
                path, metadata = self.models[model_id]
                content = path.read_bytes()
                artifact = load_artifact(path)
                if artifact.metadata != metadata or path.read_bytes() != content:
                    raise ValueError("Model changed since startup. Restart the web application.")
                self._artifacts[model_id] = (
                    artifact,
                    hashlib.sha256(content).hexdigest(),
                )
                self._model_bytes[model_id] = content
            return self._artifacts[model_id]

    def save_prediction_artifacts(self, model_id: str, directory: Path) -> None:
        """Keep the exact frozen artifacts used by inference, including registry downloads."""
        artifact, _ = self._load_model(model_id)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "model.pkl").write_bytes(self._model_bytes[model_id])
        (directory / "model.json").write_text(
            artifact.metadata.model_dump_json(indent=2), encoding="utf-8"
        )
        (directory / "player_history.parquet").write_bytes(self._history_bytes)

    def model_version(self, model_id: str) -> str:
        return self._load_model(model_id)[1]

    def status(self) -> dict[str, JsonValue]:
        cutoff = self.history.last_source_date
        assert cutoff is not None
        return {
            "history_source_date": cutoff.isoformat(),
            "history_version": self.history_version,
            "player_count": len(self.history.players),
            "minimum_prediction_date": (cutoff + timedelta(days=1)).isoformat(),
            "default_model_id": self.default_model_id,
            "models": [
                {
                    "id": model_id,
                    "name": metadata.model_name,
                    "run_id": metadata.run_id,
                    "feature_count": len(metadata.feature_columns),
                    "validation_log_loss": metadata.metrics.get("validation_log_loss"),
                    "trained_through": metadata.split.get("periods", {})
                    .get("training", {})
                    .get("last_date")
                    if isinstance(metadata.split.get("periods"), dict)
                    else None,
                }
                for model_id, (_, metadata) in self.models.items()
            ],
        }

    def search(self, query: str, limit: int, offset: int) -> dict[str, JsonValue]:
        terms = search_key(query).split()
        matches = [
            player_id
            for player_id, name in self._index.items()
            if all(term in name for term in terms)
        ]
        return {
            "total": len(matches),
            "players": [self.summary(player_id) for player_id in matches[offset : offset + limit]],
        }

    def player(self, player_id: str) -> PlayerState:
        if player_id not in self.history.players:
            raise KeyError("Player not found in this history snapshot")
        return self.history.players[player_id]

    def summary(self, player_id: str) -> dict[str, JsonValue]:
        player = self.player(player_id)
        return {
            "id": player_id,
            "name": player.name,
            "rank": player.rank,
            "elo": player.rating,
            "wins": player.wins,
            "losses": player.losses,
            "last_source_date": player.history[0].tourney_date.isoformat()
            if player.history
            else None,
        }

    def profile(self, player_id: str) -> dict[str, JsonValue]:
        player = self.player(player_id)
        recent = player.history[:10]
        return self.summary(player_id) | {
            "age": player.age,
            "rank_points": player.rank_points,
            "win_rate": rate(player.wins, player.wins + player.losses),
            "recent_win_rate": rate(sum(match.won for match in recent), len(recent)),
            "surface_ratings": player.surface_ratings,
            "surface_form": {
                surface: {
                    "matches": len(matches),
                    "wins": sum(m.won for m in matches),
                    "win_rate": rate(sum(m.won for m in matches), len(matches)),
                }
                for surface, matches in player.surface_history.items()
            },
            "serve": {
                "ace_rate": rate(player.aces, player.ace_serve_points),
                "double_fault_rate": rate(player.double_faults, player.double_fault_serve_points),
                "service_points_won_rate": rate(
                    player.service_points_won, player.service_won_serve_points
                ),
                "observed_points": player.serve_points,
            },
            "recent_matches": [m.model_dump(mode="json") for m in player.history],
            "history_source_date": self.history.last_source_date.isoformat(),
            "history_version": self.history_version,
        }

    def predict(self, request: MatchupRequest) -> dict[str, JsonValue]:
        player0 = self.player(request.player0_id)
        player1 = self.player(request.player1_id)
        features = features_for_players(
            self.history,
            request.player0_id,
            request.player1_id,
            request.match_date,
            request.surface,
        )
        artifact, model_version = self._load_model(request.model_id)
        probability = probability_for(artifact, features)
        return {
            "created_at": datetime.now(UTC).isoformat(),
            "request": request.model_dump(mode="json"),
            "player0": self.summary(request.player0_id),
            "player1": self.summary(request.player1_id),
            "player0_win_probability": 1 - probability,
            "player1_win_probability": probability,
            "player0_decimal_odds": 1 / (1 - probability) if probability < 1 else None,
            "player1_decimal_odds": 1 / probability if probability > 0 else None,
            "head_to_head": {
                "player0_wins": player0.h2h_wins.get(player1.name, 0),
                "player1_wins": player1.h2h_wins.get(player0.name, 0),
            },
            "features": features,
            "history_source_date": self.history.last_source_date.isoformat(),
            "history_version": self.history_version,
            "model_version": model_version,
            "model_name": artifact.metadata.model_name,
            "run_id": artifact.metadata.run_id,
        }
