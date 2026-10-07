"""Chronological player history and checkpoints for training and future inference."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from pipelines.historical_matches.transforms.elo import elo_delta
from pipelines.historical_matches.transforms.history import (
    order_history,
    parse_match_date,
    prepare_history,
)
from pipelines.historical_matches.transforms.imputation import fill_null_surface
from pipelines.historical_matches.transforms.player_comparison import calculate_player_comparison
from pipelines.shared.contracts import (
    FEATURE_COLUMNS,
    INITIAL_ELO,
    TRAINING_MATCHES,
    FeatureStateCheckpoint,
    FutureMatchRequest,
    PlayedMatch,
    PlayerState,
)

STATE_VERSION = 1


class FeatureState:
    """Mutable, serializable pre-match state. Results must be applied chronologically."""

    def __init__(self) -> None:
        self.players: dict[str, PlayerState] = {}
        self.last_result_date: date | None = None

    def features_for(self, request: FutureMatchRequest) -> dict[str, float]:
        """Build player1-minus-player0 features using no result after `match_date`."""
        if self.last_result_date is not None and request.match_date <= self.last_result_date:
            raise ValueError("Prediction date must be after the persisted feature-state cutoff")
        player0 = self._resolve_player(request.player0_name)
        player1 = self._resolve_player(request.player1_name)
        return calculate_player_comparison(player0, player1, request.match_date, request.surface)

    def apply_result(self, match: pd.Series) -> dict[str, float]:
        """Return pre-match winner-minus-loser features, then update both players atomically."""
        played_on = _match_date(match)
        if self.last_result_date is not None and played_on < self.last_result_date:
            raise ValueError("Results must be applied in chronological order")
        winner = self._player(str(match["winner_id"]), str(match["winner_name"]))
        loser = self._player(str(match["loser_id"]), str(match["loser_name"]))
        surface = str(match.get("surface", "Hard"))
        features = calculate_player_comparison(loser, winner, played_on, surface)
        winner_rating = winner.rating
        loser_rating = loser.rating
        winner_surface_rating = winner.surface_ratings.get(surface, INITIAL_ELO)
        loser_surface_rating = loser.surface_ratings.get(surface, INITIAL_ELO)
        winner.rating = winner_rating + elo_delta(
            winner_rating, loser_rating, 1.0, winner.wins + winner.losses
        )
        loser.rating = loser_rating + elo_delta(
            loser_rating, winner_rating, 0.0, loser.wins + loser.losses
        )
        winner.surface_ratings[surface] = winner_surface_rating + elo_delta(
            winner_surface_rating, loser_surface_rating, 1.0, winner.wins + winner.losses
        )
        loser.surface_ratings[surface] = loser_surface_rating + elo_delta(
            loser_surface_rating, winner_surface_rating, 0.0, loser.wins + loser.losses
        )
        self._update_player(winner, loser, match, played_on, surface, won=True)
        self._update_player(loser, winner, match, played_on, surface, won=False)
        self.last_result_date = played_on
        return features

    def save(self, path: Path) -> None:
        """Persist a versioned checkpoint used for incremental feature calculation."""
        payload = {
            "version": STATE_VERSION,
            "last_result_date": self.last_result_date.isoformat()
            if self.last_result_date
            else None,
            "players": {
                player_id: player.model_dump(mode="json")
                for player_id, player in self.players.items()
            },
        }
        checkpoint = FeatureStateCheckpoint.model_validate(payload)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(checkpoint.model_dump(mode="json"), sort_keys=True), encoding="utf-8"
        )

    @classmethod
    def load(cls, path: Path) -> FeatureState:
        """Load a persisted state checkpoint."""
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("version") != STATE_VERSION:
            raise ValueError("Unsupported feature-state version")
        checkpoint = FeatureStateCheckpoint.model_validate(payload)
        state = cls()
        state.last_result_date = checkpoint.last_result_date
        state.players = checkpoint.players
        return state

    def _resolve_player(self, name: str) -> PlayerState:
        exact = [
            player for player in self.players.values() if player.name.casefold() == name.casefold()
        ]
        if len(exact) != 1:
            raise ValueError(f"Could not resolve exactly one player named '{name}'")
        return exact[0]

    def _player(self, player_id: str, name: str) -> PlayerState:
        if player_id not in self.players:
            self.players[player_id] = PlayerState(name=name)
        return self.players[player_id]

    def _update_player(
        self,
        player: PlayerState,
        opponent: PlayerState,
        match: pd.Series,
        played_on: date,
        surface: str,
        *,
        won: bool,
    ) -> None:
        prefix = "w" if won else "l"
        player.wins += int(won)
        player.losses += int(not won)
        player.h2h_wins[opponent.name] = player.h2h_wins.get(opponent.name, 0) + int(won)
        player.history.insert(
            0,
            PlayedMatch(
                played_on=played_on.isoformat(),
                surface=surface,
                won=won,
                minutes=_number(match.get("minutes")),
            ),
        )
        player.history = player.history[:50]
        player.serve_points += _number(match.get(f"{prefix}_svpt"))
        player.aces += _number(match.get(f"{prefix}_ace"))
        player.double_faults += _number(match.get(f"{prefix}_df"))
        player.service_points_won += _number(match.get(f"{prefix}_1stWon")) + _number(
            match.get(f"{prefix}_2ndWon")
        )
        side = "winner" if won else "loser"
        player.rank = _optional_number(match.get(f"{side}_rank")) or player.rank
        player.rank_points = (
            _optional_number(match.get(f"{side}_rank_points")) or player.rank_points
        )
        player.age = _optional_number(match.get(f"{side}_age")) or player.age


def build_training_frame(
    matches: pd.DataFrame, *, random_seed: int = 42
) -> tuple[pd.DataFrame, FeatureState]:
    """Build a balanced, pre-match feature frame and its resulting state checkpoint."""
    temporal_features, state = _temporal_features_by_row(prepare_history(matches))
    generator = np.random.default_rng(random_seed)
    rows: list[dict[str, float | int | str]] = []
    for _, match in temporal_features.iterrows():
        winner_is_player1 = bool(generator.integers(0, 2))
        differences = match.loc[list(FEATURE_COLUMNS)]
        values = differences if winner_is_player1 else -differences
        rows.append(
            {
                **values.to_dict(),
                "winner": int(winner_is_player1),
                "match_date": match["match_date"],
            }
        )
    frame = pd.DataFrame(rows, columns=[*FEATURE_COLUMNS, "winner", "match_date"])
    # Empty histories remain usable for feature generation; training enforces its row minimum.
    if not frame.empty:
        TRAINING_MATCHES.validate_frame(frame)
    return frame, state


def attach_temporal_features(
    matches: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, FeatureState]:
    """Keep features on their source rows; also return the export and checkpoint."""
    source = matches.reset_index(drop=True)
    features, state = _temporal_features_by_row(source)
    featured = source.join(features[list(FEATURE_COLUMNS)])
    return featured, features.reset_index(drop=True), state


def _temporal_features_by_row(
    matches: pd.DataFrame,
) -> tuple[pd.DataFrame, FeatureState]:
    """Use source row positions for alignment, independent of match identifiers."""
    required = {
        "tourney_date",
        "tourney_id",
        "match_num",
        "winner_id",
        "loser_id",
        "winner_name",
        "loser_name",
    }
    missing = required.difference(matches.columns)
    if missing:
        raise ValueError(f"Missing historical match columns: {sorted(missing)}")
    state = FeatureState()
    source_rows: list[int] = []
    rows: list[dict[str, float | int | str | None]] = []
    ordered = order_history(fill_null_surface(matches.reset_index(drop=True)))
    for source_row, match in ordered.iterrows():
        if str(match.get("score", "")) == "W/O":
            continue
        winner_minus_loser = state.apply_result(match)
        source_rows.append(source_row)
        rows.append(
            {
                "tourney_id": str(match["tourney_id"]),
                "match_num": int(match["match_num"]) if pd.notna(match["match_num"]) else None,
                "winner_id": str(match["winner_id"]),
                "loser_id": str(match["loser_id"]),
                **winner_minus_loser,
                "match_date": _match_date(match).isoformat(),
            }
        )
    columns = ["tourney_id", "match_num", "winner_id", "loser_id", *FEATURE_COLUMNS, "match_date"]
    frame = pd.DataFrame(rows, columns=columns, index=source_rows)
    # Preserve missing source match numbers in the standalone temporal export.
    frame["match_num"] = frame["match_num"].astype("Int64")
    return frame, state


def _match_date(match: pd.Series) -> date:
    return parse_match_date(match["tourney_date"])


def _number(value: object) -> float:
    parsed = pd.to_numeric(value, errors="coerce")
    return float(parsed) if pd.notna(parsed) else 0.0


def _optional_number(value: object) -> float | None:
    parsed = pd.to_numeric(value, errors="coerce")
    return float(parsed) if pd.notna(parsed) else None
