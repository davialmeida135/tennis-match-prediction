"""Temporal feature engine shared by historical backfills and future inference."""

from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from pipelines.shared.contracts import (
    FEATURE_COLUMNS as FEATURE_COLUMNS,
)
from pipelines.shared.contracts import (
    INITIAL_ELO,
    FeatureStateCheckpoint,
    FutureMatchRequest,
    PlayedMatch,
    PlayerState,
)
from pipelines.shared.contracts import (
    TRAINING_MATCHES as TRAINING_MATCHES,
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
        return self._feature_values(player0, player1, request.match_date, request.surface)

    def apply_result(self, match: pd.Series) -> dict[str, float]:
        """Return pre-match winner-minus-loser features, then update both players atomically."""
        played_on = _match_date(match)
        if self.last_result_date is not None and played_on < self.last_result_date:
            raise ValueError("Results must be applied in chronological order")
        winner = self._player(str(match["winner_id"]), str(match["winner_name"]))
        loser = self._player(str(match["loser_id"]), str(match["loser_name"]))
        surface = str(match.get("surface", "Hard"))
        features = self._feature_values(loser, winner, played_on, surface)
        winner_rating = winner.rating
        loser_rating = loser.rating
        winner_surface_rating = winner.surface_ratings.get(surface, INITIAL_ELO)
        loser_surface_rating = loser.surface_ratings.get(surface, INITIAL_ELO)
        winner.rating = winner_rating + _elo_delta(
            winner_rating, loser_rating, 1.0, winner.wins + winner.losses
        )
        loser.rating = loser_rating + _elo_delta(
            loser_rating, winner_rating, 0.0, loser.wins + loser.losses
        )
        winner.surface_ratings[surface] = winner_surface_rating + _elo_delta(
            winner_surface_rating, loser_surface_rating, 1.0, winner.wins + winner.losses
        )
        loser.surface_ratings[surface] = loser_surface_rating + _elo_delta(
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

    def _feature_values(
        self, player0: PlayerState, player1: PlayerState, match_date: date, surface: str
    ) -> dict[str, float]:
        player0_recent = _recent(player0, match_date)
        player1_recent = _recent(player1, match_date)
        meetings_10 = player1.h2h_wins.get(player0.name, 0)
        meetings_01 = player0.h2h_wins.get(player1.name, 0)
        return {
            "overall_elo_diff": player1.rating - player0.rating,
            "surface_elo_diff": player1.surface_ratings.get(surface, INITIAL_ELO)
            - player0.surface_ratings.get(surface, INITIAL_ELO),
            "rank_log_advantage": _log_value(player0.rank) - _log_value(player1.rank),
            "points_log_diff": _log_value(player1.rank_points) - _log_value(player0.rank_points),
            "form_10_diff": _win_rate(player1_recent[:10]) - _win_rate(player0_recent[:10]),
            "surface_form_10_diff": _win_rate(_on_surface(player1_recent, surface)[:10])
            - _win_rate(_on_surface(player0_recent, surface)[:10]),
            "minutes_7d_diff": _minutes_since(player1_recent, match_date, 7)
            - _minutes_since(player0_recent, match_date, 7),
            "matches_14d_diff": _matches_since(player1_recent, match_date, 14)
            - _matches_since(player0_recent, match_date, 14),
            "age_diff": _value(player1.age) - _value(player0.age),
            "h2h_log_odds": math.log((meetings_10 + 1) / (meetings_01 + 1)),
            "experience_log_diff": math.log1p(player1.wins + player1.losses)
            - math.log1p(player0.wins + player0.losses),
            "ace_rate_diff": _rate(player1.aces, player1.serve_points)
            - _rate(player0.aces, player0.serve_points),
            "double_fault_rate_diff": _rate(player1.double_faults, player1.serve_points)
            - _rate(player0.double_faults, player0.serve_points),
            "service_points_won_diff": _rate(player1.service_points_won, player1.serve_points)
            - _rate(player0.service_points_won, player0.serve_points),
        }

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
    temporal_features, state = build_temporal_match_features(matches)
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


def build_temporal_match_features(
    matches: pd.DataFrame,
) -> tuple[pd.DataFrame, FeatureState]:
    """Build winner-minus-loser temporal features keyed to their source match."""
    frame, state = _temporal_features_by_row(matches)
    return frame.reset_index(drop=True), state


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
    # TennisMyLife combines legacy ISO dates with newer YYYYMMDD values. Sorting their
    # raw strings would put the two formats in separate blocks and leak future results.
    ordered = matches.reset_index(drop=True)
    ordered = ordered.assign(
        _feature_date=ordered["tourney_date"].map(_parse_match_date)
    ).sort_values(["_feature_date", "tourney_id", "match_num"])
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


def _elo_delta(rating: float, opponent_rating: float, result: float, matches: int) -> float:
    expected = 1 / (1 + 10 ** ((opponent_rating - rating) / 400))
    k_factor = 250 / ((matches + 5) ** 0.4)
    return k_factor * (result - expected)


def _match_date(match: pd.Series) -> date:
    return _parse_match_date(match["tourney_date"])


def _parse_match_date(value: object) -> date:
    """Parse the two date encodings used by the source without relying on string order."""
    if isinstance(value, date):
        return value.date() if isinstance(value, pd.Timestamp) else value
    text = str(value)
    # Rolling windows repeatedly read checkpoint dates. Avoid constructing pandas
    # datetime arrays/indexes for each of those scalar ISO calendar dates.
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return date.fromisoformat(text)
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:]))
    date_format = "%Y%m%d" if text.isdigit() else "ISO8601"
    return pd.to_datetime(text, format=date_format).date()


def _recent(player: PlayerState, match_date: date) -> list[PlayedMatch]:
    return [item for item in player.history if _parse_match_date(item.played_on) < match_date]


def _on_surface(matches: list[PlayedMatch], surface: str) -> list[PlayedMatch]:
    return [item for item in matches if item.surface == surface]


def _win_rate(matches: list[PlayedMatch]) -> float:
    return sum(item.won for item in matches) / len(matches) if matches else 0.0


def _minutes_since(matches: list[PlayedMatch], match_date: date, days: int) -> float:
    cutoff = match_date - timedelta(days=days)
    return sum(item.minutes for item in matches if _parse_match_date(item.played_on) >= cutoff)


def _matches_since(matches: list[PlayedMatch], match_date: date, days: int) -> int:
    cutoff = match_date - timedelta(days=days)
    return sum(_parse_match_date(item.played_on) >= cutoff for item in matches)


def _number(value: object) -> float:
    parsed = pd.to_numeric(value, errors="coerce")
    return float(parsed) if pd.notna(parsed) else 0.0


def _optional_number(value: object) -> float | None:
    parsed = pd.to_numeric(value, errors="coerce")
    return float(parsed) if pd.notna(parsed) else None


def _value(value: float | None) -> float:
    return value if value is not None else 0.0


def _log_value(value: float | None) -> float:
    return math.log1p(_value(value))


def _rate(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0
