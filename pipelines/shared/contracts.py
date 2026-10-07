"""Typed contracts at the boundaries of the prediction pipeline."""

from datetime import date
from pathlib import Path
from typing import Annotated, Literal

import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ContractModel(BaseModel):
    """Common validation policy for pipeline data models."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class FutureMatchRequest(ContractModel):
    """Minimum information needed to produce a pre-match prediction."""

    model_config = ConfigDict(str_strip_whitespace=True)

    player0_name: str = Field(min_length=1)
    player1_name: str = Field(min_length=1)
    match_date: date
    surface: str

    @field_validator("surface")
    @classmethod
    def normalize_surface(cls, value: str) -> str:
        """Accept source casing while keeping the feature schema finite."""
        normalized = value.title()
        if normalized not in {"Hard", "Clay", "Grass", "Carpet"}:
            raise ValueError("surface must be Hard, Clay, Grass, or Carpet")
        return normalized


class Prediction(ContractModel):
    """A prediction with enough provenance to reproduce it."""

    player0_name: str
    player1_name: str
    player0_win_probability: float = Field(ge=0, le=1)
    player1_win_probability: float = Field(ge=0, le=1)
    feature_cutoff: date
    model_path: str


INITIAL_ELO = 1500.0

NonnegativeFloat = Annotated[float, Field(ge=0)]
NonnegativeInt = Annotated[int, Field(ge=0, strict=True)]


class PlayedMatch(ContractModel):
    """Minimal past result retained for rolling windows and workload."""

    played_on: str
    surface: str
    won: bool = Field(strict=True)
    minutes: NonnegativeFloat

    @field_validator("played_on")
    @classmethod
    def calendar_date(cls, value: str) -> str:
        return date.fromisoformat(value).isoformat()


class PlayerState(ContractModel):
    """Accumulated player information, shared by the engine and its checkpoints."""

    name: str = Field(min_length=1)
    rating: float = INITIAL_ELO
    surface_ratings: dict[str, float] = Field(default_factory=dict)
    wins: NonnegativeInt = 0
    losses: NonnegativeInt = 0
    h2h_wins: dict[str, NonnegativeInt] = Field(default_factory=dict)
    history: list[PlayedMatch] = Field(default_factory=list, max_length=50)
    serve_points: NonnegativeFloat = 0.0
    aces: NonnegativeFloat = 0.0
    double_faults: NonnegativeFloat = 0.0
    service_points_won: NonnegativeFloat = 0.0
    rank: Annotated[float, Field(gt=0)] | None = None
    rank_points: NonnegativeFloat | None = None
    age: NonnegativeFloat | None = None


class FeatureStateCheckpoint(ContractModel):
    version: Literal[1]
    last_result_date: date | None
    players: dict[str, PlayerState]

    @model_validator(mode="after")
    def history_matches_cutoff(self) -> "FeatureStateCheckpoint":
        for player_id, player in self.players.items():
            if not player_id.strip():
                raise ValueError("player IDs must be nonblank")
            dates = [date.fromisoformat(item.played_on) for item in player.history]
            if dates and (self.last_result_date is None or max(dates) > self.last_result_date):
                raise ValueError("player history exceeds the checkpoint cutoff")
            if dates != sorted(dates, reverse=True):
                raise ValueError("player history must be newest first")
        return self


class DataFrameContract(ContractModel):
    """Required columns and invariants shared by transforms and orchestration checks."""

    model_config = ConfigDict(frozen=True)

    name: str
    required: tuple[str, ...]
    numeric: tuple[str, ...] = ()
    identifiers: tuple[str, ...] = ()
    date_column: str | None = None
    parsed_dates: bool = False
    target: str | None = None
    exact_columns: bool = False

    def errors(self, frame: pd.DataFrame) -> list[str]:
        """Return actionable failures, including missing and duplicate columns."""
        errors = []
        if not frame.columns.is_unique:
            return ["duplicate columns"]
        missing = sorted(set(self.required).difference(frame.columns))
        if missing:
            return [f"missing columns: {missing}"]
        if self.exact_columns and tuple(frame.columns) != self.required:
            errors.append("column set or order does not match the contract")
        for column in self.numeric:
            values = frame[column]
            if (
                not is_numeric_dtype(values.dtype)
                or values.isna().any()
                or not np.isfinite(values.to_numpy(dtype=float)).all()
            ):
                errors.append(f"{column}: expected numeric dtype with finite, non-null values")
        for column in self.identifiers:
            values = frame[column].astype("string")
            if values.isna().any() or values.str.strip().eq("").any():
                errors.append(f"{column}: expected non-null, nonblank identifiers")
        if self.date_column is not None:
            values = frame[self.date_column]
            if self.parsed_dates and not is_datetime64_any_dtype(values.dtype):
                errors.append(f"{self.date_column}: expected parsed datetime dtype")
            dates = pd.to_datetime(values, format="ISO8601", errors="coerce")
            if dates.isna().any():
                errors.append(f"{self.date_column}: invalid or missing dates")
            elif not dates.is_monotonic_increasing:
                errors.append(f"{self.date_column}: expected chronological order")
        if self.target is not None and not frame[self.target].isin([0, 1]).all():
            errors.append(f"{self.target}: expected non-null binary target")
        return errors

    def validate_frame(self, frame: pd.DataFrame) -> None:
        """Reject invalid frames before they are consumed or published."""
        errors = self.errors(frame)
        if errors:
            raise ValueError(f"{self.name} contract failed: {'; '.join(errors)}")


NORMALIZED_REQUIRED_COLUMNS = (
    "tourney_date",
    "tourney_id",
    "tourney_level",
    "surface",
    "score",
    "draw_size",
    "match_num",
    "winner_id",
    "loser_id",
    "winner_name",
    "loser_name",
    "winner_rank",
    "loser_rank",
)
NORMALIZED_MATCHES = DataFrameContract(
    name="normalized matches",
    required=NORMALIZED_REQUIRED_COLUMNS,
    identifiers=("winner_id", "loser_id", "winner_name", "loser_name"),
    date_column="tourney_date",
    parsed_dates=True,
)


class DownloadedSeason(ContractModel):
    """Metadata recorded for one downloaded source file."""

    model_config = ConfigDict(frozen=True)

    year: int
    path: Path
    sha256: str
    changed: bool


FEATURE_COLUMNS = (
    "overall_elo_diff",
    "surface_elo_diff",
    "rank_log_advantage",
    "points_log_diff",
    "form_10_diff",
    "surface_form_10_diff",
    "minutes_7d_diff",
    "matches_14d_diff",
    "age_diff",
    "h2h_log_odds",
    "experience_log_diff",
    "ace_rate_diff",
    "double_fault_rate_diff",
    "service_points_won_diff",
)

TRAINING_MATCHES = DataFrameContract(
    name="temporal training matches",
    required=(*FEATURE_COLUMNS, "winner", "match_date"),
    numeric=FEATURE_COLUMNS,
    date_column="match_date",
    target="winner",
    exact_columns=True,
)
