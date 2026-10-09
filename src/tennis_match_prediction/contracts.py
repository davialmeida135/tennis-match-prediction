"""Typed contracts at the boundaries of the prediction pipeline."""

from datetime import date
from pathlib import Path
from typing import Annotated, Literal

import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype
from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator


class ContractModel(BaseModel):
    """Common validation policy for pipeline data models."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class LogisticRegressionSettings(ContractModel):
    C: float = Field(default=1.0, gt=0)
    max_iter: int = Field(default=1000, gt=0, strict=True)
    random_state: int = Field(default=42, ge=0, strict=True)
    class_weight: Literal["balanced"] | None = None


class RandomForestSettings(ContractModel):
    n_estimators: int = Field(default=200, gt=0, strict=True)
    max_depth: int | None = Field(default=None, gt=0, strict=True)
    min_samples_leaf: int = Field(default=5, gt=0, strict=True)
    max_features: Literal["sqrt", "log2"] | None = "sqrt"
    random_state: int = Field(default=42, ge=0, strict=True)
    n_jobs: int = Field(default=1, ge=1, strict=True)
    class_weight: Literal["balanced", "balanced_subsample"] | None = None


class TrackingSettings(ContractModel):
    mode: Literal["server", "local", "disabled"] = "server"
    uri: str = "http://127.0.0.1:5000"
    experiment_name: str = Field(default="tennis-match-prediction", min_length=1)
    local_artifact_dir: Path | None = None

    @model_validator(mode="after")
    def validate_destination(self) -> "TrackingSettings":
        if self.mode == "server" and not self.uri.startswith(("http://", "https://")):
            raise ValueError("Server tracking requires an HTTP(S) tracking URI")
        if self.mode == "local" and (
            not self.uri.startswith("sqlite:///") or self.local_artifact_dir is None
        ):
            raise ValueError("Local tracking requires a SQLite URI and local artifact directory")
        return self


class ExperimentConfig(ContractModel):
    training_matches_path: Path
    output_dir: Path
    model_path: Path | None = None
    train_start: date
    validation_start: date
    test_start: date
    feature_columns: tuple[str, ...]
    model_name: str = "logistic_regression"
    model_params: dict[str, JsonValue] = Field(default_factory=dict)
    final_evaluation: bool = False
    run_name: str | None = None
    tracking: TrackingSettings = Field(default_factory=TrackingSettings)

    @field_validator("model_path")
    @classmethod
    def validate_model_path(cls, value: Path | None) -> Path | None:
        if value is not None and value.suffix != ".pkl":
            raise ValueError("Explicit model_path must end in .pkl")
        return value

    @field_validator("feature_columns")
    @classmethod
    def validate_features(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("Select at least one training feature")
        if len(set(value)) != len(value):
            raise ValueError("Training features must not contain duplicates")
        unknown = set(value).difference(PLAYER_COMPARISON_FEATURE_COLUMNS)
        if unknown:
            raise ValueError(f"Unknown training features: {sorted(unknown)}")
        return value

    @model_validator(mode="after")
    def validate_dates(self) -> "ExperimentConfig":
        if not self.train_start < self.validation_start < self.test_start:
            raise ValueError("Dates must satisfy train_start < validation_start < test_start")
        return self


class ExperimentResult(ContractModel):
    model_name: str
    model_path: Path
    metrics: dict[str, float]
    run_id: str | None = None
    artifact_uri: str | None = None
    sklearn_model_uri: str | None = None


class ModelMetadata(ContractModel):
    schema_version: Literal[1]
    model_name: str
    feature_columns: tuple[str, ...]
    target_convention: Literal["winner=1 means player1 won"]
    history_order: Literal["source_date_match_num"]
    config: ExperimentConfig
    split: dict[str, JsonValue]
    dataset_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    provenance: dict[str, JsonValue]
    metrics: dict[str, float]
    model_params: dict[str, JsonValue]
    run_id: str | None = None
    sklearn_model_uri: str | None = None

    @model_validator(mode="after")
    def consistent_configuration(self) -> "ModelMetadata":
        if self.feature_columns != self.config.feature_columns:
            raise ValueError("Artifact feature order differs from its configuration")
        if self.model_name != self.config.model_name:
            raise ValueError("Artifact model differs from its configuration")
        return self


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
    history_source_date: date
    model_path: str


INITIAL_ELO = 1500.0

NonnegativeFloat = Annotated[float, Field(ge=0)]
NonnegativeInt = Annotated[int, Field(ge=0, strict=True)]


class PlayedMatch(ContractModel):
    """Past result in source-date/match-number order; no inferred calendar day."""

    tourney_date: date
    surface: str
    won: bool = Field(strict=True)


class PlayerState(ContractModel):
    """Accumulated player information, used by historical transforms and prediction."""

    name: str = Field(min_length=1)
    rating: float = INITIAL_ELO
    surface_ratings: dict[str, float] = Field(default_factory=dict)
    wins: NonnegativeInt = 0
    losses: NonnegativeInt = 0
    h2h_wins: dict[str, NonnegativeInt] = Field(default_factory=dict)
    history: list[PlayedMatch] = Field(default_factory=list, max_length=50)
    surface_history: dict[str, list[PlayedMatch]] = Field(default_factory=dict)
    serve_points: NonnegativeFloat = 0.0
    ace_serve_points: NonnegativeFloat = 0.0
    double_fault_serve_points: NonnegativeFloat = 0.0
    service_won_serve_points: NonnegativeFloat = 0.0
    aces: NonnegativeFloat = 0.0
    double_faults: NonnegativeFloat = 0.0
    service_points_won: NonnegativeFloat = 0.0
    rank: Annotated[float, Field(gt=0)] | None = None
    rank_points: NonnegativeFloat | None = None
    age: NonnegativeFloat | None = None


class PlayerHistory(ContractModel):
    last_source_date: date | None
    players: dict[str, PlayerState]

    @model_validator(mode="after")
    def history_matches_cutoff(self) -> "PlayerHistory":
        for player_id, player in self.players.items():
            if not player_id.strip():
                raise ValueError("player IDs must be nonblank")
            dates = [item.tourney_date for item in player.history]
            if dates and (self.last_source_date is None or max(dates) > self.last_source_date):
                raise ValueError("player history exceeds the player-history cutoff")
            if dates != sorted(dates, reverse=True):
                raise ValueError("player history must be newest first")
            for surface, matches in player.surface_history.items():
                if len(matches) > 10 or any(item.surface != surface for item in matches):
                    raise ValueError("surface history must contain at most 10 matching results")
                surface_dates = [item.tourney_date for item in matches]
                if surface_dates and (
                    self.last_source_date is None or max(surface_dates) > self.last_source_date
                ):
                    raise ValueError("surface history exceeds the player-history cutoff")
                if surface_dates != sorted(surface_dates, reverse=True):
                    raise ValueError("surface history must be newest first")
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


PLAYER_COMPARISON_FEATURE_COLUMNS = (
    "overall_elo_diff",
    "surface_elo_diff",
    "rank_log_advantage",
    "points_log_diff",
    "form_10_diff",
    "surface_form_10_diff",
    "age_diff",
    "h2h_log_odds",
    "experience_log_diff",
    "ace_rate_diff",
    "double_fault_rate_diff",
    "service_points_won_diff",
)
