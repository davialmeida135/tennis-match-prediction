"""Experiment configuration, tracking settings, results and model metadata."""

from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import Field, JsonValue, field_validator, model_validator

from tennis_match_prediction.contracts.base import ContractModel


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
        # Allow extra published numeric features, but never the target or split date.
        reserved = set(value).intersection({"winner", "tourney_date"})
        if reserved:
            raise ValueError(f"Unknown training features (reserved columns): {sorted(reserved)}")
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
