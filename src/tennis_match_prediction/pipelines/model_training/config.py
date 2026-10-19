"""Launchpad controls for the shared training runner."""

from typing import Any, Literal

from dagster import Config
from pydantic import Field

from tennis_match_prediction.ml.config import (
    TEST_START,
    TRAIN_START,
    TRAINING_FEATURE_COLUMNS,
    TRAINING_MATCHES_PATH,
    VALIDATION_START,
)
from tennis_match_prediction.paths import MODELS_DIR


class ModelTrainingConfig(Config):
    """One candidate experiment; defaults match the Python and CLI entry points."""

    model_name: Literal[
        "logistic_regression", "random_forest", "extra_trees", "hist_gradient_boosting", "xgboost"
    ] = "logistic_regression"
    model_params: dict[str, Any] = Field(
        default_factory=dict,
        description="Parameter overrides validated by the selected model's settings.",
    )
    feature_columns: list[str] = Field(
        default_factory=lambda: list(TRAINING_FEATURE_COLUMNS),
        description="Ordered feature columns from the published training CSV.",
    )
    train_start: str = Field(
        default=TRAIN_START.isoformat(), description="Inclusive fitting start date (YYYY-MM-DD)."
    )
    validation_start: str = Field(
        default=VALIDATION_START.isoformat(),
        description="Inclusive validation start; fitting stops before this date (YYYY-MM-DD).",
    )
    test_start: str = Field(
        default=TEST_START.isoformat(),
        description="Inclusive test start; validation stops before this date (YYYY-MM-DD).",
    )
    final_evaluation: bool = Field(
        default=False,
        description="Also score the held-out test period. Enable only after selecting a candidate.",
    )
    training_matches_path: str = Field(
        default=str(TRAINING_MATCHES_PATH),
        description="Published training_atp_matches_csv file; override to relocate that export.",
    )
    output_dir: str = Field(
        default=str(MODELS_DIR), description="Root for unique candidate artifact directories."
    )
    tracking_mode: str | None = Field(
        default=None,
        description="server, local or disabled. Null uses TENNIS_MLFLOW_MODE, then server.",
    )
    tracking_uri: str | None = Field(
        default=None, description="Null uses the selected tracking mode's environment/default URI."
    )
    experiment_name: str | None = Field(
        default=None, description="Null uses MLFLOW_EXPERIMENT_NAME, then tennis-match-prediction."
    )
    run_name: str | None = Field(default=None, description="Optional MLflow experiment run name.")
