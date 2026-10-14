"""Editable training defaults, shared by the Python API and optional CLI overrides."""

import os
from datetime import date
from pathlib import Path

from tennis_match_prediction.contracts import DataFrameContract, TrackingSettings
from tennis_match_prediction.paths import CURATED_DIR, DATA_DIR, TRAINING_MATCHES_CSV_NAME

# Edit these defaults, then call train() or run the training module without arguments.
TRAINING_MATCHES_PATH = CURATED_DIR / TRAINING_MATCHES_CSV_NAME

# Earlier source dates build history only; they are excluded from fitting/evaluation.
TRAIN_START = date(2017, 1, 1)
VALIDATION_START = date(2024, 1, 1)
TEST_START = date(2025, 1, 1)

# Remove or reorder entries to configure the default training experiment.
TRAINING_FEATURE_COLUMNS: tuple[str, ...] = (
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

TRAINING_MATCHES = DataFrameContract(
    name="temporal training matches",
    required=(*TRAINING_FEATURE_COLUMNS, "winner", "tourney_date"),
    numeric=TRAINING_FEATURE_COLUMNS,
    date_column="tourney_date",
    target="winner",
    exact_columns=True,
)


def tracking_settings(
    *, mode: str | None, uri: str | None, experiment_name: str | None
) -> TrackingSettings:
    """Resolve CLI/Python overrides, then environment, then project defaults."""
    selected_mode = mode or os.getenv("TENNIS_MLFLOW_MODE") or "server"
    local_root = Path(os.getenv("TENNIS_MLFLOW_LOCAL_DIR") or DATA_DIR / "mlflow").resolve()
    default_uri = (
        f"sqlite:///{(local_root / 'mlflow.db').as_posix()}"
        if selected_mode == "local"
        else "http://127.0.0.1:5000"
    )
    # Local mode deliberately ignores the server URI in the usual .env file.
    selected_uri = (
        uri
        or (os.getenv("MLFLOW_TRACKING_URI") if selected_mode == "server" else None)
        or default_uri
    )
    return TrackingSettings(
        mode=selected_mode,
        uri=selected_uri,
        experiment_name=experiment_name
        or os.getenv("MLFLOW_EXPERIMENT_NAME")
        or "tennis-match-prediction",
        local_artifact_dir=local_root / "artifacts" if selected_mode == "local" else None,
    )
