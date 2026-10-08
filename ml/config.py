"""Editable training defaults, shared by the Python API and optional CLI overrides."""

from datetime import date

from pipelines.shared.contracts import DataFrameContract
from pipelines.shared.paths import CURATED_DIR, MODELS_DIR, TRAINING_MATCHES_CSV_NAME

# Edit these values, then call train() or run python -m ml.train without arguments.
TRAINING_MATCHES_PATH = CURATED_DIR / TRAINING_MATCHES_CSV_NAME
MODEL_PATH = MODELS_DIR / "match_winner.pkl"

# Earlier source dates build history only; they are excluded from fitting/evaluation.
TRAIN_START = date(2017, 1, 1)
VALIDATION_START = date(2024, 1, 1)
TEST_START = date(2025, 1, 1)

MAX_ITER = 1_000
RANDOM_STATE = 42

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
