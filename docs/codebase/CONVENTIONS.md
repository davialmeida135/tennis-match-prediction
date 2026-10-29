# Coding Conventions

## Naming Rules

Snake-case files/functions (`player_history.py`, `calculate_player_comparison`); PascalCase types (`PlayerHistory`, `ModelArtifact`); uppercase constants (`TRAINING_FEATURE_COLUMNS`). Private helpers use leading underscores (`_apply_result`).

## Formatting and Linting

Ruff: Python 3.12, 100 columns, E/F/I/UP/B/SIM rules with documented exceptions. Pylint configuration exempts legacy transforms and decorated Dagster patterns. CI runs `uv run ruff check src tests` and pipeline Pylint. [TODO] Whether automatic formatting is required; no dedicated format CI step found.

## Import and Module Conventions

Absolute installed-package imports; Ruff I enforces import organization. Prediction defers heavy imports until after CLI parsing and validation. Model types share `BaseMatchModel`. [TODO] Formal public-export policy.

## Error and Logging Conventions

Contracts validate inputs/dataframes; missing files and unsupported configurations raise exceptions. Prediction CLI converts expected errors to argparse errors. Dagster assets use context logging and metadata; MLflow records experiment provenance/failures. [TODO] Standard structured log fields and redaction policy for a future HTTP layer.

## Testing Conventions

Root `tests/test_*.py`, pytest assertions, parameterization, tmp_path and monkeypatch. Remote MLflow checks opt in via environment. No coverage threshold configured in pyproject or reviewed CI.

## Evidence

- `pyproject.toml`, `.github/workflows/pylint.yml`.
- `src/tennis_match_prediction/{contracts/,ml/predict.py,transforms/player_history.py}`.
- `tests/test_ml_experiments.py`, `tests/test_mlflow_integration.py`.
