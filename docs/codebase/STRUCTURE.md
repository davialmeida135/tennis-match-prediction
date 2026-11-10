# Codebase Structure

## Top-Level Map

| Path | Purpose |
| --- | --- |
| `src/tennis_match_prediction/` | Installed Python package |
| `tests/` | pytest suite |
| `data/raw/`, `data/staging/`, `data/curated/` | Source files, intermediate Parquet, published datasets/history |
| `data/models/`, `data/mlflow/`, `data/experiments/` | Local ML artifacts and tracking/experiment output |
| `docs/` | Design, quality and experiment documentation |
| `notebooks/` | Exploratory notebooks |
| `dagster_home/` | Local orchestrator configuration |
| `docker/` | MLflow image and startup |
| `.github/workflows/` | CI tests, definition validation, Ruff and Pylint |

## Entry Points

`workspace.yaml` loads `pipelines.defs`, which composes historical dataset, model training and daily update definitions. Python module CLIs include `web`, `ml.train`, `ml.predict`, `ml.feature_quality`, `pipelines.historical_matches.refresh_history`, `workflows.daily_update`, and `forecast_evaluation` (all prefixed with `tennis_match_prediction`).

## Module Boundaries

`pipelines/` owns ingestion and Dagster orchestration; `transforms/` owns shared data/feature calculations; `ml/` owns fitting, offline evaluation, artifacts, tracking and inference. `forecast_evaluation/` owns reconciliation of archived forecasts, prospective metrics and report publication. `workflows/daily_update.py` coordinates history refresh and forecast evaluation. `contracts/` groups schemas by responsibility; `paths.py` centralizes paths. `storage/` holds shared persistence primitives. Live Tennis API callers define their base URL locally. `web/` serves the personal application's HTTP API and static interface, including upcoming-match batch prediction.

## Naming and Organization Rules

Runtime outputs also include `data/predictions/` for batches with source/model/history copies, `data/performance/runs/` for observations/reports, `data/maintenance/` for daily stage reports, and `data/curated/.refreshes/` for isolated historical rebuilds. See `web/__main__.py`, `forecast_evaluation/service.py`, `workflows/daily_update.py`, and `pipelines/historical_matches/refresh_history.py` under the package.

Snake-case modules/functions, PascalCase contracts and model adapters, uppercase configuration constants. Absolute package imports use `tennis_match_prediction`. Source is organized by responsibility with historical ingestion grouped under one pipeline.

## Evidence

- `README.md`, `workspace.yaml`, `src/tennis_match_prediction/paths.py`.
- `src/tennis_match_prediction/{contracts/,ml/train.py,ml/predict.py,pipelines/defs.py}`.
- Source inventory obtained with `rg --files`; no `AGENTS.md` found in the reviewed project inventory.
- `.github/workflows/pylint.yml` and the runtime-output modules named above.
