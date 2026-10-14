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
| `.github/workflows/` | CI lint |

## Entry Points

`workspace.yaml` loads `pipelines.historical_matches.defs`. Python module CLIs: `ml.train`, `ml.predict`, `ml.feature_quality`, `pipelines.historical_matches.refresh_history`. README mentions `ml.compare`, but that module is absent in the current inventory. No product web entry point exists.

## Module Boundaries

`pipelines/` owns ingestion and Dagster orchestration; `transforms/` owns shared data/feature calculations; `ml/` owns fitting, evaluation, artifacts, tracking and inference. Root `contracts.py` and `paths.py` centralize schemas and paths. Keep proposed HTTP presentation outside transformation logic.

## Naming and Organization Rules

Snake-case modules/functions, PascalCase contracts and model adapters, uppercase configuration constants. Absolute package imports use `tennis_match_prediction`. Source is organized by responsibility with historical ingestion grouped under one pipeline.

## Evidence

- `README.md`, `workspace.yaml`, `src/tennis_match_prediction/paths.py`.
- `src/tennis_match_prediction/{contracts.py,ml/train.py,ml/predict.py}`.
- Source inventory obtained with `rg --files`; no `AGENTS.md` found in the reviewed project inventory.
