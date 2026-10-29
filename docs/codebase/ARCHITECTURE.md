# Architecture

## Architectural Style

File-backed data pipeline plus shared ML experiment runner and prediction API. Source layers separate orchestration, transformations and ML. Constraints: chronological prior-state features, explicit feature order, latest-snapshot inference.

## System Flow

1. Historical raw resource downloads or consolidates annual ATP CSVs.
2. Normalize/filter/order history; calculate comparisons and persisted player state.
3. Compute win rates/Elo, impute prior observations and curate rows.
4. Orient player0/player1, construct targets and publish training CSV.
5. Training splits by source date, fits adapter, evaluates and saves artifact/provenance with optional MLflow tracking.
6. Prediction loads model/history, resolves names, computes shared comparisons and returns probabilities.
7. The web application archives upcoming-match forecasts and their source/model/history snapshots.
8. Daily updates refresh history and reconcile saved forecasts with observed provider outcomes; evaluation publishes versioned JSON and Markdown reports without retraining.

## Layer/Module Responsibilities

| Module | Owns | Boundary |
| --- | --- | --- |
| `pipelines/historical_matches` | Source resource, assets/checks/job | Uses shared transforms |
| `transforms` | Normalization, chronology, state and feature formulas | Independent of web presentation |
| `ml` | Training/evaluation/tracking/model inference | Consumes published contracts |
| `forecast_evaluation` | Forecast reconciliation, prospective metrics and reports | Uses provider configuration and shared storage; independent of web presentation |
| `workflows/daily_update.py` | Coordinates history refresh and forecast evaluation | Called by CLI or the Dagster daily update component |
| `pipelines/defs.py` | Composes history, training and daily update definitions | Application code location loaded by `workspace.yaml` |
| `storage`, `providers`, `diagnostics` | Persistence primitives, provider configuration and explicit experiments | Production modules do not import diagnostic code |
| `web` | HTTP API, static application, player/model access and upcoming forecasts | Uses shared prediction and storage logic |
| `contracts/`, `paths.py` | Schemas and storage configuration | Shared across layers |

## Reused Patterns

Model factory and adapters in `ml/models`; Pydantic contracts; Dagster decorated assets/checks and injected resources; Parquet IO manager. `paths.py` loads dotenv before resolving environment settings.

## Known Architectural Risks

Latest history cannot answer past as-of queries. The standalone prediction API reloads model/history on each call; the web service caches selected models and reloads published history. Source dates do not establish actual completion times. History refresh uses full replay rather than incremental replay. Model feature selection can exceed inference feature availability.

## Evidence

- `src/tennis_match_prediction/pipelines/historical_matches/{assets,defs,config}.py`.
- `src/tennis_match_prediction/pipelines/shared/parquet_io.py`.
- `src/tennis_match_prediction/transforms/{player_history,player_comparison,history}.py`.
- `src/tennis_match_prediction/ml/{experiment,dataset,predict}.py`, `ml/models/factory.py`.
- `src/tennis_match_prediction/{forecast_evaluation/,workflows/daily_update.py,pipelines/defs.py,web/service.py,web/upcoming.py}`.
