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

## Layer/Module Responsibilities

| Module | Owns | Boundary |
| --- | --- | --- |
| `pipelines/historical_matches` | Source resource, assets/checks/job | Uses shared transforms |
| `transforms` | Normalization, chronology, state and feature formulas | Independent of web presentation |
| `ml` | Training/evaluation/tracking/model inference | Consumes published contracts |
| `contracts.py`, `paths.py` | Schemas and storage configuration | Shared across layers |

## Reused Patterns

Model factory and adapters in `ml/models`; Pydantic contracts; Dagster decorated assets/checks and injected resources; Parquet IO manager. `paths.py` loads dotenv before resolving environment settings.

## Known Architectural Risks

No HTTP service or application database. Latest history cannot answer past as-of queries. Prediction reloads model/history every call. Source dates do not establish actual completion times. Incremental replay and correction handling remain unimplemented. Model feature selection can exceed inference feature availability.

## Evidence

- `src/tennis_match_prediction/pipelines/historical_matches/{assets,defs,config}.py`.
- `src/tennis_match_prediction/pipelines/shared/parquet_io.py`.
- `src/tennis_match_prediction/transforms/{player_history,player_comparison,history}.py`.
- `src/tennis_match_prediction/ml/{experiment,dataset,predict}.py`, `ml/models/factory.py`.
