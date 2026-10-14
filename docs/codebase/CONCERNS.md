# Codebase Concerns

## Top Risks (Prioritized)

| Severity | Concern | Impact/action | Evidence |
| --- | --- | --- | --- |
| High | Source dates are not completion timestamps | Establish safe pre-match chronology before daily automation | README; `transforms/history.py` |
| High | Report's selected model has 14 inputs, inference generates 12 | Use compatible model or implement matching features | model report; `ml/predict.py`; selected model JSON |
| High | Local history ends 2026-08-30 | Refresh and publish freshness/version information | Validated local Parquet inspection |
| Medium | Exact-name resolution, name-keyed H2H, no same-player guard | Adopt stable IDs/aliases and validation | `ml/predict.py`; `transforms/player_history.py`; `contracts.py` |
| Medium | No saved pre-match forecast/history identity | Add reproducible prediction records | `contracts.py` Prediction |

## Technical Debt

Missing referenced Compose filename and `ml.compare` module; cached-refresh documentation disagrees with `refresh=True` default. TODO contains implemented work still marked pending. CI omits tests/definition loading. Historical replay and incremental updates are open work, not prerequisites for the initial manual prediction page.

## Security Concerns

`ml/artifacts.py` deserializes pickle and explicitly documents trusted artifacts. A future web layer should choose server-owned models rather than accept arbitrary uploads/paths. Compose supplies local DB credential defaults and loopback MLflow binding; [TODO] public hosting/authentication decisions. No product HTTP service exists to assess yet.

## Performance and Scaling Concerns

`predict()` reads/deserializes history/model every invocation and scans player values for names. Cache versioned artifacts and an ID/name index when serving. Annual requests are sequential; history rebuild traverses all source batches. [TODO] Measured refresh/serving runtime and memory before incremental optimization. Source has a comment describing historical slow import startup; current smoke prediction took approximately six seconds including process startup.

## Fragile/High-Churn Areas

Scan's 90-day history reports README 25 edits, legacy historical asset path 15, history-pipeline tests 11, pyproject 9 and current contracts 6. Paths include pre-src-layout history; do not treat legacy filenames as current modules. Chronology/feature contracts need regression verification across training and inference. Scan counts include cached dependencies under `data/tmp`, so aggregate code metrics/TODO counts are not reliable project-only metrics.

## [ASK USER] Questions

1. [ASK USER] Is the initial page personal/local or publicly hosted?
2. [ASK USER] Are paid fixture providers acceptable, and what monthly budget applies?

## Evidence

- `docs/project-state-2026-10-14.md`, `docs/ml-model-comparison-2026-10-13.md`.
- `README.md`, `TODO.md`, `.github/workflows/pylint.yml`, `docker-compose.yaml`.
- `src/tennis_match_prediction/{contracts.py,ml/predict.py,ml/artifacts.py,transforms/player_history.py}`.
- Discovery scan and review terminal checks; `git log` confirms ongoing source-layout and ML refactors.
