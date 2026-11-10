# Codebase Concerns

Reviewed November 8, 2026. This inventory describes source behavior, not a certification of a deployed service or a trained model.

## Top Risks (Prioritized)

| Priority | Concern | Impact/action | Evidence |
| --- | --- | --- | --- |
| High | Source dates are not official match timestamps | Preserve conservative batching; stronger chronology requires verified timing evidence. | `transforms/history.py`, `forecast_evaluation/reconciliation.py` |
| High | README says localhost-only; web CLI binds all interfaces and routes have no authentication layer | Confirm intended exposure and align binding, deployment, and documentation. | `README.md`, `web/__main__.py`, `web/app.py` |
| Medium | Latest-state inference cannot reconstruct past player state | Preserve forecast snapshots; do not evaluate past matches using current state. | `ml/predict.py`, `web/upcoming.py` |
| Medium | Provider mapping requires unique normalized names; H2H uses opponent names | Missing/ambiguous identities reduce coverage; use verified ID/alias mapping if required. | `web/upcoming.py`, `transforms/player_comparison.py` |
| Medium | Unavailable comparisons become zero | Measure source coverage and consider explicit missingness in feature experiments. | `transforms/player_comparison.py`, `ml/feature_quality.py` |
| Medium | Probability calibration is not established | Interpret reported probabilities with prospective sample sizes and descriptive bins. | `README.md`, `forecast_evaluation/metrics.py` |

## Technical Debt

README quick start references absent `compose.mlflow.yaml`; the checked-in file is `docker-compose.yaml`. Its refresh section says restart is required and ongoing ingestion/scheduling remain planned, while current code implements validated history reload, ongoing ingestion, and a daily schedule. These stale statements can misdirect operations. This review adds `docs/code-walkthrough.md` and updates the codebase references; it does not rewrite the operational README sections.

The training export is wider than the 12 shared inference features. Custom training feature selection can produce an incompatible serving model, although web discovery filters incompatible metadata. Default local selection is the newest compatible artifact by modification time, not a metric-based choice.

History refresh still fully replays changed inputs. Compatibility exports are individually atomic, not one transaction. Web readers follow an immutable snapshot pointer; direct CSV consumers should wait for successful publication.

## Security Concerns

`ml/artifacts.py` loads trusted Python pickles. Local and registry model selection preserve that trust assumption; the web application has no model-upload endpoint. The web CLI binds `0.0.0.0`, and reviewed routes have no authentication layer. [TODO] Actual deployed firewall/proxy exposure. Compose supplies local database credential defaults and loopback MLflow port binding. Private `.env` contents were not inspected.

## Performance and Scaling Concerns

Full history replay and retained refresh attempts cost runtime/storage. Every upcoming batch copies its model/history, so disk use grows with saved batches. Standalone `predict()` reloads artifacts each invocation; the web service caches models and holds a frozen history per service instance. Upcoming refresh locks/caches are process-local. [TODO] Current runtime/memory benchmarks and multi-process deployment requirements; no new performance benchmark was run.

## Fragile/High-Churn Areas

In the last 40 commits touching source, tests, or README, README appears 30 times, history-pipeline tests 14 times, and web-app tests nine times. These counts come from grouping `git log -40 --name-only --pretty=format: -- src tests README.md` by path. Check temporal semantics, feature orientation, artifact compatibility, and history reload when changing these boundaries. Legacy filenames in old commits do not imply those modules still exist.

The broad skill scan traversed cached/generated content, so its aggregate code metrics are not used here. A direct `rg -n 'TODO|FIXME|HACK' src` returned no matches; roadmap items still exist in `TODO.md` and documentation. Test coverage gaps should be assessed separately from production debt.

## [ASK USER] Questions

1. [ASK USER] Should the web app intentionally be reachable from other machines, or should its default binding match the README's localhost-only description? This decision does not block the walkthrough.

## Evidence

- `README.md`, `TODO.md`, `docker-compose.yaml`, `.github/workflows/pylint.yml`.
- `src/tennis_match_prediction/transforms/{history,player_comparison,player_history}.py`.
- `src/tennis_match_prediction/{ml/artifacts.py,ml/predict.py,ml/feature_quality.py,web/__main__.py,web/app.py,web/service.py,web/upcoming.py}`.
- `src/tennis_match_prediction/{forecast_evaluation/reconciliation.py,forecast_evaluation/metrics.py,workflows/daily_update.py,pipelines/historical_matches/refresh_history.py}`.
- November 8 terminal source search, recent-commit path counts, and successful Dagster definition validation.
