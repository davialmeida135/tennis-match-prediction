# External Integrations

## Integration Inventory

| System | Purpose | Access/auth |
| --- | --- | --- |
| TennisMyLife catalog and annual downloads | Historical ATP data | Public requests HTTPS calls |
| MLflow server/local backend | Metrics, parameters, provenance, model artifacts | Configured URI; local Compose binds loopback |
| Dagster local instance | Asset orchestration and metadata | Local workspace and SQLite; optional MySQL |

Live Tennis API provides upcoming matches and results using `X-API-Key` from `LIVE_TENNIS_API_KEY`. Upcoming players resolve through unique normalized full names; results reconcile by provider match and participant IDs. MLflow Model Registry optionally resolves a model alias/version to its originating run's artifacts at web startup.

## Data Stores

Annual/consolidated CSVs and retained ongoing results; intermediate Parquet; curated exports and history JSON payload inside Parquet; model pickles and JSON metadata. Immutable upcoming batches copy source/model/history files. Performance runs retain observations and JSON/Markdown reports. MLflow uses PostgreSQL/volume artifacts in Compose or local SQLite/files. These tracking stores are not a match/prediction application database.

## Secrets and Credentials Handling

Dotenv/environment configuration; `.env` ignored. `.env.example` and Compose contain local development PostgreSQL defaults. [TODO] Public deployment credentials/authentication/rotation. Private `.env` was not read for this review.

## Reliability and Failure Behavior

Historical requests have configurable timeout, default 60 seconds, and raise HTTP errors. Hashes detect changes; required columns are validated before annual publication; atomic replacement retries Windows sharing violations. Annual downloads are sequential, without an HTTP retry/backoff wrapper. Upcoming/result calls have 30-second timeouts, disable redirects, and have budgets of five pages and 20 result requests respectively. Upcoming source responses cache for one minute. Failed collection preserves the previous publication. Registry downloads happen once per startup; failures do not select an unrelated local model.

## Observability for Integrations

Dagster metadata/checks/logs; MLflow metrics/provenance; refresh hashes/timings; batch skip counts; versioned forecast decisions; daily stage reports. `/api/status` exposes history identity and reload errors. [TODO] External freshness alerts and tracing configuration.

## Evidence

- `src/tennis_match_prediction/pipelines/historical_matches/config.py`, `assets.py`.
- `src/tennis_match_prediction/ml/{tracking,config,predict}.py`.
- `.env.example`, `.gitignore`, `docker-compose.yaml`, `dagster_home/dagster.yaml`.
- `src/tennis_match_prediction/{providers/live_tennis.py,web/upcoming.py,web/registry.py,forecast_evaluation/service.py,workflows/daily_update.py}`.
