# External Integrations

## Integration Inventory

| System | Purpose | Access/auth |
| --- | --- | --- |
| TennisMyLife catalog and annual downloads | Historical ATP data | Public requests HTTPS calls |
| MLflow server/local backend | Metrics, parameters, provenance, model artifacts | Configured URI; local Compose binds loopback |
| Dagster local instance | Asset orchestration and metadata | Local workspace and SQLite; optional MySQL |

No fixture provider is integrated. [TODO] Provider coverage, cost, ID compatibility and permissions.

## Data Stores

Annual/consolidated CSVs; intermediate Parquet; curated exports and history JSON payload inside Parquet; model pickles and JSON metadata. MLflow uses PostgreSQL/volume artifacts in Compose or local SQLite/files. These tracking stores are not a match/prediction application database.

## Secrets and Credentials Handling

Dotenv/environment configuration; `.env` ignored. `.env.example` and Compose contain local development PostgreSQL defaults. [TODO] Public deployment credentials/authentication/rotation. Private `.env` was not read for this review.

## Reliability and Failure Behavior

Historical requests have configurable timeout, default 60 seconds, and raise HTTP errors. Download hashes detect changes; required columns are validated before publishing annual files; atomic replacement retries Windows sharing violations. Annual downloads are sequential. No HTTP retry/backoff wrapper appears in this resource. MLflow server errors do not silently fall back to local tracking.

## Observability for Integrations

Dagster asset metadata/checks and logs, MLflow experiment metrics/provenance. [TODO] Fixture synchronization metrics and freshness alerts; no implemented fixture flow exists.

## Evidence

- `src/tennis_match_prediction/pipelines/historical_matches/config.py`, `assets.py`.
- `src/tennis_match_prediction/ml/{tracking,config,predict}.py`.
- `.env.example`, `.gitignore`, `docker-compose.yaml`, `dagster_home/dagster.yaml`.
