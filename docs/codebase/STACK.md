# Technology Stack

## Runtime Summary

Python 3.12 (`>=3.12,<3.13`), uv dependency management, setuptools editable `src` package. No application frontend framework or HTTP API dependency is declared.

## Production Frameworks and Dependencies

Manifest constraints: pandas >=2.2, polars >=1.29, NumPy >=1.26, scikit-learn >=1.4, PyArrow >=14, requests >=2.31; Dagster/webserver >=1.13.25,<1.14; python-dotenv >=1.0; Pydantic >=2,<3; MLflow ==3.16.1; XGBoost >=3.1,<4. Optional MySQL storage uses dagster-mysql >=0.29.25,<0.30. Notebook extras provide JupyterLab, matplotlib, seaborn, IPython and missingno.

## Development Toolchain

pytest >=8, Ruff >=0.6, Pylint >=3.1. GitHub Actions runs lint checks. Docker Compose provisions PostgreSQL 17.6 and MLflow; Docker MLflow startup is in `docker/mlflow/start.py`.

## Key Commands

```powershell
uv sync
uv run pytest -q
uv run ruff check src tests
uv run pylint src/tennis_match_prediction/pipelines
uv run dagster dev
docker compose -f docker-compose.yaml up -d --build --wait
```

## Environment and Config

`.env.example` documents DAGSTER_HOME, TENNIS_DATA_DIR, TENNIS_RAW_MATCHES_CSV and MLflow settings. `paths.py` loads dotenv and supplies data-path defaults. `workspace.yaml` selects the Dagster code location. Local tracking can avoid Docker; server mode requires a reachable tracking server. [TODO] Public deployment configuration.

## Evidence

- `pyproject.toml`, `uv.lock`, `.env.example`, `workspace.yaml`.
- `docker-compose.yaml`, `docker/mlflow/Dockerfile`, `docker/mlflow/start.py`.
- `.github/workflows/pylint.yml`, `src/tennis_match_prediction/paths.py`.
