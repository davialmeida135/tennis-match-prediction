# Testing Patterns

## Test Stack and Commands

pytest >=8 with plain assertions, parameterization, monkeypatch and temporary directories.

```powershell
uv run pytest -q
uv run pytest tests/test_player_history.py -q
$env:TENNIS_TEST_MLFLOW_HTTP = '1'
uv run pytest tests/test_mlflow_integration.py -q
```

Docker integration uses `TENNIS_TEST_MLFLOW_DOCKER=1`. [TODO] Coverage command/tool; no coverage threshold is configured.

## Test Layout

`tests/test_*.py`; pyproject sets `testpaths = ["tests"]`. Tests cover contracts, ingestion, chronology, artifacts, models, prediction CLI, web routes, batches, registry startup, daily maintenance, and forecast reconciliation. Remote HTTP/Compose checks opt in. Representative modules define their own fixtures.

## Test Scope Matrix

| Scope | Present | Examples |
| --- | --- | --- |
| Unit | Yes | History ordering, curation, feature formulas, validation |
| Integration | Yes | Parquet, model round trips, local MLflow, optional remote restart/persistence |
| HTTP/API integration | Yes | FastAPI `TestClient`, batch archives, registry mocks and reloads |
| Browser E2E | Not found in pytest inventory | Browser behavior needs separate verification |

## Mocking and Isolation Strategy

`tmp_path` isolates file artifacts; monkeypatch controls environment/network dependencies. Remote tests manage an owned server or Compose project and require `TENNIS_TEST_MLFLOW_HTTP=1` or `TENNIS_TEST_MLFLOW_DOCKER=1`. Older documentation's environment failures do not establish a current flaky-test diagnosis.

## Coverage and Quality Signals

[TODO] Measured line/branch coverage; no coverage threshold is configured in the reviewed manifest/workflow. CI runs pytest, Dagster definition validation, Ruff, and pipeline Pylint. The November 8 documentation review ran `Definitions.validate_loadable(defs)` successfully using `.venv/Scripts/python.exe`. It did not rerun the full regression suite, train models, or collect live results. The `uv run --no-sync dg list defs --json` attempt failed on uv cache access before invoking dg; direct Python validation supplied the definition check.

## Evidence

- `pyproject.toml`, `.github/workflows/pylint.yml`.
- `tests/test_player_history.py`, `tests/test_ml_experiments.py`, `tests/test_mlflow_integration.py`, `tests/test_ingestion.py`.
- `tests/test_web_app.py`, `tests/test_daily_update.py`, `tests/test_forecast_evaluation.py`; November 8 terminal output from successful definition validation.
