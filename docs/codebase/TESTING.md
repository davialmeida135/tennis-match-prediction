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

`tests/test_*.py`; pyproject sets `testpaths = ["tests"]`. Tests cover contracts, source ingestion, transformations, feature chronology, artifacts, models and prediction CLI. Remote HTTP/Compose checks opt in.

## Test Scope Matrix

| Scope | Present | Examples |
| --- | --- | --- |
| Unit | Yes | History ordering, curation, feature formulas, validation |
| Integration | Yes | Parquet, model round trips, local MLflow, optional remote restart/persistence |
| Browser E2E | No | No product frontend exists |

## Mocking and Isolation Strategy

tmp_path isolates file artifacts; monkeypatch controls environment/network dependencies. Remote tests manage an owned server or Compose project. This review's initial test run produced 78 passes and 54 setup errors from temporary-directory access; rerun uses an explicit workspace temp root. This environment issue should not be classified as a model failure.

## Coverage and Quality Signals

[TODO] Measured line/branch coverage. CI runs lint only; pytest and Dagster definition loading need adding. Review definition validation passed and real local prediction succeeded. Focused prediction, chronology, player-state/comparison and contract checks passed: 52 tests in 8.42 seconds. The workspace-temp full rerun stalled at registered pipeline materialization and was interrupted; this review does not certify the full suite.

## Evidence

- `pyproject.toml`, `.github/workflows/pylint.yml`.
- `tests/test_player_history.py`, `tests/test_ml_experiments.py`, `tests/test_mlflow_integration.py`, `tests/test_ingestion.py`.
- Review terminal output and `data/tmp/state-review/pytest-output.txt`.
