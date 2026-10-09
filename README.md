# Tennis match prediction

Predict an ATP match from two player names, a date, and a surface. Historical
results come from TennisMyLife; experiments use chronological splits with logistic
regression or random forest. A Dagster player-history asset supplies each player's pre-match history.

## Start here

Use Python 3.12 and `uv`:

```powershell
uv sync
Copy-Item .env.example .env
docker compose -f compose.mlflow.yaml up -d --build --wait
uv run dagster job execute -m tennis_match_prediction.pipelines.historical_matches.defs -j materialize_historical_dataset
uv run python -m tennis_match_prediction.ml.train
```

Training prints a JSON result with `model_path`, metrics and MLflow run/model identifiers.
Use that path to predict a match on a date **after the latest source date in the player-history asset**:

```powershell
uv run python -m tennis_match_prediction.ml.predict "Carlos Alcaraz" "Jannik Sinner" 2027-01-01 Hard --model-path "data/models/<experiment-id>/model.pkl"
```

Both players must be present in the history. The prediction returns win
probabilities, the latest history source date, and the model path. Training also records
validation metrics and logs artifacts to MLflow at http://127.0.0.1:5000.
Test metrics require `--final-evaluation` after choosing a configuration.

To refresh selected seasons instead of downloading all seasons:

```powershell
uv run python -m tennis_match_prediction.pipelines.historical_matches.refresh_history 2025 2026
```

This materializes `raw_atp_matches` in Dagster, updates annual files, then
consolidates all locally available seasons and persists the raw Parquet snapshot.
Re-materialize the Dagster dataset after refreshing history, then retrain.

## How the code fits together

Importable code lives in `src/tennis_match_prediction/`. `uv sync` installs the
package in editable mode, so commands and tests use the installed package without
adding the repository root to `PYTHONPATH`. Tests, notebooks, docs, and data stay
outside `src/`.

`transforms/` holds data normalization, imputation, history calculations, feature
formulas, curation, and training-row construction. Pipelines and prediction import
this shared logic directly. `pipelines/` owns Dagster orchestration and ingestion;
`ml/` owns training and prediction. Shared contracts and filesystem configuration
live at the package root in `contracts.py` and `paths.py`.

Read these files in this order:

1. `src/tennis_match_prediction/pipelines/historical_matches/assets.py` — raw asset decides whether to reuse, consolidate or download source history.
2. `src/tennis_match_prediction/pipelines/historical_matches/config.py` — source resource handles annual downloads, validation and consolidation; `src/tennis_match_prediction/pipelines/historical_matches/refresh_history.py` materializes the raw asset with refresh enabled.
3. `src/tennis_match_prediction/contracts.py` — Pydantic models, feature names, and DataFrame validation.
4. `src/tennis_match_prediction/transforms/player_comparison.py` — pure formulas for the model features.
5. `src/tennis_match_prediction/transforms/player_history.py` — build match comparisons and player history in one chronological pass.
6. `src/tennis_match_prediction/ml/experiment.py` — prepare published rows, fit a model adapter, evaluate, track and save; `ml/train.py` provides the CLI and Python entry point.
7. `src/tennis_match_prediction/ml/predict.py` — load the model and player-history asset to predict a future match.

The model defaults to the 12 features in `TRAINING_FEATURE_COLUMNS` in `src/tennis_match_prediction/ml/config.py`. Edit that tuple to select or reorder model inputs; `PLAYER_COMPARISON_FEATURE_COLUMNS` defines the generated dataset schema.
Training randomly assigns the winner to player0 or player1 and gives the features
that same orientation. `winner=1` means player1 won.

All data models live in `contracts.py` and inherit from `ContractModel`.
`PlayerState`, `PlayedMatch` and `PlayerHistory` validate the prediction history
persisted by Dagster. DataFrame contracts check columns, dates, finite features, and
binary targets with vectorized operations.

## Dagster dataset workflow

There is one Dagster code location, configured in `workspace.yaml`:

```powershell
uv run dagster dev
```

Set `DAGSTER_HOME` in `.env` to the absolute path of this checkout's `dagster_home`
directory. Open `materialize_historical_dataset` in the UI to build the dataset.
The raw asset reuses an existing consolidated CSV. If it is missing, it
consolidates annual CSVs in the same directory; if none exist, it downloads
available seasons first. Existing CSVs are not refreshed automatically by age.

To request an update directly in the Dagster Launchpad, configure the resource:

```yaml
resources:
  raw_matches_csv:
    config:
      refresh: true
      first_year: 2020
```

`first_year` includes that season and every newer season available in the source
catalog, including new seasons as they appear. It also excludes older cached
seasons from consolidation and the raw asset. Use `years: [2025, 2026]` instead
to refresh specific seasons; configure either `first_year` or `years`, not both.
Omit both to download all available seasons. `csv_path` overrides the output
path and determines the directory containing annual files. The refresh CLI also
accepts `--csv-path`. Run the full dataset job with refresh enabled to update
source history and all downstream datasets together.

The assets in `src/tennis_match_prediction/pipelines/historical_matches/assets.py` run in this order:

```text
raw_atp_matches
  -> normalized_atp_matches (filter walkovers/missing IDs, clean surface, parse/order dates)
  -> player_comparison_atp_matches (12 pre-match comparisons + player_history asset)
  -> winrate_featured_atp_matches
  -> elo_featured_atp_matches
  -> imputed_atp_matches (earlier observations only)
  -> curated_atp_matches (encode context, drop per-match statistics)
  -> training_atp_matches (assign player positions, construct target)
  -> training_atp_matches_csv
```

`curated_atp_matches_csv` publishes the readable curated dataset directly from
`curated_atp_matches`. The curated and training exports are branches;
training-row construction does not depend on CSV publication.

- `transforms/history.py` supplies source-date parsing, numeric match-number ordering,
  shared history batches, and played-match filtering.
- `transforms/elo.py` owns overall and surface Elo updates and the formula shared
  by historical Elo columns and player history. Both players update from ratings
  frozen before their history batch.
- `transforms/curation.py` encodes categoricals and removes outcome statistics for
  `curated_atp_matches`. Walkover filtering belongs to `transforms/normalization.py`.
- `transforms/imputation.py` fills height/age from the pooled mean of earlier
  observed values, rank from the worst earlier rank, and points from the lowest
  earlier points. Both player sides share these references. Initial defaults are
  height 180 cm, age 25, rank 2000 and points 0; future rows never affect past fills.
- `transforms/training_rows.py` assigns winner/loser attributes to player0/player1 and
  orients signed features with the target. The asset uses random seed 42.
- `checks.py` checks intermediate and final datasets.
- `defs.py` registers the assets, checks, and job.
- `shared/parquet_io.py` persists DataFrames so individual steps can be rerun.

### History order and source-date limitations

[TennisMyLife](https://stats.tennismylife.org/tennis-match-database) documents
`tourney_date` as the tournament week. Local files have
mixed granularity: some recent tournaments have dates for individual rounds,
while older tournaments repeat the start date for every match. The pipeline
preserves that field as `tourney_date`; it does not invent a match calendar date.

All result-derived features use the same source-date/tournament/match-number
order. A larger numeric `match_num` within a tournament follows earlier matches,
even when their source dates are equal. Missing, nonpositive or duplicate numbers
within a source-date/tournament group make its order ambiguous: all its features
use the group's prior state, then its results are applied. When a player appears
in multiple tournaments on the same source date, the entire date uses one prior
state; tournament IDs do not establish chronology. Features never use the current
match's outcome. This is source sequence, not a verified order of match completion.

Calendar workload features (`minutes_7d_diff`, `matches_14d_diff`) are removed.
Recent form uses earlier matches in source sequence, including earlier rounds
sharing a source date. Models and history snapshots must be rebuilt; old formats
are rejected. Training datasets and comparison exports use `tourney_date`, not
`match_date`. Predictions expose `history_source_date`, not a result-completion
cutoff. The prediction date must exceed this source date, but that check cannot
establish when a tournament finished or validate an in-progress tournament.
Verified match dates are needed for daily workload or a calendar-accurate backtest.

This workflow is the single source of features for training and prediction.
`tennis_match_prediction.ml.train` reads `data/curated/training_atp_matches.csv` (or an explicit training
CSV path) and requires only the configured training features, target and
chronological date. Override the defaults for one run with
`uv run python -m tennis_match_prediction.ml.train --train-start 2017-01-01 --validation-start 2024-01-01 --test-start 2025-01-01 --features overall_elo_diff surface_elo_diff`.
Edit `src/tennis_match_prediction/ml/config.py` to change the training CSV, selected features
and split dates. Model settings are validated by contracts in `contracts.py`; override
them with `model_params` in Python or a JSON `--params-file` on the CLI.
The default dates are `TRAIN_START = date(2017, 1, 1)`,
`VALIDATION_START = date(2024, 1, 1)`, and `TEST_START = date(2025, 1, 1)`.
You can also train directly from Python:

```python
from tennis_match_prediction.ml.train import train

result = train(model_name="logistic_regression", model_params={"C": 0.5})
print(result.model_path, result.metrics, result.run_id)
```

The Python API accepts overrides for paths, dates (`datetime.date` values), and
`feature_columns`; CLI arguments override the same defaults for a single run.
It returns `ExperimentResult`, including metrics and the saved model path.
All three dates must be strictly increasing. The periods are:

- Warmup: `tourney_date < train_start`; retained by Dagster for history/features,
  excluded from scaler fitting, model fitting, and evaluation.
- Training: `train_start <= tourney_date < validation_start`.
- Validation: `validation_start <= tourney_date < test_start`.
- Test: `tourney_date >= test_start`, through the end of the input dataset.

Generate features from the full history before training; do not trim the raw data
at `train_start`. Every period must contain rows, and training must contain both
target classes. Equal source dates always stay together. These are source-date
cuts, not verified match-day cuts; tournaments spanning multiple source dates
can still cross boundaries. Choose dates outside overlapping tournaments when
that separation is required. The example assumes history begins before 2017.
Split boundaries, row counts, and observed date ranges are saved in the model
and JSON metadata; boundaries are also logged as MLflow parameters.

Dagster partitions are not required for these training cuts. See
[the partitioning assessment](docs/partitioning.md) for ingestion and temporal-state considerations.
Empty, duplicate, or unknown feature selections are rejected. Ordered feature
names are saved in the model and JSON metadata and logged in MLflow alongside
the feature count. Prediction uses the saved model's feature selection.
It rejects raw history and unordered training rows instead of rebuilding features.
`tennis_match_prediction.ml.predict` reads `data/curated/player_history.parquet`, produced together with
`player_comparison_atp_matches` in one chronological pass. Prediction uses the
same comparison formulas and requires a date after the asset's latest source date.
The history asset contains a validated JSON payload inside Parquet, persisted by
the existing Dagster IO manager; there is no separate checkpoint writer or engine.
The wider exported columns remain available for inspection and experiments.

Asset and CSV names changed during this refactor. Re-materialize the full
`materialize_historical_dataset` job to rebuild snapshots under the new names;
old snapshots and CSV files are not migrated or deleted automatically.

To run downstream assets automatically after inputs change, enable the default
automation condition sensor in Dagster. The root asset is triggered manually.

## Model experiments and MLflow

Compare models on the same published CSV, feature order and split dates:

```powershell
uv run python -m tennis_match_prediction.ml.train --model logistic_regression --run-name logistic-baseline
uv run python -m tennis_match_prediction.ml.train --model random_forest --run-name forest-baseline
uv run python -m tennis_match_prediction.ml.train --model random_forest --params-file examples/random-forest.json
```

Select configurations using `validation_log_loss` (lower is better), with Brier and
accuracy as supporting metrics. Runs report accuracy for choosing the better-ranked
player (lower numerical rank), the higher overall Elo player and the higher surface
Elo player. Comparisons use pre-match features and remain available when those
features are excluded from the model inputs. Ties/unavailable comparisons receive
half credit, equivalent to a fair random tie-break; each baseline logs its neutral
fraction. A custom CSV lacking a reference column omits that baseline.
These winner-picking references have accuracy metrics; model probabilities retain
Brier and log loss metrics.
After selection, rerun the chosen settings with `--final-evaluation` to also score
the test period. This still fits only on training rows. Keep the same dataset and
dates for comparisons; use the logged SHA-256 fingerprint to confirm dataset identity.

Every run writes `data/models/<experiment-id>/model.pkl`, JSON metadata and a provenance
directory containing configuration, splits, runtime dependencies, source code and a
Git diff. An explicit `--model-path` must be unused. Artifacts include schema version,
feature order, target convention and history-order marker. Retrain older artifacts;
the old pickle format is rejected. Prediction requires an explicit `--model-path`.

The `BaseMatchModel` contract is `fit`, `predict_proba` and `get_params`, plus a stable
name. `predict_proba` returns one P(player1 wins) per row. Models own preprocessing;
the shared runner owns splitting, metrics, artifacts and tracking. To add a model,
implement the contract and register its validated settings/constructor in
`ml/models/factory.py`. The current sklearn adapters also log their full fitted
estimator with an MLflow signature and input example. Its sklearn/pyfunc flavor
accepts feature rows and returns two class probabilities; the project artifact
supports prediction from player names through the history API.

Tracking modes are explicit:

| Mode | Use |
| --- | --- |
| `server` (default) | `MLFLOW_TRACKING_URI`, default `http://127.0.0.1:5000`; failure never falls back silently |
| `local` | SQLite and artifacts under `data/mlflow/`, or `TENNIS_MLFLOW_LOCAL_DIR`; ignores the server URI in `.env` |
| `disabled` | Intentional untracked runs and tests; local model artifacts are still written |

Use `--tracking-mode local` to work without Docker, or `--tracking-mode disabled`
for an untracked run. `--tracking-uri` and `--experiment-name` override environment
settings. `MLFLOW_EXPERIMENT_NAME` defaults to `tennis-match-prediction`. Tracked fit
or evaluation failures produce failed runs. Server tracking must be reachable before
fitting begins. One runner invocation owns one MLflow run; finish any active notebook
run before calling `train()`.

The Compose stack pins MLflow 3.16.1 to match `uv.lock`. PostgreSQL stores run metadata,
and MLflow proxies uploads/downloads to a named artifact volume. Clients on Windows
need only the HTTP URI. Containers on the same network use `http://mlflow:5000`.
Only the MLflow port is published, bound to localhost. Change `MLFLOW_PORT` and
`MLFLOW_TRACKING_URI` together if port 5000 is occupied.

```powershell
docker compose -f compose.mlflow.yaml logs --tail 50 mlflow
docker compose -f compose.mlflow.yaml down
docker compose -f compose.mlflow.yaml up -d --wait
```

Normal `down` preserves both named volumes. `down -v` deletes their contents.
Back up both the PostgreSQL database (using `pg_dump`) and the artifact volume;
restore them together. Existing local MLflow experiments are a separate store and
are not automatically migrated. This milestone logs experiments and models without
automatic registry promotion or deployment.

The opt-in integration test starts an isolated Compose project on port 5501, trains
both models, downloads their artifacts and recreates the containers to verify
persistence. It leaves containers stopped and volumes intact:

```powershell
$env:TENNIS_TEST_MLFLOW_DOCKER = "1"
uv run pytest tests/test_mlflow_integration.py -q -k docker
```

For the same HTTP artifact and restart checks with a temporary SQLite-backed server,
set `TENNIS_TEST_MLFLOW_HTTP=1` and run that test file with `-k http`.

See [the implementation plan](docs/ml-experimentation-plan.md) for scope and follow-ups.
The [initial comparison](docs/ml-baseline-results.md) records both default models on
the published dataset, with run IDs and validation metrics.

## Files and configuration

| Location | Contents |
| --- | --- |
| `data/raw/historical_matches/` | Annual CSVs, source manifest, and `all_atp_matches.csv` |
| `data/staging/` | One Parquet snapshot per Dagster asset |
| `data/curated/` | Player comparison, curated and training CSV exports |
| `data/curated/player_history.parquet` | Player-history asset and latest source date used for prediction |
| `data/models/` | Trained model and metric metadata |
| `dagster_home/` | Local Dagster configuration and SQLite storage |

`TENNIS_DATA_DIR` changes the data root. `TENNIS_RAW_MATCHES_CSV` overrides the
Dagster source CSV. The training CLI defaults to the published training CSV. Use `--history-path`
on the prediction CLI to select a player-history snapshot.
Defaults and environment loading are in `src/tennis_match_prediction/paths.py`.

The optional MySQL configuration is in `dagster_home/dagster.mysql.yaml.example`;
install it with `uv sync --extra mysql` if needed.

## Development and exploration

```powershell
uv run pytest
uv run ruff check src tests
uv run pylint src/tennis_match_prediction/pipelines
```

`notebooks/` contains historical EDA and experiments with alternative sources.
These are exploratory notebooks, not pipeline steps; there is no live-data
pipeline currently. Install their optional libraries with
`uv sync --extra notebooks`. The tennis-data.co.uk notebook additionally uses
Excel readers such as `xlrd`.

Feature quality rules and the local source coverage report are documented in
[docs/feature-quality.md](docs/feature-quality.md). The model uses 12 comparison
features. Surface form has an independent last-10 history; serve rates use
denominators matched to the available statistic. Comparisons with unavailable
information are neutral (zero).

Remaining work is tracked in `TODO.md`. `diagram.excalidraw` and `diagram.png`
are earlier design sketches; the workflow above describes the current code.
