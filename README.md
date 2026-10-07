# Tennis match prediction

Predict an ATP match from two player names, a date, and a surface. Historical
results come from TennisMyLife; training uses a chronological split and logistic
regression. A saved feature state supplies each player's pre-match history.

## Start here

Use Python 3.12 and `uv`:

```powershell
uv sync
Copy-Item .env.example .env
uv run python -m ml.refresh_history
uv run python -m ml.train data/raw/historical_matches/all_atp_matches.csv
```

Then predict a match on a date **after the last result in the saved state**:

```powershell
uv run python -m ml.predict "Carlos Alcaraz" "Jannik Sinner" 2027-01-01 Hard
```

Both players must be present in the history. The prediction returns win
probabilities, the history cutoff, and the model path. Training also records
validation/test metrics and logs the model artifact to local MLflow.

To refresh selected seasons instead of downloading all seasons:

```powershell
uv run python -m ml.refresh_history 2025 2026
```

This updates annual files, then consolidates all locally available seasons.
Training rebuilds the feature state from that history.

## How the code fits together

Read these files in this order:

1. `ml/refresh_history.py` — command to download data.
2. `pipelines/historical_matches/tennis_my_life.py` — download, deduplicate, and consolidate seasons.
3. `pipelines/shared/contracts.py` — Pydantic models, feature names, and DataFrame validation.
4. `pipelines/historical_matches/feature_state.py` — calculate features before applying each result; save/load player history.
5. `ml/train.py` — build features, split chronologically (70%/15%/15%), train, and save.
6. `ml/predict.py` — load the model and state to predict a future match.

The model consumes the 14 difference features listed in `FEATURE_COLUMNS`.
Training randomly assigns the winner to player0 or player1 and gives the features
that same orientation. `winner=1` means player1 won.

All data models live in `contracts.py` and inherit from `ContractModel`.
`PlayerState` and `PlayedMatch` are used both in memory and in version-one JSON
checkpoints. DataFrame contracts check columns, dates, finite features, and
binary targets with vectorized operations.

## Dagster dataset workflow

There is one Dagster code location, configured in `workspace.yaml`:

```powershell
uv run dagster dev
```

Set `DAGSTER_HOME` in `.env` to the absolute path of this checkout's `dagster_home`
directory. Open `materialize_historical_dataset` in the UI to build the dataset.
The source CSV must already exist; download it with `ml.refresh_history` first.

The assets in `pipelines/historical_matches/assets.py` run in this order:

```text
raw matches -> normalize -> temporal features -> impute
-> win rates -> head-to-head -> Elo -> curate -> publish -> anonymize
```

- `transforms/` contains the normalization, imputation, feature, and encoding functions.
- `anonymization.py` converts winner/loser columns into player0/player1 columns.
- `checks.py` reports dataset quality in Dagster.
- `defs.py` registers the assets, checks, and job.
- `shared/parquet_io.py` saves intermediate DataFrames so individual steps can be rerun.

This workflow exports a wider dataset for inspection and experiments. The current
`ml.train` command reads raw history and uses `feature_state.py` directly; it does
not train on `anonymized_matches.csv`. Both paths use the same temporal feature
engine. The additional win-rate, head-to-head, and Elo transforms are still used
by the Dagster exports.

To run downstream assets automatically after inputs change, enable the default
automation condition sensor in Dagster. The root asset is triggered manually.

## Files and configuration

| Location | Contents |
| --- | --- |
| `data/raw/historical_matches/` | Annual CSVs, source manifest, and `all_atp_matches.csv` |
| `data/staging/` | One Parquet snapshot per Dagster asset |
| `data/curated/` | Temporal features, pre-anonymized and anonymized CSV exports |
| `data/state/feature_state.json` | Player history and cutoff used for prediction |
| `data/models/` | Trained model and metric metadata |
| `dagster_home/` | Local Dagster configuration and SQLite storage |

`TENNIS_DATA_DIR` changes the data root. `TENNIS_RAW_MATCHES_CSV` overrides the
Dagster source CSV. The CLI training command takes its source path explicitly.
Defaults and environment loading are in `pipelines/shared/paths.py`.

The optional MySQL configuration is in `dagster_home/dagster.mysql.yaml.example`;
install it with `uv sync --extra mysql` if needed.

## Development and exploration

```powershell
uv run pytest
uv run ruff check pipelines ml tests
uv run pylint pipelines
```

`notebooks/` contains historical EDA and experiments with alternative sources.
These are exploratory notebooks, not pipeline steps; there is no live-data
pipeline currently. Install their optional libraries with
`uv sync --extra notebooks`. The tennis-data.co.uk notebook additionally uses
Excel readers such as `xlrd`.

Remaining work is tracked in `TODO.md`. `diagram.excalidraw` and `diagram.png`
are earlier design sketches; the workflow above describes the current code.
