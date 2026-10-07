# Tennis match prediction

Predict an ATP match from two player names, a date, and a surface. Historical
results come from TennisMyLife; training uses a chronological split and logistic
regression. A Dagster player-history asset supplies each player's pre-match history.

## Start here

Use Python 3.12 and `uv`:

```powershell
uv sync
Copy-Item .env.example .env
uv run python -m ml.refresh_history
uv run dagster asset materialize -m pipelines.historical_matches.defs --select "*"
uv run python -m ml.train
```

Then predict a match on a date **after the last result in the player-history asset**:

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
Re-materialize the Dagster dataset after refreshing history, then retrain.

## How the code fits together

Read these files in this order:

1. `ml/refresh_history.py` — command to download data.
2. `pipelines/historical_matches/tennis_my_life.py` — download, deduplicate, and consolidate seasons.
3. `pipelines/shared/contracts.py` — Pydantic models, feature names, and DataFrame validation.
4. `pipelines/historical_matches/transforms/player_comparison.py` — pure formulas for the 14 model features.
5. `pipelines/historical_matches/transforms/player_history.py` � build match comparisons and player history in one chronological pass.
6. `ml/train.py` � read the published training dataset, split chronologically (70%/15%/15%), train, and save.
7. `ml/predict.py` — load the model and player-history asset to predict a future match.

The model consumes the 14 difference features listed in `FEATURE_COLUMNS`.
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
The source CSV must already exist; download it with `ml.refresh_history` first.

The assets in `pipelines/historical_matches/assets.py` run in this order:

```text
raw_atp_matches
  -> normalized_atp_matches (filter walkovers/missing IDs, clean surface, parse/order dates)
  -> player_comparison_atp_matches (14 pre-match comparisons + player_history asset)
  -> winrate_featured_atp_matches
  -> h2h_featured_atp_matches
  -> elo_featured_atp_matches
  -> imputed_atp_matches (earlier observations only)
  -> curated_atp_matches (encode context, drop per-match statistics)
  -> training_atp_matches (assign player positions, construct target)
  -> training_atp_matches_csv
```

`curated_atp_matches_csv` publishes the readable curated dataset directly from
`curated_atp_matches`. `player_comparison_atp_matches_csv` publishes the comparison
columns directly from `player_comparison_atp_matches`. These exports are branches;
training-row construction does not depend on CSV publication.

- `transforms/history.py` supplies shared date parsing, stable ordering by date,
  tournament and match number (missing numbers last), and played-match filtering.
- `transforms/elo.py` contains the rating update formula used by both historical
  Elo columns and the player-history transform; both players update from their prior ratings.
- `transforms/head_to_head.py` calculates the prior head-to-head win difference.
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

This workflow is the single source of features for training and prediction.
`ml.train` reads `data/curated/training_atp_matches.csv` (or an explicit training
CSV path) and selects the 14 `FEATURE_COLUMNS`, target and chronological date.
It rejects raw history and unordered training rows instead of rebuilding features.
`ml.predict` reads `data/staging/player_history.parquet`, produced together with
`player_comparison_atp_matches` in one chronological pass. Prediction uses the
same comparison formulas and requires a date after the asset's history cutoff.
The history asset contains a validated JSON payload inside Parquet, persisted by
the existing Dagster IO manager; there is no separate checkpoint writer or engine.
The wider exported columns remain available for inspection and experiments.

Asset and CSV names changed during this refactor. Re-materialize the full
`materialize_historical_dataset` job to rebuild snapshots under the new names;
old snapshots and CSV files are not migrated or deleted automatically.

To run downstream assets automatically after inputs change, enable the default
automation condition sensor in Dagster. The root asset is triggered manually.

## Files and configuration

| Location | Contents |
| --- | --- |
| `data/raw/historical_matches/` | Annual CSVs, source manifest, and `all_atp_matches.csv` |
| `data/staging/` | One Parquet snapshot per Dagster asset |
| `data/curated/` | Player comparison, curated and training CSV exports |
| `data/staging/player_history.parquet` | Dagster player-history asset and cutoff used for prediction |
| `data/models/` | Trained model and metric metadata |
| `dagster_home/` | Local Dagster configuration and SQLite storage |

`TENNIS_DATA_DIR` changes the data root. `TENNIS_RAW_MATCHES_CSV` overrides the
Dagster source CSV. The training CLI defaults to the published training CSV. Use `--history-path`
on the prediction CLI to select a player-history snapshot.
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
