# tennis-match-prediction

Machine learning project that predicts the winner of an ATP tennis match.

Given the attributes of two players before a match, the model answers a single
question: which one wins? The reference concepts are Elo ratings and the
player-vs-player feature set described in
[An introduction to tennis Elo](https://www.tennisabstract.com/blog/2019/12/03/an-introduction-to-tennis-elo/).

## Data sources

| Source | Coverage | Used for |
| --- | --- | --- |
| [Kaggle: ATP daily pull](https://www.kaggle.com/datasets/dissfya/atp-tennis-2000-2023daily-pull) | 2000 to date | historical training data (`raw_atp_matches`) |
| [Kaggle: atpdata](https://www.kaggle.com/datasets/sijovm/atpdata) | up to 2022 | alternative raw input |
| [Kaggle: tennis](https://www.kaggle.com/datasets/guillemservera/tennis) | 1968 to 2024 | alternative raw input |
| [tennis-data.co.uk](http://tennis-data.co.uk/2025/2025.xlsx) | yearly spreadsheets | alternative raw input |
| [SportDevs tennis API](https://sportdevs.com/dashboard) | live | daily matches after the Kaggle dataset (`live_matches_snapshot`) |

## Quick start

[uv](https://docs.astral.sh/uv/) manages the environment. `uv.lock` is committed,
so a fresh clone reproduces the exact dependency set.

```bash
uv sync --all-extras
copy .env.example .env          # Windows; cp on Linux/macOS
uv run dagster dev              # http://localhost:3000
```

`dagster dev` reads `workspace.yaml` and loads three independent pipelines. To
materialize everything from a fresh clone, open the job
`materialize_historical_dataset` in the UI and hit "Materialize".

### Day-to-day commands

| Command | Purpose |
| --- | --- |
| `uv sync --all-extras` | Create/refresh `.venv` from `uv.lock` |
| `uv lock` | Re-resolve after editing `pyproject.toml` |
| `uv add <package>` / `uv remove <package>` | Change dependencies, then commit `pyproject.toml` and `uv.lock` |
| `uv run dagster dev` | Start the UI |
| `uv run ruff check pipelines` | Lint |
| `uv run pylint pipelines` | Lint (stricter, definition layer only) |
| `uv run pytest` | Tests |

Python 3.12 is pinned in `.python-version`. The MySQL storage extra and the
notebook libraries live behind extras so a bare `uv sync` stays small:

```bash
uv sync --extra mysql      # dagster-mysql
uv run jupyter lab         # notebooks extra
```

To run without Weights & Biases, set `TENNIS_WANDB_ENABLED=false` in `.env`.
Publishing assets still write their local CSV and skip the upload.

### Raw input

The historical pipeline expects `data/raw/historical_matches/atp_matches_2023.csv`.
Download it from Kaggle into that folder, or point `TENNIS_RAW_MATCHES_CSV` at
your own copy.

## Architecture

Three Dagster code locations, one per pipeline. Each is a package with its own
`defs.py`, so it can be moved to its own deployment later without code changes.

```
pipelines/
├── historical_matches/     Kaggle CSV -> features -> curated dataset -> W&B
├── anonymized_dataset/     W&B artifact -> anonymous player0/player1 -> W&B
├── live_matches/           SportDevs API -> daily raw snapshot
└── shared/                 paths, types, IO manager, metadata, W&B resource
```

### historical_matches

Group `historical_matches`, eight assets, materialized left to right:

| Asset | What it does |
| --- | --- |
| `raw_atp_matches` | Reads the Kaggle CSV. Root of the pipeline, manual trigger. |
| `normalized_atp_matches` | Parses dates, sorts chronologically, expands seeds into flags. |
| `imputed_atp_matches` | Fills missing surface, height, age and ranking. |
| `winrate_featured_atp_matches` | Win rates overall and per surface, last 10 and 50 matches. |
| `h2h_featured_atp_matches` | Previous meetings between the two players. |
| `elo_featured_atp_matches` | Elo per player plus the pre-match difference. |
| `curated_atp_matches` | Drops walkovers, one-hot encodes surface, codes round/level/hand, removes leaky box scores. |
| `published_pre_anonymized_dataset` | Publishes the curated dataset as W&B artifact `pre_anonymized_tennis_data`. |

Each step is a plain function in `transforms/`, so it can be unit tested or run
in a notebook without Dagster. Asset checks in `checks.py` guard the invariants:
chronological order, imputed columns free of nulls, win rates inside `[0, 1]`,
walkovers removed, and the exact schema the anonymization step selects.

### anonymized_dataset

Group `anonymized_dataset`, one asset: `anonymized_atp_matches`.

It downloads `pre_anonymized_tennis_data:latest` from W&B, shuffles winner and
loser into `player0` and `player1` per match, adds the binary `winner` target and
publishes `final_anonymized_tennis_data`. The shuffling is the point: without it
the model could learn who a specific player is. Checks verify the target is
binary, balanced (mean within 0.05 of 0.5) and that no `winner_*` / `loser_*`
column survives.

Reading from W&B instead of a local file is what keeps the exact dataset version a
model was trained on traceable.

### live_matches

Group `live_matches`, one asset: `live_matches_snapshot`.

Pulls the matches finished on a given day (yesterday by default) and stores the
flattened table plus the raw payload under `data/raw/live_matches/`. This is work
in progress: the API returns generic `home_team_*` / `away_team_*` fields, so
mapping them onto the `winner_*` / `loser_*` schema is still open.

## Data layout

```
data/
├── raw/
│   ├── historical_matches/   committed Kaggle CSVs (inputs)
│   └── live_matches/         daily API pulls (ignored)
├── staging/                  <asset_name>.parquet snapshots (ignored)
└── curated/                  published CSVs (ignored)
```

`pipelines/shared/paths.py` owns every path and calls `load_dotenv()` once, so
`TENNIS_DATA_DIR` moves the whole tree. Snapshots land in `data/staging/`
through `ParquetDataFrameIOManager`, which keeps the UI fast without putting
large frames in the event log.

## Configuration

`dagster_home/dagster.yaml` uses SQLite by default, so a fresh clone runs with no
database. For a shared deployment, copy `dagster_home/dagster.mysql.yaml.example`,
fill in the credentials and run `uv sync --extra mysql`. Dagster does not expand
`${VAR}` in that file, which is why the MySQL config is a template instead of a
committed file.

Environment variables are documented in `.env.example`. The ones that matter:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DAGSTER_HOME` | `dagster_home/` | Dagster instance directory |
| `TENNIS_DATA_DIR` | `./data` | Root of the data tree |
| `TENNIS_RAW_MATCHES_CSV` | `data/raw/historical_matches/atp_matches_2023.csv` | Raw input |
| `TENNIS_WANDB_ENABLED` | `true` | Set `false` to skip all W&B calls |
| `WANDB_PROJECT` / `WANDB_ENTITY` / `WANDB_API_KEY` | - | W&B destination |
| `SPORTDEVS_API_KEY` | empty | Required by `live_matches_snapshot` |

## Automation

Assets carry an `AutomationCondition`. The historical chain uses
`AutomationCondition.eager()`, so materializing `raw_atp_matches` cascades through
feature engineering and publishing. The root stays manual because rebuilding every
feature is expensive and should be an explicit decision.

Declarative automation also needs the default automation condition sensor
enabled in the Dagster UI (it ships disabled). `anonymized_atp_matches` runs on a
daily cron instead, since its input is a W&B artifact rather than an upstream asset.

## Project structure

```
pipelines/                Dagster code locations (one package per pipeline)
  shared/                 paths, PandasDataFrame type, IO manager, metadata, W&B resource
  historical_matches/
    assets.py checks.py config.py defs.py
    transforms/           pure functions: normalization, imputation, winrate, player_stats, finalization
  anonymized_dataset/     assets.py checks.py anonymization.py defs.py
  live_matches/           assets.py checks.py client.py config.py defs.py
notebooks/                exploration, one notebook per data source
data/                     raw inputs, staging snapshots, curated datasets
dagster_home/             Dagster instance (SQLite storage)
pyproject.toml            dependencies and tool configuration
uv.lock                   pinned dependency resolution
workspace.yaml            code locations
```

## Notebooks

- `notebooks/eda_historical_matches.ipynb` — exploration of the Kaggle datasets
- `notebooks/eda_live_matches.ipynb` — exploration of the SportDevs payload
- `notebooks/fetch_tennis_data_uk.ipynb` — pulls tennis-data.co.uk spreadsheets

## Modeling plan

- Baseline: Elo difference, to beat before trusting anything else
- Random Forest / decision tree as a first real model
- Neural network experiments with regularisation, tracked on W&B
- Feature importance pass to prune the feature set

## Roadmap

- Map SportDevs fields onto the historical schema so live matches feed features
- Map appending live snapshots onto the historical dataset
- Train/validation/test split and retraining pipeline