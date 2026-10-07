# tennis-match-prediction

Machine learning project that predicts the winner of an ATP tennis match.

Given the attributes of two players before a match, the model answers a single
question: which one wins? The reference concepts are Elo ratings and the
player-vs-player feature set described in
[An introduction to tennis Elo](https://www.tennisabstract.com/blog/2019/12/03/an-introduction-to-tennis-elo/).

## Data sources

| Source | Coverage | Used for |
| --- | --- | --- |
| [TennisMyLife](https://stats.tennismylife.org/tennis-match-database) | annual ATP files | historical training data (`raw_atp_matches`) |
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

### Raw input

Refresh the annual ATP source files and rebuild the consolidated input with:

```bash
uv run python -m ml.refresh_history  # all available ATP seasons
uv run python -m ml.refresh_history 2024 2025 2026  # selected seasons
```

The command writes one file per season plus a manifest with content hashes under
`data/raw/historical_matches/`, then produces `all_atp_matches.csv` with duplicate
matches removed. Source corrections are detected by their hash.

Train a model and calculate the durable feature state with:

```bash
uv run python -m ml.train data/raw/historical_matches/all_atp_matches.csv
uv run python -m ml.predict "Carlos Alcaraz" "Jannik Sinner" 2026-10-01 Hard
```

## Architecture

Two Dagster code locations, one per pipeline. Each is a package with its own
`defs.py`, so it can be moved to its own deployment later without code changes.

```
pipelines/
├── historical_matches/     TennisMyLife CSVs -> features -> anonymized dataset -> data/curated
├── live_matches/           SportDevs API -> daily raw snapshot
└── shared/                 paths, types, IO manager, metadata
```

### historical_matches

Assets are materialized left to right, split into six groups that follow the
stages of the pipeline:

```
raw -> normalize -> temporal features -> impute -> win rates -> h2h -> Elo -> curate -> publish
```

| Group | Asset | What it does |
| --- | --- | --- |
| `raw` | `raw_atp_matches` | Reads the consolidated TennisMyLife CSV. Root of the pipeline, manual trigger. |
| `normalize` | `normalized_atp_matches` | Parses dates, sorts chronologically, expands seeds into flags. |
| `impute` | `imputed_atp_matches` | Fills missing surface, height, age and ranking. |
| `features` | `winrate_featured_atp_matches` | Win rates overall and per surface, last 10 and 50 matches. |
| `features` | `h2h_featured_atp_matches` | Previous meetings between the two players. |
| `features` | `elo_featured_atp_matches` | Elo per player plus the pre-match difference. |
| `features` | `temporal_training_matches` | Keyed pre-match Elo, form, ranking, H2H, workload and serve features; also persists state. |
| `curate` | `curated_atp_matches` | Drops walkovers, one-hot encodes surface, codes round/level/hand, removes leaky box scores. |
| `publish` | `pre_anonymized_matches` | Writes `data/curated/pre_anonymized_matches.csv`. |
| `publish` | `anonymized_matches` | Shuffles winner/loser into `player0`/`player1`, reorients signed features consistently, writes `data/curated/anonymized_matches.csv`. |

Groups make the asset catalog filterable one stage at a time. Features stay
attached to their source rows throughout this single chain; curation does not
join independently materialized datasets. The temporal CSV and state checkpoint
are still exported alongside the pre-anonymized and anonymized outputs.

After upgrading from the branched pipeline, materialize `temporal_training_matches`
and all its downstream assets together once to replace the old snapshots.

Each step is a plain function in `transforms/` (plus `anonymization.py` for the
last one), so it can be unit tested or run in a notebook without Dagster. Asset
checks in `checks.py` guard the invariants: chronological order, imputed columns
free of nulls, win rates inside `[0, 1]`, walkovers removed, the exact schema the
anonymization step selects, and — on the output — a binary target, a balanced
target (mean within 0.05 of 0.5) and no surviving `winner_*` / `loser_*` column.

### Outputs

Both datasets are written to disk, so the ML step reads a plain CSV and nothing
else:

| File | Shape | Purpose |
| --- | --- | --- |
| `data/curated/pre_anonymized_matches.csv` | one row per match, players by name | the readable, auditable dataset |
| `data/curated/anonymized_matches.csv` | `player0_*` / `player1_*` plus `winner` | the dataset a model is trained on |

The anonymization shuffling is the point: without it the model could learn who a
specific player is. Both files come out of the same run, so the training set is
always traceable back to the readable dataset next to it.

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
└── curated/                  pre_anonymized_matches.csv, anonymized_matches.csv (ignored)
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
| `SPORTDEVS_API_KEY` | empty | Required by `live_matches_snapshot` |

## Automation

Assets carry an `AutomationCondition`. The historical chain uses
`AutomationCondition.eager()`, so materializing `raw_atp_matches` cascades all the
way through feature engineering, curation and anonymization down to the two CSVs
in `data/curated/`. The root stays manual because rebuilding every feature is
expensive and should be an explicit decision.

Declarative automation also needs the default automation condition sensor
enabled in the Dagster UI (it ships disabled).

## Project structure

```
pipelines/                Dagster code locations (one package per pipeline)
  shared/                 paths, PandasDataFrame type, IO manager, metadata
  historical_matches/
    assets.py checks.py anonymization.py config.py defs.py
    transforms/           pure functions: normalization, imputation, winrate, player_stats, finalization
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
- Neural network experiments with regularisation
- Feature importance pass to prune the feature set

## Roadmap

- Map SportDevs fields onto the historical schema so live matches feed features
- Map appending live snapshots onto the historical dataset
- Train/validation/test split and retraining pipeline
## Data contracts

Processing boundaries validate DataFrames without coercing their values. Normalized
matches require parsed, chronological dates and nonblank player IDs and names.
Temporal training frames require the ordered feature schema, finite numeric
features, valid chronological dates, and a non-null binary target. The anonymized
dataset is validated before its CSV is published.

Feature-state checkpoints use nested Pydantic models on save and load. The existing
version-one JSON format is preserved; unsupported versions, unknown fields,
invalid counters, non-finite values, and histories beyond the cutoff are rejected.
All structured models and DataFrame contracts live in `pipelines/shared/contracts.py`
and share a Pydantic base class. Player state uses the same models in memory and
in checkpoints, without duplicate serialization schemas.
