"""Assets that turn the raw ATP match history into the training dataset.

    raw_atp_matches                     read the Kaggle CSV
      -> normalized_atp_matches         dates, chronological order, seed parsing
      -> temporal_training_matches      temporal features attached to source rows
      -> imputed_atp_matches            null height / age / rank / surface filled
      -> winrate_featured_atp_matches   rolling win rates (overall and per surface)
      -> h2h_featured_atp_matches       head-to-head history
      -> elo_featured_atp_matches       Elo ratings
      -> curated_atp_matches            categorical encodings, leaky stats removed
      -> pre_anonymized_matches         data/curated/pre_anonymized_matches.csv
      -> anonymized_matches             data/curated/anonymized_matches.csv

Every transform is a separate asset: each step is materialised (and therefore
observable, comparable and re-runnable) on its own, and the feature engineering
code stays free of Dagster and IO concerns.

Groups follow the stages of the pipeline, so the asset catalog can be filtered
one layer at a time: `raw`, `normalize`, `impute`, `features`, `curate` and
`publish`. The last group holds the two assets that write files under
data/curated; anonymization is deliberately not a group of its own, it is the
tail of the same chain.
"""

from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    MetadataValue,
    asset,
)

from pipelines.shared.contracts import NORMALIZED_MATCHES, DataFrameContract

from ..shared.metadata import dataframe_metadata
from ..shared.paths import (
    ANONYMIZED_CSV_NAME,
    FEATURE_STATE_PATH,
    PRE_ANONYMIZED_CSV_NAME,
    TEMPORAL_FEATURES_CSV_NAME,
    write_curated_csv,
)
from ..shared.types import PandasDataFrame
from .anonymization import FINAL_COLUMN_ORDER, TARGET_COLUMN, anonymize
from .config import RawMatchesCsv
from .feature_state import FEATURE_COLUMNS, attach_temporal_features
from .transforms.finalization import (
    encode_surface,
    remove_stat_cols,
    remove_wo,
    transform_handedness,
    transform_round,
    transform_tourney_level,
)
from .transforms.imputation import (
    fill_null_age,
    fill_null_height,
    fill_null_rank,
    fill_null_surface,
)
from .transforms.normalization import (
    preprocess_dates,
    remove_matches_without_player_ids,
    sort_by_date,
    transform_seed_data,
)
from .transforms.player_stats import calcular_elo, calcular_h2h
from .transforms.winrate import (
    calcular_winrate_superficie,
    calcular_winrate_superficie_ultimas_n,
    calcular_winrate_total,
    calcular_winrate_ultimas_n,
)

# Run a step as soon as its input is updated (declarative automation: enable the
# `default_automation_condition_sensor` to let Dagster launch these runs).
# The root asset stays manual on purpose: rebuilding every feature is expensive,
# so somebody has to ask for it.
WHEN_INPUT_CHANGES = AutomationCondition.eager()

DOMAIN_TAGS = {"domain": "tennis", "source": "tennis_my_life"}


@asset(
    group_name="raw",
    kinds={"csv", "pandas"},
    tags={**DOMAIN_TAGS, "layer": "raw"},
    description="ATP match results exactly as published on Kaggle (one row per finished match).",
)
def raw_atp_matches(
    context: AssetExecutionContext, raw_matches_csv: RawMatchesCsv
) -> PandasDataFrame:
    """Read the raw match history CSV. Root of the pipeline: materialized by hand."""
    csv_path = Path(raw_matches_csv.csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Raw ATP match CSV not found at {csv_path}. Run `uv run python -m "
            "ml.refresh_history` or point TENNIS_RAW_MATCHES_CSV somewhere else."
        )

    context.log.info(f"Reading raw matches from {csv_path}")
    # Seed columns combine integers (e.g. 6), CSV floats (6.0) and source codes
    # across seasons. Keeping their raw representation textual makes the Arrow
    # snapshot deterministic; transform_seed_data parses them downstream.
    frame = pd.read_csv(csv_path, dtype={"winner_seed": "string", "loser_seed": "string"})
    context.add_output_metadata(
        {"source_path": MetadataValue.path(str(csv_path)), **dataframe_metadata(frame)}
    )
    return frame


@asset(
    group_name="normalize",
    kinds={"pandas"},
    tags={**DOMAIN_TAGS, "layer": "normalize"},
    description="Tourney dates parsed, matches sorted chronologically, seeds expanded into flags.",
    automation_condition=WHEN_INPUT_CHANGES,
)
def normalized_atp_matches(raw_atp_matches: PandasDataFrame) -> PandasDataFrame:
    """Make the dataset chronological and split the seed columns into numeric + flags."""
    frame = remove_matches_without_player_ids(raw_atp_matches)
    frame = preprocess_dates(frame)
    frame = sort_by_date(frame)
    frame = transform_seed_data(frame)
    NORMALIZED_MATCHES.validate_frame(frame)
    return frame


@asset(
    group_name="features",
    kinds={"pandas", "csv", "json"},
    tags={"domain": "tennis", "layer": "features", "implementation": "temporal"},
    description=(
        "Source matches with winner-minus-loser pre-match features and a durable state checkpoint "
        "used by incremental updates and future predictions."
    ),
    automation_condition=WHEN_INPUT_CHANGES,
)
def temporal_training_matches(
    context: AssetExecutionContext, normalized_atp_matches: PandasDataFrame
) -> PandasDataFrame:
    """Build keyed winner-minus-loser features and checkpoint from completed results."""
    frame, temporal, state = attach_temporal_features(normalized_atp_matches)
    csv_path = write_curated_csv(temporal, TEMPORAL_FEATURES_CSV_NAME)
    state.save(FEATURE_STATE_PATH)
    context.add_output_metadata(
        {
            "csv_path": MetadataValue.path(str(csv_path)),
            "state_path": MetadataValue.path(str(FEATURE_STATE_PATH)),
            **dataframe_metadata(frame),
        }
    )
    return frame


@asset(
    group_name="impute",
    kinds={"pandas"},
    tags={**DOMAIN_TAGS, "layer": "impute"},
    description=(
        "Missing surface, height, age and ranking values replaced by conservative defaults."
    ),
    automation_condition=WHEN_INPUT_CHANGES,
)
def imputed_atp_matches(temporal_training_matches: PandasDataFrame) -> PandasDataFrame:
    """Replace nulls so downstream feature engineering never has to guard against them."""
    frame = fill_null_surface(temporal_training_matches)
    frame = fill_null_height(frame)
    frame = fill_null_age(frame)
    return fill_null_rank(frame)


@asset(
    group_name="features",
    kinds={"pandas", "polars"},
    tags={**DOMAIN_TAGS, "layer": "features"},
    description="Win rates over the last 10/50 matches, overall and per surface.",
    automation_condition=WHEN_INPUT_CHANGES,
)
def winrate_featured_atp_matches(imputed_atp_matches: PandasDataFrame) -> PandasDataFrame:
    """Attach rolling win-rate features computed only from earlier matches."""
    frame = calcular_winrate_total(imputed_atp_matches)
    frame = calcular_winrate_ultimas_n(frame, n=50)
    frame = calcular_winrate_ultimas_n(frame, n=10)
    frame = calcular_winrate_superficie(frame)
    frame = calcular_winrate_superficie_ultimas_n(frame, n=50)
    return calcular_winrate_superficie_ultimas_n(frame, n=10)


@asset(
    group_name="features",
    kinds={"pandas", "polars"},
    tags={**DOMAIN_TAGS, "layer": "features"},
    description="Number of previous meetings between the two players.",
    automation_condition=WHEN_INPUT_CHANGES,
)
def h2h_featured_atp_matches(winrate_featured_atp_matches: PandasDataFrame) -> PandasDataFrame:
    """Attach the head-to-head history of each matchup."""
    return calcular_h2h(winrate_featured_atp_matches)


@asset(
    group_name="features",
    kinds={"pandas", "polars"},
    tags={**DOMAIN_TAGS, "layer": "features"},
    description="Elo rating per player plus the pre-match rating difference.",
    automation_condition=WHEN_INPUT_CHANGES,
)
def elo_featured_atp_matches(h2h_featured_atp_matches: PandasDataFrame) -> PandasDataFrame:
    """Attach Elo ratings, which double as the baseline the model has to beat."""
    return calcular_elo(h2h_featured_atp_matches)


@asset(
    group_name="curate",
    kinds={"pandas"},
    tags={**DOMAIN_TAGS, "layer": "curate"},
    description=(
        "Walkovers removed, surface one-hot encoded, round/tourney level/handedness coded "
        "and per-match statistics dropped. Still names the winner and the loser, so it is "
        "the input of the anonymization step."
    ),
    automation_condition=WHEN_INPUT_CHANGES,
)
def curated_atp_matches(
    elo_featured_atp_matches: PandasDataFrame,
) -> PandasDataFrame:
    """Produce the pre-anonymization dataset."""
    frame = remove_wo(elo_featured_atp_matches)
    if (
        not set(FEATURE_COLUMNS).issubset(frame.columns)
        or frame[list(FEATURE_COLUMNS)].isna().any().any()
    ):
        raise ValueError(
            "Missing temporal features. Materialize temporal_training_matches and all "
            "downstream assets together to rebuild the feature chain."
        )
    frame = encode_surface(frame)
    frame = transform_round(frame)
    frame = transform_tourney_level(frame)
    frame = transform_handedness(frame)
    return remove_stat_cols(frame)


@asset(
    group_name="publish",
    kinds={"csv", "pandas"},
    tags={**DOMAIN_TAGS, "layer": "publish"},
    description=(
        "Writes the curated dataset to data/curated/pre_anonymized_matches.csv. This is the "
        "readable version of the dataset, still identifying players by name."
    ),
    automation_condition=WHEN_INPUT_CHANGES,
)
def pre_anonymized_matches(
    context: AssetExecutionContext, curated_atp_matches: PandasDataFrame
) -> PandasDataFrame:
    """Save the curated dataset as the pre-anonymization CSV."""
    csv_path = write_curated_csv(curated_atp_matches, PRE_ANONYMIZED_CSV_NAME)
    context.log.info(f"Wrote {len(curated_atp_matches)} rows to {csv_path}")
    context.add_output_metadata(
        {
            "csv_path": MetadataValue.path(str(csv_path)),
            **dataframe_metadata(curated_atp_matches),
        }
    )
    return curated_atp_matches


@asset(
    group_name="publish",
    kinds={"csv", "pandas", "polars"},
    tags={**DOMAIN_TAGS, "layer": "publish"},
    description=(
        "Last step of the pipeline: shuffles winner/loser into player0/player1, adds the "
        f"binary `{TARGET_COLUMN}` target and writes the result to "
        f"data/curated/{ANONYMIZED_CSV_NAME}. This is the dataset a model is trained on."
    ),
    automation_condition=WHEN_INPUT_CHANGES,
)
def anonymized_matches(
    context: AssetExecutionContext, pre_anonymized_matches: PandasDataFrame
) -> PandasDataFrame:
    """Hide which player won and save the training dataset."""
    frame = anonymize(pre_anonymized_matches)
    DataFrameContract(
        name="anonymized training matches",
        required=tuple(FINAL_COLUMN_ORDER),
        numeric=FEATURE_COLUMNS,
        target=TARGET_COLUMN,
        exact_columns=True,
    ).validate_frame(frame)
    csv_path = write_curated_csv(frame, ANONYMIZED_CSV_NAME)

    target_mean = float(frame[TARGET_COLUMN].mean())
    if not 0.4 <= target_mean <= 0.6:
        context.log.warning(
            f"Target '{TARGET_COLUMN}' mean is {target_mean:.4f}, "
            "expected ~0.5 for a balanced dataset."
        )

    context.log.info(f"Wrote {len(frame)} rows to {csv_path}")
    context.add_output_metadata(
        {
            "csv_path": MetadataValue.path(str(csv_path)),
            "target_mean": MetadataValue.float(target_mean),
            **dataframe_metadata(frame),
        }
    )
    return frame
