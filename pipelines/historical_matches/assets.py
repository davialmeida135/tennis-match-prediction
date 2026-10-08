"""Historical dataset: normalize/filter, history features, impute, encode, target, publish.

History calculations all receive played matches, cleaned surfaces, and the same
chronological ordering. Imputation uses earlier observations only. CSV publication
is downstream of dataset construction, so training rows do not depend on exports.
"""

from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetOut,
    AutomationCondition,
    MetadataValue,
    asset,
    multi_asset,
)

from pipelines.shared.contracts import (
    FEATURE_COLUMNS,
    NORMALIZED_MATCHES,
    DataFrameContract,
)

from ..shared.metadata import dataframe_metadata
from ..shared.paths import (
    CURATED_MATCHES_CSV_NAME,
    TRAINING_MATCHES_CSV_NAME,
    write_curated_csv,
)
from .config import RawMatchesCsv
from .transforms.curation import (
    encode_surface,
    remove_stat_cols,
    transform_handedness,
    transform_round,
    transform_tourney_level,
)
from .transforms.elo import calcular_elo
from .transforms.head_to_head import calcular_h2h
from .transforms.imputation import (
    fill_null_age,
    fill_null_height,
    fill_null_rank,
    fill_null_surface,
)
from .transforms.normalization import (
    preprocess_dates,
    remove_matches_without_player_ids,
    remove_wo,
    sort_by_date,
    transform_seed_data,
)
from .transforms.player_history import attach_temporal_features
from .transforms.training_rows import FINAL_COLUMN_ORDER, TARGET_COLUMN, build_training_rows
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
    description="Consolidated ATP source rows, including rows excluded during normalization.",
)
def raw_atp_matches(context: AssetExecutionContext, raw_matches_csv: RawMatchesCsv) -> pd.DataFrame:
    """Refresh/consolidate source seasons when needed, then read the raw history."""
    csv_path = Path(raw_matches_csv.csv_path)
    if raw_matches_csv.refresh or not csv_path.exists():
        directory = csv_path.parent
        years = set(raw_matches_csv.years) if raw_matches_csv.years is not None else None
        if raw_matches_csv.refresh or not any(directory.glob("[0-9][0-9][0-9][0-9].csv")):
            _, downloaded = raw_matches_csv.refresh_history(directory, years)
            context.add_output_metadata(
                {
                    "downloaded_seasons": MetadataValue.json([item.year for item in downloaded]),
                    "changed_seasons": MetadataValue.json(
                        [item.year for item in downloaded if item.changed]
                    ),
                }
            )
        else:
            raw_matches_csv.consolidate_seasons(directory)

    context.log.info(f"Reading raw matches from {csv_path}")
    # Seed columns combine integers (e.g. 6), CSV floats (6.0) and source codes
    # across seasons. Keeping their raw representation textual makes the Arrow
    # snapshot deterministic; transform_seed_data parses them downstream.
    frame = pd.read_csv(
        csv_path,
        dtype={
            column: "string" for column in ("winner_seed", "loser_seed", "winner_id", "loser_id")
        },
    )
    context.add_output_metadata(
        {"source_path": MetadataValue.path(str(csv_path)), **dataframe_metadata(frame)}
    )
    return frame


@asset(
    group_name="normalize",
    kinds={"pandas"},
    tags={**DOMAIN_TAGS, "layer": "normalize"},
    description=(
        "Walkovers and unidentified players excluded, surfaces cleaned, dates parsed, "
        "matches ordered chronologically and seeds expanded into flags."
    ),
    automation_condition=WHEN_INPUT_CHANGES,
)
def normalized_atp_matches(raw_atp_matches: pd.DataFrame) -> pd.DataFrame:
    """Make the dataset chronological and split the seed columns into numeric + flags."""
    frame = remove_wo(remove_matches_without_player_ids(raw_atp_matches))
    frame = fill_null_surface(frame)
    frame = preprocess_dates(frame)
    frame = sort_by_date(frame)
    frame = transform_seed_data(frame)
    NORMALIZED_MATCHES.validate_frame(frame)
    return frame


@multi_asset(
    outs={
        "player_comparison_atp_matches": AssetOut(
            group_name="features", automation_condition=WHEN_INPUT_CHANGES
        ),
        "player_history": AssetOut(
            group_name="publish",
            description="Accumulated player state for future match predictions.",
            automation_condition=WHEN_INPUT_CHANGES,
        ),
    },
)
def player_comparison_atp_matches(
    normalized_atp_matches: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Produce pre-match comparisons and prediction history in one chronological pass."""
    frame, _, history = attach_temporal_features(normalized_atp_matches)
    snapshot = pd.DataFrame({"history": [history.model_dump_json()]})
    return frame, snapshot


@asset(
    group_name="features",
    kinds={"pandas", "polars"},
    tags={**DOMAIN_TAGS, "layer": "features"},
    description="Win rates over the last 10/50 matches, overall and per surface.",
    automation_condition=WHEN_INPUT_CHANGES,
)
def winrate_featured_atp_matches(
    player_comparison_atp_matches: pd.DataFrame,
) -> pd.DataFrame:
    """Attach rolling win-rate features computed only from earlier matches."""
    frame = calcular_winrate_total(player_comparison_atp_matches)
    frame = calcular_winrate_ultimas_n(frame, n=50)
    frame = calcular_winrate_ultimas_n(frame, n=10)
    frame = calcular_winrate_superficie(frame)
    frame = calcular_winrate_superficie_ultimas_n(frame, n=50)
    return calcular_winrate_superficie_ultimas_n(frame, n=10)


@asset(
    group_name="features",
    kinds={"pandas", "polars"},
    tags={**DOMAIN_TAGS, "layer": "features"},
    description="Pre-match head-to-head win difference between the two players.",
    automation_condition=WHEN_INPUT_CHANGES,
)
def h2h_featured_atp_matches(
    winrate_featured_atp_matches: pd.DataFrame,
) -> pd.DataFrame:
    """Attach the head-to-head history of each matchup."""
    return calcular_h2h(winrate_featured_atp_matches)


@asset(
    group_name="features",
    kinds={"pandas"},
    tags={**DOMAIN_TAGS, "layer": "features"},
    description="Elo rating per player plus the pre-match rating difference.",
    automation_condition=WHEN_INPUT_CHANGES,
)
def elo_featured_atp_matches(h2h_featured_atp_matches: pd.DataFrame) -> pd.DataFrame:
    """Attach pre-match Elo ratings using the player-history transform's update formula."""
    return calcular_elo(h2h_featured_atp_matches)


@asset(
    group_name="impute",
    kinds={"pandas"},
    tags={**DOMAIN_TAGS, "layer": "impute"},
    description=(
        "Missing player attributes filled from prior observations with fixed initial defaults."
    ),
    automation_condition=WHEN_INPUT_CHANGES,
)
def imputed_atp_matches(elo_featured_atp_matches: pd.DataFrame) -> pd.DataFrame:
    """Fill player attributes causally after history features and before encoding."""
    frame = elo_featured_atp_matches
    frame = fill_null_height(frame)
    frame = fill_null_age(frame)
    return fill_null_rank(frame)


@asset(
    group_name="curate",
    kinds={"pandas"},
    tags={**DOMAIN_TAGS, "layer": "curate"},
    description=(
        "Surface, round, tournament level and handedness encoded; per-match statistics "
        "dropped. Retains player names and winner/loser orientation for inspection."
    ),
    automation_condition=WHEN_INPUT_CHANGES,
)
def curated_atp_matches(
    imputed_atp_matches: pd.DataFrame,
) -> pd.DataFrame:
    """Encode match context and remove outcome statistics before target construction."""
    frame = remove_wo(imputed_atp_matches)
    if (
        not set(FEATURE_COLUMNS).issubset(frame.columns)
        or frame[list(FEATURE_COLUMNS)].isna().any().any()
    ):
        raise ValueError(
            "Missing temporal features. Materialize player_comparison_atp_matches and all "
            "downstream assets together to rebuild the feature chain."
        )
    frame = encode_surface(frame)
    frame = transform_round(frame)
    frame = transform_tourney_level(frame)
    frame = transform_handedness(frame)
    return remove_stat_cols(frame)


@asset(
    group_name="training",
    kinds={"pandas", "polars"},
    tags={**DOMAIN_TAGS, "layer": "training"},
    description=(
        "Assign winner/loser to player0/player1 reproducibly and construct the "
        f"binary `{TARGET_COLUMN}` target for training experiments."
    ),
    automation_condition=WHEN_INPUT_CHANGES,
)
def training_atp_matches(
    context: AssetExecutionContext, curated_atp_matches: pd.DataFrame
) -> pd.DataFrame:
    """Assign player positions reproducibly and construct the binary training target."""
    frame = build_training_rows(curated_atp_matches, random_seed=42)
    DataFrameContract(
        name="player-oriented training matches",
        required=tuple(FINAL_COLUMN_ORDER),
        numeric=FEATURE_COLUMNS,
        target=TARGET_COLUMN,
        exact_columns=True,
        date_column="tourney_date",
    ).validate_frame(frame)

    target_mean = float(frame[TARGET_COLUMN].mean())
    if not 0.4 <= target_mean <= 0.6:
        context.log.warning(
            f"Target '{TARGET_COLUMN}' mean is {target_mean:.4f}, "
            "expected ~0.5 for a balanced dataset."
        )

    context.add_output_metadata(
        {
            "target_mean": MetadataValue.float(target_mean),
            **dataframe_metadata(frame),
        }
    )
    return frame


###############
# Publish CSV #
###############


@asset(
    group_name="publish",
    kinds={"csv", "pandas"},
    tags={**DOMAIN_TAGS, "layer": "publish"},
    description=(
        "Writes the curated dataset to data/curated/curated_atp_matches.csv. This is the "
        "readable version of the dataset, still identifying players by name."
    ),
    automation_condition=WHEN_INPUT_CHANGES,
)
def curated_atp_matches_csv(
    context: AssetExecutionContext, curated_atp_matches: pd.DataFrame
) -> pd.DataFrame:
    """Publish the readable curated dataset independently of training construction."""
    csv_path = write_curated_csv(curated_atp_matches, CURATED_MATCHES_CSV_NAME)
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
    kinds={"csv", "pandas"},
    tags={**DOMAIN_TAGS, "layer": "publish"},
    automation_condition=WHEN_INPUT_CHANGES,
)
def training_atp_matches_csv(
    context: AssetExecutionContext, training_atp_matches: pd.DataFrame
) -> pd.DataFrame:
    """Publish training rows to data/curated/training_atp_matches.csv."""
    csv_path = write_curated_csv(training_atp_matches, TRAINING_MATCHES_CSV_NAME)
    context.add_output_metadata(
        {
            "csv_path": MetadataValue.path(str(csv_path)),
            **dataframe_metadata(training_atp_matches),
        }
    )
    return training_atp_matches
