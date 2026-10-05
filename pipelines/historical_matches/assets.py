"""Assets that turn the raw ATP match history into a publishable training dataset.

    raw_atp_matches                     read the Kaggle CSV
      -> normalized_atp_matches         dates, chronological order, seed parsing
      -> imputed_atp_matches            null height / age / rank / surface filled
      -> winrate_featured_atp_matches   rolling win rates (overall and per surface)
      -> h2h_featured_atp_matches       head-to-head history
      -> elo_featured_atp_matches       Elo ratings
      -> curated_atp_matches            categorical encodings, leaky stats removed
      -> published_pre_anonymized_dataset   CSV copy + W&B artifact (no table output)

Every transform is a separate asset: each step is materialised (and therefore
observable, comparable and re-runnable) on its own, and the feature engineering
code stays free of Dagster and IO concerns.
"""

from pathlib import Path

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ..shared.metadata import dataframe_metadata
from ..shared.paths import curated_dataset_path, staging_snapshot_path
from ..shared.types import PandasDataFrame
from ..shared.wandb_artifacts import (
    PRE_ANONYMIZED_ARTIFACT,
    PRE_ANONYMIZED_ARTIFACT_TYPE,
    WandbArtifactsResource,
)
from .config import RawMatchesCsv
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
from .transforms.normalization import preprocess_dates, sort_by_date, transform_seed_data
from .transforms.player_stats import calcular_elo, calcular_h2h
from .transforms.winrate import (
    calcular_winrate_superficie,
    calcular_winrate_superficie_ultimas_n,
    calcular_winrate_total,
    calcular_winrate_ultimas_n,
)

GROUP = "historical_matches"

# Run a step as soon as its input is updated (declarative automation: enable the
# `default_automation_condition_sensor` to let Dagster launch these runs).
# The root asset stays manual on purpose: rebuilding every feature is expensive,
# so somebody has to ask for it.
WHEN_INPUT_CHANGES = AutomationCondition.eager()

DOMAIN_TAGS = {"domain": "tennis", "source": "kaggle"}


@asset(
    group_name=GROUP,
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
            f"Raw ATP match CSV not found at {csv_path}. Download it from "
            "https://www.kaggle.com/datasets/dissfya/atp-tennis-2000-2023daily-pull into "
            "data/raw/historical_matches or point TENNIS_RAW_MATCHES_CSV somewhere else."
        )

    context.log.info(f"Reading raw matches from {csv_path}")
    frame = pd.read_csv(csv_path)
    context.add_output_metadata(
        {"source_path": MetadataValue.path(str(csv_path)), **dataframe_metadata(frame)}
    )
    return frame


@asset(
    group_name=GROUP,
    kinds={"pandas"},
    tags={**DOMAIN_TAGS, "layer": "normalize"},
    description="Tourney dates parsed, matches sorted chronologically, seeds expanded into flags.",
    automation_condition=WHEN_INPUT_CHANGES,
)
def normalized_atp_matches(raw_atp_matches: PandasDataFrame) -> PandasDataFrame:
    """Make the dataset chronological and split the seed columns into numeric + flags."""
    frame = preprocess_dates(raw_atp_matches)
    frame = sort_by_date(frame)
    return transform_seed_data(frame)


@asset(
    group_name=GROUP,
    kinds={"pandas"},
    tags={**DOMAIN_TAGS, "layer": "impute"},
    description=(
        "Missing surface, height, age and ranking values replaced by conservative defaults."
    ),
    automation_condition=WHEN_INPUT_CHANGES,
)
def imputed_atp_matches(normalized_atp_matches: PandasDataFrame) -> PandasDataFrame:
    """Replace nulls so downstream feature engineering never has to guard against them."""
    frame = fill_null_surface(normalized_atp_matches)
    frame = fill_null_height(frame)
    frame = fill_null_age(frame)
    return fill_null_rank(frame)


@asset(
    group_name=GROUP,
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
    group_name=GROUP,
    kinds={"pandas", "polars"},
    tags={**DOMAIN_TAGS, "layer": "features"},
    description="Number of previous meetings between the two players.",
    automation_condition=WHEN_INPUT_CHANGES,
)
def h2h_featured_atp_matches(winrate_featured_atp_matches: PandasDataFrame) -> PandasDataFrame:
    """Attach the head-to-head history of each matchup."""
    return calcular_h2h(winrate_featured_atp_matches)


@asset(
    group_name=GROUP,
    kinds={"pandas", "polars"},
    tags={**DOMAIN_TAGS, "layer": "features"},
    description="Elo rating per player plus the pre-match rating difference.",
    automation_condition=WHEN_INPUT_CHANGES,
)
def elo_featured_atp_matches(h2h_featured_atp_matches: PandasDataFrame) -> PandasDataFrame:
    """Attach Elo ratings, which double as the baseline the model has to beat."""
    return calcular_elo(h2h_featured_atp_matches)


@asset(
    group_name=GROUP,
    kinds={"pandas"},
    tags={**DOMAIN_TAGS, "layer": "curate"},
    description=(
        "Walkovers removed, surface one-hot encoded, round/tourney level/handedness coded "
        "and per-match statistics dropped. This is the dataset handed over to the "
        "anonymized_dataset pipeline."
    ),
    automation_condition=WHEN_INPUT_CHANGES,
)
def curated_atp_matches(elo_featured_atp_matches: PandasDataFrame) -> PandasDataFrame:
    """Produce the pre-anonymization dataset, ready to be published."""
    frame = remove_wo(elo_featured_atp_matches)
    frame = encode_surface(frame)
    frame = transform_round(frame)
    frame = transform_tourney_level(frame)
    frame = transform_handedness(frame)
    return remove_stat_cols(frame)


@asset(
    group_name=GROUP,
    kinds={"python", "wandb"},
    tags={"domain": "tennis", "source": "wandb", "layer": "publish"},
    deps=[curated_atp_matches],
    description=(
        "Writes the curated dataset to data/curated and uploads it to W&B as a new version of "
        f"the '{PRE_ANONYMIZED_ARTIFACT}' artifact. Only the previous step's snapshot is read, "
        "so this asset publishes without re-running the feature engineering."
    ),
    automation_condition=WHEN_INPUT_CHANGES,
)
def published_pre_anonymized_dataset(
    context: AssetExecutionContext, wandb_artifacts: WandbArtifactsResource
) -> MaterializeResult:
    """Publish a versioned copy of the curated dataset on W&B."""
    snapshot = staging_snapshot_path("curated_atp_matches")
    csv_path = curated_dataset_path()
    frame = pd.read_parquet(snapshot)

    with wandb_artifacts.run(
        run_name=f"publish_curated_{context.run_id[:8]}", job_type="publish_dataset"
    ) as wandb_run:
        artifact_reference = wandb_artifacts.publish_dataframe(
            wandb_run,
            artifact_name=PRE_ANONYMIZED_ARTIFACT,
            artifact_type=PRE_ANONYMIZED_ARTIFACT_TYPE,
            frame=frame,
            csv_path=csv_path,
            extra_metadata={
                "dagster_run_id": context.run_id,
                "source_asset_key": curated_atp_matches.key.to_user_string(),
            },
        )

    return MaterializeResult(
        metadata={
            "rows_published": MetadataValue.int(len(frame)),
            "csv_path": MetadataValue.path(str(csv_path)),
            "wandb_artifact": MetadataValue.text(artifact_reference or "skipped (W&B disabled)"),
            **dataframe_metadata(frame),
        }
    )
