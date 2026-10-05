"""Dagster entry point of the historical matches pipeline (loaded by workspace.yaml).

Running it materializes the whole chain: Kaggle CSV -> curated dataset -> W&B
artifact. The downstream `anonymized_dataset` pipeline then picks the artifact up.
"""

from dagster import AssetSelection, Definitions, define_asset_job

from ..shared.parquet_io import ParquetDataFrameIOManager
from ..shared.wandb_artifacts import WandbArtifactsResource
from .assets import (
    curated_atp_matches,
    elo_featured_atp_matches,
    h2h_featured_atp_matches,
    imputed_atp_matches,
    normalized_atp_matches,
    published_pre_anonymized_dataset,
    raw_atp_matches,
    winrate_featured_atp_matches,
)
from .checks import (
    curated_matches_feed_anonymization,
    curated_matches_have_no_walkovers,
    elo_features_present,
    h2h_features_present,
    imputed_matches_have_no_nulls,
    normalized_matches_are_chronological,
    normalized_matches_have_required_columns,
    raw_matches_not_empty,
    winrate_features_are_probabilities,
)
from .config import RawMatchesCsv

ASSETS = [
    raw_atp_matches,
    normalized_atp_matches,
    imputed_atp_matches,
    winrate_featured_atp_matches,
    h2h_featured_atp_matches,
    elo_featured_atp_matches,
    curated_atp_matches,
    published_pre_anonymized_dataset,
]

ASSET_CHECKS = [
    raw_matches_not_empty,
    normalized_matches_have_required_columns,
    normalized_matches_are_chronological,
    imputed_matches_have_no_nulls,
    winrate_features_are_probabilities,
    h2h_features_present,
    elo_features_present,
    curated_matches_have_no_walkovers,
    curated_matches_feed_anonymization,
]

materialize_historical_dataset = define_asset_job(
    name="materialize_historical_dataset",
    description="Rebuild the historical dataset from the raw CSV and publish it to W&B.",
    selection=AssetSelection.keys(*[asset.key for asset in ASSETS]),
)

defs = Definitions(
    assets=ASSETS,
    asset_checks=ASSET_CHECKS,
    jobs=[materialize_historical_dataset],
    resources={
        # Every DataFrame asset is persisted as data/staging/<asset_name>.parquet.
        "io_manager": ParquetDataFrameIOManager(),
        # Where the raw CSV is read from.
        "raw_matches_csv": RawMatchesCsv(),
        # Single entry point for every W&B call of the project.
        "wandb_artifacts": WandbArtifactsResource(),
    },
)
