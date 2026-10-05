"""Dagster entry point of the anonymized dataset pipeline (loaded by workspace.yaml)."""

from dagster import AssetSelection, Definitions, define_asset_job

from ..shared.parquet_io import ParquetDataFrameIOManager
from ..shared.wandb_artifacts import WandbArtifactsResource
from .assets import anonymized_atp_matches
from .checks import (
    no_player_identity_left,
    schema_matches_expectation,
    target_is_balanced,
    target_is_binary,
)

ASSETS = [anonymized_atp_matches]

ASSET_CHECKS = [
    target_is_binary,
    target_is_balanced,
    no_player_identity_left,
    schema_matches_expectation,
]

materialize_anonymized_dataset = define_asset_job(
    name="materialize_anonymized_dataset",
    description="Read the published dataset from W&B and publish the anonymized version.",
    selection=AssetSelection.keys(*[asset.key for asset in ASSETS]),
)

defs = Definitions(
    assets=ASSETS,
    asset_checks=ASSET_CHECKS,
    jobs=[materialize_anonymized_dataset],
    resources={
        "io_manager": ParquetDataFrameIOManager(),
        "wandb_artifacts": WandbArtifactsResource(),
    },
)
