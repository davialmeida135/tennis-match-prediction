"""Dagster entry point of the live matches pipeline."""

from dagster import AssetSelection, Definitions, define_asset_job

from ..shared.parquet_io import ParquetDataFrameIOManager
from .assets import live_matches_snapshot
from .checks import (
    live_matches_are_from_one_day,
    live_matches_have_both_players,
    live_matches_have_expected_columns,
    live_matches_not_empty,
)
from .config import SportDevsApi

ASSETS = [live_matches_snapshot]

ASSET_CHECKS = [
    live_matches_are_from_one_day,
    live_matches_have_both_players,
    live_matches_have_expected_columns,
    live_matches_not_empty,
]

materialize_live_matches = define_asset_job(
    name="materialize_live_matches",
    description="Pull one day of matches from the SportDevs API into data/raw/live_matches/.",
    selection=AssetSelection.keys(*[asset.key for asset in ASSETS]),
)

defs = Definitions(
    assets=ASSETS,
    asset_checks=ASSET_CHECKS,
    jobs=[materialize_live_matches],
    resources={
        "io_manager": ParquetDataFrameIOManager(),
        "sportdevs_api": SportDevsApi(),
    },
)
