"""Dagster entry point of the historical matches pipeline (loaded by workspace.yaml).

Running it builds chronological history features, curated and training datasets,
then publishes curated and training CSVs on separate branches.
"""

from dagster import AssetSelection, Definitions, define_asset_job

from ..shared.parquet_io import ParquetDataFrameIOManager
from .assets import (
    curated_atp_matches,
    curated_atp_matches_csv,
    elo_featured_atp_matches,
    h2h_featured_atp_matches,
    imputed_atp_matches,
    normalized_atp_matches,
    player_comparison_atp_matches,
    raw_atp_matches,
    training_atp_matches,
    training_atp_matches_csv,
    winrate_featured_atp_matches,
)
from .checks import (
    curated_matches_feed_training,
    curated_matches_have_no_walkovers,
    elo_features_present,
    h2h_features_present,
    imputed_matches_have_no_nulls,
    no_player_identity_left,
    normalized_matches_are_chronological,
    normalized_matches_have_no_walkovers,
    normalized_matches_have_required_columns,
    raw_matches_not_empty,
    schema_matches_expectation,
    target_is_balanced,
    target_is_binary,
    winrate_features_are_probabilities,
)
from .config import RawMatchesCsv

ASSETS = [
    raw_atp_matches,
    normalized_atp_matches,
    player_comparison_atp_matches,
    winrate_featured_atp_matches,
    h2h_featured_atp_matches,
    elo_featured_atp_matches,
    imputed_atp_matches,
    curated_atp_matches,
    curated_atp_matches_csv,
    training_atp_matches,
    training_atp_matches_csv,
]

ASSET_CHECKS = [
    raw_matches_not_empty,
    normalized_matches_have_required_columns,
    normalized_matches_are_chronological,
    normalized_matches_have_no_walkovers,
    imputed_matches_have_no_nulls,
    winrate_features_are_probabilities,
    h2h_features_present,
    elo_features_present,
    curated_matches_have_no_walkovers,
    curated_matches_feed_training,
    target_is_binary,
    target_is_balanced,
    no_player_identity_left,
    schema_matches_expectation,
]

materialize_historical_dataset = define_asset_job(
    name="materialize_historical_dataset",
    description=(
        "Rebuild the datasets from the raw CSV, ending with the training rows and exports "
        "written to data/curated."
    ),
    selection=AssetSelection.assets(*ASSETS),
)

defs = Definitions(
    assets=ASSETS,
    asset_checks=ASSET_CHECKS,
    jobs=[materialize_historical_dataset],
    resources={
        # Intermediate snapshots go to staging; prediction history goes to curated.
        "io_manager": ParquetDataFrameIOManager(),
        # Where the raw CSV is read from.
        "raw_matches_csv": RawMatchesCsv(),
    },
)
