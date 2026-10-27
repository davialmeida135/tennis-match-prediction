"""Dagster entry point of the historical matches pipeline (loaded by workspace.yaml).

Running it builds chronological history features, curated and training datasets,
then publishes curated and training CSVs on separate branches.
The separate train_model job fits a candidate from the published training CSV.
"""

from dagster import AssetSelection, Definitions, define_asset_job

from tennis_match_prediction.pipelines.historical_matches.assets import (
    curated_atp_matches,
    curated_atp_matches_csv,
    elo_featured_atp_matches,
    imputed_atp_matches,
    normalized_atp_matches,
    player_comparison_atp_matches,
    raw_atp_matches,
    training_atp_matches,
    training_atp_matches_csv,
    winrate_featured_atp_matches,
)
from tennis_match_prediction.pipelines.historical_matches.checks import (
    curated_matches_feed_training,
    curated_matches_have_no_walkovers,
    elo_features_present,
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
from tennis_match_prediction.pipelines.historical_matches.config import RawMatchesCsv
from tennis_match_prediction.pipelines.maintenance import DailyMaintenance
from tennis_match_prediction.pipelines.model_training.assets import trained_model
from tennis_match_prediction.pipelines.shared.parquet_io import ParquetDataFrameIOManager

ASSETS = [
    raw_atp_matches,
    normalized_atp_matches,
    player_comparison_atp_matches,
    winrate_featured_atp_matches,
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

train_model = define_asset_job(
    name="train_model",
    description=(
        "Train a candidate from the existing published CSV. Configure model, features, "
        "date splits and MLflow tracking in Launchpad; ingestion is not rerun."
    ),
    selection=AssetSelection.assets(trained_model),
)

defs = Definitions.merge(
    Definitions(
        assets=[*ASSETS, trained_model],
        asset_checks=ASSET_CHECKS,
        jobs=[materialize_historical_dataset, train_model],
        resources={
            # Intermediate snapshots go to staging; prediction history goes to curated.
            "io_manager": ParquetDataFrameIOManager(),
            # Where the raw CSV is read from.
            "raw_matches_csv": RawMatchesCsv(),
        },
    ),
    DailyMaintenance().build_defs(),
)
