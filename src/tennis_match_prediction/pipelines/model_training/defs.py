"""Dagster definitions for candidate model training."""

from dagster import AssetSelection, Definitions, define_asset_job

from tennis_match_prediction.pipelines.model_training.assets import trained_model

train_model = define_asset_job(
    name="train_model",
    description=(
        "Train a candidate from the existing published CSV. Configure model, features, "
        "date splits and MLflow tracking in Launchpad; ingestion is not rerun."
    ),
    selection=AssetSelection.assets(trained_model),
)

defs = Definitions(assets=[trained_model], jobs=[train_model])
