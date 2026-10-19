"""Candidate model asset backed by the existing experiment and artifact workflow."""

from datetime import date
from pathlib import Path

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from tennis_match_prediction.ml.train import train
from tennis_match_prediction.pipelines.model_training.config import ModelTrainingConfig


@asset(
    deps=["training_atp_matches_csv"],
    group_name="model_training",
    kinds={"python", "mlflow"},
    tags={"domain": "tennis", "layer": "model"},
)
def trained_model(context: AssetExecutionContext, config: ModelTrainingConfig) -> MaterializeResult:
    """Fit and evaluate a new candidate; never register or promote a serving model."""
    source = Path(config.training_matches_path).resolve()
    if not source.is_file():
        raise FileNotFoundError(
            f"Published training CSV not found at {source}. "
            "Materialize materialize_historical_dataset first, or relocate the export "
            "with training_matches_path."
        )
    result = train(
        training_matches_path=source,
        output_dir=Path(config.output_dir),
        train_start=date.fromisoformat(config.train_start),
        validation_start=date.fromisoformat(config.validation_start),
        test_start=date.fromisoformat(config.test_start),
        feature_columns=tuple(config.feature_columns),
        model_name=config.model_name,
        model_params=config.model_params,
        final_evaluation=config.final_evaluation,
        tracking_mode=config.tracking_mode,
        tracking_uri=config.tracking_uri,
        experiment_name=config.experiment_name,
        run_name=config.run_name,
    )
    context.log.info("Candidate %s saved to %s", result.model_name, result.model_path)
    return MaterializeResult(
        metadata={
            "model_name": result.model_name,
            "model_path": MetadataValue.path(str(result.model_path)),
            "metadata_path": MetadataValue.path(str(result.model_path.with_suffix(".json"))),
            "training_matches_path": MetadataValue.path(str(source)),
            "feature_columns": MetadataValue.json(config.feature_columns),
            "model_param_overrides": MetadataValue.json(config.model_params),
            "train_start": config.train_start,
            "validation_start": config.validation_start,
            "test_start": config.test_start,
            "final_evaluation": config.final_evaluation,
            "mlflow_run_id": result.run_id,
            "artifact_uri": result.artifact_uri,
            "sklearn_model_uri": result.sklearn_model_uri,
            **result.metrics,
        }
    )
