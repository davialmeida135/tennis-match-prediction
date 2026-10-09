"""One experiment workflow shared by every model implementation."""

import json
from uuid import uuid4

from tennis_match_prediction.contracts import ExperimentConfig, ExperimentResult, ModelMetadata
from tennis_match_prediction.ml.artifacts import save_artifact
from tennis_match_prediction.ml.dataset import prepare_dataset
from tennis_match_prediction.ml.evaluation import evaluate
from tennis_match_prediction.ml.models.factory import create_model
from tennis_match_prediction.ml.provenance import capture_provenance
from tennis_match_prediction.ml.tracking import start_tracking


def run_experiment(config: ExperimentConfig) -> ExperimentResult:
    model = create_model(config.model_name, config.model_params)
    dataset = prepare_dataset(config)
    model_path = config.model_path or config.output_dir / uuid4().hex / "model.pkl"
    if model_path.exists() or model_path.with_suffix(".json").exists():
        raise FileExistsError(f"Experiment artifact already exists: {model_path}")
    with start_tracking(config) as tracked:
        run_dir = model_path.parent
        provenance_dir = run_dir / f"{model_path.stem}_provenance"
        provenance_dir.mkdir(parents=True, exist_ok=False)
        provenance = capture_provenance(provenance_dir)
        (provenance_dir / "config.json").write_text(
            config.model_dump_json(indent=2), encoding="utf-8"
        )
        (provenance_dir / "dataset.json").write_text(
            json.dumps({"sha256": dataset.sha256, "split": dataset.split}, indent=2),
            encoding="utf-8",
        )
        (provenance_dir / "runtime.json").write_text(
            json.dumps(provenance, indent=2), encoding="utf-8"
        )
        tracked.log_params(
            {
                "model": model.name,
                "feature_columns": json.dumps(config.feature_columns),
                "feature_count": len(config.feature_columns),
                "dataset_sha256": dataset.sha256,
                "target_convention": "winner=1 means player1 won",
                "train_start": config.train_start.isoformat(),
                "validation_start": config.validation_start.isoformat(),
                "test_start": config.test_start.isoformat(),
                "git_revision": provenance["git_revision"],
                "git_dirty": provenance["git_dirty"],
                **model.get_params(),
            }
        )
        tracked.log_directory(provenance_dir, "provenance")
        features = list(config.feature_columns)
        training = dataset.periods["training"]
        model.fit(training.loc[:, features], training["winner"])
        evaluated = ("validation", "test") if config.final_evaluation else ("validation",)
        metrics = {}
        for name in evaluated:
            metrics.update(evaluate(model, dataset.periods[name], config.feature_columns, name))
        tracked.log_metrics(metrics)
        sklearn_uri = tracked.log_model(
            model, training.loc[:, features].head(5), provenance_dir / "requirements.txt"
        )
        metadata = ModelMetadata(
            schema_version=1,
            target_convention="winner=1 means player1 won",
            history_order="source_date_match_num",
            model_name=model.name,
            feature_columns=config.feature_columns,
            config=config,
            split=dataset.split,
            dataset_sha256=dataset.sha256,
            provenance=provenance,
            metrics=metrics,
            model_params=model.get_params(),
            run_id=tracked.run_id,
            sklearn_model_uri=sklearn_uri,
        )
        save_artifact(model_path, model, metadata)
        # Log just this artifact pair, even for an explicit path in a shared directory.
        tracked.log_artifact(model_path, "project_model")
        tracked.log_artifact(model_path.with_suffix(".json"), "project_model")
        return ExperimentResult(
            model_name=model.name,
            model_path=model_path.resolve(),
            metrics=metrics,
            run_id=tracked.run_id,
            artifact_uri=tracked.artifact_uri,
            sklearn_model_uri=sklearn_uri,
        )
