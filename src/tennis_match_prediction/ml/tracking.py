"""Explicit MLflow lifecycle; no implicit fallback when tracking fails."""

import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from mlflow.models import infer_signature
from pydantic import JsonValue
from sqlalchemy.engine import make_url

from tennis_match_prediction.contracts import ExperimentConfig
from tennis_match_prediction.ml.models.base import BaseMatchModel, SklearnMatchModel


@dataclass
class TrackedRun:
    run_id: str | None = None
    artifact_uri: str | None = None

    def log_params(self, parameters: dict[str, JsonValue]) -> None:
        if self.run_id is not None:
            mlflow.log_params(parameters)

    def log_metrics(self, metrics: dict[str, float]) -> None:
        if self.run_id is not None:
            mlflow.log_metrics(metrics)

    def log_directory(self, directory: Path, artifact_path: str) -> None:
        if self.run_id is not None:
            mlflow.log_artifacts(str(directory), artifact_path=artifact_path)

    def log_artifact(self, path: Path, artifact_path: str) -> None:
        if self.run_id is not None:
            mlflow.log_artifact(str(path), artifact_path=artifact_path)

    def log_model(
        self, model: BaseMatchModel, example: pd.DataFrame, requirements: Path
    ) -> str | None:
        if self.run_id is None or not isinstance(model, SklearnMatchModel):
            return None
        info = mlflow.sklearn.log_model(
            model.estimator,
            name="model",
            serialization_format="cloudpickle",
            input_example=example,
            signature=infer_signature(example, model.estimator.predict_proba(example)),
            pyfunc_predict_fn="predict_proba",
            pip_requirements=str(requirements),
        )
        return info.model_uri


@contextmanager
def start_tracking(config: ExperimentConfig) -> Iterator[TrackedRun]:
    settings = config.tracking
    if settings.mode == "disabled":
        yield TrackedRun()
        return
    if mlflow.active_run() is not None:
        raise RuntimeError("Finish the active MLflow run before starting an experiment")
    if settings.mode == "local":
        Path(make_url(settings.uri).database).parent.mkdir(parents=True, exist_ok=True)
        settings.local_artifact_dir.mkdir(parents=True, exist_ok=True)
    previous_uri = mlflow.get_tracking_uri()
    previous_environment_uri = os.environ.get("MLFLOW_TRACKING_URI")
    mlflow.set_tracking_uri(settings.uri)
    try:
        client = MlflowClient(tracking_uri=settings.uri)
        try:
            experiment = client.get_experiment_by_name(settings.experiment_name)
            experiment_id = (
                experiment.experiment_id
                if experiment is not None
                else client.create_experiment(
                    settings.experiment_name,
                    artifact_location=settings.local_artifact_dir.resolve().as_uri()
                    if settings.mode == "local"
                    else None,
                )
            )
        except MlflowException as error:
            raise RuntimeError(
                "MLflow tracking is unavailable. Start the configured server, or explicitly "
                "select local/disabled tracking."
            ) from error
        with mlflow.start_run(
            experiment_id=experiment_id,
            run_name=config.run_name,
            tags={
                "model": config.model_name,
                "evaluation_mode": "final" if config.final_evaluation else "validation",
                "history_order": "source_date_match_num",
            },
        ) as active:
            yield TrackedRun(active.info.run_id, active.info.artifact_uri)
    finally:
        mlflow.set_tracking_uri(previous_uri)
        # set_tracking_uri also mutates the process environment in MLflow 3.
        if previous_environment_uri is None:
            os.environ.pop("MLFLOW_TRACKING_URI", None)
        else:
            os.environ["MLFLOW_TRACKING_URI"] = previous_environment_uri
