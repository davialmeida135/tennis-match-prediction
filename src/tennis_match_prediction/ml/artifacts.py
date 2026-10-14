"""Versioned project artifacts preserve feature order and target interpretation."""

import pickle
from dataclasses import dataclass
from pathlib import Path

from tennis_match_prediction.contracts import ModelMetadata
from tennis_match_prediction.ml.models.base import BaseMatchModel


@dataclass
class ModelArtifact:
    model: BaseMatchModel
    metadata: ModelMetadata


def save_artifact(path: Path, model: BaseMatchModel, metadata: ModelMetadata) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Explicit output paths must not silently replace a previous experiment.
    with path.open("xb") as destination:
        pickle.dump({"model": model, "metadata": metadata.model_dump(mode="json")}, destination)
    path.with_suffix(".json").write_text(metadata.model_dump_json(indent=2), encoding="utf-8")


def load_artifact(path: Path) -> ModelArtifact:
    """Load a trusted project artifact; older formats require retraining."""
    with path.open("rb") as source:
        payload = pickle.load(source)
    if not isinstance(payload, dict) or "metadata" not in payload or "model" not in payload:
        raise ValueError(
            "Unsupported model artifact; model must be retrained with the experiment runner"
        )
    metadata = ModelMetadata.model_validate(payload["metadata"])
    model = payload["model"]
    if not isinstance(model, BaseMatchModel) or model.name != metadata.model_name:
        raise ValueError("Artifact model does not match its metadata")
    return ModelArtifact(model, metadata)
