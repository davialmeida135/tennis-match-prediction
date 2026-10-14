"""Resolve a registered version to its originating run's project artifacts."""

from dataclasses import dataclass
from pathlib import Path

from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from tennis_match_prediction.ml.artifacts import load_artifact


@dataclass(frozen=True)
class RegistryModel:
    name: str
    tracking_uri: str
    alias: str | None = None
    version: str | None = None

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.tracking_uri.strip():
            raise ValueError("Registry model name and tracking URI must not be blank")
        if self.alias is not None and self.version is not None:
            raise ValueError("Choose a registry alias or version, not both")
        if self.version is not None and (not self.version.isdecimal() or int(self.version) < 1):
            raise ValueError("Registry version must be a positive integer")
        if self.alias is not None and not self.alias.strip():
            raise ValueError("Registry alias must not be blank")


def download_registry_model(
    selection: RegistryModel, directory: Path
) -> tuple[Path, dict[str, str]]:
    """Download both project files; never substitute the registry's sklearn flavor."""
    client = MlflowClient(tracking_uri=selection.tracking_uri, registry_uri=selection.tracking_uri)
    try:
        version = (
            client.get_model_version(selection.name, selection.version)
            if selection.version is not None
            else client.get_model_version_by_alias(selection.name, selection.alias or "champion")
        )
        if version.status != "READY" or not version.run_id:
            raise ValueError("Registry model must be READY and reference its originating run")
        run_id = version.run_id
        if client.get_run(run_id).info.status != "FINISHED":
            raise ValueError("Registry model must reference a FINISHED training run")
        files = {
            item.path for item in client.list_artifacts(run_id, "project_model") if not item.is_dir
        }
        pickles = sorted(path for path in files if path.endswith(".pkl"))
        if len(pickles) != 1:
            raise ValueError("Registry run must contain exactly one project_model/*.pkl")
        pickle_path = pickles[0]
        metadata_path = str(Path(pickle_path).with_suffix(".json")).replace("\\", "/")
        if metadata_path not in files:
            raise ValueError("Registry run is missing the project model's matching JSON metadata")
        path = Path(client.download_artifacts(run_id, pickle_path, str(directory)))
        client.download_artifacts(run_id, metadata_path, str(directory))
    except MlflowException as error:
        raise RuntimeError(
            f"Cannot download registry model {selection.name!r} from {selection.tracking_uri}: "
            f"{error}"
        ) from error
    artifact = load_artifact(path)
    if artifact.metadata.run_id != run_id:
        raise ValueError("Project artifact run ID does not match the registry version's run")
    return path, {
        "name": selection.name,
        "version": str(version.version),
        "run_id": run_id,
        "model_uri": f"models:/{selection.name}/{version.version}",
    }
