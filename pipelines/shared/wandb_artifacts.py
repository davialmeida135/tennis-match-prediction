"""W&B artifact access, exposed to the pipelines as a Dagster resource.

Every W&B interaction in the project goes through this class so that project
naming, credentials and offline behaviour are configured in a single place
instead of being scattered (and swallowed) inside asset bodies.

Artifacts exchanged by the pipelines
------------------------------------
pre_anonymized_tennis_data    type=dataset             uploaded by the historical pipeline
final_anonymized_tennis_data  type=processed_dataset   uploaded by the anonymized pipeline
"""

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
import wandb
from dagster import ConfigurableResource, DagsterError
from pydantic import Field

from .metadata import dataframe_profile
from .paths import ensure_dir

# Artifact published by pipelines/historical_matches and consumed here.
PRE_ANONYMIZED_ARTIFACT = "pre_anonymized_tennis_data"
PRE_ANONYMIZED_ARTIFACT_TYPE = "dataset"

# Artifact published by this pipeline, ready for training.
ANONYMIZED_ARTIFACT = "final_anonymized_tennis_data"
ANONYMIZED_ARTIFACT_TYPE = "processed_dataset"

TRUTHY = {"1", "true", "yes", "on"}


class WandbRun(Protocol):
    """Subset of the W&B run API used by this module.

    Typed as a Protocol because `wandb.Run` is not exposed at the package
    top level in every supported wandb version.
    """

    id: str
    run_id: str

    def log_artifact(self, artifact: Any) -> None: ...

    def use_artifact(self, name: str, type: str) -> Any: ...


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    return default if raw is None else raw.strip().lower() in TRUTHY


class WandbArtifactsResource(ConfigurableResource):
    """Publish and download dataset versions on Weights & Biases."""

    project: str = Field(
        default=os.getenv("WANDB_PROJECT", "tennis-match-prediction"),
        description="W&B project that receives and serves the dataset artifacts.",
    )
    entity: str | None = Field(
        default=os.getenv("WANDB_ENTITY") or None,
        description="W&B entity (team or user). Optional.",
    )
    base_url: str | None = Field(
        default=os.getenv("WANDB_BASE_URL") or None,
        description="W&B base URL. Only needed for self-hosted deployments.",
    )
    enabled: bool = Field(
        default=_env_flag("TENNIS_WANDB_ENABLED", True),
        description=(
            "Set TENNIS_WANDB_ENABLED=false to skip every W&B call. Publishing assets then "
            "materialize without uploading anything, which is handy for local development."
        ),
    )

    @contextmanager
    def run(self, run_name: str, job_type: str) -> Iterator[WandbRun | None]:
        """Start a W&B run. Yields `None` when W&B is disabled."""
        if not self.enabled:
            yield None
            return

        init_kwargs: dict[str, Any] = {
            "project": self.project,
            "name": run_name,
            "job_type": job_type,
            "reinit": True,
        }
        if self.entity:
            init_kwargs["entity"] = self.entity
        if self.base_url:
            init_kwargs["base_url"] = self.base_url

        with wandb.init(**init_kwargs) as wandb_run:
            yield wandb_run

    def publish_dataframe(
        self,
        wandb_run: WandbRun | None,
        *,
        artifact_name: str,
        artifact_type: str,
        frame: pd.DataFrame,
        csv_path: Path,
        extra_metadata: dict[str, Any] | None = None,
    ) -> str | None:
        """Write `frame` to `csv_path` and upload it as a new artifact version.

        Returns the `name:version` of the logged artifact, or `None` when W&B is disabled.
        """
        ensure_dir(csv_path.parent)
        frame.to_csv(csv_path, index=False)

        if wandb_run is None:
            return None

        artifact = wandb.Artifact(name=artifact_name, type=artifact_type)
        artifact.add_file(str(csv_path))
        artifact.metadata.update(dataframe_profile(frame))
        for key, value in (extra_metadata or {}).items():
            artifact.metadata[key] = value

        wandb_run.log_artifact(artifact)
        artifact.wait()
        return f"{artifact.name}:{artifact.version}"

    def fetch_dataframe(
        self,
        wandb_run: WandbRun | None,
        *,
        artifact_name: str,
        artifact_type: str,
        file_name: str,
    ) -> pd.DataFrame:
        """Download a published CSV artifact and return it as a DataFrame."""
        if wandb_run is None:
            raise DagsterError(
                f"Cannot read artifact '{artifact_name}': W&B is disabled "
                "(TENNIS_WANDB_ENABLED=false). This asset reads its input from W&B, so it needs "
                "credentials and TENNIS_WANDB_ENABLED enabled."
            )

        artifact = wandb_run.use_artifact(artifact_name, type=artifact_type)
        downloaded = Path(artifact.download())
        csv_path = downloaded / file_name
        if not csv_path.exists():
            raise FileNotFoundError(
                f"Artifact '{artifact.name}' does not contain '{file_name}'. "
                f"Available files: {sorted(path.name for path in downloaded.iterdir())}"
            )
        return pd.read_csv(csv_path)

    def artifact_reference(self, wandb_run: WandbRun | None, artifact_name: str) -> str:
        """Human readable `name:version` of an artifact used by the current run."""
        if wandb_run is None or not wandb_run.run_id:
            return artifact_name
        return f"{artifact_name} (run {wandb_run.run_id})"
