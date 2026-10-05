"""Assets that publish the anonymized training dataset.

Flow: W&B artifact (`pre_anonymized_tennis_data:latest`, produced by the
historical pipeline) -> anonymize winner/loser into player0/player1 -> new W&B
artifact (`final_anonymized_tennis_data`).

Reading from W&B instead of from a local file is what keeps the dataset version
that a model was trained on traceable.
"""

import pandas as pd
from dagster import AssetExecutionContext, AutomationCondition, MetadataValue, asset

from ..shared.metadata import dataframe_metadata
from ..shared.paths import CURATED_CSV_NAME, curated_dataset_path
from ..shared.wandb_artifacts import (
    ANONYMIZED_ARTIFACT,
    ANONYMIZED_ARTIFACT_TYPE,
    PRE_ANONYMIZED_ARTIFACT,
    PRE_ANONYMIZED_ARTIFACT_TYPE,
    WandbArtifactsResource,
)
from .anonymization import anonymize

GROUP = "anonymized_dataset"

ANONYMIZED_CSV_NAME = "final_anonymized_matches.csv"

DOMAIN_TAGS = {"domain": "tennis", "source": "wandb"}


@asset(
    group_name=GROUP,
    kinds={"polars", "pandas", "wandb"},
    tags={**DOMAIN_TAGS, "layer": "anonymize"},
    description=(
        "Downloads the latest published pre-anonymization dataset from W&B, shuffles "
        "winner/loser into player0/player1 with a binary `winner` target, and publishes the "
        "result as a new `final_anonymized_tennis_data` artifact."
    ),
    automation_condition=AutomationCondition.on_cron(
        "0 6 * * *", cron_timezone="America/Sao_Paulo"
    ),
)
def anonymized_atp_matches(
    context: AssetExecutionContext, wandb_artifacts: WandbArtifactsResource
) -> pd.DataFrame:
    """Produce the dataset the model is trained on."""
    csv_path = curated_dataset_path(ANONYMIZED_CSV_NAME)

    with wandb_artifacts.run(
        run_name=f"anonymize_{context.run_id[:8]}", job_type="anonymize_dataset"
    ) as wandb_run:
        source_artifact = f"{PRE_ANONYMIZED_ARTIFACT}:latest"
        context.log.info(f"Downloading {source_artifact}")
        curated = wandb_artifacts.fetch_dataframe(
            wandb_run,
            artifact_name=source_artifact,
            artifact_type=PRE_ANONYMIZED_ARTIFACT_TYPE,
            file_name=CURATED_CSV_NAME,
        )

        frame = anonymize(curated)
        target_mean = float(frame["winner"].mean())

        artifact_reference = wandb_artifacts.publish_dataframe(
            wandb_run,
            artifact_name=ANONYMIZED_ARTIFACT,
            artifact_type=ANONYMIZED_ARTIFACT_TYPE,
            frame=frame,
            csv_path=csv_path,
            extra_metadata={
                "dagster_run_id": context.run_id,
                "source_wandb_artifact": source_artifact,
                "target_mean": target_mean,
            },
        )

    if not 0.4 <= target_mean <= 0.6:
        context.log.warning(
            f"Target 'winner' mean is {target_mean:.4f}, expected ~0.5 for a balanced dataset."
        )

    context.add_output_metadata(
        {
            "source_wandb_artifact": MetadataValue.text(source_artifact),
            "wandb_artifact": MetadataValue.text(artifact_reference or "skipped (W&B disabled)"),
            "csv_path": MetadataValue.path(str(csv_path)),
            "target_mean": MetadataValue.float(target_mean),
            **dataframe_metadata(frame),
        }
    )
    return frame
