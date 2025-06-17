import pandas as pd
import polars as pl 
import os
import pathlib
import wandb
import numpy as np 
from dagster import (
    asset, 
    Definitions, 
    DagsterType, 
    Output, 
    AssetExecutionContext, 
    AssetCheckSpec, 
    AssetCheckResult,
    AssetKey,
    define_asset_job
)

from .anonymize import anonymize 

PandasDataFrameAnonymizePast = DagsterType(
    type_check_fn=lambda _, value: isinstance(value, pd.DataFrame),
    name="PandasDataFrameAnonymizePast",
    description="A pandas DataFrame for the anonymize_past pipeline.",
)

# Define a base output folder for this specific pipeline's outputs
ANONYMIZE_PAST_OUTPUT_FOLDER = os.path.join(
    pathlib.Path(__file__).resolve().parent.parent.parent, # Project root
    "data",
    "anonymized_past_from_wandb" # Specific subfolder for outputs of this pipeline
)
os.makedirs(ANONYMIZE_PAST_OUTPUT_FOLDER, exist_ok=True)

# Define the asset check spec
winner_balance_check = AssetCheckSpec(
    name="winner_column_balance_check",
    asset=AssetKey("final_anonymized_data_from_wandb"), # Target this asset
    description="Checks if the mean of the 'winner' column is close to 0.5."
)

@asset(
    name="final_anonymized_data_from_wandb",
    description="Loads 'pre_anonymized_tennis_data:latest' from W&B, applies anonymization, "
                "and yields the anonymized DataFrame. Optionally logs a new W&B artifact.",
    kinds=["wandb"],
    group_name="anonymize_past" # Group assets in Dagit UI
)
def final_anonymized_data_from_wandb_artifact(context: AssetExecutionContext) -> PandasDataFrameAnonymizePast:
    """
    Uses the 'pre_anonymized_tennis_data:latest' W&B artifact,
    applies the anonymize function, and returns the anonymized data.
    """
    wandb_project = os.getenv("WANDB_PROJECT", "tennis-match-prediction")
    # WANDB_ENTITY can be set as an env var or inferred by wandb
    wandb_entity = os.getenv("WANDB_ENTITY") 

    run_name = f"dagster_anonymize_artifact_{context.run_id[:8]}"
    
    try:
        with wandb.init(project=wandb_project, entity=wandb_entity, name=run_name, job_type="anonymize_from_artifact", reinit=True) as run:
            context.log.info(f"Attempting to use W&B artifact 'pre_anonymized_tennis_data:latest' from project '{wandb_project}'")
            
            artifact_to_use_name = "pre_anonymized_tennis_data:latest"
            # The artifact was logged with type 'dataset'
            # The file within it was "pre_anon_dagster_asset_version.csv"
            try:
                artifact = run.use_artifact(artifact_to_use_name, type='dataset')
            except wandb.errors.CommError as e:
                context.log.error(
                    f"Could not retrieve W&B artifact '{artifact_to_use_name}'. "
                    f"Ensure it exists in project '{wandb_project}' (entity: {wandb_entity or 'default'}). Error: {e}"
                )
                raise
            
            artifact_dir = artifact.download()
            context.log.info(f"W&B artifact '{artifact.name}' downloaded to: {artifact_dir}")

            # The file added to the artifact was named "pre_anon_dagster_asset_version.csv"
            pre_anonymized_csv_filename = "pre_anon_dagster_asset_version.csv"
            pre_anonymized_csv_path = os.path.join(artifact_dir, pre_anonymized_csv_filename)

            if not os.path.exists(pre_anonymized_csv_path):
                context.log.error(f"CSV file '{pre_anonymized_csv_filename}' not found in downloaded artifact at {pre_anonymized_csv_path}")
                context.log.info(f"Files in artifact directory '{artifact_dir}': {os.listdir(artifact_dir)}")
                raise FileNotFoundError(f"Expected CSV file not found in W&B artifact: {pre_anonymized_csv_path}")

            context.log.info(f"Loading data from W&B artifact file: {pre_anonymized_csv_path}")
            # The anonymize function expects a Pandas DataFrame (it converts to Polars internally)
            df_pre_anonymized = pd.read_csv(pre_anonymized_csv_path) 
            
            context.log.info("Anonymizing data loaded from W&B artifact...")
            # The anonymize function returns a pandas DataFrame
            df_anonymized = anonymize(df_pre_anonymized.copy()) # Use .copy() if anonymize modifies in-place
            context.log.info("Anonymization complete.")

 # --- Winner column balance check ---
            winner_mean = -1.0 # Default if column not found
            if "winner" in df_anonymized.columns:
                winner_mean = df_anonymized["winner"].mean()
                is_balanced = 0.45 <= winner_mean <= 0.55
                context.log.info(f"Mean of 'winner' column: {winner_mean:.4f}. Balanced: {is_balanced}")
                yield AssetCheckResult(
                    check_name="winner_column_balance_check",
                    passed=is_balanced,
                    metadata={
                        "winner_column_mean": float(winner_mean),
                        "lower_bound": 0.45,
                        "upper_bound": 0.55,
                    },
                )
            else:
                context.log.warning("'winner' column not found in anonymized data. Skipping balance check.")
                yield AssetCheckResult(
                    check_name="winner_column_balance_check",
                    passed=False, # Fail the check if column is missing
                    metadata={"error": "'winner' column not found"},
                )
            # --- End Winner column balance check ---

            final_anonymized_filename = "final_anonymized_from_wandb.csv"
            final_anonymized_path = os.path.join(ANONYMIZE_PAST_OUTPUT_FOLDER, final_anonymized_filename)
            df_anonymized.to_csv(final_anonymized_path, index=False)
            context.log.info(f"Saved final anonymized data (from W&B artifact) to: {final_anonymized_path}")

            new_artifact_name = "final_anonymized_tennis_data" 
            new_artifact_type = "processed_dataset"
            
            logged_artifact = wandb.Artifact(name=new_artifact_name, type=new_artifact_type)
            logged_artifact.add_file(final_anonymized_path)
            logged_artifact.metadata["source_wandb_artifact_name"] = artifact.name 
            logged_artifact.metadata["source_wandb_artifact_version"] = artifact.version
            logged_artifact.metadata["dagster_run_id"] = context.run_id
            logged_artifact.metadata["num_rows"] = df_anonymized.shape[0]
            logged_artifact.metadata["num_columns"] = df_anonymized.shape[1]
            logged_artifact.metadata["columns"] = list(df_anonymized.columns)
            logged_artifact.metadata["data_types"] = df_anonymized.dtypes.astype(str).to_dict()
            logged_artifact.metadata["null_values_per_column"] = df_anonymized.isnull().sum().to_dict()
            numeric_df_anonymized_desc = df_anonymized.select_dtypes(include=np.number).describe().transpose().to_dict()
            logged_artifact.metadata["numeric_column_descriptions"] = numeric_df_anonymized_desc
            logged_artifact.metadata["data_preview"] = df_anonymized.head().to_dict(orient='records')
            if "winner" in df_anonymized.columns:
                 logged_artifact.metadata["winner_column_mean"] = float(winner_mean)

            run.log_artifact(logged_artifact)
            logged_artifact.wait() # Wait for logging to complete
            context.log.info(f"Successfully logged new W&B artifact: {logged_artifact.name} (version: {logged_artifact.version})")

            dagster_output_metadata = {
                "num_rows": df_anonymized.shape[0],
                "num_columns": df_anonymized.shape[1],
                "columns": list(df_anonymized.columns),
                "data_types": df_anonymized.dtypes.astype(str).to_dict(),
                "null_values_per_column": df_anonymized.isnull().sum().to_dict(),
                "numeric_column_descriptions": numeric_df_anonymized_desc,
                "data_preview_head": df_anonymized.head().to_dict(orient='records'),
                "wandb_source_artifact": f"{artifact.name}:{artifact.version}",
                "wandb_logged_artifact": f"{logged_artifact.name}:{logged_artifact.version}",
                "local_save_path": final_anonymized_path
            }
            if "winner" in df_anonymized.columns:
                dagster_output_metadata["winner_column_mean"] = float(winner_mean)

            yield Output(
                df_anonymized,
                metadata=dagster_output_metadata
            )

    except Exception as e:
        context.log.error(f"Error in asset '{context.asset_key.to_user_string()}': {e}")
        raise


anonymize_job = define_asset_job(
    name="anonymize_job",
    selection=[final_anonymized_data_from_wandb_artifact]
)
# Define Dagster Definitions for this pipeline file
# This allows Dagster to load these assets if this file is specified in workspace.yaml
defs = Definitions(
    assets=[final_anonymized_data_from_wandb_artifact],
    jobs=[anonymize_job],

)