"""Reusable building blocks shared by every Dagster pipeline in this project.

Modules
-------
paths      Canonical filesystem layout (raw inputs, staging snapshots, curated outputs).
types      Dagster runtime types used in asset signatures.
metadata   Helpers that turn a DataFrame into Dagster/W&B friendly metadata.
parquet_io    ParquetDataFrameIOManager, the IO manager behind data/staging/.
wandb_artifacts  WandbArtifactsResource, the single entry point for W&B calls.
"""
