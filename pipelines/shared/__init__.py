"""Reusable building blocks shared by every Dagster pipeline in this project.

Modules
-------
paths          Canonical filesystem layout (raw inputs, staging snapshots, curated CSVs).
types          Shared type aliases used in asset signatures.
metadata       Helpers that turn a DataFrame into Dagster friendly metadata.
parquet_io     ParquetDataFrameIOManager, the IO manager behind data/staging/.
"""
