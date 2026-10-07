"""Shared data contracts, paths, and Dagster storage helpers.

Modules
-------
paths          Canonical filesystem layout (raw inputs, staging snapshots, curated CSVs).
contracts      Pydantic models, feature names, and DataFrame contracts.
metadata       Helpers that turn a DataFrame into Dagster friendly metadata.
parquet_io     ParquetDataFrameIOManager, the IO manager behind data/staging/.
"""
