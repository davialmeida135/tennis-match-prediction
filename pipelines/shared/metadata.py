"""DataFrame profiles and validation summaries displayed in Dagster."""

import math
from typing import Any

import numpy as np
import pandas as pd
from dagster import MetadataValue, TableRecord

PREVIEW_ROWS = 5


def jsonable(value: Any) -> Any:
    """Convert numpy/pandas scalars, NaN and timestamps into JSON friendly values."""
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(item) for item in value]
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return None if not math.isfinite(number) else number
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, (bool, int, str)):
        return value
    return str(value)


def dataframe_profile(frame: pd.DataFrame, *, preview_rows: int = PREVIEW_ROWS) -> dict[str, Any]:
    """Summarise a DataFrame (shape, schema, nulls, preview) with plain Python values."""
    profile: dict[str, Any] = {
        "row_count": int(len(frame)),
        "column_count": int(frame.shape[1]),
        "columns": list(frame.columns),
        "dtypes": {str(column): str(dtype) for column, dtype in frame.dtypes.items()},
        "null_count": int(frame.isna().sum().sum()),
        "nulls_by_column": {
            str(column): int(count) for column, count in frame.isna().sum().items() if count
        },
        "preview": jsonable(frame.head(preview_rows).fillna("").to_dict(orient="records")),
    }

    numeric = frame.select_dtypes(include=[np.number])
    if not numeric.empty:
        summary = numeric.describe().transpose().reset_index()
        summary = summary.rename(columns={summary.columns[0]: "column"})
        profile["numeric_summary"] = jsonable(summary.to_dict(orient="records"))

    date_column = _find_date_column(frame)
    if date_column is not None:
        profile["date_range"] = {
            "column": date_column,
            "min": jsonable(frame[date_column].min()),
            "max": jsonable(frame[date_column].max()),
        }

    return profile


def _find_date_column(frame: pd.DataFrame) -> str | None:
    candidates = [name for name in ("tourney_date", "start_time", "date") if name in frame.columns]
    return candidates[0] if candidates else None


def dagster_metadata(profile: dict[str, Any]) -> dict[str, MetadataValue]:
    """Render a profile as Dagster metadata (renders as tables/charts in the UI)."""
    rendered: dict[str, MetadataValue] = {
        "row_count": MetadataValue.int(profile["row_count"]),
        "column_count": MetadataValue.int(profile["column_count"]),
        "columns": MetadataValue.json(profile["columns"]),
        "dtypes": MetadataValue.json(profile["dtypes"]),
        "null_count": MetadataValue.int(profile["null_count"]),
    }

    if profile.get("nulls_by_column"):
        rendered["nulls_by_column"] = MetadataValue.json(profile["nulls_by_column"])
    if profile.get("preview"):
        rendered["preview"] = MetadataValue.table(
            [TableRecord(jsonable(record)) for record in profile["preview"]]
        )
    if profile.get("numeric_summary"):
        rendered["numeric_summary"] = MetadataValue.table(
            [TableRecord(jsonable(record)) for record in profile["numeric_summary"]]
        )
    if profile.get("date_range"):
        date_range = profile["date_range"]
        rendered["date_range"] = MetadataValue.text(
            f"{date_range['column']}: {date_range['min']} -> {date_range['max']}"
        )

    return rendered


def dataframe_metadata(
    frame: pd.DataFrame, *, preview_rows: int = PREVIEW_ROWS
) -> dict[str, MetadataValue]:
    """One-liner helper: profile a DataFrame straight into Dagster metadata."""
    return dagster_metadata(dataframe_profile(frame, preview_rows=preview_rows))


def columns_present(frame: pd.DataFrame, columns: list[str]) -> tuple[bool, dict[str, Any]]:
    """Check helper: are all `columns` available? Does not care about null values.

    Use this before imputation, when nulls are expected but a missing column means
    the upstream schema changed.
    """
    missing = [column for column in columns if column not in frame.columns]
    return not missing, {
        "columns_checked": list(columns),
        "missing_columns": missing,
        "column_count": int(frame.shape[1]),
    }


def columns_without_nulls(frame: pd.DataFrame, columns: list[str]) -> tuple[bool, dict[str, Any]]:
    """Check helper shared by the asset checks: are `columns` present and fully populated?"""
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        return False, {"missing_columns": missing}

    null_counts = {column: int(frame[column].isna().sum()) for column in columns}
    populated = {column: count for column, count in null_counts.items() if count}
    return not populated, {"null_counts": null_counts, "columns_with_nulls": populated}
