"""Dagster previews accept missing values without changing typed source columns."""

import json

import pandas as pd

from pipelines.shared.metadata import dataframe_metadata, dataframe_profile


def test_nullable_columns_render_as_json_nulls():
    frame = pd.DataFrame(
        {
            "rank": pd.Series([1, pd.NA], dtype="Int64"),
            "score": pd.Series([0.5, pd.NA], dtype="Float64"),
            "active": pd.Series([True, pd.NA], dtype="boolean"),
            "name": pd.Series(["Alice", pd.NA], dtype="string"),
            "date": pd.to_datetime(["2024-01-01", None]),
            "legacy_float": [1.0, float("nan")],
        }
    )
    original = frame.copy(deep=True)

    profile = dataframe_profile(frame)
    assert profile["preview"][1] == dict.fromkeys(frame.columns)
    assert profile["preview"][0] == {
        "rank": 1,
        "score": 0.5,
        "active": True,
        "name": "Alice",
        "date": "2024-01-01T00:00:00",
        "legacy_float": 1.0,
    }
    assert profile["null_count"] == 6
    json.dumps(profile, allow_nan=False)

    metadata = dataframe_metadata(frame)
    assert metadata["preview"].records[1].data == dict.fromkeys(frame.columns)
    pd.testing.assert_frame_equal(frame, original)
