import pandas as pd

from pipelines.historical_matches.transforms.finalization import (
    SURFACE_COLUMNS,
    encode_surface,
)


def test_encode_surface_keeps_fixed_schema_for_carpet_and_missing_values() -> None:
    frame = pd.DataFrame({"surface": ["Carpet", "Hard", None]})

    encoded = encode_surface(frame)

    assert [column for column in encoded if column.startswith("surface_")] == SURFACE_COLUMNS
    assert encoded.loc[0, SURFACE_COLUMNS].tolist() == [0.0, 0.0, 0.0]
    assert encoded.loc[1, SURFACE_COLUMNS].tolist() == [1.0, 0.0, 0.0]
