import pandas as pd

from pipelines.historical_matches.transforms.training_rows import (
    FINAL_COLUMN_ORDER,
    MATCH_COLUMNS,
    PLAYER_ATTRIBUTE_STEMS,
    SIGNED_MATCH_COLUMNS,
    build_training_rows,
)
from pipelines.shared.contracts import FEATURE_COLUMNS


def test_temporal_features_are_in_final_dataset_and_follow_random_player_order() -> None:
    row: dict[str, int | float | bool] = {column: 0 for column in MATCH_COLUMNS}
    row.update({column: 1 for column in SIGNED_MATCH_COLUMNS})
    row.update(
        {f"{side}_{stem}": 1 for side in ("winner", "loser") for stem in PLAYER_ATTRIBUTE_STEMS}
    )

    result = build_training_rows(pd.DataFrame([row, row]), random_seed=1)

    assert list(result.columns) == FINAL_COLUMN_ORDER
    assert set(FEATURE_COLUMNS).issubset(result.columns)
    for _, match in result.iterrows():
        expected_sign = 1 if match["winner"] == 1 else -1
        assert all(match[column] == expected_sign for column in SIGNED_MATCH_COLUMNS)
