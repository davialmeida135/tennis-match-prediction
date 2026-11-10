import pandas as pd
import pytest

from tennis_match_prediction.contracts.features import PLAYER_COMPARISON_FEATURE_COLUMNS
from tennis_match_prediction.transforms.training_rows import (
    FINAL_COLUMN_ORDER,
    MATCH_COLUMNS,
    build_training_rows,
)


def test_temporal_features_are_in_final_dataset_and_follow_random_player_order() -> None:
    row: dict[str, int | float | str] = {column: 0 for column in MATCH_COLUMNS}
    row["tourney_date"] = "2026-01-01"
    row.update({column: 1 for column in PLAYER_COMPARISON_FEATURE_COLUMNS})

    # Extra source context must not enter the training table or influence the swap.
    source = pd.DataFrame([row, row]).assign(winner_id="Alice", score="6-0", winner_rank=None)
    result = build_training_rows(source, random_seed=1)

    assert list(result.columns) == FINAL_COLUMN_ORDER
    assert set(PLAYER_COMPARISON_FEATURE_COLUMNS).issubset(result.columns)
    assert len(result.columns) == 14
    assert result["winner"].tolist() == [1, 0]
    pd.testing.assert_frame_equal(result, build_training_rows(source, random_seed=1))
    for _, match in result.iterrows():
        expected_sign = 1 if match["winner"] == 1 else -1
        assert all(match[column] == expected_sign for column in PLAYER_COMPARISON_FEATURE_COLUMNS)


def test_training_rows_reject_missing_comparison_features() -> None:
    with pytest.raises(ValueError, match="overall_elo_diff"):
        build_training_rows(pd.DataFrame({"tourney_date": ["2026-01-01"]}), random_seed=42)
