import pandas as pd

from tennis_match_prediction.transforms.curation import (
    remove_match_statistics,
)


def test_curation_drops_current_match_statistics_and_preserves_source_context() -> None:
    frame = pd.DataFrame(
        {
            "surface": ["Carpet", "Hard", None],
            "winner_age": [25.0, None, 30.0],
            "w_ace": [10, 4, 5],
            "l_svpt": [100, 80, 90],
            "ace_rate_diff": [0.01, -0.02, 0.03],
        }
    )

    curated = remove_match_statistics(frame)

    pd.testing.assert_frame_equal(curated, frame[["surface", "winner_age", "ace_rate_diff"]])
