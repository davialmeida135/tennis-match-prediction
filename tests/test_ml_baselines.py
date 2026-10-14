"""Reference strategies use pre-match comparisons with the correct player orientation."""

from datetime import date

import pandas as pd
import pytest

from tennis_match_prediction.ml.evaluation import baseline_metrics
from tennis_match_prediction.ml.train import train


def test_ranking_and_elo_choose_the_favored_player_and_split_neutral_credit():
    frame = pd.DataFrame(
        {
            "winner": [1, 0, 0, 1],
            "rank_log_advantage": [1.0, -1.0, 1.0, 0.0],
            "overall_elo_diff": [-100.0, -100.0, -100.0, 0.0],
            "surface_elo_diff": [100.0, 100.0, -100.0, 0.0],
        }
    )
    metrics = baseline_metrics(frame, "validation")
    for name in ("rank", "elo", "surface_elo"):
        assert metrics[f"validation_{name}_baseline_accuracy"] == pytest.approx(2.5 / 4)
        assert metrics[f"validation_{name}_baseline_neutral_fraction"] == 0.25
    assert not any("brier" in key or "log_loss" in key for key in metrics)


def test_baselines_survive_model_feature_selection(tmp_path):
    frame = pd.DataFrame(
        {
            "tourney_date": pd.date_range("2024-01-01", periods=60),
            "winner": [0, 1] * 30,
            "age_diff": [float(i % 5) for i in range(60)],
            "rank_log_advantage": [-1.0, 1.0] * 30,
            "overall_elo_diff": [100.0, -100.0] * 30,
            "surface_elo_diff": [0.0] * 60,
        }
    )
    source = tmp_path / "training.csv"
    frame.to_csv(source, index=False)
    result = train(
        source,
        output_dir=tmp_path / "models",
        feature_columns=("age_diff",),
        train_start=date(2024, 1, 11),
        validation_start=date(2024, 2, 1),
        test_start=date(2024, 2, 15),
        tracking_mode="disabled",
    )
    assert result.metrics["validation_rank_baseline_accuracy"] == 1.0
    assert result.metrics["validation_elo_baseline_accuracy"] == 0.0
    assert result.metrics["validation_surface_elo_baseline_accuracy"] == 0.5
    assert not any(name.startswith("test_") for name in result.metrics)


def test_missing_comparisons_are_not_invented():
    assert baseline_metrics(pd.DataFrame({"winner": [0, 1]}), "test") == {}
