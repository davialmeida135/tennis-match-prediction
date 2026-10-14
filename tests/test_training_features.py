"""Training selections must survive evaluation and model serialization."""

import json
from datetime import date

import pandas as pd
import pytest

from tennis_match_prediction.ml import train as training
from tennis_match_prediction.ml.artifacts import load_artifact


def test_subset_preserves_feature_order_in_model_and_metadata(tmp_path):
    selected = ("surface_elo_diff", "overall_elo_diff")
    frame = pd.DataFrame({name: [float(i % 7) for i in range(60)] for name in selected})
    frame["winner"] = [i % 2 for i in range(60)]
    frame["tourney_date"] = pd.date_range("2024-01-01", periods=60)
    source = tmp_path / "matches.csv"
    frame.to_csv(source, index=False)
    model_path = tmp_path / "model.pkl"

    result = training.train(
        source,
        model_path,
        feature_columns=selected,
        train_start=date(2024, 1, 11),
        validation_start=date(2024, 2, 1),
        test_start=date(2024, 2, 15),
        tracking_mode="disabled",
    )

    artifact = load_artifact(model_path)
    assert artifact.metadata.feature_columns == selected
    assert tuple(artifact.model.estimator.feature_names_in_) == selected
    assert json.loads(model_path.with_suffix(".json").read_text(encoding="utf-8"))[
        "feature_columns"
    ] == list(selected)
    assert result.run_id is None
    assert len(result.metrics) == 7
    assert all(name.startswith("validation_") for name in result.metrics)
    assert artifact.model.predict_proba(frame.loc[:, list(selected)]).shape == (60,)
    assert artifact.model.estimator[0].n_samples_seen_ == 21
    assert artifact.metadata.split["periods"]["warmup"]["rows"] == 10
    assert artifact.metadata.split["periods"]["validation"]["rows"] == 14
    assert artifact.metadata.split["periods"]["test"]["rows"] == 15


def test_date_boundaries_keep_tied_dates_together_and_exclude_warmup(tmp_path):
    feature = "overall_elo_diff"
    frame = pd.DataFrame(
        {
            "tourney_date": ["2023-01-01"] * 10
            + ["2024-01-01"] * 20
            + ["2025-01-01"] * 10
            + ["2026-01-01"] * 10,
            feature: [10000.0] * 10 + [2.0] * 20 + [500.0] * 20,
            "winner": [0, 1] * 25,
        }
    )
    source = tmp_path / "matches.csv"
    frame.to_csv(source, index=False)
    model_path = tmp_path / "model.pkl"
    training.train(
        source,
        model_path,
        feature_columns=(feature,),
        train_start=date(2024, 1, 1),
        validation_start=date(2025, 1, 1),
        test_start=date(2026, 1, 1),
        tracking_mode="disabled",
    )
    artifact = load_artifact(model_path)
    assert artifact.model.estimator[0].mean_.tolist() == [2.0]
    assert {
        name: period["rows"] for name, period in artifact.metadata.split["periods"].items()
    } == {
        "warmup": 10,
        "training": 20,
        "validation": 10,
        "test": 10,
    }


@pytest.mark.parametrize(
    ("starts", "message"),
    [
        (("2024-02-01", "2024-01-01", "2024-03-01"), "Dates must satisfy"),
        (("2024-01-11", "2024-01-11", "2024-02-15"), "Dates must satisfy"),
        (("2024-01-01", "2024-02-01", "2024-02-15"), "warmup period"),
        (("2024-01-11", "2024-01-12", "2024-01-13"), "both target classes"),
        (("2024-03-01", "2024-04-01", "2024-05-01"), "training period"),
        (("2024-01-11", "2024-03-01", "2024-04-01"), "validation period"),
        (("2024-01-11", "2024-02-01", "2024-03-01"), "test period"),
    ],
)
def test_invalid_periods_fail_without_writing_model(tmp_path, starts, message):
    frame = pd.DataFrame(
        {
            "overall_elo_diff": range(60),
            "winner": [0, 1] * 30,
            "tourney_date": pd.date_range("2024-01-01", periods=60),
        }
    )
    source = tmp_path / "matches.csv"
    frame.to_csv(source, index=False)
    model_path = tmp_path / "model.pkl"
    with pytest.raises(ValueError, match=message):
        training.train(
            source,
            model_path,
            feature_columns=("overall_elo_diff",),
            train_start=date.fromisoformat(starts[0]),
            validation_start=date.fromisoformat(starts[1]),
            test_start=date.fromisoformat(starts[2]),
        )
    assert not model_path.exists()


@pytest.mark.parametrize(
    ("selected", "message"),
    [
        ((), "at least one"),
        (("overall_elo_diff", "overall_elo_diff"), "duplicates"),
        (("winner",), "Unknown training features"),
    ],
)
def test_invalid_selection_fails_before_loading_data(tmp_path, selected, message):
    with pytest.raises(ValueError, match=message):
        training.train(
            tmp_path / "absent.csv",
            tmp_path / "model.pkl",
            feature_columns=selected,
            train_start=date(2024, 1, 11),
            validation_start=date(2024, 2, 1),
            test_start=date(2024, 2, 15),
        )
