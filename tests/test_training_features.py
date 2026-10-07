"""Training selections must survive evaluation and model serialization."""

import json
import pickle

import pandas as pd
import pytest

from ml import train as training
from pipelines.shared.contracts import FEATURE_COLUMNS


def test_subset_preserves_feature_order_in_model_and_metadata(tmp_path, monkeypatch):
    frame = pd.DataFrame({name: [float(i % 7) for i in range(60)] for name in FEATURE_COLUMNS})
    frame["winner"] = [i % 2 for i in range(60)]
    frame["match_date"] = pd.date_range("2024-01-01", periods=60)
    source = tmp_path / "matches.csv"
    frame.to_csv(source, index=False)
    selected = ("surface_elo_diff", "overall_elo_diff")
    logged = []
    monkeypatch.setattr(training, "_log_mlflow", lambda *args: logged.append(args))
    model_path = tmp_path / "model.pkl"

    metrics = training.train(source, model_path, feature_columns=selected)

    with model_path.open("rb") as file:
        artifact = pickle.load(file)
    assert artifact["feature_columns"] == selected
    assert tuple(artifact["model"].feature_names_in_) == selected
    assert json.loads(model_path.with_suffix(".json").read_text(encoding="utf-8"))[
        "feature_columns"
    ] == list(selected)
    assert logged == [(model_path, metrics, selected)]
    assert len(metrics) == 6
    assert artifact["model"].predict_proba(frame.loc[:, list(selected)]).shape == (60, 2)


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
        training.train(tmp_path / "absent.csv", tmp_path / "model.pkl", feature_columns=selected)
