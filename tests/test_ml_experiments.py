"""Behavioral checks for model interchangeability, evaluation and tracking."""

import os
import pickle
from datetime import date
from hashlib import sha256
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import pytest
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from sklearn.exceptions import NotFittedError
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from tennis_match_prediction.contracts import FutureMatchRequest, PlayerHistory, PlayerState
from tennis_match_prediction.ml.artifacts import load_artifact
from tennis_match_prediction.ml.config import tracking_settings
from tennis_match_prediction.ml.evaluation import validate_probabilities
from tennis_match_prediction.ml.models.factory import create_model
from tennis_match_prediction.ml.models.logistic_regression import LogisticRegressionModel
from tennis_match_prediction.ml.predict import predict
from tennis_match_prediction.ml.train import train


@pytest.fixture
def experiment_data(tmp_path):
    frame = pd.DataFrame(
        {
            "overall_elo_diff": [float(i % 7) for i in range(60)],
            "surface_elo_diff": [float(i % 11) for i in range(60)],
            "winner": [i % 2 for i in range(60)],
            "tourney_date": pd.date_range("2024-01-01", periods=60),
        }
    )
    source = tmp_path / "training.csv"
    frame.to_csv(source, index=False)
    return frame, {
        "training_matches_path": source,
        "output_dir": tmp_path / "models",
        "train_start": date(2024, 1, 11),
        "validation_start": date(2024, 2, 1),
        "test_start": date(2024, 2, 15),
        "feature_columns": ("surface_elo_diff", "overall_elo_diff"),
        "tracking_mode": "disabled",
    }


@pytest.mark.parametrize("model_name", ["logistic_regression", "random_forest"])
def test_model_round_trip_and_future_prediction(experiment_data, tmp_path, model_name):
    frame, options = experiment_data
    parameters = {"n_estimators": 10} if model_name == "random_forest" else {}
    result = train(**options, model_name=model_name, model_params=parameters)
    artifact = load_artifact(result.model_path)
    expected = create_model(model_name, parameters)
    features = list(options["feature_columns"])
    expected.fit(frame.loc[10:30, features], frame.loc[10:30, "winner"])
    np.testing.assert_allclose(
        artifact.model.predict_proba(frame[features]), expected.predict_proba(frame[features])
    )
    assert (
        artifact.metadata.dataset_sha256
        == sha256(options["training_matches_path"].read_bytes()).hexdigest()
    )
    history = PlayerHistory(
        last_source_date=date(2024, 3, 1),
        players={"a": PlayerState(name="Alice"), "b": PlayerState(name="Bob")},
    )
    history_path = tmp_path / "history.parquet"
    pd.DataFrame({"history": [history.model_dump_json()]}).to_parquet(history_path)
    request = FutureMatchRequest(
        player0_name="Alice", player1_name="Bob", match_date="2024-03-02", surface="Hard"
    )
    prediction = predict(request, result.model_path, history_path)
    assert prediction.player0_win_probability + prediction.player1_win_probability == 1
    assert prediction.player1_win_probability == pytest.approx(
        expected.predict_proba(pd.DataFrame([[0.0, 0.0]], columns=features))[0]
    )


def test_logistic_regression_matches_original_baseline(experiment_data):
    frame, options = experiment_data
    features = list(options["feature_columns"])
    original = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000, random_state=42))
    original.fit(frame.loc[10:30, features], frame.loc[10:30, "winner"])
    result = train(**options)
    actual = load_artifact(result.model_path).model.predict_proba(frame[features])
    np.testing.assert_allclose(actual, original.predict_proba(frame[features])[:, 1], atol=1e-12)


def test_test_set_is_only_scored_when_explicit(experiment_data, monkeypatch):
    frame, options = experiment_data
    seen = []
    original = LogisticRegressionModel.predict_proba

    def record(self, features):
        seen.extend(features.index.tolist())
        return original(self, features)

    monkeypatch.setattr(LogisticRegressionModel, "predict_proba", record)
    first = train(**options)
    assert seen == list(range(31, 45))
    seen.clear()
    second = train(**options, final_evaluation=True)
    assert seen == list(range(31, len(frame)))
    assert first.model_path != second.model_path
    assert "test_log_loss" not in first.metrics
    assert "test_log_loss" in second.metrics
    assert "validation_elo_baseline_accuracy" in first.metrics


@pytest.mark.parametrize("name", ["logistic_regression", "random_forest"])
def test_models_reject_unknown_settings_and_unfitted_prediction(name):
    with pytest.raises(ValueError, match="Extra inputs"):
        create_model(name, {"typo": 1})
    with pytest.raises(NotFittedError):
        create_model(name, {}).predict_proba(pd.DataFrame({"feature": [1.0]}))


def test_positive_class_is_resolved_by_label(experiment_data):
    frame, options = experiment_data
    model = create_model("random_forest", {"n_estimators": 5})
    features = frame[list(options["feature_columns"])]
    model.fit(features, frame["winner"])
    raw = model.estimator.predict_proba(features)
    model.estimator.classes_ = np.array([1, 0])
    np.testing.assert_allclose(model.predict_proba(features), raw[:, 0])


@pytest.mark.parametrize("values", [[0.2], [[0.1, 0.9]], [0.1, np.nan], [-0.1, 1.1]])
def test_invalid_model_probabilities_fail(values):
    with pytest.raises(ValueError):
        validate_probabilities(np.asarray(values), 2)


def test_artifact_rejects_old_format_and_invalid_schema(experiment_data, tmp_path):
    path = tmp_path / "old.pkl"
    path.write_bytes(pickle.dumps({"model": "old"}))
    with pytest.raises(ValueError, match="retrain"):
        load_artifact(path)
    _, options = experiment_data
    result = train(**options)
    payload = pickle.loads(result.model_path.read_bytes())
    payload["metadata"]["schema_version"] = 99
    path.write_bytes(pickle.dumps(payload))
    with pytest.raises(ValueError, match="schema_version"):
        load_artifact(path)
    payload["metadata"]["schema_version"] = 1
    del payload["metadata"]["history_order"]
    path.write_bytes(pickle.dumps(payload))
    with pytest.raises(ValueError, match="history_order"):
        load_artifact(path)


def test_explicit_artifact_is_never_overwritten(experiment_data, tmp_path):
    _, options = experiment_data
    path = tmp_path / "selected.pkl"
    train(**options, model_path=path)
    before = path.read_bytes()
    with pytest.raises(FileExistsError):
        train(**options, model_path=path)
    assert path.read_bytes() == before


def test_tracking_precedence_and_explicit_local_mode(tmp_path, monkeypatch):
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://environment:5000")
    monkeypatch.setenv("MLFLOW_EXPERIMENT_NAME", "environment")
    monkeypatch.setenv("TENNIS_MLFLOW_LOCAL_DIR", str(tmp_path))
    settings = tracking_settings(mode="server", uri="http://override:5000", experiment_name="cli")
    assert settings.uri == "http://override:5000"
    assert settings.experiment_name == "cli"
    local = tracking_settings(mode="local", uri=None, experiment_name=None)
    assert local.uri.startswith("sqlite:///")
    assert local.local_artifact_dir == tmp_path / "artifacts"
    assert local.experiment_name == "environment"


def test_local_mlflow_round_trip_and_failed_run(experiment_data, tmp_path, monkeypatch):
    frame, options = experiment_data
    monkeypatch.setenv("TENNIS_MLFLOW_LOCAL_DIR", str(tmp_path / "tracking"))
    options["tracking_mode"] = "local"
    options["experiment_name"] = "integration"
    previous_uri = mlflow.get_tracking_uri()
    previous_environment_uri = os.environ.get("MLFLOW_TRACKING_URI")
    # Restore all environment changes made by MLflow's fluent API after this test.
    monkeypatch.setenv("MLFLOW_TRACKING_URI", previous_environment_uri or previous_uri)
    result = train(**options)
    settings = tracking_settings(mode="local", uri=None, experiment_name="integration")
    client = MlflowClient(tracking_uri=settings.uri)
    run = client.get_run(result.run_id)
    assert run.info.status == "FINISHED"
    assert run.data.metrics == result.metrics
    assert "test_log_loss" not in run.data.metrics
    assert (
        run.data.params["dataset_sha256"]
        == load_artifact(result.model_path).metadata.dataset_sha256
    )
    assert mlflow.get_tracking_uri() == previous_uri
    assert os.environ["MLFLOW_TRACKING_URI"] == (previous_environment_uri or previous_uri)
    try:
        mlflow.set_tracking_uri(settings.uri)
        restored = mlflow.sklearn.load_model(result.sklearn_model_uri)
        columns = list(options["feature_columns"])
        np.testing.assert_allclose(
            restored.predict_proba(frame[columns])[:, 1],
            load_artifact(result.model_path).model.predict_proba(frame[columns]),
        )
        pyfunc = mlflow.pyfunc.load_model(result.sklearn_model_uri)
        np.testing.assert_allclose(
            pyfunc.predict(frame[columns]), restored.predict_proba(frame[columns])
        )
    finally:
        mlflow.set_tracking_uri(previous_uri)
    downloaded = client.download_artifacts(result.run_id, "project_model/model.pkl")
    assert load_artifact(Path(downloaded)).metadata.run_id == result.run_id

    def fail_fit(*_):
        raise RuntimeError("deliberate fit failure")

    monkeypatch.setattr(LogisticRegressionModel, "fit", fail_fit)
    with pytest.raises(RuntimeError, match="deliberate fit failure"):
        train(**options)
    runs = client.search_runs([run.info.experiment_id])
    assert {item.info.status for item in runs} == {"FINISHED", "FAILED"}
    assert mlflow.get_tracking_uri() == previous_uri


def test_server_failure_does_not_fall_back(experiment_data, monkeypatch):
    _, options = experiment_data
    options["tracking_mode"] = "server"

    def unavailable(*_):
        raise MlflowException("unreachable")

    monkeypatch.setattr(MlflowClient, "get_experiment_by_name", unavailable)
    with pytest.raises(RuntimeError, match="MLflow tracking is unavailable"):
        train(**options)
    assert not options["output_dir"].exists()
