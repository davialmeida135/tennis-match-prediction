"""Exercise Launchpad configuration through the registered Dagster training job."""

from pathlib import Path

import pandas as pd
import pytest
import yaml
from dagster import AssetKey, DagsterInstance, Definitions, validate_run_config

from tennis_match_prediction.ml.artifacts import load_artifact
from tennis_match_prediction.pipelines.defs import defs
from tennis_match_prediction.pipelines.model_training.assets import trained_model


@pytest.fixture
def training_config(tmp_path):
    source = tmp_path / "training.csv"
    pd.DataFrame(
        {
            "overall_elo_diff": [float(i % 7) for i in range(60)],
            "surface_elo_diff": [float(i % 11) for i in range(60)],
            "winner": [i % 2 for i in range(60)],
            "tourney_date": pd.date_range("2024-01-01", periods=60),
        }
    ).to_csv(source, index=False)
    return {
        "training_matches_path": str(source),
        "output_dir": str(tmp_path / "models"),
        "train_start": "2024-01-11",
        "validation_start": "2024-02-01",
        "test_start": "2024-02-15",
        "feature_columns": ["surface_elo_diff", "overall_elo_diff"],
        "model_name": "random_forest",
        "model_params": {"n_estimators": 3, "max_depth": 2},
        "tracking_mode": "disabled",
        "run_name": "launchpad-test",
    }


def _execute(config):
    with DagsterInstance.ephemeral(settings={"telemetry": {"enabled": False}}) as instance:
        return defs.resolve_job_def("train_model").execute_in_process(
            instance=instance,
            run_config={"ops": {"trained_model": {"config": config}}},
            raise_on_error=False,
        )


def test_training_job_and_example_config_load():
    Definitions.validate_loadable(defs)
    assert trained_model.dependency_keys == {AssetKey("training_atp_matches_csv")}
    assert trained_model.automation_conditions_by_key == {}
    job = defs.resolve_job_def("train_model")
    assert job.graph.node_names() == ["trained_model"]
    assert (
        "trained_model"
        not in defs.resolve_job_def("materialize_historical_dataset").graph.node_names()
    )
    example = Path(__file__).resolve().parents[1] / "examples" / "dagster-training.yaml"
    validate_run_config(job, yaml.safe_load(example.read_text(encoding="utf-8")))
    validate_run_config(job, {})


@pytest.mark.parametrize("final_evaluation", [False, True])
def test_launchpad_materializes_candidate_without_ingestion(training_config, final_evaluation):
    training_config["final_evaluation"] = final_evaluation
    result = _execute(training_config)
    assert result.success
    assert [event.step_key for event in result.get_step_success_events()] == ["trained_model"]
    events = result.asset_materializations_for_node("trained_model")
    assert len(events) == 1
    metadata = events[0].metadata
    artifact = load_artifact(Path(metadata["model_path"].value))
    assert artifact.metadata.model_name == "random_forest"
    assert artifact.metadata.model_params["n_estimators"] == 3
    assert artifact.metadata.feature_columns == ("surface_elo_diff", "overall_elo_diff")
    assert artifact.metadata.config.train_start.isoformat() == "2024-01-11"
    assert artifact.metadata.config.run_name == "launchpad-test"
    assert artifact.metadata.config.tracking.mode == "disabled"
    assert metadata["validation_log_loss"].value == artifact.metadata.metrics["validation_log_loss"]
    assert ("test_log_loss" in metadata) is final_evaluation
    assert metadata["mlflow_run_id"].value is None
    assert Path(metadata["metadata_path"].value).is_file()

    second = _execute(training_config)
    assert second.success
    second_path = (
        second.asset_materializations_for_node("trained_model")[0].metadata["model_path"].value
    )
    assert second_path != metadata["model_path"].value
    assert artifact.metadata.config.model_path is None  # Runner chose a unique artifact path.


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"train_start": "bad-date"}, "Invalid isoformat string"),
        ({"validation_start": "2023-01-01"}, "Dates must satisfy"),
        ({"model_params": {"n_estimators": 0}}, "greater than 0"),
        ({"feature_columns": ["winner"]}, "reserved columns"),
    ],
)
def test_invalid_experiments_fail_without_artifacts(training_config, overrides, message):
    training_config.update(overrides)
    result = _execute(training_config)
    assert not result.success
    assert message in result.failure_data_for_node("trained_model").error.to_string()
    assert not Path(training_config["output_dir"]).exists()


def test_missing_published_csv_has_actionable_error(training_config):
    training_config["training_matches_path"] += ".missing"
    result = _execute(training_config)
    assert not result.success
    assert "Materialize materialize_historical_dataset first" in (
        result.failure_data_for_node("trained_model").error.to_string()
    )
    assert not Path(training_config["output_dir"]).exists()


def test_launchpad_local_tracking_reports_mlflow_identifiers(
    training_config, tmp_path, monkeypatch
):
    from mlflow import MlflowClient

    monkeypatch.setenv("TENNIS_MLFLOW_LOCAL_DIR", str(tmp_path / "mlflow"))
    training_config.update(tracking_mode="local", experiment_name="dagster-candidates")
    result = _execute(training_config)
    assert result.success
    metadata = result.asset_materializations_for_node("trained_model")[0].metadata
    artifact = load_artifact(Path(metadata["model_path"].value))
    client = MlflowClient(tracking_uri=artifact.metadata.config.tracking.uri)
    run = client.get_run(metadata["mlflow_run_id"].value)
    assert run.info.status == "FINISHED"
    assert run.info.run_id == artifact.metadata.run_id
    assert run.info.artifact_uri == metadata["artifact_uri"].value
    assert metadata["sklearn_model_uri"].value == artifact.metadata.sklearn_model_uri
    assert client.get_experiment(run.info.experiment_id).name == "dagster-candidates"
    assert client.search_registered_models() == []
