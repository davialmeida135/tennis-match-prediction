"""Opt-in HTTP/Compose checks exercise artifact proxying and service persistence."""

import os
import socket
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import pytest
import requests
from mlflow import MlflowClient

from tennis_match_prediction.ml.artifacts import load_artifact
from tennis_match_prediction.ml.train import train
from tennis_match_prediction.paths import PROJECT_ROOT


@pytest.mark.parametrize("backend", ["http", "docker"])
def test_remote_artifacts_survive_restart(tmp_path, monkeypatch, backend):
    if os.getenv(f"TENNIS_TEST_MLFLOW_{backend.upper()}") != "1":
        pytest.skip(f"Set TENNIS_TEST_MLFLOW_{backend.upper()}=1 to run this integration check")
    if backend == "docker":
        port = 5501
    else:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
    uri = f"http://127.0.0.1:{port}"
    environment = {**os.environ, "MLFLOW_PORT": str(port), "MLFLOW_DISABLE_AGENT_HINT": "1"}
    compose = [
        "docker",
        "compose",
        "-p",
        "tennis-mlflow-integration",
        "-f",
        str(PROJECT_ROOT / "compose.mlflow.yaml"),
    ]
    process = None
    log_path = tmp_path / "server.log"

    def stop():
        nonlocal process
        if backend == "docker":
            subprocess.run(compose + ["down"], env=environment, check=True, timeout=120)
        elif process is not None:
            # Windows MLflow may spawn workers; terminate the owned process tree.
            if sys.platform == "win32":
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    check=False,
                    capture_output=True,
                )
            else:
                process.terminate()
            process.wait(timeout=30)
            process = None

    def start():
        nonlocal process
        if backend == "docker":
            subprocess.run(
                compose + ["up", "-d", "--build", "--wait"],
                env=environment,
                check=True,
                timeout=600,
            )
        else:
            with log_path.open("ab") as log:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "mlflow",
                        "server",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                        "--workers",
                        "1",
                        "--backend-store-uri",
                        f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}",
                        "--serve-artifacts",
                        "--artifacts-destination",
                        str(tmp_path / "artifacts"),
                    ],
                    env=environment,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                response = requests.get(f"{uri}/health", timeout=2)
                if response.status_code == 200:
                    return
            except requests.ConnectionError:
                pass
            time.sleep(0.25)
        pytest.fail(f"MLflow did not become ready; inspect {log_path}")

    source = tmp_path / "training.csv"
    frame = pd.DataFrame(
        {
            "overall_elo_diff": [float(i % 7) for i in range(60)],
            "winner": [0, 1] * 30,
            "tourney_date": pd.date_range("2024-01-01", periods=60),
        }
    )
    frame.to_csv(source, index=False)
    results = []
    # Avoid leaking MLflow's environment changes into other tests.
    monkeypatch.setenv("MLFLOW_TRACKING_URI", uri)
    previous_uri = mlflow.get_tracking_uri()
    try:
        start()
        for model_name in ("logistic_regression", "random_forest"):
            results.append(
                train(
                    source,
                    output_dir=tmp_path / "models",
                    feature_columns=("overall_elo_diff",),
                    train_start=date(2024, 1, 11),
                    validation_start=date(2024, 2, 1),
                    test_start=date(2024, 2, 15),
                    model_name=model_name,
                    model_params={"n_estimators": 10} if model_name == "random_forest" else {},
                    tracking_mode="server",
                    tracking_uri=uri,
                    experiment_name=f"integration-{tmp_path.name}",
                )
            )
        stop()
        start()
        client = MlflowClient(tracking_uri=uri)
        mlflow.set_tracking_uri(uri)
        for result in results:
            run = client.get_run(result.run_id)
            assert run.info.status == "FINISHED"
            assert run.info.artifact_uri.startswith("mlflow-artifacts:")
            assert run.data.metrics == result.metrics
            download = client.download_artifacts(result.run_id, "project_model/model.pkl")
            local = load_artifact(Path(download))
            restored = mlflow.sklearn.load_model(result.sklearn_model_uri)
            np.testing.assert_allclose(
                restored.predict_proba(frame[["overall_elo_diff"]])[:, 1],
                local.model.predict_proba(frame[["overall_elo_diff"]]),
            )
    finally:
        stop()
        mlflow.set_tracking_uri(previous_uri)
