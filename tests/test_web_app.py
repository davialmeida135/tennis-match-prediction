"""HTTP behavior, ID resolution, cached inference and model compatibility."""

import json
import re
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from shutil import copyfile
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from tennis_match_prediction.contracts.experiments import ExperimentConfig, ModelMetadata
from tennis_match_prediction.contracts.model_settings import LogisticRegressionSettings
from tennis_match_prediction.contracts.player_history import PlayedMatch, PlayerHistory, PlayerState
from tennis_match_prediction.ml.artifacts import load_artifact, save_artifact
from tennis_match_prediction.ml.models.logistic_regression import LogisticRegressionModel
from tennis_match_prediction.ml.predict import predict, probability_for
from tennis_match_prediction.web.app import create_app
from tennis_match_prediction.web.registry import RegistryModel
from tennis_match_prediction.web.service import PlayerService


def _upcoming_match(match_id=100):
    return {
        "id": match_id,
        "status": "upcoming",
        "event_status": None,
        "tour": "atp",
        "draw": "singles",
        "is_qualifying": False,
        "surface": "hard",
        "tournament": "Example Open",
        "round_code": "R32",
        "scheduled_time": "2099-01-01T14:00:00Z",
        "players": {
            "p1": {"id": 10, "name": "Alice Example"},
            "p2": {"id": 20, "name": "Bob Example"},
        },
    }


@pytest.fixture
def upcoming_client(web_files, tmp_path, monkeypatch):
    history_path, models_dir, model_path = web_files
    frame = pd.read_parquet(history_path)
    history = json.loads(frame.loc[0, "history"])
    del history["players"]["alice-two"]
    frame.loc[0, "history"] = json.dumps(history)
    frame.to_parquet(history_path, index=False)
    monkeypatch.setenv("LIVE_TENNIS_API_KEY", "test-key")
    application = create_app(
        history_path=history_path,
        models_dir=models_dir,
        model_path=model_path,
        predictions_dir=tmp_path / "predictions",
    )
    with TestClient(application) as client:
        yield client, application, tmp_path / "predictions"


def test_upcoming_batch_skips_unavailable_and_postponed_and_is_reproducible(
    upcoming_client, monkeypatch, web_files
):
    client, application, directory = upcoming_client
    rows = [_upcoming_match(i) for i in range(100, 106)]
    rows[1]["event_status"] = "Postponed"
    rows[2]["players"]["p2"]["name"] = "Unknown Player"
    rows[3]["status"] = "live"
    rows[4]["scheduled_time"] = "2000-01-01T14:00:00Z"
    rows[5]["surface"] = None
    calls = []

    class Response:
        status_code = 200

        def json(self):
            return {"data": rows, "meta": {"has_more": False}}

    def get(url, **kwargs):
        calls.append(kwargs["params"])
        assert kwargs["headers"]["X-API-Key"] == "test-key"
        return Response()

    monkeypatch.setattr("tennis_match_prediction.web.upcoming.requests.get", get)
    model_id = client.get("/api/status").json()["default_model_id"]
    assert client.get("/api/upcoming").json()["batch"] is None
    original_history = web_files[0].read_bytes()
    original_model = web_files[2].read_bytes()
    # The running service must archive what it actually loaded, not later changed files.
    web_files[0].write_bytes(b"changed after startup")
    web_files[2].write_bytes(b"changed after startup")
    response = client.post("/api/upcoming/refresh", json={"model_id": model_id})
    assert response.status_code == 200, response.text
    batch = response.json()["batch"]
    assert len(batch["predictions"]) == 1
    assert len(batch["skipped"]) == 5
    assert batch["skip_counts"]["postponed_or_cancelled"] == 1
    assert batch["skip_counts"]["player_not_found_or_ambiguous"] == 1
    row = batch["predictions"][0]
    manual = client.post("/api/predict", json=row["request"]).json()
    assert manual["features"] == row["features"]
    assert manual["player1_win_probability"] == row["player1_win_probability"]
    saved = directory / batch["id"]
    assert (saved / "player_history.parquet").read_bytes() == original_history
    assert (saved / "model.pkl").read_bytes() == original_model
    artifact = load_artifact(saved / "model.pkl")
    assert probability_for(artifact, row["features"]) == row["player1_win_probability"]
    replay = PlayerService(saved / "player_history.parquet", saved, saved / "model.pkl")
    assert replay.default_model_id == model_id
    assert replay.model_version(model_id) == batch["model_version"]
    assert sha256(original_history).hexdigest() == batch["history_version"]
    assert sha256(original_model).hexdigest() == batch["model_version"]
    assert json.loads((saved / "source.json").read_text())["data"] == rows
    second = client.post("/api/upcoming/refresh", json={"model_id": model_id}).json()["batch"]
    assert second["id"] == batch["id"]
    assert len(calls) == 1
    download = client.get(f"/api/upcoming/batches/{batch['id']}")
    assert download.json()["id"] == batch["id"]
    assert "attachment" in download.headers["content-disposition"]
    assert "test-key" not in download.text

    # Subsequent reads hide a match after kickoff, preserving the archived prediction.
    class AfterKickoff(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2099, 1, 2, tzinfo=UTC)

    monkeypatch.setattr("tennis_match_prediction.web.upcoming.datetime", AfterKickoff)
    assert client.get("/api/upcoming").json()["batch"]["predictions"] == []
    assert len(client.get(f"/api/upcoming/batches/{batch['id']}").json()["predictions"]) == 1


def test_upcoming_provider_failure_and_pagination_are_not_silent(upcoming_client, monkeypatch):
    client, application, directory = upcoming_client
    model_id = client.get("/api/status").json()["default_model_id"]
    calls = []

    class Response:
        status_code = 200

        def json(self):
            return {"data": [_upcoming_match(len(calls))], "meta": {"has_more": True}}

    def get(*args, **kwargs):
        calls.append(kwargs["params"]["offset"])
        return Response()

    monkeypatch.setattr("tennis_match_prediction.web.upcoming.requests.get", get)
    result = client.post("/api/upcoming/refresh", json={"model_id": model_id})
    assert result.status_code == 503
    assert calls == [0, 100, 200, 300, 400]
    assert not list(directory.glob("*/batch.json"))
    Response.status_code = 429
    assert client.post("/api/upcoming/refresh", json={"model_id": model_id}).status_code == 503
    assert client.post("/api/upcoming/refresh", json={"model_id": "unknown"}).status_code == 422
    assert client.get("/api/upcoming/batches/invalid").status_code == 404


def test_upcoming_rejects_ambiguous_players_and_untrusted_states():
    from tennis_match_prediction.web.upcoming import UpcomingService

    names = {"alice example": ["one", "two"], "bob example": ["bob"]}
    now = datetime(2026, 1, 1, tzinfo=UTC)
    reason, _, _ = UpcomingService._eligible(_upcoming_match(), now, names)
    assert reason == "player_not_found_or_ambiguous"
    for field, value in [
        ("event_status", "Delayed"),
        ("reason", "cancelled"),
        ("event_status", "Interrupted"),
    ]:
        row = _upcoming_match()
        row[field] = value
        assert UpcomingService._eligible(row, now, names)[0] == "postponed_or_cancelled"


@pytest.fixture
def web_files(tmp_path):
    history = PlayerHistory(
        last_source_date=date(2026, 8, 30),
        players={
            "alice": PlayerState(
                name="Álice Example",
                rating=1700,
                rank=10,
                wins=20,
                losses=10,
                h2h_wins={"Bob Example": 3},
                history=[PlayedMatch(tourney_date=date(2026, 8, 30), surface="Hard", won=True)],
                surface_ratings={"Hard": 1600},
                aces=0,
                ace_serve_points=100,
            ),
            "bob": PlayerState(
                name="Bob Example", rating=1550, wins=10, losses=20, h2h_wins={"Álice Example": 2}
            ),
            "alice-two": PlayerState(name="Álice Example", rating=1400),
        },
    )
    history_path = tmp_path / "history.parquet"
    pd.DataFrame({"history": [history.model_dump_json()]}).to_parquet(history_path, index=False)
    models_dir = tmp_path / "models"
    model_path = models_dir / "test-model" / "model.pkl"
    config = ExperimentConfig(
        training_matches_path=tmp_path / "training.csv",
        output_dir=models_dir,
        train_start=date(2020, 1, 1),
        validation_start=date(2024, 1, 1),
        test_start=date(2025, 1, 1),
        feature_columns=("overall_elo_diff", "surface_elo_diff"),
    )
    model = LogisticRegressionModel(LogisticRegressionSettings())
    model.fit(
        pd.DataFrame(
            {"overall_elo_diff": [-100, 100, -200, 200], "surface_elo_diff": [-50, 50, -100, 100]}
        ),
        pd.Series([0, 1, 0, 1]),
    )
    metadata = ModelMetadata(
        schema_version=1,
        model_name=model.name,
        feature_columns=config.feature_columns,
        target_convention="winner=1 means player1 won",
        history_order="source_date_match_num",
        config=config,
        split={},
        dataset_sha256="a" * 64,
        provenance={},
        metrics={"validation_log_loss": 0.61},
        model_params=model.get_params(),
    )
    save_artifact(model_path, model, metadata)
    return history_path, models_dir, model_path


@pytest.fixture
def client(web_files):
    history_path, models_dir, model_path = web_files
    application = create_app(
        history_path=history_path, models_dir=models_dir, model_path=model_path
    )
    with TestClient(application) as test_client:
        yield test_client


def test_web_reloads_only_validated_published_history(client, web_files):
    history_path = web_files[0]
    previous = client.get("/api/status").json()
    frame = pd.read_parquet(history_path)
    history = json.loads(frame.loc[0, "history"])
    history["last_source_date"] = "2026-10-26"
    frame.loc[0, "history"] = json.dumps(history)
    snapshot = history_path.parent / ".refreshes" / "test-publication" / "history.parquet"
    snapshot.parent.mkdir(parents=True)
    frame.to_parquet(snapshot, index=False)
    publication = {
        "artifacts": {
            history_path.name: {
                "published_path": str(history_path.resolve()),
                "snapshot_path": str(snapshot.resolve()),
                "sha256": sha256(snapshot.read_bytes()).hexdigest(),
            }
        },
    }
    pointer = history_path.parent / "latest_refresh.json"
    pointer.write_text(json.dumps(publication), encoding="utf-8")
    # The compatibility file may still be old. Only the committed snapshot is read.
    updated = client.get("/api/status").json()
    assert updated["history_source_date"] == "2026-10-26"
    assert updated["history_version"] != previous["history_version"]
    assert updated["history_reload_error"] is None
    publication["artifacts"][history_path.name]["sha256"] = "incorrect"
    pointer.write_text(json.dumps(publication), encoding="utf-8")
    retained = client.get("/api/status").json()
    assert retained["history_version"] == updated["history_version"]
    assert retained["history_reload_error"] is not None
    publication["artifacts"][history_path.name]["sha256"] = sha256(
        snapshot.read_bytes()
    ).hexdigest()
    pointer.write_text(json.dumps(publication), encoding="utf-8")
    assert client.get("/api/status").json()["history_reload_error"] is None


def test_performance_page_and_report_download(web_files, tmp_path):
    from tennis_match_prediction.forecast_evaluation.service import update_performance

    application = create_app(
        history_path=web_files[0],
        models_dir=web_files[1],
        model_path=web_files[2],
        performance_dir=tmp_path / "performance",
    )
    with TestClient(application) as browser:
        assert browser.get("/api/performance").status_code == 404
        page = browser.get("/performance")
        assert page.status_code == 200
        assert page.text == browser.get("/").text
        assert '/static/style.css' in page.text
        assert 'data-view="performance"' in page.text
        assert 'id="performance-view"' in page.text
        update_performance(
            tmp_path / "predictions", tmp_path / "performance", api_key=None, max_requests=0
        )
        assert browser.get("/api/performance").json()["evaluated_count"] == 0
        page = browser.get("/performance")
        assert page.status_code == 200
        assert page.text == browser.get("/").text
        download = browser.get("/api/performance/download")
        assert download.status_code == 200
        assert "performance.md" in download.headers["content-disposition"]


@pytest.fixture
def registry_client(web_files, tmp_path, monkeypatch):
    from tennis_match_prediction.web import registry

    artifact = load_artifact(web_files[2])
    path = tmp_path / "remote" / "model.pkl"
    save_artifact(path, artifact.model, artifact.metadata.model_copy(update={"run_id": "run-123"}))
    client = MagicMock()
    version = SimpleNamespace(status="READY", run_id="run-123", version="7")
    client.get_model_version_by_alias.return_value = version
    client.get_model_version.return_value = version
    client.get_run.return_value.info.status = "FINISHED"
    client.list_artifacts.return_value = [
        SimpleNamespace(path="project_model/model.pkl", is_dir=False),
        SimpleNamespace(path="project_model/model.json", is_dir=False),
    ]

    def download(run_id, artifact_path, directory):
        destination = Path(directory) / artifact_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        copyfile(path.parent / Path(artifact_path).name, destination)
        return str(destination)

    client.download_artifacts.side_effect = download
    constructor = MagicMock(return_value=client)
    monkeypatch.setattr(registry, "MlflowClient", constructor)
    return client, constructor


@pytest.mark.parametrize("version", [None, "7"])
def test_registry_startup_downloads_once_and_serves_predictions(
    web_files, registry_client, version
):
    remote, constructor = registry_client
    application = create_app(
        history_path=web_files[0],
        models_dir=web_files[1],
        model_path=None,
        registry_model=RegistryModel("teninho", "http://192.168.9.138:5000", version=version),
    )
    with TestClient(application) as client:
        status = client.get("/api/status").json()
        assert status["registry_model"] == {
            "name": "teninho",
            "version": "7",
            "run_id": "run-123",
            "model_uri": "models:/teninho/7",
        }
        constructor.assert_called_once_with(
            tracking_uri="http://192.168.9.138:5000", registry_uri="http://192.168.9.138:5000"
        )
        if version is None:
            remote.get_model_version_by_alias.assert_called_once_with("teninho", "champion")
        else:
            remote.get_model_version.assert_called_once_with("teninho", "7")
            remote.get_model_version_by_alias.assert_not_called()
        path = next(iter(application.state.players.models.values()))[0]
        assert path.is_file()
        calls = remote.mock_calls.copy()
        assert client.post("/api/predict", json=matchup(client)).status_code == 200
        assert client.post("/api/predict", json=matchup(client)).status_code == 200
        assert remote.mock_calls == calls
    assert not path.exists()


@pytest.mark.parametrize(
    "failure,message",
    [
        ("unavailable", "Cannot download registry model"),
        ("not-ready", "READY"),
        ("missing-run", "originating run"),
        ("failed-run", "FINISHED"),
        ("missing-pickle", "exactly one"),
        ("missing-json", "matching JSON"),
        ("wrong-run", "run ID"),
        ("wrong-json", "Model changed since startup"),
    ],
)
def test_registry_startup_failures_do_not_fall_back_to_local_models(
    web_files, registry_client, failure, message
):
    from mlflow.exceptions import MlflowException

    remote, _ = registry_client
    if failure == "unavailable":
        remote.get_model_version_by_alias.side_effect = MlflowException("offline")
    elif failure == "not-ready":
        remote.get_model_version_by_alias.return_value.status = "PENDING_REGISTRATION"
    elif failure == "missing-run":
        remote.get_model_version_by_alias.return_value.run_id = None
    elif failure == "failed-run":
        remote.get_run.return_value.info.status = "FAILED"
    elif failure == "missing-pickle":
        remote.list_artifacts.return_value = []
    elif failure == "missing-json":
        remote.list_artifacts.return_value = remote.list_artifacts.return_value[:1]
    else:
        original_download = remote.download_artifacts.side_effect

        def invalid_download(run_id, artifact_path, directory):
            result = original_download(run_id, artifact_path, directory)
            if failure == "wrong-run" and artifact_path.endswith(".pkl"):
                copyfile(web_files[2], result)
            if failure == "wrong-json" and artifact_path.endswith(".json"):
                metadata = ModelMetadata.model_validate_json(Path(result).read_text("utf-8"))
                Path(result).write_text(
                    metadata.model_copy(update={"metrics": {}}).model_dump_json(), "utf-8"
                )
            return result

        remote.download_artifacts.side_effect = invalid_download
    application = create_app(
        history_path=web_files[0],
        models_dir=web_files[1],
        model_path=None,
        registry_model=RegistryModel("tennis", "http://registry:5000"),
    )
    with pytest.raises((ValueError, RuntimeError), match=message), TestClient(application):
        pass


@pytest.mark.parametrize(
    "options",
    [
        {"alias": "champion", "version": "1"},
        {"version": "0"},
        {"version": "latest"},
        {"alias": " "},
    ],
)
def test_invalid_registry_selectors(options):
    with pytest.raises(ValueError):
        RegistryModel("tennis", "http://registry:5000", **options)


def test_web_cli_registry_environment_and_overrides(monkeypatch):
    from tennis_match_prediction.web import __main__ as cli

    monkeypatch.setenv("TENNIS_WEB_REGISTRY_MODEL", "tennis")
    monkeypatch.setenv("TENNIS_WEB_REGISTRY_ALIAS", "champion")
    monkeypatch.setenv("TENNIS_WEB_REGISTRY_VERSION", "")
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://192.168.9.138:5000")
    create = MagicMock()
    monkeypatch.setattr(cli, "create_app", create)
    monkeypatch.setattr(cli.uvicorn, "run", MagicMock())
    monkeypatch.setattr("sys.argv", ["web"])
    cli.main()
    assert create.call_args.kwargs["registry_model"] == RegistryModel(
        "tennis", "http://192.168.9.138:5000", alias="champion"
    )
    monkeypatch.setattr("sys.argv", ["web", "--registry-version", "7"])
    cli.main()
    assert create.call_args.kwargs["registry_model"].version == "7"
    assert create.call_args.kwargs["registry_model"].alias is None
    monkeypatch.setattr("sys.argv", ["web", "--registry-model", "other-model"])
    cli.main()
    assert create.call_args.kwargs["registry_model"] == RegistryModel(
        "other-model", "http://192.168.9.138:5000", alias="champion"
    )
    monkeypatch.setattr("sys.argv", ["web", "--model-path", "local.pkl"])
    cli.main()
    assert create.call_args.kwargs["registry_model"] is None
    assert create.call_args.kwargs["model_path"] == Path("local.pkl")
    monkeypatch.delenv("TENNIS_WEB_REGISTRY_MODEL")
    monkeypatch.setattr("sys.argv", ["web"])
    cli.main()
    assert create.call_args.kwargs["registry_model"] is None


def matchup(client, **overrides):
    return {
        "player0_id": "alice",
        "player1_id": "bob",
        "match_date": "2026-10-14",
        "surface": "Hard",
        "model_id": client.get("/api/status").json()["default_model_id"],
    } | overrides


def test_status_static_assets_and_accent_insensitive_search(client, web_files):
    status = client.get("/api/status").json()
    assert status["history_version"] == sha256(web_files[0].read_bytes()).hexdigest()
    assert status["minimum_prediction_date"] == "2026-08-31"
    assert status["player_count"] == 3
    assert client.get("/").status_code == 200
    assert "Court Report" in client.get("/").text
    assert client.get("/static/app.js").status_code == 200
    result = client.get("/api/players", params={"q": "ALICE example", "limit": 1}).json()
    assert result["total"] == 2
    assert result["players"][0]["id"] == "alice"
    second = client.get("/api/players", params={"q": "alice", "offset": 1}).json()
    assert second["players"][0]["id"] == "alice-two"
    assert client.get("/api/players?q=no-such-player").json()["players"] == []
    assert client.get("/api/players?limit=101").status_code == 422


def test_pages_revalidate_html_and_version_assets_by_content(client):
    home = client.get("/")
    performance = client.get("/performance")
    assert home.text == performance.text
    for page in (home, performance):
        assert page.headers["cache-control"] == "no-cache"
        assert page.headers["content-type"].startswith("text/html")
    for asset in ("app.js", "style.css"):
        url = re.search(rf'/static/{re.escape(asset)}\?v=([0-9a-f]{{64}})', home.text)
        assert url is not None
        response = client.get(url.group(0))
        assert response.status_code == 200
        assert sha256(response.content).hexdigest() == url.group(1)


def test_profile_preserves_missing_statistics_and_observed_zero(client):
    alice = client.get("/api/players/alice").json()
    assert alice["serve"]["ace_rate"] == 0
    assert alice["serve"]["double_fault_rate"] is None
    assert alice["recent_win_rate"] == 1
    bob = client.get("/api/players/bob").json()
    assert bob["recent_win_rate"] is None
    assert bob["rank"] is None
    assert client.get("/api/players/missing").status_code == 404


def test_id_prediction_matches_shared_engine_and_exports_provenance(client, web_files):
    result = client.post("/api/predict", json=matchup(client)).json()
    artifact = load_artifact(web_files[2])
    assert result["player1_win_probability"] == probability_for(artifact, result["features"])
    assert result["player0_win_probability"] + result["player1_win_probability"] == 1
    assert result["player0_decimal_odds"] == 1 / result["player0_win_probability"]
    assert result["head_to_head"] == {"player0_wins": 3, "player1_wins": 2}
    assert result["model_version"] == sha256(web_files[2].read_bytes()).hexdigest()
    assert result["created_at"].endswith("+00:00")
    # Ambiguous names still work through stable IDs.
    assert result["player0"]["id"] == "alice"
    swapped = client.post(
        "/api/predict", json=matchup(client, player0_id="bob", player1_id="alice")
    ).json()
    np.testing.assert_allclose(
        result["player0_win_probability"], swapped["player1_win_probability"]
    )


@pytest.mark.parametrize(
    "overrides,status",
    [
        ({"player1_id": "alice"}, 422),
        ({"player1_id": "missing"}, 404),
        ({"surface": "Ice"}, 422),
        ({"match_date": "2026-08-30"}, 422),
        ({"match_date": "2026-08-29"}, 422),
        ({"match_date": "bad-date"}, 422),
        ({"model_id": "../../arbitrary.pkl"}, 422),
        ({"extra": True}, 422),
    ],
)
def test_invalid_matchup_is_rejected(client, overrides, status):
    assert client.post("/api/predict", json=matchup(client, **overrides)).status_code == status


def test_surface_normalization_and_future_date_semantics(client):
    first = client.post("/api/predict", json=matchup(client, surface=" clay ")).json()
    later = client.post(
        "/api/predict", json=matchup(client, surface="Clay", match_date="2027-01-01")
    ).json()
    assert first["request"]["surface"] == "Clay"
    assert first["features"] == later["features"]
    assert first["player1_win_probability"] == later["player1_win_probability"]


def test_cached_models_and_history_are_not_reloaded_per_prediction(client, monkeypatch):
    def unexpected_load(*args, **kwargs):
        raise AssertionError("Prediction reloaded a cached file")

    monkeypatch.setattr("tennis_match_prediction.web.service.load_artifact", unexpected_load)
    monkeypatch.setattr("tennis_match_prediction.web.service.pd.read_parquet", unexpected_load)
    for _ in range(2):
        assert client.post("/api/predict", json=matchup(client)).status_code == 200


@pytest.mark.parametrize("probability", [0.0, 1.0])
def test_zero_probability_odds_are_json_safe(client, monkeypatch, probability):
    monkeypatch.setattr(
        "tennis_match_prediction.web.service.probability_for", lambda *_: probability
    )
    response = client.post("/api/predict", json=matchup(client))
    assert response.status_code == 200
    result = response.json()
    assert result["player1_decimal_odds" if probability == 0 else "player0_decimal_odds"] is None


def test_feature_mismatch_is_explicit_before_scoring(web_files):
    artifact = load_artifact(web_files[2])
    with pytest.raises(ValueError, match="unavailable prediction features"):
        probability_for(artifact, {"overall_elo_diff": 10.0})


def test_incompatible_models_are_filtered_and_explicit_selection_fails(web_files):
    history_path, models_dir, model_path = web_files
    artifact = load_artifact(model_path)
    features = (*artifact.metadata.feature_columns, "player0_winrate_last_10")
    metadata = artifact.metadata.model_copy(
        update={
            "feature_columns": features,
            "config": artifact.metadata.config.model_copy(update={"feature_columns": features}),
        }
    )
    incompatible = models_dir / "incompatible" / "model.pkl"
    save_artifact(incompatible, artifact.model, metadata)
    service = PlayerService(history_path, models_dir, None)
    assert len(service.models) == 1
    with pytest.raises(ValueError, match="unavailable"):
        PlayerService(history_path, models_dir, incompatible)


def test_profiles_work_without_models(web_files, tmp_path):
    history_path, _, _ = web_files
    with TestClient(
        create_app(history_path=history_path, models_dir=tmp_path / "empty", model_path=None)
    ) as client:
        assert client.get("/api/status").json()["default_model_id"] is None
        assert client.get("/api/players/alice").status_code == 200


def test_name_based_cli_rejects_same_player(web_files):
    from tennis_match_prediction.contracts.prediction import FutureMatchRequest

    # Bob is unambiguous; both CLI player positions resolve to the same ID.
    request = FutureMatchRequest(
        player0_name="Bob Example",
        player1_name="Bob Example",
        match_date=date(2026, 10, 14),
        surface="Hard",
    )
    with pytest.raises(ValueError, match="different players"):
        predict(request, web_files[2], web_files[0])
