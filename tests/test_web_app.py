"""HTTP behavior, ID resolution, cached inference and model compatibility."""

from datetime import date
from hashlib import sha256

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from tennis_match_prediction.contracts import (
    ExperimentConfig,
    LogisticRegressionSettings,
    ModelMetadata,
    PlayedMatch,
    PlayerHistory,
    PlayerState,
)
from tennis_match_prediction.ml.artifacts import load_artifact, save_artifact
from tennis_match_prediction.ml.models.logistic_regression import LogisticRegressionModel
from tennis_match_prediction.ml.predict import predict, probability_for
from tennis_match_prediction.web.app import create_app
from tennis_match_prediction.web.service import PlayerService


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
    from tennis_match_prediction.contracts import FutureMatchRequest

    # Bob is unambiguous; both CLI player positions resolve to the same ID.
    request = FutureMatchRequest(
        player0_name="Bob Example",
        player1_name="Bob Example",
        match_date=date(2026, 10, 14),
        surface="Hard",
    )
    with pytest.raises(ValueError, match="different players"):
        predict(request, web_files[2], web_files[0])
