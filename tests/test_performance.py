"""Prospective scoring must preserve participant orientation, timing and forecast versions."""

import copy
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

from tennis_match_prediction.performance import evaluate_batches, update_performance


def batch(batch_id="a", created="2000-01-02T12:00:00Z", probability=0.8):
    return {
        "id": batch_id,
        "source": "Live Tennis API",
        "model_id": "model",
        "model_version": "b" * 64,
        "model_name": "candidate",
        "run_id": "run",
        "history_version": "c" * 64,
        "history_source_date": "2000-01-01",
        "source_fetched_at": "2000-01-02T11:59:00Z",
        "skipped": [],
        "predictions": [
            {
                "created_at": created,
                "player1_win_probability": probability,
                "player0_win_probability": 1 - probability,
                "match": {
                    "id": 100,
                    "provider_player0_id": 10,
                    "provider_player1_id": 20,
                    "scheduled_time": "2000-01-03T12:00:00Z",
                    "surface": "Hard",
                },
            }
        ],
    }


def observation():
    return {
        "observed_at": "2000-01-03T16:00:00Z",
        "sha256": "result-digest",
        "match": {
            "id": 100,
            "status": "completed",
            "outcome": "completed",
            "winner": 2,
            "result_version": 1,
            "scheduled_time": "2000-01-03T12:00:00Z",
            "live_at": "2000-01-03T12:03:00Z",
            "players": {"p1": {"id": 10}, "p2": {"id": 20}},
        },
    }


def save_batch(root, record):
    path = root / record["id"] / "batch.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(record), encoding="utf-8")


def test_metrics_and_swapped_result_orientation():
    original = batch()
    result = observation()
    result["match"]["players"] = {"p1": {"id": 20}, "p2": {"id": 10}}
    result["match"]["winner"] = 1
    report = evaluate_batches([original], {"100": result})
    model = report["models"][0]
    assert model["count"] == 1
    assert model["accuracy"] == 1
    assert model["brier"] == pytest.approx(0.04)
    assert model["log_loss"] == pytest.approx(0.22314355)
    assert model["by_surface"]["Hard"]["count"] == 1
    assert model["calibration"][0]["observed_frequency"] == 1
    assert report["decisions"][0]["result_version"] == 1
    assert original == batch()


def test_select_latest_eligible_once_per_exact_model_version():
    first = batch("a")
    later = batch("b", "2000-01-02T18:00:00Z", 0.6)
    in_play = batch("c", "2000-01-03T12:00:00Z", 0.99)
    other_model = copy.deepcopy(later)
    other_model.update(id="d", model_version="d" * 64)
    report = evaluate_batches([first, later, in_play, other_model], {"100": observation()})
    assert report["evaluated_count"] == 2
    assert report["evaluated_match_count"] == 1
    assert report["reason_counts"] == {"superseded_forecast": 1, "not_pre_match": 1}
    assert report["models"][0]["brier"] == pytest.approx(0.16)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("status", "cancelled", "cancelled"),
        ("status", "live", "not_completed"),
        ("outcome", "retired", "excluded_outcome"),
        ("outcome", "walkover", "excluded_outcome"),
        ("outcome", "default", "excluded_outcome"),
        ("outcome", "abandoned", "excluded_outcome"),
        ("outcome", "unresolved", "excluded_outcome"),
        ("outcome", None, "excluded_outcome"),
        ("outcome", "new_provider_state", "excluded_outcome"),
        ("winner", None, "unknown_winner"),
        ("winner", True, "unknown_winner"),
        ("live_at", None, "unproven_timing"),
        ("live_at", "2000-01-03T12:03:00", "unproven_timing"),
        ("live_at", "2000-01-02T10:00:00Z", "not_pre_match"),
        ("id", 999, "match_id_conflict"),
        ("players", {"p1": {"id": 10}, "p2": {"id": 999}}, "participant_conflict"),
    ],
)
def test_exclude_unverified_results(field, value, reason):
    result = observation()
    result["match"][field] = value
    report = evaluate_batches([batch()], {"100": result})
    assert report["evaluated_count"] == 0
    assert report["reason_counts"] == {reason: 1}
    assert report["models"][0]["accuracy"] is None


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"history_source_date": "2000-01-03"}, "incompatible_history_date"),
        ({"source_fetched_at": "2000-01-02T13:00:00Z"}, "not_pre_match"),
    ],
)
def test_exclude_incompatible_history_and_collection(change, reason):
    record = batch()
    record.update(change)
    assert evaluate_batches([record], {"100": observation()})["reason_counts"] == {reason: 1}


def test_missing_results_and_coverage_are_explicit():
    record = batch()
    record["skipped"] = [{"match_id": 200, "reason": "player_not_found"}]
    report = evaluate_batches([record], {})
    assert report["prediction_coverage"] == 0.5
    assert report["models"][0]["evaluated_fraction"] == 0
    assert report["reason_counts"] == {"result_not_collected": 1}


def test_result_revisions_offline_replay_and_failure_preserve_publication(tmp_path, monkeypatch):
    forecasts = tmp_path / "predictions"
    output = tmp_path / "performance"
    original = batch()
    save_batch(forecasts, original)
    response = Mock(status_code=200)
    response.json.return_value = observation()["match"]
    get = Mock(return_value=response)
    monkeypatch.setattr("tennis_match_prediction.performance.requests.get", get)
    first = update_performance(forecasts, output, api_key="secret")
    assert first["models"][0]["accuracy"] == 1
    response.json.return_value["winner"] = 1
    response.json.return_value["result_version"] = 2
    corrected = update_performance(forecasts, output, api_key="secret")
    assert corrected["models"][0]["accuracy"] == 0
    previous_report = Path(first["report_path"]).with_suffix(".json")
    assert json.loads(previous_report.read_text())["models"][0]["accuracy"] == 1
    offline = update_performance(forecasts, output, api_key=None, max_requests=0)
    assert offline["decisions"] == corrected["decisions"]
    assert get.call_count == 2
    pointer = (output / "latest.json").read_bytes()
    get.side_effect = requests.ConnectionError("secret must never be printed")
    with pytest.raises(RuntimeError, match="previous report preserved"):
        update_performance(forecasts, output, api_key="secret")
    assert (output / "latest.json").read_bytes() == pointer
    assert json.loads((forecasts / "a" / "batch.json").read_text()) == original
    assert not (output / ".performance.lock").exists()
    assert "secret" not in Path(offline["report_path"]).read_text()


@pytest.mark.parametrize("status", [403, 429, 500])
def test_provider_errors_never_publish_a_report(tmp_path, monkeypatch, status):
    save_batch(tmp_path / "predictions", batch())
    monkeypatch.setattr(
        "tennis_match_prediction.performance.requests.get",
        Mock(return_value=Mock(status_code=status)),
    )
    with pytest.raises(RuntimeError, match=f"HTTP {status}"):
        update_performance(tmp_path / "predictions", tmp_path / "report", api_key="secret")
    assert not (tmp_path / "report" / "latest.json").exists()


def test_request_budget_rotates_oldest_observation(tmp_path, monkeypatch):
    record = batch()
    record["predictions"].append(copy.deepcopy(record["predictions"][0]))
    record["predictions"][1]["match"]["id"] = 101
    save_batch(tmp_path / "predictions", record)
    calls = []

    def get(url, **kwargs):
        match_id = int(url.rsplit("/", 1)[1])
        calls.append(match_id)
        result = observation()["match"]
        result["id"] = match_id
        return Mock(status_code=200, json=lambda: result)

    monkeypatch.setattr("tennis_match_prediction.performance.requests.get", get)
    output = tmp_path / "reports"
    update_performance(tmp_path / "predictions", output, api_key="key", max_requests=1)
    second = update_performance(tmp_path / "predictions", output, api_key="key", max_requests=1)
    assert calls == [100, 101]
    assert second["evaluated_count"] == 2
    assert second["not_refreshed_count"] == 1
