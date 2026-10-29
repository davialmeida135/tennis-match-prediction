"""Evidence-driven checks for the isolated Live Tennis API experiment."""

import json
from datetime import UTC, datetime

import pytest

from tennis_match_prediction.diagnostics.live_tennis_probe import analyze_samples, run_probe


def _samples():
    fixture = {
        "id": 10,
        "match_id": 901,
        "player1_id": 101,
        "player2_id": 102,
        "status": "scheduled",
    }
    match = {
        "id": 901,
        "status": "upcoming",
        "surface": "hard",
        "tournament": "Example",
        "scheduled_time": "2099-01-01T14:00:00Z",
        "players": {"p1": {"id": 102, "name": "Bob"}, "p2": {"id": 101, "name": "Alice"}},
    }
    return (
        {"data": [fixture], "meta": {"has_more": False}},
        {"data": [match], "meta": {"has_more": False}},
    )


def test_fixture_id_and_player_order_are_not_treated_as_match_identity():
    fixtures, upcoming = _samples()
    result = analyze_samples(fixtures, upcoming, observed_at=datetime(2026, 1, 1, tzinfo=UTC))
    assert result["fixtures"]["fixture_ids_different_from_match_ids"] == 1
    assert result["fixtures"]["swapped_player_order_fixture_ids"] == [10]
    assert result["fixtures"]["conflicting_participants_fixture_ids"] == []
    assert result["upcoming"]["rows_passing_basic_checks"] == 1
    assert result["prediction_ready"] is False


@pytest.mark.parametrize(
    ("start", "reason"),
    [
        (None, "missing_start_time"),
        ("bad-time", "invalid_start_time"),
        ("2099-01-01T14:00:00", "start_time_without_timezone"),
        ("2025-01-01T14:00:00Z", "start_time_not_future"),
    ],
)
def test_unknown_or_past_start_never_passes_basic_checks(start, reason):
    fixtures, upcoming = _samples()
    upcoming["data"][0]["scheduled_time"] = start
    result = analyze_samples(fixtures, upcoming, observed_at=datetime(2026, 1, 1, tzinfo=UTC))
    assert result["upcoming"]["rows_passing_basic_checks"] == 0
    assert reason in result["upcoming"]["issues"][0]["reasons"]


def test_unverified_names_and_partial_pages_are_explicit():
    fixtures, upcoming = _samples()
    fixtures["meta"]["has_more"] = True
    upcoming["data"][0]["players"]["p1"]["id"] = 999
    players = {"a": {"name": "Alice"}, "b": {"name": "ALICE"}}
    result = analyze_samples(
        fixtures, upcoming, observed_at=datetime(2026, 1, 1, tzinfo=UTC), history_players=players
    )
    assert result["fixtures"]["complete_page_set"] is False
    assert result["fixtures"]["conflicting_participants_fixture_ids"] == [10]
    candidates = result["history_name_candidates"]
    assert candidates[0]["history_candidate_ids"] == []
    assert candidates[1]["history_candidate_ids"] == ["a", "b"]
    assert all(row["identity_verified"] is False for row in candidates)


@pytest.mark.parametrize("payload", [{}, {"data": "bad"}, {"data": [{"id": 1}, {"id": 1}]}])
def test_malformed_or_duplicate_rows_are_rejected(payload):
    _, upcoming = _samples()
    with pytest.raises(ValueError):
        analyze_samples(payload, upcoming, observed_at=datetime.now(UTC))


def test_probe_uses_bounded_requests_and_keeps_credentials_out_of_artifacts(tmp_path, monkeypatch):
    fixtures, upcoming = _samples()
    payloads = [{"status": "ok"}, {"tier": "free", "limits": {"per_day": 100}}, fixtures, upcoming]
    calls = []

    class Response:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload
            self.content = json.dumps(payload).encode()

        def json(self):
            return self.payload

    def get(url, **kwargs):
        assert kwargs["headers"] == {"X-API-Key": "test-secret"}
        assert kwargs["allow_redirects"] is False
        assert kwargs["timeout"] == 30
        calls.append((url, kwargs["params"]))
        return Response(payloads[len(calls) - 1])

    monkeypatch.setattr("tennis_match_prediction.diagnostics.live_tennis_probe.requests.get", get)
    report = run_probe("test-secret", output_dir=tmp_path)
    assert report["status"] == "success"
    assert len(calls) == 4
    assert calls[-1][1]["status"] == "upcoming"
    assert calls[-1][1]["is_qualifying"] == "false"
    for path in tmp_path.rglob("*.json"):
        assert "test-secret" not in path.read_text()


def test_authentication_failure_is_not_retried_or_leaked(tmp_path, monkeypatch):
    calls = []

    class Response:
        status_code = 401

    def get(*args, **kwargs):
        calls.append(1)
        return Response()

    monkeypatch.setattr("tennis_match_prediction.diagnostics.live_tennis_probe.requests.get", get)
    with pytest.raises(RuntimeError, match="Probe failed"):
        run_probe("test-secret", output_dir=tmp_path)
    assert len(calls) == 1
    (report_path,) = tmp_path.glob("*/report.json")
    assert "test-secret" not in report_path.read_text()
    assert json.loads(report_path.read_text())["error"] == "/health returned HTTP 401"
