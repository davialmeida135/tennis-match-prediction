"""Daily orchestration and partial-source reconciliation preserve published state."""

import json
from unittest.mock import Mock

import pandas as pd
import pytest

from tennis_match_prediction.pipelines.defs import defs
from tennis_match_prediction.pipelines.historical_matches.config import RawMatchesCsv
from tennis_match_prediction.workflows.daily_update import DailyUpdateSettings, run_daily_update


def results(number, winner="001", loser="002"):
    return pd.DataFrame(
        [
            {
                "tourney_id": "2026-1",
                "tourney_date": "20260101",
                "match_num": str(number) if number is not None else None,
                "winner_id": winner,
                "loser_id": loser,
                "winner_name": "Alice",
                "loser_name": "Bob",
                "surface": "Hard",
                "score": "6-4 6-4",
            }
        ]
    )


def test_partial_feed_retains_absent_rows_and_annual_corrections_take_precedence(
    tmp_path, monkeypatch
):
    annual = results(1)
    ongoing = results(2)
    catalog = {
        "files": [
            {"name": "2026.csv", "url": "https://example.test/annual"},
            {"name": "ongoing_tourneys.csv", "url": "https://example.test/ongoing"},
        ]
    }

    def get(url, **kwargs):
        if url.endswith("catalog"):
            return Mock(json=lambda: catalog)
        frame = annual if url.endswith("annual") else ongoing
        return Mock(content=frame.to_csv(index=False).encode())

    monkeypatch.setattr(
        "tennis_match_prediction.pipelines.historical_matches.config.requests.get", get
    )
    source = RawMatchesCsv(
        csv_path=str(tmp_path / "all.csv"),
        catalog_url="https://example.test/catalog",
        years=[2026],
        include_ongoing=True,
    )
    combined, _ = source.refresh_history(tmp_path, {2026})
    assert len(combined) == 2
    assert combined["winner_id"].tolist() == ["001", "001"]
    ongoing = results(3)
    combined, _ = source.refresh_history(tmp_path, {2026})
    assert len(combined) == 3  # match 2 is absent from the partial feed, not deleted
    annual = pd.concat([results(1), results(2, "002", "001")], ignore_index=True)
    combined, _ = source.refresh_history(tmp_path, {2026})
    assert len(combined) == 3
    assert combined.loc[combined["match_num"] == 2, "winner_id"].item() == "002"
    assert len(list((tmp_path / "ongoing_snapshots").glob("*.csv"))) == 2
    previous = (tmp_path / "all.csv").read_bytes()
    ongoing = results(4).drop(columns="winner_id")
    with pytest.raises(ValueError, match="missing columns"):
        source.refresh_history(tmp_path, {2026})
    assert (tmp_path / "all.csv").read_bytes() == previous


def test_reconciliation_preserves_legacy_rows_without_identity(tmp_path):
    annual = pd.concat([results(1), results(None), results(None, "003", "004")])
    annual.to_csv(tmp_path / "2026.csv", index=False)
    results(2).to_csv(tmp_path / "ongoing_results.csv", index=False)
    source = RawMatchesCsv(csv_path=str(tmp_path / "all.csv"), include_ongoing=True)
    combined = source.consolidate_seasons(tmp_path)
    assert len(combined) == 4
    assert combined["match_num"].isna().sum() == 2


def test_ambiguous_or_conflicting_partial_identity_is_rejected(tmp_path):
    results(1).to_csv(tmp_path / "2026.csv", index=False)
    results(1, "001", "003").to_csv(tmp_path / "ongoing_results.csv", index=False)
    source = RawMatchesCsv(csv_path=str(tmp_path / "all.csv"), include_ongoing=True)
    with pytest.raises(ValueError, match="participant conflict"):
        source.consolidate_seasons(tmp_path)
    pd.concat([results(2), results(2, "002", "001")]).to_csv(
        tmp_path / "ongoing_results.csv", index=False
    )
    with pytest.raises(ValueError, match="Ambiguous result identity"):
        source.consolidate_seasons(tmp_path)


@pytest.mark.parametrize(
    "failed_stages", [("history",), ("performance",), ("history", "performance")]
)
def test_daily_job_reports_stage_failures_and_runs_both_stages(
    tmp_path, monkeypatch, failed_stages
):
    refresh = Mock(
        return_value={"status": "success", "report_path": "history.json", "raw_sha256": "abc"},
        side_effect=RuntimeError("invalid source") if "history" in failed_stages else None,
    )
    evaluate = Mock(
        return_value={"report_path": "report.md", "evaluated_count": 3},
        side_effect=OSError("permission denied") if "performance" in failed_stages else None,
    )
    monkeypatch.setattr(
        "tennis_match_prediction.workflows.daily_update.refresh_history",
        refresh,
    )
    monkeypatch.setattr(
        "tennis_match_prediction.workflows.daily_update.update_performance", evaluate
    )
    settings = DailyUpdateSettings(reports_dir=tmp_path, max_requests=0)
    with pytest.raises(RuntimeError, match="Daily update had failures") as failure:
        run_daily_update(settings)
    assert refresh.call_count == 1
    assert evaluate.call_count == 1
    report = json.loads((tmp_path / "latest.json").read_text())
    assert report["status"] == "failed"
    for stage in failed_stages:
        result = report["stages"][stage]
        assert result["status"] == "failed"
        assert f"{stage}: {result['error']}" in str(failure.value)
    assert str((tmp_path / f"{report['id']}.json").resolve()) in str(failure.value)
    if "performance" not in failed_stages:
        assert report["stages"]["performance"]["evaluated_count"] == 3
    assert not (tmp_path / ".maintenance.lock").exists()


def test_daily_schedule_is_registered_without_training():
    schedule = next(s for s in defs.schedules if s.name == "daily_history_and_performance")
    assert schedule.cron_schedule == "0 6 * * *"
    assert schedule.execution_timezone == "America/Sao_Paulo"
    assert schedule.job_name == "daily_update"
    assert "trained_model" not in schedule.job.graph.node_dict


def test_dagster_daily_job_accepts_isolated_paths(tmp_path, monkeypatch):
    run = Mock(return_value={"status": "success"})
    monkeypatch.setattr("tennis_match_prediction.workflows.daily_update.run_daily_update", run)
    # Use the merged production definitions: their default IO manager only accepts
    # DataFrames, so this operational op must not produce a persisted dict output.
    job = defs.get_job_def("daily_update")
    result = job.execute_in_process(
        run_config={
            "ops": {
                "update_history_and_performance": {
                    "config": {
                        "csv_path": str(tmp_path / "raw.csv"),
                        "max_requests": 0,
                        "local_only": True,
                    },
                }
            }
        }
    )
    assert result.success
    settings = run.call_args.args[0]
    assert settings.csv_path == tmp_path / "raw.csv"
    assert settings.local_only is True
    assert settings.max_requests == 0
