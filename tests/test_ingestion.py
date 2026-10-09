"""Source acquisition belongs to the raw asset, including cache and refresh behavior."""

import json
from pathlib import Path

import pandas as pd
import pytest
from dagster import materialize

from tennis_match_prediction.pipelines.historical_matches.assets import raw_atp_matches
from tennis_match_prediction.pipelines.historical_matches.config import RawMatchesCsv
from tennis_match_prediction.pipelines.shared.parquet_io import ParquetDataFrameIOManager


def _csv(year):
    return (
        "tourney_id,tourney_date,match_num,winner_id,loser_id,winner_name,loser_name,surface\n"
        f"{year}-1,{year}0101,1,001,002,Alice,Bob,Hard\n"
    ).encode()


def _materialize(tmp_path, source):
    return materialize(
        [raw_atp_matches],
        resources={
            "raw_matches_csv": source,
            "io_manager": ParquetDataFrameIOManager(base_dir=str(tmp_path / "staging")),
        },
    )


def _mock_download(monkeypatch, years, calls):
    class Response:
        def __init__(self, content=b"", catalog=None):
            self.content = content
            self.catalog = catalog

        def raise_for_status(self):
            pass

        def json(self):
            return self.catalog

    def get(url, timeout):
        assert timeout > 0
        calls.append(url)
        if url == "https://example.test/catalog":
            return Response(
                catalog={
                    "files": [
                        {"name": f"{year}.csv", "url": f"https://example.test/{year}.csv"}
                        for year in years
                    ]
                }
            )
        return Response(_csv(int(url.rsplit("/", 1)[1][:4])))

    monkeypatch.setattr(
        "tennis_match_prediction.pipelines.historical_matches.config.requests.get", get
    )


@pytest.mark.parametrize("cached", [True, False])
def test_local_materialization_never_downloads(tmp_path, monkeypatch, cached):
    output = tmp_path / "all_atp_matches.csv"
    (output if cached else tmp_path / "2024.csv").write_bytes(_csv(2024))

    def unexpected_download(*args, **kwargs):
        pytest.fail("Local materialization must not contact the source")

    monkeypatch.setattr(
        "tennis_match_prediction.pipelines.historical_matches.config.requests.get",
        unexpected_download,
    )
    assert _materialize(tmp_path, RawMatchesCsv(csv_path=str(output), refresh=False)).success
    assert output.exists()
    frame = pd.read_parquet(tmp_path / "staging" / "raw_atp_matches.parquet")
    assert frame["winner_id"].tolist() == ["001"]


def test_empty_workspace_bootstraps_from_source(tmp_path, monkeypatch):
    calls = []
    _mock_download(monkeypatch, [2024, 2025], calls)
    source = RawMatchesCsv(
        csv_path=str(tmp_path / "all_atp_matches.csv"), catalog_url="https://example.test/catalog"
    )
    assert _materialize(tmp_path, source).success
    assert len(calls) == 3
    assert len(pd.read_csv(source.csv_path)) == 2


def test_selected_refresh_keeps_local_seasons_and_manifest(tmp_path, monkeypatch):
    (tmp_path / "2024.csv").write_bytes(_csv(2024))
    (tmp_path / "manifest.json").write_text(
        json.dumps({"seasons": [{"year": 2024, "path": "2024.csv", "sha256": "previous"}]}),
        encoding="utf-8",
    )
    calls = []
    _mock_download(monkeypatch, [2024, 2025], calls)
    source = RawMatchesCsv(
        csv_path=str(tmp_path / "all_atp_matches.csv"),
        catalog_url="https://example.test/catalog",
        refresh=True,
        years=[2025],
    )
    assert _materialize(tmp_path, source).success
    assert calls == ["https://example.test/catalog", "https://example.test/2025.csv"]
    assert len(pd.read_csv(source.csv_path)) == 2
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert [item["year"] for item in manifest["seasons"]] == [2024, 2025]


def test_first_year_refresh_includes_all_newer_available_seasons(tmp_path, monkeypatch):
    (tmp_path / "2023.csv").write_bytes(_csv(2023))
    calls = []
    _mock_download(monkeypatch, [2023, 2024, 2026, 2027], calls)
    source = RawMatchesCsv(
        csv_path=str(tmp_path / "all_atp_matches.csv"),
        catalog_url="https://example.test/catalog",
        first_year=2024,
    )
    assert _materialize(tmp_path, source).success
    assert calls == [
        "https://example.test/catalog",
        "https://example.test/2024.csv",
        "https://example.test/2026.csv",
        "https://example.test/2027.csv",
    ]
    assert pd.read_csv(source.csv_path)["tourney_date"].tolist() == [
        20240101,
        20260101,
        20270101,
    ]
    assert (tmp_path / "2023.csv").exists()


@pytest.mark.parametrize("cached", [True, False])
def test_first_year_filters_cached_history_without_download(tmp_path, monkeypatch, cached):
    output = tmp_path / "all_atp_matches.csv"
    for year in [2023, 2024, 2025]:
        (tmp_path / f"{year}.csv").write_bytes(_csv(year))
    if cached:
        RawMatchesCsv(csv_path=str(output)).consolidate_seasons(tmp_path)

    def unexpected_download(*args, **kwargs):
        pytest.fail("Local materialization must not contact the source")

    monkeypatch.setattr(
        "tennis_match_prediction.pipelines.historical_matches.config.requests.get",
        unexpected_download,
    )
    source = RawMatchesCsv(csv_path=str(output), refresh=False, first_year=2024)
    assert _materialize(tmp_path, source).success
    frame = pd.read_parquet(tmp_path / "staging" / "raw_atp_matches.parquet")
    assert frame["tourney_date"].tolist() == [20240101, 20250101]


def test_first_year_with_no_available_seasons_fails(tmp_path, monkeypatch):
    _mock_download(monkeypatch, [2023], [])
    source = RawMatchesCsv(catalog_url="https://example.test/catalog", first_year=2024)
    with pytest.raises(ValueError, match="did not contain requested ATP annual files"):
        source.download_seasons(tmp_path)


def test_first_year_and_explicit_years_are_mutually_exclusive():
    with pytest.raises(ValueError, match="either first_year or years"):
        RawMatchesCsv(first_year=2024, years=[2025])


def test_invalid_local_season_does_not_replace_consolidated_csv(tmp_path):
    output = tmp_path / "all_atp_matches.csv"
    output.write_bytes(_csv(2024))
    (tmp_path / "2025.csv").write_text("winner_id\n001\n", encoding="utf-8")
    source = RawMatchesCsv(csv_path=str(output))
    with pytest.raises(ValueError, match="2025.csv is incompatible"):
        source.consolidate_seasons(tmp_path)
    assert output.read_bytes() == _csv(2024)


@pytest.mark.parametrize("failures,winerror", [(2, 32), (5, 32), (1, 5)])
def test_consolidation_handles_file_locks(tmp_path, monkeypatch, failures, winerror):
    output = tmp_path / "all_atp_matches.csv"
    previous = _csv(2023)
    output.write_bytes(previous)
    (tmp_path / "2024.csv").write_bytes(_csv(2024))
    # Another execution's temporary file must remain untouched.
    shared_temporary = output.with_suffix(".tmp")
    shared_temporary.write_bytes(b"other run")
    original_replace = Path.replace
    attempts = []
    delays = []

    def locked_replace(path, target):
        attempts.append(path)
        if len(attempts) <= failures:
            error = PermissionError("file locked")
            error.winerror = winerror
            raise error
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", locked_replace)
    monkeypatch.setattr(
        "tennis_match_prediction.pipelines.historical_matches.config.time.sleep", delays.append
    )
    source = RawMatchesCsv(csv_path=str(output))
    if failures == 2:
        source.consolidate_seasons(tmp_path)
        assert pd.read_csv(output)["tourney_date"].tolist() == [20240101]
        assert len(attempts) == 3
        assert delays == [0.25, 0.5]
    else:
        with pytest.raises(PermissionError) as caught:
            source.consolidate_seasons(tmp_path)
        assert output.read_bytes() == previous
        if winerror == 32:
            assert "Close applications" in str(caught.value)
            assert len(attempts) == 5
        else:
            assert caught.value.winerror == 5
            assert len(attempts) == 1
            assert delays == []
    assert shared_temporary.read_bytes() == b"other run"
    assert not list(tmp_path.glob(".all_atp_matches.csv.*.tmp"))
