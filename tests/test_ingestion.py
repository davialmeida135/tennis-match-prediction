"""Source acquisition belongs to the raw asset, including cache and refresh behavior."""

import json

import pandas as pd
import pytest
from dagster import materialize

from pipelines.historical_matches.assets import raw_atp_matches
from pipelines.historical_matches.config import RawMatchesCsv
from pipelines.shared.parquet_io import ParquetDataFrameIOManager


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

    monkeypatch.setattr("pipelines.historical_matches.config.requests.get", get)


@pytest.mark.parametrize("cached", [True, False])
def test_local_materialization_never_downloads(tmp_path, monkeypatch, cached):
    output = tmp_path / "all_atp_matches.csv"
    (output if cached else tmp_path / "2024.csv").write_bytes(_csv(2024))

    def unexpected_download(*args, **kwargs):
        pytest.fail("Local materialization must not contact the source")

    monkeypatch.setattr("pipelines.historical_matches.config.requests.get", unexpected_download)
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


def test_invalid_local_season_does_not_replace_consolidated_csv(tmp_path):
    output = tmp_path / "all_atp_matches.csv"
    output.write_bytes(_csv(2024))
    (tmp_path / "2025.csv").write_text("winner_id\n001\n", encoding="utf-8")
    source = RawMatchesCsv(csv_path=str(output))
    with pytest.raises(ValueError, match="2025.csv is incompatible"):
        source.consolidate_seasons(tmp_path)
    assert output.read_bytes() == _csv(2024)
