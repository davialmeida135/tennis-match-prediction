"""Source configuration and ingestion resource for historical ATP matches."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd
import requests
from dagster import ConfigurableResource
from pydantic import Field

from pipelines.shared.contracts import DownloadedSeason
from pipelines.shared.paths import DEFAULT_HISTORICAL_MATCHES_CSV

DATA_FILES_URL = "https://stats.tennismylife.org/api/data-files"
REQUIRED_COLUMNS = frozenset(
    {
        "tourney_id",
        "tourney_date",
        "match_num",
        "winner_id",
        "loser_id",
        "winner_name",
        "loser_name",
        "surface",
    }
)


@contextmanager
def _atomic_output(output: Path) -> Iterator[Path]:
    """Publish through a unique, closed temporary file, tolerating brief Windows locks."""
    with NamedTemporaryFile(
        dir=output.parent, prefix=f".{output.name}.", suffix=".tmp", delete=False
    ) as handle:
        temporary = Path(handle.name)
    try:
        yield temporary
        for attempt in range(5):
            try:
                temporary.replace(output)
                break
            except PermissionError as error:
                if getattr(error, "winerror", None) not in {32, 33}:
                    raise
                if attempt == 4:
                    raise PermissionError(
                        f"Cannot replace {output}: Windows reports that a file is in use. "
                        "Close applications holding the CSV open and retry materialization."
                    ) from error
                time.sleep(0.25 * 2**attempt)
    finally:
        temporary.unlink(missing_ok=True)


class RawMatchesCsv(ConfigurableResource):
    """Read cached history or refresh annual seasons as part of materialization."""

    csv_path: str = Field(
        default=os.getenv("TENNIS_RAW_MATCHES_CSV") or str(DEFAULT_HISTORICAL_MATCHES_CSV)
    )
    refresh: bool = True
    years: list[int] | None = None
    catalog_url: str = DATA_FILES_URL
    timeout: int = Field(default=60, gt=0)

    def download_seasons(
        self, destination: Path, years: set[int] | None = None
    ) -> list[DownloadedSeason]:
        """Download requested ATP seasons and return their content-addressed metadata."""
        destination.mkdir(parents=True, exist_ok=True)
        response = requests.get(self.catalog_url, timeout=self.timeout)
        response.raise_for_status()
        payload: dict[str, Any] = response.json()
        files = payload.get("files", [])
        selected = [file for file in files if self._is_annual_atp_file(file, years)]
        if not selected:
            raise ValueError("TennisMyLife catalog did not contain requested ATP annual files")
        available = {int(Path(str(file["name"])).stem) for file in selected}
        if years is not None and years.difference(available):
            raise ValueError(f"Requested seasons are unavailable: {sorted(years - available)}")

        downloaded = [self._download_file(file, destination) for file in selected]
        self._write_manifest(destination, downloaded)
        return sorted(downloaded, key=lambda item: item.year)

    @staticmethod
    def _is_annual_atp_file(file: dict[str, Any], years: set[int] | None) -> bool:
        name = str(file.get("name", ""))
        stem = Path(name).stem
        if not stem.isdigit() or len(stem) != 4 or not name.endswith(".csv"):
            return False
        year = int(stem)
        return years is None or year in years

    def _download_file(self, file: dict[str, Any], destination: Path) -> DownloadedSeason:
        year = int(Path(str(file["name"])).stem)
        response = requests.get(str(file["url"]), timeout=self.timeout)
        response.raise_for_status()
        content = response.content
        digest = hashlib.sha256(content).hexdigest()
        path = destination / f"{year}.csv"
        changed = not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest() != digest
        frame = pd.read_csv(BytesIO(content), nrows=1)
        missing = REQUIRED_COLUMNS.difference(frame.columns)
        if missing:
            raise ValueError(f"{path.name} is incompatible; missing columns: {sorted(missing)}")
        if changed:
            with _atomic_output(path) as temporary:
                temporary.write_bytes(content)
        return DownloadedSeason(year=year, path=path, sha256=digest, changed=changed)

    def _write_manifest(self, destination: Path, files: list[DownloadedSeason]) -> None:
        manifest_path = destination / "manifest.json"
        seasons = {}
        if manifest_path.exists():
            seasons = {
                item["year"]: item
                for item in json.loads(manifest_path.read_text(encoding="utf-8"))["seasons"]
            }
        seasons.update(
            {
                item.year: {"year": item.year, "path": item.path.name, "sha256": item.sha256}
                for item in files
            }
        )
        manifest = {
            "source": self.catalog_url,
            "seasons": [seasons[year] for year in sorted(seasons)],
        }
        (destination / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
        )

    def load_seasons(self, raw_directory: Path) -> pd.DataFrame:
        """Load, validate, and deduplicate local annual ATP files into one chronological frame."""
        paths = sorted(path for path in raw_directory.glob("[0-9][0-9][0-9][0-9].csv"))
        if not paths:
            raise FileNotFoundError(f"No annual ATP files found in {raw_directory}")
        frames = []
        for path in paths:
            season = pd.read_csv(
                path,
                dtype={
                    column: "string"
                    for column in ("winner_id", "loser_id", "winner_seed", "loser_seed")
                },
            )
            missing = REQUIRED_COLUMNS.difference(season.columns)
            if missing:
                raise ValueError(f"{path.name} is incompatible; missing columns: {sorted(missing)}")
            frames.append(season)
        frame = pd.concat(frames, ignore_index=True)
        missing = REQUIRED_COLUMNS.difference(frame.columns)
        if missing:
            raise ValueError(f"Annual files are incompatible; missing columns: {sorted(missing)}")
        keys = ["tourney_id", "match_num", "winner_id", "loser_id"]
        return (
            frame.drop_duplicates(subset=keys, keep="last")
            .sort_values(["tourney_date", "tourney_id", "match_num"])
            .reset_index(drop=True)
        )

    def refresh_history(
        self, raw_directory: Path, years: set[int] | None = None
    ) -> tuple[pd.DataFrame, list[DownloadedSeason]]:
        """Download annual files and atomically rebuild the consolidated historical input."""
        downloaded = self.download_seasons(raw_directory, years)
        return self.consolidate_seasons(raw_directory), downloaded

    def consolidate_seasons(self, raw_directory: Path) -> pd.DataFrame:
        """Atomically publish consolidated local seasons without downloading anything."""
        frame = self.load_seasons(raw_directory)
        output = Path(self.csv_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        with _atomic_output(output) as temporary:
            frame.to_csv(temporary, index=False)
        return frame
