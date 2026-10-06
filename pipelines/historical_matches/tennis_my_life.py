"""Idempotent ingestion of annual ATP files published by TennisMyLife."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests

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


@dataclass(frozen=True)
class DownloadedSeason:
    """Metadata recorded for one downloaded source file."""

    year: int
    path: Path
    sha256: str
    changed: bool


class TennisMyLifeClient:
    """Downloads annual ATP match files while retaining source provenance."""

    def __init__(self, *, catalog_url: str = DATA_FILES_URL, timeout: int = 60) -> None:
        self.catalog_url = catalog_url
        self.timeout = timeout

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
        if changed:
            path.write_bytes(content)
        frame = pd.read_csv(path, nrows=1)
        missing = REQUIRED_COLUMNS.difference(frame.columns)
        if missing:
            raise ValueError(f"{path.name} is incompatible; missing columns: {sorted(missing)}")
        return DownloadedSeason(year=year, path=path, sha256=digest, changed=changed)

    @staticmethod
    def _write_manifest(destination: Path, files: list[DownloadedSeason]) -> None:
        manifest = {
            "source": DATA_FILES_URL,
            "seasons": [
                {"year": item.year, "path": item.path.name, "sha256": item.sha256} for item in files
            ],
        }
        (destination / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
        )


def load_seasons(raw_directory: Path) -> pd.DataFrame:
    """Load, validate, and deduplicate local annual ATP files into one chronological frame."""
    paths = sorted(path for path in raw_directory.glob("[0-9][0-9][0-9][0-9].csv"))
    if not paths:
        raise FileNotFoundError(f"No annual ATP files found in {raw_directory}")
    frames = [
        pd.read_csv(path, dtype={"winner_id": "string", "loser_id": "string"}) for path in paths
    ]
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
    raw_directory: Path, years: set[int] | None = None
) -> tuple[pd.DataFrame, list[DownloadedSeason]]:
    """Download annual files and atomically rebuild the consolidated historical input."""
    downloaded = TennisMyLifeClient().download_seasons(raw_directory, years)
    frame = load_seasons(raw_directory)
    output = raw_directory / "all_atp_matches.csv"
    temporary = output.with_suffix(".tmp")
    frame.to_csv(temporary, index=False)
    temporary.replace(output)
    return frame, downloaded
