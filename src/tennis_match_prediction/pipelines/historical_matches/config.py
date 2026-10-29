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
from typing import Any, Self

import pandas as pd
import requests
from dagster import ConfigurableResource
from pydantic import Field, model_validator

from tennis_match_prediction.contracts.ingestion import DownloadedSeason
from tennis_match_prediction.paths import DEFAULT_HISTORICAL_MATCHES_CSV

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
                # Windows scanners can briefly deny replacement with ACCESS_DENIED
                # (5), not only the sharing/lock violations (32/33).
                if getattr(error, "winerror", None) not in {5, 32, 33}:
                    raise
                if attempt == 4:
                    raise PermissionError(
                        f"Cannot replace {output}: Windows denied access "
                        "or reports that a file is in use. "
                        "Check file permissions and applications holding it open, then retry."
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
    include_ongoing: bool = False
    years: list[int] | None = None
    first_year: int | None = Field(
        default=None,
        ge=1000,
        le=9999,
        description="Include every available season from this year onward (inclusive).",
    )
    catalog_url: str = DATA_FILES_URL
    timeout: int = Field(default=60, gt=0)

    @model_validator(mode="after")
    def validate_year_selection(self) -> Self:
        """Keep an explicit season list separate from an open-ended range."""
        if self.first_year is not None and self.years is not None:
            raise ValueError("Configure either first_year or years, not both")
        return self

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
        if self.include_ongoing:
            ongoing = [file for file in files if file.get("name") == "ongoing_tourneys.csv"]
            if len(ongoing) != 1:
                raise ValueError("Catalog must contain exactly one ongoing_tourneys.csv")
            self._download_ongoing(ongoing[0], destination)
        self._write_manifest(destination, downloaded)
        return sorted(downloaded, key=lambda item: item.year)

    def _download_ongoing(self, file: dict[str, Any], destination: Path) -> None:
        """Retain observed results; absence from a partial feed never deletes a match."""
        response = requests.get(str(file["url"]), timeout=self.timeout)
        response.raise_for_status()
        content = response.content
        frame = pd.read_csv(BytesIO(content), dtype=str)
        self._normalize_match_numbers(frame)
        self._validate_result_keys(frame)
        if (
            "score" not in frame
            or frame["score"].fillna("").str.strip().isin(["", "To play", "to play"]).any()
        ):
            raise ValueError("Ongoing feed contains rows without a result")
        digest = hashlib.sha256(content).hexdigest()
        snapshots = destination / "ongoing_snapshots"
        snapshots.mkdir(exist_ok=True)
        snapshot = snapshots / f"{digest}.csv"
        if not snapshot.exists():
            with _atomic_output(snapshot) as temporary:
                temporary.write_bytes(content)
        retained = destination / "ongoing_results.csv"
        if retained.exists():
            previous = pd.read_csv(retained, dtype=str)
            self._normalize_match_numbers(previous)
            self._validate_result_keys(previous)
            frame = pd.concat([previous, frame], ignore_index=True).drop_duplicates(
                ["tourney_id", "match_num"], keep="last"
            )
        with _atomic_output(retained) as temporary:
            frame.to_csv(temporary, index=False)

    @staticmethod
    def _normalize_match_numbers(frame: pd.DataFrame) -> None:
        if "match_num" in frame:
            frame["match_num"] = (
                pd.to_numeric(frame["match_num"], errors="raise").astype("Int64").astype("string")
            )

    @staticmethod
    def _validate_result_keys(frame: pd.DataFrame) -> None:
        missing = REQUIRED_COLUMNS.difference(frame.columns)
        if missing:
            raise ValueError(f"Results missing columns: {sorted(missing)}")
        keys = ["tourney_id", "match_num", "winner_id", "loser_id"]
        if frame[keys].isna().any().any() or frame[keys].eq("").any().any():
            raise ValueError("Results require complete match and participant IDs")
        if frame.duplicated(["tourney_id", "match_num"]).any():
            raise ValueError("Ambiguous result identity: repeated tournament/match number")
        if pd.to_numeric(frame["match_num"], errors="raise").le(0).any():
            raise ValueError("Results require positive match numbers")
        if frame["winner_id"].eq(frame["loser_id"]).any():
            raise ValueError("Results contain identical participants")

    def _is_annual_atp_file(self, file: dict[str, Any], years: set[int] | None) -> bool:
        name = str(file.get("name", ""))
        stem = Path(name).stem
        if not stem.isdigit() or len(stem) != 4 or not name.endswith(".csv"):
            return False
        year = int(stem)
        if self.first_year is not None and year < self.first_year:
            return False
        return years is None or year in years

    def _download_file(self, file: dict[str, Any], destination: Path) -> DownloadedSeason:
        year = int(Path(str(file["name"])).stem)
        response = requests.get(str(file["url"]), timeout=self.timeout)
        response.raise_for_status()
        content = response.content
        digest = hashlib.sha256(content).hexdigest()
        path = destination / f"{year}.csv"
        changed = not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest() != digest
        # Parse the entire response before replacing a valid local season.
        frame = pd.read_csv(BytesIO(content), low_memory=False)
        missing = REQUIRED_COLUMNS.difference(frame.columns)
        if missing:
            raise ValueError(f"{path.name} is incompatible; missing columns: {sorted(missing)}")
        if frame.empty:
            raise ValueError(f"{path.name} contains no matches")
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
        with _atomic_output(manifest_path) as temporary:
            temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    def load_seasons(self, raw_directory: Path) -> pd.DataFrame:
        """Load, validate, and deduplicate local annual ATP files into one chronological frame."""
        paths = sorted(
            path
            for path in raw_directory.glob("[0-9][0-9][0-9][0-9].csv")
            if self.first_year is None or int(path.stem) >= self.first_year
        )
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
        retained = raw_directory / "ongoing_results.csv"
        if self.include_ongoing and retained.exists():
            ongoing = pd.read_csv(retained, dtype=str)
            # Read both identity columns as text before joining across sources.
            annual = frame.copy()
            for column in ("tourney_id", "match_num", "winner_id", "loser_id"):
                annual[column] = annual[column].astype("string")
            self._normalize_match_numbers(annual)
            self._normalize_match_numbers(ongoing)
            annual = annual.drop_duplicates().reset_index(drop=True)
            # Legacy annual rows may lack match numbers or player IDs. Preserve
            # them for the existing chronology/normalization policy, but do not
            # use an incomplete identity to reconcile a partial feed.
            complete = (
                annual[["tourney_id", "match_num", "winner_id", "loser_id"]].notna().all(axis=1)
            )
            complete &= ~annual.duplicated(["tourney_id", "match_num"], keep=False)
            complete &= annual["winner_id"].ne(annual["loser_id"]).fillna(False)
            identified = annual.loc[complete]
            self._validate_result_keys(identified)
            self._validate_result_keys(ongoing)
            if self.first_year is not None:
                years = pd.to_datetime(ongoing["tourney_date"], format="mixed").dt.year
                ongoing = ongoing.loc[years >= self.first_year]
            unresolved = annual.loc[~complete, ["tourney_id", "match_num"]]
            if not unresolved.merge(ongoing, on=["tourney_id", "match_num"]).empty:
                raise ValueError(
                    "Ongoing result overlaps an ambiguous annual identity; review source"
                )
            overlap = identified.merge(
                ongoing, on=["tourney_id", "match_num"], suffixes=("_a", "_o")
            )
            for row in overlap.to_dict("records"):
                if {row["winner_id_a"], row["loser_id_a"]} != {
                    row["winner_id_o"],
                    row["loser_id_o"],
                }:
                    raise ValueError("Annual/ongoing participant conflict; review identity")
            # Annual is authoritative, including corrected winners and scores.
            reconciled = pd.concat([ongoing, identified], ignore_index=True).drop_duplicates(
                ["tourney_id", "match_num"], keep="last"
            )
            frame = pd.concat([reconciled, annual.loc[~complete]], ignore_index=True)
            frame["tourney_date"] = pd.to_datetime(
                frame["tourney_date"].astype(str), format="mixed", errors="raise"
            ).dt.strftime("%Y%m%d")
            frame["match_num"] = pd.to_numeric(frame["match_num"], errors="raise")
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
