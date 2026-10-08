"""Download and consolidate annual ATP source files from TennisMyLife."""

from __future__ import annotations

import argparse
from pathlib import Path

from dagster import materialize

from pipelines.historical_matches.assets import raw_atp_matches
from pipelines.historical_matches.config import RawMatchesCsv
from pipelines.shared.parquet_io import ParquetDataFrameIOManager


def main() -> None:
    """Refresh a requested set of annual ATP files."""
    parser = argparse.ArgumentParser()
    parser.add_argument("years", nargs="*", type=int)
    parser.add_argument("--csv-path", type=Path)
    arguments = parser.parse_args()
    source = RawMatchesCsv(refresh=True, years=arguments.years or None)
    if arguments.csv_path is not None:
        source = RawMatchesCsv(
            csv_path=str(arguments.csv_path), refresh=True, years=arguments.years or None
        )
    result = materialize(
        [raw_atp_matches],
        resources={
            "raw_matches_csv": source,
            "io_manager": ParquetDataFrameIOManager(),
        },
    )
    if not result.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
