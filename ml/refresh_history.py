"""Download and consolidate annual ATP source files from TennisMyLife."""

from __future__ import annotations

import argparse

from pipelines.historical_matches.tennis_my_life import refresh_history
from pipelines.shared.paths import HISTORICAL_MATCHES_RAW_DIR


def main() -> None:
    """Refresh a requested set of annual ATP files."""
    parser = argparse.ArgumentParser()
    parser.add_argument("years", nargs="*", type=int)
    arguments = parser.parse_args()
    years = set(arguments.years) if arguments.years else None
    frame, downloaded = refresh_history(HISTORICAL_MATCHES_RAW_DIR, years)
    changed = [str(item.year) for item in downloaded if item.changed]
    print(f"Consolidated {len(frame)} matches; changed seasons: {', '.join(changed) or 'none'}")


if __name__ == "__main__":
    main()
