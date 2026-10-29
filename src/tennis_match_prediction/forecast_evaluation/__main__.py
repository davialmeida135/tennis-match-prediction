"""Collect results and evaluate archived forecasts."""

import argparse
import json
import os
from pathlib import Path

from tennis_match_prediction.forecast_evaluation.service import update_performance
from tennis_match_prediction.paths import DATA_DIR


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--predictions-dir", type=Path, default=DATA_DIR / "predictions")
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR / "performance")
    parser.add_argument("--max-requests", type=int, default=20)
    parser.add_argument("--offline", action="store_true", help="reuse saved results without HTTP")
    args = parser.parse_args()
    try:
        report = update_performance(
            args.predictions_dir,
            args.output_dir,
            api_key=os.getenv("LIVE_TENNIS_API_KEY"),
            max_requests=0 if args.offline else args.max_requests,
        )
    except (OSError, ValueError, RuntimeError) as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps({k: v for k, v in report.items() if k != "decisions"}, indent=2))


if __name__ == "__main__":
    main()
