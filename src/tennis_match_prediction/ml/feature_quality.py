"""Measure source coverage before imputation; run with python -m ...ml.feature_quality."""

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from tennis_match_prediction.paths import DEFAULT_HISTORICAL_MATCHES_CSV


def coverage_report(frame: pd.DataFrame) -> dict[str, object]:
    """Separate missing, invalid and valid zero observations by season and field."""
    rows = []
    fields = {
        **{
            f"{side}_{field}": field == "rank"
            for side in ("winner", "loser")
            for field in ("rank", "rank_points", "age")
        },
        **{
            f"{side}_{field}": field == "svpt"
            for side in ("w", "l")
            for field in ("svpt", "ace", "df", "1stWon", "2ndWon")
        },
    }
    season = frame["tourney_id"].astype("string").str.extract(r"^(\d{4})", expand=False)
    for year, group in frame.groupby(season.fillna("unknown"), sort=True):
        for field, positive in fields.items():
            raw = group[field] if field in group else pd.Series(None, index=group.index)
            values = pd.to_numeric(raw, errors="coerce")
            missing = raw.isna() | raw.astype("string").str.strip().eq("").fillna(False)
            valid = np.isfinite(values) & (values > 0 if positive else values >= 0)
            rows.append(
                {
                    "season": str(year),
                    "field": field,
                    "observations": len(group),
                    "missing": int(missing.sum()),
                    "invalid": int((~missing & ~valid).sum()),
                    "valid_zero": int((valid & values.eq(0)).sum()),
                    "available_fraction": float(valid.mean()),
                }
            )
        for side in ("w", "l"):
            points = _numbers(group, f"{side}_svpt")
            for metric, fields_used in (
                ("ace_rate", ("ace",)),
                ("double_fault_rate", ("df",)),
                ("service_points_won_rate", ("1stWon", "2ndWon")),
            ):
                components = [_numbers(group, f"{side}_{field}") for field in fields_used]
                total = sum(components)
                valid = np.isfinite(points) & points.gt(0) & total.le(points)
                for component in components:
                    valid &= np.isfinite(component) & component.ge(0)
                rows.append(
                    {
                        "season": str(year),
                        "field": f"{side}_{metric}",
                        "observations": len(group),
                        "available": int(valid.sum()),
                        "available_fraction": float(valid.mean()),
                    }
                )
    return {"rows": len(frame), "coverage": rows}


def _numbers(group: pd.DataFrame, field: str) -> pd.Series:
    raw = group.get(field, pd.Series(None, index=group.index))
    return pd.to_numeric(raw, errors="coerce")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, nargs="?", default=DEFAULT_HISTORICAL_MATCHES_CSV)
    parser.add_argument("--output", type=Path, default=Path("data/curated/feature_quality.json"))
    args = parser.parse_args()
    frame = pd.read_csv(args.source, low_memory=False)
    eligible = frame["winner_id"].notna() & frame["loser_id"].notna()
    if "score" in frame:
        eligible &= frame["score"].ne("W/O").fillna(True)
    report = coverage_report(frame.loc[eligible])
    report.update(
        {
            "source": str(args.source),
            "sha256": hashlib.sha256(args.source.read_bytes()).hexdigest(),
            "source_rows": len(frame),
            "excluded_rows": int((~eligible).sum()),
        }
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Coverage for {report['rows']} eligible matches saved to {args.output}")


if __name__ == "__main__":
    main()
