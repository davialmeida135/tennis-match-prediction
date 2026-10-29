"""Prospective probability metrics and calibration summaries."""

import math
from typing import Any

import numpy as np
import pandas as pd

from tennis_match_prediction.ml.evaluation import probability_metrics


def forecast_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize reconciled forecasts, including history lag and calibration buckets."""
    if not rows:
        return {"count": 0, "accuracy": None, "brier": None, "log_loss": None}
    target = pd.Series([row["target"] for row in rows])
    probabilities = np.array([row["probability"] for row in rows])
    metrics = probability_metrics(target, probabilities, "observed")
    return {
        "count": len(rows),
        **{name.removeprefix("observed_"): value for name, value in metrics.items()},
        "mean_history_lag_days": sum(row["history_lag_days"] for row in rows) / len(rows),
        "neutral_baseline_brier": 0.25,
        "neutral_baseline_log_loss": math.log(2),
        "calibration": [
            {
                "lower": lower / 10,
                "upper": (lower + 1) / 10,
                "count": len(bucket),
                "mean_probability": sum(row["probability"] for row in bucket) / len(bucket),
                "observed_frequency": sum(row["target"] for row in bucket) / len(bucket),
            }
            for lower in range(10)
            if (bucket := [row for row in rows if min(int(row["probability"] * 10), 9) == lower])
        ],
    }
