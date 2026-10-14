"""Probability metrics for models and winner-picking ranking/Elo references."""

import numpy as np
import pandas as pd
from numpy.typing import NDArray
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss

from tennis_match_prediction.ml.models.base import BaseMatchModel

BASELINE_COLUMNS = ("rank_log_advantage", "overall_elo_diff", "surface_elo_diff")


def baseline_metrics(frame: pd.DataFrame, prefix: str) -> dict[str, float]:
    """Positive differences favor player1; neutral comparisons receive half credit.

    A lower numerical rank is better. rank_log_advantage already encodes that
    direction. Zero also represents unavailable comparisons in the published data,
    so neutral rows use the expected accuracy of a fair tie-break.
    """
    metrics = {}
    for name, column in (
        ("rank", "rank_log_advantage"),
        ("elo", "overall_elo_diff"),
        ("surface_elo", "surface_elo_diff"),
    ):
        if column not in frame.columns:
            continue
        difference = frame[column].to_numpy(dtype=float)
        if not np.isfinite(difference).all():
            raise ValueError(f"Baseline column {column} must contain finite values")
        correct = (difference > 0) == frame["winner"].to_numpy()
        neutral = difference == 0
        metrics[f"{prefix}_{name}_baseline_accuracy"] = float(
            np.where(neutral, 0.5, correct).mean()
        )
        metrics[f"{prefix}_{name}_baseline_neutral_fraction"] = float(neutral.mean())
    return metrics


def validate_probabilities(values: NDArray[np.float64], rows: int) -> NDArray[np.float64]:
    probabilities = np.asarray(values, dtype=float)
    if probabilities.shape != (rows,) or not np.isfinite(probabilities).all():
        raise ValueError("Model must return one finite probability per row")
    if ((probabilities < 0) | (probabilities > 1)).any():
        raise ValueError("Model probabilities must be between 0 and 1")
    return probabilities


def probability_metrics(target: pd.Series, probabilities: NDArray, prefix: str) -> dict[str, float]:
    probabilities = validate_probabilities(probabilities, len(target))
    return {
        f"{prefix}_accuracy": float(accuracy_score(target, probabilities >= 0.5)),
        f"{prefix}_brier": float(brier_score_loss(target, probabilities)),
        f"{prefix}_log_loss": float(log_loss(target, probabilities, labels=[0, 1])),
    }


def evaluate(
    model: BaseMatchModel, frame: pd.DataFrame, features: tuple[str, ...], prefix: str
) -> dict[str, float]:
    return probability_metrics(
        frame["winner"], model.predict_proba(frame.loc[:, list(features)]), prefix
    ) | baseline_metrics(frame, prefix)
