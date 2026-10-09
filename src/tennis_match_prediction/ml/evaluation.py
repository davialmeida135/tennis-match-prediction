"""Comparable metrics for model and constant-0.5 probabilities."""

import numpy as np
import pandas as pd
from numpy.typing import NDArray
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss

from tennis_match_prediction.ml.models.base import BaseMatchModel


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
    ) | probability_metrics(frame["winner"], np.full(len(frame), 0.5), f"{prefix}_baseline")
