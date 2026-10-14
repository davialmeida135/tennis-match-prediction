"""Histogram boosting fits nonlinear interactions without feature scaling."""

from sklearn.ensemble import HistGradientBoostingClassifier

from tennis_match_prediction.contracts import HistGradientBoostingSettings
from tennis_match_prediction.ml.models.base import SklearnMatchModel


class HistGradientBoostingModel(SklearnMatchModel):
    name = "hist_gradient_boosting"

    def __init__(self, settings: HistGradientBoostingSettings) -> None:
        self.estimator = HistGradientBoostingClassifier(**settings.model_dump())
