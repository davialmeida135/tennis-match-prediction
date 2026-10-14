"""Logistic regression owns its train-only standardization pipeline."""

from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from tennis_match_prediction.contracts import LogisticRegressionSettings
from tennis_match_prediction.ml.models.base import SklearnMatchModel


class LogisticRegressionModel(SklearnMatchModel):
    name = "logistic_regression"

    def __init__(self, settings: LogisticRegressionSettings) -> None:
        self.estimator = make_pipeline(
            StandardScaler(), LogisticRegression(**settings.model_dump())
        )
