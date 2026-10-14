"""Random forest uses the same features without scaling."""

from sklearn.ensemble import RandomForestClassifier

from tennis_match_prediction.contracts import RandomForestSettings
from tennis_match_prediction.ml.models.base import SklearnMatchModel


class RandomForestModel(SklearnMatchModel):
    name = "random_forest"

    def __init__(self, settings: RandomForestSettings) -> None:
        self.estimator = RandomForestClassifier(**settings.model_dump())
