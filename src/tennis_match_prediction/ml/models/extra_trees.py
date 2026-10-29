"""Extra Trees randomizes thresholds to reduce tree ensemble variance."""

from sklearn.ensemble import ExtraTreesClassifier

from tennis_match_prediction.contracts.model_settings import ExtraTreesSettings
from tennis_match_prediction.ml.models.base import SklearnMatchModel


class ExtraTreesModel(SklearnMatchModel):
    name = "extra_trees"

    def __init__(self, settings: ExtraTreesSettings) -> None:
        self.estimator = ExtraTreesClassifier(**settings.model_dump())
