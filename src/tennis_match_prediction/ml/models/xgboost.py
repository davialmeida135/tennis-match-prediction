"""CPU XGBoost shares chronological splits and tracking with the sklearn adapters."""

from pydantic import JsonValue
from xgboost import XGBClassifier

from tennis_match_prediction.contracts import XGBoostSettings
from tennis_match_prediction.ml.models.base import SklearnMatchModel


class XGBoostModel(SklearnMatchModel):
    name = "xgboost"

    def __init__(self, settings: XGBoostSettings) -> None:
        # Fixed iterations: fitting never consumes validation or test targets.
        self.estimator = XGBClassifier(**settings.model_dump())

    def get_params(self) -> dict[str, JsonValue]:
        parameters = super().get_params()
        # XGBoost's default missing-value sentinel is NaN. Metadata requires finite
        # JSON numbers, so record the sentinel by name while retaining it in the estimator.
        parameters["missing"] = "NaN"
        return parameters
