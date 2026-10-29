"""Explicit model selection with validated, model-specific settings."""

from pydantic import JsonValue

from tennis_match_prediction.contracts.model_settings import (
    ExtraTreesSettings,
    HistGradientBoostingSettings,
    LogisticRegressionSettings,
    RandomForestSettings,
    XGBoostSettings,
)
from tennis_match_prediction.ml.models.base import BaseMatchModel
from tennis_match_prediction.ml.models.extra_trees import ExtraTreesModel
from tennis_match_prediction.ml.models.hist_gradient_boosting import HistGradientBoostingModel
from tennis_match_prediction.ml.models.logistic_regression import LogisticRegressionModel
from tennis_match_prediction.ml.models.random_forest import RandomForestModel
from tennis_match_prediction.ml.models.xgboost import XGBoostModel


def create_model(name: str, parameters: dict[str, JsonValue]) -> BaseMatchModel:
    factories = {
        "logistic_regression": (LogisticRegressionSettings, LogisticRegressionModel),
        "random_forest": (RandomForestSettings, RandomForestModel),
        "extra_trees": (ExtraTreesSettings, ExtraTreesModel),
        "hist_gradient_boosting": (HistGradientBoostingSettings, HistGradientBoostingModel),
        "xgboost": (XGBoostSettings, XGBoostModel),
    }
    if name not in factories:
        raise ValueError(f"Unknown model {name!r}; choose from {', '.join(factories)}")
    settings_type, model_type = factories[name]
    return model_type(settings_type.model_validate(parameters))
