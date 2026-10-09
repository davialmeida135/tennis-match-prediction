"""Explicit model selection with validated, model-specific settings."""

from pydantic import JsonValue

from tennis_match_prediction.contracts import LogisticRegressionSettings, RandomForestSettings
from tennis_match_prediction.ml.models.base import BaseMatchModel
from tennis_match_prediction.ml.models.logistic_regression import LogisticRegressionModel
from tennis_match_prediction.ml.models.random_forest import RandomForestModel


def create_model(name: str, parameters: dict[str, JsonValue]) -> BaseMatchModel:
    factories = {
        "logistic_regression": (LogisticRegressionSettings, LogisticRegressionModel),
        "random_forest": (RandomForestSettings, RandomForestModel),
    }
    if name not in factories:
        raise ValueError(f"Unknown model {name!r}; choose from {', '.join(factories)}")
    settings_type, model_type = factories[name]
    return model_type(settings_type.model_validate(parameters))
