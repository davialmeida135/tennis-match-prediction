"""The model boundary uses P(player1 wins), independently of estimator class order."""

from abc import ABC, abstractmethod
from typing import ClassVar

import numpy as np
import pandas as pd
from numpy.typing import NDArray
from pydantic import JsonValue
from sklearn.base import BaseEstimator
from sklearn.utils.validation import check_is_fitted


class BaseMatchModel(ABC):
    name: ClassVar[str]

    @abstractmethod
    def fit(self, features: pd.DataFrame, target: pd.Series) -> None: ...

    @abstractmethod
    def predict_proba(self, features: pd.DataFrame) -> NDArray[np.float64]: ...

    @abstractmethod
    def get_params(self) -> dict[str, JsonValue]: ...


class SklearnMatchModel(BaseMatchModel):
    """Shared adapter for sklearn estimators; other frameworks can implement the ABC."""

    estimator: BaseEstimator

    def fit(self, features: pd.DataFrame, target: pd.Series) -> None:
        if set(target.unique()) != {0, 1}:
            raise ValueError("Training requires both target classes 0 and 1")
        self.estimator.fit(features, target)

    def predict_proba(self, features: pd.DataFrame) -> NDArray[np.float64]:
        check_is_fitted(self.estimator)
        classes = np.asarray(self.estimator.classes_)
        positive = np.flatnonzero(classes == 1)
        if len(positive) != 1 or set(classes) != {0, 1}:
            raise ValueError("Fitted estimator must have binary classes 0 and 1")
        return self.estimator.predict_proba(features)[:, positive[0]]

    def get_params(self) -> dict[str, JsonValue]:
        # Pipeline steps themselves are objects; their scalar parameters are reproducible.
        return {
            key: value
            for key, value in self.estimator.get_params(deep=True).items()
            if value is None or isinstance(value, (str, bool, int, float))
        }
