"""Validated hyperparameters for supported model adapters."""

from typing import Literal

from pydantic import Field

from tennis_match_prediction.contracts.base import ContractModel


class LogisticRegressionSettings(ContractModel):
    C: float = Field(default=1.0, gt=0)
    max_iter: int = Field(default=1000, gt=0, strict=True)
    random_state: int = Field(default=42, ge=0, strict=True)
    class_weight: Literal["balanced"] | None = None


class RandomForestSettings(ContractModel):
    n_estimators: int = Field(default=200, gt=0, strict=True)
    max_depth: int | None = Field(default=None, gt=0, strict=True)
    min_samples_leaf: int = Field(default=5, gt=0, strict=True)
    max_features: Literal["sqrt", "log2"] | None = "sqrt"
    random_state: int = Field(default=42, ge=0, strict=True)
    n_jobs: int = Field(default=1, ge=1, strict=True)
    class_weight: Literal["balanced", "balanced_subsample"] | None = None


class ExtraTreesSettings(RandomForestSettings):
    """Randomized tree thresholds with the same controls as the forest adapter."""


class HistGradientBoostingSettings(ContractModel):
    learning_rate: float = Field(default=0.1, gt=0)
    max_iter: int = Field(default=200, gt=0, strict=True)
    max_leaf_nodes: int = Field(default=15, ge=2, strict=True)
    max_depth: int | None = Field(default=None, gt=0, strict=True)
    min_samples_leaf: int = Field(default=20, gt=0, strict=True)
    l2_regularization: float = Field(default=1.0, ge=0)
    random_state: int = Field(default=42, ge=0, strict=True)
    # Fit all requested iterations; never create a random internal validation split.
    early_stopping: Literal[False] = False


class XGBoostSettings(ContractModel):
    n_estimators: int = Field(default=300, gt=0, strict=True)
    max_depth: int = Field(default=3, gt=0, strict=True)
    learning_rate: float = Field(default=0.05, gt=0)
    min_child_weight: float = Field(default=5.0, ge=0)
    subsample: float = Field(default=0.8, gt=0, le=1)
    colsample_bytree: float = Field(default=0.8, gt=0, le=1)
    reg_alpha: float = Field(default=0.0, ge=0)
    reg_lambda: float = Field(default=1.0, ge=0)
    random_state: int = Field(default=42, ge=0, strict=True)
    n_jobs: int = Field(default=2, ge=1, strict=True)
    objective: Literal["binary:logistic"] = "binary:logistic"
    eval_metric: Literal["logloss"] = "logloss"
    tree_method: Literal["hist"] = "hist"
    device: Literal["cpu"] = "cpu"
