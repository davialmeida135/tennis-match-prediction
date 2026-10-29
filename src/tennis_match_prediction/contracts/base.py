"""Shared validation policy for typed application boundaries."""

from pydantic import BaseModel, ConfigDict


class ContractModel(BaseModel):
    """Common validation policy for pipeline data models."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
