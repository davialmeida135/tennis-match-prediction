"""Typed contracts at the boundaries of the prediction pipeline."""

from datetime import date

from pydantic import BaseModel, ConfigDict, Field, field_validator


class FutureMatchRequest(BaseModel):
    """Minimum information needed to produce a pre-match prediction."""

    model_config = ConfigDict(str_strip_whitespace=True)

    player0_name: str = Field(min_length=1)
    player1_name: str = Field(min_length=1)
    match_date: date
    surface: str

    @field_validator("surface")
    @classmethod
    def normalize_surface(cls, value: str) -> str:
        """Accept source casing while keeping the feature schema finite."""
        normalized = value.title()
        if normalized not in {"Hard", "Clay", "Grass", "Carpet"}:
            raise ValueError("surface must be Hard, Clay, Grass, or Carpet")
        return normalized


class Prediction(BaseModel):
    """A prediction with enough provenance to reproduce it."""

    player0_name: str
    player1_name: str
    player0_win_probability: float = Field(ge=0, le=1)
    player1_win_probability: float = Field(ge=0, le=1)
    feature_cutoff: date
    model_path: str
