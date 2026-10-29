"""Pre-match prediction inputs and probabilities with provenance."""

from datetime import date

from pydantic import ConfigDict, Field, field_validator

from tennis_match_prediction.contracts.base import ContractModel


class FutureMatchRequest(ContractModel):
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


class Prediction(ContractModel):
    """A prediction with enough provenance to reproduce it."""

    player0_name: str
    player1_name: str
    player0_win_probability: float = Field(ge=0, le=1)
    player1_win_probability: float = Field(ge=0, le=1)
    history_source_date: date
    model_path: str
    run_id: str | None = None
