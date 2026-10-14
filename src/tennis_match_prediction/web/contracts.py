"""HTTP contracts for the personal prediction application."""

from datetime import date

from pydantic import Field, field_validator, model_validator

from tennis_match_prediction.contracts import ContractModel, FutureMatchRequest


class MatchupRequest(ContractModel):
    player0_id: str = Field(min_length=1, max_length=100)
    player1_id: str = Field(min_length=1, max_length=100)
    match_date: date
    surface: str
    model_id: str = Field(min_length=1, max_length=100)

    @field_validator("surface")
    @classmethod
    def normalize_surface(cls, value: str) -> str:
        return FutureMatchRequest.normalize_surface(value.strip())

    @model_validator(mode="after")
    def distinct_players(self) -> "MatchupRequest":
        if self.player0_id == self.player1_id:
            raise ValueError("Choose two different players")
        return self
