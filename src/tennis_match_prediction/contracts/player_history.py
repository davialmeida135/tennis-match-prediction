"""Accumulated player state and source-dated result history."""

from datetime import date
from typing import Annotated

from pydantic import Field, model_validator

from tennis_match_prediction.contracts.base import ContractModel

INITIAL_ELO = 1500.0

NonnegativeFloat = Annotated[float, Field(ge=0)]
NonnegativeInt = Annotated[int, Field(ge=0, strict=True)]


class PlayedMatch(ContractModel):
    """Past result in source-date/match-number order; no inferred calendar day."""

    tourney_date: date
    surface: str
    won: bool = Field(strict=True)


class PlayerState(ContractModel):
    """Accumulated player information, used by historical transforms and prediction."""

    name: str = Field(min_length=1)
    rating: float = INITIAL_ELO
    surface_ratings: dict[str, float] = Field(default_factory=dict)
    wins: NonnegativeInt = 0
    losses: NonnegativeInt = 0
    h2h_wins: dict[str, NonnegativeInt] = Field(default_factory=dict)
    history: list[PlayedMatch] = Field(default_factory=list, max_length=50)
    surface_history: dict[str, list[PlayedMatch]] = Field(default_factory=dict)
    serve_points: NonnegativeFloat = 0.0
    ace_serve_points: NonnegativeFloat = 0.0
    double_fault_serve_points: NonnegativeFloat = 0.0
    service_won_serve_points: NonnegativeFloat = 0.0
    aces: NonnegativeFloat = 0.0
    double_faults: NonnegativeFloat = 0.0
    service_points_won: NonnegativeFloat = 0.0
    rank: Annotated[float, Field(gt=0)] | None = None
    rank_points: NonnegativeFloat | None = None
    age: NonnegativeFloat | None = None


class PlayerHistory(ContractModel):
    last_source_date: date | None
    players: dict[str, PlayerState]

    @model_validator(mode="after")
    def history_matches_cutoff(self) -> "PlayerHistory":
        for player_id, player in self.players.items():
            if not player_id.strip():
                raise ValueError("player IDs must be nonblank")
            dates = [item.tourney_date for item in player.history]
            if dates and (self.last_source_date is None or max(dates) > self.last_source_date):
                raise ValueError("player history exceeds the player-history cutoff")
            if dates != sorted(dates, reverse=True):
                raise ValueError("player history must be newest first")
            for surface, matches in player.surface_history.items():
                if len(matches) > 10 or any(item.surface != surface for item in matches):
                    raise ValueError("surface history must contain at most 10 matching results")
                surface_dates = [item.tourney_date for item in matches]
                if surface_dates and (
                    self.last_source_date is None or max(surface_dates) > self.last_source_date
                ):
                    raise ValueError("surface history exceeds the player-history cutoff")
                if surface_dates != sorted(surface_dates, reverse=True):
                    raise ValueError("surface history must be newest first")
        return self
