"""Match-level forecast and observed-result details, in forecast participant order."""

import math
from typing import Any


def prediction_details(
    forecast: dict[str, Any], observation: dict[str, Any] | None
) -> dict[str, Any]:
    """Preserve both records; map a winner only after verifying match and participant IDs."""
    match = forecast["match"]
    result = observation.get("match") if observation else None
    expected = [match.get("provider_player0_id"), match.get("provider_player1_id")]
    winner_index = None
    if result and type(result.get("id")) is int and result["id"] == match["id"]:
        players = result.get("players") or {}
        ids = [(players.get(side) or {}).get("id") for side in ("p1", "p2")]
        winner = result.get("winner")
        if (
            all(type(value) is int for value in [*ids, *expected])
            and len(set(ids)) == len(set(expected)) == 2
            and set(ids) == set(expected)
            and result.get("status") == "completed"
            and type(winner) is int
            and winner in (1, 2)
        ):
            winner_index = expected.index(ids[winner - 1])
    probabilities = [forecast.get(f"player{index}_win_probability") for index in (0, 1)]
    valid = all(
        type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 1
        for value in probabilities
    )
    predicted_index = None
    if valid and math.isclose(sum(probabilities), 1, abs_tol=1e-9):
        predicted_index = int(probabilities[1] >= 0.5)
    return {
        "player0_name": _player_name(forecast, 0, expected[0]),
        "player1_name": _player_name(forecast, 1, expected[1]),
        "forecast": forecast,
        "result": result,
        "result_observed_at": observation.get("observed_at") if observation else None,
        "winner_player_index": winner_index,
        "predicted_player_index": predicted_index,
    }


def _player_name(forecast: dict[str, Any], index: int, provider_id: int | None) -> str:
    player = forecast.get(f"player{index}") or {}
    return player.get("name") or forecast.get(f"player{index}_name") or f"Player {provider_id}"
