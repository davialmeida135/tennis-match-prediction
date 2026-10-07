"""Chronological transforms producing match comparisons and player-history assets."""

from datetime import date

import pandas as pd

from pipelines.historical_matches.transforms.elo import elo_delta
from pipelines.historical_matches.transforms.history import order_history, parse_match_date
from pipelines.historical_matches.transforms.imputation import fill_null_surface
from pipelines.historical_matches.transforms.player_comparison import calculate_player_comparison
from pipelines.shared.contracts import (
    FEATURE_COLUMNS,
    INITIAL_ELO,
    PlayedMatch,
    PlayerHistory,
    PlayerState,
)


def apply_result(history: PlayerHistory, match: pd.Series) -> dict[str, float]:
    """Return pre-match winner-minus-loser features, then update both players atomically."""
    played_on = _match_date(match)
    if history.last_result_date is not None and played_on < history.last_result_date:
        raise ValueError("Results must be applied in chronological order")
    winner = history.players.setdefault(
        str(match["winner_id"]), PlayerState(name=str(match["winner_name"]))
    )
    loser = history.players.setdefault(
        str(match["loser_id"]), PlayerState(name=str(match["loser_name"]))
    )
    surface = str(match.get("surface", "Hard"))
    features = calculate_player_comparison(loser, winner, played_on, surface)
    winner_rating = winner.rating
    loser_rating = loser.rating
    winner_surface_rating = winner.surface_ratings.get(surface, INITIAL_ELO)
    loser_surface_rating = loser.surface_ratings.get(surface, INITIAL_ELO)
    winner.rating = winner_rating + elo_delta(
        winner_rating, loser_rating, 1.0, winner.wins + winner.losses
    )
    loser.rating = loser_rating + elo_delta(
        loser_rating, winner_rating, 0.0, loser.wins + loser.losses
    )
    winner.surface_ratings[surface] = winner_surface_rating + elo_delta(
        winner_surface_rating, loser_surface_rating, 1.0, winner.wins + winner.losses
    )
    loser.surface_ratings[surface] = loser_surface_rating + elo_delta(
        loser_surface_rating, winner_surface_rating, 0.0, loser.wins + loser.losses
    )
    _update_player(winner, loser, match, played_on, surface, won=True)
    _update_player(loser, winner, match, played_on, surface, won=False)
    history.last_result_date = played_on
    return features


def _update_player(
    player: PlayerState,
    opponent: PlayerState,
    match: pd.Series,
    played_on: date,
    surface: str,
    *,
    won: bool,
) -> None:
    prefix = "w" if won else "l"
    player.wins += int(won)
    player.losses += int(not won)
    player.h2h_wins[opponent.name] = player.h2h_wins.get(opponent.name, 0) + int(won)
    player.history.insert(
        0,
        PlayedMatch(
            played_on=played_on.isoformat(),
            surface=surface,
            won=won,
            minutes=_number(match.get("minutes")),
        ),
    )
    player.history = player.history[:50]
    player.serve_points += _number(match.get(f"{prefix}_svpt"))
    player.aces += _number(match.get(f"{prefix}_ace"))
    player.double_faults += _number(match.get(f"{prefix}_df"))
    player.service_points_won += _number(match.get(f"{prefix}_1stWon")) + _number(
        match.get(f"{prefix}_2ndWon")
    )
    side = "winner" if won else "loser"
    player.rank = _optional_number(match.get(f"{side}_rank")) or player.rank
    player.rank_points = _optional_number(match.get(f"{side}_rank_points")) or player.rank_points
    player.age = _optional_number(match.get(f"{side}_age")) or player.age


def attach_temporal_features(
    matches: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, PlayerHistory]:
    """Keep features on their source rows; also return the comparisons and player history."""
    source = matches.reset_index(drop=True)
    features, state = _temporal_features_by_row(source)
    featured = source.join(features[[*FEATURE_COLUMNS, "match_date"]])
    return featured, features.reset_index(drop=True), state


def _temporal_features_by_row(
    matches: pd.DataFrame,
) -> tuple[pd.DataFrame, PlayerHistory]:
    """Use source row positions for alignment, independent of match identifiers."""
    required = {
        "tourney_date",
        "tourney_id",
        "match_num",
        "winner_id",
        "loser_id",
        "winner_name",
        "loser_name",
    }
    missing = required.difference(matches.columns)
    if missing:
        raise ValueError(f"Missing historical match columns: {sorted(missing)}")
    state = PlayerHistory(last_result_date=None, players={})
    source_rows: list[int] = []
    rows: list[dict[str, float | int | str | None]] = []
    ordered = order_history(fill_null_surface(matches.reset_index(drop=True)))
    for source_row, match in ordered.iterrows():
        if str(match.get("score", "")) == "W/O":
            continue
        winner_minus_loser = apply_result(state, match)
        source_rows.append(source_row)
        rows.append(
            {
                "tourney_id": str(match["tourney_id"]),
                "match_num": int(match["match_num"]) if pd.notna(match["match_num"]) else None,
                "winner_id": str(match["winner_id"]),
                "loser_id": str(match["loser_id"]),
                **winner_minus_loser,
                "match_date": _match_date(match).isoformat(),
            }
        )
    columns = ["tourney_id", "match_num", "winner_id", "loser_id", *FEATURE_COLUMNS, "match_date"]
    frame = pd.DataFrame(rows, columns=columns, index=source_rows)
    # Preserve missing source match numbers in the standalone temporal export.
    frame["match_num"] = frame["match_num"].astype("Int64")
    return frame, PlayerHistory.model_validate(state.model_dump())


def _match_date(match: pd.Series) -> date:
    return parse_match_date(match["tourney_date"])


def _number(value: object) -> float:
    parsed = pd.to_numeric(value, errors="coerce")
    return float(parsed) if pd.notna(parsed) else 0.0


def _optional_number(value: object) -> float | None:
    parsed = pd.to_numeric(value, errors="coerce")
    return float(parsed) if pd.notna(parsed) else None
