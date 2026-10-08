"""Chronological transforms producing match comparisons and player-history assets."""

from datetime import date
from math import isfinite

import pandas as pd

from tennis_match_prediction.contracts import (
    FEATURE_COLUMNS,
    PlayedMatch,
    PlayerHistory,
    PlayerState,
)
from tennis_match_prediction.transforms.elo import update_match_ratings
from tennis_match_prediction.transforms.history import (
    history_batches,
    order_history,
)
from tennis_match_prediction.transforms.imputation import (
    fill_null_surface,
)
from tennis_match_prediction.transforms.player_comparison import calculate_player_comparison


def _apply_result(history: PlayerHistory, match: pd.Series, prior: dict[str, PlayerState]) -> None:
    """Apply a result using ratings and counts frozen before its batch."""
    winner = history.players[str(match["winner_id"])]
    loser = history.players[str(match["loser_id"])]
    previous_winner = prior[str(match["winner_id"])]
    previous_loser = prior[str(match["loser_id"])]
    surface = str(match.get("surface", "Hard"))
    update_match_ratings(
        winner, loser, surface, prior_winner=previous_winner, prior_loser=previous_loser
    )
    source_date = match["tourney_date"].date()
    _update_player(winner, loser, match, source_date, surface, won=True)
    _update_player(loser, winner, match, source_date, surface, won=False)
    history.last_source_date = source_date


def _update_player(
    player: PlayerState,
    opponent: PlayerState,
    match: pd.Series,
    source_date: date,
    surface: str,
    *,
    won: bool,
) -> None:
    """
    Update a player's state based on the outcome of a match.
    """
    prefix = "w" if won else "l"
    player.wins += int(won)
    player.losses += int(not won)
    player.h2h_wins[opponent.name] = player.h2h_wins.get(opponent.name, 0) + int(won)
    player.history.insert(
        0,
        PlayedMatch(
            tourney_date=source_date,
            surface=surface,
            won=won,
        ),
    )
    player.history = player.history[:50]
    player.surface_history.setdefault(surface, []).insert(0, player.history[0])
    player.surface_history[surface] = player.surface_history[surface][:10]
    points = _optional_number(match.get(f"{prefix}_svpt"))
    if points is not None and points > 0:
        player.serve_points += points
        for numerator, denominator, fields in (
            ("aces", "ace_serve_points", ("ace",)),
            ("double_faults", "double_fault_serve_points", ("df",)),
            ("service_points_won", "service_won_serve_points", ("1stWon", "2ndWon")),
        ):
            values = [_optional_number(match.get(f"{prefix}_{field}")) for field in fields]
            if all(value is not None and value >= 0 for value in values):
                total = sum(values)
                if total <= points:
                    setattr(player, numerator, getattr(player, numerator) + total)
                    setattr(player, denominator, getattr(player, denominator) + points)
    side = "winner" if won else "loser"
    for attribute in ("rank", "rank_points", "age"):
        value = _optional_number(match.get(f"{side}_{attribute}"))
        if value is not None and value >= 0 and (attribute != "rank" or value > 0):
            setattr(player, attribute, value)


def attach_temporal_features(
    matches: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, PlayerHistory]:
    """Keep features on their source rows; also return the comparisons and player history."""
    source = matches.reset_index(drop=True)
    features, state = _temporal_features_by_row(source)
    featured = source.join(features[list(FEATURE_COLUMNS)])
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
    state = PlayerHistory(last_source_date=None, players={})
    source_rows: list[int] = []
    rows: list[dict[str, float | int | str | None]] = []
    ordered = order_history(fill_null_surface(matches.reset_index(drop=True)))
    if "score" in ordered:
        ordered = ordered.loc[ordered["score"].ne("W/O").fillna(True)]
    for batch in history_batches(ordered):
        for _, match in batch.iterrows():
            for side in ("winner", "loser"):
                state.players.setdefault(
                    str(match[f"{side}_id"]), PlayerState(name=str(match[f"{side}_name"]))
                )
        prior = {
            player_id: state.players[player_id].model_copy(
                update={"surface_ratings": state.players[player_id].surface_ratings.copy()}
            )
            for player_id in set(batch["winner_id"].astype(str))
            | set(batch["loser_id"].astype(str))
        }
        # Compute every comparison before mutating shared history lists or H2H.
        for source_row, match in batch.iterrows():
            source_rows.append(source_row)
            rows.append(
                {
                    "tourney_id": str(match["tourney_id"]),
                    "match_num": int(match["match_num"]) if pd.notna(match["match_num"]) else None,
                    "winner_id": str(match["winner_id"]),
                    "loser_id": str(match["loser_id"]),
                    **calculate_player_comparison(
                        prior[str(match["loser_id"])],
                        prior[str(match["winner_id"])],
                        str(match["surface"]),
                    ),
                    "tourney_date": match["tourney_date"].date().isoformat(),
                }
            )
        for _, match in batch.iterrows():
            _apply_result(state, match, prior)
    columns = ["tourney_id", "match_num", "winner_id", "loser_id", *FEATURE_COLUMNS, "tourney_date"]
    frame = pd.DataFrame(rows, columns=columns, index=source_rows)
    # Preserve missing source match numbers in the standalone temporal export.
    frame["match_num"] = frame["match_num"].astype("Int64")
    return frame, PlayerHistory.model_validate(state.model_dump())


def _optional_number(value: object) -> float | None:
    parsed = pd.to_numeric(value, errors="coerce")
    return float(parsed) if pd.notna(parsed) and isfinite(parsed) else None
