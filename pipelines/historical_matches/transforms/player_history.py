"""Chronological transforms producing match comparisons and player-history assets."""

from datetime import date

import pandas as pd

from pipelines.historical_matches.transforms.elo import elo_delta
from pipelines.historical_matches.transforms.history import history_batches, order_history
from pipelines.historical_matches.transforms.imputation import fill_null_surface
from pipelines.historical_matches.transforms.player_comparison import calculate_player_comparison
from pipelines.shared.contracts import (
    FEATURE_COLUMNS,
    INITIAL_ELO,
    PlayedMatch,
    PlayerHistory,
    PlayerState,
)


def _apply_result(history: PlayerHistory, match: pd.Series, prior: dict[str, PlayerState]) -> None:
    """Apply a result using ratings and counts frozen before its batch."""
    winner = history.players[str(match["winner_id"])]
    loser = history.players[str(match["loser_id"])]
    previous_winner = prior[str(match["winner_id"])]
    previous_loser = prior[str(match["loser_id"])]
    surface = str(match.get("surface", "Hard"))
    winner_rating = previous_winner.rating
    loser_rating = previous_loser.rating
    winner_surface_rating = previous_winner.surface_ratings.get(surface, INITIAL_ELO)
    loser_surface_rating = previous_loser.surface_ratings.get(surface, INITIAL_ELO)
    winner.rating += elo_delta(
        winner_rating, loser_rating, 1.0, previous_winner.wins + previous_winner.losses
    )
    loser.rating += elo_delta(
        loser_rating, winner_rating, 0.0, previous_loser.wins + previous_loser.losses
    )
    winner.surface_ratings[surface] = winner.surface_ratings.get(surface, INITIAL_ELO) + elo_delta(
        winner_surface_rating,
        loser_surface_rating,
        1.0,
        previous_winner.wins + previous_winner.losses,
    )
    loser.surface_ratings[surface] = loser.surface_ratings.get(surface, INITIAL_ELO) + elo_delta(
        loser_surface_rating,
        winner_surface_rating,
        0.0,
        previous_loser.wins + previous_loser.losses,
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


def _number(value: object) -> float:
    parsed = pd.to_numeric(value, errors="coerce")
    return float(parsed) if pd.notna(parsed) else 0.0


def _optional_number(value: object) -> float | None:
    parsed = pd.to_numeric(value, errors="coerce")
    return float(parsed) if pd.notna(parsed) else None
