"""Head-to-head history and Elo ratings.

Both features are computed chronologically: every row only sees matches that
already happened at that point of the season.
"""

import math

import pandas as pd
import polars as pl


def _calculate_elo_update(player_elo, opponent_elo, player_won, player_match_count):
    if not player_won:
        expected = 1 / (1 + 10 ** ((opponent_elo - player_elo) / 400))
    else:
        expected = 1 / (1 + 10 ** ((player_elo - opponent_elo) / 400))
    actual = 1.0 if player_won else 0.0

    # K-factor calculation (as in the original logic)
    k_factor = 250 / ((player_match_count + 5) ** 0.4)

    # Handle potential NaN/Inf from extreme Elo differences (though less likely with standard init)
    if math.isnan(expected) or math.isinf(expected):
        expected = 0.5  # Fallback if calculation fails

    delta = k_factor * (actual - expected)

    # Ensure Elo doesn't become NaN/Inf after update
    new_elo = player_elo + delta
    if math.isnan(new_elo) or math.isinf(new_elo):
        return player_elo  # Return previous Elo if update results in invalid number
    return new_elo


def calcular_elo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates Elo ratings using standard update logic with Polars.
    Iterates once through matches chronologically to handle state dependency.
    """
    print("Calculando elo (Polars Standard Logic)")

    # Ensure DataFrame is Polars
    if not isinstance(df, pl.DataFrame):
        df = pl.from_pandas(df)  # Convert if input is pandas

    # 1. Prepare data: Select necessary columns, sort, add unique order
    df_sorted = df.with_row_index("original_order")

    # 2. Melt to long format (one row per player per match)
    winners = df_sorted.select(
        [
            pl.col("original_order"),
            "tourney_date",
            "match_num",
            pl.col("winner_id").alias("player_id"),
            pl.col("loser_id").alias("opponent_id"),
            pl.lit(True).alias("won"),
        ]
    )
    losers = df_sorted.select(
        [
            pl.col("original_order"),
            "tourney_date",
            "match_num",
            pl.col("loser_id").alias("player_id"),
            pl.col("winner_id").alias("opponent_id"),
            pl.lit(False).alias("won"),
        ]
    )

    # Combine and sort by the original match order to process chronologically
    matches_long = pl.concat([winners, losers]).sort("original_order")

    # 3. Calculate cumulative match count per player efficiently
    matches_long = matches_long.with_columns(
        # Count matches *before* the current one
        (pl.col("player_id").cum_count().over("player_id")).alias("player_match_count")
    )
    # 4. Iterate once to calculate Elo updates (managing state)
    elo_state = {}  # Dictionary: player_id -> current Elo rating
    results_list = []  # List to store results including pre-match Elo

    for match_dict in matches_long.iter_rows(named=True):
        player_id = match_dict["player_id"]
        opponent_id = match_dict["opponent_id"]

        # Get Elo ratings *before* the current match from state
        player_elo_before = elo_state.get(player_id, 1500.0)
        opponent_elo_before = elo_state.get(opponent_id, 1500.0)

        # Store pre-match Elo with the match data
        match_dict["player_elo_before"] = player_elo_before
        # We only need the player's pre-match elo for joining back later
        results_list.append(match_dict)

        # Calculate the player's Elo *after* the current match
        new_player_elo = _calculate_elo_update(
            player_elo=player_elo_before,
            opponent_elo=opponent_elo_before,
            player_won=match_dict["won"],
            player_match_count=match_dict["player_match_count"],
        )

        # Update the Elo state for this player for the *next* iteration
        elo_state[player_id] = new_player_elo

    # Convert the results list (with pre-match Elos) back to a DataFrame
    results_df = pl.DataFrame(results_list)

    # 5. Join pre-match Elos back to the original DataFrame structure

    # Select winner's pre-match Elo
    winner_elo_df = results_df.filter(pl.col("won") == True).select(
        pl.col("original_order"), pl.col("player_elo_before").alias("winner_elo")
    )

    # Select loser's pre-match Elo
    loser_elo_df = results_df.filter(pl.col("won") == False).select(
        pl.col("original_order"), pl.col("player_elo_before").alias("loser_elo")
    )

    # Join back to the original sorted DataFrame (df_sorted)
    final_df = df_sorted.join(winner_elo_df, on="original_order", how="left").join(
        loser_elo_df, on="original_order", how="left"
    )

    # Calculate Elo difference and clean up
    final_df = final_df.with_columns(
        (pl.col("winner_elo") - pl.col("loser_elo")).alias("elo_diff")
    ).drop("original_order")  # Remove the temporary ordering column

    return final_df.to_pandas()


def calcular_h2h(df: pl.DataFrame) -> pd.DataFrame:
    """
    Para cada partida, calcula o histórico de confrontos entre os jogadores (Polars version).
    h2h = (vitórias do winner_id contra loser_id) - (vitórias do loser_id contra winner_id) nos encontros anteriores.
    O resultado é apresentado da perspectiva do winner_id da partida atual.
    """
    print("Calculando h2h (Polars)")

    if not isinstance(df, pl.DataFrame):
        df = pl.from_pandas(df)

    # Ensure DataFrame is sorted for chronological processing of encounters
    df_sorted = df.sort(["tourney_date", "match_num"]).with_row_index("original_order")

    h2h_results = []
    # State: encounter_state stores (p1_id, p2_id) -> {'p1_wins': count, 'p2_wins': count}
    # where p1_id < p2_id, representing wins of p1 vs p2 and p2 vs p1 respectively.
    encounter_state = {}

    for row_dict in df_sorted.iter_rows(named=True):
        winner_id = row_dict["winner_id"]
        loser_id = row_dict["loser_id"]
        current_original_order = row_dict["original_order"]

        # Direct callers may pass unnormalized source data. Rows without two
        # player IDs cannot participate in a head-to-head history.
        if (
            winner_id is None
            or loser_id is None
            or pd.isna(winner_id)
            or pd.isna(loser_id)
            or str(winner_id).strip() == ""
            or str(loser_id).strip() == ""
        ):
            h2h_results.append({"original_order": current_original_order, "h2h": 0})
            continue

        # Key for state dictionary (order-independent: p1_id is always the smaller ID)
        p1_id_key = min(winner_id, loser_id)
        p2_id_key = max(winner_id, loser_id)
        pair_key = (p1_id_key, p2_id_key)

        # Get previous wins for this pair from state
        # These are counts *before* the current match
        history = encounter_state.get(pair_key, {"p1_wins": 0, "p2_wins": 0})
        previous_wins_for_p1_key = history["p1_wins"]
        previous_wins_for_p2_key = history["p2_wins"]

        # Calculate H2H for the current match from the current winner's perspective,
        # based on encounters *before* this match.
        h2h_value = 0
        if winner_id == p1_id_key:  # Current winner is p1_id_key
            h2h_value = previous_wins_for_p1_key - previous_wins_for_p2_key
        else:  # Current winner is p2_id_key
            h2h_value = previous_wins_for_p2_key - previous_wins_for_p1_key

        h2h_results.append({"original_order": current_original_order, "h2h": h2h_value})

        # Update state with the outcome of the current match for future calculations
        updated_history = history.copy()
        if winner_id == p1_id_key:
            updated_history["p1_wins"] += 1
        else:  # winner_id == p2_id_key
            updated_history["p2_wins"] += 1
        encounter_state[pair_key] = updated_history

    h2h_df = pl.DataFrame(h2h_results, schema={"original_order": pl.UInt32, "h2h": pl.Int64})

    final_df = df_sorted.join(h2h_df, on="original_order", how="left")

    # Fill nulls if any match didn't get an h2h value (should not happen with this logic)
    # and ensure the column type is correct.
    final_df = final_df.with_columns(pl.col("h2h").fill_null(0).cast(pl.Int64))

    final_df = final_df.drop("original_order")

    return final_df.to_pandas()
