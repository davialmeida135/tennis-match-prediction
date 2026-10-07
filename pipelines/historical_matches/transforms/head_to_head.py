"""Pre-match head-to-head win differences in shared chronological order."""

import pandas as pd
import polars as pl

from .history import order_history


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
    source = df.to_pandas()
    if "score" in source:
        source = source.loc[source["score"].ne("W/O").fillna(True)]
    df_sorted = pl.from_pandas(order_history(source).reset_index(drop=True)).with_row_index(
        "original_order"
    )

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
