import pandas as pd

import polars as pl
import datetime

# ... existing pandas code ...

def _get_previous_matches(df: pl.DataFrame, winner_id: int, loser_id: int, date, match_num: int) -> list[pl.DataFrame]:
    """
    Retorna todas as partidas de um jogador antes de uma partida específica, usando Polars.
    Assumes df is sorted by tourney_date and match_num.
    """
    # Filter for matches involving the winner that occurred before the current match
    winner_matches = df.filter(
        ((pl.col("winner_id") == winner_id) | (pl.col("loser_id") == winner_id)) 
    )
    print(len(winner_matches))
    winner_matches = winner_matches.filter(
        ((pl.col("tourney_date") < date) | ((pl.col("tourney_date") == date) & (pl.col("match_num") < match_num)))
    )
    # Filter for matches involving the loser that occurred before the current match
    loser_matches = df.filter(
        ((pl.col("winner_id") == loser_id) | (pl.col("loser_id") == loser_id)) &
        ((pl.col("tourney_date") < date) | ((pl.col("tourney_date") == date) & (pl.col("match_num") < match_num)))
    )

    return [winner_matches, loser_matches]

def _get_previous_encounters(df: pl.DataFrame, player1_id: int, player2_id: int, date: datetime.date, match_num: int) -> pl.DataFrame:
    """
    Retorna todas as partidas entre dois jogadores antes de uma partida específica, usando Polars.
    Assumes df is sorted by tourney_date and match_num.
    """
    encounters = df.filter(
        (
            ((pl.col("winner_id") == player1_id) & (pl.col("loser_id") == player2_id)) |
            ((pl.col("winner_id") == player2_id) & (pl.col("loser_id") == player1_id))
        ) &
        (
            (pl.col("tourney_date") < date) |
            ((pl.col("tourney_date") == date) & (pl.col("match_num") < match_num))
        )
    )
    # Sorting might not be strictly necessary here if df is already sorted,
    # but it ensures chronological order within the result.
    return encounters.sort("tourney_date", "match_num")

if __name__ == '__main__':
    
    # df = pd.read_csv("dados_tratados/initial_clean.csv", parse_dates=['tourney_date'])
    # for index, row in df.iterrows():
    #     w,l = _get_previous_matches(df,row)
    #     print("Len: ",len(w))

    df_pl = pl.read_csv("dados_tratados/initial_clean.csv", 
                        try_parse_dates=True,
                        schema_overrides={"winner_seed": pl.Utf8, 
                                        "loser_seed": pl.Utf8} ) 
    
    print(df_pl.get_column("tourney_date").dtype)
    winner,loser = _get_previous_matches(df_pl, 211663, 105430, datetime.date(2024,10,10), 289)
    print(winner)