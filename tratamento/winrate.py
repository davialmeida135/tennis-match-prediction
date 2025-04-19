import pandas as pd
import polars as pl

from util import _get_previous_matches

def calcular_winrate_total(df:pl.DataFrame)->pl.DataFrame:
    """
    Calculate total winrate for each player before each match
    """
    # Ensure DataFrame is Polars and sorted chronologically
    if not isinstance(df, pl.DataFrame):
        df = pl.from_pandas(df)
    
    # --- Essential Columns Check ---
    required_cols = ["tourney_date", "match_num", "winner_id", "loser_id"]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"DataFrame missing one or more required columns: {required_cols}")
    
    df_sorted = df.with_row_index("original_order")

    # Uma linha para cada jogador, com o resultado da partida
    # Se o jogador ganhou, won = 1, se perdeu, won = 0
    winners = df_sorted.select([
        pl.col("original_order"),
        pl.col("winner_id").alias("player_id"),
        pl.lit(1).alias("won") 
    ])
    losers = df_sorted.select([
        pl.col("original_order"),
        pl.col("loser_id").alias("player_id"),
        pl.lit(0).alias("won") 
    ])
    matches_long = pl.concat([winners, losers]).sort("original_order")

    # Cria 5 colunas: cumulative_wins, cumulative_matches, prev_wins, prev_matches e player_winrate_before
    matches_long = matches_long.with_columns([ # Contar quantas vitórias e quantas partidas o jogador tem
        pl.col("won").cum_sum().over("player_id").alias("cumulative_wins"),
        pl.col("player_id").cum_count().over("player_id").alias("cumulative_matches")
    ]).with_columns([ # Cria as colunas de vitórias e partidas anteriores
        pl.col("cumulative_wins").shift(1).over("player_id").fill_null(0).alias("prev_wins"),
        pl.col("cumulative_matches").shift(1).over("player_id").fill_null(0).alias("prev_matches")
    ]).with_columns( # Se o jogador não teve partidas anteriores, o winrate é 0.0
        pl.when(pl.col("prev_matches") > 0)
        .then(pl.col("prev_wins") / pl.col("prev_matches"))
        .otherwise(0.0) 
        .alias("player_winrate_before")
    )

    # Filtra as colunas que não são necessárias
    winner_winrate_df = matches_long.filter(pl.col("won") == 1).select(
        pl.col("original_order"),
        pl.col("player_winrate_before").alias("winner_winrate") # Creates the column
    )
    loser_winrate_df = matches_long.filter(pl.col("won") == 0).select(
        pl.col("original_order"),
        pl.col("player_winrate_before").alias("loser_winrate") # Creates the column
    )

    # Coloca as colunas de winrate no DataFrame original
    final_df = df_sorted.join(
        winner_winrate_df, on="original_order", how="left"
    ).join(
        loser_winrate_df, on="original_order", how="left"
    ).drop("original_order")

    return final_df


def calcular_winrate_ultimas_n(df:pd.DataFrame, n=50)->pd.DataFrame:
    """
    Calculate winrate for each player in their last n matches before each match
    """
    print(f"Calculando winrate para cada jogador nas últimas {n} partidas")

    column_winner = f'winner_winrate_last_{n}'
    column_loser = f'loser_winrate_last_{n}'
    
    df[column_winner] = 0.0
    df[column_loser] = 0.0
    
    for index, row in df.iterrows():
        winner_id = row['winner_id']
        loser_id = row['loser_id']
        tourney_date = row['tourney_date']
        
        # Get previous matches for winner
        winner_matches,loser_matches = _get_previous_matches(df, row)
        if len(winner_matches) > 0:
            winner_matches = winner_matches.tail(n)
            winner_wins = len(winner_matches[winner_matches['winner_id'] == winner_id])
            winner_winrate = winner_wins / len(winner_matches)
            df.loc[index, column_winner] = winner_winrate
        
        # Get previous matches for loser
        if len(loser_matches) > 0:
            loser_matches = loser_matches.tail(n)
            loser_wins = len(loser_matches[loser_matches['winner_id'] == loser_id])
            loser_winrate = loser_wins / len(loser_matches)
            df.loc[index, column_loser] = loser_winrate
    
    return df


def calcular_winrate_superficie(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate winrate for each player on a specific surface before each match
    """
    print("Calculando winrate para cada jogador em cada superficie")

    column_winner = 'winner_winrate_surface'
    column_loser = 'loser_winrate_surface'
    
    df[column_winner] = 0.0
    df[column_loser] = 0.0
    
    for index, row in df.iterrows():
        winner_id = row['winner_id']
        loser_id = row['loser_id']
        tourney_date = row['tourney_date']
        surface = row['surface']
        
        # Get previous matches
        winner_matches,loser_matches = _get_previous_matches(df, row)

        winner_matches = winner_matches[winner_matches['surface'] == surface]
            
        if len(winner_matches) > 0:
            winner_wins = len(winner_matches[winner_matches['winner_id'] == winner_id])
            winner_winrate = winner_wins / len(winner_matches)
            df.loc[index, column_winner] = winner_winrate
        
        loser_matches = loser_matches[loser_matches['surface'] == surface]
            
        if len(loser_matches) > 0:
            loser_wins = len(loser_matches[loser_matches['winner_id'] == loser_id])
            loser_winrate = loser_wins / len(loser_matches)
            df.loc[index, column_loser] = loser_winrate
    
    return df


def calcular_winrate_superficie_ultimas_n(df:pd.DataFrame, n=50)->pd.DataFrame:
    """
    Calculate winrate for each player on a specific surface in their last n matches before each match
    """
    print(f"Calculando winrate para cada jogador em cada superficie nas últimas {n} partidas")	 
    column_winner = 'winner_winrate_surface'
    column_loser = 'loser_winrate_surface'
    
    df[column_winner] = 0.0
    df[column_loser] = 0.0
    
    for index, row in df.iterrows():
        winner_id = row['winner_id']
        loser_id = row['loser_id']
        tourney_date = row['tourney_date']
        surface = row['surface']
        
        # Get previous matches for winner
        winner_matches,loser_matches = _get_previous_matches(df, row)
        
        winner_matches = winner_matches[winner_matches['surface'] == surface]
        
        if len(winner_matches) > 0:
            winner_matches = winner_matches.tail(n)
            winner_wins = len(winner_matches[winner_matches['winner_id'] == winner_id])
            winner_winrate = winner_wins / len(winner_matches)
            df.loc[index, column_winner] = winner_winrate
        
        # Get previous matches for loser
        loser_matches = loser_matches[loser_matches['surface'] == surface]
            
        if len(loser_matches) > 0:
            loser_matches = loser_matches.tail(n)
            loser_wins = len(loser_matches[loser_matches['winner_id'] == loser_id])
            loser_winrate = loser_wins / len(loser_matches)
            df.loc[index, column_loser] = loser_winrate
    
    return df


def calcular_winrate_torneio(df):
    """
    Calculate winrate for each player in a specific tournament before each match
    """
    print("Calculando winrate para cada jogador em cada torneio")
    df['winner_winrate_tournament'] = 0.0
    df['loser_winrate_tournament'] = 0.0
    
    for index, row in df.iterrows():
        winner_id = row['winner_id']
        loser_id = row['loser_id']
        tourney_date = row['tourney_date']
        tourney_id = row['tourney_id']
        
        winner_matches,loser_matches = _get_previous_matches(df, row)
        # Get previous tournament matches for winner
        winner_matches = winner_matches[winner_matches['tourney_id'] == tourney_id]
            
        if len(winner_matches) > 0:
            winner_wins = len(winner_matches[winner_matches['winner_id'] == winner_id])
            winner_winrate = winner_wins / len(winner_matches)
            df.loc[index, 'winner_winrate_tournament'] = winner_winrate
        
        # Get previous tournament matches for loser
        loser_matches = loser_matches[loser_matches['tourney_id'] == tourney_id]
            
        if len(loser_matches) > 0:
            loser_wins = len(loser_matches[loser_matches['winner_id'] == loser_id])
            loser_winrate = loser_wins / len(loser_matches)
            df.loc[index, 'loser_winrate_tournament'] = loser_winrate
    
    return df


def calcular_todas_winrates(df):
    """
    Calculate all winrate statistics for players
    """
    df = calcular_winrate_total(df)
    # df = calcular_winrate_ultimas_n(df, n=50) 
    # df = calcular_winrate_ultimas_n(df, n=10)    
    # df = calcular_winrate_superficie(df)
    # df = calcular_winrate_superficie_ultimas_n(df, n=50)
    # df = calcular_winrate_superficie_ultimas_n(df, n=10)
    # df = calcular_winrate_torneio(df)
    
    return df


if __name__ == "__main__":
    # Test the functions
    #df = pd.read_csv("../dataset/tennis_atp/atp_matches_2023.csv")
    df = pl.read_csv("dados_tratados/all_atp_matches2.csv", schema_overrides={'loser_seed': str, 'winner_seed': str})
    df_processed = calcular_todas_winrates(df)
    df_processed.write_csv("dados_tratados/winrate_stats.csv")