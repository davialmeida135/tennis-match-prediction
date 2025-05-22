import pandas as pd
from etl.kaggle.util import _get_previous_matches
import math
import polars as pl
#from prefect import flow, task


def _calculate_elo_update(player_elo, opponent_elo, player_won, player_match_count, tourney_level):
    if not player_won:
        expected = 1 / (1 + 10**((opponent_elo - player_elo) / 400))
    else:
        expected = 1 / (1 + 10**((player_elo - opponent_elo) / 400))
    actual = 1.0 if player_won else 0.0
    
    # K-factor calculation (as in the original logic)
    k_factor = 250 / ((player_match_count + 5) ** 0.4) 
    
    # Tournament level multiplier (as in the original logic)
    #level_mult = 1.1 if tourney_level == 'G' else 1.0 # Ensure tourney_level column/values match this
    level_mult = 1
    # Handle potential NaN/Inf from extreme Elo differences (though less likely with standard init)
    if math.isnan(expected) or math.isinf(expected):
        expected = 0.5 # Fallback if calculation fails

    delta = level_mult * k_factor * (actual - expected)
    
    # Ensure Elo doesn't become NaN/Inf after update
    new_elo = player_elo + delta
    if math.isnan(new_elo) or math.isinf(new_elo):
        return player_elo # Return previous Elo if update results in invalid number
    return new_elo

def calcular_elo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates Elo ratings using standard update logic with Polars.
    Iterates once through matches chronologically to handle state dependency.
    """
    print("Calculando elo (Polars Standard Logic)")

    # Ensure DataFrame is Polars
    if not isinstance(df, pl.DataFrame):
        df = pl.from_pandas(df) # Convert if input is pandas

    # 1. Prepare data: Select necessary columns, sort, add unique order
    df_sorted = df.select([
        "tourney_date", "match_num", "tourney_level", # Ensure these columns exist
        "winner_id", "loser_id", "winner_name", "loser_name" # Ensure these columns exist
    ]).with_row_index("original_order")

    # 2. Melt to long format (one row per player per match)
    winners = df_sorted.select([
        pl.col("original_order"), "tourney_date", "match_num", "tourney_level",
        pl.col("winner_id").alias("player_id"),
        pl.col("loser_id").alias("opponent_id"),
        pl.col("winner_name").alias("player_name"), # Keep name if needed later
        pl.lit(True).alias("won")
    ])
    losers = df_sorted.select([
        pl.col("original_order"), "tourney_date", "match_num", "tourney_level",
        pl.col("loser_id").alias("player_id"),
        pl.col("winner_id").alias("opponent_id"),
        pl.col("loser_name").alias("player_name"), # Keep name if needed later
        pl.lit(False).alias("won")
    ])

    # Combine and sort by the original match order to process chronologically
    matches_long = pl.concat([winners, losers]).sort("original_order")

    # 3. Calculate cumulative match count per player efficiently
    matches_long = matches_long.with_columns(
        # Count matches *before* the current one
        (pl.col("player_id").cum_count().over("player_id")).alias("player_match_count")
    )
    matches_long.write_csv("dados_tratados/matches_long.csv")
    # 4. Iterate once to calculate Elo updates (managing state)
    elo_state = {} # Dictionary: player_id -> current Elo rating
    results_list = [] # List to store results including pre-match Elo

    for match_dict in matches_long.iter_rows(named=True):
        player_id = match_dict['player_id']
        opponent_id = match_dict['opponent_id']

        # Get Elo ratings *before* the current match from state
        player_elo_before = elo_state.get(player_id, 1500.0)
        opponent_elo_before = elo_state.get(opponent_id, 1500.0)

        # Store pre-match Elo with the match data
        match_dict['player_elo_before'] = player_elo_before
        # We only need the player's pre-match elo for joining back later
        results_list.append(match_dict) 

        # Calculate the player's Elo *after* the current match
        new_player_elo = _calculate_elo_update(
            player_elo=player_elo_before,
            opponent_elo=opponent_elo_before,
            player_won=match_dict['won'],
            player_match_count=match_dict['player_match_count'],
            tourney_level=match_dict['tourney_level']
        )

        # Update the Elo state for this player for the *next* iteration
        elo_state[player_id] = new_player_elo

    # Convert the results list (with pre-match Elos) back to a DataFrame
    results_df = pl.DataFrame(results_list)

    # 5. Join pre-match Elos back to the original DataFrame structure
    
    # Select winner's pre-match Elo
    winner_elo_df = results_df.filter(pl.col("won")==True).select(
        pl.col("original_order"),
        pl.col("player_elo_before").alias("winner_elo")
    )
    
    # Select loser's pre-match Elo
    loser_elo_df = results_df.filter(pl.col("won")==False).select(
        pl.col("original_order"),
        pl.col("player_elo_before").alias("loser_elo")
    )

    # Join back to the original sorted DataFrame (df_sorted)
    final_df = df_sorted.join(
        winner_elo_df, on="original_order", how="left"
    ).join(
        loser_elo_df, on="original_order", how="left"
    )

    # Calculate Elo difference and clean up
    final_df = final_df.with_columns(
        (pl.col("winner_elo") - pl.col("loser_elo")).alias("elo_diff")
    ).drop("original_order") # Remove the temporary ordering column

    # Join back any columns from the original df that were dropped
    # Assuming 'tourney_date', 'match_num', 'winner_id', 'loser_id' are keys
    original_cols_to_keep = [col for col in df.columns if col not in final_df.columns] + ["tourney_date", "match_num", "winner_id", "loser_id"]
    if original_cols_to_keep:
        final_df = df.select(original_cols_to_keep).join(
            final_df, on=["tourney_date", "match_num", "winner_id", "loser_id"], how="left"
        )

    return final_df.to_pandas()

def calcular_elo_superficies(df:pl.DataFrame)->pl.DataFrame:
    """
    Soma um ponto para cada vitória do jogador e subtrai um ponto para cada derrota em cada superfície.
    Um jogador novo deve iniciar com 1500 pontos.
    """
    print("Calculando elo (Polars Standard Logic)")

    # Ensure DataFrame is Polars
    if not isinstance(df, pl.DataFrame):
        df = pl.from_pandas(df) # Convert if input is pandas

    # 1. Prepare data: Select necessary columns, sort, add unique order
    df_sorted = df.select([
        "tourney_date", "match_num", "tourney_level", # Ensure these columns exist
        "winner_id", "loser_id", "winner_name", "loser_name", "surface" # Ensure these columns exist
    ]).with_row_index("original_order")

    # 2. Melt to long format (one row per player per match)
    winners = df_sorted.select([
        pl.col("original_order"), "tourney_date", "match_num", "tourney_level", "surface",
        pl.col("winner_id").alias("player_id"),
        pl.col("loser_id").alias("opponent_id"),
        pl.col("winner_name").alias("player_name"), # Keep name if needed later
        pl.lit(True).alias("won")
    ])
    losers = df_sorted.select([
        pl.col("original_order"), "tourney_date", "match_num", "tourney_level", "surface",
        pl.col("loser_id").alias("player_id"),
        pl.col("winner_id").alias("opponent_id"),
        pl.col("loser_name").alias("player_name"), # Keep name if needed later
        pl.lit(False).alias("won")
    ])

    # Combine and sort by the original match order to process chronologically
    matches_long = pl.concat([winners, losers]).sort("original_order")

    # 3. Calculate cumulative match count per player efficiently
    matches_long = matches_long.with_columns(
        # Count matches *before* the current one
        (pl.col("player_id").cum_count().over("player_id")).alias("player_match_count")
    )
    elo_state = {}
    results_list = [] # List to store results including pre-match Elo
    for match_dict in matches_long.iter_rows(named=True):
        #print('i')
        player_id = match_dict['player_id']
        opponent_id = match_dict['opponent_id']
        surface = match_dict['surface']

        # Create keys for the dictionary lookup
        player_key = (player_id, surface.lower())
        opponent_key = (opponent_id, surface.lower())

        # Get Elo ratings *before* the current match from state using dict.get()
        player_elo_before = elo_state.get(player_key, 1500.0)
        opponent_elo_before = elo_state.get(opponent_key, 1500.0)

        # Store pre-match Elo with the match data
        match_dict['player_elo_before'] = player_elo_before
        # We only need the player's pre-match elo for joining back later
        results_list.append(match_dict)    

        # Calculate the player's Elo *after* the current match
        new_player_elo = _calculate_elo_update(
            player_elo=player_elo_before,
            opponent_elo=opponent_elo_before,
            player_won=match_dict['won'],
            player_match_count=match_dict['player_match_count'],
            tourney_level=match_dict['tourney_level']
        )

        # Update the Elo state for this player for the *next* iteration
        elo_state[player_key] = new_player_elo


    # Convert the results list (with pre-match Elos) back to a DataFrame
    results_df = pl.DataFrame(results_list)

    # 5. Join pre-match Elos back to the original DataFrame structure
    
    # Select winner's pre-match Elo
    winner_elo_df = results_df.filter(pl.col("won")==True).select(
        pl.col("original_order"),
        pl.col("player_elo_before").alias("winner_surface_elo")
    )
    
    # Select loser's pre-match Elo
    loser_elo_df = results_df.filter(pl.col("won")==False).select(
        pl.col("original_order"),
        pl.col("player_elo_before").alias("loser_surface_elo")
    )

    # Join back to the original sorted DataFrame (df_sorted)
    final_df = df_sorted.join(
        winner_elo_df, on="original_order", how="left"
    ).join(
        loser_elo_df, on="original_order", how="left"
    )

    # Calculate Elo difference and clean up
    final_df = final_df.with_columns(
        (pl.col("winner_surface_elo") - pl.col("loser_surface_elo")).alias("surface_elo_diff")
    ).drop("original_order") # Remove the temporary ordering column

    # Join back any columns from the original df that were dropped
    # Assuming 'tourney_date', 'match_num', 'winner_id', 'loser_id' are keys
    original_cols_to_keep = [col for col in df.columns if col not in final_df.columns] + ["tourney_date", "match_num", "winner_id", "loser_id"]
    if original_cols_to_keep:
        final_df = df.select(original_cols_to_keep).join(
            final_df, on=["tourney_date", "match_num", "winner_id", "loser_id"], how="left"
        )

    return final_df

def calcular_h2h(df: pl.DataFrame) -> pd.DataFrame:
    '''
    Para cada partida, calcula o histórico de confrontos entre os jogadores (Polars version).
    h2h = (vitórias do winner_id contra loser_id) - (vitórias do loser_id contra winner_id) nos encontros anteriores.
    O resultado é apresentado da perspectiva do winner_id da partida atual.
    '''
    print("Calculando h2h (Polars)")

    if not isinstance(df, pl.DataFrame):
        try:
            df = pl.from_pandas(df)
        except Exception as e:
            raise TypeError(f"Input must be a Polars or Pandas DataFrame. Conversion failed: {e}")

    # Ensure DataFrame is sorted for chronological processing of encounters
    df_sorted = df.sort(["tourney_date", "match_num"]).with_row_index("original_order")

    h2h_results = []
    # State: encounter_state stores (p1_id, p2_id) -> {'p1_wins': count, 'p2_wins': count}
    # where p1_id < p2_id, representing wins of p1 vs p2 and p2 vs p1 respectively.
    encounter_state = {}

    for row_dict in df_sorted.iter_rows(named=True):
        winner_id = row_dict['winner_id']
        loser_id = row_dict['loser_id']
        current_original_order = row_dict['original_order']

        # Key for state dictionary (order-independent: p1_id is always the smaller ID)
        p1_id_key = min(winner_id, loser_id)
        p2_id_key = max(winner_id, loser_id)
        pair_key = (p1_id_key, p2_id_key)

        # Get previous wins for this pair from state
        # These are counts *before* the current match
        history = encounter_state.get(pair_key, {'p1_wins': 0, 'p2_wins': 0})
        previous_wins_for_p1_key = history['p1_wins']
        previous_wins_for_p2_key = history['p2_wins']

        # Calculate H2H for the current match from the current winner's perspective,
        # based on encounters *before* this match.
        h2h_value = 0
        if winner_id == p1_id_key: # Current winner is p1_id_key
            h2h_value = previous_wins_for_p1_key - previous_wins_for_p2_key
        else: # Current winner is p2_id_key
            h2h_value = previous_wins_for_p2_key - previous_wins_for_p1_key
        
        h2h_results.append({
            "original_order": current_original_order,
            "h2h": h2h_value
        })

        # Update state with the outcome of the current match for future calculations
        updated_history = history.copy()
        if winner_id == p1_id_key:
            updated_history['p1_wins'] += 1
        else: # winner_id == p2_id_key
            updated_history['p2_wins'] += 1
        encounter_state[pair_key] = updated_history

    if not h2h_results: # Handle empty input df
        # Add an empty 'h2h' column if df is not empty, otherwise return df
        if df_sorted.height > 0:
             return df_sorted.with_columns(pl.lit(0).cast(pl.Int64).alias("h2h")).drop("original_order").to_pandas()
        else:
             return df_sorted.drop("original_order", errors='ignore').to_pandas()


    h2h_df = pl.DataFrame(h2h_results, schema={"original_order": pl.UInt32, "h2h": pl.Int64})

    final_df = df_sorted.join(h2h_df, on="original_order", how="left")
    
    # Fill nulls if any match didn't get an h2h value (should not happen with this logic)
    # and ensure the column type is correct.
    final_df = final_df.with_columns(pl.col("h2h").fill_null(0).cast(pl.Int64)) 
    
    final_df = final_df.drop("original_order")

    return final_df.to_pandas()

def calcular_partidas_jogadas(df:pd.DataFrame)->pd.DataFrame:
    """
    Calcula o número de partidas jogadas por cada jogador
    """
    # FIXME: This function is not working properly with Polars
    print("Calculando partidas jogadas")
    df['winner_matches_played'] = 0
    df['loser_matches_played'] = 0

    for index, row in df.iterrows():
        
        # Get previous matches for winner
        winner_id = row['winner_id']
        loser_id = row['loser_id']

        winner_matches, loser_matches = _get_previous_matches(df, row)
        if len(winner_matches) > 0:
            df.loc[index, 'winner_matches_played'] = len(winner_matches)
        
        # Get previous matches for loser
        if len(loser_matches) > 0:
            df.loc[index, 'loser_matches_played'] = len(loser_matches)

    return df

def calcular_partidas_jogadas_ultimo_mes(df:pd.DataFrame)->pd.DataFrame:
    """Calculate matches played in the last month"""
    print("Calculando partidas jogadas no último mês")
    df['winner_matches_played_last_month'] = 0
    df['loser_matches_played_last_month'] = 0

    
    for index, row in df.iterrows():
        tourney_date = row['tourney_date']
        one_month_ago = tourney_date - pd.Timedelta(days=30)
        
        winner_matches, loser_matches = _get_previous_matches(df, row)
        
        if len(winner_matches) > 0:
            # This comparison works perfectly with datetime objects
            recent_matches = winner_matches[winner_matches['tourney_date'] >= one_month_ago]
            df.loc[index, 'winner_matches_played_last_month'] += len(recent_matches)
        
        if len(loser_matches) > 0:
            recent_matches = loser_matches[loser_matches['tourney_date'] >= one_month_ago]
            df.loc[index, 'loser_matches_played_last_month'] += len(recent_matches)
    
    return df

def calcular_round_semana_passada(df:pd.DataFrame)->pd.DataFrame:
    """
    Calcula o round da semana passada para cada jogador
    """
    print("Calculando round da semana passada")
    df['winner_round_last_week'] = 0
    df['loser_round_last_week'] = 0

    for index, row in df.iterrows():
        tourney_date = row['tourney_date']
        one_week_ago = tourney_date - pd.Timedelta(days=7)
        
        winner_matches, loser_matches = _get_previous_matches(df, row)
        
        recent_matches = winner_matches[winner_matches['tourney_date'] == one_week_ago]
        if len(recent_matches) > 0:
            df.loc[index, 'winner_round_last_week'] = recent_matches.iloc[-1]['round']
        
        recent_matches = loser_matches[loser_matches['tourney_date'] == one_week_ago]
        if len(recent_matches) > 0:
            df.loc[index, 'loser_round_last_week'] = recent_matches.iloc[-1]['round']
    
    return df

def calcular_minutos_acumulados_torneio(df):
    """
    Calcula para todas as partidas, o tempo jogado por cada jogador no torneio antes da partida atual
    """
    rows_list = []
    for index, row in df.iterrows():
        tourney = df[df['tourney_id']==row['tourney_id']]
        new_row = _calcular_carga_previa_jogadores(row,tourney)
        #print(new_row)
        if new_row is not None:
            rows_list.append(new_row)

    new_df = pd.DataFrame(rows_list)
    #new_df.to_csv("dados_tratados/atp_matches_2017_tempo_jogado.csv", index=False)
    return new_df

def _calcular_carga_previa_jogadores(row, df):
    """
    Recebe uma partida e um DataFrame com todas as partidas
    do torneio da partida.
    Calcula o tempo jogado por cada jogador no torneio antes 
    da partida atual
    """
    
    t_round = row['round']
    player1 = row['winner_id']
    player2 = row['loser_id'] 
    row['winner_tournament_minutes'] = 0
    row['loser_tournament_minutes'] = 0

    round_order = ['R128', 'R64', 'R32', 'R16', 'QF', 'SF', 'F']
    if t_round not in round_order:
        return row
    
    round_index = round_order.index(t_round)

    if round_index == 0:
        return row
    
    for i in range(0,round_index):
        round_name = round_order[i]
        round_matches = df[df['round']==round_name]
        #print(f"Looking at round {round_name} for tournament {tourney}")
        if len(round_matches) == 0:
            continue
        
        match_p1 = round_matches[round_matches['winner_id']==player1]
        match_p2 = round_matches[round_matches['winner_id']==player2]

        if len(match_p1) == 1:
            #print(match_p1[['winner_name', 'tourney_name', 'round', 'minutes']])
            row['winner_tournament_minutes'] += match_p1['minutes'].values[0]
        if len(match_p2) == 1:
            #print(match_p2[['winner_name', 'tourney_name', 'round', 'minutes']])
            row['loser_tournament_minutes'] += match_p2['minutes'].values[0]

    #print(row[['player1_tournament_minutes','player2_tournament_minutes', 'winner_name', 'loser_name']])
    return row 

def main():
    #df_processed = calcular_round_semana_passada(df)
    # df = pd.read_csv("dados_tratados/all_atp_matches2.csv", parse_dates=['tourney_date'])
    # #df = df[df['tourney_date'] >= '1990-01-01']
    # df_processed = calcular_elo(df)
    # df_processed.to_csv("dados_tratados/teste_stats.csv", index=False)
    df_pl = pl.read_csv("dados_tratados/all_atp_matches2.csv", 
                        try_parse_dates=True,
                        schema_overrides={"winner_seed": pl.Utf8, 
                                        "loser_seed": pl.Utf8} ) # Read directly
    
    # Ensure necessary columns exist before calling
    required_cols = ["tourney_date", "match_num", "tourney_level", "winner_id", "loser_id", "winner_name", "loser_name"]
    if not all(col in df_pl.columns for col in required_cols):
         print(f"Error: Missing one or more required columns: {required_cols}")
         return 

    #df_processed = calcular_elo(df_pl) # Call the Polars version
    df_processed = calcular_elo_superficies(df_pl) # Call the Polars version
    df_processed.write_csv("dados_tratados/teste_surf_stats_polars.csv") # Save the result



if __name__ == "__main__":
    main()
