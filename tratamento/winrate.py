import pandas as pd
from util import _get_previous_matches


def calcular_winrate_total(df):
    """
    Calculate total winrate for each player before each match
    """
    df['winner_winrate'] = 0.0
    df['loser_winrate'] = 0.0
    
    for index, row in df.iterrows():
        winner_id = row['winner_id']
        loser_id = row['loser_id']
        tourney_date = row['tourney_date']
        
        # Get previous matches for winner
        winner_matches = _get_previous_matches(df, winner_id, tourney_date)
        if len(winner_matches) > 0:
            winner_wins = len(winner_matches[winner_matches['winner_id'] == winner_id])
            winner_winrate = winner_wins / len(winner_matches)
            df.loc[index, 'winner_winrate'] = winner_winrate
        
        # Get previous matches for loser
        loser_matches = _get_previous_matches(df, loser_id, tourney_date)
        if len(loser_matches) > 0:
            loser_wins = len(loser_matches[loser_matches['winner_id'] == loser_id])
            loser_winrate = loser_wins / len(loser_matches)
            df.loc[index, 'loser_winrate'] = loser_winrate
    
    return df


def calcular_winrate_ultimas_n(df, n=50):
    """
    Calculate winrate for each player in their last n matches before each match
    """
    column_winner = f'winner_winrate_last_{n}'
    column_loser = f'loser_winrate_last_{n}'
    
    df[column_winner] = 0.0
    df[column_loser] = 0.0
    
    for index, row in df.iterrows():
        winner_id = row['winner_id']
        loser_id = row['loser_id']
        tourney_date = row['tourney_date']
        
        # Get previous matches for winner
        winner_matches = _get_previous_matches(df, winner_id, tourney_date)
        if len(winner_matches) > 0:
            winner_matches = winner_matches.tail(n)
            winner_wins = len(winner_matches[winner_matches['winner_id'] == winner_id])
            winner_winrate = winner_wins / len(winner_matches)
            df.loc[index, column_winner] = winner_winrate
        
        # Get previous matches for loser
        loser_matches = _get_previous_matches(df, loser_id, tourney_date)
        if len(loser_matches) > 0:
            loser_matches = loser_matches.tail(n)
            loser_wins = len(loser_matches[loser_matches['winner_id'] == loser_id])
            loser_winrate = loser_wins / len(loser_matches)
            df.loc[index, column_loser] = loser_winrate
    
    return df


def calcular_winrate_superficie(df, superficie=None):
    """
    Calculate winrate for each player on a specific surface before each match
    """
    column_suffix = f"_{superficie}" if superficie else "_all_surfaces"
    column_winner = f'winner_winrate_surface{column_suffix}'
    column_loser = f'loser_winrate_surface{column_suffix}'
    
    df[column_winner] = 0.0
    df[column_loser] = 0.0
    
    for index, row in df.iterrows():
        winner_id = row['winner_id']
        loser_id = row['loser_id']
        tourney_date = row['tourney_date']
        
        # Get previous matches for winner
        winner_matches = _get_previous_matches(df, winner_id, tourney_date)
        if superficie:
            winner_matches = winner_matches[winner_matches['surface'] == superficie]
            
        if len(winner_matches) > 0:
            winner_wins = len(winner_matches[winner_matches['winner_id'] == winner_id])
            winner_winrate = winner_wins / len(winner_matches)
            df.loc[index, column_winner] = winner_winrate
        
        # Get previous matches for loser
        loser_matches = _get_previous_matches(df, loser_id, tourney_date)
        if superficie:
            loser_matches = loser_matches[loser_matches['surface'] == superficie]
            
        if len(loser_matches) > 0:
            loser_wins = len(loser_matches[loser_matches['winner_id'] == loser_id])
            loser_winrate = loser_wins / len(loser_matches)
            df.loc[index, column_loser] = loser_winrate
    
    return df


def calcular_winrate_superficie_ultimas_n(df, superficie=None, n=50):
    """
    Calculate winrate for each player on a specific surface in their last n matches before each match
    """
    surface_suffix = f"_{superficie}" if superficie else "_all_surfaces"
    column_suffix = f"{surface_suffix}_last_{n}"
    column_winner = f'winner_winrate_surface{column_suffix}'
    column_loser = f'loser_winrate_surface{column_suffix}'
    
    df[column_winner] = 0.0
    df[column_loser] = 0.0
    
    for index, row in df.iterrows():
        winner_id = row['winner_id']
        loser_id = row['loser_id']
        tourney_date = row['tourney_date']
        
        # Get previous matches for winner
        winner_matches = _get_previous_matches(df, winner_id, tourney_date)
        if superficie:
            winner_matches = winner_matches[winner_matches['surface'] == superficie]
        
        if len(winner_matches) > 0:
            winner_matches = winner_matches.tail(n)
            winner_wins = len(winner_matches[winner_matches['winner_id'] == winner_id])
            winner_winrate = winner_wins / len(winner_matches)
            df.loc[index, column_winner] = winner_winrate
        
        # Get previous matches for loser
        loser_matches = _get_previous_matches(df, loser_id, tourney_date)
        if superficie:
            loser_matches = loser_matches[loser_matches['surface'] == superficie]
            
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
    df['winner_winrate_tournament'] = 0.0
    df['loser_winrate_tournament'] = 0.0
    
    for index, row in df.iterrows():
        winner_id = row['winner_id']
        loser_id = row['loser_id']
        tourney_date = row['tourney_date']
        tourney_id = row['tourney_id']
        
        # Get previous tournament matches for winner
        winner_matches = _get_previous_matches(df, winner_id, tourney_date)
        winner_matches = winner_matches[winner_matches['tourney_id'] == tourney_id]
            
        if len(winner_matches) > 0:
            winner_wins = len(winner_matches[winner_matches['winner_id'] == winner_id])
            winner_winrate = winner_wins / len(winner_matches)
            df.loc[index, 'winner_winrate_tournament'] = winner_winrate
        
        # Get previous tournament matches for loser
        loser_matches = _get_previous_matches(df, loser_id, tourney_date)
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
    print("Calculating winrate statistics...")
    df = calcular_winrate_total(df)
    print("Calculating winrate statistics...")
    df = calcular_winrate_ultimas_n(df, n=50)
    print("Calculating winrate statistics...")
    df = calcular_winrate_ultimas_n(df, n=10)
    print("Calculating winrate statistics...")
    #df = calcular_winrate_superficie(df)  # All surfaces
    
    # Calculate for specific surfaces
    for superficie in df['surface'].unique():
        print("Calculating winrate statistics...")
        df = calcular_winrate_superficie(df, superficie)
        print("Calculating winrate statistics...")
        df = calcular_winrate_superficie_ultimas_n(df, superficie, n=50)
        print("Calculating winrate statistics...")
        df = calcular_winrate_superficie_ultimas_n(df, superficie, n=10)
    
    print("Calculating winrate statistics...")
    df = calcular_winrate_torneio(df)
    
    return df


if __name__ == "__main__":
    # Test the functions
    df = pd.read_csv("../dataset/tennis_atp/atp_matches_2023.csv")
    df_processed = calcular_todas_winrates(df)
    df_processed.to_csv("../dados_tratados/winrate_stats.csv", index=False)