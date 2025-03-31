import pandas as pd

def _get_previous_matches(df:pd.DataFrame, player_id:int, date) -> pd.DataFrame:
    """
    Retorna todas as partidas de um jogador antes de uma data
    """
    df = df[(df['winner_id']==player_id) | (df['loser_id']==player_id)]
    return df[df['tourney_date']<date]

def _get_previous_encounters(df, player1_id, player2_id, date):
    """
    Retorna todas as partidas entre dois jogadores antes de uma data
    """
    df1 = df[(df['winner_id']==player1_id) & (df['loser_id']==player2_id)]
    df2 = df[(df['winner_id']==player2_id) & (df['loser_id']==player1_id)]
    df = pd.concat([df1, df2])
    df.sort_values(by='tourney_date', inplace=True)
    return df[df['tourney_date']<date]