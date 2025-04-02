import pandas as pd

def _get_previous_matches(df:pd.DataFrame, match_row) -> list[pd.DataFrame]:
    """
    Retorna todas as partidas de um jogador antes de uma data
    """
    date = match_row['tourney_date']
    winner = match_row['winner_id']
    loser = match_row['loser_id']
    winner_matches:pd.DataFrame = df[(df['winner_id']==winner) | (df['loser_id']==winner)]
    winner_matches:pd.DataFrame = winner_matches[winner_matches['tourney_date']<=date]
    while True:
        row = winner_matches.iloc[-1]
        if row.equals(match_row):
            winner_matches = winner_matches.iloc[:-1]
            break
        winner_matches = winner_matches.iloc[:-1]
    
    loser_matches:pd.DataFrame = df[(df['winner_id']==loser) | (df['loser_id']==loser)]
    loser_matches:pd.DataFrame = loser_matches[loser_matches['tourney_date']<=date]
    while True:
        row = loser_matches.iloc[-1]
        if row.equals(match_row):
            loser_matches = loser_matches.iloc[:-1]
            break
        loser_matches = loser_matches.iloc[:-1]

    return [winner_matches,loser_matches]

def _get_previous_encounters(df, player1_id, player2_id, date):
    """
    Retorna todas as partidas entre dois jogadores antes de uma data
    """
    df1 = df[(df['winner_id']==player1_id) & (df['loser_id']==player2_id)]
    df2 = df[(df['winner_id']==player2_id) & (df['loser_id']==player1_id)]
    df = pd.concat([df1, df2])
    df.sort_values(by='tourney_date', inplace=True)
    return df[df['tourney_date']<date]

if __name__ == '__main__':
    df = pd.read_csv("../dataset/tennis_atp/atp_matches_2023.csv")
    row = df.iloc[12]
    w,l = _get_previous_matches(df,row)
    print(l)
    #print(l)