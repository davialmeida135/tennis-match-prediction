import pandas as pd
from util import _get_previous_encounters, _get_previous_matches
from datetime import datetime,timedelta
#from prefect import flow, task

#@task

#@task
def calcular_elo(df:pd.DataFrame)->pd.DataFrame:
    """
    Soma um ponto para cada vitória do jogador e subtrai um ponto para cada derrota.
    Um jogador novo deve iniciar com 100 pontos.
    """
    print("Calculando elo")
    elo = pd.DataFrame(columns=['player_id', 'elo', 'date'])
    for index, row in df.iterrows():
        player1 = row['winner_id']
        player2 = row['loser_id']
        date = row['tourney_date']
        
        if player1 not in elo['player_id'].values:
            # Use loc to add a row
            elo.loc[len(elo)] = {'player_id': player1, 'elo': 100, 'date': date}
            
        if player2 not in elo['player_id'].values:
            elo.loc[len(elo)] = {'player_id': player2, 'elo': 100, 'date': date}
            
        # Get most recent elo record
        player1_elo = elo[elo['player_id']==player1].iloc[-1]['elo']
        player2_elo = elo[elo['player_id']==player2].iloc[-1]['elo']
        
        player1_elo += 1
        player2_elo -= 1
        # Update elo
        elo.loc[len(elo)] = {'player_id': player1, 'elo': player1_elo, 'date': date}
        elo.loc[len(elo)] = {'player_id': player2, 'elo': player2_elo, 'date': date}

    return elo
#@task
def calcular_h2h(df:pd.DataFrame)->pd.DataFrame:
    '''
    Para cada partida, calcula o histórico de confrontos entre os jogadores
    h2h = vitórias do jogador 1 - vitórias do jogador 2
    '''
    print("Calculando h2h")
    df['h2h']=0
    for index, row in df.iterrows():
        winner = row['winner_id']
        loser = row['loser_id']
        
        previous_encounters = _get_previous_encounters(df, winner, loser, row['tourney_date'])
        if len(previous_encounters) > 0:
            last_encounter = previous_encounters.iloc[-1]
            last_encounter_h2h = last_encounter['h2h']
            if winner == last_encounter['winner_id']:
                last_encounter_h2h += 1
            else:
                last_encounter_h2h += 1
                last_encounter_h2h *= -1
            df.loc[index, 'h2h'] = last_encounter_h2h
        else:
            continue
    return df

def calcular_partidas_jogadas(df:pd.DataFrame)->pd.DataFrame:
    """
    Calcula o número de partidas jogadas por cada jogador
    """
    print("Calculando partidas jogadas")
    df['winner_matches_played'] = 0
    df['loser_matches_played'] = 0

    for index, row in df.iterrows():
        
        # Get previous matches for winner
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
    new_df.to_csv("dados_tratados/atp_matches_2017_tempo_jogado.csv", index=False)
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
    df = pd.read_csv("dados_tratados/initial_clean.csv", parse_dates=['tourney_date'])
    #df_processed = calcular_round_semana_passada(df)
    df_processed = calcular_round_semana_passada(df)
    df_processed.to_csv("dados_tratados/teste_pipe.csv", index=False)

if __name__ == "__main__":
    main()
