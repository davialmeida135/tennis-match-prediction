import pandas as pd
from util import _get_previous_encounters, _get_previous_matches
#from prefect import flow, task

#@task

#@task
def calcular_elo(df:pd.DataFrame)->pd.DataFrame:
    """
    Soma um ponto para cada vitória do jogador e subtrai um ponto para cada derrota.
    Um jogador novo deve iniciar com 1500 pontos.
    """
    print("Calculando elo")
    elo = {}
    df['winner_elo'] = 0
    df['loser_elo'] = 0
    df['elo_diff'] = 0
    for index, row in df.iterrows():
        winner_matches, loser_matches = _get_previous_matches(df,row)

        winner = row['winner_name']
        loser = row['loser_name']
        
        if winner not in elo:
            elo[winner]= 1500.0
            new_winner_elo = 1500.0
            
        if loser not in elo:
            elo[loser]= 1500.0
            new_loser_elo = 1500.0
            
        # Get most recent elo record
        winner_elo = elo[winner]
        loser_elo = elo[loser]
        
        if len(winner_matches)>0:
            last_winner_match = winner_matches.iloc[-1]
            last_winner_match_winner = last_winner_match['winner_name']
            last_winner_match_loser = last_winner_match['loser_name']
            # This winner won last match
            if last_winner_match_winner == winner:
                last_winner_match_loser_elo = elo[last_winner_match_loser]
            
                expected_winner = 1/(1 + 10 ** ((last_winner_match_loser_elo - winner_elo)/400))
                kwinner = 250/((len(winner_matches)+5)**0.4)
                k = 1.1 if last_winner_match['tourney_level']=='G' else 1
                new_winner_elo = winner_elo + (k*kwinner)*(1-expected_winner)

            # This winner lost last match
            else:
                last_winner_match_winner_elo = elo[last_winner_match_loser]
                expected_loser = 1/(1 + 10 ** ((last_winner_match_winner_elo - winner_elo)/400))            
                kloser = 250/((len(loser_matches)+5)**0.4)
                k = 1.1 if last_winner_match['tourney_level']=='G' else 1
                new_winner_elo = winner_elo + (k*kloser)*(-expected_loser)
        
        if len(loser_matches)>0:
            last_loser_match = loser_matches.iloc[-1]
            last_loser_match_winner = last_loser_match['winner_name']
            last_loser_match_loser = last_loser_match['loser_name']

            if last_loser_match_winner == loser:
                last_loser_match_loser_elo = elo[last_loser_match_loser]
                expected_winner = 1/(1 + 10 ** ((last_loser_match_loser_elo - loser_elo)/400))
                kwinner = 250/((len(loser_matches)+5)**0.4)
                k = 1.1 if last_winner_match['tourney_level']=='G' else 1
                new_loser_elo = loser_elo + (k*kwinner)*(1-expected_winner)
            else:
                last_loser_match_winner_elo = elo[last_loser_match_winner]
                expected_loser =  1/(1 + 10 ** ((last_loser_match_winner_elo - loser_elo)/400))
                kloser = 250/((len(loser_matches)+5)**0.4)
                k = 1.1 if last_winner_match['tourney_level']=='G' else 1
                new_loser_elo = loser_elo + (k*kloser)*(-expected_loser)
    
        elo[winner] = new_winner_elo
        elo[loser] = new_loser_elo
        # # Update elo
        df.loc[index, 'winner_elo'] = new_winner_elo
        df.loc[index, 'loser_elo'] = new_loser_elo
        df.loc[index, 'elo_diff'] = new_winner_elo-new_loser_elo
    
    return df

def calcular_elo_superficies(df:pd.DataFrame)->pd.DataFrame:
    """
    Soma um ponto para cada vitória do jogador e subtrai um ponto para cada derrota em cada superfície.
    Um jogador novo deve iniciar com 1500 pontos.
    """
    print("Calculando elo")
    elo = pd.DataFrame(columns=['player','surface','elo'])
    for index, row in df.iterrows():
        winner = row['winner_name']
        winner = str(winner).lower()
        loser = row['loser_name']
        loser = str(loser).lower()
        surface = row['surface']
        surface = str(surface).lower()

        if len(elo[(elo['player']==winner) & (elo['surface']==surface)]) == 0:
            elo.loc[len(elo)]= [winner,surface,1500.0]
            
        if len(elo[(elo['player']==loser) & (elo['surface']==surface)]) == 0:
            elo.loc[len(elo)]= [loser,surface,1500.0]
            
        # Get most recent elo record
        winner_elo = elo[(elo['player']==winner) & (elo['surface']==surface)].iloc[-1].elo
        loser_elo = elo[(elo['player']==loser) & (elo['surface']==surface)].iloc[-1].elo
        
        winner_matches, loser_matches = _get_previous_matches(df,row)

        expected_winner = 1/(1 + 10 ** ((loser_elo - winner_elo)/400))
        expected_loser = 1/(1 + 10 ** ((winner_elo - loser_elo)/400))
        
        kwinner = 250/((len(winner_matches)+5)**0.4)
        kloser = 250/((len(loser_matches)+5)**0.4)

        k = 1.1 if row['tourney_level']=='G' else 1

        winner_elo = winner_elo + (k*kwinner)*(1-expected_winner)
        loser_elo = loser_elo + (k*kloser)*(-expected_loser)

        elo.loc[(elo['player']==winner) & (elo['surface']==surface),'elo']= winner_elo
        elo.loc[(elo['player']==loser) & (elo['surface']==surface),'elo']= loser_elo
        # Update elo
        df.loc[index, 'winner_surface_elo'] = winner_elo
        df.loc[index, 'loser_surface_elo'] = loser_elo
        df.loc[index, 'surface_elo_diff'] = winner_elo-loser_elo
    
    return df
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
    #df_processed = calcular_round_semana_passada(df)
    df = pd.read_csv("dados_tratados/all_atp_matches.csv", parse_dates=['tourney_date'])
    df_processed = calcular_elo(df)
    df_processed.to_csv("dados_tratados/teste_stats.csv", index=False)

if __name__ == "__main__":
    main()
