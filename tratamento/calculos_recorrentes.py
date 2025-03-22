import pandas as pd
import numpy as np
import time

def tempo_jogado_dataframe(df):
    """
    Calcula para todas as partidas, o tempo jogado por cada jogador no torneio antes da partida atual
    """
    rows_list = []
    for index, row in df.iterrows():
        tourney = df[df['tourney_id']==row['tourney_id']]
        # Skip Davies Cup
        if tourney['round'].values[0] == 'D' or tourney['round'].values[0] == 'RR':
            continue
        new_row = tempo_jogado_torneio(row,tourney)
        print(new_row)
        if new_row is not None:
            rows_list.append(new_row)

    new_df = pd.DataFrame(rows_list)
    new_df.to_csv("dados_tratados/atp_matches_2017_tempo_jogado.csv", index=False)

def tempo_jogado_torneio(row, df):
    """
    Recebe uma partida e um DataFrame com todas as partidas
    do torneio da partida.
    Calcula o tempo jogado por cada jogador no torneio antes 
    da partida atual
    """
    minutes = row['minutes']
    tourney = row['tourney_id']
    t_round = row['round']
    player1 = row['winner_id']
    player2 = row['loser_id']
    row['player1_tournament_minutes'] = 0
    row['player2_tournament_minutes'] = 0

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
            row['player1_tournament_minutes'] += match_p1['minutes'].values[0]
        if len(match_p2) == 1:
            #print(match_p2[['winner_name', 'tourney_name', 'round', 'minutes']])
            row['player2_tournament_minutes'] += match_p2['minutes'].values[0]

    #print(row[['player1_tournament_minutes','player2_tournament_minutes', 'winner_name', 'loser_name']])
    return row 

def calcular_elo(df):
    """
    Soma um ponto para cada vitória do jogador e subtrai um ponto para cada derrota.
    Um jogador novo deve iniciar com 100 pontos.
    """
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
    print(elo)  

    elo.to_csv("dados_tratados/atp_matches_2017_elo.csv", index=False)

def calcular_h2h(df):
    h2h = pd.DataFrame(columns=['player1_id', 'player2_id', 'h2h'])
    for index, row in df.iterrows():
        winner = row['winner_id']
        loser = row['loser_id']
        
        current_h2h = h2h[(h2h['player1_id']==winner) & (h2h['player2_id']==loser)]
        
        if len(current_h2h) >0:
            if winner == current_h2h['player1_id'].values[0]:
                h2h.loc[(h2h['player1_id']==winner) & (h2h['player2_id']==loser), 'h2h'] += 1
                continue
            elif winner == current_h2h['player2_id'].values[0]:
                h2h.loc[(h2h['player2_id']==winner) & (h2h['player1_id']==loser), 'h2h'] -= 1
                continue

        current_h2h = h2h[(h2h['player1_id']==loser) & (h2h['player2_id']==winner)]

        if len(current_h2h) > 0:
            if loser == current_h2h['player1_id'].values[0]:
                h2h.loc[(h2h['player1_id']==loser) & (h2h['player2_id']==winner), 'h2h'] -= 1
                continue
            elif loser == current_h2h['player2_id'].values[0]:
                h2h.loc[(h2h['player2_id']==loser) & (h2h['player1_id']==winner), 'h2h'] += 1
                continue

        h2h.loc[len(h2h)] = {'player1_id': winner, 'player2_id': loser, 'h2h': 1}

    h2h.to_csv("dados_tratados/atp_matches_2017_h2h.csv", index=False)
    return

def get_partidas_jogadores(df, player1_id, player2_id):
    """
    Retorna todas as partidas entre dois jogadores
    """
    df1 = df[(df['winner_id']==player1_id) & (df['loser_id']==player2_id)]
    df2 = df[(df['winner_id']==player2_id) & (df['loser_id']==player1_id)]
    return pd.concat([df1, df2])


df = pd.read_csv("dataset/tennis_atp/atp_matches_2017.csv")
print(get_partidas_jogadores(df, 106378,106298)[['winner_name', 'loser_name','tourney_name']])

#calcular_h2h(df)
#tempo_jogado_dataframe(df)
#print(calcular_elo(df))
