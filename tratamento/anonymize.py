import pandas as pd
import numpy as np

def anonymize(df):
    """
    Troca os nomes dos jogadores por player_1 e player_2"
    """
    rows_list = []

    # Iterar sobre as linhas do DataFrame trocando loser por player_1 e winner por player_2
    # Fazer de maneira aleatória para evitar bias
    for index, row in df.iterrows():
        if np.random.randint(0, 2) == 0:
            rows_list.append({
                'tourney_id': row['tourney_id'],
                'tourney_name': row['tourney_name'],
                'surface': row['surface'],
                'draw_size': row['draw_size'],
                'tourney_level': row['tourney_level'],
                'tourney_date': row['tourney_date'],
                'match_num': row['match_num'],
                'player1_id': row['loser_id'],
                'player1_seed': row['loser_seed'],
                'player1_entry': row['loser_entry'],
                'player1_name': row['loser_name'],
                'player1_hand': row['loser_hand'],
                'player1_ht': row['loser_ht'],
                'player1_ioc': row['loser_ioc'],
                'player1_age': row['loser_age'],
                'player1_rank': row['loser_rank'],
                'player1_rank_points': row['loser_rank_points'],
                'player2_id': row['winner_id'],
                'player2_seed': row['winner_seed'],
                'player2_entry': row['winner_entry'],
                'player2_name': row['winner_name'],
                'player2_hand': row['winner_hand'],
                'player2_ht': row['winner_ht'],
                'player2_ioc': row['winner_ioc'],
                'player2_age': row['winner_age'],
                'player2_rank': row['winner_rank'],
                'player2_rank_points': row['winner_rank_points'],
                'best_of': row['best_of'],
                'round': row['round'],
                'winner':2,
            })
        else:
            rows_list.append({
                'tourney_id': row['tourney_id'],
                'tourney_name': row['tourney_name'],
                'surface': row['surface'],
                'draw_size': row['draw_size'],
                'tourney_level': row['tourney_level'],
                'tourney_date': row['tourney_date'],
                'match_num': row['match_num'],
                'player1_id': row['winner_id'],
                'player1_seed': row['winner_seed'],
                'player1_entry': row['winner_entry'],
                'player1_name': row['winner_name'],
                'player1_hand': row['winner_hand'],
                'player1_ht': row['winner_ht'],
                'player1_ioc': row['winner_ioc'],
                'player1_age': row['winner_age'],
                'player1_rank': row['winner_rank'],
                'player1_rank_points': row['winner_rank_points'],
                'player2_id': row['loser_id'],
                'player2_seed': row['loser_seed'],
                'player2_entry': row['loser_entry'],
                'player2_name': row['loser_name'],
                'player2_hand': row['loser_hand'],
                'player2_ht': row['loser_ht'],
                'player2_ioc': row['loser_ioc'],
                'player2_age': row['loser_age'],
                'player2_rank': row['loser_rank'],
                'player2_rank_points': row['loser_rank_points'],
                'best_of': row['best_of'],
                'round': row['round'],
                'winner':1,
            })

    anon_df = pd.DataFrame(rows_list)
    return anon_df
#print(anon_df['round'].value_counts())
#anon_df.to_csv("dados_tratados/anon_atp_matches_2017.csv", index=False)