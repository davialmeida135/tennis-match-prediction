from prefect import flow, task
import pandas as pd
import os
import pathlib

class Transform():
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.output_folder = os.path.join(pathlib.Path(__file__).parent.parent.absolute(), "dados_tratados")
        
    @flow
    def transform(self):
        self.df = self.calcular_minutos_acumulados_torneio()
        self.df = self.calcular_h2h()
        self.df.to_csv(os.path.join(self.output_folder, "final_dataset.csv"), index=False)
        return self.df
    

    @task
    def calcular_minutos_acumulados_torneio(self)->pd.DataFrame:
        """
        Calcula para todas as partidas, o tempo jogado por cada jogador no torneio antes da partida atual
        """
        rows_list = []
        for index, row in self.df.iterrows():
            tourney = self.df[self.df['tourney_id']==row['tourney_id']]
            # Skip Davies Cup
            if tourney['round'].values[0] == 'D' or tourney['round'].values[0] == 'RR':
                continue
            new_row = self._calcular_carga_previa_jogadores(row,tourney)

            if new_row is not None:
                rows_list.append(new_row)

        new_df = pd.DataFrame(rows_list)
        new_df.to_csv("dados_tratados/parcial_tempo_jogado.csv", index=False)
        return new_df
    
    @task
    def calcular_h2h(self)->pd.DataFrame:
        '''
        Para cada partida, calcula o histórico de confrontos entre os jogadores
        h2h = vitórias do jogador 1 - vitórias do jogador 2
        '''
        self.df['h2h']=0
        for index, row in self.df.iterrows():
            winner = row['winner_id']
            loser = row['loser_id']
            
            previous_encounters = self._get_previous_encounters(self.df, winner, loser, row['tourney_date'])
            if len(previous_encounters) > 0:
                last_encounter = previous_encounters.iloc[-1]
                last_encounter_h2h = last_encounter['h2h']
                if winner == last_encounter['winner_id']:
                    last_encounter_h2h += 1
                else:
                    last_encounter_h2h += 1
                    last_encounter_h2h *= -1
                self.df.loc[index, 'h2h'] = last_encounter_h2h
            else:
                continue

        self.df.to_csv("dados_tratados/parcial_h2h.csv", index=False)
        return self.df


    def _get_previous_matches(self,df, player_id, date):
        """
        Retorna todas as partidas de um jogador antes de uma data
        """
        df = df[(df['winner_id']==player_id) | (df['loser_id']==player_id)]
        return df[df['tourney_date']<date]

    def _get_previous_encounters(self,df, player1_id, player2_id, date):
        """
        Retorna todas as partidas entre dois jogadores antes de uma data
        """
        df1 = df[(df['winner_id']==player1_id) & (df['loser_id']==player2_id)]
        df2 = df[(df['winner_id']==player2_id) & (df['loser_id']==player1_id)]
        df = pd.concat([df1, df2])
        df.sort_values(by='tourney_date', inplace=True)
        return df[df['tourney_date']<date]

    def _calcular_carga_previa_jogadores(self,row, df):
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
    
if __name__ == "__main__":
    df = pd.read_csv("dataset/tennis_atp/atp_matches_2017.csv")
    t = Transform(df)
    t.transform()