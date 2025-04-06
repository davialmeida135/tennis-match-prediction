from initial_cleaning import preprocess_dates, transform_seed_data, remove_wo, remove_stat_cols, sort_by_date
from winrate import calcular_winrate_total, calcular_winrate_superficie,calcular_winrate_superficie_ultimas_n,calcular_winrate_torneio,calcular_winrate_ultimas_n
from anonymize import anonymize
from player_stats import calcular_h2h, calcular_elo, calcular_partidas_jogadas, calcular_partidas_jogadas_ultimo_mes
#from prefect import flow
import pandas as pd
import os
import pathlib

class CompletePipeline():
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.output_folder = os.path.join(pathlib.Path(__file__).parent.parent.absolute(), "dados_tratados")

    #@flow
    def initial_clean_pipeline(self):
        self.df = remove_wo(self.df)
        self.df = remove_stat_cols(self.df)
        self.df = preprocess_dates(self.df)
        self.df = sort_by_date(self.df)
        self.df = transform_seed_data(self.df)
        self.df.to_csv(os.path.join(self.output_folder, "initial_clean.csv"), index=False)
        return self.df
    
    #@flow
    def winrate_stats_pipeline(self):
        self.df = calcular_winrate_total(self.df)
        self.df = calcular_winrate_ultimas_n(self.df, n=50) 
        self.df = calcular_winrate_ultimas_n(self.df, n=10)    
        self.df = calcular_winrate_superficie(self.df)
        self.df = calcular_winrate_superficie_ultimas_n(self.df, n=50)
        self.df = calcular_winrate_superficie_ultimas_n(self.df, n=10)
        self.df = calcular_winrate_torneio(self.df)
        self.df.to_csv(os.path.join(self.output_folder, "winrate_stats.csv"), index=False)
        return self.df

    #@flow 
    def encounter_stats_pipeline(self):
        self.df = calcular_h2h(self.df)
        self.df.to_csv(os.path.join(self.output_folder, "encounter_stats.csv"), index=False)
        return self.df

    #@flow
    def player_stats_pipeline(self):
        self.df = calcular_partidas_jogadas(self.df)
        self.df = calcular_partidas_jogadas_ultimo_mes(self.df)
        #self.df = calcular_elo(self.df)
        self.df.to_csv(os.path.join(self.output_folder, "player_stats.csv"), index=False)
        return self.df
    
    #@flow
    def final_clean_pipeline(self):
        self.df = self.df.pipe(anonymize)
        return self.df

        
    #@flow
    def run(self):
        self.initial_clean_pipeline()
        self.winrate_stats_pipeline()
        self.encounter_stats_pipeline()
        self.player_stats_pipeline()
        #self.final_clean_pipeline()
        self.df.to_csv(os.path.join(self.output_folder, "final.csv"), index=False)
        return self.df


if __name__ == "__main__":
    #pipeline = CompletePipeline(pd.read_csv("dados_tratados/all_atp_matches.csv"))
    self_path = pathlib.Path(__file__).parent.absolute()
    csv_path = self_path/".."/"dataset"/"tennis_atp"/"atp_matches_2023.csv"
    pipeline = CompletePipeline(pd.read_csv(csv_path))
    pipeline.run()