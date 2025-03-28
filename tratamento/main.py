from initial_cleaning import transform_seed_data
from anonymize import anonymize
from player_stats import calcular_h2h, calcular_elo
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
        self.df = self.df.pipe(transform_seed_data)
        self.df.to_csv(os.path.join(self.output_folder, "initial_clean.csv"), index=False)
        return self.df
    
    #@flow
    def surface_stats_pipeline(self):
        return self.df

    #@flow 
    def encounter_stats_pipeline(self):
        self.df = self.df.pipe(calcular_h2h)
        return self.df

    #@flow
    def player_stats_pipeline(self):
        self.df = self.df.pipe(calcular_elo)
        return self.df
    #@flow
    def final_clean_pipeline(self):
        self.df = self.df.pipe(anonymize)
        return self.df

        
    #@flow
    def run(self):
        self.initial_clean_pipeline()

        return self.df


if __name__ == "__main__":
    #pipeline = CompletePipeline(pd.read_csv("dados_tratados/all_atp_matches.csv"))
    pipeline = CompletePipeline(pd.read_csv("dataset/tennis_atp/atp_matches_2023.csv"))
    
    pipeline.run()