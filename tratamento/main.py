from initial_cleaning import merge_datasets, transform_seed_data
from tratamento.player_stats import calcular_h2h, calcular_elo
from prefect import flow
import pandas as pd
import os
import pathlib

class CompletePipeline():
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.output_folder = os.path.join(pathlib.Path(__file__).parent.parent.absolute(), "dados_tratados")

    @flow
    def initial_clean_pipeline(self):
        self.df = self.df.pipe(transform_seed_data)
        return self.df
    
    @flow
    def surface_stats_pipeline(self):
        return self.df

    @flow 
    def encounter_stats_pipeline(self):
        self.df = self.df.pipe(calcular_h2h)
        return self.df

    @flow
    def player_stats_pipeline(self):
        self.df = self.df.pipe(calcular_elo)
        return self.df
    @flow
    def final_clean_pipeline(self):
        # self.df = self.df.pipe(anonymize)
        return self.df

        
    @flow
    def run(self):


        return self.df


if __name__ == "__main__":
    pipeline = CompletePipeline(pd.read_csv("dados_tratados/all_atp_matches.csv"))
    