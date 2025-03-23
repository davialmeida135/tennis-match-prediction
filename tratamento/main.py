
from tratamento.player_stats import calcular_h2h, calcular_elo, tempo_jogado_dataframe
from prefect import flow, task
import pandas as pd
import os
import pathlib

class CompletePipeline():
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.output_folder = os.path.join(pathlib.Path(__file__).parent.parent.absolute(), "dados_tratados")

    def merge_pipeline(self):
        #self.df = self.df.pipe(merge_datasets)
        return
    
    def clean_pipeline(self):
        self.df = self.df.pipe()
        return self.df
    
    def calculate_stats_pipeline(self):
        self.df = self.df.pipe(tempo_jogado_dataframe)

    def anonymize_pipeline(self):
        # self.df = self.df.pipe(anonymize)
        return self.df

        
    @flow
    def run(self):
        self.df = self.df.pipe(tempo_jogado_dataframe).pipe(calcular_h2h).pipe(calcular_elo)
        self.df.to_csv(os.path.join(self.output_folder, "final_dataset.csv"), index=False)
        return self.df
    
@flow(log_prints=True)
def modularizado():
    df = pd.read_csv("dataset/tennis_atp/atp_matches_2017.csv")
    df_processed = df.pipe(tempo_jogado_dataframe)
    return df_processed

@flow(log_prints=True)
def modularizado2(df_processed):
    df_processed.pipe(calcular_h2h)
    df_processed.to_csv("dados_tratados/teste_pipe.csv", index=False)

if __name__ == "__main__":
    df = modularizado()
    modularizado2(df)