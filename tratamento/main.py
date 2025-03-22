
from calculos_recorrentes import calcular_h2h, calcular_elo, tempo_jogado_dataframe
from prefect import flow, task
import pandas as pd

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