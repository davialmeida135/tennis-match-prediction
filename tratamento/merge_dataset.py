import os
import pandas as pd
import numpy as np

def merge_datasets():
    # Read csv files from folder and merge datasets
    path = "dataset/tennis_atp/"
    files = os.listdir(path)
    dfs = []
    for file in files:
        if file.startswith("atp_matches_1") or file.startswith("atp_matches_2"):
            dfs.append(pd.read_csv(path+file))
        
    df = pd.concat(dfs)
    df.to_csv("dados_tratados/all_atp_matches.csv", index=False)

if __name__ == "__main__":
    merge_datasets()