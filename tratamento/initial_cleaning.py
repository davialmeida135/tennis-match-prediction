import os
import pandas as pd
import numpy as np

def merge_datasets():
    # Read csv files from folder and merge datasets
    path = "dataset/tennis_atp/"
    files = os.listdir(path)
    dfs = []
    for file in files:
        if file.startswith("atp_matches_199") or file.startswith("atp_matches_2"):
            dfs.append(pd.read_csv(path+file))
            print("Reading file: ", file)
        
    df = pd.concat(dfs)
    df.to_csv("dados_tratados/all_atp_matches.csv", index=False)


def transform_seed_data(df: pd.DataFrame) -> pd.DataFrame:
    # Create copy to avoid warnings
    result = df.copy()

    result['winner_seed_value'] = pd.to_numeric(result['winner_seed'], errors='coerce')
    result['loser_seed_value'] = pd.to_numeric(result['loser_seed'], errors='coerce')
    result['winner_entry_method'] = 'Not Set'
    result['loser_entry_method'] = 'Not Set'

    entry_methods = {'S':1, 'US':2, 'WC':3, 'Q':4, 'LL':5, 'PR':6, 'SE':7, 'ALT':8}
    
    for index, row in result.iterrows():
        if str(row['winner_seed_value'])[0].isdigit():
            result.at[index, 'winner_entry_method'] = 1
        elif pd.isna(row['winner_seed_value']) and pd.isna(row['winner_entry']):
            result.at[index, 'winner_entry_method'] = 2
            result.at[index, 'winner_seed_value'] = row['draw_size']
        else:
            result.at[index, 'winner_entry_method'] = entry_methods.get(row['winner_entry'], "INVALID ENTRY")
            result.at[index, 'winner_seed_value'] = row['draw_size']

        if str(row['loser_seed_value'])[0].isdigit():
            result.at[index, 'loser_entry_method'] = 1
        elif pd.isna(row['loser_seed_value']) and pd.isna(row['loser_entry']): 
            result.at[index, 'loser_entry_method'] = 2
            result.at[index, 'loser_seed_value'] = row['draw_size']
        else:
            result.at[index, 'loser_entry_method'] = entry_methods.get(row['loser_entry'], "INVALID ENTRY")
            result.at[index, 'loser_seed_value'] = row['draw_size']
   
    # Drop original columns
    result = result.drop(columns=['winner_seed', 'loser_seed', 'winner_entry', 'loser_entry'])
    df['winner_seed_value'] = df['winner_seed_value'].astype('Int64')
    df['winner_entry_method'] = df['winner_entry_method'].astype('Int64')
    df['loser_seed_value'] = df['loser_seed_value'].astype('Int64')
    df['loser_entry_method'] = df['loser_entry_method'].astype('Int64')
    
    return result

if __name__ == "__main__":
    merge_datasets()