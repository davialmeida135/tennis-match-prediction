import os
import pandas as pd

def merge_datasets() -> pd.DataFrame:
    # Read csv files from folder and merge datasets
    path = "dataset/tennis_atp/"
    files = os.listdir(path)
    dfs = []
    for file in files:
        # Only files from 2000s
        if len(file) != 20 or file[0:5] != "atp_m":
            continue
        dfs.append(pd.read_csv(path+file))
        print("Reading file: ", file)
        
    df = pd.concat(dfs)

    df.to_csv("dados_tratados/all_atp_matches.csv", index=False)
    
    return df

def preprocess_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Convert integer dates to datetime format"""
    df['tourney_date'] = pd.to_datetime(df['tourney_date'].astype(str), format='%Y%m%d')
    df['week'] = df['tourney_date'].dt.isocalendar().week
    df['year'] = df['tourney_date'].dt.isocalendar().year

    return df

def sort_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """Sort dataframe by date"""
    df = df.sort_values(by=['tourney_date','tourney_id','match_num'])
    return df

def transform_seed_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Transforming seed data")
    # Create copy to avoid warnings
    result = df.copy()

    result['winner_seed_value'] = pd.to_numeric(result['winner_seed'], errors='coerce')
    result['loser_seed_value'] = pd.to_numeric(result['loser_seed'], errors='coerce')
    result['winner_seeded'] = False
    result['loser_seeded'] = False
    result['winner_unseeded'] = False
    result['loser_unseeded'] = False
    result['winner_qualifier'] = False
    result['loser_qualifier'] = False
    result['winner_lucky_loser'] = False
    result['loser_lucky_loser'] = False
    result['winner_special_exempt'] = False
    result['loser_special_exempt'] = False
    result['winner_alternate'] = False
    result['loser_alternate'] = False
    result['winner_wildcard'] = False
    result['loser_wildcard'] = False
    result['winner_protected_ranking'] = False
    result['loser_protected_ranking'] = False

    winner_entry_methods = {'S':'winner_seeded', 
                            'US':'winner_unseeded', 
                            'WC':'winner_wildcard',
                            'Q':'winner_qualifier', 
                            'LL':'winner_lucky_loser',
                            'PR':'winner_protected_ranking',
                            'SE':'winner_special_exempt',
                            'ALT':'winner_alternate'}
    
    loser_entry_methods = {'S':'loser_seeded',
                            'US':'loser_unseeded', 
                            'WC':'loser_wildcard',
                            'Q':'loser_qualifier', 
                            'LL':'loser_lucky_loser',
                            'PR':'loser_protected_ranking',
                            'SE':'loser_special_exempt',
                            'ALT':'loser_alternate'}
    
    for index, row in result.iterrows():
        if str(row['winner_seed_value'])[0].isdigit():
            result.at[index, 'winner_seeded'] = True
        elif pd.isna(row['winner_seed_value']) and pd.isna(row['winner_entry']):
            result.at[index, 'winner_unseeded'] = True
            result.at[index, 'winner_seed_value'] = row['draw_size']
        else:
            result.at[index, winner_entry_methods.get(row['winner_entry'].upper())] = True
            result.at[index, 'winner_seed_value'] = row['draw_size']

        if str(row['loser_seed_value'])[0].isdigit():
            result.at[index, 'loser_seeded'] = True
        elif pd.isna(row['loser_seed_value']) and pd.isna(row['loser_entry']): 
            result.at[index, 'loser_unseeded'] = True
            result.at[index, 'loser_seed_value'] = row['draw_size']
        else:
            result.at[index,loser_entry_methods.get(row['loser_entry'].upper())] = True
            result.at[index, 'loser_seed_value'] = row['draw_size']
   
    # Drop original columns
    result = result.drop(columns=['winner_seed', 'loser_seed', 'winner_entry', 'loser_entry'])
    result['winner_seed_value'] = result['winner_seed_value'].astype('Int64',errors='ignore')
    result['loser_seed_value'] = result['loser_seed_value'].astype('Int64',errors='ignore')
    
    print("Seed data transformed")
    return result

if __name__ == "__main__":
    df = pd.read_csv("dados_tratados/all_atp_matches.csv")
    #df = transform_tourney_level(df)
    #df.to_csv("dados_tratados/teste.csv", index=False)
    merge_datasets()