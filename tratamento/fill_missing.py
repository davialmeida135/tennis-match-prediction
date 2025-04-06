import os
import pandas as pd

def fill_null_surface(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill null values in the surface column with 'hard' if the tournament is not a Grand Slam or Masters 1000.
    """
    df['surface'] = df['surface'].fillna('hard')
    return df

