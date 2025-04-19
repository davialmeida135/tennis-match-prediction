import pandas as pd

def fill_null_surface(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill null values in the surface column with 'hard' if the tournament is not a Grand Slam or Masters 1000.
    """
    df['surface'] = df['surface'].fillna('Hard')
    return df

def fill_null_height(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill null values in the height column with the average height of the player.
    """
    df['winner_ht'] = df['winner_ht'].fillna(df['winner_ht'].mean())
    df['loser_ht'] = df['loser_ht'].fillna(df['loser_ht'].mean())

    return df

def fill_null_age(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill null values in the age column with the average age of the player.
    """
    df['winner_age'] = df['winner_age'].fillna(df['winner_age'].mean())
    df['loser_age'] = df['loser_age'].fillna(df['loser_age'].mean())

    return df

def fill_null_rank(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill null values in the rank column with the average rank of the player.
    """
    df['winner_rank'] = df['winner_rank'].fillna(df['winner_rank'].max())
    df['loser_rank'] = df['loser_rank'].fillna(df['loser_rank'].max())

    df['winner_rank_points'] = df['winner_rank_points'].fillna(df['winner_rank_points'].min())
    df['loser_rank_points'] = df['loser_rank_points'].fillna(df['loser_rank_points'].min())

    return df