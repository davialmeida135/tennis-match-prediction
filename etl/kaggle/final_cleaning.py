import pandas as pd
import numpy as np
import polars as pl

def remove_wo(df: pd.DataFrame) -> pd.DataFrame:
    # Remove matches with walkover
    print("Removing Walkovers")
    df = df[df['score'] != 'W/O']
    print("Removed Walkovers")
    return df

def encode_surface(df:pd.DataFrame) -> pd.DataFrame:
    """Apply one-hot encoding to the surface column"""
    import pandas as pd
    from sklearn.preprocessing import OneHotEncoder
    print('Applying one-hot encoding...')

    encoder = OneHotEncoder(sparse_output=False,)
    encoded_surface = encoder.fit_transform(df[['surface']])
    encoded_surface_df = pd.DataFrame(encoded_surface, columns=encoder.get_feature_names_out(['surface']))
    df = df.join(encoded_surface_df)

    return df

def transform_round(df: pd.DataFrame) -> pd.DataFrame:
    """Transform round to categorical values"""
    dictionary = {'F':0,'SF':1,'QF':2,'R16':3,'R32':4,'R64':5,'R128':6, 'RR':3} # Talvez one hot melhor
    df['round'] = df['round'].apply(lambda x: dictionary.get(x, 0))
    return df

def transform_tourney_level(df: pd.DataFrame) -> pd.DataFrame:
    """Transform tournament level to categorical values"""
    dictionary = {'D':0,'A':1,'M':2,'G':3,'F':4,}
    df['tourney_level'] = df['tourney_level'].apply(lambda x: dictionary.get(x, 0))
    return df

def transform_handedness(df: pd.DataFrame) -> pd.DataFrame:
    """Transform handedness to categorical values"""
    df['winner_hand'] = df['winner_hand'].apply(lambda x: 0 if x == 'R' else 1)
    df['loser_hand'] = df['loser_hand'].apply(lambda x: 0 if x == 'R' else 1)
    return df

def remove_stat_cols(df :pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=['w_ace',
                     'w_df',
                     'w_svpt',
                     'w_1stIn',
                     'w_1stWon',
                     'w_2ndWon',
                     'w_SvGms',
                     'w_bpSaved',
                     'w_bpFaced',
                     'l_ace',
                     'l_df',
                     'l_svpt',
                     'l_1stIn',
                     'l_1stWon',
                     'l_2ndWon',
                     'l_SvGms',
                     'l_bpSaved',
                     'l_bpFaced',
                     'score',
                     'winner_ioc',
                     'loser_ioc',
                     #'winner_id',
                     #'loser_id'
                     ])
    return df


def anonymize(df: pl.DataFrame) -> pl.DataFrame:
    """
    Randomly assigns winner/loser stats to player0/player1 and sets a target 'winner' column.
    Uses Polars vectorized operations for efficiency.
    """
    print('Anonymizing data using Polars...')

    if not isinstance(df, pl.DataFrame):
        try:
            df = pl.from_pandas(df) # Convert if input is pandas
        except Exception as e:
            raise TypeError(f"Input must be a Polars or Pandas DataFrame. Conversion failed: {e}")

    # Define common columns (not swapped) and attribute stems for player-specific columns
    common_columns = [
        'draw_size', 'tourney_level', 'week', 'year', 'match_num',
        'best_of', 'round',
        'surface_Hard', 'surface_Clay', 'surface_Grass',
    ]
    attribute_stems = [
        "hand", "ht", "age", "rank", "rank_points", "seed_value", "seeded",
        "unseeded", "qualifier", "lucky_loser", "special_exempt", "alternate",
        "wildcard", "protected_ranking"
    ]

    # Check for required input columns
    required_input_cols = common_columns[:]
    for stem in attribute_stems:
        required_input_cols.append(f"winner_{stem}")
        required_input_cols.append(f"loser_{stem}")
    
    missing_cols = [col for col in required_input_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Input DataFrame is missing required columns: {missing_cols}")

    # 1. Generate a random choice for each row (0 or 1)
    # 0: player0 = loser, player1 = winner (target winner = 1)
    # 1: player0 = winner, player1 = loser (target winner = 0)
    df = df.with_columns(
        swap_choice = pl.Series(np.random.randint(0, 2, size=df.height))
    )

    # 2. Build expressions for new columns
    expressions = []

    # Player0 attributes
    for stem in attribute_stems:
        expressions.append(
            pl.when(pl.col("swap_choice") == 0)
            .then(pl.col(f"loser_{stem}"))
            .otherwise(pl.col(f"winner_{stem}"))
            .alias(f"player0_{stem}")
        )

    # Player1 attributes
    for stem in attribute_stems:
        expressions.append(
            pl.when(pl.col("swap_choice") == 0)
            .then(pl.col(f"winner_{stem}"))
            .otherwise(pl.col(f"loser_{stem}"))
            .alias(f"player1_{stem}")
        )

    # Target 'winner' column
    expressions.append(
        pl.when(pl.col("swap_choice") == 0)
        .then(pl.lit(1))  # player1 (original winner) won
        .otherwise(pl.lit(0))  # player1 (original loser) lost, so player0 won
        .alias("winner")
    )

    # 3. Apply expressions to create new columns
    df_anonymized = df.with_columns(expressions)

    # 4. Select and order final columns
    final_column_order = [
        'draw_size', 'tourney_level', 'tourney_date', 'match_num',
        'player0_hand', 'player0_ht', 'player0_age', 'player0_rank', 'player0_rank_points',
        'player1_hand', 'player1_ht', 'player1_age', 'player1_rank', 'player1_rank_points',
        'best_of', 'round',
        'player0_seed_value', 'player1_seed_value',
        'player0_seeded', 'player1_seeded',
        'player0_unseeded', 'player1_unseeded',
        'player0_qualifier', 'player1_qualifier',
        'player0_lucky_loser', 'player1_lucky_loser',
        'player0_special_exempt', 'player1_special_exempt',
        'player0_alternate', 'player1_alternate',
        'player0_wildcard', 'player1_wildcard',
        'player0_protected_ranking', 'player1_protected_ranking',
        'surface_Hard', 'surface_Clay', 'surface_Grass',
        'winner'
    ]
    
    # Ensure all columns in final_column_order are present before selecting
    missing_final_cols = [col for col in final_column_order if col not in df_anonymized.columns and col not in common_columns]
    if missing_final_cols:
         # This case should ideally not be hit if attribute_stems and common_columns are correct
        raise ValueError(f"Logic error: some expected final columns were not generated: {missing_final_cols}")

    df_anonymized = df_anonymized.select(final_column_order)

    print('Anonymization complete.')
    return df_anonymized.to_pandas()