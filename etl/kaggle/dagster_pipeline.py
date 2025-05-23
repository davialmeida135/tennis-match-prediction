import pandas as pd
import os
import pathlib
from dagster import (
    asset, 
    Definitions,
    Field, 
    DagsterType, 
    define_asset_job,
    AssetCheckSpec,
    AssetCheckResult,
    Output,
    AssetKey,
    )
from .pandas_parquet import PandasParquetIOManager
# Imports from your existing modules
from .initial_cleaning import preprocess_dates, transform_seed_data, sort_by_date
from .winrate import calcular_winrate_total, calcular_winrate_ultimas_n
from .final_cleaning import (
    anonymize,
    remove_stat_cols,
    transform_handedness,
    transform_tourney_level,
    remove_wo,
    encode_surface,
    transform_round
)
from .player_stats import (
    calcular_h2h,
    calcular_elo
)
from .fill_missing import fill_null_surface, fill_null_age, fill_null_height, fill_null_rank

# Define the base output folder
# TODO mudar isso para um arquivo de configuração
BASE_OUTPUT_FOLDER = os.path.join(pathlib.Path(__file__).parent.parent.parent.absolute(), "data/kaggle/transformed")
os.makedirs(BASE_OUTPUT_FOLDER, exist_ok=True)

# Dagster Type for Pandas DataFrame
PandasDataFrame = DagsterType(
    type_check_fn=lambda _, value: isinstance(value, pd.DataFrame),
    name="PandasDataFrame",
    description="A pandas DataFrame.",
)

@asset(
        config_schema={"csv_path": Field(
            str, 
            default_value=str(pathlib.Path(__file__).parent.absolute() / ".." / ".." / "data" / "kaggle" / "raw" /"atp_matches_2023.csv"), description="Path to the initial CSV data file.")},
        check_specs=[
        AssetCheckSpec(name="raw_data_not_empty", asset =AssetKey("raw_tennis_data"), description="Check if the raw data DataFrame is not empty.")
    ],
    metadata={"source_system": "ATP Tour CSV Files", "data_category": "raw"})
def raw_tennis_data(context) -> PandasDataFrame:
    """Loads the initial tennis data from a CSV file."""
    csv_path = context.op_config["csv_path"]
    context.log.info(f"Loading raw data from {csv_path}")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at path: {csv_path}")
    
    df = pd.read_csv(csv_path)
        # Perform check
    is_not_empty = not df.empty
    yield AssetCheckResult(
        check_name="raw_data_not_empty",
        passed=bool(is_not_empty),
        metadata={"num_rows": df.shape[0], "num_cols": df.shape[1]}
    )

    # Yield Output with metadata
    yield Output(
        df,
        metadata={
            "num_rows": df.shape[0],
            "num_columns": df.shape[1],
            "memory_usage": int(df.memory_usage(deep=True).sum()),
            "start_date": int(df["tourney_date"].min()) if "tourney_date" in df.columns else "N/A",
            "end_date": int(df["tourney_date"].max()) if "tourney_date" in df.columns else "N/A",
            "data_types": df.dtypes.astype(str).to_json(),
            "null_values": df.isnull().sum().to_json(),
            "numeric_describe": df.describe().transpose().to_dict(),
            "preview": df.head().to_dict(),
        }
    )

@asset
def initial_cleaned_data(context, raw_tennis_data: PandasDataFrame) -> PandasDataFrame:
    """Applies initial cleaning steps to the raw tennis data."""
    context.log.info("Running initial cleaning")
    df = preprocess_dates(raw_tennis_data.copy())
    df = sort_by_date(df)
    df = transform_seed_data(df)
    return df

@asset
def filled_missing_data(context, initial_cleaned_data: PandasDataFrame) -> PandasDataFrame:
    """Fills missing values in the cleaned data."""
    context.log.info("Filling missing values")
    df = fill_null_surface(initial_cleaned_data.copy())
    df = fill_null_height(df)
    df = fill_null_age(df)
    df = fill_null_rank(df)
    return df

@asset
def winrate_featured_data(context, filled_missing_data: PandasDataFrame) -> PandasDataFrame:
    """Calculates winrate statistics and adds them as features."""
    context.log.info("Calculating winrate statistics")
    df = calcular_winrate_total(filled_missing_data.copy())
    df = calcular_winrate_ultimas_n(df, n=50)
    df = calcular_winrate_ultimas_n(df, n=10)

    return df

@asset
def h2h_featured_data(context, winrate_featured_data: PandasDataFrame) -> PandasDataFrame:
    """Calculates head-to-head (H2H) statistics."""
    context.log.info("Calculating H2H statistics")
    df = calcular_h2h(winrate_featured_data.copy())
    return df

@asset
def elo_featured_data(context, h2h_featured_data: PandasDataFrame) -> PandasDataFrame:
    """Calculates Elo ratings for players."""
    context.log.info("Calculating Elo ratings")
    df = calcular_elo(h2h_featured_data.copy())
    return df

@asset
def pre_anonymized_data(context, elo_featured_data: PandasDataFrame) -> PandasDataFrame:
    """Applies final cleaning steps before anonymization. 
       This asset represents the 'pre_anon_dagster.csv' state."""
    context.log.info("Running final cleaning steps before anonymization")
    df = remove_wo(elo_featured_data.copy())
    df = encode_surface(df)
    df = transform_round(df)
    df = transform_tourney_level(df)
    df = transform_handedness(df)
    df = remove_stat_cols(df)
    
    # If you still need the specific pre_anon_dagster.csv for other purposes,
    # you can save it here as a side-effect.
    # The I/O manager will also save this asset's output (likely as parquet).
    pre_anon_path = os.path.join(BASE_OUTPUT_FOLDER, "pre_anon_dagster_asset_version.csv")
    df.to_csv(pre_anon_path, index=False)
    context.log.info(f"Side-effect: Saved pre_anon_dagster_asset_version.csv to {pre_anon_path}")
    return df

@asset
def final_anonymized_data(context, pre_anonymized_data: PandasDataFrame) -> PandasDataFrame:
    """Anonymizes the data. This is the final dataset."""
    context.log.info("Anonymizing data")
    df = anonymize(pre_anonymized_data.copy())
    # Save the final anonymized dataset
    final_path = os.path.join(BASE_OUTPUT_FOLDER, "final_anonymized_dagster_asset_version.csv")
    df.to_csv(final_path, index=False)
    context.log.info("Anonymization complete. This is the final dataset.")
    return df

# Define all assets for Dagster
all_assets = [
    raw_tennis_data,
    initial_cleaned_data,
    filled_missing_data,
    winrate_featured_data,
    h2h_featured_data,
    elo_featured_data,
    pre_anonymized_data,
    final_anonymized_data,
]
# Define a job that targets all assets in the `all_assets` list
materialize_all_tennis_data_job = define_asset_job(
    name="materialize_all_tennis_data_job",
    selection=all_assets
)
defs = Definitions(
    assets=all_assets,
    jobs=[materialize_all_tennis_data_job],
    resources={
        "io_manager": PandasParquetIOManager(base_dir=BASE_OUTPUT_FOLDER),
    }
)