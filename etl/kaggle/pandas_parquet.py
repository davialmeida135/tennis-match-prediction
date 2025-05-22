import os
import pandas as pd
from dagster import ConfigurableIOManager, InputContext, OutputContext, Field
from typing import Union
class PandasParquetIOManager(ConfigurableIOManager):
    """
    Handles I/O for pandas DataFrames, saving them to and loading them from Parquet files.
    """
    base_dir: str # Root directory for storing Parquet files

    def _get_path(self, context: Union[InputContext, OutputContext]) -> str:
        """
        Constructs the file path for a given asset.
        The path will be <base_dir>/<asset_key_path_elements_joined_by_/>.parquet
        """
        # context.asset_key.path is a list of strings (e.g., ["my_asset"] or ["my_group", "my_asset"])
        # We join them to form a sub-path and add the .parquet extension.
        asset_path = os.path.join(*context.asset_key.path) + ".parquet"
        return os.path.join(self.base_dir, asset_path)

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        """
        Saves the pandas DataFrame to a Parquet file.
        Creates directories if they don't exist.
        """
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"Expected a pandas DataFrame, but got {type(obj)}.")

        fpath = self._get_path(context)
        # Ensure the directory exists
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        
        try:
            obj.to_parquet(fpath, index=False, engine='pyarrow')
            context.log.info(f"Saved pandas DataFrame to Parquet: {fpath}")
        except Exception as e:
            context.log.error(f"Error saving DataFrame to Parquet {fpath}: {e}")
            raise

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """
        Loads a pandas DataFrame from a Parquet file.
        """
        fpath = self._get_path(context)
        if not os.path.exists(fpath):
            context.log.error(f"Parquet file not found at: {fpath}")
            # Depending on your needs, you might raise an error or return None/empty DataFrame
            raise FileNotFoundError(f"Parquet file not found at: {fpath} for asset {context.asset_key}")
        
        try:
            df = pd.read_parquet(fpath, engine='pyarrow')
            context.log.info(f"Loaded pandas DataFrame from Parquet: {fpath}")
            return df
        except Exception as e:
            context.log.error(f"Error loading DataFrame from Parquet {fpath}: {e}")
            raise