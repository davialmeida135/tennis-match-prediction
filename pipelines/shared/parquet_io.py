"""IO manager that persists every pandas DataFrame asset as a parquet snapshot.

Why an IO manager: assets return data and Dagster decides where it is stored.
Keeping assets free of `df.to_csv(...)` side effects is what allows a step to be
re-run without silently overwriting files outside of Dagster's control.

Layout: `<base_dir>/<asset_name>.parquet`, so `data/staging/elo_featured_atp_matches.parquet`
holds the snapshot of the asset named `elo_featured_atp_matches`.
"""

from pathlib import Path

import pandas as pd
from dagster import ConfigurableIOManager, InputContext, MetadataValue, OutputContext

from .paths import STAGING_DIR


class ParquetDataFrameIOManager(ConfigurableIOManager):
    """Store/load pandas DataFrames as parquet, one file per asset."""

    base_dir: str = str(STAGING_DIR)

    def snapshot_path(self, context: InputContext | OutputContext) -> Path:
        return Path(self.base_dir).joinpath(*context.asset_key.path).with_suffix(".parquet")

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"Expected a pandas DataFrame, got {type(obj).__name__}.")

        path = self.snapshot_path(context)
        path.parent.mkdir(parents=True, exist_ok=True)
        obj.to_parquet(path, index=False)
        context.log.info(f"Snapshot written to {path}")
        context.add_output_metadata({"snapshot_path": MetadataValue.path(str(path))})

    def load_input(self, context: InputContext) -> pd.DataFrame:
        path = self.snapshot_path(context)
        if not path.exists():
            raise FileNotFoundError(
                f"No parquet snapshot for asset {context.asset_key.to_user_string()} at {path}. "
                "Materialize the upstream asset before running the downstream one."
            )
        context.log.info(f"Loading snapshot {path}")
        return pd.read_parquet(path)
