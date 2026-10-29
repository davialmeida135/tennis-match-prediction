"""DataFrame validation and the normalized historical match schema."""

import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype
from pydantic import ConfigDict

from tennis_match_prediction.contracts.base import ContractModel


class DataFrameContract(ContractModel):
    """Required columns and invariants shared by transforms and orchestration checks."""

    model_config = ConfigDict(frozen=True)

    name: str
    required: tuple[str, ...]
    numeric: tuple[str, ...] = ()
    identifiers: tuple[str, ...] = ()
    date_column: str | None = None
    parsed_dates: bool = False
    target: str | None = None
    exact_columns: bool = False

    def errors(self, frame: pd.DataFrame) -> list[str]:
        """Return actionable failures, including missing and duplicate columns."""
        errors = []
        if not frame.columns.is_unique:
            return ["duplicate columns"]
        missing = sorted(set(self.required).difference(frame.columns))
        if missing:
            return [f"missing columns: {missing}"]
        if self.exact_columns and tuple(frame.columns) != self.required:
            errors.append("column set or order does not match the contract")
        for column in self.numeric:
            values = frame[column]
            if (
                not is_numeric_dtype(values.dtype)
                or values.isna().any()
                or not np.isfinite(values.to_numpy(dtype=float)).all()
            ):
                errors.append(f"{column}: expected numeric dtype with finite, non-null values")
        for column in self.identifiers:
            values = frame[column].astype("string")
            if values.isna().any() or values.str.strip().eq("").any():
                errors.append(f"{column}: expected non-null, nonblank identifiers")
        if self.date_column is not None:
            values = frame[self.date_column]
            if self.parsed_dates and not is_datetime64_any_dtype(values.dtype):
                errors.append(f"{self.date_column}: expected parsed datetime dtype")
            dates = pd.to_datetime(values, format="ISO8601", errors="coerce")
            if dates.isna().any():
                errors.append(f"{self.date_column}: invalid or missing dates")
            elif not dates.is_monotonic_increasing:
                errors.append(f"{self.date_column}: expected chronological order")
        if self.target is not None and not frame[self.target].isin([0, 1]).all():
            errors.append(f"{self.target}: expected non-null binary target")
        return errors

    def validate_frame(self, frame: pd.DataFrame) -> None:
        """Reject invalid frames before they are consumed or published."""
        errors = self.errors(frame)
        if errors:
            raise ValueError(f"{self.name} contract failed: {'; '.join(errors)}")


NORMALIZED_REQUIRED_COLUMNS = (
    "tourney_date",
    "tourney_id",
    "tourney_level",
    "surface",
    "score",
    "draw_size",
    "match_num",
    "winner_id",
    "loser_id",
    "winner_name",
    "loser_name",
    "winner_rank",
    "loser_rank",
)
NORMALIZED_MATCHES = DataFrameContract(
    name="normalized matches",
    required=NORMALIZED_REQUIRED_COLUMNS,
    identifiers=("winner_id", "loser_id", "winner_name", "loser_name"),
    date_column="tourney_date",
    parsed_dates=True,
)
