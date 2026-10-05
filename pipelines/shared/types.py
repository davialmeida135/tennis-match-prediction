"""Shared type aliases used across the pipelines."""

from typing import TypeAlias

import pandas as pd

# `TypeAlias` rather than PEP 695 `type`: Dagster resolves asset return
# annotations through `typing.get_type_hints`, which cannot follow a lazily
# evaluated PEP 695 alias.
PandasDataFrame: TypeAlias = pd.DataFrame  # noqa: UP040
