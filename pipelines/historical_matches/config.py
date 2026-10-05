"""Dagster configuration of the historical matches pipeline.

Every external dependency of the pipeline is declared here as a resource, so it
can be overridden per environment without touching the assets.
"""

import os

from dagster import ConfigurableResource
from pydantic import Field

from ..shared.paths import DEFAULT_HISTORICAL_MATCHES_CSV


class RawMatchesCsv(ConfigurableResource):
    """Location of the ATP match history CSV downloaded from Kaggle.

    Sources: https://www.kaggle.com/datasets/dissfya/atp-tennis-2000-2023daily-pull
    """

    csv_path: str = Field(
        default=os.getenv("TENNIS_RAW_MATCHES_CSV") or str(DEFAULT_HISTORICAL_MATCHES_CSV),
        description="CSV with one row per finished ATP match, as published on Kaggle.",
    )
