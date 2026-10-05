"""Configuration of the live matches ingestion."""

import os

from dagster import ConfigurableResource
from pydantic import Field

from .client import DEFAULT_BASE_URL


class SportDevsApi(ConfigurableResource):
    """Credentials and endpoint of the SportDevs tennis API."""

    api_key: str = Field(
        default=os.getenv("SPORTDEVS_API_KEY", ""),
        description="SportDevs API key. Materializing the asset fails when it is empty.",
    )
    base_url: str = Field(
        default=os.getenv("SPORTDEVS_BASE_URL", DEFAULT_BASE_URL),
        description="Base URL of the tennis API.",
    )
