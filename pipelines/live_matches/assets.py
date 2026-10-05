"""Asset that pulls finished matches from the SportDevs API.

This is the entry point for matches played after the historical Kaggle dataset
ends: every day is pulled as its own snapshot under `data/raw/live_matches/`,
ready to be appended to the historical dataset.

Work in progress: the API returns generic `home_team_*` / `away_team_*` fields,
so mapping them onto the historical `winner_*` / `loser_*` schema is still
pending before this data can feed the feature engineering.
"""

from datetime import date, timedelta

from dagster import AssetExecutionContext, Config, MetadataValue, asset
from pydantic import Field

from ..shared.metadata import dataframe_metadata
from ..shared.paths import LIVE_MATCHES_RAW_DIR, ensure_dir
from ..shared.types import PandasDataFrame
from .client import SportDevsClient, dump_raw_payload
from .config import SportDevsApi

GROUP = "live_matches"


class LiveMatchesQuery(Config):
    """Which day to pull."""

    match_date: str | None = Field(
        default=None,
        description="Day to pull, as YYYY-MM-DD. Defaults to yesterday.",
    )


@asset(
    group_name=GROUP,
    kinds={"api", "json", "pandas"},
    tags={"domain": "tennis", "source": "sportdevs", "layer": "raw"},
    description=(
        "Downloads the matches finished on a given day (yesterday by default) and stores the "
        "flattened table plus the raw payload under data/raw/live_matches/."
    ),
)
def live_matches_snapshot(
    context: AssetExecutionContext,
    sportdevs_api: SportDevsApi,
    config: LiveMatchesQuery,
) -> PandasDataFrame:
    """Pull one day of matches from the SportDevs API."""
    if not sportdevs_api.api_key:
        raise ValueError(
            "SPORTDEVS_API_KEY is not set. Copy .env.example to .env and fill it in to pull "
            "matches from https://sportdevs.com/dashboard."
        )

    match_date = config.match_date or (date.today() - timedelta(days=1)).isoformat()
    context.log.info(f"Pulling matches for {match_date} from {sportdevs_api.base_url}")

    client = SportDevsClient(api_key=sportdevs_api.api_key, base_url=sportdevs_api.base_url)
    payload, frame = client.fetch_matches(match_date)

    output_dir = ensure_dir(LIVE_MATCHES_RAW_DIR)
    raw_path = output_dir / f"matches_{match_date}.json"
    csv_path = output_dir / f"matches_{match_date}.csv"
    raw_path.write_text(dump_raw_payload(payload), encoding="utf-8")
    frame.to_csv(csv_path, index=False)

    context.add_output_metadata(
        {
            "match_date": MetadataValue.text(match_date),
            "raw_payload": MetadataValue.path(str(raw_path)),
            "csv_path": MetadataValue.path(str(csv_path)),
            **dataframe_metadata(frame),
        }
    )
    return frame
