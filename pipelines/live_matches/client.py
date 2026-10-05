"""Minimal client for the SportDevs tennis API.

Docs: https://sportdevs.com/dashboard
The public endpoint returns every match of a given day; only the fields the
project needs are kept.
"""

import json
from dataclasses import dataclass
from typing import Any

import pandas as pd
import requests

DEFAULT_BASE_URL = "https://tennis.sportdevs.com"
MATCH_FIELDS = (
    "status",
    "start_time",
    "tournament_id",
    "tournament_name",
    "home_team_name",
    "away_team_name",
    "home_team_score",
    "away_team_score",
)


@dataclass(frozen=True)
class SportDevsClient:
    """HTTP client for `tennis.sportdevs.com`."""

    api_key: str
    base_url: str = DEFAULT_BASE_URL
    timeout: int = 30

    def fetch_matches(self, date: str) -> tuple[dict[str, Any], pd.DataFrame]:
        """Return `(raw_payload, flattened_table)` for every match played on `date`.

        `date` must be ISO formatted (YYYY-MM-DD).
        """
        url = f"{self.base_url}/matches-by-date"
        response = requests.get(
            url,
            params={"date": f"eq.{date}"},
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=self.timeout,
        )
        response.raise_for_status()

        payload = response.json()
        # The endpoint answers with a list of pages; the last one holds the matches.
        payload = payload[-1] if isinstance(payload, list) and payload else payload or {}

        matches = [payload.get("matches") or []] if isinstance(payload, dict) else []
        rows = [{field: match.get(field) for field in MATCH_FIELDS} for match in matches]
        return payload, pd.DataFrame(rows, columns=list(MATCH_FIELDS))


def dump_raw_payload(payload: dict[str, Any]) -> str:
    """Serialize the API payload exactly as it was received (for debugging)."""
    return json.dumps(payload, indent=2)
