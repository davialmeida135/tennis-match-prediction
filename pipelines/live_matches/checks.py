"""Data quality checks for the live matches snapshots."""

from dagster import AssetCheckResult, MetadataValue, asset_check

from ..shared.types import PandasDataFrame
from .assets import live_matches_snapshot

# The API always returns these fields for a finished match. `match_status` is
# intentionally not required: unfinished matches are filtered out later, and the
# snapshot also keeps retirements and walkovers.
EXPECTED_COLUMNS = [
    "match_id",
    "match_date",
    "match_status",
    "start_time",
    "tournament_id",
    "tournament_name",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "home_team_score",
    "away_team_score",
]


@asset_check(
    asset=live_matches_snapshot,
    description="The pulled snapshot exposes the columns the mapping step relies on.",
    blocking=True,
)
def live_matches_have_expected_columns(live_matches_snapshot: PandasDataFrame) -> AssetCheckResult:
    """Guard against API schema drift before anything downstream reads the snapshot."""
    missing = [column for column in EXPECTED_COLUMNS if column not in live_matches_snapshot.columns]
    return AssetCheckResult(
        passed=not missing,
        metadata={
            "expected_columns": MetadataValue.json(EXPECTED_COLUMNS),
            "missing_columns": missing,
            "rows": len(live_matches_snapshot),
        },
    )


@asset_check(
    asset=live_matches_snapshot,
    description="A day with no finished matches is suspicious, but not an error.",
    blocking=False,
)
def live_matches_not_empty(live_matches_snapshot: PandasDataFrame) -> AssetCheckResult:
    """Warn on empty days: tournaments are on winter break, holidays, etc."""
    return AssetCheckResult(
        passed=len(live_matches_snapshot) > 0,
        metadata={"rows": len(live_matches_snapshot)},
    )


@asset_check(
    asset=live_matches_snapshot,
    description="Every row belongs to a single day, the one that was requested.",
    blocking=True,
)
def live_matches_are_from_one_day(live_matches_snapshot: PandasDataFrame) -> AssetCheckResult:
    """A single snapshot must not mix several days, otherwise it duplicates rows."""
    if live_matches_snapshot.empty or "match_date" not in live_matches_snapshot.columns:
        return AssetCheckResult(
            passed=True, metadata={"note": "empty snapshot or no match_date column"}
        )

    dates = sorted({str(value) for value in live_matches_snapshot["match_date"].dropna()})
    return AssetCheckResult(
        passed=len(dates) == 1,
        metadata={"distinct_match_dates": MetadataValue.json(dates)},
    )


@asset_check(
    asset=live_matches_snapshot,
    description="Both players are named on every finished match.",
    blocking=False,
)
def live_matches_have_both_players(live_matches_snapshot: PandasDataFrame) -> AssetCheckResult:
    """Missing names usually mean the tournament changed the API representation."""
    if live_matches_snapshot.empty:
        return AssetCheckResult(passed=True, metadata={"note": "empty snapshot"})

    unnamed = live_matches_snapshot[
        live_matches_snapshot["home_team_name"].isna()
        | live_matches_snapshot["away_team_name"].isna()
    ]
    return AssetCheckResult(
        passed=unnamed.empty,
        metadata={
            "rows_without_player_names": len(unnamed),
            "rows": len(live_matches_snapshot),
        },
    )
