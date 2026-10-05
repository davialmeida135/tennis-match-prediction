"""Data quality checks for the historical matches pipeline.

Checks are separate from the assets on purpose: they run on the materialised
snapshot, they can be re-evaluated without recomputing anything, and a failure is
reported in the Dagster UI with the numbers that caused it.
"""

from dagster import AssetCheckResult, MetadataValue, asset_check

from ..shared.metadata import columns_present, columns_without_nulls
from ..shared.types import PandasDataFrame
from .assets import (
    curated_atp_matches,
    elo_featured_atp_matches,
    h2h_featured_atp_matches,
    imputed_atp_matches,
    normalized_atp_matches,
    raw_atp_matches,
    winrate_featured_atp_matches,
)

# Columns that must always survive the pipeline: the anonymization step selects
# exactly this schema and fails loudly when something is missing.
REQUIRED_COLUMNS = [
    "tourney_date",
    "tourney_level",
    "surface",
    "score",
    "draw_size",
    "match_num",
    "winner_id",
    "loser_id",
    "winner_rank",
    "loser_rank",
]

IMPUTED_COLUMNS = [
    "surface",
    "winner_ht",
    "loser_ht",
    "winner_age",
    "loser_age",
    "winner_rank",
    "loser_rank",
    "winner_rank_points",
    "loser_rank_points",
]

WINRATE_COLUMNS = [
    "winner_winrate",
    "loser_winrate",
    "winner_winrate_last_10",
    "loser_winrate_last_10",
    "winner_winrate_last_50",
    "loser_winrate_last_50",
    "winner_winrate_surface",
    "loser_winrate_surface",
    "winner_winrate_surface_last_10",
    "loser_winrate_surface_last_10",
    "winner_winrate_surface_last_50",
    "loser_winrate_surface_last_50",
]

ELO_COLUMNS = ["winner_elo", "loser_elo", "elo_diff"]


@asset_check(
    asset=raw_atp_matches,
    name="not_empty",
    description="The raw CSV produced at least one match.",
    blocking=True,
)
def raw_matches_not_empty(raw_atp_matches: PandasDataFrame) -> AssetCheckResult:
    row_count = len(raw_atp_matches)
    return AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": MetadataValue.int(row_count)},
    )


@asset_check(
    asset=normalized_atp_matches,
    name="required_columns_present",
    description="Normalization did not drop any column the rest of the pipeline needs.",
    blocking=True,
)
def normalized_matches_have_required_columns(
    normalized_atp_matches: PandasDataFrame,
) -> AssetCheckResult:
    passed, metadata = columns_present(normalized_atp_matches, REQUIRED_COLUMNS)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(
    asset=normalized_atp_matches,
    name="chronological_order",
    description="Matches are sorted by date, otherwise every lookback feature is wrong.",
    blocking=True,
)
def normalized_matches_are_chronological(
    normalized_atp_matches: PandasDataFrame,
) -> AssetCheckResult:
    dates = normalized_atp_matches["tourney_date"]
    is_sorted = bool(dates.is_monotonic_increasing)
    return AssetCheckResult(
        passed=is_sorted,
        metadata={
            "first_match": MetadataValue.text(str(dates.min())),
            "last_match": MetadataValue.text(str(dates.max())),
        },
    )


@asset_check(
    asset=imputed_atp_matches,
    name="imputed_columns_have_no_nulls",
    description="Imputation filled every height, age, ranking and surface value.",
)
def imputed_matches_have_no_nulls(imputed_atp_matches: PandasDataFrame) -> AssetCheckResult:
    passed, metadata = columns_without_nulls(imputed_atp_matches, IMPUTED_COLUMNS)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(
    asset=winrate_featured_atp_matches,
    name="winrates_are_probabilities",
    description="Every win-rate feature is between 0 and 1.",
)
def winrate_features_are_probabilities(
    winrate_featured_atp_matches: PandasDataFrame,
) -> AssetCheckResult:
    present = [
        column for column in WINRATE_COLUMNS if column in winrate_featured_atp_matches.columns
    ]
    offenders = {
        column: {
            "min": float(winrate_featured_atp_matches[column].min()),
            "max": float(winrate_featured_atp_matches[column].max()),
        }
        for column in present
        if winrate_featured_atp_matches[column].min() < 0
        or winrate_featured_atp_matches[column].max() > 1
    }
    return AssetCheckResult(
        passed=not offenders,
        metadata={
            "columns_checked": MetadataValue.json(present),
            "out_of_range": MetadataValue.json(offenders),
        },
    )


@asset_check(
    asset=h2h_featured_atp_matches,
    name="feature_columns_present",
    description="Win-rate and head-to-head features are available before the dataset is curated.",
)
def h2h_features_present(h2h_featured_atp_matches: PandasDataFrame) -> AssetCheckResult:
    expected = [*WINRATE_COLUMNS, "h2h"]
    passed, metadata = columns_present(h2h_featured_atp_matches, expected)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(
    asset=elo_featured_atp_matches,
    name="elo_features_present",
    description=(
        "Both player ratings and their difference are available before the dataset is curated."
    ),
)
def elo_features_present(elo_featured_atp_matches: PandasDataFrame) -> AssetCheckResult:
    passed, metadata = columns_present(elo_featured_atp_matches, ELO_COLUMNS)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(
    asset=curated_atp_matches,
    name="no_walkovers",
    description="Walkovers are excluded from the training dataset.",
)
def curated_matches_have_no_walkovers(curated_atp_matches: PandasDataFrame) -> AssetCheckResult:
    remaining = int((curated_atp_matches["score"] == "W/O").sum())
    return AssetCheckResult(
        passed=remaining == 0,
        metadata={"walkovers": MetadataValue.int(remaining)},
    )


@asset_check(
    asset=curated_atp_matches,
    name="anonymization_input_columns_present",
    description="Everything the anonymization step selects still exists in the curated dataset.",
    blocking=True,
)
def curated_matches_feed_anonymization(curated_atp_matches: PandasDataFrame) -> AssetCheckResult:
    # Imported here, not at module level: anonymization belongs to the
    # anonymized_dataset pipeline, and importing it at the top would couple the
    # two code locations.
    from ..anonymized_dataset.anonymization import MATCH_COLUMNS, PLAYER_ATTRIBUTE_STEMS

    required = [
        *MATCH_COLUMNS,
        *(f"{side}_{stem}" for stem in PLAYER_ATTRIBUTE_STEMS for side in ("winner", "loser")),
    ]
    missing = [column for column in required if column not in curated_atp_matches.columns]
    return AssetCheckResult(
        passed=not missing,
        metadata={
            "missing_columns": MetadataValue.json(missing),
            "columns_checked": MetadataValue.int(len(required)),
        },
    )
