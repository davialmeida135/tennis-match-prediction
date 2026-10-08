"""Data quality checks for the historical matches pipeline.

Checks are separate from the assets on purpose: they run on the materialised
snapshot, they can be re-evaluated without recomputing anything, and a failure is
reported in the Dagster UI with the numbers that caused it.
"""

import pandas as pd
from dagster import AssetCheckResult, MetadataValue, asset_check

from tennis_match_prediction.contracts import NORMALIZED_REQUIRED_COLUMNS
from tennis_match_prediction.pipelines.historical_matches.assets import (
    curated_atp_matches,
    elo_featured_atp_matches,
    h2h_featured_atp_matches,
    imputed_atp_matches,
    normalized_atp_matches,
    raw_atp_matches,
    training_atp_matches,
    winrate_featured_atp_matches,
)
from tennis_match_prediction.pipelines.shared.metadata import columns_present, columns_without_nulls
from tennis_match_prediction.transforms.history import order_history
from tennis_match_prediction.transforms.training_rows import (
    FINAL_COLUMN_ORDER,
    MATCH_COLUMNS,
    PLAYER_ATTRIBUTE_STEMS,
    TARGET_COLUMN,
)

# Columns that must always survive the pipeline: the training-row construction step selects
# exactly this schema and fails loudly when something is missing.
REQUIRED_COLUMNS = list(NORMALIZED_REQUIRED_COLUMNS)

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

# player0/player1 are shuffled at random, so the target must land near 50/50.
TARGET_BALANCE_TOLERANCE = 0.05


@asset_check(
    asset=raw_atp_matches,
    name="not_empty",
    description="The raw CSV produced at least one match.",
    blocking=True,
)
def raw_matches_not_empty(raw_atp_matches: pd.DataFrame) -> AssetCheckResult:
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
    normalized_atp_matches: pd.DataFrame,
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
    normalized_atp_matches: pd.DataFrame,
) -> AssetCheckResult:
    dates = normalized_atp_matches["tourney_date"]
    is_sorted = normalized_atp_matches.index.equals(order_history(normalized_atp_matches).index)
    return AssetCheckResult(
        passed=is_sorted,
        metadata={
            "first_match": MetadataValue.text(str(dates.min())),
            "last_match": MetadataValue.text(str(dates.max())),
        },
    )


@asset_check(
    asset=normalized_atp_matches,
    name="no_walkovers",
    description="Unplayed walkovers are excluded before any history calculations.",
    blocking=True,
)
def normalized_matches_have_no_walkovers(normalized_atp_matches: pd.DataFrame) -> AssetCheckResult:
    remaining = int(normalized_atp_matches["score"].eq("W/O").sum())
    return AssetCheckResult(passed=remaining == 0, metadata={"walkovers": remaining})


@asset_check(
    asset=imputed_atp_matches,
    name="imputed_columns_have_no_nulls",
    description="Earlier observations or initial defaults filled missing player attributes.",
)
def imputed_matches_have_no_nulls(imputed_atp_matches: pd.DataFrame) -> AssetCheckResult:
    passed, metadata = columns_without_nulls(imputed_atp_matches, IMPUTED_COLUMNS)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(
    asset=winrate_featured_atp_matches,
    name="winrates_are_probabilities",
    description="Every win-rate feature is between 0 and 1.",
)
def winrate_features_are_probabilities(
    winrate_featured_atp_matches: pd.DataFrame,
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
        passed=not offenders
        and len(present) == len(WINRATE_COLUMNS)
        and not winrate_featured_atp_matches[present].isna().any().any(),
        metadata={
            "columns_checked": MetadataValue.json(present),
            "out_of_range": MetadataValue.json(offenders),
            "missing_columns": MetadataValue.json(sorted(set(WINRATE_COLUMNS).difference(present))),
        },
    )


@asset_check(
    asset=h2h_featured_atp_matches,
    name="feature_columns_present",
    description="Win-rate features and the pre-match head-to-head win difference are present.",
)
def h2h_features_present(h2h_featured_atp_matches: pd.DataFrame) -> AssetCheckResult:
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
def elo_features_present(elo_featured_atp_matches: pd.DataFrame) -> AssetCheckResult:
    passed, metadata = columns_present(elo_featured_atp_matches, ELO_COLUMNS)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(
    asset=curated_atp_matches,
    name="no_walkovers",
    description="Walkovers are excluded from the training dataset.",
)
def curated_matches_have_no_walkovers(curated_atp_matches: pd.DataFrame) -> AssetCheckResult:
    remaining = int((curated_atp_matches["score"] == "W/O").sum())
    return AssetCheckResult(
        passed=remaining == 0,
        metadata={"walkovers": MetadataValue.int(remaining)},
    )


@asset_check(
    asset=curated_atp_matches,
    name="training_input_columns_present",
    description="Every column needed to construct training rows exists in the curated dataset.",
    blocking=True,
)
def curated_matches_feed_training(curated_atp_matches: pd.DataFrame) -> AssetCheckResult:
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


@asset_check(
    asset=training_atp_matches,
    name="target_is_binary",
    description=f"`{TARGET_COLUMN}` only contains 0/1, so it can be used directly as the target.",
    blocking=True,
)
def target_is_binary(training_atp_matches: pd.DataFrame) -> AssetCheckResult:
    values = sorted(training_atp_matches[TARGET_COLUMN].dropna().unique().tolist())
    unexpected = [value for value in values if value not in (0, 1)]
    return AssetCheckResult(
        passed=not unexpected and not training_atp_matches[TARGET_COLUMN].isna().any(),
        metadata={
            "values": MetadataValue.json(values),
            "unexpected": MetadataValue.json(unexpected),
        },
    )


@asset_check(
    asset=training_atp_matches,
    name="target_is_balanced",
    description=(
        "player0/player1 are shuffled at random, so the target must be ~50/50. A skewed mean "
        "means the training-row construction step leaked the winner."
    ),
    blocking=True,
)
def target_is_balanced(training_atp_matches: pd.DataFrame) -> AssetCheckResult:
    target_mean = float(training_atp_matches[TARGET_COLUMN].mean())
    return AssetCheckResult(
        passed=abs(target_mean - 0.5) <= TARGET_BALANCE_TOLERANCE,
        metadata={
            "target_mean": MetadataValue.float(target_mean),
            "tolerance": MetadataValue.float(TARGET_BALANCE_TOLERANCE),
        },
    )


@asset_check(
    asset=training_atp_matches,
    name="no_player_identity_left",
    description=(
        "No winner_*/loser_* column survives training-row construction; the model could read the "
        f"outcome straight from the column names. The `{TARGET_COLUMN}` target itself is "
        "expected and excluded."
    ),
    blocking=True,
)
def no_player_identity_left(training_atp_matches: pd.DataFrame) -> AssetCheckResult:
    leaked = [
        column
        for column in training_atp_matches.columns
        if column != TARGET_COLUMN and column.startswith(("winner_", "loser_", "winner", "loser"))
    ]
    return AssetCheckResult(
        passed=not leaked,
        metadata={
            "leaked_columns": MetadataValue.json(leaked),
            "excluded_target": MetadataValue.text(TARGET_COLUMN),
        },
    )


@asset_check(
    asset=training_atp_matches,
    name="schema_matches_expectation",
    description="Column set and order of the player-oriented training dataset.",
    blocking=True,
)
def schema_matches_expectation(training_atp_matches: pd.DataFrame) -> AssetCheckResult:
    actual = list(training_atp_matches.columns)
    missing = [column for column in FINAL_COLUMN_ORDER if column not in actual]
    return AssetCheckResult(
        passed=not missing,
        metadata={
            "missing_columns": MetadataValue.json(missing),
            "extra_columns": MetadataValue.json(
                [column for column in actual if column not in FINAL_COLUMN_ORDER]
            ),
        },
    )
