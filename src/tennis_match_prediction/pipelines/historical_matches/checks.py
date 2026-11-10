"""Data quality checks for the historical matches pipeline.

Checks are separate from the assets on purpose: they run on the materialised
snapshot, they can be re-evaluated without recomputing anything, and a failure is
reported in the Dagster UI with the numbers that caused it.
"""

import pandas as pd
from dagster import AssetCheckResult, MetadataValue, asset_check

from tennis_match_prediction.contracts.datasets import (
    NORMALIZED_REQUIRED_COLUMNS,
    DataFrameContract,
)
from tennis_match_prediction.contracts.features import PLAYER_COMPARISON_FEATURE_COLUMNS
from tennis_match_prediction.pipelines.historical_matches.assets import (
    curated_atp_matches,
    normalized_atp_matches,
    raw_atp_matches,
    training_atp_matches,
)
from tennis_match_prediction.pipelines.shared.metadata import columns_present
from tennis_match_prediction.transforms.match_preparation import order_history
from tennis_match_prediction.transforms.training_rows import (
    FINAL_COLUMN_ORDER,
    MATCH_COLUMNS,
    TARGET_COLUMN,
)

# Columns that must always survive the pipeline: the training-row construction step selects
# exactly this schema and fails loudly when something is missing.
REQUIRED_COLUMNS = list(NORMALIZED_REQUIRED_COLUMNS)

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
    required = MATCH_COLUMNS
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
    errors = DataFrameContract(
        name="player-oriented training matches",
        required=tuple(FINAL_COLUMN_ORDER),
        numeric=PLAYER_COMPARISON_FEATURE_COLUMNS,
        date_column="tourney_date",
        target=TARGET_COLUMN,
        exact_columns=True,
    ).errors(training_atp_matches)
    return AssetCheckResult(passed=not errors, metadata={"errors": MetadataValue.json(errors)})
