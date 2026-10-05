"""Data quality checks for the anonymized training dataset."""

from dagster import AssetCheckResult, MetadataValue, asset_check

from ..shared.types import PandasDataFrame
from .anonymization import FINAL_COLUMN_ORDER, TARGET_COLUMN
from .assets import anonymized_atp_matches

TARGET_BALANCE_TOLERANCE = 0.05


@asset_check(
    asset=anonymized_atp_matches,
    name="target_is_binary",
    description="`winner` only contains 0/1, so it can be used directly as the model target.",
    blocking=True,
)
def target_is_binary(anonymized_atp_matches: PandasDataFrame) -> AssetCheckResult:
    values = sorted(anonymized_atp_matches[TARGET_COLUMN].dropna().unique().tolist())
    unexpected = [value for value in values if value not in (0, 1)]
    return AssetCheckResult(
        passed=not unexpected,
        metadata={
            "values": MetadataValue.json(values),
            "unexpected": MetadataValue.json(unexpected),
        },
    )


@asset_check(
    asset=anonymized_atp_matches,
    name="target_is_balanced",
    description=(
        "player0/player1 are shuffled at random, so the target must be ~50/50. A skewed mean "
        "means the anonymization step leaked the winner."
    ),
    blocking=True,
)
def target_is_balanced(anonymized_atp_matches: PandasDataFrame) -> AssetCheckResult:
    target_mean = float(anonymized_atp_matches[TARGET_COLUMN].mean())
    return AssetCheckResult(
        passed=abs(target_mean - 0.5) <= TARGET_BALANCE_TOLERANCE,
        metadata={
            "target_mean": MetadataValue.float(target_mean),
            "tolerance": MetadataValue.float(TARGET_BALANCE_TOLERANCE),
        },
    )


@asset_check(
    asset=anonymized_atp_matches,
    name="no_player_identity_left",
    description=(
        "No winner_*/loser_* column survives anonymization, otherwise the model could read the "
        "outcome straight from the column names."
    ),
    blocking=True,
)
def no_player_identity_left(anonymized_atp_matches: PandasDataFrame) -> AssetCheckResult:
    leaked = [
        column
        for column in anonymized_atp_matches.columns
        if column.startswith(("winner_", "loser_", "winner", "loser"))
    ]
    return AssetCheckResult(
        passed=not leaked,
        metadata={"leaked_columns": MetadataValue.json(leaked)},
    )


@asset_check(
    asset=anonymized_atp_matches,
    name="schema_matches_expectation",
    description="Column set and order of the published training dataset.",
    blocking=True,
)
def schema_matches_expectation(anonymized_atp_matches: PandasDataFrame) -> AssetCheckResult:
    actual = list(anonymized_atp_matches.columns)
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
