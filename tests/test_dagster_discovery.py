"""Verify the workspace entry point loads through Dagster's autodiscovery."""

from dagster import Definitions
from dagster._core.workspace.autodiscovery import loadable_targets_from_python_module


def test_workspace_discovers_only_combined_definitions():
    targets = loadable_targets_from_python_module(
        "tennis_match_prediction.pipelines.defs", working_directory=None
    )

    assert len(targets) == 1
    assert targets[0].attribute == "defs"
    Definitions.validate_loadable(targets[0].target_definition)
