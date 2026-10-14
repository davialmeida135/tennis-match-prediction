"""CLI validation must not wait for the prediction dependencies to load."""

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    ("arguments", "exit_code", "message"),
    [
        (["--help"], 0, "YYYY-MM-DD"),
        (["10/10/2026", "Clay"], 2, "YYYY-MM-DD"),
        (["2026-02-30", "Clay"], 2, "YYYY-MM-DD"),
        (["20261010", "Clay"], 2, "YYYY-MM-DD"),
        (["2026-10-10", "Sand"], 2, "invalid choice"),
    ],
)
def test_help_and_invalid_arguments_do_not_import_ml_stack(arguments, exit_code, message):
    script = """
import importlib.abc
import runpy
import sys

class BlockPredictionDependencies(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'pandas', 'numpy', 'sklearn', 'mlflow', 'pydantic'}:
            raise AssertionError(f'Dependency imported before CLI validation: {fullname}')

sys.meta_path.insert(0, BlockPredictionDependencies())
sys.argv = ['predict', *sys.argv[1:]]
runpy.run_module('tennis_match_prediction.ml.predict', run_name='__main__')
"""
    cli_arguments = (
        arguments
        if arguments == ["--help"]
        else ["Carlos Alcaraz", "Jannik Sinner", *arguments, "--model-path", "model.pkl"]
    )
    result = subprocess.run(
        [sys.executable, "-c", script, *cli_arguments],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == exit_code, result.stderr
    assert message in result.stdout + result.stderr
    assert "Traceback" not in result.stderr
