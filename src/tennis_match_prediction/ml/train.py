"""Python and CLI entry points for reproducible model experiments."""

import argparse
import json
from datetime import date
from pathlib import Path

from pydantic import JsonValue

from tennis_match_prediction.contracts import ExperimentConfig, ExperimentResult
from tennis_match_prediction.ml.config import (
    TEST_START,
    TRAIN_START,
    TRAINING_FEATURE_COLUMNS,
    TRAINING_MATCHES_PATH,
    VALIDATION_START,
    tracking_settings,
)
from tennis_match_prediction.ml.experiment import run_experiment
from tennis_match_prediction.paths import MODELS_DIR


def train(
    training_matches_path: Path = TRAINING_MATCHES_PATH,
    model_path: Path | None = None,
    *,
    train_start: date = TRAIN_START,
    validation_start: date = VALIDATION_START,
    test_start: date = TEST_START,
    feature_columns: tuple[str, ...] = TRAINING_FEATURE_COLUMNS,
    model_name: str = "logistic_regression",
    model_params: dict[str, JsonValue] | None = None,
    output_dir: Path = MODELS_DIR,
    final_evaluation: bool = False,
    tracking_mode: str | None = None,
    tracking_uri: str | None = None,
    experiment_name: str | None = None,
    run_name: str | None = None,
) -> ExperimentResult:
    """Fit on training rows; score validation, and optionally the final test period."""
    return run_experiment(
        ExperimentConfig(
            training_matches_path=training_matches_path.resolve(),
            model_path=model_path.resolve() if model_path is not None else None,
            output_dir=output_dir.resolve(),
            train_start=train_start,
            validation_start=validation_start,
            test_start=test_start,
            feature_columns=feature_columns,
            model_name=model_name,
            model_params={} if model_params is None else model_params,
            final_evaluation=final_evaluation,
            run_name=run_name,
            tracking=tracking_settings(
                mode=tracking_mode, uri=tracking_uri, experiment_name=experiment_name
            ),
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("training_matches", type=Path, nargs="?", default=TRAINING_MATCHES_PATH)
    parser.add_argument("--model-path", type=Path, help="Explicit unused output path")
    parser.add_argument("--output-dir", type=Path, default=MODELS_DIR)
    parser.add_argument("--model", default="logistic_regression")
    parser.add_argument("--params-file", type=Path, help="JSON object of model parameters")
    parser.add_argument("--train-start", type=date.fromisoformat, default=TRAIN_START)
    parser.add_argument("--validation-start", type=date.fromisoformat, default=VALIDATION_START)
    parser.add_argument("--test-start", type=date.fromisoformat, default=TEST_START)
    parser.add_argument("--features", nargs="+", default=TRAINING_FEATURE_COLUMNS)
    parser.add_argument("--final-evaluation", action="store_true")
    parser.add_argument("--tracking-mode", choices=("server", "local", "disabled"))
    parser.add_argument("--tracking-uri")
    parser.add_argument("--experiment-name")
    parser.add_argument("--run-name")
    arguments = parser.parse_args()
    parameters = (
        json.loads(arguments.params_file.read_text(encoding="utf-8"))
        if arguments.params_file is not None
        else {}
    )
    if not isinstance(parameters, dict):
        parser.error("--params-file must contain a JSON object")
    result = train(
        arguments.training_matches,
        arguments.model_path,
        train_start=arguments.train_start,
        validation_start=arguments.validation_start,
        test_start=arguments.test_start,
        feature_columns=tuple(arguments.features),
        model_name=arguments.model,
        model_params=parameters,
        output_dir=arguments.output_dir,
        final_evaluation=arguments.final_evaluation,
        tracking_mode=arguments.tracking_mode,
        tracking_uri=arguments.tracking_uri,
        experiment_name=arguments.experiment_name,
        run_name=arguments.run_name,
    )
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
