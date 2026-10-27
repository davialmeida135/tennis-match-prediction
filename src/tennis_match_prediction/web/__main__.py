"""Run the personal application with `python -m tennis_match_prediction.web`."""

import argparse
import os
from pathlib import Path

import uvicorn
from dotenv import load_dotenv

from tennis_match_prediction.paths import DATA_DIR, MODELS_DIR, PLAYER_HISTORY_PATH
from tennis_match_prediction.web.app import create_app
from tennis_match_prediction.web.registry import RegistryModel

load_dotenv()


def main() -> None:
    parser = argparse.ArgumentParser(description="Local tennis profiles and matchup predictions")
    parser.add_argument("--port", type=int, default=8000)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--model-path", type=Path, help="serve only this compatible project model")
    source.add_argument("--registry-model", help="registered MLflow model name")
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument("--registry-alias", help="registry alias (default: champion)")
    selector.add_argument(
        "--registry-version",
        help="pin a positive registry version number",
    )
    parser.add_argument("--tracking-uri", help="MLflow tracking and registry server URL")
    parser.add_argument("--history-path", type=Path, default=PLAYER_HISTORY_PATH)
    parser.add_argument("--models-dir", type=Path, default=MODELS_DIR)
    parser.add_argument("--predictions-dir", type=Path, default=DATA_DIR / "predictions")
    arguments = parser.parse_args()
    if not 1 <= arguments.port <= 65535:
        parser.error("port must be between 1 and 65535")
    registry_name = arguments.registry_model or (
        os.getenv("TENNIS_WEB_REGISTRY_MODEL") if arguments.model_path is None else None
    )
    registry_model = None
    if registry_name:
        # An explicit CLI selector overrides either environment selector.
        explicit_selector = (
            arguments.registry_alias is not None or arguments.registry_version is not None
        )
        try:
            registry_model = RegistryModel(
                name=registry_name,
                tracking_uri=arguments.tracking_uri
                or os.getenv("MLFLOW_TRACKING_URI")
                or "http://127.0.0.1:5000",
                alias=(
                    arguments.registry_alias
                    if explicit_selector
                    else os.getenv("TENNIS_WEB_REGISTRY_ALIAS") or None
                ),
                version=(
                    arguments.registry_version
                    if explicit_selector
                    else os.getenv("TENNIS_WEB_REGISTRY_VERSION") or None
                ),
            )
        except ValueError as error:
            parser.error(str(error))
    elif arguments.registry_alias or arguments.registry_version or arguments.tracking_uri:
        parser.error(
            "--registry-alias, --registry-version and --tracking-uri require --registry-model"
        )
    application = create_app(
        history_path=arguments.history_path,
        models_dir=arguments.models_dir,
        model_path=arguments.model_path,
        registry_model=registry_model,
        predictions_dir=arguments.predictions_dir,
    )
    uvicorn.run(application, host="0.0.0.0", port=arguments.port)


if __name__ == "__main__":
    main()
