"""Run the personal application with `python -m tennis_match_prediction.web`."""

import argparse
from pathlib import Path

import uvicorn

from tennis_match_prediction.paths import MODELS_DIR, PLAYER_HISTORY_PATH
from tennis_match_prediction.web.app import create_app


def main() -> None:
    parser = argparse.ArgumentParser(description="Local tennis profiles and matchup predictions")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--model-path", type=Path, help="serve only this compatible project model")
    parser.add_argument("--history-path", type=Path, default=PLAYER_HISTORY_PATH)
    parser.add_argument("--models-dir", type=Path, default=MODELS_DIR)
    arguments = parser.parse_args()
    if not 1 <= arguments.port <= 65535:
        parser.error("port must be between 1 and 65535")
    application = create_app(
        history_path=arguments.history_path,
        models_dir=arguments.models_dir,
        model_path=arguments.model_path,
    )
    uvicorn.run(application, host="127.0.0.1", port=arguments.port)


if __name__ == "__main__":
    main()
