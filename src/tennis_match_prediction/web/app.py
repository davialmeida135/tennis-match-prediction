"""Same-origin HTTP API and static frontend for personal use."""

import os
from collections.abc import AsyncIterator
from contextlib import ExitStack, asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import JsonValue

from tennis_match_prediction.web.contracts import BatchPredictionRequest, MatchupRequest
from tennis_match_prediction.web.registry import RegistryModel, download_registry_model
from tennis_match_prediction.web.service import PlayerService
from tennis_match_prediction.web.upcoming import UpcomingService


def create_app(
    *,
    history_path: Path,
    models_dir: Path,
    model_path: Path | None,
    registry_model: RegistryModel | None = None,
    predictions_dir: Path | None = None,
) -> FastAPI:
    """Load history once at startup, never run ingestion or training in requests."""

    if model_path is not None and registry_model is not None:
        raise ValueError("Choose a local model path or a registry model, not both")

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        with ExitStack() as stack:
            selected_path = model_path
            application.state.registry_model = None
            if registry_model is not None:
                directory = stack.enter_context(TemporaryDirectory(prefix="tennis-web-model-"))
                selected_path, provenance = download_registry_model(registry_model, Path(directory))
                application.state.registry_model = provenance
            application.state.players = PlayerService(history_path, models_dir, selected_path)
            application.state.upcoming = UpcomingService(
                application.state.players,
                predictions_dir
                if predictions_dir is not None
                else history_path.parent / "upcoming_predictions",
                os.getenv("LIVE_TENNIS_API_KEY"),
            )
            yield

    application = FastAPI(title="Court Report", lifespan=lifespan)
    static = Path(__file__).parent / "static"

    @application.get("/api/status")
    def status() -> dict[str, JsonValue]:
        return application.state.players.status() | {
            "registry_model": application.state.registry_model
        }

    @application.get("/api/players")
    def search(
        q: Annotated[str, Query(max_length=100)] = "",
        limit: Annotated[int, Query(ge=1, le=100)] = 24,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> dict[str, JsonValue]:
        return application.state.players.search(q, limit, offset)

    @application.get("/api/players/{player_id}")
    def profile(player_id: str) -> dict[str, JsonValue]:
        try:
            return application.state.players.profile(player_id)
        except KeyError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error

    @application.post("/api/predict")
    def predict(request: MatchupRequest) -> dict[str, JsonValue]:
        try:
            return application.state.players.predict(request)
        except KeyError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

    @application.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(static / "index.html")

    @application.get("/api/upcoming")
    def upcoming(model_id: Annotated[str | None, Query(max_length=100)] = None) -> dict:
        return application.state.upcoming.latest(
            model_id or application.state.players.default_model_id
        )

    @application.post("/api/upcoming/refresh")
    def refresh_upcoming(request: BatchPredictionRequest) -> dict:
        try:
            return application.state.upcoming.refresh(request.model_id)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except (RuntimeError, OSError) as error:
            raise HTTPException(status_code=503, detail=str(error)) from error

    @application.get("/api/upcoming/batches/{batch_id}")
    def download_batch(batch_id: str) -> JSONResponse:
        try:
            batch = application.state.upcoming.read_batch(batch_id)
        except FileNotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return JSONResponse(
            batch,
            headers={
                "Content-Disposition": f'attachment; filename="predictions-{batch_id[:12]}.json"'
            },
        )

    application.mount("/static", StaticFiles(directory=static), name="static")
    return application
