"""Same-origin HTTP API and static frontend for personal use."""

from collections.abc import AsyncIterator
from contextlib import ExitStack, asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import JsonValue

from tennis_match_prediction.web.contracts import MatchupRequest
from tennis_match_prediction.web.registry import RegistryModel, download_registry_model
from tennis_match_prediction.web.service import PlayerService


def create_app(
    *,
    history_path: Path,
    models_dir: Path,
    model_path: Path | None,
    registry_model: RegistryModel | None = None,
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

    application.mount("/static", StaticFiles(directory=static), name="static")
    return application
