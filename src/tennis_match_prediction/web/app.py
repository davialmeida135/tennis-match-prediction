"""Same-origin HTTP API and static frontend for personal use."""

import json
import logging
import os
from collections.abc import AsyncIterator
from contextlib import ExitStack, asynccontextmanager
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import JsonValue

from tennis_match_prediction.forecast_evaluation.archive import load_report
from tennis_match_prediction.forecast_evaluation.reporting import markdown_report
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
    performance_dir: Path | None = None,
) -> FastAPI:
    """Reload validated published snapshots; never ingest or train in HTTP requests."""

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
            application.state.history_publication = publication_bytes()
            application.state.players = PlayerService(history_path, models_dir, selected_path)
            application.state.selected_path = selected_path
            application.state.history_reload_error = None
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
    # Keep the HTML and browser assets in sync across service deployments.
    index_html = (static / "index.html").read_text(encoding="utf-8")
    for asset in ("app.js", "style.css"):
        version = sha256((static / asset).read_bytes()).hexdigest()
        index_html = index_html.replace(f"/static/{asset}", f"/static/{asset}?v={version}")
    reload_lock = Lock()
    report_directory = (
        performance_dir
        if performance_dir is not None
        else history_path.parent.parent / "performance"
    )

    def publication_bytes() -> bytes | None:
        pointer = history_path.parent / "latest_refresh.json"
        return pointer.read_bytes() if pointer.is_file() else None

    def current_services() -> tuple[PlayerService, UpcomingService]:
        with reload_lock:
            try:
                version = publication_bytes()
                if version is not None and version != application.state.history_publication:
                    players = PlayerService(
                        history_path, models_dir, application.state.selected_path
                    )
                    upcoming = UpcomingService(
                        players,
                        application.state.upcoming.directory,
                        os.getenv("LIVE_TENNIS_API_KEY"),
                    )
                    application.state.players = players
                    application.state.upcoming = upcoming
                    application.state.history_publication = version
                    application.state.history_reload_error = None
                elif version == application.state.history_publication:
                    application.state.history_reload_error = None
            except (OSError, ValueError, KeyError):
                application.state.history_reload_error = (
                    "Cannot load published history; serving previous snapshot"
                )
                logging.getLogger(__name__).warning(application.state.history_reload_error)
            return application.state.players, application.state.upcoming

    @application.get("/api/status")
    def status() -> dict[str, JsonValue]:
        return current_services()[0].status() | {
            "registry_model": application.state.registry_model,
            "history_reload_error": application.state.history_reload_error,
        }

    @application.get("/api/players")
    def search(
        q: Annotated[str, Query(max_length=100)] = "",
        limit: Annotated[int, Query(ge=1, le=100)] = 24,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> dict[str, JsonValue]:
        return current_services()[0].search(q, limit, offset)

    @application.get("/api/players/{player_id}")
    def profile(player_id: str) -> dict[str, JsonValue]:
        try:
            return current_services()[0].profile(player_id)
        except KeyError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error

    @application.post("/api/predict")
    def predict(request: MatchupRequest) -> dict[str, JsonValue]:
        try:
            return current_services()[0].predict(request)
        except KeyError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

    @application.get("/", include_in_schema=False)
    def index() -> HTMLResponse:
        return HTMLResponse(index_html, headers={"Cache-Control": "no-cache"})

    @application.get("/dagster", include_in_schema=False)
    def dagster_redirect(request: Request) -> RedirectResponse:
        return RedirectResponse(request.url.replace(port=3000, path="/", query=""))

    @application.get("/api/upcoming")
    def upcoming(model_id: Annotated[str | None, Query(max_length=100)] = None) -> dict:
        players, service = current_services()
        return service.latest(model_id or players.default_model_id)

    @application.post("/api/upcoming/refresh")
    def refresh_upcoming(request: BatchPredictionRequest) -> dict:
        try:
            return current_services()[1].refresh(request.model_id)
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

    def read_performance() -> dict:
        pointer = report_directory / "latest.json"
        if not pointer.is_file():
            raise HTTPException(
                status_code=404, detail="Run the daily update to create a performance report"
            )
        version = json.loads(pointer.read_text(encoding="utf-8"))["id"]
        if len(version) != 32 or any(c not in "0123456789abcdef" for c in version):
            raise HTTPException(status_code=503, detail="Invalid performance report version")
        directory = report_directory / "runs" / version
        return load_report(directory, application.state.upcoming.directory)

    @application.get("/api/performance")
    def performance() -> dict:
        return read_performance()

    @application.get("/api/performance/download")
    def download_performance() -> Response:
        return Response(
            markdown_report(read_performance()),
            media_type="text/markdown",
            headers={"Content-Disposition": 'attachment; filename="performance.md"'},
        )

    @application.get("/performance", include_in_schema=False)
    def performance_page() -> HTMLResponse:
        return index()

    application.mount("/static", StaticFiles(directory=static), name="static")
    return application
