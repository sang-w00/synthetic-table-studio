from __future__ import annotations

from collections.abc import Iterable
from contextlib import asynccontextmanager
from pathlib import Path, PurePosixPath

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, Response

from sts.jobs.runtime import UtilityJobRuntime, UtilityWorkerAdapter
from sts.storage import CatalogRepository, WorkspaceLayout

from .artifacts import ArtifactService, create_artifact_router
from .datasets import DatasetService, create_dataset_router
from .jobs import JobService, create_job_router
from .problems import install_problem_handlers
from .security import (
    LocalSecurityConfig,
    LocalSecurityMiddleware,
    bootstrap_response,
    loopback_hosts,
    loopback_origins,
)

DEFAULT_PORT = 8765
_PROJECT_ROOT = Path(__file__).resolve().parents[4]
_DEFAULT_STATIC_DIR = _PROJECT_ROOT / "web" / "dist"


def _static_file(root: Path, requested_path: str) -> Path | None:
    if "\\" in requested_path or "\x00" in requested_path:
        return None
    relative = PurePosixPath(requested_path)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        return None
    candidate = root.joinpath(*relative.parts).resolve(strict=False)
    try:
        candidate.relative_to(root)
    except ValueError:
        return None
    return candidate if candidate.is_file() else None


def create_app(
    workspace: str | Path,
    *,
    public_port: int = DEFAULT_PORT,
    allowed_hosts: Iterable[str] | None = None,
    allowed_origins: Iterable[str] | None = None,
    static_dir: str | Path | None = None,
    utility_adapter: UtilityWorkerAdapter | None = None,
) -> FastAPI:
    """Build one isolated application instance and its per-start browser session."""

    layout = WorkspaceLayout(workspace)
    repository = CatalogRepository.open_workspace(layout)
    dataset_service = DatasetService(repository, layout)
    job_service = JobService(
        repository,
        layout,
        argn_gate_required=utility_adapter is None,
    )
    job_runtime = UtilityJobRuntime(
        repository,
        layout,
        job_service,
        adapter=utility_adapter,
        worker_lease_bytes=job_service.worker_lease_bytes,
    )
    job_service.attach_runtime(job_runtime)
    artifact_service = ArtifactService(repository, layout)
    security = LocalSecurityConfig(
        allowed_hosts=loopback_hosts(public_port) if allowed_hosts is None else allowed_hosts,
        allowed_origins=(
            loopback_origins(public_port) if allowed_origins is None else allowed_origins
        ),
    )
    web_root = Path(static_dir or _DEFAULT_STATIC_DIR).expanduser().resolve(strict=False)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await job_runtime.start()
        try:
            yield
        finally:
            await job_runtime.stop()
            repository.close()

    app = FastAPI(
        title="Synthetic Table Studio",
        version="1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.workspace = layout
    app.state.repository = repository
    app.state.dataset_service = dataset_service
    app.state.job_service = job_service
    app.state.artifact_service = artifact_service
    app.state.job_runtime = job_runtime
    app.state.security = security
    app.state.static_dir = web_root

    install_problem_handlers(app)
    app.add_middleware(LocalSecurityMiddleware, config=security)

    @app.get("/api/v1/bootstrap", tags=["session"])
    def bootstrap() -> Response:
        return bootstrap_response(security)

    app.include_router(create_dataset_router(dataset_service))
    app.include_router(create_job_router(job_service))
    app.include_router(create_artifact_router(artifact_service))

    @app.api_route("/{requested_path:path}", methods=["GET", "HEAD"], include_in_schema=False)
    def static_fallback(requested_path: str) -> Response:
        if requested_path == "api" or requested_path.startswith("api/"):
            return JSONResponse(status_code=404, content={"detail": "Not Found"})

        requested = requested_path or "index.html"
        asset = _static_file(web_root, requested)
        if asset is not None:
            return FileResponse(asset)
        if PurePosixPath(requested).suffix:
            return PlainTextResponse("Not Found", status_code=404)
        index = _static_file(web_root, "index.html")
        if index is not None:
            return FileResponse(index, media_type="text/html")
        return PlainTextResponse(
            "Synthetic Table Studio web build is unavailable. Run the web production build.",
            status_code=503,
        )

    return app


def app_from_environment() -> FastAPI:
    """Small import target for ASGI runners that configure the app through environment variables."""

    import os

    workspace = os.environ.get("STS_WORKSPACE", "var/workspace")
    public_port = int(os.environ.get("STS_PUBLIC_PORT", str(DEFAULT_PORT)))
    static_dir: str | Path | None = os.environ.get("STS_STATIC_DIR")
    extra_host = os.environ.get("STS_PUBLIC_HOST")
    if extra_host:
        hosts = {*loopback_hosts(public_port), f"{extra_host.lower()}:{public_port}"}
        origins = {*loopback_origins(public_port), f"http://{extra_host}:{public_port}"}
    else:
        hosts = loopback_hosts(public_port)
        origins = loopback_origins(public_port)
    return create_app(
        workspace,
        public_port=public_port,
        allowed_hosts=hosts,
        allowed_origins=origins,
        static_dir=static_dir,
    )


__all__ = ["DEFAULT_PORT", "app_from_environment", "create_app"]
