from __future__ import annotations

import json
import mimetypes
import os
import stat
from collections.abc import Iterator
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import quote
from uuid import UUID

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from sts.domain import ArtifactManifest, DomainError, ErrorCode
from sts.storage import CatalogRepository, WorkspaceLayout
from sts.storage.repository import ArtifactScope

_CHUNK_SIZE = 1024 * 1024
_REPORT_KINDS = {
    "primary_utility": "primary_report_json",
    "primary_dp": "primary_report_json",
    "release": "dp_release_report_json",
    "internal": "internal_diagnostic_report_json",
}


def _manifest_json(manifest: ArtifactManifest) -> dict[str, Any]:
    return manifest.model_dump(mode="json")


def _range_bounds(value: str, size: int) -> tuple[int, int]:
    if not value.startswith("bytes=") or "," in value:
        raise ValueError("only a single bytes range is supported")
    specification = value.removeprefix("bytes=").strip()
    if "-" not in specification:
        raise ValueError("invalid byte range")
    start_text, end_text = specification.split("-", 1)
    if not start_text:
        try:
            suffix = int(end_text)
        except ValueError as error:
            raise ValueError("invalid suffix byte range") from error
        if suffix <= 0 or size == 0:
            raise ValueError("unsatisfiable suffix byte range")
        start = max(0, size - suffix)
        return start, size - 1
    try:
        start = int(start_text)
        end = size - 1 if not end_text else int(end_text)
    except ValueError as error:
        raise ValueError("invalid byte range") from error
    if start < 0 or end < start or start >= size:
        raise ValueError("unsatisfiable byte range")
    return start, min(end, size - 1)


def _stream_descriptor(descriptor: int, start: int, length: int) -> Iterator[bytes]:
    remaining = length
    try:
        os.lseek(descriptor, start, os.SEEK_SET)
        while remaining:
            chunk = os.read(descriptor, min(_CHUNK_SIZE, remaining))
            if not chunk:
                raise OSError("artifact ended before its manifested size")
            remaining -= len(chunk)
            yield chunk
    finally:
        os.close(descriptor)


class ArtifactService:
    """Artifact/report projection that never infers release safety from a filename."""

    def __init__(self, repository: CatalogRepository, workspace: WorkspaceLayout) -> None:
        self.repository = repository
        self.workspace = workspace
        self.workspace.initialize()

    def _artifact_path(self, manifest: ArtifactManifest) -> Path:
        try:
            relative = PurePosixPath(manifest.relative_path)
            if (
                relative.is_absolute()
                or not relative.parts
                or any(part in {"", ".", ".."} for part in relative.parts)
            ):
                raise ValueError("invalid relative path")
            if relative.name.endswith(".part"):
                raise DomainError(
                    ErrorCode.ARTIFACT_NOT_READY,
                    "partial artifact files are never downloadable",
                )
            if manifest.job_id is not None:
                owner_prefix = (
                    "jobs",
                    str(manifest.job_id),
                    f"attempt-{manifest.attempt}",
                )
            elif manifest.dataset_id is not None:
                owner_prefix = ("datasets", str(manifest.dataset_id))
            else:
                raise ValueError("artifact has no owner")
            if relative.parts[: len(owner_prefix)] != owner_prefix:
                raise ValueError("artifact path is outside its immutable owner directory")
            raw_path = self.workspace.root.joinpath(*relative.parts)
            resolved = raw_path.resolve(strict=True)
            resolved.relative_to(self.workspace.root)
            if raw_path.is_symlink():
                raise ValueError("artifact path cannot be a symbolic link")
            return raw_path
        except DomainError:
            raise
        except (OSError, ValueError) as error:
            raise DomainError(
                ErrorCode.ARTIFACT_NOT_READY,
                "artifact file is unavailable or outside its immutable owner directory",
            ) from error

    def _open_regular(self, manifest: ArtifactManifest) -> int:
        path = self._artifact_path(manifest)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
            details = os.fstat(descriptor)
            if not stat.S_ISREG(details.st_mode):
                raise OSError("artifact is not a regular file")
            if details.st_size != manifest.size_bytes:
                raise DomainError(
                    ErrorCode.CHECKSUM_MISMATCH,
                    "artifact size no longer matches its stored manifest",
                    context={
                        "expected_size": manifest.size_bytes,
                        "actual_size": details.st_size,
                    },
                )
            return descriptor
        except Exception:
            if "descriptor" in locals():
                os.close(descriptor)
            raise

    def list_job_artifacts(
        self,
        job_id: UUID | str,
        scope: ArtifactScope | str,
    ) -> dict[str, Any]:
        artifact_scope = ArtifactScope(scope)
        self.repository.get_job(job_id)
        manifests = self.repository.list_artifacts(job_id=job_id, scope=artifact_scope)
        if artifact_scope is ArtifactScope.DP_RELEASE:
            # Defense in depth: this exact predicate is the only DP release projection.
            manifests = tuple(
                manifest
                for manifest in manifests
                if manifest.release_safe and not manifest.contains_private_source_information
            )
        return {
            "job_id": str(job_id),
            "scope": artifact_scope.value,
            "artifacts": [_manifest_json(manifest) for manifest in manifests],
        }

    def _report_manifest(self, job_id: UUID | str, report: str) -> ArtifactManifest:
        request = self.repository.get_job_request(job_id).value
        if report == "release" and request.mode != "differential_privacy":
            raise DomainError(
                ErrorCode.REPORT_NOT_RELEASE_SAFE,
                "utility jobs do not have a formal-DP release report",
            )
        if report == "primary":
            key = "primary_dp" if request.mode == "differential_privacy" else "primary_utility"
        else:
            key = report
        kind = _REPORT_KINDS[key]
        manifests = self.repository.list_artifacts(job_id=job_id, scope=ArtifactScope.INTERNAL)
        selected = next((item for item in reversed(manifests) if item.kind == kind), None)
        if selected is None:
            code = (
                ErrorCode.REPORT_NOT_RELEASE_SAFE
                if report == "release"
                else ErrorCode.ARTIFACT_NOT_READY
            )
            raise DomainError(code, f"{report} report is not available")
        if report == "release" and (
            not selected.release_safe or selected.contains_private_source_information
        ):
            raise DomainError(
                ErrorCode.REPORT_NOT_RELEASE_SAFE,
                "report does not satisfy the exact DP release-safety predicate",
            )
        if report == "internal" and selected.release_safe:
            raise DomainError(
                ErrorCode.ARTIFACT_NOT_READY,
                "internal diagnostic report has an invalid safety classification",
            )
        return selected

    def read_report(self, job_id: UUID | str, report: str) -> JSONResponse:
        manifest = self._report_manifest(job_id, report)
        descriptor = self._open_regular(manifest)
        try:
            chunks: list[bytes] = []
            remaining = manifest.size_bytes
            while remaining:
                chunk = os.read(descriptor, min(_CHUNK_SIZE, remaining))
                if not chunk:
                    raise DomainError(
                        ErrorCode.CHECKSUM_MISMATCH,
                        "report ended before its manifested size",
                    )
                chunks.append(chunk)
                remaining -= len(chunk)
            try:
                payload = json.loads(b"".join(chunks))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise DomainError(
                    ErrorCode.OUTPUT_INVALID,
                    "stored report JSON is invalid",
                ) from error
        finally:
            os.close(descriptor)
        return JSONResponse(content=payload)

    def download(self, artifact_id: UUID | str, range_header: str | None) -> StreamingResponse:
        manifest = self.repository.get_artifact(artifact_id)
        if not manifest.downloadable:
            raise DomainError(
                ErrorCode.ARTIFACT_NOT_READY,
                "artifact is internal or has not been published for download",
            )
        descriptor = self._open_regular(manifest)
        size = manifest.size_bytes
        start = 0
        end = size - 1
        response_status = 200
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(size),
            "Content-Disposition": (
                f"attachment; filename*=UTF-8''{quote(PurePosixPath(manifest.relative_path).name)}"
            ),
            "ETag": f'"{manifest.sha256}"',
        }
        if range_header is not None:
            try:
                start, end = _range_bounds(range_header.strip(), size)
            except ValueError:
                os.close(descriptor)
                return StreamingResponse(
                    iter(()),
                    status_code=416,
                    headers={
                        "Accept-Ranges": "bytes",
                        "Content-Range": f"bytes */{size}",
                        "Content-Length": "0",
                    },
                )
            response_status = 206
            headers["Content-Range"] = f"bytes {start}-{end}/{size}"
            headers["Content-Length"] = str(end - start + 1)
        media_type = mimetypes.guess_type(PurePosixPath(manifest.relative_path).name)[0]
        return StreamingResponse(
            _stream_descriptor(descriptor, start, end - start + 1),
            status_code=response_status,
            media_type=media_type or "application/octet-stream",
            headers=headers,
        )


def create_artifact_router(service: ArtifactService) -> APIRouter:
    router = APIRouter(tags=["artifacts", "reports"])

    @router.get("/api/v1/jobs/{job_id}/artifacts")
    def list_artifacts(
        job_id: UUID,
        scope: ArtifactScope = ArtifactScope.DOWNLOADABLE,
    ) -> dict[str, Any]:
        return service.list_job_artifacts(job_id, scope)

    @router.get("/api/v1/jobs/{job_id}/reports/primary")
    def primary_report(job_id: UUID) -> Response:
        return service.read_report(job_id, "primary")

    @router.get("/api/v1/jobs/{job_id}/reports/release")
    def release_report(job_id: UUID) -> Response:
        return service.read_report(job_id, "release")

    @router.get("/api/v1/jobs/{job_id}/reports/internal")
    def internal_report(job_id: UUID) -> Response:
        return service.read_report(job_id, "internal")

    @router.get("/api/v1/artifacts/{artifact_id}/download")
    def download_artifact(artifact_id: UUID, request: Request) -> Response:
        return service.download(artifact_id, request.headers.get("Range"))

    return router
