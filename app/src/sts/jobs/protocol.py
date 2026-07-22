from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

PROTOCOL_VERSION = "1.0"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class SnapshotFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    sha256: str
    size_bytes: Annotated[int, Field(ge=0)]

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return validate_workspace_relative_path(value)

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("sha256 must be 64 lowercase hexadecimal characters")
        return value


class ManifestSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: Literal["1.0"] = PROTOCOL_VERSION
    workspace_root: str
    files: dict[str, SnapshotFile]

    @field_validator("workspace_root")
    @classmethod
    def validate_workspace_root(cls, value: str) -> str:
        if not value or not Path(value).is_absolute():
            raise ValueError("workspace_root must be an absolute path")
        return value

    @field_validator("files")
    @classmethod
    def validate_files(cls, value: dict[str, SnapshotFile]) -> dict[str, SnapshotFile]:
        if any(not key for key in value):
            raise ValueError("manifest snapshot keys must be nonempty")
        paths = [entry.path for entry in value.values()]
        if len(paths) != len(set(paths)):
            raise ValueError("manifest snapshot paths must be unique")
        return value


class WorkerRequestEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: Literal["1.0"] = PROTOCOL_VERSION
    request_id: Annotated[str, Field(min_length=1)]
    job_id: Annotated[str, Field(min_length=1)]
    attempt: Annotated[int, Field(ge=1)]
    worker_kind: Literal["argn", "dpmm", "eval"]
    operation: Annotated[str, Field(min_length=1)]
    manifest_snapshot: ManifestSnapshot
    limits: dict[str, Any]
    cancellation_path: str

    @field_validator("cancellation_path")
    @classmethod
    def validate_cancellation_path(cls, value: str) -> str:
        return validate_workspace_relative_path(value)


class WorkerEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: Literal["1.0"] = PROTOCOL_VERSION
    sequence: Annotated[int, Field(ge=1)]
    timestamp: datetime
    stage: Annotated[str, Field(min_length=1)]
    completed: Annotated[int | float, Field(ge=0)]
    total: Annotated[int | float, Field(ge=0)]
    unit: Annotated[str, Field(min_length=1)]
    message_code: Annotated[str, Field(min_length=1)]
    metrics: dict[str, Any]

    @model_validator(mode="after")
    def completed_does_not_exceed_total(self) -> WorkerEvent:
        if self.completed > self.total:
            raise ValueError("completed must not exceed total")
        return self


class WorkerError(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    code: Annotated[str, Field(min_length=1)]
    message: Annotated[str, Field(min_length=1)]
    details: dict[str, Any] = Field(default_factory=dict)


class WorkerResultEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: Literal["1.0"] = PROTOCOL_VERSION
    status: Literal["success", "failure", "cancelled"]
    artifacts: list[dict[str, Any]]
    resource_usage: dict[str, Any]
    error: WorkerError | None

    @model_validator(mode="after")
    def validate_error(self) -> WorkerResultEnvelope:
        if self.status == "success" and self.error is not None:
            raise ValueError("successful results cannot contain an error")
        if self.status == "failure" and self.error is None:
            raise ValueError("failure results require an error")
        return self


class ResolvedSnapshotFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    name: str
    path: Path
    sha256: str
    size_bytes: int


def validate_workspace_relative_path(value: str) -> str:
    if not value or "\\" in value or "\x00" in value:
        raise ValueError("path must be a nonempty POSIX workspace-relative path")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or value in (".", "..")
        or any(part in ("", ".", "..") for part in path.parts)
    ):
        raise ValueError("path must not be absolute or contain traversal components")
    return path.as_posix()


def canonical_json_bytes(value: BaseModel | dict[str, Any]) -> bytes:
    payload = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    return json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _resolved_workspace_root(snapshot: ManifestSnapshot) -> Path:
    root = Path(snapshot.workspace_root).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("workspace_root must resolve to a directory")
    return root


def confined_existing_path(workspace_root: Path, relative_path: str) -> Path:
    relative = validate_workspace_relative_path(relative_path)
    root = workspace_root.resolve(strict=True)
    candidate = (root / relative).resolve(strict=True)
    if not candidate.is_relative_to(root):
        raise ValueError(f"workspace path escapes root: {relative}")
    return candidate


def confined_output_path(workspace_root: Path, relative_path: str) -> Path:
    relative = validate_workspace_relative_path(relative_path)
    root = workspace_root.resolve(strict=True)
    candidate = root / relative
    parent = candidate.parent.resolve(strict=True)
    if not parent.is_relative_to(root):
        raise ValueError(f"workspace output path escapes root: {relative}")
    if candidate.is_symlink():
        raise ValueError(f"workspace output path must not be a symlink: {relative}")
    if candidate.exists() and not candidate.resolve(strict=True).is_relative_to(root):
        raise ValueError(f"workspace output path escapes root: {relative}")
    return candidate


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_manifest_snapshot(snapshot: ManifestSnapshot) -> dict[str, ResolvedSnapshotFile]:
    root = _resolved_workspace_root(snapshot)
    resolved: dict[str, ResolvedSnapshotFile] = {}
    for name, entry in snapshot.files.items():
        path = confined_existing_path(root, entry.path)
        if not path.is_file():
            raise ValueError(f"snapshot entry is not a regular file: {entry.path}")
        actual_size = path.stat().st_size
        if actual_size != entry.size_bytes:
            raise ValueError(
                f"snapshot size mismatch for {entry.path}: expected "
                f"{entry.size_bytes}, got {actual_size}"
            )
        actual_sha256 = _sha256_file(path)
        if actual_sha256 != entry.sha256:
            raise ValueError(f"snapshot SHA-256 mismatch for {entry.path}")
        resolved[name] = ResolvedSnapshotFile(
            name=name,
            path=path,
            sha256=actual_sha256,
            size_bytes=actual_size,
        )
    return resolved


def read_request(path: Path) -> WorkerRequestEnvelope:
    return WorkerRequestEnvelope.model_validate_json(path.read_bytes())


def read_result(path: Path) -> WorkerResultEnvelope:
    return WorkerResultEnvelope.model_validate_json(path.read_bytes())


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("short write")
        view = view[written:]


def fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_result_atomic(path: Path, result: WorkerResultEnvelope) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(f"{path.name}.part")
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(part, flags, 0o600)
    try:
        _write_all(fd, canonical_json_bytes(result))
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        os.replace(part, path)
        fsync_directory(path.parent)
    except BaseException:
        try:
            part.unlink(missing_ok=True)
        finally:
            raise


class WorkerEventWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._last_sequence = self._read_last_sequence()

    def _read_last_sequence(self) -> int:
        if not self.path.exists():
            return 0
        last = 0
        with self.path.open("rb") as handle:
            for line in handle:
                if line.strip():
                    event = WorkerEvent.model_validate_json(line)
                    if event.sequence <= last:
                        raise ValueError("existing worker events are not strictly monotonic")
                    last = event.sequence
        return last

    def append(self, event: WorkerEvent) -> None:
        if event.sequence != self._last_sequence + 1:
            raise ValueError(
                f"event sequence must be {self._last_sequence + 1}, got {event.sequence}"
            )
        data = canonical_json_bytes(event) + b"\n"
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            _write_all(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        self._last_sequence = event.sequence
