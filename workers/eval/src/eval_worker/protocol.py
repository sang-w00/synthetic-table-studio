from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

PROTOCOL_VERSION = "1.0"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _keys(value: Mapping[str, Any], expected: set[str], kind: str) -> None:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError(f"invalid {kind} fields")


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a nonempty string")
    return value


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{field} must be a JSON object")
    canonical_json_bytes(value)
    return dict(value)


def validate_workspace_relative_path(value: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise ValueError("path must be a nonempty POSIX workspace-relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or value in (".", "..") or any(part in ("", ".", "..") for part in path.parts):
        raise ValueError("path must not be absolute or contain traversal components")
    return path.as_posix()


@dataclass(frozen=True)
class SnapshotFile:
    path: str
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        validate_workspace_relative_path(self.path)
        if not isinstance(self.sha256, str) or not _SHA256_RE.fullmatch(self.sha256):
            raise ValueError("sha256 must be 64 lowercase hexadecimal characters")
        if isinstance(self.size_bytes, bool) or not isinstance(self.size_bytes, int) or self.size_bytes < 0:
            raise ValueError("size_bytes must be a nonnegative integer")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SnapshotFile":
        _keys(value, {"path", "sha256", "size_bytes"}, "snapshot file")
        return cls(value["path"], value["sha256"], value["size_bytes"])

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "sha256": self.sha256, "size_bytes": self.size_bytes}


@dataclass(frozen=True)
class ManifestSnapshot:
    version: str
    workspace_root: str
    files: dict[str, SnapshotFile]

    def __post_init__(self) -> None:
        if self.version != PROTOCOL_VERSION:
            raise ValueError("unsupported manifest snapshot version")
        if not isinstance(self.workspace_root, str) or not Path(self.workspace_root).is_absolute():
            raise ValueError("workspace_root must be an absolute path")
        if not isinstance(self.files, dict) or any(not key for key in self.files):
            raise ValueError("files must be an object with nonempty keys")
        paths = [entry.path for entry in self.files.values()]
        if len(paths) != len(set(paths)):
            raise ValueError("manifest snapshot paths must be unique")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ManifestSnapshot":
        _keys(value, {"version", "workspace_root", "files"}, "manifest snapshot")
        raw = value["files"]
        if not isinstance(raw, dict):
            raise ValueError("manifest snapshot files must be an object")
        return cls(value["version"], value["workspace_root"], {_string(k, "manifest key"): SnapshotFile.from_dict(v) for k, v in raw.items()})

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.version, "workspace_root": self.workspace_root, "files": {key: value.to_dict() for key, value in self.files.items()}}


@dataclass(frozen=True)
class WorkerRequestEnvelope:
    version: str
    request_id: str
    job_id: str
    attempt: int
    worker_kind: str
    operation: str
    manifest_snapshot: ManifestSnapshot
    limits: dict[str, Any]
    cancellation_path: str

    def __post_init__(self) -> None:
        if self.version != PROTOCOL_VERSION:
            raise ValueError("unsupported worker request version")
        _string(self.request_id, "request_id")
        _string(self.job_id, "job_id")
        if isinstance(self.attempt, bool) or not isinstance(self.attempt, int) or self.attempt < 1:
            raise ValueError("attempt must be an integer >= 1")
        if self.worker_kind not in {"argn", "dpmm", "eval"}:
            raise ValueError("unsupported worker_kind")
        _string(self.operation, "operation")
        _object(self.limits, "limits")
        validate_workspace_relative_path(self.cancellation_path)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerRequestEnvelope":
        _keys(value, {"version", "request_id", "job_id", "attempt", "worker_kind", "operation", "manifest_snapshot", "limits", "cancellation_path"}, "worker request")
        return cls(
            value["version"], value["request_id"], value["job_id"], value["attempt"],
            value["worker_kind"], value["operation"], ManifestSnapshot.from_dict(value["manifest_snapshot"]),
            _object(value["limits"], "limits"), value["cancellation_path"],
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> "WorkerRequestEnvelope":
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise ValueError("worker request JSON must be an object")
        return cls.from_dict(parsed)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version, "request_id": self.request_id, "job_id": self.job_id,
            "attempt": self.attempt, "worker_kind": self.worker_kind, "operation": self.operation,
            "manifest_snapshot": self.manifest_snapshot.to_dict(), "limits": self.limits,
            "cancellation_path": self.cancellation_path,
        }


@dataclass(frozen=True)
class WorkerEvent:
    version: str
    sequence: int
    timestamp: str
    stage: str
    completed: int | float
    total: int | float
    unit: str
    message_code: str
    metrics: dict[str, Any]

    def __post_init__(self) -> None:
        if self.version != PROTOCOL_VERSION:
            raise ValueError("unsupported worker event version")
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 1:
            raise ValueError("sequence must be an integer >= 1")
        try:
            datetime.fromisoformat(_string(self.timestamp, "timestamp").replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("timestamp must be ISO 8601") from exc
        _string(self.stage, "stage")
        for field, number in (("completed", self.completed), ("total", self.total)):
            if isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(number) or number < 0:
                raise ValueError(f"{field} must be a nonnegative finite number")
        if self.completed > self.total:
            raise ValueError("completed must not exceed total")
        _string(self.unit, "unit")
        _string(self.message_code, "message_code")
        _object(self.metrics, "metrics")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerEvent":
        _keys(value, {"version", "sequence", "timestamp", "stage", "completed", "total", "unit", "message_code", "metrics"}, "worker event")
        return cls(**value)

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.version, "sequence": self.sequence, "timestamp": self.timestamp, "stage": self.stage, "completed": self.completed, "total": self.total, "unit": self.unit, "message_code": self.message_code, "metrics": self.metrics}


@dataclass(frozen=True)
class WorkerResultEnvelope:
    version: str
    status: str
    artifacts: list[dict[str, Any]]
    resource_usage: dict[str, Any]
    error: dict[str, Any] | None

    def __post_init__(self) -> None:
        if self.version != PROTOCOL_VERSION or self.status not in {"success", "failure", "cancelled"}:
            raise ValueError("unsupported worker result version or status")
        if not isinstance(self.artifacts, list) or any(not isinstance(item, dict) for item in self.artifacts):
            raise ValueError("artifacts must be a list of JSON objects")
        canonical_json_bytes(self.artifacts)
        _object(self.resource_usage, "resource_usage")
        if self.error is not None:
            error = _object(self.error, "error")
            _keys(error, {"code", "message", "details"}, "worker error")
            _string(error["code"], "error.code")
            _string(error["message"], "error.message")
            _object(error["details"], "error.details")
        if self.status == "success" and self.error is not None:
            raise ValueError("successful results cannot contain an error")
        if self.status == "failure" and self.error is None:
            raise ValueError("failure results require an error")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerResultEnvelope":
        _keys(value, {"version", "status", "artifacts", "resource_usage", "error"}, "worker result")
        return cls(**value)

    @classmethod
    def from_json(cls, value: str | bytes) -> "WorkerResultEnvelope":
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise ValueError("worker result JSON must be an object")
        return cls.from_dict(parsed)

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.version, "status": self.status, "artifacts": self.artifacts, "resource_usage": self.resource_usage, "error": self.error}


@dataclass(frozen=True)
class ResolvedSnapshotFile:
    name: str
    path: Path
    sha256: str
    size_bytes: int


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


def resolve_manifest_snapshot(snapshot: ManifestSnapshot) -> dict[str, ResolvedSnapshotFile]:
    root = Path(snapshot.workspace_root).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("workspace_root must resolve to a directory")
    output = {}
    for name, entry in snapshot.files.items():
        path = confined_existing_path(root, entry.path)
        if not path.is_file():
            raise ValueError(f"snapshot entry is not a regular file: {entry.path}")
        size = path.stat().st_size
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
        if size != entry.size_bytes or digest.hexdigest() != entry.sha256:
            raise ValueError(f"snapshot metadata mismatch for {entry.path}")
        output[name] = ResolvedSnapshotFile(name, path, entry.sha256, size)
    return output


def read_request(path: Path) -> WorkerRequestEnvelope:
    return WorkerRequestEnvelope.from_json(path.read_bytes())


def read_result(path: Path) -> WorkerResultEnvelope:
    return WorkerResultEnvelope.from_json(path.read_bytes())


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
    part = path.with_name(path.name + ".part")
    fd = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        _write_all(fd, canonical_json_bytes(result.to_dict()))
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        os.replace(part, path)
        fsync_directory(path.parent)
    except BaseException:
        part.unlink(missing_ok=True)
        raise


class WorkerEventWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._last_sequence = 0
        if path.exists():
            for line in path.read_bytes().splitlines():
                if line:
                    event = WorkerEvent.from_dict(json.loads(line))
                    if event.sequence <= self._last_sequence:
                        raise ValueError("existing worker events are not strictly monotonic")
                    self._last_sequence = event.sequence

    def append(self, event: WorkerEvent) -> None:
        if event.sequence != self._last_sequence + 1:
            raise ValueError(f"event sequence must be {self._last_sequence + 1}, got {event.sequence}")
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            _write_all(fd, canonical_json_bytes(event.to_dict()) + b"\n")
            os.fsync(fd)
        finally:
            os.close(fd)
        self._last_sequence = event.sequence
