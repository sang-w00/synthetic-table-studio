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
_WORKER_KINDS = frozenset(("argn", "dpmm", "eval"))
_RESULT_STATUSES = frozenset(("success", "failure", "cancelled"))


def _exact_keys(value: Mapping[str, Any], keys: set[str], kind: str) -> None:
    actual = set(value)
    if actual != keys:
        missing = sorted(keys - actual)
        extra = sorted(actual - keys)
        raise ValueError(f"invalid {kind} fields; missing={missing}, extra={extra}")


def _nonempty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a nonempty string")
    return value


def _json_object(value: Any, field: str) -> dict[str, Any]:
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
        _exact_keys(value, {"path", "sha256", "size_bytes"}, "snapshot file")
        return cls(path=value["path"], sha256=value["sha256"], size_bytes=value["size_bytes"])

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "sha256": self.sha256, "size_bytes": self.size_bytes}


@dataclass(frozen=True)
class ManifestSnapshot:
    version: str
    workspace_root: str
    files: dict[str, SnapshotFile]

    def __post_init__(self) -> None:
        if self.version != PROTOCOL_VERSION:
            raise ValueError(f"unsupported manifest snapshot version: {self.version!r}")
        if not isinstance(self.workspace_root, str) or not Path(self.workspace_root).is_absolute():
            raise ValueError("workspace_root must be an absolute path")
        if not isinstance(self.files, dict) or any(not key for key in self.files):
            raise ValueError("files must be an object with nonempty keys")
        paths = [entry.path for entry in self.files.values()]
        if len(paths) != len(set(paths)):
            raise ValueError("manifest snapshot paths must be unique")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ManifestSnapshot":
        _exact_keys(value, {"version", "workspace_root", "files"}, "manifest snapshot")
        raw_files = value["files"]
        if not isinstance(raw_files, dict):
            raise ValueError("manifest snapshot files must be an object")
        files = {
            _nonempty_string(name, "manifest snapshot key"): SnapshotFile.from_dict(entry)
            for name, entry in raw_files.items()
        }
        return cls(version=value["version"], workspace_root=value["workspace_root"], files=files)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "workspace_root": self.workspace_root,
            "files": {name: entry.to_dict() for name, entry in self.files.items()},
        }


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
            raise ValueError(f"unsupported worker request version: {self.version!r}")
        _nonempty_string(self.request_id, "request_id")
        _nonempty_string(self.job_id, "job_id")
        if isinstance(self.attempt, bool) or not isinstance(self.attempt, int) or self.attempt < 1:
            raise ValueError("attempt must be an integer >= 1")
        if self.worker_kind not in _WORKER_KINDS:
            raise ValueError(f"unsupported worker_kind: {self.worker_kind!r}")
        _nonempty_string(self.operation, "operation")
        _json_object(self.limits, "limits")
        validate_workspace_relative_path(self.cancellation_path)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerRequestEnvelope":
        _exact_keys(
            value,
            {
                "version",
                "request_id",
                "job_id",
                "attempt",
                "worker_kind",
                "operation",
                "manifest_snapshot",
                "limits",
                "cancellation_path",
            },
            "worker request",
        )
        return cls(
            version=value["version"],
            request_id=value["request_id"],
            job_id=value["job_id"],
            attempt=value["attempt"],
            worker_kind=value["worker_kind"],
            operation=value["operation"],
            manifest_snapshot=ManifestSnapshot.from_dict(value["manifest_snapshot"]),
            limits=_json_object(value["limits"], "limits"),
            cancellation_path=value["cancellation_path"],
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> "WorkerRequestEnvelope":
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise ValueError("worker request JSON must be an object")
        return cls.from_dict(parsed)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "request_id": self.request_id,
            "job_id": self.job_id,
            "attempt": self.attempt,
            "worker_kind": self.worker_kind,
            "operation": self.operation,
            "manifest_snapshot": self.manifest_snapshot.to_dict(),
            "limits": self.limits,
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
            raise ValueError(f"unsupported worker event version: {self.version!r}")
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 1:
            raise ValueError("sequence must be an integer >= 1")
        timestamp = _nonempty_string(self.timestamp, "timestamp")
        try:
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("timestamp must be ISO 8601") from exc
        _nonempty_string(self.stage, "stage")
        for field, number in (("completed", self.completed), ("total", self.total)):
            if isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(number) or number < 0:
                raise ValueError(f"{field} must be a nonnegative finite number")
        if self.completed > self.total:
            raise ValueError("completed must not exceed total")
        _nonempty_string(self.unit, "unit")
        _nonempty_string(self.message_code, "message_code")
        _json_object(self.metrics, "metrics")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerEvent":
        _exact_keys(
            value,
            {"version", "sequence", "timestamp", "stage", "completed", "total", "unit", "message_code", "metrics"},
            "worker event",
        )
        return cls(**value)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "stage": self.stage,
            "completed": self.completed,
            "total": self.total,
            "unit": self.unit,
            "message_code": self.message_code,
            "metrics": self.metrics,
        }


@dataclass(frozen=True)
class WorkerResultEnvelope:
    version: str
    status: str
    artifacts: list[dict[str, Any]]
    resource_usage: dict[str, Any]
    error: dict[str, Any] | None

    def __post_init__(self) -> None:
        if self.version != PROTOCOL_VERSION:
            raise ValueError(f"unsupported worker result version: {self.version!r}")
        if self.status not in _RESULT_STATUSES:
            raise ValueError(f"unsupported result status: {self.status!r}")
        if not isinstance(self.artifacts, list) or any(not isinstance(item, dict) for item in self.artifacts):
            raise ValueError("artifacts must be a list of JSON objects")
        canonical_json_bytes(self.artifacts)
        _json_object(self.resource_usage, "resource_usage")
        if self.error is not None:
            error = _json_object(self.error, "error")
            _exact_keys(error, {"code", "message", "details"}, "worker error")
            _nonempty_string(error["code"], "error.code")
            _nonempty_string(error["message"], "error.message")
            _json_object(error["details"], "error.details")
        if self.status == "success" and self.error is not None:
            raise ValueError("successful results cannot contain an error")
        if self.status == "failure" and self.error is None:
            raise ValueError("failure results require an error")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkerResultEnvelope":
        _exact_keys(value, {"version", "status", "artifacts", "resource_usage", "error"}, "worker result")
        return cls(**value)

    @classmethod
    def from_json(cls, value: str | bytes) -> "WorkerResultEnvelope":
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise ValueError("worker result JSON must be an object")
        return cls.from_dict(parsed)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "status": self.status,
            "artifacts": self.artifacts,
            "resource_usage": self.resource_usage,
            "error": self.error,
        }


@dataclass(frozen=True)
class ResolvedSnapshotFile:
    name: str
    path: Path
    sha256: str
    size_bytes: int


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


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
    resolved: dict[str, ResolvedSnapshotFile] = {}
    for name, entry in snapshot.files.items():
        path = confined_existing_path(root, entry.path)
        if not path.is_file():
            raise ValueError(f"snapshot entry is not a regular file: {entry.path}")
        size = path.stat().st_size
        if size != entry.size_bytes:
            raise ValueError(f"snapshot size mismatch for {entry.path}: expected {entry.size_bytes}, got {size}")
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
        actual_sha256 = digest.hexdigest()
        if actual_sha256 != entry.sha256:
            raise ValueError(f"snapshot SHA-256 mismatch for {entry.path}")
        resolved[name] = ResolvedSnapshotFile(name, path, actual_sha256, size)
    return resolved


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
    part = path.with_name(f"{path.name}.part")
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
                    parsed = json.loads(line)
                    event = WorkerEvent.from_dict(parsed)
                    if event.sequence <= last:
                        raise ValueError("existing worker events are not strictly monotonic")
                    last = event.sequence
        return last

    def append(self, event: WorkerEvent) -> None:
        if event.sequence != self._last_sequence + 1:
            raise ValueError(f"event sequence must be {self._last_sequence + 1}, got {event.sequence}")
        data = canonical_json_bytes(event.to_dict()) + b"\n"
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            _write_all(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        self._last_sequence = event.sequence
