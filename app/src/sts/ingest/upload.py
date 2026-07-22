from __future__ import annotations

import fcntl
import json
import os
import re
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO
from uuid import UUID, uuid4

from sts.domain import DomainError, ErrorCode
from sts.storage import AtomicPublisher, PublishedFile, WorkspaceLayout, verify_regular_file

MAX_UPLOAD_BYTES = 8 * 1024**3
MAX_UPLOAD_CHUNK_BYTES = 64 * 1024**2
_METADATA_NAME = ".upload.json"
_PART_NAME = ".upload.part"
_LOCK_NAME = ".upload.lock"
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class UploadSourceFormat(StrEnum):
    CSV = "csv"
    XLSX = "xlsx"


class UploadState(StrEnum):
    UPLOADING = "uploading"
    STAGED = "staged"


@dataclass(frozen=True, slots=True)
class UploadSession:
    dataset_id: UUID
    filename: str
    size_bytes: int
    source_format: UploadSourceFormat
    state: UploadState
    offset: int
    staged_file: PublishedFile | None = None


class UploadManager:
    """Disk-backed resumable uploads scoped to immutable dataset directories.

    The current offset is always derived from the regular staging file while an
    upload is in progress. Metadata never acts as an offset counter, so a process
    crash cannot make HEAD advertise bytes that were not fsynced to disk.
    """

    def __init__(
        self,
        workspace: WorkspaceLayout | str | os.PathLike[str],
        *,
        max_upload_bytes: int = MAX_UPLOAD_BYTES,
        max_chunk_bytes: int = MAX_UPLOAD_CHUNK_BYTES,
    ) -> None:
        self.layout = (
            workspace if isinstance(workspace, WorkspaceLayout) else WorkspaceLayout(workspace)
        )
        self.layout.initialize()
        if max_upload_bytes < 0:
            raise ValueError("max_upload_bytes must be non-negative")
        if max_chunk_bytes <= 0 or max_chunk_bytes > MAX_UPLOAD_CHUNK_BYTES:
            raise ValueError(f"max_chunk_bytes must be in 1..{MAX_UPLOAD_CHUNK_BYTES}")
        self.max_upload_bytes = max_upload_bytes
        self.max_chunk_bytes = max_chunk_bytes
        self.publisher = AtomicPublisher(self.layout)

    def create(
        self,
        dataset_id: UUID | str,
        filename: str,
        size_bytes: int,
        source_format: UploadSourceFormat | str | None = None,
    ) -> UploadSession:
        identifier = _as_uuid(dataset_id)
        clean_filename = _validate_filename(filename)
        resolved_format = _resolve_source_format(clean_filename, source_format)
        if size_bytes < 0:
            raise DomainError(ErrorCode.SCHEMA_INVALID, "upload size must be non-negative")
        if size_bytes > self.max_upload_bytes:
            raise DomainError(
                ErrorCode.UPLOAD_TOO_LARGE,
                f"upload exceeds the {self.max_upload_bytes}-byte limit",
                context={"size_bytes": size_bytes, "limit_bytes": self.max_upload_bytes},
            )

        directory = self.layout.dataset_dir(identifier, create=True)
        with _exclusive_lock(directory / _LOCK_NAME):
            existing = self._load_metadata(directory, required=False)
            if existing is not None:
                if (
                    existing["filename"] == clean_filename
                    and existing["size_bytes"] == size_bytes
                    and existing["source_format"] == resolved_format.value
                ):
                    return self._session_from_metadata(identifier, directory, existing)
                raise DomainError(
                    ErrorCode.INVALID_STATE,
                    "an upload session already exists for this dataset",
                    context={"dataset_id": str(identifier)},
                )

            destination = directory / f"source.{resolved_format.value}"
            if destination.exists() or destination.is_symlink():
                raise DomainError(
                    ErrorCode.INVALID_STATE,
                    "the dataset source has already been staged",
                    context={"dataset_id": str(identifier)},
                )

            part = directory / _PART_NAME
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(part, flags, 0o600)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            _fsync_directory(directory)
            metadata = {
                "version": "1.0",
                "dataset_id": str(identifier),
                "filename": clean_filename,
                "size_bytes": size_bytes,
                "source_format": resolved_format.value,
                "state": UploadState.UPLOADING.value,
            }
            try:
                _write_metadata(directory / _METADATA_NAME, metadata)
            except Exception:
                part.unlink(missing_ok=True)
                _fsync_directory(directory)
                raise
            return UploadSession(
                dataset_id=identifier,
                filename=clean_filename,
                size_bytes=size_bytes,
                source_format=resolved_format,
                state=UploadState.UPLOADING,
                offset=0,
            )

    def get(self, dataset_id: UUID | str) -> UploadSession:
        identifier = _as_uuid(dataset_id)
        directory = self.layout.dataset_dir(identifier)
        with _exclusive_lock(directory / _LOCK_NAME):
            metadata = self._load_metadata(directory)
            return self._session_from_metadata(identifier, directory, metadata)

    def head_offset(self, dataset_id: UUID | str) -> int:
        """Return the durable current offset without mutating session state."""
        return self.get(dataset_id).offset

    def append(
        self,
        dataset_id: UUID | str,
        offset: int,
        body: bytes | bytearray | memoryview,
    ) -> int:
        identifier = _as_uuid(dataset_id)
        if offset < 0:
            raise DomainError(ErrorCode.INVALID_STATE, "Upload-Offset must be non-negative")
        chunk = memoryview(body)
        if chunk.nbytes > self.max_chunk_bytes:
            raise DomainError(
                ErrorCode.UPLOAD_TOO_LARGE,
                f"upload chunks may not exceed {self.max_chunk_bytes} bytes",
                context={"chunk_bytes": chunk.nbytes, "limit_bytes": self.max_chunk_bytes},
            )

        directory = self.layout.dataset_dir(identifier)
        with _exclusive_lock(directory / _LOCK_NAME):
            metadata = self._load_metadata(directory)
            if metadata["state"] != UploadState.UPLOADING.value:
                raise DomainError(ErrorCode.INVALID_STATE, "the upload is already complete")
            part = directory / _PART_NAME
            descriptor = _open_regular(part, os.O_WRONLY | os.O_APPEND)
            try:
                current = os.fstat(descriptor).st_size
                if current != offset:
                    raise DomainError(
                        ErrorCode.INVALID_STATE,
                        "Upload-Offset does not match the durable server offset",
                        context={"provided_offset": offset, "upload_offset": current},
                    )
                declared_size = int(metadata["size_bytes"])
                if current + chunk.nbytes > declared_size:
                    raise DomainError(
                        ErrorCode.UPLOAD_TOO_LARGE,
                        "upload chunk exceeds the declared upload size",
                        context={
                            "upload_offset": current,
                            "chunk_bytes": chunk.nbytes,
                            "declared_size": declared_size,
                        },
                    )
                remaining = chunk
                while remaining:
                    written = os.write(descriptor, remaining)
                    remaining = remaining[written:]
                os.fsync(descriptor)
                return current + chunk.nbytes
            finally:
                os.close(descriptor)

    def complete(self, dataset_id: UUID | str, sha256: str) -> PublishedFile:
        identifier = _as_uuid(dataset_id)
        if not _SHA256_RE.fullmatch(sha256):
            raise DomainError(
                ErrorCode.CHECKSUM_MISMATCH, "sha256 must contain 64 hexadecimal digits"
            )
        expected_sha256 = sha256.lower()
        directory = self.layout.dataset_dir(identifier)

        with _exclusive_lock(directory / _LOCK_NAME):
            metadata = self._load_metadata(directory)
            expected_size = int(metadata["size_bytes"])
            source_format = UploadSourceFormat(metadata["source_format"])
            relative_path = f"datasets/{identifier}/source.{source_format.value}"
            destination = self.layout.resolve_relative(relative_path)

            if metadata["state"] == UploadState.STAGED.value:
                published = self.publisher.verify(relative_path, expected_sha256, expected_size)
                (directory / _PART_NAME).unlink(missing_ok=True)
                _fsync_directory(directory)
                return published

            part = directory / _PART_NAME
            descriptor = _open_regular(part, os.O_RDONLY)
            try:
                actual_size = os.fstat(descriptor).st_size
            finally:
                os.close(descriptor)
            if actual_size != expected_size:
                raise DomainError(
                    ErrorCode.CHECKSUM_MISMATCH,
                    "upload is incomplete",
                    context={"expected_size": expected_size, "actual_size": actual_size},
                )

            # Recover a crash after immutable publication but before the mutable
            # session receipt was updated. An unexpected existing file still fails.
            if destination.exists() or destination.is_symlink():
                actual_sha256, verified_size = verify_regular_file(
                    destination, expected_sha256, expected_size
                )
                published = PublishedFile(
                    relative_path=relative_path,
                    path=destination,
                    sha256=actual_sha256,
                    size_bytes=verified_size,
                )
            else:
                published = self.publisher.publish_file(
                    part,
                    relative_path,
                    expected_sha256=expected_sha256,
                    expected_size=expected_size,
                )

            staged_metadata = dict(metadata)
            staged_metadata.update(
                {
                    "state": UploadState.STAGED.value,
                    "relative_path": published.relative_path,
                    "sha256": published.sha256,
                }
            )
            _write_metadata(directory / _METADATA_NAME, staged_metadata)
            part.unlink(missing_ok=True)
            _fsync_directory(directory)
            return published

    def _load_metadata(self, directory: Path, *, required: bool = True) -> dict[str, object] | None:
        path = directory / _METADATA_NAME
        try:
            descriptor = _open_regular(path, os.O_RDONLY)
        except FileNotFoundError:
            if required:
                raise DomainError(
                    ErrorCode.INVALID_STATE,
                    "upload session does not exist",
                    context={"dataset_id": directory.name},
                ) from None
            return None
        try:
            with os.fdopen(descriptor, "rb", closefd=True) as stream:
                payload = stream.read(64 * 1024 + 1)
        except Exception:
            raise
        if len(payload) > 64 * 1024:
            raise DomainError(ErrorCode.INVALID_STATE, "upload metadata is invalid")
        try:
            value = json.loads(payload)
            if not isinstance(value, dict):
                raise TypeError
            if value.get("version") != "1.0" or value.get("dataset_id") != directory.name:
                raise ValueError
            filename = _validate_filename(value["filename"])
            size_bytes = value["size_bytes"]
            if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes < 0:
                raise ValueError
            source_format = UploadSourceFormat(value["source_format"])
            state = UploadState(value["state"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DomainError(ErrorCode.INVALID_STATE, "upload metadata is invalid") from exc
        normalized = dict(value)
        normalized.update(
            {
                "filename": filename,
                "size_bytes": size_bytes,
                "source_format": source_format.value,
                "state": state.value,
            }
        )
        return normalized

    def _session_from_metadata(
        self, identifier: UUID, directory: Path, metadata: dict[str, object]
    ) -> UploadSession:
        state = UploadState(metadata["state"])
        staged_file: PublishedFile | None = None
        if state is UploadState.UPLOADING:
            descriptor = _open_regular(directory / _PART_NAME, os.O_RDONLY)
            try:
                offset = os.fstat(descriptor).st_size
            finally:
                os.close(descriptor)
        else:
            relative_path = str(metadata["relative_path"])
            destination = self.layout.resolve_relative(relative_path, require_exists=True)
            digest, offset = verify_regular_file(
                destination, str(metadata["sha256"]), int(metadata["size_bytes"])
            )
            staged_file = PublishedFile(relative_path, destination, digest, offset)
        return UploadSession(
            dataset_id=identifier,
            filename=str(metadata["filename"]),
            size_bytes=int(metadata["size_bytes"]),
            source_format=UploadSourceFormat(metadata["source_format"]),
            state=state,
            offset=offset,
            staged_file=staged_file,
        )


def _as_uuid(value: UUID | str) -> UUID:
    try:
        return value if isinstance(value, UUID) else UUID(str(value))
    except ValueError as exc:
        raise ValueError("dataset_id must be a UUID") from exc


def _validate_filename(filename: object) -> str:
    if not isinstance(filename, str) or not filename or "\x00" in filename:
        raise DomainError(ErrorCode.SCHEMA_INVALID, "filename must be non-empty")
    if (
        filename in {".", ".."}
        or Path(filename).name != filename
        or "/" in filename
        or "\\" in filename
    ):
        raise DomainError(ErrorCode.SCHEMA_INVALID, "filename must not contain a path")
    if len(filename.encode("utf-8")) > 255:
        raise DomainError(ErrorCode.SCHEMA_INVALID, "filename is too long")
    return filename


def _resolve_source_format(
    filename: str, source_format: UploadSourceFormat | str | None
) -> UploadSourceFormat:
    if source_format is not None:
        try:
            return UploadSourceFormat(source_format)
        except ValueError as exc:
            raise DomainError(
                ErrorCode.INPUT_FORMAT_UNSUPPORTED,
                "source_format must be csv or xlsx",
            ) from exc
    suffix = Path(filename).suffix.lower().removeprefix(".")
    try:
        return UploadSourceFormat(suffix)
    except ValueError as exc:
        raise DomainError(
            ErrorCode.INPUT_FORMAT_UNSUPPORTED,
            "the filename extension must be .csv or .xlsx",
        ) from exc


def _open_regular(path: Path, flags: int) -> int:
    descriptor = os.open(path, flags | getattr(os, "O_NOFOLLOW", 0))
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError(f"not a regular file: {path}")
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[BinaryIO]:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    stream = os.fdopen(descriptor, "a+b", closefd=True)
    try:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        yield stream
    finally:
        fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        stream.close()


def _write_metadata(path: Path, value: dict[str, object]) -> None:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.part")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    except Exception:
        os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise
    else:
        os.close(descriptor)
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
