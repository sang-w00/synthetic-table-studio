from __future__ import annotations

import contextlib
import fcntl
import hashlib
import os
import stat
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from sts.domain import (
    DomainError,
    ErrorCode,
    canonical_json_bytes,
    require_versioned_json,
)

from .layout import WorkspaceLayout

_CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True, slots=True)
class PublishedFile:
    relative_path: str
    path: Path
    sha256: str
    size_bytes: int


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(directory, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _open_regular_readonly(path: Path) -> int:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("artifact source must be a regular file")
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


def sha256_file(path: str | os.PathLike[str]) -> tuple[str, int]:
    source = Path(path)
    descriptor = _open_regular_readonly(source)
    digest = hashlib.sha256()
    size = 0
    try:
        while chunk := os.read(descriptor, _CHUNK_SIZE):
            digest.update(chunk)
            size += len(chunk)
    finally:
        os.close(descriptor)
    return digest.hexdigest(), size


def verify_regular_file(
    path: str | os.PathLike[str], expected_sha256: str, expected_size: int | None = None
) -> tuple[str, int]:
    actual_sha256, actual_size = sha256_file(path)
    if actual_sha256 != expected_sha256.lower() or (
        expected_size is not None and actual_size != expected_size
    ):
        raise DomainError(
            ErrorCode.CHECKSUM_MISMATCH,
            "published artifact checksum or size does not match its manifest",
            context={
                "expected_sha256": expected_sha256.lower(),
                "actual_sha256": actual_sha256,
                "expected_size": expected_size,
                "actual_size": actual_size,
            },
        )
    return actual_sha256, actual_size


class AtomicPublisher:
    def __init__(self, layout: WorkspaceLayout) -> None:
        self.layout = layout
        self.layout.initialize()

    def publish_bytes(
        self,
        relative_path: str,
        content: bytes | bytearray | memoryview,
        *,
        expected_sha256: str | None = None,
        expected_size: int | None = None,
    ) -> PublishedFile:
        view = memoryview(content)
        return self._publish_chunks(
            relative_path,
            (view[offset : offset + _CHUNK_SIZE] for offset in range(0, len(view), _CHUNK_SIZE)),
            expected_sha256=expected_sha256,
            expected_size=expected_size,
        )

    def publish_json(
        self,
        relative_path: str,
        value: object,
        *,
        expected_sha256: str | None = None,
    ) -> PublishedFile:
        require_versioned_json(value)
        payload = canonical_json_bytes(value)
        return self.publish_bytes(
            relative_path,
            payload,
            expected_sha256=expected_sha256,
            expected_size=len(payload),
        )

    def publish_file(
        self,
        source_path: str | os.PathLike[str],
        relative_path: str,
        *,
        expected_sha256: str | None = None,
        expected_size: int | None = None,
    ) -> PublishedFile:
        descriptor = _open_regular_readonly(Path(source_path))

        def chunks() -> Iterable[bytes]:
            while chunk := os.read(descriptor, _CHUNK_SIZE):
                yield chunk

        try:
            return self._publish_chunks(
                relative_path,
                chunks(),
                expected_sha256=expected_sha256,
                expected_size=expected_size,
            )
        finally:
            os.close(descriptor)

    def verify(
        self, relative_path: str, expected_sha256: str, expected_size: int | None = None
    ) -> PublishedFile:
        destination = self.layout.resolve_relative(relative_path, require_exists=True)
        digest, size = verify_regular_file(destination, expected_sha256, expected_size)
        return PublishedFile(relative_path, destination, digest, size)

    def _publish_chunks(
        self,
        relative_path: str,
        chunks: Iterable[bytes | bytearray | memoryview],
        *,
        expected_sha256: str | None,
        expected_size: int | None,
    ) -> PublishedFile:
        destination = self.layout.resolve_relative(relative_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Resolve again after parent creation so any symlink is checked against the workspace root.
        destination = self.layout.resolve_relative(relative_path)
        part = destination.with_name(f".{destination.name}.{uuid4().hex}.part")
        lock_path = destination.parent / ".publication.lock"
        descriptor = -1
        digest = hashlib.sha256()
        size = 0
        try:
            descriptor = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            for chunk in chunks:
                view = memoryview(chunk)
                while view:
                    written = os.write(descriptor, view)
                    view = view[written:]
                digest.update(chunk)
                size += len(chunk)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            actual_sha256 = digest.hexdigest()
            if expected_sha256 is not None and actual_sha256 != expected_sha256.lower():
                raise DomainError(
                    ErrorCode.CHECKSUM_MISMATCH,
                    "temporary artifact checksum does not match the expected checksum",
                    context={
                        "expected_sha256": expected_sha256.lower(),
                        "actual_sha256": actual_sha256,
                    },
                )
            if expected_size is not None and size != expected_size:
                raise DomainError(
                    ErrorCode.CHECKSUM_MISMATCH,
                    "temporary artifact size does not match the expected size",
                    context={"expected_size": expected_size, "actual_size": size},
                )
            _fsync_directory(destination.parent)
            lock_descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
                if destination.exists() or destination.is_symlink():
                    raise DomainError(
                        ErrorCode.IMMUTABLE_PATH_EXISTS,
                        f"immutable artifact path already exists: {relative_path}",
                    )
                os.rename(part, destination)
                _fsync_directory(destination.parent)
            finally:
                fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                os.close(lock_descriptor)
            return PublishedFile(relative_path, destination, actual_sha256, size)
        except Exception:
            if descriptor >= 0:
                os.close(descriptor)
            with contextlib.suppress(FileNotFoundError):
                part.unlink()
            _fsync_directory(destination.parent)
            raise
