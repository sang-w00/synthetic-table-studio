from __future__ import annotations

import errno
import os
from pathlib import Path
from uuid import uuid4

from sts.domain import DomainError, ErrorCode


def fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def temporary_output_path(destination: str | Path) -> tuple[Path, Path]:
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        raise DomainError(
            ErrorCode.IMMUTABLE_PATH_EXISTS,
            f"immutable export path already exists: {target}",
        )
    part = target.with_name(f".{target.name}.{uuid4().hex}.part")
    return target, part


def publish_completed_part(part: Path, destination: Path) -> None:
    descriptor = os.open(part, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(destination.parent)
    try:
        # Hard-link publication never overwrites an immutable destination. Both paths are in the
        # same directory, so the completed bytes become visible atomically under the final name.
        os.link(part, destination, follow_symlinks=False)
    except FileExistsError as exc:
        raise DomainError(
            ErrorCode.IMMUTABLE_PATH_EXISTS,
            f"immutable export path already exists: {destination}",
        ) from exc
    except OSError as exc:
        if exc.errno == errno.EXDEV:
            raise RuntimeError(
                "atomic export temp file must be on the destination filesystem"
            ) from exc
        raise
    try:
        fsync_directory(destination.parent)
    finally:
        part.unlink(missing_ok=True)
        fsync_directory(destination.parent)


def discard_part(part: Path) -> None:
    try:
        part.unlink()
    except FileNotFoundError:
        return
    fsync_directory(part.parent)
