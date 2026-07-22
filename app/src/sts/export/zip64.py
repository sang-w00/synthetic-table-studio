from __future__ import annotations

import os
import shutil
import stat
import zipfile
from collections.abc import Sequence
from pathlib import Path, PurePosixPath

from sts.storage.atomic import sha256_file

from .atomic import discard_part, publish_completed_part, temporary_output_path
from .models import ExportedFile, ParquetShardManifest

_COPY_BUFFER = 1024 * 1024


def _safe_archive_name(value: str) -> str:
    if "\\" in value or "\x00" in value:
        raise ValueError("ZIP entry names must be normalized relative POSIX paths")
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("ZIP entry names must not be absolute or contain traversal")
    if path.as_posix() != value:
        raise ValueError("ZIP entry names must be normalized relative POSIX paths")
    return value


def _regular_file(path: Path) -> None:
    mode = path.stat(follow_symlinks=False).st_mode
    if not stat.S_ISREG(mode):
        raise ValueError(f"ZIP input must be a regular file: {path}")


def _write_stored_entry(archive: zipfile.ZipFile, source: Path, archive_name: str) -> None:
    _regular_file(source)
    info = zipfile.ZipInfo(_safe_archive_name(archive_name), date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_STORED
    info.create_system = 3
    info.external_attr = 0o600 << 16
    info.file_size = source.stat().st_size
    with source.open("rb") as input_file, archive.open(info, mode="w", force_zip64=True) as output:
        shutil.copyfileobj(input_file, output, length=_COPY_BUFFER)


def create_parquet_zip64_store(
    shard_paths: Sequence[str | Path],
    destination: str | Path,
    *,
    archive_names: Sequence[str] | None = None,
    manifest_path: str | Path | None = None,
    manifest_archive_name: str = "manifest.json",
    manifest: ParquetShardManifest | None = None,
) -> ExportedFile:
    """Create an atomic, deterministic ZIP64-capable archive with no compression."""

    paths = tuple(Path(path) for path in shard_paths)
    if not paths:
        raise ValueError("at least one Parquet shard is required")
    names = (
        tuple(archive_names) if archive_names is not None else tuple(path.name for path in paths)
    )
    if len(names) != len(paths):
        raise ValueError("archive_names must have one entry per Parquet shard")
    entries = list(zip(paths, names, strict=True))
    if manifest_path is not None:
        entries.append((Path(manifest_path), manifest_archive_name))
    safe_names = [_safe_archive_name(name) for _, name in entries]
    if len(safe_names) != len(set(safe_names)):
        raise ValueError("ZIP entry names must be unique")

    target, part = temporary_output_path(destination)
    try:
        with zipfile.ZipFile(
            part,
            mode="x",
            compression=zipfile.ZIP_STORED,
            allowZip64=True,
            strict_timestamps=False,
        ) as archive:
            for source, archive_name in entries:
                _write_stored_entry(archive, source, archive_name)
        descriptor = os.open(part, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        sha256, size_bytes = sha256_file(part)
        publish_completed_part(part, target)
    except Exception:
        discard_part(part)
        raise
    return ExportedFile(
        path=str(target),
        sha256=sha256,
        size_bytes=size_bytes,
        row_count=manifest.row_count if manifest else None,
        canonical_content_sha256=manifest.canonical_content_sha256 if manifest else None,
    )


def create_zip64_store(*args: object, **kwargs: object) -> ExportedFile:
    return create_parquet_zip64_store(*args, **kwargs)
