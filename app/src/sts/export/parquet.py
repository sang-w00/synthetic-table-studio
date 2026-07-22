from __future__ import annotations

import hashlib
from collections.abc import Sequence
from pathlib import Path

from sts.domain import ColumnSchema, DomainError, ErrorCode
from sts.storage.atomic import sha256_file

from .atomic import discard_part, publish_completed_part, temporary_output_path
from .canonical import scan_parquet
from .models import ExportedFile, ParquetShardEntry, ParquetShardManifest


def build_parquet_shard_manifest(
    shard_paths: Sequence[str | Path],
    columns: Sequence[ColumnSchema],
    *,
    relative_paths: Sequence[str] | None = None,
) -> ParquetShardManifest:
    paths = tuple(Path(path) for path in shard_paths)
    names = tuple(path.name for path in paths) if relative_paths is None else tuple(relative_paths)
    if len(paths) != len(names):
        raise ValueError("relative_paths must have one entry per Parquet shard")
    scan = scan_parquet(paths, columns)
    entries: list[ParquetShardEntry] = []
    for ordinal, (path, relative_path, row_count) in enumerate(
        zip(paths, names, scan.source_row_counts, strict=True)
    ):
        sha256, size_bytes = sha256_file(path)
        entries.append(
            ParquetShardEntry(
                ordinal=ordinal,
                relative_path=relative_path,
                sha256=sha256,
                size_bytes=size_bytes,
                row_count=row_count,
            )
        )
    return ParquetShardManifest(
        columns=tuple(columns),
        row_count=scan.row_count,
        canonical_content_sha256=scan.canonical_content_sha256,
        shards=tuple(entries),
    )


def write_parquet_shard_manifest(
    manifest: ParquetShardManifest, destination: str | Path
) -> ExportedFile:
    payload = manifest.canonical_bytes()
    target, part = temporary_output_path(destination)
    try:
        with part.open("xb") as output:
            output.write(payload)
            output.flush()
        publish_completed_part(part, target)
    except Exception:
        discard_part(part)
        raise
    return ExportedFile(
        path=str(target),
        sha256=hashlib.sha256(payload).hexdigest(),
        size_bytes=len(payload),
        row_count=manifest.row_count,
        canonical_content_sha256=manifest.canonical_content_sha256,
    )


def verify_parquet_shard_manifest(manifest: ParquetShardManifest, *, root: str | Path) -> None:
    base = Path(root)
    paths: list[Path] = []
    for shard in manifest.shards:
        path = base / shard.relative_path
        actual_sha256, actual_size = sha256_file(path)
        if actual_sha256 != shard.sha256 or actual_size != shard.size_bytes:
            raise DomainError(
                ErrorCode.CHECKSUM_MISMATCH,
                "Parquet shard checksum or size does not match its manifest",
                context={
                    "relative_path": shard.relative_path,
                    "expected_sha256": shard.sha256,
                    "actual_sha256": actual_sha256,
                    "expected_size": shard.size_bytes,
                    "actual_size": actual_size,
                },
            )
        paths.append(path)
    scan = scan_parquet(paths, manifest.columns)
    if (
        scan.row_count != manifest.row_count
        or scan.canonical_content_sha256 != manifest.canonical_content_sha256
        or scan.source_row_counts != tuple(shard.row_count for shard in manifest.shards)
    ):
        raise DomainError(
            ErrorCode.CHECKSUM_MISMATCH,
            "decoded Parquet content does not match its shard manifest",
            context={
                "expected_rows": manifest.row_count,
                "actual_rows": scan.row_count,
                "expected_content_sha256": manifest.canonical_content_sha256,
                "actual_content_sha256": scan.canonical_content_sha256,
            },
        )
