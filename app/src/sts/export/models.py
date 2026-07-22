from __future__ import annotations

import re
from pathlib import Path, PurePosixPath
from typing import Literal

from pydantic import Field, field_validator, model_validator

from sts.domain import CanonicalModel, ColumnSchema

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _sha256(value: str) -> str:
    lowered = value.lower()
    if not _SHA256_RE.fullmatch(lowered):
        raise ValueError("must be a 64-character lowercase hexadecimal SHA-256")
    return lowered


def _relative_path(value: str) -> str:
    if "\\" in value or "\x00" in value:
        raise ValueError("must be a workspace-relative POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("must be a normalized workspace-relative path")
    if path.as_posix() != value:
        raise ValueError("must be a normalized workspace-relative path")
    return value


class ParquetShardEntry(CanonicalModel):
    ordinal: int = Field(ge=0)
    relative_path: str
    sha256: str
    size_bytes: int = Field(ge=0)
    row_count: int = Field(ge=0)

    _path = field_validator("relative_path")(_relative_path)
    _sha = field_validator("sha256")(_sha256)


class ParquetShardManifest(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    kind: Literal["synthetic_parquet_manifest"] = "synthetic_parquet_manifest"
    columns: tuple[ColumnSchema, ...]
    row_count: int = Field(ge=0)
    canonical_content_sha256: str
    shards: tuple[ParquetShardEntry, ...]

    _content_sha = field_validator("canonical_content_sha256")(_sha256)

    @model_validator(mode="after")
    def validate_shards(self) -> ParquetShardManifest:
        if tuple(item.ordinal for item in self.shards) != tuple(range(len(self.shards))):
            raise ValueError("Parquet shard ordinals must be contiguous and ordered from zero")
        paths = [item.relative_path for item in self.shards]
        if len(paths) != len(set(paths)):
            raise ValueError("Parquet shard paths must be unique")
        if sum(item.row_count for item in self.shards) != self.row_count:
            raise ValueError("Parquet shard row counts must equal manifest row_count")
        names = [column.name for column in self.columns]
        if len(names) != len(set(names)):
            raise ValueError("manifest columns must have unique names")
        return self


class ScanResult(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    row_count: int = Field(ge=0)
    canonical_content_sha256: str
    schema_signature: tuple[str, ...]
    source_row_counts: tuple[int, ...] = ()

    _content_sha = field_validator("canonical_content_sha256")(_sha256)


class ExportedFile(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    path: str
    sha256: str
    size_bytes: int = Field(ge=0)
    row_count: int | None = Field(default=None, ge=0)
    canonical_content_sha256: str | None = None
    null_marker: str | None = None

    _sha = field_validator("sha256")(_sha256)

    @field_validator("canonical_content_sha256")
    @classmethod
    def validate_optional_sha(cls, value: str | None) -> str | None:
        return None if value is None else _sha256(value)

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        if not value or "\x00" in value:
            raise ValueError("path must not be empty or contain NUL")
        return str(Path(value))
