from __future__ import annotations

import hashlib
import math
import re
import struct
import unicodedata
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import duckdb
import pyarrow as pa
from pydantic import Field

from sts.domain.canonical import CanonicalModel, canonical_json_bytes
from sts.domain.errors import DomainError, ErrorCode
from sts.domain.models import ColumnKind, ColumnSchema

if TYPE_CHECKING:
    from sts.rules.compiler import CompiledRules
    from sts.rules.models import StructuralCodecs


class ArtifactDigest(CanonicalModel):
    path: str
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class CategoryCount(CanonicalModel):
    value: Any
    count: int = Field(gt=0)


class ExactColumnCheck(CanonicalModel):
    name: str
    kind: ColumnKind
    arrow_dtype: str
    null_count: int = Field(ge=0)
    category_counts: tuple[CategoryCount, ...] = ()
    unique_non_null_count: int | None = Field(default=None, ge=0)
    duplicate_non_null_count: int | None = Field(default=None, ge=0)
    format_violations: int | None = Field(default=None, ge=0)


class ExactScanResult(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    requested_rows: int = Field(ge=0)
    actual_rows: int = Field(ge=0)
    column_order: tuple[str, ...]
    columns: tuple[ExactColumnCheck, ...]
    rule_violations: Mapping[str, int]
    hard_rule_violations: int = Field(ge=0)
    artifacts: tuple[ArtifactDigest, ...]
    canonical_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


ExactSource = str | Path | Sequence[str | Path] | pa.Table


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _paths(source: ExactSource) -> tuple[Path, ...]:
    if isinstance(source, (str, Path)):
        return (Path(source),)
    if isinstance(source, pa.Table):
        return ()
    return tuple(Path(path) for path in source)


def _artifact_digest(path: Path) -> ArtifactDigest:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return ArtifactDigest(path=str(path), size_bytes=size, sha256=digest.hexdigest())


def _register_source(connection: duckdb.DuckDBPyConnection, source: ExactSource) -> None:
    if isinstance(source, pa.Table):
        connection.from_arrow(source).create_view("_sts_exact_source", replace=True)
        return
    paths = _paths(source)
    if not paths:
        raise ValueError("an exact scan requires at least one source path")
    suffixes = {path.suffix.lower() for path in paths}
    values = [str(path) for path in paths]
    argument: str | list[str] = values[0] if len(values) == 1 else values
    if suffixes <= {".parquet", ".pq"}:
        relation = connection.read_parquet(argument, union_by_name=False)
    elif len(paths) == 1 and suffixes == {".csv"}:
        relation = connection.read_csv(values[0], header=True, auto_detect=True)
    else:
        raise ValueError("exact scan sources must be all Parquet shards or one CSV file")
    relation.create_view("_sts_exact_source", replace=True)


def _logical_dtype_matches(kind: ColumnKind, dtype: pa.DataType) -> bool:
    if kind is ColumnKind.INTEGER:
        return pa.types.is_integer(dtype)
    if kind is ColumnKind.FIXED_DECIMAL:
        return pa.types.is_decimal(dtype)
    if kind is ColumnKind.FLOAT:
        return pa.types.is_floating(dtype)
    if kind is ColumnKind.CATEGORICAL:
        return (
            pa.types.is_string(dtype)
            or pa.types.is_large_string(dtype)
            or pa.types.is_dictionary(dtype)
        )
    if kind is ColumnKind.BOOLEAN:
        return pa.types.is_boolean(dtype)
    if kind is ColumnKind.DATE:
        return pa.types.is_date(dtype)
    if kind is ColumnKind.DATETIME:
        return pa.types.is_timestamp(dtype)
    if kind in {ColumnKind.TEXT, ColumnKind.IDENTIFIER}:
        return pa.types.is_string(dtype) or pa.types.is_large_string(dtype)
    return True


def _canonical_integer(value: object) -> bytes:
    if isinstance(value, bool):
        raise ValueError("boolean is not a canonical integer")
    return str(int(value)).encode("ascii")


def _canonical_fixed_decimal(value: object, decimal_places: int) -> bytes:
    try:
        decimal = value if isinstance(value, Decimal) else Decimal(str(value))
        scaled = decimal * (Decimal(10) ** decimal_places)
        integral = scaled.to_integral_exact()
    except (InvalidOperation, TypeError, ValueError) as error:
        message = "fixed decimal is not exactly representable at its declared scale"
        raise ValueError(message) from error
    if scaled != integral:
        raise ValueError("fixed decimal is not exactly representable at its declared scale")
    return str(int(integral)).encode("ascii")


def _datetime_micros(value: datetime) -> int:
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=UTC)
    utc = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = utc - epoch
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _encode_nonnull(value: object, column: ColumnSchema) -> bytes:
    kind = column.kind
    if kind is ColumnKind.INTEGER:
        return _canonical_integer(value)
    if kind is ColumnKind.FIXED_DECIMAL:
        return _canonical_fixed_decimal(value, column.decimal_places or 0)
    if kind is ColumnKind.FLOAT:
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("canonical content does not support non-finite floats")
        return struct.pack("<d", number)
    if kind is ColumnKind.DATE:
        if not isinstance(value, date) or isinstance(value, datetime):
            raise ValueError("date column contains a non-date value")
        return struct.pack("<q", (value - date(1970, 1, 1)).days)
    if kind is ColumnKind.DATETIME:
        if not isinstance(value, datetime):
            raise ValueError("datetime column contains a non-datetime value")
        return struct.pack("<q", _datetime_micros(value))
    if kind is ColumnKind.BOOLEAN:
        if not isinstance(value, bool):
            raise ValueError("boolean column contains a non-boolean value")
        return b"\x01" if value else b"\x00"
    if isinstance(value, bool):
        return b"b1" if value else b"b0"
    if isinstance(value, int):
        return b"i" + _canonical_integer(value)
    text = unicodedata.normalize("NFC", str(value))
    return b"s" + text.encode("utf-8")


def _update_content_hash(
    digest: Any,
    batch: pa.RecordBatch,
    columns: Sequence[ColumnSchema],
) -> None:
    values = [batch.column(index).to_pylist() for index in range(batch.num_columns)]
    for row_index in range(batch.num_rows):
        for column_index, column in enumerate(columns):
            value = values[column_index][row_index]
            payload = b"\x00" if value is None else b"\x01" + _encode_nonnull(value, column)
            digest.update(struct.pack("<Q", len(payload)))
            digest.update(payload)


def canonical_content_sha256(
    source: ExactSource,
    columns: Sequence[ColumnSchema],
) -> tuple[int, str]:
    """Hash decoded rows using the canonical version 1.0 cell encoding."""

    connection = duckdb.connect()
    try:
        _register_source(connection, source)
        reader = connection.execute("SELECT * FROM _sts_exact_source").to_arrow_reader(
            batch_size=65_536
        )
        expected_names = [column.name for column in columns]
        if reader.schema.names != expected_names:
            raise DomainError(
                ErrorCode.OUTPUT_INVALID,
                "synthetic output column order does not match its schema",
                context={"expected": expected_names, "actual": reader.schema.names},
            )
        digest = hashlib.sha256()
        rows = 0
        for batch in reader:
            _update_content_hash(digest, batch, columns)
            rows += batch.num_rows
        return rows, digest.hexdigest()
    finally:
        connection.close()


def exact_full_scan(
    source: ExactSource,
    *,
    expected_columns: Sequence[ColumnSchema],
    requested_rows: int,
    expected_arrow_dtypes: Mapping[str, str] | None = None,
    expected_artifact_sha256: str | Mapping[str, str] | None = None,
    expected_content_sha256: str | None = None,
    compiled_rules: CompiledRules | None = None,
    codecs: StructuralCodecs | None = None,
    expected_hard_rule_violations: int = 0,
) -> ExactScanResult:
    """Perform exact, fail-closed validation over every decoded output row."""

    if requested_rows < 0:
        raise ValueError("requested_rows must be non-negative")
    columns = tuple(expected_columns)
    names = [column.name for column in columns]
    if len(names) != len(set(names)):
        raise ValueError("expected columns must have unique names")
    artifacts = tuple(_artifact_digest(path) for path in _paths(source))
    if expected_artifact_sha256 is not None:
        if isinstance(expected_artifact_sha256, str):
            if len(artifacts) != 1 or artifacts[0].sha256 != expected_artifact_sha256.lower():
                raise DomainError(ErrorCode.CHECKSUM_MISMATCH, "artifact SHA-256 does not match")
        else:
            expected_map = {
                str(path): digest.lower() for path, digest in expected_artifact_sha256.items()
            }
            actual_map = {artifact.path: artifact.sha256 for artifact in artifacts}
            if actual_map != expected_map:
                raise DomainError(
                    ErrorCode.CHECKSUM_MISMATCH,
                    "artifact SHA-256 set does not match",
                    context={
                        "expected_paths": sorted(expected_map),
                        "actual_paths": sorted(actual_map),
                    },
                )

    connection = duckdb.connect()
    try:
        _register_source(connection, source)
        reader = connection.execute("SELECT * FROM _sts_exact_source").to_arrow_reader(
            batch_size=65_536
        )
        actual_names = reader.schema.names
        if actual_names != names:
            raise DomainError(
                ErrorCode.OUTPUT_INVALID,
                "synthetic output schema or column order does not match",
                context={"expected": names, "actual": actual_names},
            )
        expected_arrow_dtypes = expected_arrow_dtypes or {}
        for field, column in zip(reader.schema, columns, strict=True):
            exact_dtype = expected_arrow_dtypes.get(column.name)
            if exact_dtype is not None and str(field.type) != exact_dtype:
                raise DomainError(
                    ErrorCode.OUTPUT_INVALID,
                    f"column {column.name!r} has the wrong decoded dtype",
                    context={"expected": exact_dtype, "actual": str(field.type)},
                )
            if not _logical_dtype_matches(column.kind, field.type):
                raise DomainError(
                    ErrorCode.OUTPUT_INVALID,
                    f"column {column.name!r} is incompatible with logical kind {column.kind.value}",
                    context={"actual": str(field.type)},
                )

        null_counts = [0] * len(columns)
        content_digest = hashlib.sha256()
        actual_rows = 0
        per_rule: Counter[str] = Counter()
        hard_rule_violations = 0
        if compiled_rules is not None:
            from sts.rules.execution import full_validate
        for batch in reader:
            actual_rows += batch.num_rows
            for index in range(batch.num_columns):
                null_counts[index] += batch.column(index).null_count
            try:
                _update_content_hash(content_digest, batch, columns)
            except (OverflowError, TypeError, ValueError) as error:
                raise DomainError(
                    ErrorCode.OUTPUT_INVALID,
                    f"canonical content encoding failed: {error}",
                ) from error
            if compiled_rules is not None:
                validation = full_validate(batch, compiled_rules, codecs=codecs)
                hard_rule_violations += validation.report.violation_union_count
                per_rule.update(validation.report.per_rule_violations)

        if actual_rows != requested_rows:
            raise DomainError(
                ErrorCode.OUTPUT_INVALID,
                "synthetic output row count does not match the request",
                context={"requested_rows": requested_rows, "actual_rows": actual_rows},
            )
        for column, null_count in zip(columns, null_counts, strict=True):
            if not column.nullable and null_count:
                raise DomainError(
                    ErrorCode.OUTPUT_INVALID,
                    f"non-nullable column {column.name!r} contains null values",
                    context={"null_count": null_count},
                )
        if hard_rule_violations != expected_hard_rule_violations:
            raise DomainError(
                ErrorCode.OUTPUT_INVALID,
                "synthetic output hard-rule violation count does not match",
                context={
                    "expected": expected_hard_rule_violations,
                    "actual": hard_rule_violations,
                    "per_rule": dict(per_rule),
                },
            )

        checks: list[ExactColumnCheck] = []
        for field, column, null_count in zip(reader.schema, columns, null_counts, strict=True):
            category_counts: tuple[CategoryCount, ...] = ()
            unique_count: int | None = None
            duplicate_count: int | None = None
            format_violations: int | None = None
            quoted = _quote_identifier(column.name)
            if column.kind in {ColumnKind.CATEGORICAL, ColumnKind.BOOLEAN}:
                rows = connection.execute(
                    f"SELECT {quoted}, COUNT(*) FROM _sts_exact_source "
                    f"WHERE {quoted} IS NOT NULL GROUP BY {quoted}"
                ).fetchall()
                rows.sort(key=lambda row: canonical_json_bytes(row[0]))
                category_counts = tuple(
                    CategoryCount(value=value, count=count) for value, count in rows
                )
            if column.kind in {ColumnKind.IDENTIFIER, ColumnKind.TEXT}:
                nonnull, unique_count = connection.execute(
                    f"SELECT COUNT({quoted}), COUNT(DISTINCT {quoted}) FROM _sts_exact_source"
                ).fetchone()
                duplicate_count = nonnull - unique_count
                if column.format:
                    try:
                        re.compile(column.format)
                    except re.error as error:
                        message = f"invalid format regex for {column.name!r}: {error}"
                        raise ValueError(message) from error
                    format_violations = connection.execute(
                        f"SELECT COUNT(*) FROM _sts_exact_source WHERE {quoted} IS NOT NULL "
                        f"AND NOT regexp_full_match(CAST({quoted} AS VARCHAR), ?)",
                        [column.format],
                    ).fetchone()[0]
                    if format_violations:
                        raise DomainError(
                            ErrorCode.OUTPUT_INVALID,
                            f"column {column.name!r} contains values outside its format",
                            context={"format_violations": format_violations},
                        )
                if column.kind is ColumnKind.IDENTIFIER and duplicate_count:
                    raise DomainError(
                        ErrorCode.OUTPUT_INVALID,
                        f"identifier column {column.name!r} is not unique",
                        context={"duplicate_non_null_count": duplicate_count},
                    )
            checks.append(
                ExactColumnCheck(
                    name=column.name,
                    kind=column.kind,
                    arrow_dtype=str(field.type),
                    null_count=null_count,
                    category_counts=category_counts,
                    unique_non_null_count=unique_count,
                    duplicate_non_null_count=duplicate_count,
                    format_violations=format_violations,
                )
            )

        content_sha256 = content_digest.hexdigest()
        if (
            expected_content_sha256 is not None
            and content_sha256 != expected_content_sha256.lower()
        ):
            raise DomainError(
                ErrorCode.CHECKSUM_MISMATCH,
                "canonical content SHA-256 does not match",
            )
        return ExactScanResult(
            requested_rows=requested_rows,
            actual_rows=actual_rows,
            column_order=tuple(actual_names),
            columns=tuple(checks),
            rule_violations=dict(sorted(per_rule.items())),
            hard_rule_violations=hard_rule_violations,
            artifacts=artifacts,
            canonical_content_sha256=content_sha256,
        )
    finally:
        connection.close()
