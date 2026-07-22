from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import duckdb

from sts.domain import ColumnKind, ColumnSchema, DomainError, ErrorCode
from sts.storage.atomic import sha256_file

from .atomic import discard_part, publish_completed_part, temporary_output_path
from .canonical import scan_csv, scan_parquet
from .models import ExportedFile, ScanResult

_TEXT_KINDS = frozenset({ColumnKind.CATEGORICAL, ColumnKind.TEXT, ColumnKind.IDENTIFIER})


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _duck_type(column: ColumnSchema) -> str:
    if column.kind is ColumnKind.INTEGER:
        return "BIGINT"
    if column.kind is ColumnKind.FIXED_DECIMAL:
        return f"DECIMAL(38,{column.decimal_places})"
    if column.kind is ColumnKind.FLOAT:
        return "DOUBLE"
    if column.kind in _TEXT_KINDS:
        return "VARCHAR"
    if column.kind is ColumnKind.BOOLEAN:
        return "BOOLEAN"
    if column.kind is ColumnKind.DATE:
        return "DATE"
    if column.kind is ColumnKind.DATETIME:
        return "TIMESTAMPTZ" if column.timezone else "TIMESTAMP"
    raise ValueError(f"column {column.name!r} cannot be exported")


def _projection(columns: Sequence[ColumnSchema]) -> str:
    return ", ".join(
        f"CAST({_quote_identifier(column.name)} AS {_duck_type(column)}) "
        f"AS {_quote_identifier(column.name)}"
        for column in columns
    )


def _contains_marker(
    connection: duckdb.DuckDBPyConnection,
    paths: Sequence[Path],
    columns: Sequence[ColumnSchema],
    marker: str,
) -> bool:
    for path in paths:
        for column in columns:
            if column.kind not in _TEXT_KINDS:
                continue
            found = connection.execute(
                f"SELECT EXISTS(SELECT 1 FROM read_parquet(?) "
                f"WHERE CAST({_quote_identifier(column.name)} AS VARCHAR) = ? LIMIT 1)",
                [str(path), marker],
            ).fetchone()[0]
            if found:
                return True
    return False


def _choose_null_marker(
    connection: duckdb.DuckDBPyConnection,
    paths: Sequence[Path],
    columns: Sequence[ColumnSchema],
    content_sha256: str,
) -> str:
    candidate = f"__STS_NULL_{content_sha256}__"
    counter = 0
    while _contains_marker(connection, paths, columns, candidate):
        counter += 1
        candidate = f"__STS_NULL_{content_sha256}_{counter}__"
    return candidate


def _parquet_union_query(
    paths: Sequence[Path], columns: Sequence[ColumnSchema]
) -> tuple[str, list[str]]:
    projection = _projection(columns)
    queries = [f"SELECT {projection} FROM read_parquet(?)" for _ in paths]
    return " UNION ALL ".join(queries), [str(path) for path in paths]


def _assert_equivalent(parquet: ScanResult, csv: ScanResult) -> None:
    mismatches: dict[str, object] = {}
    if parquet.row_count != csv.row_count:
        mismatches["row_count"] = {"parquet": parquet.row_count, "csv": csv.row_count}
    if parquet.schema_signature != csv.schema_signature:
        mismatches["schema_signature"] = {
            "parquet": parquet.schema_signature,
            "csv": csv.schema_signature,
        }
    if parquet.canonical_content_sha256 != csv.canonical_content_sha256:
        mismatches["canonical_content_sha256"] = {
            "parquet": parquet.canonical_content_sha256,
            "csv": csv.canonical_content_sha256,
        }
    if mismatches:
        raise DomainError(
            ErrorCode.OUTPUT_INVALID,
            "Parquet and CSV decoded content are not equivalent",
            context=mismatches,
        )


def export_csv_from_parquet(
    shard_paths: Sequence[str | Path],
    destination: str | Path,
    columns: Sequence[ColumnSchema],
) -> ExportedFile:
    """Use DuckDB COPY to write CSV directly to disk, then full-rescan before publication."""

    paths = tuple(Path(path) for path in shard_paths)
    schema = tuple(columns)
    parquet_scan = scan_parquet(paths, schema)
    target, part = temporary_output_path(destination)
    connection = duckdb.connect()
    try:
        connection.execute("SET threads=1")
        connection.execute("SET preserve_insertion_order=true")
        connection.execute("SET TimeZone='UTC'")
        null_marker = _choose_null_marker(
            connection, paths, schema, parquet_scan.canonical_content_sha256
        )
        query, parameters = _parquet_union_query(paths, schema)
        copy_sql = (
            f"COPY ({query}) TO {_sql_literal(str(part))} "
            f"(FORMAT CSV, HEADER true, FORCE_QUOTE *, NULL {_sql_literal(null_marker)})"
        )
        connection.execute(copy_sql, parameters)
    except Exception:
        discard_part(part)
        raise
    finally:
        connection.close()
    try:
        csv_scan = scan_csv(part, schema, null_marker=null_marker)
        _assert_equivalent(parquet_scan, csv_scan)
        sha256, size_bytes = sha256_file(part)
        publish_completed_part(part, target)
    except Exception:
        discard_part(part)
        raise
    return ExportedFile(
        path=str(target),
        sha256=sha256,
        size_bytes=size_bytes,
        row_count=csv_scan.row_count,
        canonical_content_sha256=csv_scan.canonical_content_sha256,
        null_marker=null_marker,
    )


def verify_parquet_csv_equivalence(
    shard_paths: Sequence[str | Path],
    csv_path: str | Path,
    columns: Sequence[ColumnSchema],
    *,
    null_marker: str,
    expected_rows: int | None = None,
) -> tuple[ScanResult, ScanResult]:
    parquet = scan_parquet(shard_paths, columns)
    csv = scan_csv(csv_path, columns, null_marker=null_marker)
    _assert_equivalent(parquet, csv)
    if expected_rows is not None and parquet.row_count != expected_rows:
        raise DomainError(
            ErrorCode.OUTPUT_INVALID,
            "decoded artifact row count does not match the requested output row count",
            context={"expected_rows": expected_rows, "actual_rows": parquet.row_count},
        )
    return parquet, csv
