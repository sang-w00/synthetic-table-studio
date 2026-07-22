from __future__ import annotations

import contextlib
import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import duckdb
import pyarrow.parquet as pq

from sts.domain import ColumnKind, ColumnSchema, DomainError, ErrorCode
from sts.storage.atomic import sha256_file

MAX_COLUMNS = 70
ROW_GROUP_TARGET_BYTES = 192 * 1024 * 1024


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class NormalizationResult:
    path: Path
    sha256: str
    size_bytes: int
    row_count: int
    column_count: int
    row_group_target_bytes: int = ROW_GROUP_TARGET_BYTES


def raw_columns(path: str | Path) -> tuple[str, ...]:
    names = pq.ParquetFile(Path(path)).schema_arrow.names
    if len(names) != len(set(names)):
        raise DomainError(ErrorCode.SCHEMA_INVALID, "input has duplicate column names")
    visible = tuple(name for name in names if name != "__sts_row_id")
    if not visible:
        raise DomainError(ErrorCode.SCHEMA_INVALID, "input has no data columns")
    if len(visible) > MAX_COLUMNS:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            f"input has {len(visible)} columns; maximum is {MAX_COLUMNS}",
        )
    return visible


def validate_schema(path: str | Path, columns: tuple[ColumnSchema, ...]) -> tuple[str, ...]:
    source_columns = raw_columns(path)
    names = tuple(column.name for column in columns)
    if not columns:
        raise DomainError(ErrorCode.SCHEMA_INVALID, "schema must contain at least one column")
    if len(columns) > MAX_COLUMNS:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID, f"schema exceeds the {MAX_COLUMNS}-column limit"
        )
    if len(names) != len(set(names)):
        raise DomainError(ErrorCode.SCHEMA_INVALID, "schema has duplicate column names")
    if names != source_columns:
        missing = sorted(set(source_columns) - set(names))
        unexpected = sorted(set(names) - set(source_columns))
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            "schema column names and order must exactly match the raw dataset",
            context={"missing": missing, "unexpected": unexpected},
        )
    return source_columns


def _cast_expression(column: ColumnSchema) -> tuple[str, str]:
    source = _identifier(column.name)
    kind = column.kind
    if kind is ColumnKind.INTEGER:
        valid = (
            f"regexp_full_match(trim(CAST({source} AS VARCHAR)), '[+-]?[0-9]+') "
            f"AND try_cast({source} AS BIGINT) IS NOT NULL"
        )
        expression = f"CAST(trim(CAST({source} AS VARCHAR)) AS BIGINT)"
    elif kind is ColumnKind.FIXED_DECIMAL:
        assert column.decimal_places is not None
        valid = (
            "regexp_full_match("
            f"trim(CAST({source} AS VARCHAR)), "
            "'[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)') "
            f"AND try_cast({source} AS DECIMAL(38,{column.decimal_places})) IS NOT NULL"
        )
        expression = f"CAST(trim(CAST({source} AS VARCHAR)) AS DECIMAL(38,{column.decimal_places}))"
    elif kind is ColumnKind.FLOAT:
        valid = (
            f"try_cast({source} AS DOUBLE) IS NOT NULL AND isfinite(try_cast({source} AS DOUBLE))"
        )
        expression = f"CAST({source} AS DOUBLE)"
    elif kind is ColumnKind.BOOLEAN:
        valid = f"lower(trim(CAST({source} AS VARCHAR))) IN ('true', 'false')"
        expression = f"CAST(lower(trim(CAST({source} AS VARCHAR))) AS BOOLEAN)"
    elif kind is ColumnKind.DATE:
        if column.format:
            parsed = f"try_strptime(CAST({source} AS VARCHAR), {_literal(column.format)})"
        else:
            parsed = f"try_cast({source} AS DATE)"
        valid = f"{parsed} IS NOT NULL"
        expression = f"CAST({parsed} AS DATE)"
    elif kind is ColumnKind.DATETIME:
        if not column.timezone:
            raise DomainError(
                ErrorCode.SCHEMA_INVALID,
                f"datetime column {column.name!r} requires an explicit timezone",
                context={"column": column.name, "reason": "timezone_ambiguity"},
            )
        if column.format:
            parsed = f"try_strptime(CAST({source} AS VARCHAR), {_literal(column.format)})"
        else:
            parsed = f"try_cast({source} AS TIMESTAMP)"
        valid = f"{parsed} IS NOT NULL"
        expression = f"timezone({_literal(column.timezone)}, CAST({parsed} AS TIMESTAMP))"
    else:
        valid = "TRUE"
        expression = f"CAST({source} AS VARCHAR)"
    return valid, expression


def normalize_to_parquet(
    raw_path: str | Path,
    output_path: str | Path,
    columns: tuple[ColumnSchema, ...],
    *,
    memory_limit: str = "1GB",
    temp_directory: str | Path | None = None,
) -> NormalizationResult:
    """Validate and normalize raw Parquet using DuckDB's streaming Parquet writer."""

    source_path = Path(raw_path).resolve(strict=True)
    destination = Path(output_path).resolve(strict=False)
    validate_schema(source_path, columns)
    if destination.exists() or destination.is_symlink():
        raise DomainError(
            ErrorCode.IMMUTABLE_PATH_EXISTS,
            f"normalized output already exists: {destination.name}",
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    spill = (
        Path(temp_directory)
        if temp_directory is not None
        else destination.parent / ".normalize-spill"
    )
    spill.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(database=":memory:")
    source_sql = f"read_parquet({_literal(str(source_path))})"
    part = destination.with_name(f".{destination.name}.{uuid4().hex}.part")
    try:
        connection.execute(f"SET memory_limit = {_literal(memory_limit)}")
        connection.execute(f"SET temp_directory = {_literal(str(spill))}")
        connection.execute("SET threads = 2")
        connection.execute("SET preserve_insertion_order = false")
        described = connection.execute(f"DESCRIBE SELECT * FROM {source_sql}").fetchall()
        source_names = {str(row[0]) for row in described}
        if "__sts_row_id" not in source_names:
            raise DomainError(ErrorCode.SCHEMA_INVALID, "raw dataset is missing __sts_row_id")
        row_stats = connection.execute(
            f"""
            SELECT count(*), min(__sts_row_id), max(__sts_row_id), count(DISTINCT __sts_row_id)
            FROM {source_sql}
            """
        ).fetchone()
        assert row_stats is not None
        row_count = int(row_stats[0])
        if row_count == 0:
            raise DomainError(ErrorCode.SCHEMA_INVALID, "input contains no records")
        if (
            int(row_stats[1]) != 0
            or int(row_stats[2]) != row_count - 1
            or int(row_stats[3]) != row_count
        ):
            raise DomainError(
                ErrorCode.SCHEMA_INVALID,
                "__sts_row_id must be a unique contiguous 0-based logical record number",
            )

        selections = ['CAST("__sts_row_id" AS BIGINT) AS "__sts_row_id"']
        for column in columns:
            valid, cast = _cast_expression(column)
            source = _identifier(column.name)
            semantic_null = f"({source} IS NULL OR CAST({source} AS VARCHAR) = '')"
            invalid_count = int(
                connection.execute(
                    f"""
                    SELECT count(*) FROM {source_sql}
                    WHERE NOT ({semantic_null}) AND NOT ({valid})
                    """
                ).fetchone()[0]
            )
            if invalid_count:
                raise DomainError(
                    ErrorCode.SCHEMA_INVALID,
                    f"column {column.name!r} has {invalid_count} values that cannot "
                    f"be normalized as {column.kind.value}",
                    context={"column": column.name, "invalid_count": invalid_count},
                )
            if not column.nullable:
                null_count = int(
                    connection.execute(
                        f"SELECT count(*) FROM {source_sql} WHERE {semantic_null}"
                    ).fetchone()[0]
                )
                if null_count:
                    raise DomainError(
                        ErrorCode.SCHEMA_INVALID,
                        f"non-nullable column {column.name!r} contains null values",
                        context={"column": column.name, "null_count": null_count},
                    )
            selections.append(f"CASE WHEN {semantic_null} THEN NULL ELSE {cast} END AS {source}")

        select_sql = ",\n".join(selections)
        connection.execute(
            f"""
            COPY (
                SELECT {select_sql}
                FROM {source_sql}
                ORDER BY __sts_row_id
            ) TO {_literal(str(part))} (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE_BYTES {ROW_GROUP_TARGET_BYTES}
            )
            """
        )
        descriptor = os.open(part, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _fsync_directory(destination.parent)
        os.rename(part, destination)
        _fsync_directory(destination.parent)
        digest, size = sha256_file(destination)
        return NormalizationResult(
            path=destination,
            sha256=digest,
            size_bytes=size,
            row_count=row_count,
            column_count=len(columns),
        )
    except DomainError:
        with contextlib.suppress(FileNotFoundError):
            part.unlink()
        raise
    except duckdb.Error as exc:
        with contextlib.suppress(FileNotFoundError):
            part.unlink()
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            "DuckDB rejected the normalization schema or source values",
        ) from exc
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            part.unlink()
        raise
    finally:
        connection.close()
