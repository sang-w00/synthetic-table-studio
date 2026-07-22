from __future__ import annotations

import csv
import hashlib
import math
import struct
import unicodedata
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from sts.domain import ColumnKind, ColumnSchema, DomainError, ErrorCode

from .models import ScanResult

_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_DATETIME = datetime(1970, 1, 1, tzinfo=UTC)
_BATCH_ROWS = 8_192
_INTEGER_TYPES = frozenset({"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT"})
_FLOAT_TYPES = frozenset({"FLOAT", "REAL", "DOUBLE"})


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _duck_type(column: ColumnSchema) -> str:
    kind = column.kind
    if kind is ColumnKind.INTEGER:
        return "BIGINT"
    if kind is ColumnKind.FIXED_DECIMAL:
        return f"DECIMAL(38,{column.decimal_places})"
    if kind is ColumnKind.FLOAT:
        return "DOUBLE"
    if kind in {ColumnKind.CATEGORICAL, ColumnKind.TEXT, ColumnKind.IDENTIFIER}:
        return "VARCHAR"
    if kind is ColumnKind.BOOLEAN:
        return "BOOLEAN"
    if kind is ColumnKind.DATE:
        return "DATE"
    if kind is ColumnKind.DATETIME:
        return "TIMESTAMPTZ" if column.timezone else "TIMESTAMP"
    raise ValueError(f"column {column.name!r} cannot be included in release output")


def _compatible_source_type(column: ColumnSchema, source_type: str) -> bool:
    upper = source_type.upper()
    kind = column.kind
    if kind is ColumnKind.INTEGER:
        return upper in _INTEGER_TYPES
    if kind is ColumnKind.FIXED_DECIMAL:
        if not upper.startswith("DECIMAL(") or not upper.endswith(")"):
            return False
        try:
            scale = int(upper[:-1].rsplit(",", 1)[1])
        except (IndexError, ValueError):
            return False
        return scale == column.decimal_places
    if kind is ColumnKind.FLOAT:
        return upper in _FLOAT_TYPES
    if kind is ColumnKind.CATEGORICAL:
        return upper == "VARCHAR"
    if kind is ColumnKind.BOOLEAN:
        return upper == "BOOLEAN"
    if kind is ColumnKind.DATE:
        return upper == "DATE"
    if kind is ColumnKind.DATETIME:
        return upper.startswith("TIMESTAMP")
    if kind is ColumnKind.TEXT:
        return upper == "VARCHAR"
    if kind is ColumnKind.IDENTIFIER:
        return upper == "VARCHAR"
    return False


def _schema_signature(columns: Sequence[ColumnSchema]) -> tuple[str, ...]:
    return tuple(column.canonical_sha256 for column in columns)


def _validate_columns(columns: Sequence[ColumnSchema]) -> tuple[ColumnSchema, ...]:
    result = tuple(columns)
    if not result:
        raise ValueError("at least one output column is required")
    names = [column.name for column in result]
    if len(names) != len(set(names)):
        raise ValueError("output column names must be unique")
    if any(column.kind is ColumnKind.EXCLUDED for column in result):
        raise ValueError("excluded columns cannot appear in release output")
    return result


def _describe_parquet(connection: duckdb.DuckDBPyConnection, path: Path) -> list[tuple[str, str]]:
    rows = connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    return [(str(row[0]), str(row[1])) for row in rows]


def _validate_described_schema(
    described: Sequence[tuple[str, str]], columns: Sequence[ColumnSchema], *, source: Path
) -> None:
    actual_names = [name for name, _ in described]
    expected_names = [column.name for column in columns]
    if actual_names != expected_names:
        raise DomainError(
            ErrorCode.OUTPUT_INVALID,
            "artifact column order does not match the declared output schema",
            context={
                "source": str(source),
                "expected_columns": expected_names,
                "actual_columns": actual_names,
            },
        )
    mismatched = [
        {
            "column": column.name,
            "expected_kind": column.kind.value,
            "actual_type": source_type,
        }
        for column, (_, source_type) in zip(columns, described, strict=True)
        if not _compatible_source_type(column, source_type)
    ]
    if mismatched:
        raise DomainError(
            ErrorCode.OUTPUT_INVALID,
            "artifact dtypes do not match the declared output schema",
            context={"source": str(source), "mismatches": mismatched},
        )


def _canonical_integer(value: Any) -> bytes:
    if isinstance(value, bool):
        raise ValueError("boolean is not a canonical integer")
    if isinstance(value, int):
        integer = value
    elif isinstance(value, Decimal) and value == value.to_integral_value():
        integer = int(value)
    else:
        raise ValueError("integer value is not integral")
    return str(integer).encode("ascii")


def _canonical_fixed_decimal(value: Any, scale: int) -> bytes:
    decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    if not decimal.is_finite():
        raise ValueError("fixed decimal must be finite")
    scaled = decimal.scaleb(scale)
    if scaled != scaled.to_integral_value():
        raise ValueError("fixed decimal has more fractional digits than its declared scale")
    return str(int(scaled)).encode("ascii")


def _canonical_float(value: Any) -> bytes:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("float must be finite")
    return struct.pack("<d", number)


def _canonical_date(value: Any) -> bytes:
    if isinstance(value, datetime):
        raise ValueError("datetime is not a canonical date")
    parsed = value if isinstance(value, date) else date.fromisoformat(str(value))
    return struct.pack("<q", (parsed - _EPOCH_DATE).days)


def _canonical_datetime(value: Any) -> bytes:
    if isinstance(value, datetime):
        parsed = value
    else:
        rendered = str(value)
        if rendered.endswith("Z"):
            rendered = rendered[:-1] + "+00:00"
        parsed = datetime.fromisoformat(rendered)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)
    delta = parsed - _EPOCH_DATETIME
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return struct.pack("<q", micros)


def _canonical_boolean(value: Any) -> bytes:
    if not isinstance(value, bool):
        raise ValueError("boolean column contains a non-boolean value")
    return b"\x01" if value else b"\x00"


def _canonical_text(value: Any) -> bytes:
    return b"s" + unicodedata.normalize("NFC", str(value)).encode("utf-8")


def canonical_cell_bytes(value: Any, column: ColumnSchema) -> bytes:
    """Encode one typed cell as uint64-LE length followed by null tag and payload."""

    if value is None:
        body = b"\x00"
    else:
        if column.kind is ColumnKind.INTEGER:
            payload = _canonical_integer(value)
        elif column.kind is ColumnKind.FIXED_DECIMAL:
            payload = _canonical_fixed_decimal(value, column.decimal_places or 0)
        elif column.kind is ColumnKind.FLOAT:
            payload = _canonical_float(value)
        elif column.kind is ColumnKind.DATE:
            payload = _canonical_date(value)
        elif column.kind is ColumnKind.DATETIME:
            payload = _canonical_datetime(value)
        elif column.kind is ColumnKind.BOOLEAN:
            payload = _canonical_boolean(value)
        elif column.kind in {
            ColumnKind.CATEGORICAL,
            ColumnKind.TEXT,
            ColumnKind.IDENTIFIER,
        }:
            payload = _canonical_text(value)
        else:
            raise ValueError(f"unsupported output kind: {column.kind.value}")
        body = b"\x01" + payload
    return struct.pack("<Q", len(body)) + body


def _projection(columns: Sequence[ColumnSchema]) -> str:
    return ", ".join(
        f"CAST({_quote_identifier(column.name)} AS {_duck_type(column)}) "
        f"AS {_quote_identifier(column.name)}"
        for column in columns
    )


def _hash_query(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    parameters: Sequence[Any],
    columns: Sequence[ColumnSchema],
    digest: Any,
) -> int:
    reader = connection.execute(query, list(parameters)).to_arrow_reader(batch_size=_BATCH_ROWS)
    row_count = 0
    for batch in reader:
        arrays = [batch.column(index) for index in range(batch.num_columns)]
        for row_index in range(batch.num_rows):
            for column_index, column in enumerate(columns):
                value = arrays[column_index][row_index].as_py()
                if value is None and not column.nullable:
                    raise ValueError(f"non-nullable column {column.name!r} contains null")
                digest.update(canonical_cell_bytes(value, column))
        row_count += batch.num_rows
    return row_count


def scan_parquet(shard_paths: Sequence[str | Path], columns: Sequence[ColumnSchema]) -> ScanResult:
    paths = tuple(Path(path) for path in shard_paths)
    schema = _validate_columns(columns)
    if not paths:
        raise ValueError("at least one Parquet shard is required")
    digest = hashlib.sha256()
    source_rows: list[int] = []
    connection = duckdb.connect()
    try:
        connection.execute("SET threads=1")
        connection.execute("SET preserve_insertion_order=true")
        connection.execute("SET TimeZone='UTC'")
        select = _projection(schema)
        for path in paths:
            if not path.is_file():
                raise FileNotFoundError(path)
            described = _describe_parquet(connection, path)
            _validate_described_schema(described, schema, source=path)
            try:
                rows = _hash_query(
                    connection,
                    f"SELECT {select} FROM read_parquet(?)",
                    (str(path),),
                    schema,
                    digest,
                )
            except DomainError:
                raise
            except Exception as exc:
                raise DomainError(
                    ErrorCode.OUTPUT_INVALID,
                    "Parquet contains a value that cannot be canonically decoded",
                    context={"source": str(path), "reason": str(exc)},
                ) from exc
            source_rows.append(rows)
    finally:
        connection.close()
    return ScanResult(
        row_count=sum(source_rows),
        canonical_content_sha256=digest.hexdigest(),
        schema_signature=_schema_signature(schema),
        source_row_counts=tuple(source_rows),
    )


def _read_csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as source:
        try:
            return next(csv.reader(source))
        except StopIteration:
            return []


def scan_csv(
    csv_path: str | Path,
    columns: Sequence[ColumnSchema],
    *,
    null_marker: str,
) -> ScanResult:
    path = Path(csv_path)
    schema = _validate_columns(columns)
    if not path.is_file():
        raise FileNotFoundError(path)
    header = _read_csv_header(path)
    expected_header = [column.name for column in schema]
    if header != expected_header:
        raise DomainError(
            ErrorCode.OUTPUT_INVALID,
            "CSV column order does not match the declared output schema",
            context={"expected_columns": expected_header, "actual_columns": header},
        )
    column_types = ",".join(
        f"{_sql_literal(column.name)}:{_sql_literal(_duck_type(column))}" for column in schema
    )
    select = _projection(schema)
    table = (
        "read_csv(?, header=true, auto_detect=false, strict_mode=true, "
        f"columns={{{column_types}}}, nullstr={_sql_literal(null_marker)}, "
        "encoding='utf-8')"
    )
    digest = hashlib.sha256()
    connection = duckdb.connect()
    try:
        connection.execute("SET threads=1")
        connection.execute("SET preserve_insertion_order=true")
        connection.execute("SET TimeZone='UTC'")
        try:
            rows = _hash_query(
                connection,
                f"SELECT {select} FROM {table}",
                (str(path),),
                schema,
                digest,
            )
        except DomainError:
            raise
        except Exception as exc:
            raise DomainError(
                ErrorCode.OUTPUT_INVALID,
                "CSV contains a value that cannot be canonically decoded",
                context={"source": str(path), "reason": str(exc)},
            ) from exc
    finally:
        connection.close()
    return ScanResult(
        row_count=rows,
        canonical_content_sha256=digest.hexdigest(),
        schema_signature=_schema_signature(schema),
        source_row_counts=(rows,),
    )
