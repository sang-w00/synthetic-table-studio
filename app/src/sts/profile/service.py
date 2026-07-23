from __future__ import annotations

from pathlib import Path
from typing import Literal

import duckdb
import pyarrow.parquet as pq

from sts.domain import ColumnKind, DomainError, ErrorCode

from .models import ColumnProfile, DatasetProfile, ParseSuccess, ValueCount

MAX_COLUMNS = 70
LOW_CARDINALITY_LIMIT = 256
IDENTIFIER_CARDINALITY_RATIO = 0.9


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _connection(memory_limit: str, temp_directory: Path) -> duckdb.DuckDBPyConnection:
    temp_directory.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(database=":memory:")
    connection.execute(f"SET memory_limit = {_sql_literal(memory_limit)}")
    connection.execute(f"SET temp_directory = {_sql_literal(str(temp_directory))}")
    connection.execute("SET threads = 2")
    return connection


def _physical_candidate(storage_type: str) -> ColumnKind | None:
    upper = storage_type.upper()
    if upper in {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
    }:
        return ColumnKind.INTEGER
    if upper.startswith("DECIMAL"):
        return ColumnKind.FIXED_DECIMAL
    if upper in {"FLOAT", "DOUBLE", "REAL"}:
        return ColumnKind.FLOAT
    if upper == "BOOLEAN":
        return ColumnKind.BOOLEAN
    if upper == "DATE":
        return ColumnKind.DATE
    if upper.startswith("TIMESTAMP"):
        return ColumnKind.DATETIME
    return None


def _candidate(
    *,
    view: Literal["raw", "typed"],
    storage_type: str,
    nonnull_count: int,
    approx_cardinality: int,
    parse: ParseSuccess,
    digit_only_count: int,
) -> tuple[ColumnKind, bool, tuple[ColumnKind, ...]]:
    physical = _physical_candidate(storage_type) if view == "typed" else None
    if physical is not None:
        return physical, False, ()
    if nonnull_count == 0:
        return ColumnKind.CATEGORICAL, False, ()
    if parse.boolean == nonnull_count:
        return ColumnKind.BOOLEAN, False, ()
    if parse.integer == nonnull_count:
        if digit_only_count == nonnull_count:
            # Codes, postal codes, and identifiers are commonly digit-only. Numeric is only a
            # proposal here and must never be silently confirmed from lexical shape alone.
            return (
                ColumnKind.CATEGORICAL,
                True,
                (ColumnKind.INTEGER, ColumnKind.IDENTIFIER),
            )
        return ColumnKind.INTEGER, False, ()
    if parse.float == nonnull_count:
        return ColumnKind.FLOAT, False, ()
    if parse.date == nonnull_count:
        return ColumnKind.DATE, False, ()
    if parse.datetime == nonnull_count:
        return ColumnKind.DATETIME, False, ()
    if (
        nonnull_count > LOW_CARDINALITY_LIMIT
        and approx_cardinality >= nonnull_count * IDENTIFIER_CARDINALITY_RATIO
    ):
        # approx_count_distinct is intentionally used only for a review-required proposal:
        # HyperLogLog error and free-form unique text make silent identifier confirmation unsafe.
        return ColumnKind.IDENTIFIER, True, (ColumnKind.TEXT,)
    if approx_cardinality <= LOW_CARDINALITY_LIMIT:
        return ColumnKind.CATEGORICAL, False, ()
    return ColumnKind.TEXT, False, ()


def profile_parquet(
    path: str | Path,
    *,
    view: Literal["raw", "typed"] = "raw",
    memory_limit: str = "1GB",
    temp_directory: str | Path | None = None,
) -> DatasetProfile:
    """Profile Parquet with aggregate-only DuckDB scans.

    No source column is materialized in Python. The only variable-size query result is a
    low-cardinality frequency table hard-limited to 257 rows.
    """

    source_path = Path(path).resolve(strict=True)
    parquet_names = pq.ParquetFile(source_path).schema_arrow.names
    if len(parquet_names) != len(set(parquet_names)):
        raise DomainError(ErrorCode.SCHEMA_INVALID, "input has duplicate column names")
    visible_names = [name for name in parquet_names if name != "__sts_row_id"]
    if not visible_names:
        raise DomainError(ErrorCode.SCHEMA_INVALID, "input has no data columns")
    if len(visible_names) > MAX_COLUMNS:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            f"input has {len(visible_names)} columns; maximum is {MAX_COLUMNS}",
        )

    spill = (
        Path(temp_directory)
        if temp_directory is not None
        else source_path.parent / ".profile-spill"
    )
    connection = _connection(memory_limit, spill)
    source_sql = f"read_parquet({_sql_literal(str(source_path))})"
    try:
        described = connection.execute(f"DESCRIBE SELECT * FROM {source_sql}").fetchall()
        storage_types = {str(row[0]): str(row[1]) for row in described}
        row_count = int(connection.execute(f"SELECT count(*) FROM {source_sql}").fetchone()[0])
        if row_count == 0:
            raise DomainError(ErrorCode.SCHEMA_INVALID, "input contains no records")

        profiles: list[ColumnProfile] = []
        for name in visible_names:
            quoted = _identifier(name)
            storage_type = storage_types[name]
            text = f"CAST({quoted} AS VARCHAR)"
            null_predicate = (
                f"({quoted} IS NULL OR {text} = '')" if view == "raw" else f"{quoted} IS NULL"
            )
            nonnull_predicate = f"NOT ({null_predicate})"
            aggregate = connection.execute(
                f"""
                SELECT
                    count(*) FILTER (WHERE {null_predicate}),
                    count(*) FILTER (WHERE {nonnull_predicate}),
                    CAST(min({quoted}) FILTER (WHERE {nonnull_predicate}) AS VARCHAR),
                    CAST(max({quoted}) FILTER (WHERE {nonnull_predicate}) AS VARCHAR),
                    CAST(
                        approx_count_distinct({quoted})
                        FILTER (WHERE {nonnull_predicate}) AS BIGINT
                    ),
                    min(length({text})) FILTER (WHERE {nonnull_predicate}),
                    max(length({text})) FILTER (WHERE {nonnull_predicate}),
                    count(*) FILTER (
                        WHERE {nonnull_predicate}
                          AND try_cast(trim({text}) AS BIGINT) IS NOT NULL
                    ),
                    count(*) FILTER (
                        WHERE {nonnull_predicate}
                          AND try_cast(trim({text}) AS DOUBLE) IS NOT NULL
                          AND isfinite(try_cast(trim({text}) AS DOUBLE))
                    ),
                    count(*) FILTER (
                        WHERE {nonnull_predicate}
                          AND lower(trim({text})) IN ('true', 'false')
                    ),
                    count(*) FILTER (
                        WHERE {nonnull_predicate}
                          AND regexp_full_match(trim({text}), '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}')
                          AND try_cast(trim({text}) AS DATE) IS NOT NULL
                    ),
                    count(*) FILTER (
                        WHERE {nonnull_predicate}
                          AND regexp_matches(trim({text}), '[T ][0-9]{{2}}:[0-9]{{2}}')
                          AND try_cast(trim({text}) AS TIMESTAMP) IS NOT NULL
                    ),
                    count(*) FILTER (
                        WHERE {nonnull_predicate}
                          AND regexp_full_match(trim({text}), '[0-9]+')
                    )
                FROM {source_sql}
                """
            ).fetchone()
            assert aggregate is not None
            null_count, nonnull_count = int(aggregate[0]), int(aggregate[1])
            approx_cardinality = max(0, int(aggregate[4] or 0))
            parse = ParseSuccess(
                integer=int(aggregate[7]),
                float=int(aggregate[8]),
                boolean=int(aggregate[9]),
                date=int(aggregate[10]),
                datetime=int(aggregate[11]),
            )
            candidate, confirmation, alternatives = _candidate(
                view=view,
                storage_type=storage_type,
                nonnull_count=nonnull_count,
                approx_cardinality=approx_cardinality,
                parse=parse,
                digit_only_count=int(aggregate[12]),
            )
            fixed_length = (
                int(aggregate[5])
                if nonnull_count > 0 and aggregate[5] is not None and aggregate[5] == aggregate[6]
                else None
            )

            exact_counts: tuple[ValueCount, ...] | None = None
            if approx_cardinality <= LOW_CARDINALITY_LIMIT:
                rows = connection.execute(
                    f"""
                    SELECT CAST({quoted} AS VARCHAR), count(*)
                    FROM {source_sql}
                    WHERE {nonnull_predicate}
                    GROUP BY {quoted}
                    ORDER BY count(*) DESC, CAST({quoted} AS VARCHAR)
                    LIMIT {LOW_CARDINALITY_LIMIT + 1}
                    """
                ).fetchall()
                if len(rows) <= LOW_CARDINALITY_LIMIT:
                    exact_counts = tuple(
                        ValueCount(value=str(value), count=int(count)) for value, count in rows
                    )
                    approx_cardinality = len(rows)

            quantiles: tuple[str | None, str | None, str | None] | None = None
            if candidate in {ColumnKind.INTEGER, ColumnKind.FIXED_DECIMAL, ColumnKind.FLOAT}:
                values = connection.execute(
                    f"""
                    SELECT approx_quantile(try_cast({quoted} AS DOUBLE), [0.25, 0.5, 0.75])
                    FROM {source_sql}
                    WHERE try_cast({quoted} AS DOUBLE) IS NOT NULL
                      AND isfinite(try_cast({quoted} AS DOUBLE))
                    """
                ).fetchone()[0]
                if values is not None:
                    quantiles = tuple(None if value is None else str(value) for value in values)  # type: ignore[assignment]

            profiles.append(
                ColumnProfile(
                    name=name,
                    storage_type=storage_type,
                    row_count=row_count,
                    null_count=null_count,
                    nonnull_count=nonnull_count,
                    minimum=None if aggregate[2] is None else str(aggregate[2]),
                    maximum=None if aggregate[3] is None else str(aggregate[3]),
                    approx_quantiles=quantiles,
                    approx_cardinality=approx_cardinality,
                    exact_low_cardinality=exact_counts,
                    fixed_length=fixed_length,
                    parse_success=parse,
                    candidate_type=candidate,
                    candidate_requires_confirmation=confirmation,
                    candidate_alternatives=alternatives,
                )
            )

        return DatasetProfile(
            view=view,
            row_count=row_count,
            column_count=len(profiles),
            columns=tuple(profiles),
            metadata={
                "bounded": True,
                "engine": f"duckdb-{duckdb.__version__}",
                "low_cardinality_limit": LOW_CARDINALITY_LIMIT,
            },
        )
    except DomainError:
        raise
    except duckdb.Error as exc:
        raise DomainError(
            ErrorCode.WORKER_FAILED,
            "DuckDB could not complete the bounded dataset profile",
        ) from exc
    finally:
        connection.close()
