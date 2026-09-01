from __future__ import annotations

import hashlib
import hmac
import json
import math
import uuid
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from sts.domain import DomainError, ErrorCode
from sts.domain.canonical import CanonicalModel
from sts.jobs.protocol import (
    ManifestSnapshot,
    SnapshotFile,
    WorkerRequestEnvelope,
    confined_existing_path,
    resolve_manifest_snapshot,
    validate_workspace_relative_path,
)
from sts.jobs.seeds import CandidateShardPlan
from sts.storage.atomic import AtomicPublisher

ROW_ID_COLUMN = "__sts_row_id"
TRAIN_NUMERATOR = 4
TRAIN_DENOMINATOR = 5
TRAIN_THRESHOLD_U64 = (2**64 * TRAIN_NUMERATOR) // TRAIN_DENOMINATOR
CANDIDATE_TARGET_BYTES = 128 * 1024**2
MIN_CANDIDATE_ROWS = 10_000
MAX_CANDIDATE_ROWS = 250_000
_HMAC_PARTITION_DOMAIN = b"sts/utility/train-holdout/v1\x00"
_HMAC_PRIORITY_DOMAIN = b"sts/utility/training-priority/v1\x00"
_SHA256_HEX = frozenset("0123456789abcdef")
_FORBIDDEN_GENERATE_SNAPSHOT_KEYS = {
    "bounded_training_sample",
    "external_holdout",
    "holdout",
    "normalized_source",
    "source",
}


class HmacPartitionSQL(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    algorithm: Literal["hmac-sha256-first-u64-be"] = "hmac-sha256-first-u64-be"
    key_commitment_sha256: str
    train_threshold_u64: Literal[TRAIN_THRESHOLD_U64] = TRAIN_THRESHOLD_U64
    train_numerator: Literal[TRAIN_NUMERATOR] = TRAIN_NUMERATOR
    train_denominator: Literal[TRAIN_DENOMINATOR] = TRAIN_DENOMINATOR
    source_view: str = Field(exclude=True)
    normalized_path: str = Field(exclude=True)

    @field_validator("key_commitment_sha256")
    @classmethod
    def validate_commitment(cls, value: str) -> str:
        lowered = value.lower()
        if len(lowered) != 64 or any(character not in _SHA256_HEX for character in lowered):
            raise ValueError("key commitment must be a lowercase SHA-256 digest")
        return lowered

    @property
    def source_sql(self) -> str:
        return _quote_identifier(self.source_view)

    def train_predicate(self, row_id_column: str = ROW_ID_COLUMN) -> str:
        del row_id_column
        return f"__sts_partition_score < {self.train_threshold_u64}::UBIGINT"

    def holdout_predicate(self, row_id_column: str = ROW_ID_COLUMN) -> str:
        return f"NOT ({self.train_predicate(row_id_column)})"

    def priority_expression(self, row_id_column: str = ROW_ID_COLUMN) -> str:
        del row_id_column
        return "__sts_priority"

    def report_projection(self) -> dict[str, Any]:
        """Return the only partition fields permitted in reports and manifests."""

        return {
            "key_commitment_sha256": self.key_commitment_sha256,
            "train_threshold_u64": self.train_threshold_u64,
        }


class PartitionCounts(CanonicalModel):
    total_rows: Annotated[int, Field(ge=0)]
    train_rows: Annotated[int, Field(ge=0)]
    holdout_rows: Annotated[int, Field(ge=0)]

    @model_validator(mode="after")
    def validate_partition(self) -> PartitionCounts:
        if self.train_rows + self.holdout_rows != self.total_rows:
            raise ValueError("train and holdout counts must cover the source exactly")
        return self


class StratumAllocation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    values: tuple[Any, ...]
    population_rows: Annotated[int, Field(ge=0)]
    sampled_rows: Annotated[int, Field(ge=0)]

    @model_validator(mode="after")
    def validate_allocation(self) -> StratumAllocation:
        if self.sampled_rows > self.population_rows:
            raise ValueError("sampled_rows cannot exceed population_rows")
        return self


class BoundedTrainingSample:
    """A bounded Arrow sample and the deterministic allocation used to produce it."""

    __slots__ = (
        "allocation",
        "partition",
        "requested_max_rows",
        "table",
        "train_population_rows",
    )

    def __init__(
        self,
        *,
        table: pa.Table,
        partition: HmacPartitionSQL,
        requested_max_rows: int,
        train_population_rows: int,
        allocation: tuple[StratumAllocation, ...],
    ) -> None:
        if table.num_rows != min(requested_max_rows, train_population_rows):
            raise ValueError("bounded sample does not contain the exact admitted row count")
        if ROW_ID_COLUMN in table.column_names:
            raise ValueError("bounded training sample must not expose __sts_row_id")
        if allocation and sum(item.sampled_rows for item in allocation) != table.num_rows:
            raise ValueError("stratification allocation does not sum to sampled rows")
        self.table = table
        self.partition = partition
        self.requested_max_rows = requested_max_rows
        self.train_population_rows = train_population_rows
        self.allocation = allocation

    @property
    def sampled_rows(self) -> int:
        return self.table.num_rows

    def allocation_manifest(self, stratify_by: Sequence[str]) -> dict[str, Any]:
        if len(stratify_by) > 2:
            raise ValueError("at most two stratification columns are supported")
        return {
            "stratify_by": list(stratify_by),
            "requested_max_rows": self.requested_max_rows,
            "train_population_rows": self.train_population_rows,
            "sampled_rows": self.sampled_rows,
            "strata": [
                {
                    "values": [_json_scalar(value) for value in item.values],
                    "population_rows": item.population_rows,
                    "sampled_rows": item.sampled_rows,
                }
                for item in self.allocation
            ],
        }


class TrainingMemoryEstimate(CanonicalModel):
    arrow_deep_bytes: Annotated[int, Field(ge=0)]
    pandas_deep_bytes_estimate: Annotated[int, Field(ge=0)]
    safety_factor: Literal[1.5] = 1.5
    required_lease_bytes: Annotated[int, Field(ge=0)]
    worker_lease_bytes: Annotated[int, Field(gt=0)]
    admitted: bool


class CheckpointCompatibility(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    source_manifest_sha256: str
    schema_sha256: str
    rules_sha256: str
    engine_sha256: str

    @field_validator(
        "source_manifest_sha256",
        "schema_sha256",
        "rules_sha256",
        "engine_sha256",
    )
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        lowered = value.lower()
        if len(lowered) != 64 or any(character not in _SHA256_HEX for character in lowered):
            raise ValueError("compatibility values must be SHA-256 digests")
        return lowered


class ArgnFeatureAvailability(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    probe_sha256: str
    engine_sha256: str
    platform_machine: str
    backend_enabled: bool
    bounded_generation: bool
    fresh_process_checkpoint_generation: bool
    deterministic_generation: bool
    mps_enabled: bool
    multiprocess_clones_enabled: bool
    cuda_device_count: Annotated[int, Field(ge=0)]
    max_generation_processes: Annotated[int, Field(ge=0)]
    reasons: tuple[str, ...]

    @model_validator(mode="after")
    def validate_process_availability(self) -> ArgnFeatureAvailability:
        expected_minimum = 1 if self.backend_enabled else 0
        if self.max_generation_processes < expected_minimum:
            raise ValueError("enabled backend must permit one generation process")
        if not self.multiprocess_clones_enabled and self.max_generation_processes > 1:
            raise ValueError("multiprocess-disabled gate cannot permit multiple processes")
        return self


def _quote_identifier(value: str) -> str:
    if not value or "\x00" in value:
        raise ValueError("SQL identifier must be nonempty and cannot contain NUL")
    return '"' + value.replace('"', '""') + '"'


def _validate_partition_key(key: bytes) -> bytes:
    if not isinstance(key, bytes):
        raise TypeError("HMAC partition key must be bytes")
    if len(key) < 32:
        raise ValueError("HMAC partition key must contain at least 256 bits")
    return key


def hmac_key_commitment(key: bytes) -> str:
    return hashlib.sha256(_validate_partition_key(key)).hexdigest()


def _hmac_u64(key: bytes, domain: bytes, row_id: int) -> int:
    if isinstance(row_id, bool) or not isinstance(row_id, int) or not 0 <= row_id < 2**63:
        raise ValueError("__sts_row_id must be a nonnegative signed 64-bit integer")
    message = domain + row_id.to_bytes(8, "big", signed=False)
    return int.from_bytes(hmac.new(key, message, hashlib.sha256).digest()[:8], "big")


def register_hmac_partition_sql(
    connection: duckdb.DuckDBPyConnection,
    *,
    key: bytes,
    normalized_parquet: str | Path,
) -> HmacPartitionSQL:
    """Build a DuckDB partition view from bounded Arrow HMAC scoring batches."""

    validated_key = _validate_partition_key(key)
    path = str(Path(normalized_parquet).resolve(strict=True))
    _validate_normalized_schema(connection, path, ())
    commitment = hmac_key_commitment(validated_key)
    suffix = commitment[:16]
    score_table = f"__sts_hmac_scores_{suffix}"
    source_view = f"__sts_hmac_partitioned_{suffix}"
    quoted_score_table = _quote_identifier(score_table)
    quoted_source_view = _quote_identifier(source_view)
    connection.execute(f"DROP VIEW IF EXISTS {quoted_source_view}")
    connection.execute(f"DROP TABLE IF EXISTS {quoted_score_table}")
    connection.execute(
        f"""
        CREATE TEMP TABLE {quoted_score_table} (
            {_quote_identifier(ROW_ID_COLUMN)} BIGINT PRIMARY KEY,
            __sts_partition_score UBIGINT NOT NULL,
            __sts_priority UBIGINT NOT NULL
        )
        """
    )

    parquet_file = pq.ParquetFile(path)
    arrow_batch_name = f"__sts_hmac_batch_{suffix}"
    try:
        for batch in parquet_file.iter_batches(
            batch_size=65_536,
            columns=[ROW_ID_COLUMN],
            use_threads=True,
        ):
            row_ids = batch.column(0).to_pylist()
            if any(row_id is None for row_id in row_ids):
                raise DomainError(
                    ErrorCode.SCHEMA_INVALID,
                    "normalized __sts_row_id cannot contain nulls",
                )
            try:
                partition_scores = [
                    _hmac_u64(validated_key, _HMAC_PARTITION_DOMAIN, row_id) for row_id in row_ids
                ]
                priorities = [
                    _hmac_u64(validated_key, _HMAC_PRIORITY_DOMAIN, row_id) for row_id in row_ids
                ]
            except (TypeError, ValueError) as error:
                raise DomainError(
                    ErrorCode.SCHEMA_INVALID,
                    "normalized __sts_row_id values must be unique nonnegative int64 values",
                ) from error
            scores = pa.table(
                {
                    ROW_ID_COLUMN: pa.array(row_ids, type=pa.int64()),
                    "__sts_partition_score": pa.array(partition_scores, type=pa.uint64()),
                    "__sts_priority": pa.array(priorities, type=pa.uint64()),
                }
            )
            connection.register(arrow_batch_name, scores)
            try:
                connection.execute(
                    f"INSERT INTO {quoted_score_table} SELECT * FROM "
                    f"{_quote_identifier(arrow_batch_name)}"
                )
            except duckdb.ConstraintException as error:
                raise DomainError(
                    ErrorCode.SCHEMA_INVALID,
                    "normalized __sts_row_id values must be unique",
                ) from error
            finally:
                connection.unregister(arrow_batch_name)
        path_literal = "'" + path.replace("'", "''") + "'"
        connection.execute(
            f"""
            CREATE TEMP VIEW {quoted_source_view} AS
            SELECT src.*, scores.__sts_partition_score, scores.__sts_priority
            FROM read_parquet({path_literal}) AS src
            JOIN {quoted_score_table} AS scores
              ON src.{_quote_identifier(ROW_ID_COLUMN)}
               = scores.{_quote_identifier(ROW_ID_COLUMN)}
            """
        )
    except Exception:
        connection.execute(f"DROP VIEW IF EXISTS {quoted_source_view}")
        connection.execute(f"DROP TABLE IF EXISTS {quoted_score_table}")
        raise

    return HmacPartitionSQL(
        key_commitment_sha256=commitment,
        source_view=source_view,
        normalized_path=path,
    )


def partition_counts(
    connection: duckdb.DuckDBPyConnection,
    *,
    normalized_parquet: str | Path,
    partition: HmacPartitionSQL,
) -> PartitionCounts:
    path = str(Path(normalized_parquet).resolve(strict=True))
    if path != partition.normalized_path:
        raise ValueError("partition SQL was registered for a different normalized Parquet file")
    row = connection.execute(
        f"""
        SELECT
            count(*)::UBIGINT,
            count(*) FILTER (
                WHERE {partition.train_predicate()}
            )::UBIGINT
        FROM {partition.source_sql}
        """
    ).fetchone()
    assert row is not None
    total_rows, train_rows = (int(value) for value in row)
    return PartitionCounts(
        total_rows=total_rows,
        train_rows=train_rows,
        holdout_rows=total_rows - train_rows,
    )


def _validate_normalized_schema(
    connection: duckdb.DuckDBPyConnection,
    path: str,
    stratify_by: Sequence[str],
) -> None:
    if len(stratify_by) > 2:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            "utility sampling supports at most two explicit stratification columns",
        )
    if len(set(stratify_by)) != len(stratify_by):
        raise DomainError(ErrorCode.SCHEMA_INVALID, "stratification columns must be unique")
    description = connection.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)",
        [path],
    ).fetchall()
    column_types = {str(row[0]): str(row[1]).upper() for row in description}
    if ROW_ID_COLUMN not in column_types:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            f"normalized Parquet must contain {ROW_ID_COLUMN}",
        )
    if "INT" not in column_types[ROW_ID_COLUMN]:
        raise DomainError(ErrorCode.SCHEMA_INVALID, f"{ROW_ID_COLUMN} must be an integer")
    missing = [column for column in stratify_by if column not in column_types]
    if missing:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            "stratification columns are absent from normalized Parquet",
            context={"columns": missing},
        )
    reserved = {"__sts_priority", "__sts_rank", "__sts_population_rows", "__sts_quota"}
    collisions = sorted(reserved.intersection(column_types))
    if collisions:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            "normalized Parquet uses reserved utility planning columns",
            context={"columns": collisions},
        )


def _json_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"hex": bytes(value).hex()}
    return str(value)


def _allocation_sort_key(values: tuple[Any, ...]) -> str:
    tagged = [{"type": type(value).__name__, "value": _json_scalar(value)} for value in values]
    return json.dumps(tagged, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hamilton_allocation(
    groups: Sequence[tuple[tuple[Any, ...], int]],
    target_rows: int,
) -> list[tuple[tuple[Any, ...], int, int]]:
    population_rows = sum(count for _, count in groups)
    if not 0 <= target_rows <= population_rows:
        raise ValueError("target rows must be bounded by the stratum population")
    if population_rows == 0:
        return []

    staged: list[dict[str, Any]] = []
    allocated = 0
    for values, count in groups:
        numerator = count * target_rows
        quota, remainder = divmod(numerator, population_rows)
        allocated += quota
        staged.append(
            {
                "values": values,
                "population": count,
                "quota": quota,
                "remainder": remainder,
                "tie": _allocation_sort_key(values),
            }
        )
    staged_by_remainder = sorted(staged, key=lambda item: (-item["remainder"], item["tie"]))
    for item in staged_by_remainder[: target_rows - allocated]:
        item["quota"] += 1
    return [
        (item["values"], item["population"], item["quota"])
        for item in sorted(staged, key=lambda item: item["tie"])
    ]


def bounded_priority_sample(
    connection: duckdb.DuckDBPyConnection,
    *,
    normalized_parquet: str | Path,
    partition_key: bytes,
    max_rows: int,
    stratify_by: Sequence[str] = (),
) -> BoundedTrainingSample:
    """Select an exact bounded sample from only the HMAC-defined training partition."""

    if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows <= 0:
        raise ValueError("max_rows must be a positive integer")
    strata = tuple(stratify_by)
    path = str(Path(normalized_parquet).resolve(strict=True))
    _validate_normalized_schema(connection, path, strata)
    partition = register_hmac_partition_sql(
        connection,
        key=partition_key,
        normalized_parquet=path,
    )
    counts = partition_counts(
        connection,
        normalized_parquet=path,
        partition=partition,
    )
    target_rows = min(max_rows, counts.train_rows)
    if target_rows == 0:
        empty_projection = (
            f"* EXCLUDE ({_quote_identifier(ROW_ID_COLUMN)}, __sts_partition_score, __sts_priority)"
        )
        table = connection.execute(
            f"SELECT {empty_projection} FROM {partition.source_sql} LIMIT 0"
        ).to_arrow_table()
        return BoundedTrainingSample(
            table=table,
            partition=partition,
            requested_max_rows=max_rows,
            train_population_rows=counts.train_rows,
            allocation=(),
        )

    train_predicate = partition.train_predicate()
    row_id = _quote_identifier(ROW_ID_COLUMN)
    excluded = f"{row_id}, __sts_partition_score, __sts_priority, __sts_rank"

    if not strata:
        table = connection.execute(
            f"""
            WITH ranked AS (
                SELECT *, row_number() OVER (
                    ORDER BY __sts_priority, {row_id}
                ) AS __sts_rank
                FROM {partition.source_sql}
                WHERE {train_predicate}
            )
            SELECT * EXCLUDE ({excluded})
            FROM ranked
            WHERE __sts_rank <= ?
            ORDER BY __sts_priority, {row_id}
            """,
            [target_rows],
        ).to_arrow_table()
        allocation: tuple[StratumAllocation, ...] = ()
    else:
        stratum_sql = ", ".join(_quote_identifier(column) for column in strata)
        raw_groups = connection.execute(
            f"""
            SELECT {stratum_sql}, count(*)::UBIGINT AS __sts_population_rows
            FROM {partition.source_sql}
            WHERE {train_predicate}
            GROUP BY {stratum_sql}
            """
        ).fetchall()
        groups = [(tuple(row[:-1]), int(row[-1])) for row in raw_groups]
        apportioned = _hamilton_allocation(groups, target_rows)
        allocation = tuple(
            StratumAllocation(
                values=values,
                population_rows=population,
                sampled_rows=quota,
            )
            for values, population, quota in apportioned
        )

        allocation_table = f"__sts_allocation_{uuid.uuid4().hex}"
        quoted_allocation_table = _quote_identifier(allocation_table)
        connection.execute(
            f"""
            CREATE TEMP TABLE {quoted_allocation_table} AS
            SELECT {stratum_sql},
                   count(*)::UBIGINT AS __sts_population_rows,
                   0::UBIGINT AS __sts_quota
            FROM {partition.source_sql}
            WHERE FALSE
            GROUP BY {stratum_sql}
            """
        )
        placeholders = ", ".join("?" for _ in range(len(strata) + 2))
        try:
            connection.executemany(
                f"INSERT INTO {quoted_allocation_table} VALUES ({placeholders})",
                [(*values, population, quota) for values, population, quota in apportioned],
            )
            partition_by = ", ".join(f"src.{_quote_identifier(column)}" for column in strata)
            joins = " AND ".join(
                f"r.{_quote_identifier(column)} IS NOT DISTINCT FROM a.{_quote_identifier(column)}"
                for column in strata
            )
            table = connection.execute(
                f"""
                WITH ranked AS (
                    SELECT *, row_number() OVER (
                        PARTITION BY {partition_by}
                        ORDER BY __sts_priority, {row_id}
                    ) AS __sts_rank
                    FROM {partition.source_sql} AS src
                    WHERE {train_predicate}
                )
                SELECT r.* EXCLUDE ({excluded})
                FROM ranked AS r
                JOIN {quoted_allocation_table} AS a ON {joins}
                WHERE r.__sts_rank <= a.__sts_quota
                ORDER BY r.__sts_priority, r.{row_id}
                """
            ).to_arrow_table()
        finally:
            connection.execute(f"DROP TABLE {quoted_allocation_table}")

    return BoundedTrainingSample(
        table=table,
        partition=partition,
        requested_max_rows=max_rows,
        train_population_rows=counts.train_rows,
        allocation=allocation,
    )


def _estimated_pandas_column_bytes(column: pa.ChunkedArray) -> int:
    rows = len(column)
    nonnull = rows - column.null_count
    data_type = column.type
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return column.nbytes + rows * 8 + nonnull * 49
    if pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
        return column.nbytes + rows * 8 + nonnull * 33
    if pa.types.is_decimal(data_type):
        return rows * 8 + nonnull * 104 + math.ceil(rows / 8)
    if pa.types.is_dictionary(data_type):
        dictionary_bytes = sum(chunk.dictionary.nbytes for chunk in column.chunks)
        return max(column.nbytes, rows * 4 + dictionary_bytes) + math.ceil(rows / 8)
    if pa.types.is_boolean(data_type):
        return rows * 2
    if pa.types.is_integer(data_type) or pa.types.is_floating(data_type):
        width = max(1, data_type.bit_width // 8)
        return rows * width + math.ceil(rows / 8)
    if (
        pa.types.is_date(data_type)
        or pa.types.is_timestamp(data_type)
        or pa.types.is_time(data_type)
    ):
        return max(column.nbytes, rows * 8) + math.ceil(rows / 8)
    return max(column.nbytes, rows * 8) + nonnull * 64


def estimate_training_memory(
    table: pa.Table,
    *,
    worker_lease_bytes: int,
) -> TrainingMemoryEstimate:
    if isinstance(worker_lease_bytes, bool) or not isinstance(worker_lease_bytes, int):
        raise TypeError("worker_lease_bytes must be an integer")
    if worker_lease_bytes <= 0:
        raise ValueError("worker_lease_bytes must be positive")
    arrow_bytes = int(table.nbytes)
    pandas_bytes = 132 + sum(
        _estimated_pandas_column_bytes(table[column]) for column in table.column_names
    )
    required = math.ceil(max(arrow_bytes, pandas_bytes) * 1.5)
    return TrainingMemoryEstimate(
        arrow_deep_bytes=arrow_bytes,
        pandas_deep_bytes_estimate=pandas_bytes,
        required_lease_bytes=required,
        worker_lease_bytes=worker_lease_bytes,
        admitted=required <= worker_lease_bytes,
    )


def admit_training_sample(
    table: pa.Table,
    *,
    worker_lease_bytes: int,
) -> TrainingMemoryEstimate:
    """Fail before any pandas DataFrame materialization when the lease is too small."""

    estimate = estimate_training_memory(table, worker_lease_bytes=worker_lease_bytes)
    if not estimate.admitted:
        raise DomainError(
            ErrorCode.RESOURCE_LIMIT,
            "bounded training sample exceeds the worker memory lease before pandas materialization",
            context=estimate.model_dump(mode="json"),
        )
    return estimate


def rows_per_candidate(p95_decoded_row_bytes: int | float) -> int:
    if isinstance(p95_decoded_row_bytes, bool) or not isinstance(
        p95_decoded_row_bytes, (int, float)
    ):
        raise TypeError("p95_decoded_row_bytes must be numeric")
    if not math.isfinite(float(p95_decoded_row_bytes)) or p95_decoded_row_bytes <= 0:
        raise ValueError("p95_decoded_row_bytes must be finite and positive")
    calculated = math.floor(CANDIDATE_TARGET_BYTES / p95_decoded_row_bytes)
    return min(MAX_CANDIDATE_ROWS, max(MIN_CANDIDATE_ROWS, calculated))


def assert_checkpoint_compatible(
    actual: CheckpointCompatibility,
    expected: CheckpointCompatibility,
) -> None:
    mismatches = {
        field: {"expected": getattr(expected, field), "actual": getattr(actual, field)}
        for field in (
            "source_manifest_sha256",
            "schema_sha256",
            "rules_sha256",
            "engine_sha256",
        )
        if getattr(actual, field) != getattr(expected, field)
    }
    if mismatches:
        raise DomainError(
            ErrorCode.BACKEND_INCOMPATIBLE,
            "ARGN checkpoint compatibility hashes do not match this job",
            context={"mismatches": mismatches},
        )


def publish_checkpoint_compatibility(
    publisher: AtomicPublisher,
    *,
    relative_path: str,
    compatibility: CheckpointCompatibility,
) -> SnapshotFile:
    published = publisher.publish_json(relative_path, compatibility)
    return SnapshotFile(
        path=published.relative_path,
        sha256=published.sha256,
        size_bytes=published.size_bytes,
    )


def load_checkpoint_compatibility(path: str | Path) -> CheckpointCompatibility:
    return CheckpointCompatibility.model_validate_json(Path(path).read_bytes())


def _verified_snapshot(
    *,
    workspace_root: str | Path,
    files: Mapping[str, SnapshotFile],
) -> ManifestSnapshot:
    snapshot = ManifestSnapshot(
        workspace_root=str(Path(workspace_root).resolve(strict=True)),
        files=dict(files),
    )
    resolve_manifest_snapshot(snapshot)
    return snapshot


def _assert_bounded_sample_has_no_row_id(workspace_root: Path, sample: SnapshotFile) -> None:
    path = confined_existing_path(workspace_root, sample.path)
    try:
        schema = pq.read_schema(path)
    except (pa.ArrowInvalid, OSError) as error:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            "bounded training sample must be a readable Parquet file",
        ) from error
    if ROW_ID_COLUMN in schema.names:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            "bounded training sample must remove __sts_row_id before ARGN fit",
        )


def create_argn_fit_request(
    *,
    workspace_root: str | Path,
    request_id: str,
    job_id: str,
    attempt: int,
    cancellation_path: str,
    bounded_training_sample: SnapshotFile,
    checkpoint_path: str,
    compatibility: CheckpointCompatibility,
    max_epochs: int,
    max_minutes: int,
    model_size: str,
    device: str,
    split_seed: int,
    worker_lease_bytes: int,
) -> WorkerRequestEnvelope:
    root = Path(workspace_root).resolve(strict=True)
    _assert_bounded_sample_has_no_row_id(root, bounded_training_sample)
    validate_workspace_relative_path(checkpoint_path)
    if not 0 <= split_seed <= 2**32 - 1:
        raise ValueError("split_seed must be an unsigned 32-bit integer")
    for name, value in (
        ("max_epochs", max_epochs),
        ("max_minutes", max_minutes),
        ("worker_lease_bytes", worker_lease_bytes),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if not model_size or not device:
        raise ValueError("model_size and device must be nonempty")

    snapshot = _verified_snapshot(
        workspace_root=root,
        files={"bounded_training_sample": bounded_training_sample},
    )
    return WorkerRequestEnvelope(
        request_id=request_id,
        job_id=job_id,
        attempt=attempt,
        worker_kind="argn",
        operation="fit",
        manifest_snapshot=snapshot,
        limits={
            "worker_rss_bytes": worker_lease_bytes,
            "max_process_tree_rss_bytes": worker_lease_bytes,
            "argn": {
                "checkpoint_path": checkpoint_path,
                "max_epochs": max_epochs,
                "max_minutes": max_minutes,
                "model_size": model_size,
                "device": device,
                "deterministic_split": {
                    "callable_fraction": 0.9,
                    "fallback_fraction": 0.9,
                    "seed": split_seed,
                },
                "checkpoint_compatibility": compatibility.model_dump(
                    mode="json", exclude={"version"}
                ),
            },
        },
        cancellation_path=cancellation_path,
    )


def create_argn_generate_request(
    *,
    workspace_root: str | Path,
    request_id: str,
    job_id: str,
    attempt: int,
    cancellation_path: str,
    checkpoint_files: Mapping[str, SnapshotFile],
    checkpoint_path: str,
    candidate_output_path: str,
    candidate: CandidateShardPlan,
    seed_count: int,
    batch_size: int,
    device: str,
    process_count: int,
    actual_compatibility: CheckpointCompatibility,
    expected_compatibility: CheckpointCompatibility,
    worker_lease_bytes: int,
) -> WorkerRequestEnvelope:
    assert_checkpoint_compatible(actual_compatibility, expected_compatibility)
    if not checkpoint_files:
        raise ValueError("checkpoint_files must not be empty")
    forbidden = _FORBIDDEN_GENERATE_SNAPSHOT_KEYS.intersection(checkpoint_files)
    if forbidden:
        raise ValueError(f"generate snapshot contains forbidden inputs: {sorted(forbidden)}")
    validate_workspace_relative_path(checkpoint_path)
    validate_workspace_relative_path(candidate_output_path)
    checkpoint_root = PurePosixPath(checkpoint_path)
    for snapshot_file in checkpoint_files.values():
        try:
            PurePosixPath(snapshot_file.path).relative_to(checkpoint_root)
        except ValueError as error:
            raise ValueError("generate snapshots must be contained by checkpoint_path") from error
    for name, value in (
        ("seed_count", seed_count),
        ("batch_size", batch_size),
        ("process_count", process_count),
        ("worker_lease_bytes", worker_lease_bytes),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if candidate.candidate_index >= seed_count:
        raise ValueError("candidate index must be less than seed_count")
    if not device:
        raise ValueError("device must be nonempty")

    snapshot = _verified_snapshot(workspace_root=workspace_root, files=checkpoint_files)
    return WorkerRequestEnvelope(
        request_id=request_id,
        job_id=job_id,
        attempt=attempt,
        worker_kind="argn",
        operation="generate",
        manifest_snapshot=snapshot,
        limits={
            "worker_rss_bytes": worker_lease_bytes,
            "max_process_tree_rss_bytes": worker_lease_bytes,
            "argn": {
                "checkpoint_path": checkpoint_path,
                "candidate_output_path": candidate_output_path,
                "candidate_rows": candidate.output_rows,
                "batch_size": min(batch_size, candidate.output_rows),
                "engine_seed": candidate.engine_seed,
                "shard_index": candidate.candidate_index,
                "seed_count": seed_count,
                "process_count": process_count,
                "device": device,
                "checkpoint_compatibility": expected_compatibility.model_dump(
                    mode="json", exclude={"version"}
                ),
            },
        },
        cancellation_path=cancellation_path,
    )


def _enabled(feature_gates: Mapping[str, Any], name: str) -> bool:
    gate = feature_gates.get(name)
    return isinstance(gate, Mapping) and gate.get("enabled") is True


def _gate_reason(feature_gates: Mapping[str, Any], name: str) -> str:
    gate = feature_gates.get(name)
    if not isinstance(gate, Mapping):
        return f"{name}: missing"
    reason = gate.get("reason")
    return f"{name}: {reason if isinstance(reason, str) else 'no reason recorded'}"


def load_argn_feature_availability(path: str | Path) -> ArgnFeatureAvailability:
    probe_path = Path(path)
    payload_bytes = probe_path.read_bytes()
    try:
        payload = json.loads(payload_bytes)
    except json.JSONDecodeError as error:
        raise DomainError(ErrorCode.BACKEND_INCOMPATIBLE, "ARGN probe JSON is invalid") from error
    if not isinstance(payload, dict) or payload.get("probe") != "argn_contract":
        raise DomainError(ErrorCode.BACKEND_INCOMPATIBLE, "ARGN probe identity is invalid")
    if payload.get("schema_version") != "1.0":
        raise DomainError(ErrorCode.BACKEND_INCOMPATIBLE, "ARGN probe schema is unsupported")

    feature_gates = payload.get("feature_gates")
    if not isinstance(feature_gates, Mapping):
        raise DomainError(ErrorCode.BACKEND_INCOMPATIBLE, "ARGN probe feature gates are absent")
    required_names = (
        "utility_backend",
        "bounded_generation",
        "fresh_process_checkpoint_generation",
        "deterministic_generation",
    )
    required = {name: _enabled(feature_gates, name) for name in required_names}
    backend_enabled = all(required.values())
    mps_enabled = backend_enabled and _enabled(feature_gates, "mps_parity")
    multiprocess_enabled = backend_enabled and _enabled(feature_gates, "multiprocess_clones")

    environment = payload.get("environment")
    environment = environment if isinstance(environment, Mapping) else {}
    cuda = environment.get("cuda")
    cuda = cuda if isinstance(cuda, Mapping) else {}
    cuda_count_value = cuda.get("cuda_device_count", 0)
    cuda_count = cuda_count_value if isinstance(cuda_count_value, int) else 0
    multiprocess_gate = feature_gates.get("multiprocess_clones")
    multiprocess_gate = multiprocess_gate if isinstance(multiprocess_gate, Mapping) else {}
    levels = multiprocess_gate.get(
        "supported_levels", multiprocess_gate.get("attempted_levels", [])
    )
    supported_levels = (
        [
            level
            for level in levels
            if isinstance(level, int) and not isinstance(level, bool) and level > 1
        ]
        if isinstance(levels, list)
        else []
    )
    max_processes = (
        max(supported_levels, default=1)
        if backend_enabled and multiprocess_enabled
        else (1 if backend_enabled else 0)
    )

    package_identity = payload.get("package_identity")
    package_identity = package_identity if isinstance(package_identity, Mapping) else {}
    locked_wheels = package_identity.get("locked_wheels")
    if not isinstance(locked_wheels, list) or not locked_wheels:
        raise DomainError(ErrorCode.BACKEND_INCOMPATIBLE, "ARGN wheel identity is absent")
    wheel_hash = locked_wheels[0].get("hash") if isinstance(locked_wheels[0], Mapping) else None
    if not isinstance(wheel_hash, str) or not wheel_hash.startswith("sha256:"):
        raise DomainError(ErrorCode.BACKEND_INCOMPATIBLE, "ARGN wheel hash is invalid")
    engine_sha256 = wheel_hash.removeprefix("sha256:").lower()
    if len(engine_sha256) != 64 or any(character not in _SHA256_HEX for character in engine_sha256):
        raise DomainError(ErrorCode.BACKEND_INCOMPATIBLE, "ARGN wheel SHA-256 is invalid")

    reasons = tuple(
        _gate_reason(feature_gates, name)
        for name in (*required_names, "mps_parity", "multiprocess_clones")
    )
    machine = environment.get("machine")
    return ArgnFeatureAvailability(
        probe_sha256=hashlib.sha256(payload_bytes).hexdigest(),
        engine_sha256=engine_sha256,
        platform_machine=machine if isinstance(machine, str) else "unknown",
        backend_enabled=backend_enabled,
        bounded_generation=required["bounded_generation"],
        fresh_process_checkpoint_generation=required["fresh_process_checkpoint_generation"],
        deterministic_generation=required["deterministic_generation"],
        mps_enabled=mps_enabled,
        multiprocess_clones_enabled=multiprocess_enabled,
        cuda_device_count=max(0, cuda_count),
        max_generation_processes=max_processes,
        reasons=reasons,
    )


def require_argn_feature_configuration(
    availability: ArgnFeatureAvailability,
    *,
    device: str,
    process_count: int,
) -> None:
    if not availability.backend_enabled:
        raise DomainError(
            ErrorCode.BACKEND_INCOMPATIBLE,
            "ARGN utility backend did not pass its required Phase 0 gates",
            context={"reasons": list(availability.reasons)},
        )
    if isinstance(process_count, bool) or not isinstance(process_count, int) or process_count <= 0:
        raise ValueError("process_count must be a positive integer")
    if process_count > availability.max_generation_processes:
        raise DomainError(
            ErrorCode.BACKEND_INCOMPATIBLE,
            "requested ARGN generation process count did not pass the clone gate",
            context={
                "requested_processes": process_count,
                "max_generation_processes": availability.max_generation_processes,
            },
        )
    if device == "mps" and not availability.mps_enabled:
        raise DomainError(ErrorCode.BACKEND_INCOMPATIBLE, "ARGN MPS parity gate is disabled")
    if device.startswith("cuda") and availability.cuda_device_count == 0:
        raise DomainError(ErrorCode.BACKEND_INCOMPATIBLE, "ARGN CUDA is unavailable on this host")
    if device != "cpu" and device != "mps" and not device.startswith("cuda"):
        raise DomainError(ErrorCode.BACKEND_INCOMPATIBLE, "ARGN device is unsupported")
