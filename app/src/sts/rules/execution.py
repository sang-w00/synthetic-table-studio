from __future__ import annotations

import hashlib
import math
import os
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from sts.domain import ColumnKind, ColumnSchema, DomainError, ErrorCode
from sts.domain.canonical import canonical_json_bytes
from sts.rules.models import (
    AllowedValuesRule,
    CompareRule,
    ConditionalSetRule,
    FixedCombinationRule,
    MaskPrefixRule,
    NotNullRule,
    RangeRule,
    RuleSpecValue,
    SourceAction,
    SumEqualsRule,
)

if TYPE_CHECKING:
    from sts.rules.compiler import CompiledRules

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_EPOCH_DATE = date(1970, 1, 1)
_INTERNAL_INVALID = "__sts_rule_invalid"
_ARROW_BATCH_ROWS = 65_536
_MAX_INFERRED_TUPLES = 1_000_000

ArrowSource = (
    str
    | Path
    | pa.Table
    | pa.RecordBatch
    | pa.RecordBatchReader
    | Iterable[pa.RecordBatch | pa.Table]
    | Callable[[], Iterable[pa.RecordBatch | pa.Table]]
)


@dataclass(frozen=True)
class StructuralCodecs:
    """Bounded structural domains needed on both sides of the model boundary."""

    fixed_tuples: Mapping[str, tuple[tuple[Any, ...], ...]]

    def tuples_for(self, rule: FixedCombinationRule) -> tuple[tuple[Any, ...], ...]:
        try:
            return self.fixed_tuples[rule.id]
        except KeyError as exc:
            raise DomainError(
                ErrorCode.RULE_CONFLICT,
                f"fixed-combination rule {rule.id!r} has no resolved tuple domain",
                context={"rule_id": rule.id},
            ) from exc


@dataclass(frozen=True)
class SourceAuditReport:
    source_rows: int
    retained_rows: int
    per_rule_violations: Mapping[str, int]
    violation_union_count: int
    violation_overlap_count: int
    block_union_count: int
    block_overlap_count: int
    drop_union_count: int
    drop_overlap_count: int

    def public_context(self) -> dict[str, Any]:
        return {
            "source_rows": self.source_rows,
            "retained_rows": self.retained_rows,
            "per_rule_violations": dict(self.per_rule_violations),
            "violation_union_count": self.violation_union_count,
            "violation_overlap_count": self.violation_overlap_count,
            "block_union_count": self.block_union_count,
            "block_overlap_count": self.block_overlap_count,
            "drop_union_count": self.drop_union_count,
            "drop_overlap_count": self.drop_overlap_count,
        }


@dataclass(frozen=True)
class SourceAuditResult:
    report: SourceAuditReport
    codecs: StructuralCodecs
    output_path: Path | None = None


@dataclass(frozen=True)
class ValidationReport:
    rows: int
    per_rule_violations: Mapping[str, int]
    schema_violations: Mapping[str, int]
    violation_union_count: int
    violation_overlap_count: int
    invalid_rows: tuple[bool, ...]

    @property
    def valid_rows(self) -> int:
        return self.rows - self.violation_union_count


@dataclass(frozen=True)
class FullValidationResult:
    table: pa.Table
    report: ValidationReport


def _kind(rule: RuleSpecValue) -> str:
    return str(rule.kind)


def _schema_map(compiled: CompiledRules) -> Mapping[str, ColumnSchema]:
    return compiled.schema_by_name


def _as_batches(value: pa.RecordBatch | pa.Table) -> Iterator[pa.RecordBatch]:
    if isinstance(value, pa.RecordBatch):
        yield value
    elif isinstance(value, pa.Table):
        yield from value.to_batches(max_chunksize=_ARROW_BATCH_ROWS)
    else:  # pragma: no cover - guarded by public source validation
        raise TypeError(f"expected an Arrow table or record batch, got {type(value)!r}")


def iter_arrow_batches(
    source: ArrowSource, *, batch_rows: int = _ARROW_BATCH_ROWS
) -> Iterator[pa.RecordBatch]:
    """Read a normalized source in bounded Arrow batches.

    Parquet paths are scanned through DuckDB. Arrow readers and iterable worker streams
    remain one-pass and are never combined into a whole-source Table.
    """

    if batch_rows <= 0:
        raise ValueError("batch_rows must be positive")
    if isinstance(source, (str, Path)):
        path = Path(source).resolve(strict=True)
        connection = duckdb.connect(database=":memory:")
        quoted = str(path).replace("'", "''")
        try:
            reader = connection.execute(f"SELECT * FROM read_parquet('{quoted}')").to_arrow_reader(
                batch_size=batch_rows
            )
            yield from reader
        finally:
            connection.close()
        return
    if isinstance(source, (pa.RecordBatch, pa.Table)):
        if isinstance(source, pa.RecordBatch):
            yield source
        else:
            yield from source.to_batches(max_chunksize=batch_rows)
        return
    if isinstance(source, pa.RecordBatchReader):
        yield from source
        return
    stream = source() if callable(source) else source
    for value in stream:
        yield from _as_batches(value)


def _is_null(value: Any) -> bool:
    return value is None


def _equal(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return left == right


def _predicate_matches(predicate: Any, row: Mapping[str, Any]) -> bool:
    value = row.get(predicate.column)
    if predicate.kind == "is_null":
        return value is None
    if value is None:
        return False
    expected = predicate.value
    try:
        return {
            "eq": lambda: value == expected,
            "ne": lambda: value != expected,
            "lt": lambda: value < expected,
            "lte": lambda: value <= expected,
            "gt": lambda: value > expected,
            "gte": lambda: value >= expected,
        }[predicate.op]()
    except (TypeError, ValueError):
        return False


def _nullable(schema: Mapping[str, ColumnSchema], column: str) -> bool:
    return schema[column].nullable


def _decimal(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise InvalidOperation
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise InvalidOperation
        result = Decimal(str(value))
    else:
        result = Decimal(value)
    if not result.is_finite():
        raise InvalidOperation
    return result


def _scaled_int(value: Any, decimal_places: int) -> int:
    quantum = Decimal(1).scaleb(-decimal_places)
    normalized = _decimal(value).quantize(quantum, rounding=ROUND_HALF_EVEN)
    scaled = int(normalized.scaleb(decimal_places))
    if scaled < _INT64_MIN or scaled > _INT64_MAX:
        raise OverflowError("scaled integer is outside int64")
    return scaled


def _sum_expected(
    row: Mapping[str, Any], rule: SumEqualsRule, schema: Mapping[str, ColumnSchema]
) -> Any:
    target_schema = schema[rule.target]
    values = [row.get(column) for column in rule.sources]
    if any(value is None for value in values):
        if target_schema.nullable:
            return None
        raise ValueError("a non-nullable sum target has a null source")
    scale = target_schema.decimal_places or 0
    total = 0
    for value in values:
        component = _scaled_int(value, scale)
        if (component > 0 and total > _INT64_MAX - component) or (
            component < 0 and total < _INT64_MIN - component
        ):
            raise OverflowError("sum target overflows int64")
        total += component
    if target_schema.kind is ColumnKind.INTEGER:
        return total
    return Decimal(total).scaleb(-scale)


def _compare_holds(left: Any, op: str, right: Any) -> bool:
    if left is None or right is None:
        return False
    try:
        return {"<": left < right, "<=": left <= right, ">": left > right, ">=": left >= right}[op]
    except (TypeError, ValueError):
        return False


def _rule_violation(
    rule: RuleSpecValue,
    row: Mapping[str, Any],
    schema: Mapping[str, ColumnSchema],
    *,
    source: bool,
    codecs: StructuralCodecs | None = None,
) -> bool:
    if isinstance(rule, MaskPrefixRule):
        return False
    if isinstance(rule, NotNullRule):
        return row.get(rule.column) is None
    if isinstance(rule, AllowedValuesRule):
        value = row.get(rule.column)
        if value is None:
            return not _nullable(schema, rule.column)
        return not any(value == allowed for allowed in rule.values)
    if isinstance(rule, RangeRule):
        value = row.get(rule.column)
        if value is None:
            return not _nullable(schema, rule.column)
        if isinstance(value, float) and not math.isfinite(value):
            return True
        try:
            lower = value >= rule.min if rule.inclusive_min else value > rule.min
            upper = value <= rule.max if rule.inclusive_max else value < rule.max
            return not (lower and upper)
        except (TypeError, ValueError):
            return True
    if isinstance(rule, FixedCombinationRule):
        if source and rule.allowed_tuples is None:
            return False
        allowed = (
            rule.allowed_tuples if rule.allowed_tuples is not None else codecs.tuples_for(rule)
        )  # type: ignore[union-attr]
        current = tuple(row.get(column) for column in rule.columns)
        return not any(current == candidate for candidate in allowed)
    if isinstance(rule, ConditionalSetRule):
        return _predicate_matches(rule.when, row) and not _equal(row.get(rule.target), rule.value)
    if isinstance(rule, SumEqualsRule):
        try:
            expected = _sum_expected(row, rule, schema)
        except (InvalidOperation, OverflowError, TypeError, ValueError):
            return True
        actual = row.get(rule.target)
        if expected is None:
            return actual is not None
        if actual is None:
            return True
        try:
            difference = abs(_decimal(actual) - _decimal(expected))
        except (InvalidOperation, TypeError, ValueError):
            return True
        return difference > (rule.tolerance if source else Decimal(0))
    if isinstance(rule, CompareRule):
        return not _compare_holds(row.get(rule.left), rule.op, row.get(rule.right))
    raise TypeError(f"unsupported rule type: {type(rule)!r}")


def _source_action(rule: RuleSpecValue) -> str:
    return str(rule.source_action)


def _build_codecs(
    compiled: CompiledRules,
    inferred: Mapping[str, set[tuple[Any, ...]]] | None = None,
) -> StructuralCodecs:
    resolved: dict[str, tuple[tuple[Any, ...], ...]] = {}
    inferred = inferred or {}
    for rule in compiled.rules:
        if not isinstance(rule, FixedCombinationRule):
            continue
        if rule.allowed_tuples is not None:
            resolved[rule.id] = tuple(tuple(row) for row in rule.allowed_tuples)
            continue
        values = inferred.get(rule.id, set())
        resolved[rule.id] = tuple(sorted(values, key=canonical_json_bytes))
    return StructuralCodecs(fixed_tuples=resolved)


def _audit_batches(
    batches: Iterable[pa.RecordBatch],
    compiled: CompiledRules,
    *,
    spool_path: Path | None = None,
    max_inferred_tuples: int = _MAX_INFERRED_TUPLES,
) -> SourceAuditResult:
    rules = compiled.source_phase
    schema = _schema_map(compiled)
    counts: Counter[str] = Counter({rule.id: 0 for rule in rules})
    source_rows = union = overlap = 0
    block_union = block_overlap = drop_union = drop_overlap = 0
    inferred: dict[str, set[tuple[Any, ...]]] = {
        rule.id: set()
        for rule in rules
        if isinstance(rule, FixedCombinationRule) and rule.allowed_tuples is None
    }
    writer: pq.ParquetWriter | None = None
    try:
        for batch in batches:
            if spool_path is not None:
                if writer is None:
                    writer = pq.ParquetWriter(spool_path, batch.schema, compression="zstd")
                writer.write_batch(batch)
            for row in batch.to_pylist():
                source_rows += 1
                violated: list[RuleSpecValue] = []
                for rule in rules:
                    if _rule_violation(rule, row, schema, source=True):
                        counts[rule.id] += 1
                        violated.append(rule)
                if violated:
                    union += 1
                if len(violated) > 1:
                    overlap += 1
                blocked = [rule for rule in violated if _source_action(rule) == SourceAction.BLOCK]
                dropped = [
                    rule for rule in violated if _source_action(rule) == SourceAction.DROP_ROW
                ]
                if blocked:
                    block_union += 1
                if len(blocked) > 1:
                    block_overlap += 1
                if dropped:
                    drop_union += 1
                if len(dropped) > 1:
                    drop_overlap += 1
                if not dropped:
                    for rule_id, tuples in inferred.items():
                        fixed = next(rule for rule in rules if rule.id == rule_id)
                        assert isinstance(fixed, FixedCombinationRule)
                        tuples.add(tuple(row.get(column) for column in fixed.columns))
                        if len(tuples) > max_inferred_tuples:
                            raise DomainError(
                                ErrorCode.RESOURCE_LIMIT,
                                f"fixed-combination rule {rule_id!r} exceeds the "
                                "runtime tuple-domain limit",
                                context={"rule_id": rule_id, "limit": max_inferred_tuples},
                            )
    finally:
        if writer is not None:
            writer.close()
    report = SourceAuditReport(
        source_rows=source_rows,
        retained_rows=source_rows - drop_union,
        per_rule_violations=dict(counts),
        violation_union_count=union,
        violation_overlap_count=overlap,
        block_union_count=block_union,
        block_overlap_count=block_overlap,
        drop_union_count=drop_union,
        drop_overlap_count=drop_overlap,
    )
    return SourceAuditResult(report=report, codecs=_build_codecs(compiled, inferred))


def audit_source(
    source: ArrowSource,
    compiled: CompiledRules,
    *,
    batch_rows: int = _ARROW_BATCH_ROWS,
    max_inferred_tuples: int = _MAX_INFERRED_TUPLES,
) -> SourceAuditResult:
    """Audit every source row without applying any destructive transform."""

    return _audit_batches(
        iter_arrow_batches(source, batch_rows=batch_rows),
        compiled,
        max_inferred_tuples=max_inferred_tuples,
    )


def _replayable(source: ArrowSource) -> bool:
    return isinstance(source, (str, Path, pa.Table, pa.RecordBatch)) or callable(source)


def _drop_mask(row: Mapping[str, Any], compiled: CompiledRules) -> bool:
    schema = _schema_map(compiled)
    return any(
        _source_action(rule) == SourceAction.DROP_ROW
        and _rule_violation(rule, row, schema, source=True)
        for rule in compiled.source_phase
    )


def audit_and_filter_source(
    source: ArrowSource,
    output_path: str | Path,
    compiled: CompiledRules,
    *,
    batch_rows: int = _ARROW_BATCH_ROWS,
    max_inferred_tuples: int = _MAX_INFERRED_TUPLES,
) -> SourceAuditResult:
    """Audit first, then atomically write the source with the drop-row union removed."""

    destination = Path(output_path).resolve(strict=False)
    if destination.exists() or destination.is_symlink():
        raise DomainError(
            ErrorCode.IMMUTABLE_PATH_EXISTS,
            f"rule-filtered output already exists: {destination.name}",
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(f".{destination.name}.{uuid4().hex}.part")
    spool = destination.with_name(f".{destination.name}.{uuid4().hex}.source.part")
    needs_spool = not _replayable(source)
    try:
        audited = _audit_batches(
            iter_arrow_batches(source, batch_rows=batch_rows),
            compiled,
            spool_path=spool if needs_spool else None,
            max_inferred_tuples=max_inferred_tuples,
        )
        if audited.report.block_union_count:
            raise DomainError(
                ErrorCode.SOURCE_RULE_VIOLATION,
                "normalized source violates one or more blocking rules",
                context=audited.report.public_context(),
            )
        replay: ArrowSource = spool if needs_spool else source
        writer: pq.ParquetWriter | None = None
        source_schema: pa.Schema | None = None
        try:
            for batch in iter_arrow_batches(replay, batch_rows=batch_rows):
                source_schema = source_schema or batch.schema
                keep = pa.array(
                    [not _drop_mask(row, compiled) for row in batch.to_pylist()],
                    type=pa.bool_(),
                )
                filtered = batch.filter(keep)
                if writer is None:
                    writer = pq.ParquetWriter(part, source_schema, compression="zstd")
                if filtered.num_rows:
                    writer.write_batch(filtered)
            if writer is None:
                raise DomainError(
                    ErrorCode.SCHEMA_INVALID, "cannot filter a source with no Arrow schema"
                )
        finally:
            if writer is not None:
                writer.close()
        with part.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(part, destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return SourceAuditResult(
            report=audited.report,
            codecs=audited.codecs,
            output_path=destination,
        )
    except Exception:
        part.unlink(missing_ok=True)
        raise
    finally:
        spool.unlink(missing_ok=True)


def _latent_name(rule_id: str, purpose: str) -> str:
    digest = hashlib.sha256(rule_id.encode("utf-8")).hexdigest()[:16]
    return f"__sts_rule_{digest}_{purpose}"


def mask_prefix(value: str | None, keep_chars: int) -> str | None:
    """Mask Unicode code points, preserving nulls and short values exactly."""

    if value is None or len(value) <= keep_chars:
        return value
    return value[:keep_chars] + "*" * (len(value) - keep_chars)


def _to_units(value: Any, column: ColumnSchema) -> int:
    if column.kind is ColumnKind.INTEGER:
        if isinstance(value, bool):
            raise ValueError("boolean is not an integer value")
        units = int(value)
        if units != value:
            raise ValueError("integer value has a fractional component")
    elif column.kind is ColumnKind.FIXED_DECIMAL:
        units = _scaled_int(value, column.decimal_places or 0)
    elif column.kind is ColumnKind.DATE:
        if isinstance(value, datetime) or not isinstance(value, date):
            raise ValueError("expected a date")
        units = (value - _EPOCH_DATE).days
    elif column.kind is ColumnKind.DATETIME:
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("expected an aware datetime")
        utc_value = value.astimezone(UTC)
        delta = utc_value - datetime(1970, 1, 1, tzinfo=UTC)
        units = delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
    else:
        raise ValueError(f"compare units are unsupported for {column.kind}")
    if units < _INT64_MIN or units > _INT64_MAX:
        raise OverflowError("compare value is outside int64 units")
    return units


def _from_units(units: int, column: ColumnSchema) -> Any:
    if units < _INT64_MIN or units > _INT64_MAX:
        raise OverflowError("reconstructed compare value is outside int64 units")
    if column.kind is ColumnKind.INTEGER:
        return units
    if column.kind is ColumnKind.FIXED_DECIMAL:
        return Decimal(units).scaleb(-(column.decimal_places or 0))
    if column.kind is ColumnKind.DATE:
        return _EPOCH_DATE + timedelta(days=units)
    if column.kind is ColumnKind.DATETIME:
        value = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=units)
        return value.astimezone(ZoneInfo(column.timezone or "UTC"))
    raise ValueError(f"compare units are unsupported for {column.kind}")


def _compare_unit(rule: CompareRule, column: ColumnSchema) -> int | float:
    if rule.granularity is not None:
        if column.kind is ColumnKind.FIXED_DECIMAL:
            unit: int | float = _scaled_int(rule.granularity, column.decimal_places or 0)
        elif column.kind is ColumnKind.FLOAT:
            unit = float(rule.granularity)
        else:
            unit = int(rule.granularity)
        if unit <= 0 or (isinstance(unit, float) and not math.isfinite(unit)):
            raise ValueError("compare granularity is below the represented precision")
        return unit
    if column.kind in {
        ColumnKind.INTEGER,
        ColumnKind.FIXED_DECIMAL,
        ColumnKind.DATE,
        ColumnKind.DATETIME,
    }:
        return 1
    raise ValueError(f"compare unit is unsupported for {column.kind}")


def _encode_compare(
    row: Mapping[str, Any], rule: CompareRule, schema: Mapping[str, ColumnSchema]
) -> int | float | None:
    left = row.get(rule.left)
    right = row.get(rule.right)
    if left is None or right is None:
        return None
    if schema[rule.left].kind is ColumnKind.FLOAT:
        left_value = float(left)
        right_value = float(right)
        strict_unit = (
            float(_compare_unit(rule, schema[rule.right])) if rule.op in {"<", ">"} else 0.0
        )
        delta = (
            right_value - left_value - strict_unit
            if rule.op in {"<", "<="}
            else left_value - right_value - strict_unit
        )
        return delta if math.isfinite(delta) and delta >= 0 else None
    left_units = _to_units(left, schema[rule.left])
    right_units = _to_units(right, schema[rule.right])
    strict_unit = _compare_unit(rule, schema[rule.right]) if rule.op in {"<", ">"} else 0
    delta = (
        right_units - left_units - strict_unit
        if rule.op in {"<", "<="}
        else left_units - right_units - strict_unit
    )
    if delta < 0 or delta > _INT64_MAX:
        return None
    return delta


def _table_rows(table: pa.Table | pa.RecordBatch) -> list[dict[str, Any]]:
    return table.to_pylist()


def prepare_model_batch(
    batch: pa.Table | pa.RecordBatch,
    compiled: CompiledRules,
    *,
    codecs: StructuralCodecs | None = None,
) -> pa.Table:
    """Apply masking and structural encoding to one bounded source batch."""

    codecs = codecs or _build_codecs(compiled)
    schema = _schema_map(compiled)
    rows = _table_rows(batch)
    for rule in compiled.model_phase:
        if isinstance(rule, MaskPrefixRule):
            for row in rows:
                row[rule.column] = mask_prefix(row.get(rule.column), rule.keep_chars)
        elif isinstance(rule, FixedCombinationRule):
            tuples = codecs.tuples_for(rule)
            encoded = {canonical_json_bytes(value): index for index, value in enumerate(tuples)}
            latent = _latent_name(rule.id, "tuple")
            for row in rows:
                key = canonical_json_bytes(tuple(row.get(column) for column in rule.columns))
                row[latent] = encoded.get(key)
                for column in rule.columns:
                    row.pop(column, None)
        elif isinstance(rule, SumEqualsRule):
            for row in rows:
                row.pop(rule.target, None)
        elif isinstance(rule, CompareRule):
            latent = _latent_name(rule.id, "delta")
            for row in rows:
                row[latent] = _encode_compare(row, rule, schema)
                row.pop(rule.right, None)
    return pa.Table.from_pylist(rows)


def _mark_invalid(row: dict[str, Any]) -> None:
    row[_INTERNAL_INVALID] = True


def reconstruct_batch(
    batch: pa.Table | pa.RecordBatch,
    compiled: CompiledRules,
    *,
    codecs: StructuralCodecs | None = None,
) -> pa.Table:
    """Decode and reconstruct writers in compiler-proven topological order."""

    codecs = codecs or _build_codecs(compiled)
    schema = _schema_map(compiled)
    rows = _table_rows(batch)
    for rule in compiled.reconstruction_phase:
        if isinstance(rule, FixedCombinationRule):
            tuples = codecs.tuples_for(rule)
            latent = _latent_name(rule.id, "tuple")
            for row in rows:
                code = row.pop(latent, None)
                try:
                    if isinstance(code, bool):
                        raise ValueError
                    index = int(code)
                    if index != code or index < 0 or index >= len(tuples):
                        raise ValueError
                    values = tuples[index]
                except (TypeError, ValueError):
                    _mark_invalid(row)
                    values = (None,) * len(rule.columns)
                row.update(zip(rule.columns, values, strict=True))
        elif isinstance(rule, ConditionalSetRule):
            for row in rows:
                if _predicate_matches(rule.when, row):
                    row[rule.target] = rule.value
        elif isinstance(rule, SumEqualsRule):
            for row in rows:
                try:
                    row[rule.target] = _sum_expected(row, rule, schema)
                except OverflowError as exc:
                    raise DomainError(
                        ErrorCode.OUTPUT_INVALID,
                        f"sum rule {rule.id!r} overflowed signed int64 during reconstruction",
                        context={"rule_id": rule.id, "reason": "int64_overflow"},
                    ) from exc
                except (InvalidOperation, TypeError, ValueError):
                    _mark_invalid(row)
                    row[rule.target] = None
        elif isinstance(rule, CompareRule):
            latent = _latent_name(rule.id, "delta")
            for row in rows:
                delta = row.pop(latent, None)
                try:
                    if isinstance(delta, bool):
                        raise ValueError
                    if schema[rule.left].kind is ColumnKind.FLOAT:
                        delta_value = float(delta)
                        left_value = float(row.get(rule.left))
                        if not math.isfinite(delta_value) or delta_value < 0:
                            raise ValueError
                        strict_unit = (
                            float(_compare_unit(rule, schema[rule.right]))
                            if rule.op in {"<", ">"}
                            else 0.0
                        )
                        right_value = (
                            left_value + strict_unit + delta_value
                            if rule.op in {"<", "<="}
                            else left_value - strict_unit - delta_value
                        )
                        if not math.isfinite(right_value):
                            raise ValueError
                        row[rule.right] = right_value
                    else:
                        delta_units = int(delta)
                        if delta_units != delta or delta_units < 0:
                            raise ValueError
                        left_units = _to_units(row.get(rule.left), schema[rule.left])
                        strict_unit = (
                            _compare_unit(rule, schema[rule.right]) if rule.op in {"<", ">"} else 0
                        )
                        right_units = (
                            left_units + strict_unit + delta_units
                            if rule.op in {"<", "<="}
                            else left_units - strict_unit - delta_units
                        )
                        row[rule.right] = _from_units(right_units, schema[rule.right])
                except (InvalidOperation, OverflowError, TypeError, ValueError):
                    _mark_invalid(row)
                    row[rule.right] = None
    return pa.Table.from_pylist(rows)


def _normalize_value(value: Any, column: ColumnSchema) -> Any:
    if value is None:
        return None
    if column.kind is ColumnKind.INTEGER:
        if isinstance(value, bool):
            raise ValueError
        normalized = int(value)
        if normalized != value or normalized < _INT64_MIN or normalized > _INT64_MAX:
            raise ValueError
        return normalized
    if column.kind is ColumnKind.FIXED_DECIMAL:
        scale = column.decimal_places or 0
        quantum = Decimal(1).scaleb(-scale)
        normalized = _decimal(value).quantize(quantum, rounding=ROUND_HALF_EVEN)
        unscaled = int(normalized.scaleb(scale))
        if len(str(abs(unscaled))) > 38:
            raise ValueError
        return normalized
    if column.kind is ColumnKind.FLOAT:
        normalized = float(value)
        if not math.isfinite(normalized):
            raise ValueError
        return normalized
    if column.kind is ColumnKind.BOOLEAN:
        if not isinstance(value, bool):
            raise ValueError
        return value
    if column.kind is ColumnKind.DATE:
        if isinstance(value, datetime) or not isinstance(value, date):
            raise ValueError
        return value
    if column.kind is ColumnKind.DATETIME:
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError
        return value.astimezone(ZoneInfo(column.timezone or "UTC"))
    if column.kind in {
        ColumnKind.CATEGORICAL,
        ColumnKind.TEXT,
        ColumnKind.IDENTIFIER,
        ColumnKind.EXCLUDED,
    }:
        if not isinstance(value, str):
            raise ValueError
        return value
    raise ValueError(f"unsupported column kind {column.kind}")


def _arrow_type(column: ColumnSchema) -> pa.DataType:
    if column.kind is ColumnKind.INTEGER:
        return pa.int64()
    if column.kind is ColumnKind.FIXED_DECIMAL:
        return pa.decimal128(38, column.decimal_places or 0)
    if column.kind is ColumnKind.FLOAT:
        return pa.float64()
    if column.kind is ColumnKind.BOOLEAN:
        return pa.bool_()
    if column.kind is ColumnKind.DATE:
        return pa.date32()
    if column.kind is ColumnKind.DATETIME:
        return pa.timestamp("us", tz=column.timezone or "UTC")
    return pa.string()


def normalize_dtypes(
    batch: pa.Table | pa.RecordBatch,
    compiled: CompiledRules,
) -> pa.Table:
    """Half-even normalize precision and cast a reconstructed bounded batch."""

    rows = _table_rows(batch)
    arrays: list[pa.Array] = []
    fields: list[pa.Field] = []
    invalid = [bool(row.get(_INTERNAL_INVALID, False)) for row in rows]
    for column in compiled.columns:
        values: list[Any] = []
        for index, row in enumerate(rows):
            if column.name not in row:
                invalid[index] = True
                values.append(None)
                continue
            value = row[column.name]
            if value is None and not column.nullable:
                invalid[index] = True
            try:
                values.append(_normalize_value(value, column))
            except (InvalidOperation, OverflowError, TypeError, ValueError):
                invalid[index] = True
                values.append(None)
        arrow_type = _arrow_type(column)
        arrays.append(pa.array(values, type=arrow_type))
        fields.append(pa.field(column.name, arrow_type, nullable=column.nullable))
    arrays.append(pa.array(invalid, type=pa.bool_()))
    fields.append(pa.field(_INTERNAL_INVALID, pa.bool_(), nullable=False))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def full_validate(
    batch: pa.Table | pa.RecordBatch,
    compiled: CompiledRules,
    *,
    codecs: StructuralCodecs | None = None,
    raise_on_error: bool = False,
) -> FullValidationResult:
    """Normalize dtypes and evaluate every hard rule over a bounded output batch."""

    normalized = normalize_dtypes(batch, compiled)
    codecs = codecs or _build_codecs(compiled)
    rows = normalized.to_pylist()
    counts: Counter[str] = Counter({rule.id: 0 for rule in compiled.rules})
    schema_counts: Counter[str] = Counter({column.name: 0 for column in compiled.columns})
    invalid_rows: list[bool] = []
    overlap = 0
    for row in rows:
        violations = 0
        schema_invalid = bool(row.get(_INTERNAL_INVALID, False))
        for column in compiled.columns:
            if row.get(column.name) is None and not column.nullable:
                schema_counts[column.name] += 1
                schema_invalid = True
        if schema_invalid:
            violations += 1
        for rule in compiled.rules:
            if _rule_violation(rule, row, _schema_map(compiled), source=False, codecs=codecs):
                counts[rule.id] += 1
                violations += 1
        invalid_rows.append(violations > 0)
        if violations > 1:
            overlap += 1
    union = sum(invalid_rows)
    report = ValidationReport(
        rows=len(rows),
        per_rule_violations=dict(counts),
        schema_violations=dict(schema_counts),
        violation_union_count=union,
        violation_overlap_count=overlap,
        invalid_rows=tuple(invalid_rows),
    )
    output = normalized.select([column.name for column in compiled.columns])
    if raise_on_error and union:
        raise DomainError(
            ErrorCode.OUTPUT_INVALID,
            "synthetic output violates hard schema or rule constraints",
            context={
                "rows": report.rows,
                "violation_union_count": report.violation_union_count,
                "violation_overlap_count": report.violation_overlap_count,
                "per_rule_violations": dict(report.per_rule_violations),
                "schema_violations": dict(report.schema_violations),
            },
        )
    return FullValidationResult(table=output, report=report)


def repair_and_validate_candidate(
    batch: pa.Table | pa.RecordBatch,
    compiled: CompiledRules,
    *,
    codecs: StructuralCodecs | None = None,
) -> FullValidationResult:
    reconstructed = reconstruct_batch(batch, compiled, codecs=codecs)
    return full_validate(reconstructed, compiled, codecs=codecs)


def valid_rows(result: FullValidationResult) -> pa.Table:
    if result.report.rows == 0:
        return result.table
    keep = pc.invert(pa.array(result.report.invalid_rows, type=pa.bool_()))
    return result.table.filter(keep)
