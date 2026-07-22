from __future__ import annotations

import heapq
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from enum import StrEnum
from types import MappingProxyType
from typing import Literal

from sts.domain.errors import DomainError, ErrorCode
from sts.domain.models import ColumnKind, ColumnSchema

from .models import (
    AllowedValuesRule,
    CompareRule,
    ComparisonPredicate,
    ConditionalSetRule,
    FixedCombinationRule,
    IsNullPredicate,
    MaskPrefixRule,
    NotNullRule,
    RangeRule,
    RuleProvenance,
    RuleScalar,
    RuleSpec,
    RuleSpecValue,
    SumEqualsRule,
)

type SynthesisMode = Literal["utility", "differential_privacy"]

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_RULE_CLASSES = (
    MaskPrefixRule,
    NotNullRule,
    AllowedValuesRule,
    RangeRule,
    FixedCombinationRule,
    ConditionalSetRule,
    SumEqualsRule,
    CompareRule,
)
_RANGE_KINDS = {
    ColumnKind.INTEGER,
    ColumnKind.FIXED_DECIMAL,
    ColumnKind.FLOAT,
    ColumnKind.DATE,
    ColumnKind.DATETIME,
}
_ORDERED_PREDICATE_KINDS = _RANGE_KINDS
_FIXED_COMBINATION_KINDS = {ColumnKind.CATEGORICAL, ColumnKind.BOOLEAN}
_COMPARE_KINDS = {
    ColumnKind.INTEGER,
    ColumnKind.FIXED_DECIMAL,
    ColumnKind.FLOAT,
    ColumnKind.DATE,
    ColumnKind.DATETIME,
}


class ReportDenominatorKind(StrEnum):
    NON_NULL_ROWS = "nonnull_rows"
    ALL_ROWS = "all_rows"
    CONDITION_TRUE_ROWS = "condition_true_rows"


@dataclass(frozen=True, slots=True)
class ReportDenominator:
    rule_id: str
    kind: ReportDenominatorKind
    numerator: str
    denominator: str


@dataclass(frozen=True, slots=True)
class RuleDependency:
    before_rule_id: str
    after_rule_id: str
    columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CompiledRule:
    spec: RuleSpecValue
    reads: frozenset[str]
    writes: frozenset[str]
    reconstruction_reads: frozenset[str]
    structural_columns: frozenset[str]
    denominator: ReportDenominator
    requires_runtime_overflow_check: bool = False


@dataclass(frozen=True, slots=True)
class CompiledRules:
    columns: tuple[ColumnSchema, ...]
    rules: tuple[RuleSpecValue, ...]
    nodes: tuple[CompiledRule, ...]
    source_phase: tuple[RuleSpecValue, ...]
    model_phase: tuple[RuleSpecValue, ...]
    reconstruction_phase: tuple[RuleSpecValue, ...]
    reconstruction_order: tuple[str, ...]
    dependencies: tuple[RuleDependency, ...]
    report_denominators: tuple[ReportDenominator, ...]
    mode: SynthesisMode
    schema_by_name: Mapping[str, ColumnSchema] = field(init=False, repr=False, compare=False)
    node_by_id: Mapping[str, CompiledRule] = field(init=False, repr=False, compare=False)
    writer_by_column: Mapping[str, str] = field(init=False, repr=False, compare=False)
    structural_owner_by_column: Mapping[str, str] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        schema_by_name = {column.name: column for column in self.columns}
        node_by_id = {node.spec.id: node for node in self.nodes}
        writer_by_column = {column: node.spec.id for node in self.nodes for column in node.writes}
        structural_owner_by_column = {
            column: node.spec.id for node in self.nodes for column in node.structural_columns
        }
        object.__setattr__(self, "schema_by_name", MappingProxyType(schema_by_name))
        object.__setattr__(self, "node_by_id", MappingProxyType(node_by_id))
        object.__setattr__(self, "writer_by_column", MappingProxyType(writer_by_column))
        object.__setattr__(
            self,
            "structural_owner_by_column",
            MappingProxyType(structural_owner_by_column),
        )


def _conflict(detail: str, **context: object) -> DomainError:
    return DomainError(ErrorCode.RULE_CONFLICT, detail, context=dict(context))


def _dp_private(detail: str, *, rule_id: str) -> DomainError:
    return DomainError(
        ErrorCode.DP_METADATA_NOT_PUBLIC,
        detail,
        context={"rule_id": rule_id},
    )


def _column(columns: Mapping[str, ColumnSchema], name: str, *, rule_id: str) -> ColumnSchema:
    try:
        return columns[name]
    except KeyError as exc:
        raise _conflict(
            f"rule {rule_id!r} references unknown column {name!r}",
            rule_id=rule_id,
            column=name,
        ) from exc


def _decimal(value: RuleScalar, *, field_name: str, rule_id: str) -> Decimal:
    if isinstance(value, (bool, date, datetime)):
        raise _conflict(
            f"{field_name} in rule {rule_id!r} must be numeric",
            rule_id=rule_id,
            field=field_name,
        )
    try:
        converted = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise _conflict(
            f"{field_name} in rule {rule_id!r} must be numeric",
            rule_id=rule_id,
            field=field_name,
        ) from exc
    if not converted.is_finite():
        raise _conflict(
            f"{field_name} in rule {rule_id!r} must be finite",
            rule_id=rule_id,
            field=field_name,
        )
    return converted


def _normalize_scalar(
    value: RuleScalar | None,
    column: ColumnSchema,
    *,
    rule_id: str,
    field_name: str,
    allow_none: bool = False,
) -> object:
    if value is None:
        if not allow_none or not column.nullable:
            raise _conflict(
                f"{field_name} in rule {rule_id!r} cannot be null for non-nullable "
                f"column {column.name!r}",
                rule_id=rule_id,
                column=column.name,
                field=field_name,
            )
        return None

    kind = column.kind
    invalid = False
    normalized: object = value
    if kind is ColumnKind.INTEGER:
        invalid = isinstance(value, bool) or not isinstance(value, int)
    elif kind is ColumnKind.FIXED_DECIMAL:
        converted = _decimal(value, field_name=field_name, rule_id=rule_id)
        places = column.decimal_places
        if places is None:
            raise _conflict(
                f"fixed-decimal column {column.name!r} has no decimal_places",
                rule_id=rule_id,
                column=column.name,
            )
        quantum = Decimal(1).scaleb(-places)
        if converted != converted.quantize(quantum, rounding=ROUND_HALF_EVEN):
            raise _conflict(
                f"{field_name} in rule {rule_id!r} exceeds the {places}-place scale of "
                f"column {column.name!r}",
                rule_id=rule_id,
                column=column.name,
                field=field_name,
            )
        normalized = converted
    elif kind is ColumnKind.FLOAT:
        invalid = isinstance(value, bool) or not isinstance(value, (int, float, Decimal))
        if not invalid:
            try:
                normalized = float(value)
            except (OverflowError, ValueError):
                invalid = True
            else:
                invalid = not math.isfinite(normalized)
    elif kind is ColumnKind.BOOLEAN:
        invalid = not isinstance(value, bool)
    elif kind is ColumnKind.DATE:
        if isinstance(value, datetime):
            invalid = True
        elif isinstance(value, date):
            normalized = value
        elif isinstance(value, str):
            try:
                normalized = date.fromisoformat(value)
            except ValueError:
                invalid = True
        else:
            invalid = True
    elif kind is ColumnKind.DATETIME:
        candidate: datetime | None = None
        if isinstance(value, datetime):
            candidate = value
        elif isinstance(value, str):
            try:
                candidate = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                invalid = True
        else:
            invalid = True
        if candidate is not None:
            if candidate.tzinfo is None or candidate.utcoffset() is None:
                invalid = True
            else:
                normalized = candidate.astimezone(UTC)
    elif kind in {ColumnKind.TEXT, ColumnKind.IDENTIFIER}:
        invalid = not isinstance(value, str)
    elif kind is ColumnKind.CATEGORICAL:
        invalid = not isinstance(value, (str, int, bool)) or isinstance(value, (float, Decimal))
    else:
        invalid = True

    if invalid:
        raise _conflict(
            f"{field_name} in rule {rule_id!r} is incompatible with {kind.value} column "
            f"{column.name!r}",
            rule_id=rule_id,
            column=column.name,
            field=field_name,
            column_kind=kind.value,
        )
    return normalized


def _validate_public_category_domain(
    rule: AllowedValuesRule,
    column: ColumnSchema,
    normalized_values: Sequence[object],
) -> None:
    if column.public_categories is None:
        return
    normalized_public = {
        _normalize_scalar(
            item,
            column,
            rule_id=rule.id,
            field_name="public_categories",
        )
        for item in column.public_categories
    }
    outside = [value for value in normalized_values if value not in normalized_public]
    if outside:
        raise _conflict(
            f"allowed-values rule {rule.id!r} contains values outside the column's public "
            "category domain",
            rule_id=rule.id,
            column=column.name,
        )


def _denominator(rule: RuleSpecValue) -> ReportDenominator:
    if isinstance(rule, MaskPrefixRule):
        return ReportDenominator(
            rule.id,
            ReportDenominatorKind.NON_NULL_ROWS,
            "transformed_nonnull",
            "nonnull",
        )
    if isinstance(rule, ConditionalSetRule):
        return ReportDenominator(
            rule.id,
            ReportDenominatorKind.CONDITION_TRUE_ROWS,
            "condition_true_satisfied",
            "condition_true_rows",
        )
    return ReportDenominator(
        rule.id,
        ReportDenominatorKind.ALL_ROWS,
        "satisfied_rows",
        "all_rows",
    )


def _compile_mask(rule: MaskPrefixRule, columns: Mapping[str, ColumnSchema]) -> CompiledRule:
    column = _column(columns, rule.column, rule_id=rule.id)
    if column.kind not in {ColumnKind.TEXT, ColumnKind.CATEGORICAL}:
        raise _conflict(
            f"mask-prefix rule {rule.id!r} requires a text or categorical column",
            rule_id=rule.id,
            column=column.name,
            column_kind=column.kind.value,
        )
    return CompiledRule(
        rule,
        frozenset({rule.column}),
        frozenset({rule.column}),
        frozenset(),
        frozenset(),
        _denominator(rule),
    )


def _compile_not_null(rule: NotNullRule, columns: Mapping[str, ColumnSchema]) -> CompiledRule:
    _column(columns, rule.column, rule_id=rule.id)
    return CompiledRule(
        rule,
        frozenset({rule.column}),
        frozenset(),
        frozenset(),
        frozenset(),
        _denominator(rule),
    )


def _compile_allowed(
    rule: AllowedValuesRule,
    columns: Mapping[str, ColumnSchema],
    *,
    mode: SynthesisMode,
) -> CompiledRule:
    column = _column(columns, rule.column, rule_id=rule.id)
    normalized = [
        _normalize_scalar(value, column, rule_id=rule.id, field_name="values")
        for value in rule.values
    ]
    if len(normalized) != len(set(normalized)):
        raise _conflict(
            f"allowed-values rule {rule.id!r} contains duplicate typed values",
            rule_id=rule.id,
            column=column.name,
        )
    if mode == "differential_privacy":
        _validate_public_category_domain(rule, column, normalized)
    return CompiledRule(
        rule,
        frozenset({rule.column}),
        frozenset(),
        frozenset(),
        frozenset(),
        _denominator(rule),
    )


def _compile_range(rule: RangeRule, columns: Mapping[str, ColumnSchema]) -> CompiledRule:
    column = _column(columns, rule.column, rule_id=rule.id)
    if column.kind not in _RANGE_KINDS:
        raise _conflict(
            f"range rule {rule.id!r} requires an integer, fixed-decimal, float, date, or "
            "datetime column",
            rule_id=rule.id,
            column=column.name,
            column_kind=column.kind.value,
        )
    minimum = _normalize_scalar(rule.min, column, rule_id=rule.id, field_name="min")
    maximum = _normalize_scalar(rule.max, column, rule_id=rule.id, field_name="max")
    if minimum > maximum or (
        minimum == maximum and (not rule.inclusive_min or not rule.inclusive_max)
    ):
        raise _conflict(
            f"range rule {rule.id!r} has an empty domain",
            rule_id=rule.id,
            column=column.name,
        )
    return CompiledRule(
        rule,
        frozenset({rule.column}),
        frozenset(),
        frozenset(),
        frozenset(),
        _denominator(rule),
    )


def _compile_fixed_combination(
    rule: FixedCombinationRule,
    columns: Mapping[str, ColumnSchema],
    *,
    mode: SynthesisMode,
) -> CompiledRule:
    combination_columns = [_column(columns, name, rule_id=rule.id) for name in rule.columns]
    for column in combination_columns:
        if column.kind not in _FIXED_COMBINATION_KINDS:
            raise _conflict(
                f"fixed-combination rule {rule.id!r} requires categorical or boolean columns",
                rule_id=rule.id,
                column=column.name,
                column_kind=column.kind.value,
            )
    if rule.allowed_tuples is None:
        if mode == "differential_privacy":
            raise _dp_private(
                f"fixed-combination rule {rule.id!r} requires public allowed_tuples in "
                "differential-privacy mode",
                rule_id=rule.id,
            )
        if rule.provenance is not RuleProvenance.PRIVATE_INFERRED:
            raise _conflict(
                f"fixed-combination rule {rule.id!r} that omits allowed_tuples must be "
                "marked private_inferred",
                rule_id=rule.id,
            )
    else:
        normalized_rows: set[tuple[object, ...]] = set()
        for row in rule.allowed_tuples:
            normalized = tuple(
                _normalize_scalar(
                    value,
                    column,
                    rule_id=rule.id,
                    field_name="allowed_tuples",
                    allow_none=True,
                )
                for value, column in zip(row, combination_columns, strict=True)
            )
            if normalized in normalized_rows:
                raise _conflict(
                    f"fixed-combination rule {rule.id!r} contains duplicate typed tuples",
                    rule_id=rule.id,
                )
            normalized_rows.add(normalized)
    owned = frozenset(rule.columns)
    return CompiledRule(
        rule,
        owned,
        owned,
        frozenset(),
        owned,
        _denominator(rule),
    )


def _compile_conditional(
    rule: ConditionalSetRule, columns: Mapping[str, ColumnSchema]
) -> CompiledRule:
    target = _column(columns, rule.target, rule_id=rule.id)
    predicate_column = _column(columns, rule.when.column, rule_id=rule.id)
    if isinstance(rule.when, ComparisonPredicate):
        _normalize_scalar(
            rule.when.value,
            predicate_column,
            rule_id=rule.id,
            field_name="when.value",
        )
        if (
            rule.when.op in {"lt", "lte", "gt", "gte"}
            and predicate_column.kind not in _ORDERED_PREDICATE_KINDS
        ):
            raise _conflict(
                f"ordered predicate in rule {rule.id!r} requires an ordered scalar column",
                rule_id=rule.id,
                column=predicate_column.name,
                column_kind=predicate_column.kind.value,
            )
    elif not isinstance(rule.when, IsNullPredicate):
        raise _conflict(f"rule {rule.id!r} has an unsupported condition", rule_id=rule.id)
    _normalize_scalar(
        rule.value,
        target,
        rule_id=rule.id,
        field_name="value",
        allow_none=True,
    )
    return CompiledRule(
        rule,
        frozenset({predicate_column.name, target.name}),
        frozenset({target.name}),
        frozenset({predicate_column.name}),
        frozenset(),
        _denominator(rule),
    )


def _scaled_int(
    value: RuleScalar,
    column: ColumnSchema,
    *,
    scale: int,
    rule_id: str,
    field_name: str,
) -> int:
    normalized = _normalize_scalar(
        value,
        column,
        rule_id=rule_id,
        field_name=field_name,
    )
    converted = Decimal(normalized) if isinstance(normalized, int) else normalized
    if not isinstance(converted, Decimal):
        raise _conflict(
            f"{field_name} in rule {rule_id!r} is not an integer or fixed decimal",
            rule_id=rule_id,
            column=column.name,
        )
    scaled = int((converted * (Decimal(10) ** scale)).to_integral_value(rounding=ROUND_HALF_EVEN))
    if scaled < _INT64_MIN or scaled > _INT64_MAX:
        raise _conflict(
            f"{field_name} for column {column.name!r} exceeds int64 at scale {scale}",
            rule_id=rule_id,
            column=column.name,
        )
    return scaled


def _public_bound_pair(
    column: ColumnSchema,
    *,
    scale: int,
    rule_id: str,
) -> tuple[int, int] | None:
    if column.public_min is None or column.public_max is None:
        return None
    minimum = _scaled_int(
        column.public_min,
        column,
        scale=scale,
        rule_id=rule_id,
        field_name="public_min",
    )
    maximum = _scaled_int(
        column.public_max,
        column,
        scale=scale,
        rule_id=rule_id,
        field_name="public_max",
    )
    if minimum > maximum:
        raise _conflict(
            f"column {column.name!r} has an invalid public bound domain",
            rule_id=rule_id,
            column=column.name,
        )
    return minimum, maximum


def _compile_sum(rule: SumEqualsRule, columns: Mapping[str, ColumnSchema]) -> CompiledRule:
    if rule.target in rule.sources:
        raise _conflict(
            f"sum rule {rule.id!r} target must not overlap its sources",
            rule_id=rule.id,
            column=rule.target,
        )
    sources = [_column(columns, name, rule_id=rule.id) for name in rule.sources]
    target = _column(columns, rule.target, rule_id=rule.id)
    for column in [*sources, target]:
        if column.kind not in {ColumnKind.INTEGER, ColumnKind.FIXED_DECIMAL}:
            raise _conflict(
                f"sum rule {rule.id!r} requires integer or fixed-decimal columns",
                rule_id=rule.id,
                column=column.name,
                column_kind=column.kind.value,
            )
    scale = target.decimal_places or 0
    source_bounds = [_public_bound_pair(column, scale=scale, rule_id=rule.id) for column in sources]
    target_bounds = _public_bound_pair(target, scale=scale, rule_id=rule.id)
    runtime_overflow_check = any(bounds is None for bounds in source_bounds)
    if not runtime_overflow_check:
        minimum = sum(bounds[0] for bounds in source_bounds if bounds is not None)
        maximum = sum(bounds[1] for bounds in source_bounds if bounds is not None)
        if minimum < _INT64_MIN or maximum > _INT64_MAX:
            raise _conflict(
                f"sum rule {rule.id!r} can overflow int64 at the target scale",
                rule_id=rule.id,
                target=rule.target,
                scale=scale,
            )
        if target_bounds is not None and (target_bounds[0] > minimum or target_bounds[1] < maximum):
            raise _conflict(
                f"sum rule {rule.id!r} result range is outside the target public domain",
                rule_id=rule.id,
                target=rule.target,
            )
    reads = frozenset((*rule.sources, rule.target))
    return CompiledRule(
        rule,
        reads,
        frozenset({rule.target}),
        frozenset(rule.sources),
        frozenset(),
        _denominator(rule),
        runtime_overflow_check,
    )


def _compile_compare(rule: CompareRule, columns: Mapping[str, ColumnSchema]) -> CompiledRule:
    if rule.left == rule.right:
        raise _conflict(
            f"compare rule {rule.id!r} left and right columns must differ",
            rule_id=rule.id,
            column=rule.left,
        )
    left = _column(columns, rule.left, rule_id=rule.id)
    right = _column(columns, rule.right, rule_id=rule.id)
    if left.kind not in _COMPARE_KINDS or right.kind not in _COMPARE_KINDS:
        raise _conflict(
            f"compare rule {rule.id!r} requires integer, fixed-decimal, float, date, or "
            "datetime columns",
            rule_id=rule.id,
            left_kind=left.kind.value,
            right_kind=right.kind.value,
        )
    if left.kind is not right.kind:
        raise _conflict(
            f"compare rule {rule.id!r} requires matching column kinds",
            rule_id=rule.id,
            left_kind=left.kind.value,
            right_kind=right.kind.value,
        )
    if left.kind is ColumnKind.FIXED_DECIMAL and left.decimal_places != right.decimal_places:
        raise _conflict(
            f"compare rule {rule.id!r} requires matching fixed-decimal scales",
            rule_id=rule.id,
            left_scale=left.decimal_places,
            right_scale=right.decimal_places,
        )
    if left.kind is ColumnKind.FLOAT:
        if rule.granularity is None:
            raise _conflict(
                f"float compare rule {rule.id!r} requires positive public granularity",
                rule_id=rule.id,
            )
        if rule.provenance is not RuleProvenance.PUBLIC:
            raise _conflict(
                f"float compare rule {rule.id!r} granularity must have public provenance",
                rule_id=rule.id,
            )
    elif rule.granularity is not None:
        raise _conflict(
            f"compare granularity is only valid for float columns in rule {rule.id!r}",
            rule_id=rule.id,
        )
    owned = frozenset({rule.left, rule.right})
    return CompiledRule(
        rule,
        owned,
        frozenset({rule.right}),
        frozenset({rule.left}),
        owned,
        _denominator(rule),
    )


def _compile_node(
    rule: RuleSpecValue,
    columns: Mapping[str, ColumnSchema],
    *,
    mode: SynthesisMode,
) -> CompiledRule:
    if isinstance(rule, MaskPrefixRule):
        return _compile_mask(rule, columns)
    if isinstance(rule, NotNullRule):
        return _compile_not_null(rule, columns)
    if isinstance(rule, AllowedValuesRule):
        return _compile_allowed(rule, columns, mode=mode)
    if isinstance(rule, RangeRule):
        return _compile_range(rule, columns)
    if isinstance(rule, FixedCombinationRule):
        return _compile_fixed_combination(rule, columns, mode=mode)
    if isinstance(rule, ConditionalSetRule):
        return _compile_conditional(rule, columns)
    if isinstance(rule, SumEqualsRule):
        return _compile_sum(rule, columns)
    if isinstance(rule, CompareRule):
        return _compile_compare(rule, columns)
    raise TypeError(f"unsupported rule type: {type(rule).__name__}")


def _validate_global_conflicts(nodes: Sequence[CompiledRule]) -> None:
    writer: dict[str, str] = {}
    for node in nodes:
        for column in sorted(node.writes):
            previous = writer.get(column)
            if previous is not None:
                raise _conflict(
                    f"column {column!r} has multiple writers: {previous!r} and {node.spec.id!r}",
                    column=column,
                    rule_ids=[previous, node.spec.id],
                )
            writer[column] = node.spec.id

    structural_owner: dict[str, str] = {}
    for node in nodes:
        for column in sorted(node.structural_columns):
            previous = structural_owner.get(column)
            if previous is not None:
                raise _conflict(
                    f"structural rules {previous!r} and {node.spec.id!r} overlap on "
                    f"column {column!r}",
                    column=column,
                    rule_ids=[previous, node.spec.id],
                )
            structural_owner[column] = node.spec.id

    for node in nodes:
        if not isinstance(node.spec, ConditionalSetRule):
            continue
        predicate_column = node.spec.when.column
        previous_writer = writer.get(predicate_column)
        if previous_writer is not None and previous_writer != node.spec.id:
            raise _conflict(
                f"condition in rule {node.spec.id!r} reads column {predicate_column!r} "
                f"written by rule {previous_writer!r}; condition order would be ambiguous",
                column=predicate_column,
                rule_ids=[previous_writer, node.spec.id],
            )


def _reconstruction_graph(
    nodes: Sequence[CompiledRule],
) -> tuple[tuple[RuleDependency, ...], tuple[str, ...]]:
    reconstructors = {
        node.spec.id: node
        for node in nodes
        if isinstance(
            node.spec,
            (FixedCombinationRule, ConditionalSetRule, SumEqualsRule, CompareRule),
        )
    }
    outgoing: dict[str, set[str]] = {rule_id: set() for rule_id in reconstructors}
    indegree: dict[str, int] = {rule_id: 0 for rule_id in reconstructors}
    dependencies: list[RuleDependency] = []
    for before_id, before in reconstructors.items():
        for after_id, after in reconstructors.items():
            if before_id == after_id:
                continue
            shared = tuple(sorted(before.writes & after.reconstruction_reads))
            if not shared:
                continue
            outgoing[before_id].add(after_id)
            indegree[after_id] += 1
            dependencies.append(RuleDependency(before_id, after_id, shared))

    ready = [rule_id for rule_id, degree in indegree.items() if degree == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        rule_id = heapq.heappop(ready)
        order.append(rule_id)
        for after_id in sorted(outgoing[rule_id]):
            indegree[after_id] -= 1
            if indegree[after_id] == 0:
                heapq.heappush(ready, after_id)
    if len(order) != len(reconstructors):
        cycle_members = sorted(rule_id for rule_id, degree in indegree.items() if degree > 0)
        raise _conflict(
            "rule reconstruction graph contains a cycle",
            rule_ids=cycle_members,
        )
    return tuple(
        sorted(dependencies, key=lambda item: (item.before_rule_id, item.after_rule_id))
    ), tuple(order)


def _parse_rule(value: RuleSpecValue | RuleSpec | Mapping[str, object]) -> RuleSpecValue:
    if isinstance(value, RuleSpec):
        return value.value
    if isinstance(value, _RULE_CLASSES):
        return value
    return RuleSpec.model_validate(value).value


def compile_rules(
    columns: Sequence[ColumnSchema],
    rules: Sequence[RuleSpecValue | RuleSpec | Mapping[str, object]],
    *,
    mode: SynthesisMode = "utility",
) -> CompiledRules:
    """Validate and compile rules without assigning any meaning to input/UI order."""

    if mode not in {"utility", "differential_privacy"}:
        raise ValueError("mode must be 'utility' or 'differential_privacy'")
    frozen_columns = tuple(columns)
    column_names = [column.name for column in frozen_columns]
    if len(column_names) != len(set(column_names)):
        raise _conflict("schema columns must have unique names")
    columns_by_name = {column.name: column for column in frozen_columns}

    parsed = tuple(_parse_rule(rule) for rule in rules)
    rule_ids = [rule.id for rule in parsed]
    if len(rule_ids) != len(set(rule_ids)):
        duplicates = sorted(rule_id for rule_id in set(rule_ids) if rule_ids.count(rule_id) > 1)
        raise _conflict("rule ids must be unique", rule_ids=duplicates)
    ordered_rules = tuple(sorted(parsed, key=lambda rule: rule.id))

    if mode == "differential_privacy":
        for rule in ordered_rules:
            if rule.provenance is not RuleProvenance.PUBLIC:
                raise _dp_private(
                    f"rule {rule.id!r} uses private-inferred metadata in differential-privacy mode",
                    rule_id=rule.id,
                )

    nodes = tuple(_compile_node(rule, columns_by_name, mode=mode) for rule in ordered_rules)
    _validate_global_conflicts(nodes)
    dependencies, reconstruction_order = _reconstruction_graph(nodes)
    nodes_by_id = {node.spec.id: node for node in nodes}

    source_phase = tuple(node.spec for node in nodes if not isinstance(node.spec, MaskPrefixRule))
    model_rank = {
        MaskPrefixRule: 0,
        NotNullRule: 1,
        AllowedValuesRule: 1,
        RangeRule: 1,
        FixedCombinationRule: 2,
        SumEqualsRule: 2,
        CompareRule: 2,
    }
    model_phase = tuple(
        node.spec
        for node in sorted(
            (node for node in nodes if type(node.spec) in model_rank),
            key=lambda node: (model_rank[type(node.spec)], node.spec.id),
        )
    )
    reconstruction_phase = tuple(nodes_by_id[rule_id].spec for rule_id in reconstruction_order)
    return CompiledRules(
        columns=frozen_columns,
        rules=ordered_rules,
        nodes=nodes,
        source_phase=source_phase,
        model_phase=model_phase,
        reconstruction_phase=reconstruction_phase,
        reconstruction_order=reconstruction_order,
        dependencies=dependencies,
        report_denominators=tuple(node.denominator for node in nodes),
        mode=mode,
    )
