from __future__ import annotations

import math
import re
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field, RootModel, field_validator, model_validator

from sts.domain.canonical import CanonicalModel, canonical_json_bytes

_RULE_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]*$")


class RuleProvenance(StrEnum):
    PUBLIC = "public"
    PRIVATE_INFERRED = "private_inferred"


class SourceAction(StrEnum):
    BLOCK = "block"
    DROP_ROW = "drop_row"


type RuleScalar = bool | int | float | Decimal | date | datetime | str
type NullableRuleScalar = RuleScalar | None


def _validate_scalar(value: NullableRuleScalar) -> NullableRuleScalar:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("rule values must be finite")
    if isinstance(value, Decimal) and not value.is_finite():
        raise ValueError("rule values must be finite")
    if isinstance(value, datetime) and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError("datetime rule values must be timezone-aware")
    return value


def _unique(values: tuple[NullableRuleScalar, ...]) -> bool:
    encoded = [canonical_json_bytes(value) for value in values]
    return len(encoded) == len(set(encoded))


class ComparisonPredicate(CanonicalModel):
    kind: Literal["comparison"] = "comparison"
    column: str = Field(min_length=1)
    op: Literal["eq", "ne", "lt", "lte", "gt", "gte"]
    value: RuleScalar

    _finite_value = field_validator("value")(_validate_scalar)


class IsNullPredicate(CanonicalModel):
    kind: Literal["is_null"] = "is_null"
    column: str = Field(min_length=1)


type ConditionalPredicate = Annotated[
    ComparisonPredicate | IsNullPredicate,
    Field(discriminator="kind"),
]


class _RuleBase(CanonicalModel):
    id: str = Field(min_length=1, max_length=128)
    provenance: RuleProvenance
    source_action: SourceAction = SourceAction.BLOCK

    @field_validator("id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        if not _RULE_ID_RE.fullmatch(value):
            raise ValueError(
                "rule id must start with a letter and contain only letters, digits, '.', ':', "
                "'_', or '-'"
            )
        return value


class MaskPrefixRule(_RuleBase):
    kind: Literal["mask_prefix"] = "mask_prefix"
    source_action: Literal[SourceAction.BLOCK] = SourceAction.BLOCK
    column: str = Field(min_length=1)
    keep_chars: int = Field(ge=0)


class NotNullRule(_RuleBase):
    kind: Literal["not_null"] = "not_null"
    column: str = Field(min_length=1)


class AllowedValuesRule(_RuleBase):
    kind: Literal["allowed_values"] = "allowed_values"
    column: str = Field(min_length=1)
    values: tuple[RuleScalar, ...] = Field(min_length=1)

    @field_validator("values")
    @classmethod
    def validate_values(cls, values: tuple[RuleScalar, ...]) -> tuple[RuleScalar, ...]:
        normalized = tuple(_validate_scalar(value) for value in values)
        if not _unique(normalized):
            raise ValueError("allowed values must not contain duplicates")
        return normalized


class RangeRule(_RuleBase):
    kind: Literal["range"] = "range"
    column: str = Field(min_length=1)
    min: RuleScalar
    max: RuleScalar
    inclusive_min: bool = True
    inclusive_max: bool = True

    @field_validator("min", "max")
    @classmethod
    def validate_bound(cls, value: RuleScalar) -> RuleScalar:
        return _validate_scalar(value)


class FixedCombinationRule(_RuleBase):
    kind: Literal["fixed_combination"] = "fixed_combination"
    columns: tuple[str, ...] = Field(min_length=2)
    allowed_tuples: tuple[tuple[NullableRuleScalar, ...], ...] | None = None

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, columns: tuple[str, ...]) -> tuple[str, ...]:
        if any(not column for column in columns):
            raise ValueError("fixed-combination columns must not be empty")
        if len(columns) != len(set(columns)):
            raise ValueError("fixed-combination columns must be unique")
        return columns

    @model_validator(mode="after")
    def validate_tuples(self) -> FixedCombinationRule:
        if self.allowed_tuples is None:
            return self
        if not self.allowed_tuples:
            raise ValueError("allowed_tuples must not be empty when supplied")
        encoded_rows: list[bytes] = []
        for row in self.allowed_tuples:
            if len(row) != len(self.columns):
                raise ValueError("every allowed tuple must match the number of columns")
            normalized = tuple(_validate_scalar(value) for value in row)
            encoded_rows.append(canonical_json_bytes(normalized))
        if len(encoded_rows) != len(set(encoded_rows)):
            raise ValueError("allowed_tuples must not contain duplicates")
        return self


class ConditionalSetRule(_RuleBase):
    kind: Literal["conditional_set"] = "conditional_set"
    when: ConditionalPredicate
    target: str = Field(min_length=1)
    value: NullableRuleScalar

    _finite_value = field_validator("value")(_validate_scalar)


class SumEqualsRule(_RuleBase):
    kind: Literal["sum_equals"] = "sum_equals"
    sources: tuple[str, ...] = Field(min_length=1)
    target: str = Field(min_length=1)
    tolerance: Decimal = Field(default=Decimal(0), ge=0)

    @field_validator("sources")
    @classmethod
    def validate_sources(cls, sources: tuple[str, ...]) -> tuple[str, ...]:
        if any(not source for source in sources):
            raise ValueError("sum sources must not be empty")
        if len(sources) != len(set(sources)):
            raise ValueError("sum sources must be unique")
        return sources

    @field_validator("tolerance")
    @classmethod
    def validate_tolerance(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("sum tolerance must be finite")
        return value


class CompareRule(_RuleBase):
    kind: Literal["compare"] = "compare"
    left: str = Field(min_length=1)
    op: Literal["<", "<=", ">", ">="]
    right: str = Field(min_length=1)
    granularity: int | float | Decimal | None = None

    @field_validator("granularity")
    @classmethod
    def validate_granularity(
        cls, value: int | float | Decimal | None
    ) -> int | float | Decimal | None:
        if value is None:
            return None
        _validate_scalar(value)
        if isinstance(value, bool) or value <= 0:
            raise ValueError("compare granularity must be a positive finite number")
        return value


type RuleSpecValue = Annotated[
    MaskPrefixRule
    | NotNullRule
    | AllowedValuesRule
    | RangeRule
    | FixedCombinationRule
    | ConditionalSetRule
    | SumEqualsRule
    | CompareRule,
    Field(discriminator="kind"),
]


class RuleSpec(RootModel[RuleSpecValue]):
    root: RuleSpecValue

    @property
    def value(self) -> RuleSpecValue:
        return self.root
