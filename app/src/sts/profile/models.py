from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from sts.domain import CanonicalModel, ColumnKind


class ValueCount(CanonicalModel):
    value: str
    count: int = Field(ge=1)


class ParseSuccess(CanonicalModel):
    integer: int = Field(ge=0)
    float: int = Field(ge=0)
    boolean: int = Field(ge=0)
    date: int = Field(ge=0)
    datetime: int = Field(ge=0)


class ColumnProfile(CanonicalModel):
    name: str
    storage_type: str
    row_count: int = Field(ge=0)
    null_count: int = Field(ge=0)
    nonnull_count: int = Field(ge=0)
    minimum: str | None = None
    maximum: str | None = None
    approx_quantiles: tuple[str | None, str | None, str | None] | None = None
    approx_cardinality: int = Field(ge=0)
    exact_low_cardinality: tuple[ValueCount, ...] | None = None
    fixed_length: int | None = Field(default=None, ge=0)
    parse_success: ParseSuccess
    candidate_type: ColumnKind
    candidate_requires_confirmation: bool = False
    candidate_alternatives: tuple[ColumnKind, ...] = ()


class DatasetProfile(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    view: Literal["raw", "typed"]
    row_count: int = Field(ge=0)
    column_count: int = Field(ge=0, le=70)
    columns: tuple[ColumnProfile, ...]
    metadata: dict[str, Any] = Field(default_factory=dict)
