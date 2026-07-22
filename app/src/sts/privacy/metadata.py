from __future__ import annotations

import re
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal

from pydantic import (
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    ValidationError,
    field_validator,
    model_validator,
)

from sts.domain import CanonicalModel, DomainError, ErrorCode

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
CategoryValue = StrictStr | StrictInt | StrictBool
BinEdge = StrictStr | StrictInt | float | Decimal


def _sha256(value: str) -> str:
    lowered = value.lower()
    if not _SHA256_RE.fullmatch(lowered):
        raise ValueError("must be a 64-character hexadecimal SHA-256 digest")
    return lowered


def _ordered_edge(kind: str, value: BinEdge) -> Decimal | date | datetime:
    if kind in {"integer", "fixed_decimal", "float"}:
        if isinstance(value, bool):
            raise ValueError("boolean values are not numeric bin edges")
        try:
            edge = Decimal(str(value))
        except InvalidOperation as error:
            raise ValueError(f"invalid numeric bin edge: {value!r}") from error
        if not edge.is_finite():
            raise ValueError("numeric bin edges must be finite")
        if kind == "integer" and edge != edge.to_integral_value():
            raise ValueError("integer bin edges must be integral")
        return edge
    if not isinstance(value, str):
        raise ValueError(f"{kind} bin edges must be ISO-8601 strings")
    if kind == "date":
        try:
            return date.fromisoformat(value)
        except ValueError as error:
            raise ValueError(f"invalid date bin edge: {value!r}") from error
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"invalid datetime bin edge: {value!r}") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("datetime bin edges must include an explicit timezone")
    return parsed.astimezone(UTC)


class PublicMetadataProvenance(CanonicalModel):
    provenance: Literal["public"] = "public"
    issuer: str = Field(min_length=1)
    description: str = Field(min_length=1)
    source_sha256: str
    user_attested_public: Literal[True]
    attested_by: str = Field(min_length=1)

    _source_sha = field_validator("source_sha256")(_sha256)


class PublicWithinBinDistribution(CanonicalModel):
    kind: Literal["uniform"]


class PublicCategoricalCodebook(CanonicalModel):
    encoding: Literal["categories"]
    name: str = Field(min_length=1)
    kind: Literal["categorical", "boolean", "integer", "text"]
    categories: tuple[CategoryValue, ...]
    nullable: bool = False
    missing_sentinel: CategoryValue | None = None

    @model_validator(mode="after")
    def validate_codebook(self) -> PublicCategoricalCodebook:
        if not self.categories:
            raise ValueError("public categories must not be empty")
        typed = [(type(value), value) for value in self.categories]
        if len(set(typed)) != len(typed):
            raise ValueError("public categories must not contain duplicates")
        if self.kind == "boolean" and any(type(value) is not bool for value in self.categories):
            raise ValueError("boolean codebooks may contain only strict boolean categories")
        if self.nullable != (self.missing_sentinel is not None):
            raise ValueError("nullable codebooks require a public non-null missing sentinel")
        if (
            self.missing_sentinel is not None
            and (
                type(self.missing_sentinel),
                self.missing_sentinel,
            )
            in typed
        ):
            raise ValueError("the public missing sentinel must not collide with a category")
        return self

    @property
    def state_count(self) -> int:
        return len(self.categories) + int(self.nullable)


class PublicBinnedCodebook(CanonicalModel):
    encoding: Literal["bins"]
    name: str = Field(min_length=1)
    kind: Literal["integer", "fixed_decimal", "float", "date", "datetime"]
    bins: tuple[BinEdge, ...]
    within_bin: PublicWithinBinDistribution
    nullable: bool = False
    missing_sentinel: CategoryValue | None = None
    decimal_places: int | None = Field(default=None, ge=0, le=18)

    @model_validator(mode="after")
    def validate_codebook(self) -> PublicBinnedCodebook:
        if len(self.bins) < 2:
            raise ValueError("public bins require at least two ordered edges")
        ordered = [_ordered_edge(self.kind, edge) for edge in self.bins]
        if any(left >= right for left, right in zip(ordered, ordered[1:], strict=False)):
            raise ValueError("public bin edges must be strictly increasing")
        if self.kind == "fixed_decimal" and self.decimal_places is None:
            raise ValueError("fixed_decimal public bins require decimal_places")
        if self.kind != "fixed_decimal" and self.decimal_places is not None:
            raise ValueError("decimal_places is only valid for fixed_decimal bins")
        if self.nullable != (self.missing_sentinel is not None):
            raise ValueError("nullable codebooks require a public non-null missing sentinel")
        return self

    @property
    def state_count(self) -> int:
        return len(self.bins) - 1 + int(self.nullable)

    @property
    def ordered_bins(self) -> tuple[Decimal | date | datetime, ...]:
        return tuple(_ordered_edge(self.kind, edge) for edge in self.bins)


PublicColumnCodebook = Annotated[
    PublicCategoricalCodebook | PublicBinnedCodebook,
    Field(discriminator="encoding"),
]


class PublicMetadataManifest(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    provenance: PublicMetadataProvenance
    epsilon_preprocess: Literal[0]
    columns: tuple[PublicColumnCodebook, ...]
    public_rules_sha256: str | None = None

    @field_validator("public_rules_sha256")
    @classmethod
    def validate_optional_sha(cls, value: str | None) -> str | None:
        return None if value is None else _sha256(value)

    @model_validator(mode="after")
    def validate_manifest(self) -> PublicMetadataManifest:
        if not self.columns:
            raise ValueError("at least one publicly modeled column is required")
        names = [column.name for column in self.columns]
        if len(names) != len(set(names)):
            raise ValueError("public metadata columns must have unique names")
        return self

    @property
    def domain_sizes(self) -> tuple[int, ...]:
        return tuple(column.state_count for column in self.columns)


def validate_public_metadata(
    value: PublicMetadataManifest | dict[str, object],
) -> PublicMetadataManifest:
    """Validate a fully public, zero-budget metadata manifest or fail closed."""

    if isinstance(value, PublicMetadataManifest):
        return value
    try:
        return PublicMetadataManifest.model_validate(value)
    except (ValidationError, ValueError, TypeError) as error:
        raise DomainError(
            ErrorCode.DP_METADATA_NOT_PUBLIC,
            "formal-DP metadata must be explicitly public, attested, and codebook-complete",
            context={"validation_error": str(error)},
        ) from error


__all__ = [
    "CategoryValue",
    "PublicBinnedCodebook",
    "PublicCategoricalCodebook",
    "PublicColumnCodebook",
    "PublicMetadataManifest",
    "PublicMetadataProvenance",
    "PublicWithinBinDistribution",
    "validate_public_metadata",
]
