from __future__ import annotations

import re
from decimal import Decimal
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import Field, RootModel, field_validator, model_validator

from .canonical import CanonicalModel, canonical_json_bytes

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_KIND_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _validate_sha256(value: str) -> str:
    lowered = value.lower()
    if not _SHA256_RE.fullmatch(lowered):
        raise ValueError("must be a 64-character hexadecimal SHA-256 digest")
    return lowered


def _validate_relative_path(value: str) -> str:
    if "\\" in value or "\x00" in value:
        raise ValueError("must be a workspace-relative POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("must be a normalized workspace-relative path without traversal")
    normalized = path.as_posix()
    if normalized != value:
        raise ValueError("must be a normalized workspace-relative POSIX path")
    return value


class ColumnKind(StrEnum):
    INTEGER = "integer"
    FIXED_DECIMAL = "fixed_decimal"
    FLOAT = "float"
    CATEGORICAL = "categorical"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    TEXT = "text"
    IDENTIFIER = "identifier"
    EXCLUDED = "excluded"


class ColumnRole(StrEnum):
    MODEL = "model"
    DERIVED = "derived"
    IDENTIFIER = "identifier"
    EXCLUDED = "excluded"


class IdentifierStrategy(StrEnum):
    SEQUENTIAL = "sequential"
    UUID4 = "uuid4"


class OutputFormat(StrEnum):
    PARQUET = "parquet"
    CSV = "csv"


class ManifestFile(CanonicalModel):
    relative_path: str
    sha256: str
    size_bytes: int = Field(ge=0)

    _path = field_validator("relative_path")(_validate_relative_path)
    _sha = field_validator("sha256")(_validate_sha256)


class ColumnSchema(CanonicalModel):
    name: str = Field(min_length=1)
    kind: ColumnKind
    nullable: bool
    role: ColumnRole
    decimal_places: int | None = Field(default=None, ge=0, le=18)
    timezone: str | None = None
    format: str | None = None
    public_min: int | float | Decimal | str | None = None
    public_max: int | float | Decimal | str | None = None
    public_bins: tuple[int | float | Decimal | str, ...] | None = None
    public_categories: tuple[str | int | bool, ...] | None = None
    identifier_strategy: IdentifierStrategy | None = None

    @model_validator(mode="after")
    def validate_contract(self) -> ColumnSchema:
        if self.kind is ColumnKind.FIXED_DECIMAL and self.decimal_places is None:
            raise ValueError("fixed_decimal columns require decimal_places")
        if self.kind is not ColumnKind.FIXED_DECIMAL and self.decimal_places is not None:
            raise ValueError("decimal_places is only valid for fixed_decimal columns")
        identifier = self.kind is ColumnKind.IDENTIFIER or self.role is ColumnRole.IDENTIFIER
        if identifier and not (
            self.kind is ColumnKind.IDENTIFIER and self.role is ColumnRole.IDENTIFIER
        ):
            raise ValueError("identifier kind and role must be selected together")
        if identifier and self.identifier_strategy is None:
            raise ValueError("identifier columns require identifier_strategy")
        if not identifier and self.identifier_strategy is not None:
            raise ValueError("identifier_strategy is only valid for identifier columns")
        excluded = self.kind is ColumnKind.EXCLUDED or self.role is ColumnRole.EXCLUDED
        if excluded and not (self.kind is ColumnKind.EXCLUDED and self.role is ColumnRole.EXCLUDED):
            raise ValueError("excluded kind and role must be selected together")
        if self.public_categories is not None and len(set(self.public_categories)) != len(
            self.public_categories
        ):
            raise ValueError("public_categories must not contain duplicates")
        if self.public_bins is not None and len(self.public_bins) < 2:
            raise ValueError("public_bins must contain at least two edges")
        return self


class DatasetManifest(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    dataset_id: UUID
    source: ManifestFile
    schema_version: str = Field(min_length=1)
    rules_version: str = Field(min_length=1)
    columns: tuple[ColumnSchema, ...] = ()
    normalized: ManifestFile | None = None
    row_count: int | None = Field(default=None, ge=0)
    rules_sha256: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("schema_version", "rules_version")
    @classmethod
    def validate_version(cls, value: str) -> str:
        if not _VERSION_RE.fullmatch(value):
            raise ValueError(
                "version must contain only letters, digits, dot, underscore, or hyphen"
            )
        return value

    @field_validator("rules_sha256")
    @classmethod
    def validate_optional_sha(cls, value: str | None) -> str | None:
        return None if value is None else _validate_sha256(value)

    @model_validator(mode="after")
    def validate_columns(self) -> DatasetManifest:
        names = [column.name for column in self.columns]
        if len(names) != len(set(names)):
            raise ValueError("dataset columns must have unique names")
        return self


class ArtifactManifest(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    artifact_id: UUID
    kind: str
    relative_path: str
    sha256: str
    size_bytes: int = Field(ge=0)
    downloadable: bool
    release_safe: bool
    contains_private_source_information: bool
    dataset_id: UUID | None = None
    job_id: UUID | None = None
    attempt: int = Field(default=1, ge=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    _path = field_validator("relative_path")(_validate_relative_path)
    _sha = field_validator("sha256")(_validate_sha256)

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: str) -> str:
        if not _KIND_RE.fullmatch(value):
            raise ValueError("artifact kind must be a lowercase identifier")
        return value

    @model_validator(mode="after")
    def validate_safety(self) -> ArtifactManifest:
        if self.release_safe and self.contains_private_source_information:
            raise ValueError("release-safe artifacts cannot contain private source information")
        if self.dataset_id is not None and self.job_id is not None:
            raise ValueError("an artifact cannot belong to both a dataset and a job")
        return self


class UtilityTrainingConfig(CanonicalModel):
    max_rows: int = Field(gt=0)
    max_epochs: int = Field(gt=0)
    max_minutes: int = Field(gt=0)
    model_size: str = Field(min_length=1)
    device: str = Field(min_length=1)


class DifferentialPrivacyConfig(CanonicalModel):
    adjacency: Literal["add_remove_one_row"]
    privacy_unit: Literal["row"]
    epsilon_model: Decimal = Field(gt=0)
    delta: Decimal = Field(gt=0, lt=1)
    epsilon_preprocess: Literal[0]
    public_metadata_manifest: ManifestFile
    public_target_count: int = Field(gt=0)
    fit_sampling_rate: Decimal = Field(gt=0, le=1)
    sampling_seed: int | None = Field(default=None, ge=0, le=2**64 - 1)


class _SynthesisRequestBase(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    dataset_id: UUID
    dataset_manifest_sha: str
    schema_version: str = Field(min_length=1)
    rules_version: str = Field(min_length=1)
    output_rows: int = Field(gt=0)
    output_formats: tuple[OutputFormat, ...]
    resource_profile: str = Field(min_length=1)
    evaluation_config_version: str = Field(min_length=1)
    generation_seed: int | None = Field(default=None, ge=0, le=2**64 - 1)

    _manifest_sha = field_validator("dataset_manifest_sha")(_validate_sha256)

    @field_validator("output_formats")
    @classmethod
    def validate_formats(cls, value: tuple[OutputFormat, ...]) -> tuple[OutputFormat, ...]:
        if not value:
            raise ValueError("at least one output format is required")
        if len(value) != len(set(value)):
            raise ValueError("output formats must not contain duplicates")
        return value


class UtilitySynthesisRequest(_SynthesisRequestBase):
    mode: Literal["utility"]
    synthesizer: Literal["tabular_argn"]
    training: UtilityTrainingConfig


class DifferentialPrivacySynthesisRequest(_SynthesisRequestBase):
    mode: Literal["differential_privacy"]
    synthesizer: Literal["mst", "aim"]
    privacy: DifferentialPrivacyConfig


SynthesisRequestValue = Annotated[
    UtilitySynthesisRequest | DifferentialPrivacySynthesisRequest,
    Field(discriminator="mode"),
]


class SynthesisRequest(RootModel[SynthesisRequestValue]):
    """Pydantic tagged union whose serialized shape is the API request object."""

    root: SynthesisRequestValue

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.root)

    @property
    def canonical_sha256(self) -> str:
        import hashlib

        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    @property
    def value(self) -> SynthesisRequestValue:
        return self.root

    @property
    def mode(self) -> Literal["utility", "differential_privacy"]:
        return self.root.mode
