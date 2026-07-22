from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any, Literal
from uuid import UUID

from jinja2 import Environment, StrictUndefined, select_autoescape
from pydantic import BaseModel

from sts.domain import canonical_json_bytes

ReportKind = Literal["utility_primary", "dp_release", "curator_internal"]

UTILITY_PRIVACY_WARNING = (
    "No formal privacy guarantee: utility synthetic data and this report may reveal "
    "information derived from the private source."
)
INTERNAL_PRIVACY_WARNING = (
    "Curator-only diagnostic: contains private source information and must not be included "
    "in a differential-privacy release bundle."
)
DP_RELEASE_NOTICE = (
    "Release-safe projection for the ledgered differential-privacy model. Empirical attacks "
    "and private-source fidelity diagnostics are intentionally excluded."
)

# These keys describe private source material that a DP release projection must never carry.
# Projection still uses positive allowlists; this set is a second, recursively enforced guard.
DP_RELEASE_FORBIDDEN_FIELDS = frozenset(
    {
        "source_count",
        "source_counts",
        "source_row_count",
        "source_rows",
        "raw_source_count",
        "private_domain",
        "private_domains",
        "exact_private_domain",
        "holdout",
        "holdout_metrics",
        "real_holdout",
        "real_holdout_metrics",
        "attack",
        "attacks",
        "attack_metrics",
        "anonymeter",
        "dcr",
        "nndr",
        "example",
        "examples",
        "raw_example",
        "raw_examples",
        "fit_timing",
        "timing",
        "fit_duration",
        "rss",
        "peak_rss",
        "worker_rss",
        "fit_seed",
        "private_seed",
        "rng_seed",
        "seed",
        "raw_dataset_digest",
        "dataset_digest",
        "source_digest",
        "dataset_manifest_digest",
    }
)

DP_LEDGER_ALLOWLIST = frozenset(
    {
        "adjacency",
        "privacy_unit",
        "epsilon",
        "epsilon_model",
        "epsilon_preprocess",
        "delta",
        "mechanism",
        "accountant",
        "conversion",
        "package",
        "package_version",
        "wheel_sha256",
        "package_wheel_sha256",
        "public_metadata_hashes",
        "run_id",
        "model_id",
        "privacy_scope_id",
        "public_target_count",
        "public_target_count_provenance",
        "release_count",
        "rule_postprocessing",
        "limitations",
    }
)
DP_OUTPUT_ALLOWLIST = frozenset(
    {
        "requested_rows",
        "actual_rows",
        "schema",
        "schema_order",
        "column_order",
        "columns",
        "dtypes",
        "null_counts",
        "category_counts",
        "hard_rule_violations",
        "artifact_hashes",
        "artifacts",
        "rule_violations",
        "canonical_content_sha256",
        "parquet_content_sha256",
        "csv_content_sha256",
    }
)
DP_ARTIFACT_ALLOWLIST = frozenset(
    {
        "artifact_id",
        "kind",
        "sha256",
        "size_bytes",
        "content_sha256",
        "row_count",
        "downloadable",
    }
)
DP_COLUMN_ALLOWLIST = frozenset(
    {
        "name",
        "kind",
        "nullable",
        "role",
        "decimal_places",
        "timezone",
        "format",
        "arrow_dtype",
        "null_count",
        "category_counts",
        "unique_non_null_count",
        "duplicate_non_null_count",
        "format_violations",
    }
)
DP_CATEGORY_COUNT_ALLOWLIST = frozenset({"value", "count"})

_KEY_NORMALIZER = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True, slots=True)
class ArtifactSafety:
    downloadable: bool
    release_safe: bool
    contains_private_source_information: bool

    def __post_init__(self) -> None:
        if self.release_safe and self.contains_private_source_information:
            raise ValueError("release-safe artifacts cannot contain private source information")


@dataclass(frozen=True, slots=True)
class BuiltReport:
    report_kind: ReportKind
    document: Mapping[str, Any]
    safety: ArtifactSafety
    json_artifact_kind: str
    html_artifact_kind: str

    def json_bytes(self) -> bytes:
        return canonical_json_bytes(self.document)

    def html_bytes(self) -> bytes:
        return render_report_html(self.document).encode("utf-8")


def _as_mapping(value: Mapping[str, Any] | BaseModel) -> Mapping[str, Any]:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", by_alias=True, exclude_none=False)
    return value


def _json_safe(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _json_safe(value.model_dump(mode="json", by_alias=True, exclude_none=False))
    if isinstance(value, Enum):
        return _json_safe(value.value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("report values must be finite")
        return format(value, "f")
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
        return [_json_safe(item) for item in value]
    return value


def _normalized_key(value: object) -> str:
    return _KEY_NORMALIZER.sub("_", str(value).strip().lower()).strip("_")


def _assert_no_forbidden_fields(value: Any, path: tuple[str, ...] = ()) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = _normalized_key(key)
            if normalized in DP_RELEASE_FORBIDDEN_FIELDS:
                location = ".".join((*path, str(key)))
                raise ValueError(f"DP release report contains forbidden field: {location}")
            _assert_no_forbidden_fields(item, (*path, str(key)))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
        for index, item in enumerate(value):
            _assert_no_forbidden_fields(item, (*path, str(index)))


def _project_mapping(
    source: Mapping[str, Any] | BaseModel, allowlist: frozenset[str]
) -> dict[str, Any]:
    mapping = _as_mapping(source)
    return {
        key: _json_safe(mapping[key])
        for key in sorted(allowlist)
        if key in mapping and mapping[key] is not None
    }


def _project_columns(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray, memoryview)):
        return []
    projected: list[dict[str, Any]] = []
    for column in value:
        if not isinstance(column, (Mapping, BaseModel)):
            continue
        mapping = _as_mapping(column)
        item = _project_mapping(mapping, DP_COLUMN_ALLOWLIST)
        category_counts = mapping.get("category_counts")
        if isinstance(category_counts, Sequence) and not isinstance(
            category_counts, (str, bytes, bytearray, memoryview)
        ):
            item["category_counts"] = [
                _project_mapping(count, DP_CATEGORY_COUNT_ALLOWLIST)
                for count in category_counts
                if isinstance(count, (Mapping, BaseModel))
            ]
        projected.append(item)
    return projected


def _project_artifacts(artifacts: Sequence[Mapping[str, Any] | BaseModel]) -> list[dict[str, Any]]:
    return [_project_mapping(artifact, DP_ARTIFACT_ALLOWLIST) for artifact in artifacts]


def _freeze_document(document: dict[str, Any]) -> Mapping[str, Any]:
    # The outer mapping is immutable so safety metadata cannot be changed after construction.
    return MappingProxyType(document)


def build_utility_primary_report(
    *,
    job_id: UUID | str,
    evaluation: Mapping[str, Any] | BaseModel,
    artifacts: Sequence[Mapping[str, Any] | BaseModel] = (),
    title: str = "Synthetic Table Studio utility quality report",
) -> BuiltReport:
    document = {
        "version": "1.0",
        "report_kind": "utility_primary",
        "mode": "utility",
        "title": title,
        "job_id": str(job_id),
        "privacy_notice": UTILITY_PRIVACY_WARNING,
        "evaluation": _json_safe(_as_mapping(evaluation)),
        "artifacts": [_json_safe(_as_mapping(artifact)) for artifact in artifacts],
        "release_safe": False,
        "contains_private_source_information": True,
    }
    return BuiltReport(
        report_kind="utility_primary",
        document=_freeze_document(document),
        safety=ArtifactSafety(
            downloadable=True,
            release_safe=False,
            contains_private_source_information=True,
        ),
        json_artifact_kind="primary_report_json",
        html_artifact_kind="primary_report_html",
    )


def build_dp_release_report(
    *,
    job_id: UUID | str,
    ledger_projection: Mapping[str, Any] | BaseModel,
    output_summary: Mapping[str, Any] | BaseModel,
    artifacts: Sequence[Mapping[str, Any] | BaseModel] = (),
    limitations: Sequence[str] = (),
    title: str = "Synthetic Table Studio differential-privacy release report",
) -> BuiltReport:
    ledger = _project_mapping(ledger_projection, DP_LEDGER_ALLOWLIST)
    output = _project_mapping(output_summary, DP_OUTPUT_ALLOWLIST)
    if "schema" in output:
        output["schema"] = _project_columns(output["schema"])
    if "columns" in output:
        output["columns"] = _project_columns(output["columns"])
    if "artifacts" in output:
        output["artifacts"] = _project_artifacts(output["artifacts"])
    document = {
        "version": "1.0",
        "report_kind": "dp_release",
        "mode": "differential_privacy",
        "title": title,
        "job_id": str(job_id),
        "privacy_notice": DP_RELEASE_NOTICE,
        "ledger": ledger,
        "output": output,
        "artifacts": _project_artifacts(artifacts),
        "limitations": [str(item) for item in limitations],
        "release_safe": True,
        "contains_private_source_information": False,
    }
    _assert_no_forbidden_fields(document)
    return BuiltReport(
        report_kind="dp_release",
        document=_freeze_document(document),
        safety=ArtifactSafety(
            downloadable=True,
            release_safe=True,
            contains_private_source_information=False,
        ),
        json_artifact_kind="dp_release_report_json",
        html_artifact_kind="dp_release_report_html",
    )


def build_curator_internal_report(
    *,
    job_id: UUID | str,
    diagnostics: Mapping[str, Any] | BaseModel,
    artifacts: Sequence[Mapping[str, Any] | BaseModel] = (),
    title: str = "Synthetic Table Studio curator diagnostic report",
) -> BuiltReport:
    document = {
        "version": "1.0",
        "report_kind": "curator_internal",
        "mode": "internal",
        "title": title,
        "job_id": str(job_id),
        "privacy_notice": INTERNAL_PRIVACY_WARNING,
        "diagnostics": _json_safe(_as_mapping(diagnostics)),
        "artifacts": [_json_safe(_as_mapping(artifact)) for artifact in artifacts],
        "release_safe": False,
        "contains_private_source_information": True,
    }
    return BuiltReport(
        report_kind="curator_internal",
        document=_freeze_document(document),
        safety=ArtifactSafety(
            downloadable=False,
            release_safe=False,
            contains_private_source_information=True,
        ),
        json_artifact_kind="internal_diagnostic_report_json",
        html_artifact_kind="internal_diagnostic_report_html",
    )


_REPORT_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ report.title }}</title>
<style>
:root{color-scheme:light dark;font-family:ui-sans-serif,system-ui,sans-serif;line-height:1.45}
body{max-width:76rem;margin:0 auto;padding:2rem;background:#f7f8fa;color:#17202a}
main{background:#fff;border:1px solid #d9dee7;border-radius:.75rem;padding:1.5rem;
box-shadow:0 1px 3px #0001}
h1{font-size:1.55rem;margin:.25rem 0 1rem}
.notice{border-left:.35rem solid #8b5e00;background:#fff5d6;padding:.8rem 1rem}
dl{display:grid;grid-template-columns:minmax(11rem,auto) 1fr;gap:.35rem .8rem;margin:.45rem 0}
dt{font-weight:650;overflow-wrap:anywhere}
dd{margin:0;overflow-wrap:anywhere}
ul{margin:.25rem 0;padding-left:1.3rem}
.scalar{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}.mark{width:2.25rem;height:2.25rem;color:#3559a8}
@media(prefers-color-scheme:dark){body{background:#111821;color:#e7ebf2}main{background:#18212d;border-color:#364252}.notice{background:#352b12}}
</style>
</head>
<body>
<main>
<svg class="mark" viewBox="0 0 36 36" role="img" aria-label="Report">
<rect x="3" y="3" width="30" height="30" rx="5" fill="none"
 stroke="currentColor" stroke-width="2"/>
<path d="M10 25V18m8 7V10m8 15V14" stroke="currentColor" stroke-width="3"/>
</svg>
<h1>{{ report.title }}</h1>
<p class="notice"><strong>Privacy boundary:</strong> {{ report.privacy_notice }}</p>
{% macro render_value(value) -%}
{% if value is mapping -%}
<dl>{% for key,item in value|dictsort %}
<dt>{{ key }}</dt><dd>{{ render_value(item) }}</dd>
{% endfor %}</dl>
{%- elif value is sequence and value is not string -%}
<ul>{% for item in value %}<li>{{ render_value(item) }}</li>{% endfor %}</ul>
{%- elif value is none -%}<span class="scalar">null</span>
{%- elif value is sameas true -%}<span class="scalar">true</span>
{%- elif value is sameas false -%}<span class="scalar">false</span>
{%- else -%}<span class="scalar">{{ value }}</span>
{%- endif %}
{%- endmacro %}
{% for key,value in report|dictsort if key not in ("title","privacy_notice") %}
<section><h2>{{ key }}</h2>{{ render_value(value) }}</section>
{% endfor %}
</main>
</body>
</html>
"""

_REPORT_ENVIRONMENT = Environment(
    autoescape=select_autoescape(default=True, default_for_string=True),
    undefined=StrictUndefined,
    enable_async=False,
)
_REPORT_ENVIRONMENT.globals.clear()
_REPORT_TEMPLATE_COMPILED = _REPORT_ENVIRONMENT.from_string(_REPORT_TEMPLATE)


def render_report_html(report: Mapping[str, Any] | BaseModel) -> str:
    mapping = _as_mapping(report)
    if "title" not in mapping or "privacy_notice" not in mapping:
        raise ValueError("report HTML requires title and privacy_notice")
    return _REPORT_TEMPLATE_COMPILED.render(report=mapping)


def assert_dp_release_safe(report: Mapping[str, Any] | BaseModel) -> None:
    mapping = _as_mapping(report)
    if mapping.get("report_kind") != "dp_release":
        raise ValueError("only DP release reports can pass the release-safety assertion")
    if mapping.get("release_safe") is not True:
        raise ValueError("DP release report must explicitly set release_safe=true")
    if mapping.get("contains_private_source_information") is not False:
        raise ValueError("DP release report must explicitly exclude private source information")
    _assert_no_forbidden_fields(mapping)
