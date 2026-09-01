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

ReportKind = Literal["utility_primary", "dp_curator", "dp_release", "curator_internal"]

UTILITY_PRIVACY_WARNING = (
    "이 일반 합성 결과에는 수학적으로 계산된 개인정보 보호 보장이 없습니다. "
    "보고서의 유사도와 공격 진단은 위험을 평가하는 보조 지표이며 안전성 보증이 아닙니다."
)
INTERNAL_PRIVACY_WARNING = (
    "자료 관리자 전용 진단입니다. 원본 자료에서 파생된 정보를 포함하므로 "
    "차등프라이버시 공개 묶음에 포함하면 안 됩니다."
)
DP_CURATOR_PRIVACY_WARNING = (
    "자료 담당자 전용 차등프라이버시 품질 보고서입니다. 원본·holdout 기반 유사도와 "
    "경험적 프라이버시 진단을 포함하므로 외부 공개 묶음에 포함하면 안 됩니다."
)
DP_RELEASE_NOTICE = (
    "검증된 privacy ledger에 기록된 차등프라이버시 모델의 공개 가능 보고서입니다. "
    "원본 기반 유사도와 경험적 공격 진단은 공개 경계를 지키기 위해 제외했습니다."
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


def _format_metric(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return "확인 불가"
    if isinstance(value, int):
        return f"{value:,}"
    return f"{float(value):.4f}"


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    numeric = float(value)
    return numeric if numeric == numeric and abs(numeric) != float("inf") else None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _items(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
        return value
    return ()


def _metric_sentence(value: float) -> str:
    return f"{value:.4f}({value * 100:.2f}%p)"


def _readable_distance(value: float | None) -> str:
    return "확인 불가" if value is None else _metric_sentence(value)


def _collect_metric_values(value: Any, key: str) -> list[float]:
    collected: list[float] = []
    if isinstance(value, Mapping):
        candidate = value.get(key)
        if isinstance(candidate, Mapping):
            numeric = _number(candidate.get("value"))
            if numeric is not None:
                collected.append(numeric)
        for child_key, child in value.items():
            if child_key != key:
                collected.extend(_collect_metric_values(child, key))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
        for child in value:
            collected.extend(_collect_metric_values(child, key))
    return collected


def _pairwise_differences(pairwise: Mapping[str, Any]) -> list[float]:
    values: list[float] = []
    for pair in _items(pairwise.get("pairs")):
        pair_mapping = _mapping(pair)
        metric = _mapping(pair_mapping.get("synthetic_difference"))
        family = pair_mapping.get("family")
        candidates: tuple[Any, ...]
        if family == "numeric":
            candidates = (
                metric.get("pearson_absolute_difference"),
                metric.get("spearman_absolute_difference"),
            )
        elif family == "categorical":
            candidates = (metric.get("tvd"),)
        elif family == "mixed":
            candidates = (metric.get("conditional_tvd"),)
        else:
            candidates = ()
        numeric = [_number(candidate) for candidate in candidates]
        values.extend(candidate for candidate in numeric if candidate is not None)
    return values


def _summary_number(evaluation: Mapping[str, Any], key: str) -> float | None:
    summary = _mapping(evaluation.get("summary"))
    exact = _mapping(evaluation.get("exact"))
    for source in (summary, exact, evaluation):
        value = _number(source.get(key))
        if value is not None:
            return value
    return None


def _quality_summary(evaluation: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    requested = _summary_number(evaluation, "requested_rows")
    actual = _summary_number(evaluation, "actual_rows")
    requested_text = f"{int(requested):,}행" if requested is not None else "요청 행 수"
    actual_text = f"{int(actual):,}행" if actual is not None else "생성 행 수를 확인할 수 없는 결과"
    violations = _number(_mapping(evaluation.get("exact")).get("hard_rule_violations"))
    if violations is None:
        violations = _number(evaluation.get("hard_rule_violations"))

    exact_ok = (
        requested is not None
        and actual is not None
        and int(requested) == int(actual)
        and violations == 0
    )
    if exact_ok:
        overall = (
            f"요청한 {requested_text}을 모두 생성했고 강제 규칙 위반 없이 구조 검증을 통과했습니다."
        )
        exact_paragraph = (
            f"행 수와 규칙: 요청한 {requested_text}과 실제 생성한 {actual_text}이 일치하고, "
            "전체 결과를 다시 검사한 강제 규칙 위반은 0건입니다."
        )
    else:
        violation_text = (
            f"{int(violations):,}건" if violations is not None else "확인할 수 없습니다"
        )
        overall = (
            f"요청한 {requested_text}에 대해 {actual_text}을 생성했지만 필수 구조 검증을 "
            "완전히 통과했다고 판단할 수 없습니다."
        )
        exact_paragraph = (
            f"행 수와 규칙: 요청 {requested_text}, 생성 {actual_text}, 강제 규칙 위반 "
            f"{violation_text}입니다. 행 수가 다르거나 위반이 1건이라도 있으면 이 결과를 "
            "사용하기 전에 원인을 해결해야 합니다."
        )

    summary = _mapping(evaluation.get("summary"))
    median = _number(summary.get("median_excess"))
    p95 = _number(summary.get("p95_excess"))
    maximum = _number(summary.get("max_excess"))
    paragraphs = [exact_paragraph]
    if median is not None or p95 is not None or maximum is not None:
        paragraphs.append(
            "분포 재현: 원본 표본 자체의 차이를 뺀 baseline-excess는 "
            f"중앙값 {_readable_distance(median)}, "
            f"95백분위 {_readable_distance(p95)}, "
            f"최댓값 {_readable_distance(maximum)}입니다. 0에 가까울수록 합성자료 때문에 "
            "추가된 분포 오차가 작다는 뜻이며, 이 수치는 데이터의 사용 목적과 함께 "
            "판단해야 합니다."
        )
    else:
        paragraphs.append(
            "분포 재현: 해석 가능한 baseline-excess 결과가 없어 원본 분포를 얼마나 "
            "재현했는지 판단할 수 없습니다."
        )

    columns = [_mapping(column) for column in _items(evaluation.get("columns"))]
    ranked = sorted(
        (
            (_number(column.get("baseline_excess")), str(column.get("name", "")))
            for column in columns
        ),
        key=lambda item: -1.0 if item[0] is None else item[0],
        reverse=True,
    )
    weakest = [(value, name) for value, name in ranked if value is not None and name][:3]
    if weakest:
        paragraphs.append(
            "우선 확인할 열: "
            + ", ".join(f"{name} {_metric_sentence(value)}" for value, name in weakest)
            + " 순으로 baseline-excess가 컸습니다. 이 열의 분포와 실제 분석 결과를 먼저 "
            "확인해야 합니다."
        )

    advanced = _mapping(evaluation.get("advanced"))
    pairwise_values = _pairwise_differences(_mapping(advanced.get("pairwise")))
    c2st = _mapping(_mapping(advanced.get("c2st")).get("synthetic_vs_untouched_holdout"))
    c2st_values = [
        value
        for family in ("linear", "nonlinear")
        if (value := _number(_mapping(c2st.get(family)).get("auroc"))) is not None
    ]
    relationship_parts: list[str] = []
    if pairwise_values:
        relationship_parts.append(f"열 관계 거리의 최댓값은 {max(pairwise_values):.4f}")
    if c2st_values:
        relationship_parts.append(
            "실제/합성 판별 AUROC는 "
            + "~".join(f"{value:.4f}" for value in (min(c2st_values), max(c2st_values)))
        )
    if relationship_parts:
        paragraphs.append(
            "열 관계와 전체 표: "
            + ", ".join(relationship_parts)
            + "입니다. 열 관계 거리는 0에, 판별 AUROC는 실제자료끼리 비교한 대조값과 "
            "가까울수록 원본 구조를 더 비슷하게 재현한 것으로 해석합니다."
        )

    downstream = _mapping(advanced.get("downstream_utility"))
    if downstream.get("applicable") is True:
        paragraphs.append(
            f"분석 목적 재현: {downstream.get('target')} 예측에서 실제자료 학습 결과"
            f"(TRTR)는 {_format_metric(downstream.get('trtr'))}, 합성자료 학습 결과"
            f"(TSTR)는 {_format_metric(downstream.get('tstr'))}입니다. 두 값의 차이가 "
            "실제 사용 목적에서 허용 가능한지 확인해야 합니다."
        )
    else:
        paragraphs.append(
            "분석 목적 재현: 목표 열과 분류·회귀 과제가 지정되지 않아 실제 분석 결과를 "
            "얼마나 재현하는지는 평가하지 않았습니다."
        )

    return overall, {"heading": "재현 품질", "paragraphs": paragraphs}


def _privacy_summary(
    evaluation: Mapping[str, Any] | None,
    *,
    formal_dp_ledger: Mapping[str, Any] | None = None,
    release_report: bool = False,
) -> dict[str, Any]:
    paragraphs: list[str] = []
    if formal_dp_ledger is None:
        paragraphs.append(
            "이 일반 합성 결과에는 형식적 차등프라이버시 보장은 없습니다. 원본과 비슷하지 "
            "않다는 관측이나 아래 공격 진단만으로 개인정보가 안전하다고 판단하면 안 됩니다."
        )
    else:
        mechanism = formal_dp_ledger.get("mechanism", "MST")
        epsilon = formal_dp_ledger.get(
            "epsilon", formal_dp_ledger.get("epsilon_model", "확인 불가")
        )
        delta = formal_dp_ledger.get("delta", "확인 불가")
        paragraphs.append(
            f"이 결과에는 {mechanism} 메커니즘의 형식적 차등프라이버시가 적용되었습니다. "
            f"보호 매개변수는 ε={epsilon}, δ={delta}이며, 이는 안전한 사람의 비율이나 "
            "재식별 확률이 아니라 한 행의 포함 여부가 결과 확률에 미치는 영향의 상한입니다."
        )
        release_count = formal_dp_ledger.get("release_count")
        if release_count is not None:
            paragraphs.append(
                f"같은 privacy scope에서 확인된 누적 공개 횟수는 "
                f"{_format_metric(release_count)}회입니다. 반복 공개에서는 각 실행을 따로 "
                "보지 말고 ledger의 누적 ε와 δ를 함께 검토해야 합니다."
            )

    if release_report:
        paragraphs.append(
            "이 외부 공개용 보고서에는 원본 기반 유사도와 공격 진단은 포함하지 않았습니다. "
            "그 값은 원본에서 파생된 비공개 정보이므로 담당자용 내부 보고서에서만 확인해야 합니다."
        )
        return {"heading": "프라이버시 보호", "paragraphs": paragraphs}

    advanced = _mapping(_mapping(evaluation or {}).get("advanced"))
    empirical = _mapping(advanced.get("empirical_privacy"))
    gower = _mapping(empirical.get("gower"))
    gower_difference = _number(_mapping(gower.get("dcr")).get("synthetic_median_minus_control"))
    if gower.get("applicable") is True and gower_difference is not None:
        direction = (
            "holdout 대조군보다 원본 학습행에서 더 멀었습니다"
            if gower_difference >= 0
            else "holdout 대조군보다 원본 학습행에 더 가까웠습니다"
        )
        paragraphs.append(
            f"Gower 최근접거리 중앙값 차이(합성-대조)는 {gower_difference:.4f}로, "
            f"합성행이 {direction}. 이 값은 원본 근접복제 가능성을 살피는 경고 신호일 뿐 "
            "안전하다는 뜻은 아닙니다."
        )
    else:
        paragraphs.append(
            "Gower 최근접거리 진단을 실행하지 못했으므로 원본 근접 가능성에 대한 이 "
            "경험적 검사는 확인할 수 없습니다."
        )

    anonymeter = _mapping(empirical.get("anonymeter"))
    risks = _collect_metric_values(anonymeter.get("results_by_seed"), "excess_risk")
    if anonymeter.get("applicable") is True and risks:
        paragraphs.append(
            f"Anonymeter 공격 진단에서 {len(risks):,}개 excess-risk 측정값을 얻었고 "
            f"범위는 {min(risks):.4f}~{max(risks):.4f}입니다. 값이 클수록 합성자료로 "
            "인해 공격 성공이 늘어난 것이므로 공개 전 세부 결과를 검토해야 합니다."
        )
    else:
        paragraphs.append(
            "비밀 열과 보조 열이 지정되지 않았거나 실행 조건을 충족하지 않아 Anonymeter "
            "공격 진단은 수행하지 않았습니다."
        )
    return {"heading": "프라이버시 보호", "paragraphs": paragraphs}


def _executive_summary(
    evaluation: Mapping[str, Any],
    *,
    formal_dp_ledger: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    overall, quality = _quality_summary(evaluation)
    return {
        "overall_conclusion": overall,
        "quality": quality,
        "privacy": _privacy_summary(evaluation, formal_dp_ledger=formal_dp_ledger),
        "limitations": [
            (
                "품질 지표에는 모든 데이터와 분석 목적에 공통으로 적용할 보편적인 "
                "합격 기준이 없습니다."
            ),
            (
                "재현자료는 원본 사실 확인, 개인 단위 판단 또는 공식 통계를 "
                "자동으로 대체하지 않습니다."
            ),
        ],
    }


def _dp_release_executive_summary(
    ledger: Mapping[str, Any], output: Mapping[str, Any]
) -> dict[str, Any]:
    overall, quality = _quality_summary(output)
    return {
        "overall_conclusion": overall,
        "quality": quality,
        "privacy": _privacy_summary(None, formal_dp_ledger=ledger, release_report=True),
        "limitations": [
            ("공개용 보고서는 원본에서 계산한 통계적 유사도와 공격 진단을 의도적으로 제외합니다."),
            (
                "품질 판단은 접근이 통제된 담당자용 내부 보고서와 실제 사용 목적을 "
                "함께 검토해야 합니다."
            ),
        ],
    }


def _utility_presentation(evaluation: Mapping[str, Any]) -> dict[str, Any]:
    """Project the runtime evaluation shape into stable report summary fields."""

    document = dict(evaluation)
    exact = evaluation.get("exact")
    primary = evaluation.get("primary")
    exact_mapping = exact if isinstance(exact, Mapping) else {}
    primary_mapping = primary if isinstance(primary, Mapping) else {}
    aggregate = primary_mapping.get("baseline_excess")
    aggregate_mapping = aggregate if isinstance(aggregate, Mapping) else {}

    if not isinstance(document.get("summary"), Mapping):

        def aggregate_value(name: str) -> Any:
            metric = aggregate_mapping.get(name)
            return metric.get("value") if isinstance(metric, Mapping) else None

        document["summary"] = {
            "requested_rows": exact_mapping.get("requested_rows"),
            "actual_rows": exact_mapping.get("actual_rows"),
            "median_excess": aggregate_value("median"),
            "p95_excess": aggregate_value("p95"),
            "max_excess": aggregate_value("maximum"),
        }

    source_columns = primary_mapping.get("columns")
    if (
        not isinstance(document.get("columns"), Sequence)
        or isinstance(document.get("columns"), (str, bytes, bytearray))
    ) and isinstance(source_columns, Sequence):
        projected_columns: list[dict[str, Any]] = []
        for column in source_columns:
            if not isinstance(column, Mapping) or not column.get("included_in_fidelity_aggregate"):
                continue
            synthetic = column.get("synthetic_distance")
            excess = column.get("baseline_excess")
            missingness = column.get("synthetic_missingness_difference")
            synthetic_mapping = synthetic if isinstance(synthetic, Mapping) else {}
            excess_mapping = excess if isinstance(excess, Mapping) else {}
            missingness_mapping = missingness if isinstance(missingness, Mapping) else {}
            projected_columns.append(
                {
                    "name": column.get("name"),
                    "metric": synthetic_mapping.get("metric"),
                    "distance": synthetic_mapping.get("value"),
                    "baseline_excess": excess_mapping.get("value"),
                    "missingness_difference": missingness_mapping.get("value"),
                }
            )
        document["columns"] = projected_columns
    return document


def _utility_narrative(
    evaluation: Mapping[str, Any],
    *,
    formal_dp_ledger: Mapping[str, Any] | None = None,
) -> list[str]:
    summary = _mapping(evaluation.get("summary"))
    columns = [_mapping(column) for column in _items(evaluation.get("columns"))]
    exact = _mapping(evaluation.get("exact"))
    advanced = _mapping(evaluation.get("advanced"))
    narrative = [
        (
            "생성 결과: 요청한 "
            f"{_format_metric(summary.get('requested_rows'))}행 중 "
            f"{_format_metric(summary.get('actual_rows'))}행을 생성했습니다. "
            f"전체 결과 검사에서 확인된 강제 규칙 위반은 "
            f"{_format_metric(exact.get('hard_rule_violations'))}건입니다."
        ),
        (
            "단일 열 유사도: 원본 holdout 자체의 변동을 뺀 거리인 baseline-excess의 "
            f"중앙값은 {_format_metric(summary.get('median_excess'))}, "
            f"95백분위는 {_format_metric(summary.get('p95_excess'))}, "
            f"최댓값은 {_format_metric(summary.get('max_excess'))}입니다. "
            "0에 가까울수록 합성 자료의 추가 오차가 작습니다."
        ),
    ]

    ranked_columns = sorted(
        (
            (_number(column.get("baseline_excess")), str(column.get("name", "")))
            for column in columns
        ),
        key=lambda item: -1.0 if item[0] is None else item[0],
        reverse=True,
    )
    ranked_columns = [item for item in ranked_columns if item[0] is not None and item[1]]
    if ranked_columns:
        weakest = ", ".join(
            f"{name} {_metric_sentence(value)}" for value, name in ranked_columns[:3]
        )
        narrative.append(
            f"열별 확인: 비교 가능한 {len(columns):,}개 열 가운데 baseline-excess가 "
            f"큰 열은 {weakest} 순입니다. 이 열들은 사용 목적에 맞는지 우선 검토해야 합니다."
        )
    else:
        narrative.append("열별 확인: 해석 가능한 단일 열 유사도 측정값이 없습니다.")

    pairwise = _mapping(advanced.get("pairwise"))
    pairwise_values = _pairwise_differences(pairwise)
    if pairwise_values:
        ordered = sorted(pairwise_values)
        median = ordered[len(ordered) // 2]
        narrative.append(
            f"열 관계 보존: {int(pairwise.get('pairs_considered', 0)):,}개 열 쌍에서 "
            f"상관계수 차이 또는 TVD 계열 거리의 중앙값은 {_metric_sentence(median)}, "
            f"최댓값은 {_metric_sentence(max(ordered))}입니다. 0에 가까울수록 "
            "원본의 두 열 관계를 더 비슷하게 보존한 것입니다."
        )
    else:
        narrative.append("열 관계 보존: 적용 가능한 열 쌍 측정값이 없어 평가하지 못했습니다.")

    c2st = _mapping(advanced.get("c2st"))
    synthetic_c2st = _mapping(c2st.get("synthetic_vs_untouched_holdout"))
    control_c2st = _mapping(c2st.get("real_train_eval_vs_untouched_holdout_control"))
    c2st_parts: list[str] = []
    for family, label in (("linear", "선형"), ("nonlinear", "비선형")):
        auc = _number(_mapping(synthetic_c2st.get(family)).get("auroc"))
        control_auc = _number(_mapping(control_c2st.get(family)).get("auroc"))
        if auc is not None:
            comparison = (
                f", 실제 자료끼리 비교한 대조값은 {control_auc:.4f}"
                if control_auc is not None
                else ""
            )
            c2st_parts.append(f"{label} AUROC {auc:.4f}{comparison}")
    if c2st_parts:
        narrative.append(
            "전체 표 판별(C2ST): "
            + "; ".join(c2st_parts)
            + "입니다. AUROC 0.5에 가까울수록 분류기가 실제 holdout과 합성 자료를 "
            "구별하기 어렵고, 값이 커질수록 구별하기 쉽습니다."
        )
    else:
        narrative.append("전체 표 판별(C2ST): 적용 가능한 측정값이 없어 평가하지 못했습니다.")

    downstream = _mapping(advanced.get("downstream_utility"))
    if downstream.get("applicable") is True:
        metric = str(downstream.get("metric", "metric"))
        trtr = _format_metric(downstream.get("trtr"))
        tstr = _format_metric(downstream.get("tstr"))
        difference = _format_metric(downstream.get("difference_tstr_minus_trtr"))
        narrative.append(
            f"분석 활용성: 사용자가 지정한 {downstream.get('target')} 예측에서 "
            f"실제 자료 학습(TRTR) {metric}은 {trtr}, 합성 자료 학습(TSTR)은 {tstr}, "
            f"TSTR-TRTR 차이는 {difference}입니다. 두 값을 직접 비교해 합성 자료가 "
            "해당 분석을 얼마나 재현하는지 판단해야 합니다."
        )
    else:
        narrative.append(
            "분석 활용성: 목표 열과 분류·회귀 작업을 지정하지 않아 TRTR/TSTR 평가는 "
            "실행하지 않았습니다."
        )

    empirical = _mapping(advanced.get("empirical_privacy"))
    gower = _mapping(empirical.get("gower"))
    gower_dcr = _mapping(gower.get("dcr"))
    dcr_difference = _number(gower_dcr.get("synthetic_median_minus_control"))
    if gower.get("applicable") is True and dcr_difference is not None:
        direction = (
            "합성 행이 원본 학습 행에서 holdout 대조군보다 더 멉니다"
            if dcr_difference >= 0
            else "합성 행이 원본 학습 행에 holdout 대조군보다 더 가깝습니다"
        )
        narrative.append(
            f"경험적 프라이버시 진단: Gower 최근접거리 중앙값 차이"
            f"(합성-대조)는 {dcr_difference:.4f}로, {direction}. "
            "음수이거나 0에 가까운 결과는 원본 근접 가능성을 추가 검토할 신호지만 "
            "그 자체가 재식별 또는 안전을 확정하지는 않습니다."
        )
    else:
        narrative.append("경험적 프라이버시 진단: Gower 최근접거리 진단을 실행하지 못했습니다.")

    anonymeter = _mapping(empirical.get("anonymeter"))
    risks = _collect_metric_values(anonymeter.get("results_by_seed"), "excess_risk")
    if anonymeter.get("applicable") is True and risks:
        narrative.append(
            f"Anonymeter 공격 진단: 명시적으로 지정한 비밀·보조 열에 대해 "
            f"{len(risks):,}개 excess-risk 측정값을 얻었고 범위는 "
            f"{min(risks):.4f}~{max(risks):.4f}입니다. 값이 클수록 합성 자료로 인해 "
            "공격 성공이 대조 공격보다 늘어난 것이므로 공개 전 세부 결과를 검토해야 합니다."
        )
    else:
        narrative.append(
            "Anonymeter 공격 진단: 비밀 열과 보조 열을 사용자가 명시하지 않았거나 "
            "평가 조건을 충족하지 않아 실행하지 않았습니다. 자동으로 비밀 열을 선택하지 않습니다."
        )

    if formal_dp_ledger is None:
        narrative.append(
            "개인정보 보호 결론: 이 일반 합성 모드는 형식적 차등프라이버시를 적용하지 "
            "않았습니다. 위 거리와 공격 결과는 위험 진단일 뿐 개인정보 보호 보장이 아니므로, "
            "외부 공개 여부는 별도 심사와 사용 목적을 함께 고려해 결정해야 합니다."
        )
    else:
        epsilon = formal_dp_ledger.get("epsilon", formal_dp_ledger.get("epsilon_model"))
        delta = formal_dp_ledger.get("delta")
        narrative.append(
            "개인정보 보호 결론: 이 결과에는 "
            f"{formal_dp_ledger.get('mechanism', 'MST')} 메커니즘의 "
            f"(ε={epsilon}, δ={delta}) 차등프라이버시가 적용되었습니다. "
            f"보호 단위는 {formal_dp_ledger.get('privacy_unit', '확인 불가')}, "
            f"인접성은 {formal_dp_ledger.get('adjacency', '확인 불가')}입니다. "
            "Gower와 Anonymeter 결과는 형식적 보장과 별개의 경험적 보조 진단이며, "
            "반복 공개 시에는 privacy ledger의 누적 예산을 함께 확인해야 합니다."
        )
    return narrative


def build_utility_primary_report(
    *,
    job_id: UUID | str,
    evaluation: Mapping[str, Any] | BaseModel,
    artifacts: Sequence[Mapping[str, Any] | BaseModel] = (),
    title: str = "Synthetic Table Studio 합성데이터 품질 분석 보고서",
) -> BuiltReport:
    evaluation_document = _utility_presentation(_json_safe(_as_mapping(evaluation)))
    document = {
        "version": "1.0",
        "report_kind": "utility_primary",
        "mode": "utility",
        "title": title,
        "job_id": str(job_id),
        "privacy_notice": UTILITY_PRIVACY_WARNING,
        "narrative": _utility_narrative(evaluation_document),
        "executive_summary": _executive_summary(evaluation_document),
        "evaluation": evaluation_document,
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


def build_dp_curator_report(
    *,
    job_id: UUID | str,
    evaluation: Mapping[str, Any] | BaseModel,
    ledger_projection: Mapping[str, Any] | BaseModel,
    artifacts: Sequence[Mapping[str, Any] | BaseModel] = (),
    limitations: Sequence[str] = (),
    title: str = "Synthetic Table Studio 담당자용 DP 품질·프라이버시 종합 보고서",
) -> BuiltReport:
    evaluation_document = _utility_presentation(_json_safe(_as_mapping(evaluation)))
    ledger = _json_safe(_as_mapping(ledger_projection))
    document = {
        "version": "1.0",
        "report_kind": "dp_curator",
        "mode": "differential_privacy",
        "title": title,
        "job_id": str(job_id),
        "privacy_notice": DP_CURATOR_PRIVACY_WARNING,
        "narrative": _utility_narrative(
            evaluation_document,
            formal_dp_ledger=ledger,
        ),
        "executive_summary": _executive_summary(
            evaluation_document,
            formal_dp_ledger=ledger,
        ),
        "evaluation": evaluation_document,
        "ledger": ledger,
        "artifacts": [_json_safe(_as_mapping(artifact)) for artifact in artifacts],
        "limitations": [str(item) for item in limitations],
        "release_safe": False,
        "contains_private_source_information": True,
    }
    return BuiltReport(
        report_kind="dp_curator",
        document=_freeze_document(document),
        safety=ArtifactSafety(
            downloadable=True,
            release_safe=False,
            contains_private_source_information=True,
        ),
        json_artifact_kind="primary_report_json",
        html_artifact_kind="primary_report_html",
    )


def _dp_narrative(
    ledger: Mapping[str, Any],
    output: Mapping[str, Any],
    limitations: Sequence[str],
) -> list[str]:
    epsilon = ledger.get("epsilon", ledger.get("epsilon_model"))
    delta = ledger.get("delta")
    mechanism = ledger.get("mechanism", "MST")
    adjacency = ledger.get("adjacency", "확인 불가")
    privacy_unit = ledger.get("privacy_unit", "확인 불가")
    hard_violations = output.get("hard_rule_violations")
    narrative = [
        (
            f"생성 결과: 요청한 {_format_metric(output.get('requested_rows'))}행 중 "
            f"{_format_metric(output.get('actual_rows'))}행을 생성했고, 공개 결과의 "
            f"강제 규칙 위반은 {_format_metric(hard_violations)}건입니다."
        ),
        (
            f"형식적 개인정보 보호: {mechanism} 메커니즘에 "
            f"ε={epsilon if epsilon is not None else '확인 불가'}, "
            f"δ={delta if delta is not None else '확인 불가'}를 적용했습니다. "
            f"보호 단위는 {privacy_unit}, 인접성 정의는 {adjacency}입니다. "
            "같은 조건에서는 일반적으로 ε와 δ가 작을수록 더 강한 보호를 뜻하지만, "
            "자료 범위와 반복 공개 횟수를 함께 봐야 합니다."
        ),
        (
            f"Privacy ledger: 이 공개의 누적 공개 횟수는 "
            f"{_format_metric(ledger.get('release_count'))}회입니다. 이 값과 ledger의 "
            "누적 예산을 확인하지 않고 다른 결과와 독립적인 보호 보장으로 해석하면 안 됩니다."
        ),
        (
            "유사도 분석: 이 공개용 DP 보고서에는 원본·holdout에서 계산한 KS, TVD, "
            "C2ST 및 경험적 공격 결과를 넣지 않았습니다. 해당 값은 원본에서 파생된 "
            "비공개 진단이므로 release-safe 공개 경계를 통과할 수 없습니다."
        ),
        (
            "프라이버시 해석: 위 (ε, δ) 보장은 ledger에 기록된 메커니즘과 인접성에 "
            "대한 수학적 보장입니다. 데이터가 분석 목적에 충분히 유사한지는 별도의 "
            "관리자 내부 품질평가에서 확인해야 하며, 그 결과를 이 공개 보고서와 혼합하면 안 됩니다."
        ),
    ]
    if limitations:
        narrative.append("제한사항: " + " ".join(str(item) for item in limitations))
    return narrative


def build_dp_release_report(
    *,
    job_id: UUID | str,
    ledger_projection: Mapping[str, Any] | BaseModel,
    output_summary: Mapping[str, Any] | BaseModel,
    artifacts: Sequence[Mapping[str, Any] | BaseModel] = (),
    limitations: Sequence[str] = (),
    title: str = "Synthetic Table Studio 차등프라이버시 공개 보고서",
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
        "narrative": _dp_narrative(ledger, output, limitations),
        "executive_summary": _dp_release_executive_summary(ledger, output),
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
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ report.title }}</title>
<style>
:root{color-scheme:light dark;font-family:ui-sans-serif,system-ui,sans-serif;line-height:1.65}
body{max-width:76rem;margin:0 auto;padding:2rem;background:#f7f8fa;color:#17202a}
main{background:#fff;border:1px solid #d9dee7;border-radius:.75rem;padding:1.5rem;
box-shadow:0 1px 3px #0001}
h1{font-size:1.65rem;margin:.25rem 0 1rem}h2{margin-top:2rem}h3{overflow-wrap:anywhere}
.notice{border-left:.35rem solid #8b5e00;background:#fff5d6;padding:.8rem 1rem}
.analysis{border:1px solid #cfd8e8;border-radius:.6rem;padding:1rem 1.25rem;background:#f8faff}
.executive{border:2px solid #3559a8;border-radius:.7rem;padding:1rem 1.25rem;background:#f5f8ff}
.executive h2{margin-top:0}.executive h3{margin:1.25rem 0 .35rem}.executive p{margin:.55rem 0}
.executive .overall{font-size:1.08rem}.limitations{color:#4b5563}
.analysis p{margin:.8rem 0}
.technical-details{border-top:1px solid #d9dee7;margin-top:2rem;padding-top:1rem}
summary{cursor:pointer;font-weight:700}.technical-details>p{color:#4b5563}
dl{display:grid;grid-template-columns:minmax(11rem,auto) 1fr;gap:.35rem .8rem;margin:.45rem 0}
dt{font-weight:650;overflow-wrap:anywhere}
dd{margin:0;overflow-wrap:anywhere}
ul{margin:.25rem 0;padding-left:1.3rem}
.scalar{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}.mark{width:2.25rem;height:2.25rem;color:#3559a8}
@media(prefers-color-scheme:dark){body{background:#111821;color:#e7ebf2}main{background:#18212d;border-color:#364252}.notice{background:#352b12}.analysis{background:#151f2e;border-color:#3b4b63}}
</style>
</head>
<body>
<main>
<svg class="mark" viewBox="0 0 36 36" role="img" aria-label="품질 분석 보고서">
<rect x="3" y="3" width="30" height="30" rx="5" fill="none"
 stroke="currentColor" stroke-width="2"/>
<path d="M10 25V18m8 7V10m8 15V14" stroke="currentColor" stroke-width="3"/>
</svg>
<h1>{{ report.title }}</h1>
<p class="notice"><strong>개인정보 보호 경계:</strong> {{ report.privacy_notice }}</p>
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
{% if report.executive_summary is defined %}
<section class="executive">
<h2>한눈에 보는 결론</h2>
<p class="overall"><strong>{{ report.executive_summary.overall_conclusion }}</strong></p>
{% for section_name in ("quality","privacy") %}
{% set section = report.executive_summary[section_name] %}
<h3>{{ section.heading }}</h3>
{% for paragraph in section.paragraphs %}<p>{{ paragraph }}</p>{% endfor %}
{% endfor %}
{% if report.executive_summary.limitations %}
<h3>해석할 때 주의할 점</h3>
<ul class="limitations">
{% for item in report.executive_summary.limitations %}<li>{{ item }}</li>{% endfor %}
</ul>
{% endif %}
</section>
{% endif %}
{% if report.narrative is defined %}
<section class="analysis"><h2>결과 해석</h2>
{% for paragraph in report.narrative %}<p>{{ paragraph }}</p>{% endfor %}
</section>
{% endif %}
<details class="technical-details">
<summary>기술 검증 데이터 보기</summary>
<p>이 영역은 시스템 연동과 정밀 검증을 위한 구조화된 원본 지표입니다.
일반적인 품질 검토에는 위 자연어 결론을 먼저 사용하세요.</p>
<section class="details"><h2>세부 측정값 및 재현 정보</h2>
{% for key,value in report|dictsort
if key not in ("title","privacy_notice","executive_summary","narrative") %}
<h3>{{ key }}</h3>{{ render_value(value) }}
{% endfor %}
</section>
</details>
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
