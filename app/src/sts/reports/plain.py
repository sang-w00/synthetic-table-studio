"""Plain-language quality report for readers who are not evaluation specialists.

The HTML/JSON reports built in :mod:`sts.reports.builders` are complete but
dense. This module projects the same, already-computed report document into a
short Korean document a reviewer can read end to end: a verdict, a small table
of the numbers that decide it, the columns worth checking first, the privacy
boundary, and a glossary of every term the document uses.

It never computes new statistics. Every number here is copied from the report
document that was built and safety-classified elsewhere, so the release
boundary of the source report is preserved exactly: a DP release report only
ever yields release-safe content because that is all its document contains.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .builders import (
    ArtifactSafety,
    BuiltReport,
    ReportKind,
    _items,
    _mapping,
    _number,
)
from .hwpx import Block, Paragraph, Table, build_hwpx

_ARTIFACT_KINDS: Mapping[ReportKind, tuple[str, str]] = {
    "utility_primary": ("primary_report_hwpx", "quality-report.hwpx"),
    "dp_curator": ("primary_report_hwpx", "quality-report.hwpx"),
    "dp_release": ("dp_release_report_hwpx", "dp-release-quality-report.hwpx"),
}

_PLAIN_TITLES: Mapping[ReportKind, str] = {
    "utility_primary": "재현자료 품질 보고서",
    "dp_curator": "재현자료 품질 보고서 (담당자용)",
    "dp_release": "재현자료 품질 보고서 (외부 공개용)",
}

_INTRO: Mapping[ReportKind, str] = {
    "utility_primary": (
        "이 문서는 원본 자료를 대신해 사용할 수 있도록 새로 만든 재현자료(합성자료)가 "
        "원본을 얼마나 비슷하게 재현했는지, 그리고 그 결과를 해석할 때 무엇을 조심해야 "
        "하는지를 정리한 것입니다. 통계 전문 용어는 뒤쪽 용어 해설에서 설명합니다."
    ),
    "dp_curator": (
        "이 문서는 형식적 차등프라이버시를 적용해 만든 재현자료의 품질과 개인정보 보호 "
        "진단을 자료 담당자가 검토할 수 있도록 정리한 것입니다. 원본에서 계산한 값이 "
        "들어 있으므로 외부에 그대로 공개하면 안 됩니다."
    ),
    "dp_release": (
        "이 문서는 형식적 차등프라이버시를 적용해 만든 재현자료를 외부에 공개할 때 함께 "
        "전달하는 설명서입니다. 원본에서 계산한 유사도와 공격 진단은 공개 경계를 지키기 "
        "위해 일부러 넣지 않았습니다."
    ),
}


@dataclass(frozen=True, slots=True)
class PlainLanguageReport:
    """A ready-to-render plain-language document with its inherited safety class."""

    report_kind: ReportKind
    title: str
    blocks: tuple[Block, ...]
    safety: ArtifactSafety
    artifact_kind: str
    filename: str

    def hwpx_bytes(self) -> bytes:
        return build_hwpx(self.title, self.blocks)


def _count(value: object) -> str:
    numeric = _number(value)
    return "확인 불가" if numeric is None else f"{int(numeric):,}"


def _distance(value: object) -> str:
    numeric = _number(value)
    return "확인 불가" if numeric is None else f"{numeric:.4f} ({numeric * 100:.2f}%p)"


def _evaluation_of(document: Mapping[str, object]) -> Mapping[str, object]:
    """The metric source: the evaluation for curator reports, the output for a release."""

    evaluation = _mapping(document.get("evaluation"))
    return evaluation if evaluation else _mapping(document.get("output"))


def _summary_values(document: Mapping[str, object]) -> dict[str, float | None]:
    evaluation = _evaluation_of(document)
    summary = _mapping(evaluation.get("summary"))
    exact = _mapping(evaluation.get("exact"))

    def pick(key: str) -> float | None:
        for source in (summary, exact, evaluation):
            value = _number(source.get(key))
            if value is not None:
                return value
        return None

    return {
        key: pick(key)
        for key in (
            "requested_rows",
            "actual_rows",
            "hard_rule_violations",
            "median_excess",
            "p95_excess",
            "max_excess",
        )
    }


def _structure_verdict(values: Mapping[str, float | None]) -> tuple[str, str]:
    requested = values["requested_rows"]
    actual = values["actual_rows"]
    violations = values["hard_rule_violations"]
    if requested is None or actual is None or violations is None:
        return (
            "확인 필요",
            "행 수 또는 규칙 검증 결과를 읽을 수 없어 구조 검증 통과 여부를 판단할 수 "
            "없습니다. 이 결과를 사용하기 전에 원인을 확인해야 합니다.",
        )
    if int(requested) == int(actual) and int(violations) == 0:
        return (
            "정상",
            f"요청한 {int(requested):,}행을 모두 만들었고, 전체 결과를 다시 검사한 "
            "강제 규칙 위반은 한 건도 없습니다. 형식과 구조 면에서는 그대로 사용할 수 "
            "있는 상태입니다.",
        )
    return (
        "확인 필요",
        f"요청은 {int(requested):,}행인데 실제로는 {int(actual):,}행을 만들었고, 강제 "
        f"규칙 위반은 {int(violations):,}건입니다. 행 수가 다르거나 위반이 한 건이라도 "
        "있으면 원인을 해결하기 전에는 이 결과를 사용하면 안 됩니다.",
    )


def _fidelity_band(median: float | None) -> tuple[str, str]:
    """A reading aid, deliberately not a pass/fail standard."""

    if median is None:
        return (
            "확인 불가",
            "해석할 수 있는 분포 차이 측정값이 없어 원본을 얼마나 재현했는지 판단할 수 없습니다.",
        )
    if median < 0.02:
        band = "차이가 매우 작음"
    elif median < 0.05:
        band = "차이가 작음"
    elif median < 0.10:
        band = "차이가 보통"
    else:
        band = "차이가 큼"
    return (
        band,
        f"열 하나하나의 분포 차이는 중앙값 기준 {_distance(median)}이며, 위 구간으로는 "
        f"'{band}'에 해당합니다. 이 구간은 읽기를 돕기 위한 참고선일 뿐이며, 모든 자료와 "
        "모든 분석 목적에 통용되는 합격 기준은 존재하지 않습니다. 실제 사용 여부는 이 "
        "자료로 어떤 분석을 할 것인지와 함께 판단해야 합니다.",
    )


_GLOSSARY: tuple[tuple[str, str, str], ...] = (
    (
        "always",
        "재현자료(합성자료)",
        "원본 자료의 통계적 성질을 흉내 내도록 새로 만들어 낸 가짜 자료입니다. "
        "원본에 있던 특정한 사람의 기록이 그대로 옮겨진 것이 아닙니다.",
    ),
    (
        "always",
        "강제 규칙",
        "'값이 비어 있으면 안 된다', '합계가 맞아야 한다'처럼 재현자료가 반드시 지켜야 "
        "하도록 지정한 조건입니다. 위반이 0건이어야 정상입니다.",
    ),
    (
        "excess",
        "기준선 초과 (baseline-excess)",
        "원본 자료를 둘로 나눠 서로 비교해도 우연히 생기는 차이가 있습니다. 그 차이를 "
        "빼고 남은, 재현자료 때문에 추가로 생긴 차이만 나타낸 값입니다. 0에 가까울수록 "
        "잘 재현한 것입니다.",
    ),
    (
        "ks",
        "KS 거리",
        "숫자형 열에서 두 자료의 분포가 얼마나 다른지를 0에서 1 사이로 나타낸 값입니다. "
        "0이면 분포가 같고 1이면 완전히 다릅니다.",
    ),
    (
        "tvd",
        "TVD (총변동거리)",
        "범주형 열에서 각 항목이 차지하는 비율이 얼마나 다른지를 0에서 1 사이로 나타낸 "
        "값입니다. 0이면 비율이 같습니다.",
    ),
    (
        "c2st",
        "C2ST · AUROC",
        "'이 행이 원본인지 재현자료인지' 맞히도록 학습시킨 판별기의 성적입니다. AUROC가 "
        "0.5에 가까우면 구별하지 못한다는 뜻이고, 1에 가까우면 쉽게 구별된다는 뜻입니다.",
    ),
    (
        "downstream",
        "TRTR · TSTR",
        "같은 예측 분석을 원본으로 학습했을 때(TRTR)와 재현자료로 학습했을 때(TSTR)의 "
        "성적입니다. 두 값이 비슷할수록 재현자료로도 같은 분석 결론을 얻을 수 있습니다.",
    ),
    (
        "gower",
        "Gower 최근접거리",
        "재현자료의 각 행이 원본의 가장 비슷한 행과 얼마나 떨어져 있는지를 잰 값입니다. "
        "원본을 그대로 베낀 행이 있는지 살피는 경고용 지표입니다.",
    ),
    (
        "anonymeter",
        "Anonymeter 공격 진단",
        "공격자가 재현자료를 이용해 특정인의 비밀 값을 알아맞힐 수 있는지 모의로 "
        "시험한 결과입니다. 값이 클수록 공격이 더 잘 통했다는 뜻입니다.",
    ),
    (
        "dp",
        "차등프라이버시 (ε, δ)",
        "한 사람의 기록이 결과에 미칠 수 있는 영향의 상한을 수학적으로 보장하는 기법과 "
        "그 세기를 나타내는 값입니다. 같은 조건이라면 ε과 δ가 작을수록 보호가 강합니다. "
        "'안전한 사람의 비율'이나 '재식별 확률'이 아닙니다.",
    ),
)


def _glossary_topics(document: Mapping[str, object]) -> set[str]:
    topics = {"always"}
    evaluation = _evaluation_of(document)
    summary = _summary_values(document)
    if summary["median_excess"] is not None:
        topics.add("excess")
    for column in _items(evaluation.get("columns")):
        metric = str(_mapping(column).get("metric", "")).upper()
        if "KS" in metric:
            topics.add("ks")
        if "TVD" in metric:
            topics.add("tvd")
    advanced = _mapping(evaluation.get("advanced"))
    if _mapping(advanced.get("c2st")):
        topics.add("c2st")
    if _mapping(advanced.get("downstream_utility")).get("applicable") is True:
        topics.add("downstream")
    empirical = _mapping(advanced.get("empirical_privacy"))
    if _mapping(empirical.get("gower")):
        topics.add("gower")
    if _mapping(empirical.get("anonymeter")):
        topics.add("anonymeter")
    if document.get("mode") == "differential_privacy":
        topics.add("dp")
    return topics


def _key_metric_table(document: Mapping[str, object]) -> Table:
    values = _summary_values(document)
    rows = [
        (
            "요청한 행 수",
            f"{_count(values['requested_rows'])}행",
            "사용자가 만들어 달라고 요청한 재현자료의 크기입니다.",
        ),
        (
            "실제 생성한 행 수",
            f"{_count(values['actual_rows'])}행",
            "요청한 행 수와 같아야 정상입니다.",
        ),
        (
            "강제 규칙 위반",
            f"{_count(values['hard_rule_violations'])}건",
            "0건이어야 정상입니다. 한 건이라도 있으면 사용 전에 원인을 확인해야 합니다.",
        ),
    ]
    if any(values[key] is not None for key in ("median_excess", "p95_excess", "max_excess")):
        rows.extend(
            [
                (
                    "분포 차이 중앙값",
                    _distance(values["median_excess"]),
                    "절반의 열이 이 값보다 잘 재현되었습니다. 0에 가까울수록 좋습니다.",
                ),
                (
                    "분포 차이 95백분위",
                    _distance(values["p95_excess"]),
                    "성적이 나쁜 쪽 5% 열이 이 값보다 차이가 컸습니다.",
                ),
                (
                    "분포 차이 최댓값",
                    _distance(values["max_excess"]),
                    "가장 재현이 어려웠던 열의 차이입니다.",
                ),
            ]
        )
    ledger = _mapping(document.get("ledger"))
    if ledger:
        epsilon = ledger.get("epsilon", ledger.get("epsilon_model", "확인 불가"))
        rows.append(
            (
                "차등프라이버시 ε / δ",
                f"ε={epsilon} / δ={ledger.get('delta', '확인 불가')}",
                "한 사람의 기록이 결과에 미치는 영향의 수학적 상한입니다.",
            )
        )
        rows.append(
            (
                "보호 단위 / 인접성",
                f"{ledger.get('privacy_unit', '확인 불가')} / "
                f"{ledger.get('adjacency', '확인 불가')}",
                "무엇을 한 사람으로 보고 보호했는지를 나타냅니다.",
            )
        )
    return Table(("항목", "값", "어떻게 읽나요"), tuple(rows), (5, 5, 12))


def _column_table(document: Mapping[str, object]) -> Table | None:
    evaluation = _evaluation_of(document)
    ranked: list[tuple[float, Mapping[str, object]]] = []
    for column in _items(evaluation.get("columns")):
        mapping = _mapping(column)
        excess = _number(mapping.get("baseline_excess"))
        if excess is None or not str(mapping.get("name", "")):
            continue
        ranked.append((excess, mapping))
    if not ranked:
        return None
    ranked.sort(key=lambda item: item[0], reverse=True)
    rows = tuple(
        (
            str(mapping.get("name", "")),
            str(mapping.get("metric") or "—"),
            _distance(mapping.get("distance")),
            _distance(excess),
        )
        for excess, mapping in ranked[:10]
    )
    return Table(("열 이름", "사용한 지표", "원본과의 거리", "기준선 초과"), rows, (7, 4, 5, 5))


def _paragraphs(section: Mapping[str, object]) -> list[Block]:
    return [Paragraph(str(text)) for text in _items(section.get("paragraphs")) if str(text)]


def _artifact_table(document: Mapping[str, object]) -> Table | None:
    rows: list[tuple[str, str, str]] = []
    for artifact in _items(document.get("artifacts")):
        mapping = _mapping(artifact)
        if not mapping.get("kind"):
            continue
        size = _number(mapping.get("size_bytes"))
        rows.append(
            (
                str(mapping.get("kind")),
                "내려받기 가능" if mapping.get("downloadable") else "내부 보관",
                "확인 불가" if size is None else f"{int(size):,} 바이트",
            )
        )
    if not rows:
        return None
    return Table(("산출물 종류", "제공 여부", "크기"), tuple(rows), (8, 4, 4))


def _document_blocks(document: Mapping[str, object]) -> tuple[Block, ...]:
    kind: ReportKind = document["report_kind"]  # type: ignore[assignment]
    values = _summary_values(document)
    structure_verdict, structure_text = _structure_verdict(values)
    band, band_text = _fidelity_band(values["median_excess"])
    executive = _mapping(document.get("executive_summary"))
    release = kind == "dp_release"

    blocks: list[Block] = [
        Paragraph(_PLAIN_TITLES[kind], "title"),
        Paragraph(f"작업 번호 {document.get('job_id', '확인 불가')}", "subtitle"),
        Paragraph(_INTRO[kind]),
        Paragraph(f"※ 공개 경계 안내: {document.get('privacy_notice', '')}", "caption"),
        Paragraph("1. 한눈에 보는 결론", "heading"),
    ]
    conclusion = str(executive.get("overall_conclusion", "")).strip()
    if conclusion:
        blocks.append(Paragraph(conclusion))
    blocks.append(Paragraph(f"구조 검증 판정: {structure_verdict}. {structure_text}"))
    blocks.append(Paragraph(f"분포 재현 판정: {band}. {band_text}"))

    blocks.append(Paragraph("2. 핵심 지표", "heading"))
    blocks.append(_key_metric_table(document))
    blocks.append(Paragraph("표 1. 이 결과를 판단하는 데 가장 먼저 보는 값들", "caption"))

    quality = _mapping(executive.get("quality"))
    if quality:
        blocks.append(Paragraph("3. 재현 품질 자세히 보기", "heading"))
        blocks.extend(_paragraphs(quality))

    columns = None if release else _column_table(document)
    if columns is not None:
        blocks.append(Paragraph("4. 먼저 확인할 열", "heading"))
        blocks.append(
            Paragraph(
                "원본과의 차이가 컸던 순서대로 최대 10개 열을 정리했습니다. 이 열들을 "
                "실제 분석에 쓸 예정이라면 원본과 재현자료의 분포를 눈으로 한 번 더 "
                "비교하는 것이 좋습니다."
            )
        )
        blocks.append(columns)
        blocks.append(Paragraph("표 2. 기준선 초과가 큰 열", "caption"))

    privacy = _mapping(executive.get("privacy"))
    if privacy:
        blocks.append(Paragraph("5. 개인정보 보호", "heading"))
        blocks.extend(_paragraphs(privacy))

    limitations = [str(item) for item in _items(executive.get("limitations")) if str(item)]
    limitations.extend(str(item) for item in _items(document.get("limitations")) if str(item))
    if limitations:
        blocks.append(Paragraph("6. 해석할 때 주의할 점", "heading"))
        blocks.extend(Paragraph(item, "bullet") for item in dict.fromkeys(limitations))

    topics = _glossary_topics(document)
    entries = tuple(
        (term, description) for topic, term, description in _GLOSSARY if topic in topics
    )
    if entries:
        blocks.append(Paragraph("7. 용어 해설", "heading"))
        blocks.append(Table(("용어", "쉬운 설명"), entries, (4, 11)))
        blocks.append(Paragraph("표 3. 이 보고서에 나오는 용어", "caption"))

    blocks.append(Paragraph("8. 확인 정보", "heading"))
    artifacts = _artifact_table(document)
    if artifacts is not None:
        blocks.append(artifacts)
        blocks.append(Paragraph("표 4. 이 작업이 만든 산출물", "caption"))
    blocks.append(
        Paragraph(
            "이 문서의 모든 수치는 함께 배포되는 기계 판독용 보고서(JSON)와 같은 값을 "
            "옮긴 것입니다. 정밀한 검증이나 시스템 연동에는 JSON 보고서를 사용하십시오.",
            "caption",
        )
    )
    return tuple(blocks)


def build_plain_language_report(report: BuiltReport) -> PlainLanguageReport | None:
    """Project a built report into its plain-language companion.

    Returns ``None`` for report kinds that have no lay-reader companion, so the
    caller can publish unconditionally without special-casing each mode.
    """

    if report.report_kind not in _ARTIFACT_KINDS:
        return None
    artifact_kind, filename = _ARTIFACT_KINDS[report.report_kind]
    return PlainLanguageReport(
        report_kind=report.report_kind,
        title=_PLAIN_TITLES[report.report_kind],
        blocks=_document_blocks(report.document),
        safety=report.safety,
        artifact_kind=artifact_kind,
        filename=filename,
    )


__all__ = ["PlainLanguageReport", "build_plain_language_report"]
