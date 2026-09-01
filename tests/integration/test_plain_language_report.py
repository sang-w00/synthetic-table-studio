from __future__ import annotations

import io
import zipfile
from uuid import uuid4
from xml.dom.minidom import parseString

import pytest

from sts.reports import (
    Paragraph,
    Table,
    build_dp_release_report,
    build_hwpx,
    build_plain_language_report,
    build_utility_primary_report,
    publish_plain_report_artifact,
)
from sts.domain import (
    ColumnKind,
    ColumnRole,
    ColumnSchema,
    DatasetManifest,
    DatasetState,
    ManifestFile,
)
from sts.reports.builders import build_curator_internal_report
from sts.storage import CatalogRepository, WorkspaceLayout
from sts.storage.repository import ArtifactScope


def _catalog_job(tmp_path):
    layout = WorkspaceLayout(tmp_path / "workspace")
    repository = CatalogRepository.open_workspace(layout)
    dataset_id = uuid4()
    manifest = DatasetManifest(
        dataset_id=dataset_id,
        source=ManifestFile(
            relative_path=f"datasets/{dataset_id}/source.csv",
            sha256="0" * 64,
            size_bytes=0,
        ),
        schema_version="schema-1",
        rules_version="rules-1",
        columns=(
            ColumnSchema(
                name="age",
                kind=ColumnKind.INTEGER,
                nullable=False,
                role=ColumnRole.MODEL,
            ),
        ),
    )
    dataset = repository.create_dataset(manifest, state=DatasetState.NORMALIZED)
    request = {
        "version": "1.0",
        "dataset_id": str(dataset_id),
        "dataset_manifest_sha": dataset.manifest_sha256,
        "schema_version": "schema-1",
        "rules_version": "rules-1",
        "mode": "utility",
        "synthesizer": "tabular_argn",
        "output_rows": 4,
        "output_formats": ["parquet"],
        "resource_profile": "m4-default",
        "evaluation_config_version": "1.0",
        "training": {
            "max_rows": 4,
            "max_epochs": 1,
            "max_minutes": 1,
            "model_size": "tiny",
            "device": "cpu",
        },
    }
    return repository, repository.create_job(request, idempotency_key="plain-report")


_XML_PARTS = (
    "version.xml",
    "META-INF/container.xml",
    "META-INF/manifest.xml",
    "Contents/content.hpf",
    "Contents/header.xml",
    "Contents/section0.xml",
    "settings.xml",
)


def _utility_report(**overrides: object):
    evaluation = {
        "exact": {
            "requested_rows": 2_000,
            "actual_rows": 2_000,
            "hard_rule_violations": 0,
        },
        "primary": {
            "baseline_excess": {
                "median": {"value": 0.012},
                "p95": {"value": 0.048},
                "maximum": {"value": 0.131},
            },
            "columns": [
                {
                    "name": "age",
                    "included_in_fidelity_aggregate": True,
                    "synthetic_distance": {"metric": "KS", "value": 0.052},
                    "baseline_excess": {"value": 0.012},
                    "synthetic_missingness_difference": {"value": 0.0},
                },
                {
                    "name": "region",
                    "included_in_fidelity_aggregate": True,
                    "synthetic_distance": {"metric": "TVD", "value": 0.180},
                    "baseline_excess": {"value": 0.131},
                    "synthetic_missingness_difference": {"value": 0.002},
                },
            ],
        },
        "advanced": {
            "c2st": {"synthetic_vs_untouched_holdout": {"linear": {"auroc": 0.53}}},
            "empirical_privacy": {"gower": {"applicable": True, "dcr": {}}},
        },
    }
    evaluation.update(overrides)
    return build_utility_primary_report(job_id=uuid4(), evaluation=evaluation)


def _document_text(blob: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        return archive.read("Contents/section0.xml").decode("utf-8")


def test_hwpx_package_layout_matches_the_owpml_container_contract() -> None:
    blob = build_hwpx(
        "표지",
        [
            Paragraph("제목", "title"),
            Paragraph("본문", "body"),
            Table(("가", "나"), (("1", "2"),), (1, 1)),
        ],
    )
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        names = archive.namelist()
        # Hangul reads the package type from an uncompressed first member.
        assert names[0] == "mimetype"
        assert archive.getinfo("mimetype").compress_type == zipfile.ZIP_STORED
        assert archive.read("mimetype") == b"application/hwp+zip"
        assert set(_XML_PARTS) <= set(names)
        for part in _XML_PARTS:
            parseString(archive.read(part))
        assert archive.testzip() is None
    # Identical input must publish an identical artifact hash.
    assert build_hwpx("표지", [Paragraph("가")]) == build_hwpx(
        "표지", [Paragraph("가")]
    )


def test_hwpx_escapes_markup_and_rejects_malformed_tables() -> None:
    blob = build_hwpx("표지", [Paragraph('<img src="x" onerror="alert(1)"> & 끝')])
    section = _document_text(blob)
    assert "<img" not in section
    assert "&lt;img src=&quot;x&quot;" in section
    assert "&amp; 끝" in section
    with pytest.raises(ValueError):
        Table(("가", "나"), (("1",),))
    with pytest.raises(ValueError):
        build_hwpx("   ", [Paragraph("가")])


def test_plain_language_report_states_a_verdict_and_defines_its_terms() -> None:
    plain = build_plain_language_report(_utility_report())
    assert plain is not None
    section = _document_text(plain.hwpx_bytes())
    assert "재현자료 품질 보고서" in section
    assert "구조 검증 판정: 정상" in section
    assert "분포 재현 판정: 차이가 매우 작음" in section
    # The column table lists the worst-scoring column first.
    table = section.split("먼저 확인할 열", 1)[1]
    assert table.index("<hp:t>region</hp:t>") < table.index("<hp:t>age</hp:t>")
    # The reading band must never be presented as a pass mark.
    assert "합격 기준은 존재하지 않습니다" in section
    # Only the terms the document actually uses are explained.
    assert "기준선 초과 (baseline-excess)" in section
    assert "KS 거리" in section and "TVD (총변동거리)" in section
    assert "차등프라이버시" not in section.split("용어 해설")[0]
    assert plain.safety.release_safe is False
    assert plain.safety.contains_private_source_information is True


def test_plain_language_report_flags_a_row_count_or_rule_failure() -> None:
    plain = build_plain_language_report(
        _utility_report(
            exact={
                "requested_rows": 2_000,
                "actual_rows": 1_998,
                "hard_rule_violations": 3,
            }
        )
    )
    assert plain is not None
    section = _document_text(plain.hwpx_bytes())
    assert "구조 검증 판정: 확인 필요" in section
    assert "사용하면 안 됩니다" in section


def test_dp_release_plain_report_stays_inside_the_public_boundary() -> None:
    release = build_dp_release_report(
        job_id=uuid4(),
        ledger_projection={
            "mechanism": "MST",
            "epsilon_model": "3",
            "delta": "0.000001",
            "privacy_unit": "row",
            "adjacency": "add_remove_one_row",
            "release_count": 2,
        },
        output_summary={
            "requested_rows": 1_000,
            "actual_rows": 1_000,
            "hard_rule_violations": 0,
        },
        limitations=("공개용 보고서는 원본 유사도를 포함하지 않습니다.",),
    )
    plain = build_plain_language_report(release)
    assert plain is not None
    assert plain.artifact_kind == "dp_release_report_hwpx"
    assert plain.safety.release_safe is True
    assert plain.safety.contains_private_source_information is False
    section = _document_text(plain.hwpx_bytes())
    assert "ε=3" in section and "δ=0.000001" in section
    assert "차등프라이버시 (ε, δ)" in section
    # A release document never carries the per-column comparison against the source.
    assert "먼저 확인할 열" not in section
    assert "기준선 초과" not in section
    assert "안전한 사람의 비율" in section


def test_internal_diagnostics_have_no_lay_reader_companion() -> None:
    internal = build_curator_internal_report(
        job_id=uuid4(), diagnostics={"source_count": 12}
    )
    assert build_plain_language_report(internal) is None


def test_published_plain_report_is_downloadable_with_inherited_safety(tmp_path) -> None:
    repository, job = _catalog_job(tmp_path)
    plain = build_plain_language_report(_utility_report())
    assert plain is not None
    manifest = publish_plain_report_artifact(
        repository, plain, job_id=job.job_id, attempt=job.attempt
    )
    assert manifest.kind == "primary_report_hwpx"
    assert manifest.relative_path.endswith("/reports/quality-report.hwpx")
    assert manifest.downloadable is True
    assert manifest.release_safe is False
    downloadable = repository.list_artifacts(
        job_id=job.job_id, scope=ArtifactScope.DOWNLOADABLE
    )
    assert any(item.kind == "primary_report_hwpx" for item in downloadable)
    release_scope = repository.list_artifacts(
        job_id=job.job_id, scope=ArtifactScope.DP_RELEASE
    )
    assert all(item.kind != "primary_report_hwpx" for item in release_scope)
