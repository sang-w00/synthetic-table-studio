from __future__ import annotations

import hashlib
import json
import re
import zipfile
from datetime import UTC, date, datetime
from decimal import Decimal
from html.parser import HTMLParser
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sts.domain import (
    ColumnKind,
    ColumnRole,
    ColumnSchema,
    DatasetManifest,
    DatasetState,
    DomainError,
    ErrorCode,
    ManifestFile,
)
from sts.evaluation import canonical_content_sha256
from sts.export import (
    build_parquet_shard_manifest,
    create_parquet_zip64_store,
    export_csv_from_parquet,
    publish_export_artifact,
    scan_parquet,
    verify_parquet_csv_equivalence,
    verify_parquet_shard_manifest,
    write_parquet_shard_manifest,
)
from sts.reports import (
    ArtifactSafety,
    assert_dp_release_safe,
    build_curator_internal_report,
    build_dp_release_report,
    build_utility_primary_report,
    publish_report_artifacts,
)
from sts.storage import CatalogRepository, WorkspaceLayout
from sts.storage.repository import ArtifactScope


class _AssetParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.remote_attributes: list[tuple[str, str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for name, value in attrs:
            rendered = value or ""
            if name in {"src", "href", "xlink:href"} or re.search(
                r"(?:https?:)?//", rendered, re.IGNORECASE
            ):
                self.remote_attributes.append((tag, name, rendered))


def _columns() -> tuple[ColumnSchema, ...]:
    return (
        ColumnSchema(
            name="count",
            kind=ColumnKind.INTEGER,
            nullable=False,
            role=ColumnRole.MODEL,
        ),
        ColumnSchema(
            name="price",
            kind=ColumnKind.FIXED_DECIMAL,
            decimal_places=2,
            nullable=True,
            role=ColumnRole.MODEL,
        ),
        ColumnSchema(
            name="ratio",
            kind=ColumnKind.FLOAT,
            nullable=True,
            role=ColumnRole.MODEL,
        ),
        ColumnSchema(
            name="day",
            kind=ColumnKind.DATE,
            nullable=True,
            role=ColumnRole.MODEL,
        ),
        ColumnSchema(
            name="instant",
            kind=ColumnKind.DATETIME,
            timezone="UTC",
            nullable=True,
            role=ColumnRole.MODEL,
        ),
        ColumnSchema(
            name="enabled",
            kind=ColumnKind.BOOLEAN,
            nullable=True,
            role=ColumnRole.MODEL,
        ),
        ColumnSchema(
            name="category",
            kind=ColumnKind.CATEGORICAL,
            nullable=True,
            role=ColumnRole.MODEL,
        ),
        ColumnSchema(
            name="note",
            kind=ColumnKind.TEXT,
            nullable=True,
            role=ColumnRole.MODEL,
        ),
    )


def _write_shards(root: Path) -> tuple[Path, Path]:
    parquet_dir = root / "parquet"
    parquet_dir.mkdir()
    rows = {
        "count": pa.array([1, 2, 3, 4], type=pa.int64()),
        "price": pa.array(
            [Decimal("1.20"), None, Decimal("-0.05"), Decimal("99.99")],
            type=pa.decimal128(10, 2),
        ),
        "ratio": pa.array([0.5, -0.0, None, 9.25], type=pa.float64()),
        "day": pa.array(
            [date(1970, 1, 1), None, date(2026, 7, 23), date(1969, 12, 31)]
        ),
        "instant": pa.array(
            [
                datetime(1970, 1, 1, tzinfo=UTC),
                datetime(2026, 7, 23, 12, 1, 2, 3, tzinfo=UTC),
                None,
                datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
        "enabled": pa.array([True, False, None, True], type=pa.bool_()),
        "category": pa.array(["a", "b", None, ""], type=pa.string()),
        "note": pa.array(
            ["e\u0301", "<script>alert(1)</script>", "", None], type=pa.string()
        ),
    }
    table = pa.table(rows)
    paths = (parquet_dir / "part-000.parquet", parquet_dir / "part-001.parquet")
    pq.write_table(table.slice(0, 2), paths[0])
    pq.write_table(table.slice(2, 2), paths[1])
    return paths


def _catalog_job(tmp_path: Path) -> tuple[CatalogRepository, WorkspaceLayout, object]:
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
        columns=_columns(),
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
        "output_formats": ["parquet", "csv"],
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
    return (
        repository,
        layout,
        repository.create_job(request, idempotency_key="reports-exports"),
    )


def test_reports_escape_injection_are_self_contained_and_preserve_privacy_boundaries() -> (
    None
):
    injection = '<img src="https://attacker.invalid/pixel" onerror="alert(1)">'
    utility = build_utility_primary_report(
        job_id=uuid4(),
        title=f"Quality {injection}",
        evaluation={
            "column": injection,
            "baseline_excess": {"median": 0.1, "p95": 0.2},
        },
    )
    html = utility.html_bytes().decode()
    assert injection not in html
    assert "&lt;img src=&#34;https://attacker.invalid/pixel&#34;" in html
    assert "No formal privacy guarantee" in html
    assert "<style>" in html and "<svg" in html
    assert "url(" not in html.lower() and "@import" not in html.lower()
    parser = _AssetParser()
    parser.feed(html)
    assert parser.remote_attributes == []
    assert utility.safety.release_safe is False
    assert utility.safety.contains_private_source_information is True

    internal = build_curator_internal_report(
        job_id=uuid4(), diagnostics={"source_count": 12, "examples": [injection]}
    )
    assert internal.safety.downloadable is False
    assert internal.safety.release_safe is False
    assert internal.safety.contains_private_source_information is True
    assert injection not in internal.html_bytes().decode()


def test_dp_release_uses_positive_allowlists_and_recursively_excludes_private_fields() -> (
    None
):
    release = build_dp_release_report(
        job_id=uuid4(),
        ledger_projection={
            "adjacency": "add_remove_one_row",
            "privacy_unit": "row",
            "epsilon_model": "3",
            "delta": "0.000001",
            "mechanism": "MST",
            "wheel_sha256": "a" * 64,
            "release_count": 1,
            "source_count": 989_502,
            "private_domain": {"secret": ["x"]},
            "fit_timing": 99,
            "peak_rss": 123,
            "fit_seed": 42,
            "raw_dataset_digest": "never-release",
        },
        output_summary={
            "requested_rows": 4,
            "actual_rows": 4,
            "schema": [
                {
                    "name": "count",
                    "kind": "integer",
                    "nullable": False,
                    "role": "model",
                    "source_count": 10,
                }
            ],
            "hard_rule_violations": 0,
            "canonical_content_sha256": "b" * 64,
            "holdout_metrics": {"ks": 0.1},
            "attacks": {"dcr": 0.0},
            "examples": ["private"],
        },
        artifacts=[
            {
                "artifact_id": str(uuid4()),
                "kind": "synthetic_csv",
                "sha256": "c" * 64,
                "size_bytes": 10,
                "downloadable": True,
                "relative_path": "/private/source/path",
                "source_counts": 20,
            }
        ],
        limitations=["Row-level add/remove adjacency; this is not entity DP."],
    )
    assert_dp_release_safe(release.document)
    payload = json.loads(release.json_bytes())
    assert set(payload["ledger"]).isdisjoint(
        {
            "source_count",
            "private_domain",
            "fit_timing",
            "peak_rss",
            "fit_seed",
            "raw_dataset_digest",
        }
    )
    assert set(payload["output"]).isdisjoint({"holdout_metrics", "attacks", "examples"})
    assert "source_count" not in payload["output"]["schema"][0]
    assert "relative_path" not in payload["artifacts"][0]
    assert "source_counts" not in payload["artifacts"][0]
    assert "never-release" not in json.dumps(payload, sort_keys=True)
    assert payload["ledger"]["epsilon_model"] == "3"
    assert payload["output"]["actual_rows"] == 4
    assert payload["release_safe"] is True
    assert payload["contains_private_source_information"] is False
    assert release.safety.release_safe is True


def test_report_and_export_artifacts_use_explicit_catalog_safety_scopes(
    tmp_path: Path,
) -> None:
    repository, layout, job = _catalog_job(tmp_path)
    try:
        utility = build_utility_primary_report(
            job_id=job.job_id, evaluation={"ks": 0.2}
        )
        internal = build_curator_internal_report(
            job_id=job.job_id, diagnostics={"source_count": 4}
        )
        release = build_dp_release_report(
            job_id=job.job_id,
            ledger_projection={"epsilon_model": "3", "delta": "0.000001"},
            output_summary={"actual_rows": 4},
        )
        utility_manifests = publish_report_artifacts(
            repository, utility, job_id=job.job_id, attempt=1
        )
        publish_report_artifacts(repository, internal, job_id=job.job_id, attempt=1)
        release_manifests = publish_report_artifacts(
            repository, release, job_id=job.job_id, attempt=1
        )

        downloadable = repository.list_artifacts(
            job_id=job.job_id, scope=ArtifactScope.DOWNLOADABLE
        )
        dp_release = repository.list_artifacts(
            job_id=job.job_id, scope=ArtifactScope.DP_RELEASE
        )
        internal_scope = repository.list_artifacts(
            job_id=job.job_id, scope=ArtifactScope.INTERNAL
        )
        assert {item.artifact_id for item in downloadable} == {
            *(item.artifact_id for item in utility_manifests),
            *(item.artifact_id for item in release_manifests),
        }
        assert {item.artifact_id for item in dp_release} == {
            item.artifact_id for item in release_manifests
        }
        assert len(internal_scope) == 6
        for manifest in release_manifests:
            assert manifest.release_safe is True
            assert manifest.contains_private_source_information is False
            assert layout.resolve_relative(manifest.relative_path).is_file()
    finally:
        repository.close()


def test_parquet_manifest_csv_and_zip64_are_atomic_and_canonically_equivalent(
    tmp_path: Path,
) -> None:
    columns = _columns()
    shards = _write_shards(tmp_path)
    manifest = build_parquet_shard_manifest(
        shards,
        columns,
        relative_paths=("parquet/part-000.parquet", "parquet/part-001.parquet"),
    )
    assert manifest.row_count == 4
    assert [item.row_count for item in manifest.shards] == [2, 2]
    for shard, source in zip(manifest.shards, shards, strict=True):
        assert shard.sha256 == hashlib.sha256(source.read_bytes()).hexdigest()
        assert shard.size_bytes == source.stat().st_size
    verify_parquet_shard_manifest(manifest, root=tmp_path)
    evaluation_rows, evaluation_hash = canonical_content_sha256(shards, columns)
    assert evaluation_rows == manifest.row_count
    assert evaluation_hash == manifest.canonical_content_sha256

    manifest_file = write_parquet_shard_manifest(manifest, tmp_path / "manifest.json")
    assert (
        manifest_file.sha256
        == hashlib.sha256((tmp_path / "manifest.json").read_bytes()).hexdigest()
    )
    csv_file = export_csv_from_parquet(shards, tmp_path / "synthetic.csv", columns)
    parquet_scan, csv_scan = verify_parquet_csv_equivalence(
        shards,
        tmp_path / "synthetic.csv",
        columns,
        null_marker=csv_file.null_marker or "",
        expected_rows=4,
    )
    assert parquet_scan.canonical_content_sha256 == csv_scan.canonical_content_sha256
    assert csv_file.canonical_content_sha256 == manifest.canonical_content_sha256
    assert (
        csv_file.sha256
        == hashlib.sha256((tmp_path / "synthetic.csv").read_bytes()).hexdigest()
    )
    assert csv_file.sha256 not in {item.sha256 for item in manifest.shards}

    archive = create_parquet_zip64_store(
        shards,
        tmp_path / "synthetic-parquet.zip",
        archive_names=("part-000.parquet", "part-001.parquet"),
        manifest_path=tmp_path / "manifest.json",
        manifest=manifest,
    )
    assert (
        archive.sha256
        == hashlib.sha256((tmp_path / "synthetic-parquet.zip").read_bytes()).hexdigest()
    )
    with zipfile.ZipFile(tmp_path / "synthetic-parquet.zip") as zipped:
        assert zipped.namelist() == [
            "part-000.parquet",
            "part-001.parquet",
            "manifest.json",
        ]
        assert all(
            info.compress_type == zipfile.ZIP_STORED for info in zipped.infolist()
        )
        assert all(info.compress_size == info.file_size for info in zipped.infolist())
        assert all(info.extract_version >= 45 for info in zipped.infolist())
    assert not list(tmp_path.glob("*.part"))
    assert not list(tmp_path.glob(".*.part"))


def test_canonical_rescans_reject_mismatch_nonfinite_and_checksum_corruption(
    tmp_path: Path,
) -> None:
    columns = _columns()
    shards = _write_shards(tmp_path)
    csv_file = export_csv_from_parquet(shards, tmp_path / "synthetic.csv", columns)
    content = (tmp_path / "synthetic.csv").read_text()
    (tmp_path / "mutated.csv").write_text(content.replace('"4"', '"40"', 1))
    with pytest.raises(DomainError) as mismatch:
        verify_parquet_csv_equivalence(
            shards,
            tmp_path / "mutated.csv",
            columns,
            null_marker=csv_file.null_marker or "",
        )
    assert mismatch.value.code is ErrorCode.OUTPUT_INVALID

    nonfinite = tmp_path / "nonfinite.parquet"
    table = pq.read_table(shards[0]).set_column(
        2,
        "ratio",
        pa.array([float("nan"), 1.0], type=pa.float64()),
    )
    pq.write_table(table, nonfinite)
    with pytest.raises(DomainError) as invalid:
        scan_parquet((nonfinite,), columns)
    assert invalid.value.code is ErrorCode.OUTPUT_INVALID

    manifest = build_parquet_shard_manifest(
        shards,
        columns,
        relative_paths=("parquet/part-000.parquet", "parquet/part-001.parquet"),
    )
    shards[0].write_bytes(shards[0].read_bytes() + b"corruption")
    with pytest.raises(DomainError) as corrupt:
        verify_parquet_shard_manifest(manifest, root=tmp_path)
    assert corrupt.value.code is ErrorCode.CHECKSUM_MISMATCH


def test_export_registration_requires_explicit_safety_and_preserves_sha(
    tmp_path: Path,
) -> None:
    repository, _, job = _catalog_job(tmp_path)
    try:
        source = tmp_path / "synthetic.csv"
        payload = b"value\n1\n"
        source.write_bytes(payload)
        from sts.export import ExportedFile

        exported = ExportedFile(
            path=str(source),
            sha256=hashlib.sha256(payload).hexdigest(),
            size_bytes=len(payload),
            row_count=1,
            canonical_content_sha256="d" * 64,
            null_marker="__NULL__",
        )
        manifest = publish_export_artifact(
            repository,
            exported,
            kind="synthetic_csv",
            relative_path=f"jobs/{job.job_id}/attempt-1/synthetic.csv",
            job_id=job.job_id,
            attempt=1,
            safety=ArtifactSafety(
                downloadable=True,
                release_safe=False,
                contains_private_source_information=False,
            ),
        )
        assert manifest.sha256 == hashlib.sha256(payload).hexdigest()
        assert manifest.release_safe is False
        assert manifest.metadata["csv_null_marker"] == "__NULL__"
    finally:
        repository.close()
