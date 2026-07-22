from __future__ import annotations

import hashlib
import json
import runpy
import subprocess
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
VERIFY = PROJECT_ROOT / "scripts" / "verify"
RESULT_SCHEMA = PROJECT_ROOT / "benchmarks" / "result-schema.json"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _assert_result_schema(result: dict[str, object]) -> None:
    schema = json.loads(RESULT_SCHEMA.read_text(encoding="utf-8"))
    required = set(schema["required"])
    assert set(result) == required
    assert result["schema_version"] == "1.0"
    assert result["capacity_estimator_version"] == 1
    assert result["verification"] in schema["properties"]["verification"]["enum"]
    assert result["status"] in schema["properties"]["status"]["enum"]
    for field in ("started_at", "completed_at"):
        assert datetime.fromisoformat(str(result[field]).replace("Z", "+00:00")).tzinfo

    host = result["host"]
    assert isinstance(host, dict)
    assert set(host) == {"platform", "machine", "python", "duckdb"}
    assert all(isinstance(value, str) and value for value in host.values())

    resources = result["resources"]
    assert isinstance(resources, dict)
    assert resources["peak_process_tree_rss_bytes"] >= 0
    assert resources["rss_limit_bytes"] > 0
    assert resources["spill_bytes_observed"] >= 0
    assert isinstance(resources["spill_required"], bool)

    gates = result["gates"]
    assert isinstance(gates, list) and gates
    for gate in gates:
        assert set(gate).issubset({"name", "status", "observed", "expected", "message"})
        assert {"name", "status", "observed", "expected"}.issubset(gate)
        assert gate["status"] in {"passed", "failed", "unavailable", "skipped"}


def test_reduced_scale_pipeline_is_bounded_exact_and_fail_closed(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    results_dir = tmp_path / "benchmarks" / "results"
    completed = subprocess.run(
        [
            str(VERIFY),
            "scale-m4",
            "--rows",
            "10000",
            "--batch-rows",
            "2500",
            "--evaluation-rows",
            "2000",
            "--memory-limit",
            "128MB",
            "--spill-memory-limit",
            "32MB",
            "--rss-limit-bytes",
            str(1024**3),
            "--workspace",
            str(workspace),
            "--results-dir",
            str(results_dir),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == ""

    result_path = results_dir / "scale-m4.json"
    assert result_path.is_file()
    result = json.loads(result_path.read_text(encoding="utf-8"))
    _assert_result_schema(result)
    assert result["status"] == "passed"
    assert result["error"] is None
    assert result["configuration"]["rows"] == 10_000
    assert result["configuration"]["columns"] == 70
    assert result["configuration"]["batch_rows"] == 2_500
    assert result["configuration"]["duckdb_memory_limit"] == "128MB"
    assert result["configuration"]["spill_probe_memory_limit"] == "32MB"
    assert result["configuration"]["reduced_smoke"] is True

    gates = {gate["name"]: gate for gate in result["gates"]}
    required_gates = {
        "fixture_rows",
        "fixture_columns",
        "normalization_rows",
        "normalized_rules",
        "duckdb_spill",
        "cancel_no_partial_publish",
        "resume_from_normalized_boundary",
        "synthetic_rows",
        "synthetic_rules",
        "evaluation_samples",
        "canonical_content_equivalence",
        "report_remote_assets",
        "no_partial_files",
        "rss_limit",
    }
    assert required_gates.issubset(gates)
    assert all(gates[name]["status"] == "passed" for name in required_gates)
    assert gates["duckdb_spill"]["observed"]["peak_bytes"] > 0
    hashes = gates["canonical_content_equivalence"]["observed"]
    assert hashes["parquet_rows"] == hashes["csv_rows"] == 10_000
    assert hashes["parquet_sha256"] == hashes["csv_sha256"]
    assert len(hashes["parquet_sha256"]) == 64

    benchmark = result["engine_benchmark"]
    assert benchmark["status"] == "unavailable"
    assert benchmark["reason_code"] == "PINNED_COMPARATIVE_ENGINES_UNAVAILABLE"
    assert benchmark["implementations"]["argn"]["status"] == "unavailable"
    assert benchmark["implementations"]["argn"]["reason_code"] == (
        "LIGHTWEIGHT_ADAPTER_IS_NOT_ARGN"
    )
    for name in ("forestflow", "arf"):
        implementation = benchmark["implementations"][name]
        assert implementation == {
            "status": "unavailable",
            "pinned": False,
            "reason_code": "PINNED_IMPLEMENTATION_NOT_PRESENT",
        }
    assert benchmark["noninferiority"] == {
        "status": "not_evaluated",
        "passed": None,
        "reason_code": "THREE_ENGINE_THREE_SEED_RESULTS_UNAVAILABLE",
        "metrics": None,
    }

    for item in result["artifacts"]:
        path = workspace / item["path"]
        assert path.is_file()
        assert path.stat().st_size == item["size_bytes"]
        assert _sha256(path) == item["sha256"]
    assert not [
        path for path in workspace.rglob("*") if path.is_file() and ".part" in path.name
    ]


def test_sample_sum_drop_rows_record_union_and_overlap(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("STS_VERIFY_VENV", "1")
    verify = runpy.run_path(str(VERIFY), run_name="sts_verify_test")
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            CREATE TABLE fixture (
                REP_SEX_CODE VARCHAR,
                INDUSTRY_CODE VARCHAR,
                CLOSE_FLAG VARCHAR,
                CLOSE_YM VARCHAR,
                EMP_REGULAR_MALE BIGINT,
                EMP_REGULAR_FEMALE BIGINT,
                EMP_REGULAR_TOTAL BIGINT,
                EMP_TEMP_MALE BIGINT,
                EMP_TEMP_FEMALE BIGINT,
                EMP_TEMP_TOTAL BIGINT,
                EMP_ALL_TOTAL BIGINT
            )
            """
        )
        connection.executemany(
            "INSERT INTO fixture VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("1", "I", "N", None, 1, 2, 3, 3, 4, 7, 10),
                ("1", "I", "N", None, 1, 2, 9, 3, 4, 7, 16),
                ("2", "I", "Y", "202501", 1, 2, 3, 3, 4, 9, 12),
                ("2", "I", "Y", "202502", 1, 2, 3, 3, 4, 7, 99),
                ("9", "I", "N", None, 1, 2, 9, 3, 4, 9, 0),
                ("1", "I", "N", None, None, 2, None, 3, 4, 7, None),
                ("1", "I", "N", None, None, 2, 5, 3, 4, 7, 12),
                ("2", "I", "Y", "202503", 1, 2, 3, None, 4, None, None),
                ("2", "I", "Y", "202504", 1, 2, 3, None, 4, 5, 8),
                ("9", "I", "N", None, None, 2, None, 3, 4, 7, 9),
            ],
        )
        source = tmp_path / "normalized.parquet"
        filtered = tmp_path / "normalized-source-valid.parquet"
        verify["copy_query_atomic"](
            connection,
            "SELECT * FROM fixture",
            [],
            source,
            format_clause="FORMAT PARQUET, COMPRESSION ZSTD",
        )

        audit = verify["sample_rule_audit"](connection, source)
        assert audit["per_rule"]["employee_sum"] == 3
        assert audit["per_rule"]["temporary_employee_sum"] == 3
        assert audit["per_rule"]["overall_employee_sum"] == 3
        assert audit["drop_union_count"] == 7
        assert audit["drop_overlap_count"] == 1

        verify["filter_sample_drop_rows"](connection, source, filtered)
        filtered_audit = verify["sample_rule_audit"](connection, filtered)
        assert filtered_audit["rows"] == 3
        assert filtered_audit["drop_union_count"] == 0
        assert filtered_audit["drop_overlap_count"] == 0
        assert all(value == 0 for value in filtered_audit["per_rule"].values())
        assert not [path for path in tmp_path.rglob("*") if ".part" in path.name]
    finally:
        connection.close()


def test_residual_rejection_exact_target_and_exhaustion_cleanup(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("STS_VERIFY_VENV", "1")
    verify = runpy.run_path(str(VERIFY), run_name="sts_verify_rejection_test")
    connection = duckdb.connect()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    expected_columns = {
        column
        for column in verify["SAMPLE_COLUMNS"]
        if column
        not in {
            "Index",
            "REP_SEX_CODE",
            "INDUSTRY_CODE",
            "EMP_ALL_TOTAL",
            "EMP_REGULAR_TOTAL",
            "EMP_TEMP_TOTAL",
        }
    } | {verify["SAMPLE_STRUCTURAL_LATENT"]}

    def write_candidate(
        candidate_index: int,
        requested_rows: int,
        latent_values: list[str | None],
    ) -> Path:
        assert len(latent_values) == requested_rows
        table = f"latent_{candidate_index}_{len(list(workspace.glob('candidate-*.parquet')))}"
        connection.execute(f"CREATE TEMP TABLE {table} (i BIGINT, latent VARCHAR)")
        connection.executemany(
            f"INSERT INTO {table} VALUES (?, ?)",
            list(enumerate(latent_values)),
        )
        path = workspace / f"candidate-{candidate_index:05d}.parquet"
        verify["copy_query_atomic"](
            connection,
            f"""
            SELECT
              2020::BIGINT AS BASE_YEAR,
              1980::BIGINT AS BIRTH_YEAR,
              '202001'::VARCHAR AS REGISTER_YM,
              '202001'::VARCHAR AS OPEN_YM,
              NULL::VARCHAR AS CLOSE_YM,
              'N'::VARCHAR AS CLOSE_FLAG,
              '11'::VARCHAR AS REGION_CODE,
              '11000'::VARCHAR AS DISTRICT_CODE,
              1::BIGINT AS ACTIVITY_CODE,
              10::BIGINT AS AMOUNT_CODE,
              1::BIGINT AS EMP_REGULAR_MALE,
              2::BIGINT AS EMP_REGULAR_FEMALE,
              3::BIGINT AS EMP_TEMP_MALE,
              4::BIGINT AS EMP_TEMP_FEMALE,
              '100'::VARCHAR AS SUBCLASS_CODE,
              latent AS {verify["SAMPLE_STRUCTURAL_LATENT"]}
            FROM {table}
            ORDER BY i
            """,
            [],
            path,
            format_clause="FORMAT PARQUET, COMPRESSION ZSTD",
        )
        connection.execute(f"DROP TABLE {table}")
        return path

    success_batches = [
        ["1:I", None, "unknown", "2:I"],
        ["9:I", "unknown", None, "unknown"],
    ]

    def successful_generator(index: int, _start: int, requested: int) -> Path:
        return write_candidate(index, requested, success_batches[index])

    output = workspace / "synthetic-success.parquet"
    metrics = verify["coordinate_sample_residual_rejection"](
        connection,
        workspace=workspace,
        output_path=output,
        output_rows=3,
        candidate_batch_rows=4,
        expected_candidate_columns=expected_columns,
        generate_candidate=successful_generator,
    )
    assert metrics == {
        "requested_rows": 3,
        "accepted_rows": 3,
        "candidate_rows": 8,
        "candidate_shards": 2,
        "rejected_structural_rows": 5,
        "max_candidate_rows": 60,
    }
    assert (
        connection.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(output)]
        ).fetchone()[0]
        == 3
    )
    assert all(
        value == 0
        for value in verify["sample_rule_violations"](connection, output).values()
    )

    def exhausting_generator(index: int, _start: int, requested: int) -> Path:
        return write_candidate(index + 100, requested, [None, "unknown", None])

    exhausted_output = workspace / "synthetic-exhausted.parquet"
    with pytest.raises(
        verify["VerificationFailure"], match="RULE_FEASIBILITY_EXHAUSTED"
    ):
        verify["coordinate_sample_residual_rejection"](
            connection,
            workspace=workspace,
            output_path=exhausted_output,
            output_rows=3,
            candidate_batch_rows=3,
            expected_candidate_columns=expected_columns,
            generate_candidate=exhausting_generator,
            max_candidate_multiplier=1,
        )
    assert not exhausted_output.exists()
    assert not list(workspace.glob("candidate-*.parquet"))
    assert not [path for path in workspace.rglob("*") if ".part" in path.name]
    connection.close()


def test_verify_help_and_invalid_target() -> None:
    help_result = subprocess.run(
        [str(VERIFY), "--help"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert help_result.returncode == 0
    assert "sample-m4" in help_result.stdout
    assert "scale-m4" in help_result.stdout

    invalid = subprocess.run(
        [str(VERIFY), "not-a-target"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert invalid.returncode == 2
    assert "invalid choice" in invalid.stderr
