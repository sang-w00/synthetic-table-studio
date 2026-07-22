from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

import sts.jobs.seeds as seed_planning
from sts.domain import DomainError, ErrorCode
from sts.jobs.protocol import SnapshotFile
from sts.jobs.seeds import plan_generation_seeds
from sts.jobs.utility import (
    CANDIDATE_TARGET_BYTES,
    CheckpointCompatibility,
    admit_training_sample,
    assert_checkpoint_compatible,
    bounded_priority_sample,
    create_argn_fit_request,
    create_argn_generate_request,
    load_argn_feature_availability,
    partition_counts,
    register_hmac_partition_sql,
    require_argn_feature_configuration,
    rows_per_candidate,
)
from sts.storage.atomic import sha256_file

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PARTITION_KEY = bytes.fromhex("bdf3173b45156ef1aa91448e3aebc7fa" * 2)
SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64


@pytest.fixture
def normalized_parquet(tmp_path: Path) -> Path:
    rows = 10_000
    path = tmp_path / "datasets" / "fixture" / "normalized.parquet"
    path.parent.mkdir(parents=True)
    table = pa.table(
        {
            "__sts_row_id": pa.array(range(rows), type=pa.int64()),
            "segment": pa.array(
                ["rare" if row % 10 == 0 else "common" for row in range(rows)],
                type=pa.string(),
            ),
            "region": pa.array(
                [None if row % 17 == 0 else f"r{row % 3}" for row in range(rows)],
                type=pa.string(),
            ),
            "value": pa.array(range(rows), type=pa.int64()),
            "note": pa.array(
                [f"record-{row:05d}" for row in range(rows)], type=pa.string()
            ),
        }
    )
    pq.write_table(table, path, compression="zstd")
    return path


def _snapshot(workspace: Path, path: Path) -> SnapshotFile:
    digest, size = sha256_file(path)
    return SnapshotFile(
        path=path.relative_to(workspace).as_posix(),
        sha256=digest,
        size_bytes=size,
    )


def _compatibility(**changes: str) -> CheckpointCompatibility:
    values = {
        "source_manifest_sha256": SHA_A,
        "schema_sha256": SHA_B,
        "rules_sha256": SHA_C,
        "engine_sha256": SHA_D,
    }
    values.update(changes)
    return CheckpointCompatibility(**values)


def test_hmac_partition_sql_is_deterministic_disjoint_and_complete(
    normalized_parquet: Path,
) -> None:
    with duckdb.connect() as connection:
        partition = register_hmac_partition_sql(
            connection,
            key=PARTITION_KEY,
            normalized_parquet=normalized_parquet,
        )
        first_counts = partition_counts(
            connection,
            normalized_parquet=normalized_parquet,
            partition=partition,
        )
        train_ids = {
            row[0]
            for row in connection.execute(
                f"SELECT __sts_row_id FROM {partition.source_sql} "
                f"WHERE {partition.train_predicate()}"
            ).fetchall()
        }
        holdout_ids = {
            row[0]
            for row in connection.execute(
                f"SELECT __sts_row_id FROM {partition.source_sql} "
                f"WHERE {partition.holdout_predicate()}"
            ).fetchall()
        }

    with duckdb.connect() as second_connection:
        second_partition = register_hmac_partition_sql(
            second_connection,
            key=PARTITION_KEY,
            normalized_parquet=normalized_parquet,
        )
        second_train_ids = {
            row[0]
            for row in second_connection.execute(
                f"SELECT __sts_row_id FROM {second_partition.source_sql} "
                f"WHERE {second_partition.train_predicate()}"
            ).fetchall()
        }

    assert train_ids == second_train_ids
    assert train_ids.isdisjoint(holdout_ids)
    assert train_ids | holdout_ids == set(range(10_000))
    assert first_counts.train_rows == len(train_ids)
    assert first_counts.holdout_rows == len(holdout_ids)
    assert 0.78 <= first_counts.train_rows / first_counts.total_rows <= 0.82
    assert partition.report_projection() == second_partition.report_projection()
    serialized_projection = json.dumps(partition.report_projection(), sort_keys=True)
    assert PARTITION_KEY.hex() not in serialized_projection
    assert "__sts_row_id" not in serialized_projection


def test_priority_sample_is_exact_bounded_deterministic_and_train_only(
    normalized_parquet: Path,
) -> None:
    with duckdb.connect() as connection:
        first = bounded_priority_sample(
            connection,
            normalized_parquet=normalized_parquet,
            partition_key=PARTITION_KEY,
            max_rows=777,
        )
        partition = register_hmac_partition_sql(
            connection,
            key=PARTITION_KEY,
            normalized_parquet=normalized_parquet,
        )
        train_values = {
            row[0]
            for row in connection.execute(
                f"SELECT value FROM {partition.source_sql} "
                f"WHERE {partition.train_predicate()}"
            ).fetchall()
        }
    with duckdb.connect() as connection:
        repeated = bounded_priority_sample(
            connection,
            normalized_parquet=normalized_parquet,
            partition_key=PARTITION_KEY,
            max_rows=777,
        )

    assert first.sampled_rows == 777
    assert first.table.equals(repeated.table)
    assert "__sts_row_id" not in first.table.column_names
    assert set(first.table["value"].to_pylist()) <= train_values


def test_two_column_stratification_records_exact_hamilton_allocation(
    normalized_parquet: Path,
) -> None:
    with duckdb.connect() as connection:
        sample = bounded_priority_sample(
            connection,
            normalized_parquet=normalized_parquet,
            partition_key=PARTITION_KEY,
            max_rows=997,
            stratify_by=("segment", "region"),
        )

    actual = Counter(
        zip(sample.table["segment"].to_pylist(), sample.table["region"].to_pylist())
    )
    assert sample.sampled_rows == 997
    assert len(sample.allocation) == 8
    assert sum(item.sampled_rows for item in sample.allocation) == 997
    for item in sample.allocation:
        assert actual[item.values] == item.sampled_rows
        assert item.sampled_rows <= item.population_rows
    manifest = sample.allocation_manifest(("segment", "region"))
    assert manifest["sampled_rows"] == 997
    assert manifest["stratify_by"] == ["segment", "region"]

    with duckdb.connect() as connection, pytest.raises(DomainError) as failure:
        bounded_priority_sample(
            connection,
            normalized_parquet=normalized_parquet,
            partition_key=PARTITION_KEY,
            max_rows=10,
            stratify_by=("segment", "region", "value"),
        )
    assert failure.value.code is ErrorCode.SCHEMA_INVALID


def test_memory_lease_rejects_before_any_pandas_materialization(
    normalized_parquet: Path,
) -> None:
    with duckdb.connect() as connection:
        sample = bounded_priority_sample(
            connection,
            normalized_parquet=normalized_parquet,
            partition_key=PARTITION_KEY,
            max_rows=500,
        )

    pandas_before = sys.modules.get("pandas")
    with pytest.raises(DomainError) as failure:
        admit_training_sample(sample.table, worker_lease_bytes=1)
    assert failure.value.code is ErrorCode.RESOURCE_LIMIT
    assert sys.modules.get("pandas") is pandas_before
    estimate = admit_training_sample(sample.table, worker_lease_bytes=10_000_000)
    assert estimate.admitted
    assert estimate.required_lease_bytes == pytest.approx(
        max(estimate.arrow_deep_bytes, estimate.pandas_deep_bytes_estimate) * 1.5,
        abs=1,
    )


def test_fit_request_contains_only_bounded_sample_and_no_row_id_or_holdout(
    normalized_parquet: Path,
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    sample_path = workspace / "jobs" / "job-1" / "attempt-1" / "bounded.parquet"
    sample_path.parent.mkdir(parents=True)
    with duckdb.connect() as connection:
        sample = bounded_priority_sample(
            connection,
            normalized_parquet=normalized_parquet,
            partition_key=PARTITION_KEY,
            max_rows=250,
        )
    pq.write_table(sample.table, sample_path)

    request = create_argn_fit_request(
        workspace_root=workspace,
        request_id="request-1",
        job_id="job-1",
        attempt=1,
        cancellation_path="jobs/job-1/attempt-1/cancel",
        bounded_training_sample=_snapshot(workspace, sample_path),
        checkpoint_path="jobs/job-1/attempt-1/checkpoint",
        compatibility=_compatibility(),
        max_epochs=2,
        max_minutes=10,
        model_size="MOSTLY_AI/Small",
        device="mps",
        split_seed=1234,
        worker_lease_bytes=24 * 1024**3,
    )

    assert set(request.manifest_snapshot.files) == {"bounded_training_sample"}
    assert request.limits["argn"]["deterministic_split"] == {
        "callable_fraction": 0.9,
        "fallback_fraction": 0.9,
        "seed": 1234,
    }
    payload = request.model_dump_json()
    assert "holdout" not in payload.lower()
    assert PARTITION_KEY.hex() not in payload
    assert "__sts_row_id" not in pq.read_schema(sample_path).names
    with pytest.raises(ValidationError):
        request.operation = "generate"


def test_fit_request_rejects_a_sample_that_still_contains_internal_row_ids(
    normalized_parquet: Path,
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    bad_sample = workspace / "jobs" / "job-1" / "attempt-1" / "bad.parquet"
    bad_sample.parent.mkdir(parents=True)
    pq.write_table(pq.read_table(normalized_parquet).slice(0, 10), bad_sample)

    with pytest.raises(DomainError) as failure:
        create_argn_fit_request(
            workspace_root=workspace,
            request_id="request-1",
            job_id="job-1",
            attempt=1,
            cancellation_path="jobs/job-1/attempt-1/cancel",
            bounded_training_sample=_snapshot(workspace, bad_sample),
            checkpoint_path="jobs/job-1/attempt-1/checkpoint",
            compatibility=_compatibility(),
            max_epochs=2,
            max_minutes=10,
            model_size="MOSTLY_AI/Small",
            device="cpu",
            split_seed=1234,
            worker_lease_bytes=1024,
        )
    assert failure.value.code is ErrorCode.SCHEMA_INVALID


def test_checkpoint_compatibility_mismatch_blocks_generation(tmp_path: Path) -> None:
    expected = _compatibility()
    actual = _compatibility(rules_sha256="e" * 64)
    with pytest.raises(DomainError) as failure:
        assert_checkpoint_compatible(actual, expected)
    assert failure.value.code is ErrorCode.BACKEND_INCOMPATIBLE
    assert set(failure.value.problem.context["mismatches"]) == {"rules_sha256"}

    workspace = tmp_path / "workspace"
    checkpoint_file = (
        workspace / "jobs" / "job-1" / "attempt-1" / "checkpoint" / "model.bin"
    )
    checkpoint_file.parent.mkdir(parents=True)
    checkpoint_file.write_bytes(b"checkpoint")
    candidate = plan_generation_seeds(
        master_seed=7,
        output_rows=10_000,
        rows_per_candidate=10_000,
        process_count=1,
    ).candidates[0]
    with pytest.raises(DomainError):
        create_argn_generate_request(
            workspace_root=workspace,
            request_id="request-generate",
            job_id="job-1",
            attempt=1,
            cancellation_path="jobs/job-1/attempt-1/cancel",
            checkpoint_files={"model": _snapshot(workspace, checkpoint_file)},
            checkpoint_path="jobs/job-1/attempt-1/checkpoint",
            candidate_output_path="jobs/job-1/attempt-1/candidates/part-00000.parquet",
            candidate=candidate,
            seed_count=1,
            batch_size=1_000,
            device="cpu",
            process_count=1,
            actual_compatibility=actual,
            expected_compatibility=expected,
            worker_lease_bytes=1024**3,
        )

    request = create_argn_generate_request(
        workspace_root=workspace,
        request_id="request-generate",
        job_id="job-1",
        attempt=1,
        cancellation_path="jobs/job-1/attempt-1/cancel",
        checkpoint_files={"model": _snapshot(workspace, checkpoint_file)},
        checkpoint_path="jobs/job-1/attempt-1/checkpoint",
        candidate_output_path="jobs/job-1/attempt-1/candidates/part-00000.parquet",
        candidate=candidate,
        seed_count=1,
        batch_size=1_000,
        device="cpu",
        process_count=1,
        actual_compatibility=expected,
        expected_compatibility=expected,
        worker_lease_bytes=1024**3,
    )
    assert request.operation == "generate"
    assert set(request.manifest_snapshot.files) == {"model"}
    assert request.limits["argn"]["engine_seed"] == candidate.engine_seed
    assert request.limits["argn"]["candidate_rows"] == 10_000
    assert "holdout" not in request.model_dump_json().lower()


def test_candidate_rows_formula_clamps_and_uses_binary_128_mib() -> None:
    assert rows_per_candidate(1) == 250_000
    assert rows_per_candidate(CANDIDATE_TARGET_BYTES / 131_072) == 131_072
    assert rows_per_candidate(CANDIDATE_TARGET_BYTES) == 10_000
    with pytest.raises(ValueError):
        rows_per_candidate(0)


def test_hkdf_seed_plan_has_disjoint_seeds_contiguous_ranges_and_exact_sum() -> None:
    plan = plan_generation_seeds(
        master_seed=0x123456789ABCDEF,
        output_rows=1_000_003,
        rows_per_candidate=128_000,
        process_count=4,
    )
    repeated = plan_generation_seeds(
        master_seed=0x123456789ABCDEF,
        output_rows=1_000_003,
        rows_per_candidate=128_000,
        process_count=4,
    )

    assert plan == repeated
    assert len(plan.candidates) == 8
    assert sum(candidate.output_rows for candidate in plan.candidates) == 1_000_003
    assert sum(process.output_rows for process in plan.processes) == 1_000_003
    assert len({candidate.engine_seed for candidate in plan.candidates}) == 8
    assert all(
        0 <= candidate.engine_seed <= 0xFFFFFFFF for candidate in plan.candidates
    )
    assert [candidate.row_start for candidate in plan.candidates] == [
        0,
        128_000,
        256_000,
        384_000,
        512_000,
        640_000,
        768_000,
        896_000,
    ]


def test_seed_derivation_fails_closed_on_uint32_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        seed_planning,
        "derive_uint32_seed",
        lambda master_seed, *, purpose, index: 17,
    )
    with pytest.raises(seed_planning.SeedCollisionError):
        seed_planning.derive_disjoint_uint32_seeds(
            9,
            purpose="argn-generation-candidate",
            count=2,
        )


def test_authoritative_m4_probe_enables_mps_but_forces_single_process() -> None:
    availability = load_argn_feature_availability(
        PROJECT_ROOT / "probes" / "results" / "argn_contract.json"
    )

    assert availability.platform_machine == "arm64"
    assert availability.backend_enabled
    assert availability.bounded_generation
    assert availability.fresh_process_checkpoint_generation
    assert availability.deterministic_generation
    assert availability.mps_enabled
    assert not availability.multiprocess_clones_enabled
    assert availability.max_generation_processes == 1
    require_argn_feature_configuration(availability, device="mps", process_count=1)
    with pytest.raises(DomainError) as failure:
        require_argn_feature_configuration(availability, device="mps", process_count=2)
    assert failure.value.code is ErrorCode.BACKEND_INCOMPATIBLE
