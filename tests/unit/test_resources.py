from decimal import Decimal

import pytest

from sts.storage.resources import (
    GIB,
    MIB,
    ArtifactComponent,
    DiskEstimationInput,
    MemoryProjection,
    ResourceAdmissionError,
    ResourceErrorCode,
    ResourceProfileName,
    admit_disk,
    admit_memory,
    estimate_disk,
    estimate_dp_state,
    l40s_lease,
    M4_LEASE,
)


def test_capacity_estimator_v1_exact_integer_ceilings() -> None:
    estimate = estimate_disk(
        DiskEstimationInput(
            source_bytes=101,
            measured_raw_ratio=Decimal("0.5"),
            rows=3,
            measured_normalized_bytes_per_row=Decimal("10.1"),
            output_rows=7,
            measured_synth_parquet_bytes_per_row=Decimal("4.2"),
            measured_csv_bytes_per_row=Decimal("8.1"),
            model_workspace_estimate_bytes=17,
            duckdb_memory_limit_bytes=4 * GIB,
            include_csv=True,
            include_parquet_zip64=True,
        )
    )

    assert estimate.capacity_estimator_version == 1
    assert estimate.raw_parquet_estimate_bytes == 66
    assert estimate.normalized_estimate_bytes == 40
    assert estimate.generated_parquet_estimate_bytes == 45
    assert estimate.csv_estimate_bytes == 86
    assert estimate.parquet_zip64_estimate_bytes == 46
    assert estimate.duckdb_spill_reserve_bytes == 8 * GIB
    assert estimate.atomic_reserve_bytes == 8 * GIB
    assert estimate.required_additional_free_bytes == 30 * GIB + 300


def test_existing_immutable_artifacts_are_not_counted_again() -> None:
    base = DiskEstimationInput(
        source_bytes=1_000,
        measured_raw_ratio=Decimal("1"),
        rows=100,
        measured_normalized_bytes_per_row=Decimal("2"),
        output_rows=10,
        measured_synth_parquet_bytes_per_row=Decimal("3"),
        model_workspace_estimate_bytes=50,
        duckdb_memory_limit_bytes=8 * GIB,
    )
    full = estimate_disk(base)
    existing = estimate_disk(
        base.model_copy(
            update={
                "existing_immutable_artifacts": frozenset(
                    {
                        ArtifactComponent.RAW_PARQUET,
                        ArtifactComponent.NORMALIZED_PARQUET,
                    }
                )
            }
        )
    )
    assert (
        full.required_additional_free_bytes - existing.required_additional_free_bytes
        == full.raw_parquet_estimate_bytes + full.normalized_estimate_bytes
    )
    assert existing.atomic_reserve_bytes == 16 * GIB


def test_disk_admission_rejects_when_short_by_exactly_one_byte() -> None:
    estimate = estimate_disk(
        DiskEstimationInput(
            source_bytes=0,
            measured_raw_ratio=Decimal("0"),
            rows=0,
            measured_normalized_bytes_per_row=Decimal("0"),
            output_rows=0,
            measured_synth_parquet_bytes_per_row=Decimal("0"),
            model_workspace_estimate_bytes=0,
            duckdb_memory_limit_bytes=1,
        )
    )
    required = estimate.required_additional_free_bytes
    assert admit_disk(estimate, required) is estimate

    with pytest.raises(ResourceAdmissionError) as raised:
        admit_disk(estimate, required - 1)

    error = raised.value
    assert error.status_code == 507
    assert error.code is ResourceErrorCode.DISK_QUOTA_EXCEEDED
    assert error.required_bytes == required
    assert error.available_bytes == required - 1
    assert error.as_problem()["status"] == 507


def test_m4_lease_and_memory_projection_boundaries() -> None:
    assert M4_LEASE.profile is ResourceProfileName.M4
    assert M4_LEASE.duckdb_memory_limit_bytes == 8 * GIB
    assert M4_LEASE.control_plane_browser_target_bytes == 4 * GIB
    assert M4_LEASE.worker_rss_hard_bytes == 24 * GIB
    assert M4_LEASE.process_tree_rss_hard_bytes == 32 * GIB
    assert M4_LEASE.utility_training_default_max_rows == 250_000
    assert M4_LEASE.utility_generation_processes == 1
    assert M4_LEASE.dp_projected_state_hard_bytes == 512 * MIB

    at_limit = MemoryProjection(
        worker_rss_bytes=24 * GIB,
        process_tree_rss_bytes=32 * GIB,
    )
    assert admit_memory(at_limit, M4_LEASE) is at_limit

    with pytest.raises(ResourceAdmissionError) as raised:
        admit_memory(
            MemoryProjection(
                worker_rss_bytes=24 * GIB + 1,
                process_tree_rss_bytes=32 * GIB,
            ),
            M4_LEASE,
        )
    assert raised.value.status_code == 507
    assert raised.value.code is ResourceErrorCode.RESOURCE_LIMIT


def test_l40s_host_and_vram_leases_are_fail_closed() -> None:
    lease = l40s_lease(256 * GIB, generation_processes=4)
    assert lease.host_ram_lease_bytes == (256 * GIB * 80) // 100
    assert lease.utility_training_default_max_rows == 2_000_000
    assert lease.utility_generation_processes == 4
    assert lease.gpu_allocated_reserved_hard_bytes_exclusive == 40 * GIB

    accepted = MemoryProjection(
        worker_rss_bytes=lease.worker_rss_hard_bytes,
        process_tree_rss_bytes=lease.process_tree_rss_hard_bytes,
        gpu_allocated_reserved_bytes=40 * GIB - 1,
    )
    admit_memory(accepted, lease)
    with pytest.raises(ResourceAdmissionError):
        admit_memory(
            accepted.model_copy(update={"gpu_allocated_reserved_bytes": 40 * GIB}),
            lease,
        )


def test_dp_state_estimator_uses_largest_d_minus_one_pair_products() -> None:
    estimate = estimate_dp_state([2, 3, 4])
    assert estimate.modeled_columns == 3
    assert estimate.one_way_cells == 9
    assert estimate.largest_pair_cells == 12
    assert estimate.selected_pair_cells == 20
    assert estimate.estimated_state_bytes == 8 * (9 + 20) * 8

    with pytest.raises(ResourceAdmissionError) as too_many_columns:
        estimate_dp_state([2] * 33)
    assert too_many_columns.value.code is ResourceErrorCode.RESOURCE_LIMIT

    with pytest.raises(ResourceAdmissionError):
        estimate_dp_state([257])
