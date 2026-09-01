from __future__ import annotations

import json
import platform
import shutil
import subprocess
from collections.abc import Iterable
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from enum import StrEnum
from functools import lru_cache
from itertools import combinations
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

CAPACITY_ESTIMATOR_VERSION = 1
MIB = 1024**2
GIB = 1024**3


class HostResourceSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    platform_system: str = Field(min_length=1)
    platform_machine: str = Field(min_length=1)
    logical_cpu_count: Annotated[int, Field(gt=0)]
    total_memory_bytes: Annotated[int, Field(gt=0)]
    available_memory_bytes: Annotated[int, Field(gt=0)]
    disk_total_bytes: Annotated[int, Field(gt=0)]
    disk_free_bytes: Annotated[int, Field(ge=0)]
    gpu_backend: Literal["none", "mps", "cuda"] = "none"
    gpu_device_count: Annotated[int, Field(ge=0)] = 0
    gpu_name: str | None = None
    gpu_memory_total_bytes: Annotated[int, Field(gt=0)] | None = None


class WorkerBackendSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    mps_available: bool = False
    cuda_device_count: Annotated[int, Field(ge=0)] = 0


@lru_cache(maxsize=4)
def detect_worker_backends(interpreter: str | Path) -> WorkerBackendSnapshot:
    executable = Path(interpreter)
    if not executable.is_file():
        return WorkerBackendSnapshot()
    script = (
        "import json,torch;"
        "print(json.dumps({'mps_available':bool(torch.backends.mps.is_built() "
        "and torch.backends.mps.is_available()),"
        "'cuda_device_count':int(torch.cuda.device_count()) "
        "if torch.cuda.is_available() else 0}))"
    )
    try:
        completed = subprocess.run(
            [str(executable), "-c", script],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
        payload = json.loads(completed.stdout)
        if not isinstance(payload, dict):
            return WorkerBackendSnapshot()
        return WorkerBackendSnapshot.model_validate(payload)
    except (OSError, subprocess.SubprocessError, ValueError):
        return WorkerBackendSnapshot()


class RuntimeResourcePlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    resource_profile: str = Field(min_length=1)
    recommended_device: str = Field(min_length=1)
    worker_lease_bytes: Annotated[int, Field(gt=0)]
    utility_max_rows: Annotated[int, Field(gt=0)]
    duckdb_memory_limit_bytes: Annotated[int, Field(gt=0)]
    max_concurrent_jobs: Annotated[int, Field(gt=0)]
    disk_free_bytes: Annotated[int, Field(ge=0)]


def _nvidia_resources() -> tuple[int, str | None, int | None]:
    executable = shutil.which("nvidia-smi")
    if executable is None:
        return 0, None, None
    try:
        completed = subprocess.run(
            [
                executable,
                "--query-gpu=name,memory.total",
                "--format=csv,noheader,nounits",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.SubprocessError):
        return 0, None, None
    devices: list[tuple[str, int]] = []
    for line in completed.stdout.splitlines():
        name, separator, memory = line.rpartition(",")
        if not separator:
            continue
        try:
            memory_bytes = int(memory.strip()) * MIB
        except ValueError:
            continue
        if name.strip() and memory_bytes > 0:
            devices.append((name.strip(), memory_bytes))
    if not devices:
        return 0, None, None
    names = sorted({name for name, _ in devices})
    return len(devices), ", ".join(names), sum(memory for _, memory in devices)


def detect_host_resources(workspace: str | Path) -> HostResourceSnapshot:
    import psutil

    system = platform.system() or "unknown"
    machine = platform.machine() or "unknown"
    memory = psutil.virtual_memory()
    disk = shutil.disk_usage(Path(workspace))
    cpu_count = psutil.cpu_count(logical=True) or 1

    gpu_backend: Literal["none", "mps", "cuda"] = "none"
    gpu_count = 0
    gpu_name: str | None = None
    gpu_memory: int | None = None
    cuda_count, cuda_name, cuda_memory = _nvidia_resources()
    if cuda_count:
        gpu_backend = "cuda"
        gpu_count = cuda_count
        gpu_name = cuda_name
        gpu_memory = cuda_memory
    elif system == "Darwin" and machine in {"arm64", "aarch64"}:
        gpu_backend = "mps"
        gpu_count = 1
        gpu_name = "Apple Silicon GPU"

    return HostResourceSnapshot(
        platform_system=system,
        platform_machine=machine,
        logical_cpu_count=cpu_count,
        total_memory_bytes=int(memory.total),
        available_memory_bytes=max(1, int(memory.available)),
        disk_total_bytes=disk.total,
        disk_free_bytes=disk.free,
        gpu_backend=gpu_backend,
        gpu_device_count=gpu_count,
        gpu_name=gpu_name,
        gpu_memory_total_bytes=gpu_memory,
    )


def derive_runtime_resource_plan(
    snapshot: HostResourceSnapshot,
    *,
    mps_validated: bool = False,
    cuda_validated_device_count: int = 0,
) -> RuntimeResourcePlan:
    reserve = max(2 * GIB, snapshot.total_memory_bytes // 12)
    available_for_jobs = max(GIB, snapshot.available_memory_bytes - reserve)
    worker_lease = min(24 * GIB, available_for_jobs)
    if worker_lease >= 16 * GIB:
        utility_max_rows = 250_000
    elif worker_lease >= 8 * GIB:
        utility_max_rows = 100_000
    else:
        utility_max_rows = 50_000
    duckdb_memory = max(
        512 * MIB,
        min(8 * GIB, snapshot.available_memory_bytes // 8),
    )
    concurrency_by_memory = max(1, available_for_jobs // worker_lease)
    concurrency_by_cpu = max(1, snapshot.logical_cpu_count // 4)
    max_concurrent = min(4, concurrency_by_memory, concurrency_by_cpu)

    recommended_device = "cpu"
    if snapshot.gpu_backend == "mps" and mps_validated:
        recommended_device = "mps"
    elif (
        snapshot.gpu_backend == "cuda"
        and cuda_validated_device_count > 0
        and snapshot.gpu_device_count > 0
    ):
        recommended_device = "cuda:0"

    return RuntimeResourcePlan(
        resource_profile=f"auto_{recommended_device.split(':', 1)[0]}",
        recommended_device=recommended_device,
        worker_lease_bytes=worker_lease,
        utility_max_rows=utility_max_rows,
        duckdb_memory_limit_bytes=duckdb_memory,
        max_concurrent_jobs=max_concurrent,
        disk_free_bytes=snapshot.disk_free_bytes,
    )


class ResourceErrorCode(StrEnum):
    RESOURCE_LIMIT = "RESOURCE_LIMIT"
    DISK_QUOTA_EXCEEDED = "DISK_QUOTA_EXCEEDED"


class ResourceAdmissionError(RuntimeError):
    status_code = 507

    def __init__(
        self,
        code: ResourceErrorCode,
        detail: str,
        *,
        required_bytes: int | None = None,
        available_bytes: int | None = None,
    ) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.required_bytes = required_bytes
        self.available_bytes = available_bytes

    def as_problem(self) -> dict[str, object]:
        problem: dict[str, object] = {
            "type": f"urn:sts:error:{self.code.value}",
            "title": self.code.value,
            "status": self.status_code,
            "detail": self.detail,
            "code": self.code.value,
        }
        if self.required_bytes is not None:
            problem["required_bytes"] = self.required_bytes
        if self.available_bytes is not None:
            problem["available_bytes"] = self.available_bytes
        return problem


class ArtifactComponent(StrEnum):
    RAW_PARQUET = "raw_parquet"
    NORMALIZED_PARQUET = "normalized_parquet"
    MODEL_WORKSPACE = "model_workspace"
    GENERATED_PARQUET = "generated_parquet"
    CSV = "csv"
    PARQUET_ZIP64 = "parquet_zip64"


DecimalInput = Decimal | int | float | str


def _nonnegative_decimal(value: DecimalInput, name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a nonnegative finite number")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be a nonnegative finite number") from exc
    if not number.is_finite() or number < 0:
        raise ValueError(f"{name} must be a nonnegative finite number")
    return number


def _ceil_product(*values: DecimalInput) -> int:
    product = Decimal(1)
    for index, value in enumerate(values):
        product *= _nonnegative_decimal(value, f"factor[{index}]")
    return int(product.to_integral_value(rounding=ROUND_CEILING))


class DiskEstimationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_bytes: Annotated[int, Field(ge=0)]
    measured_raw_ratio: Decimal
    rows: Annotated[int, Field(ge=0)]
    measured_normalized_bytes_per_row: Decimal
    output_rows: Annotated[int, Field(ge=0)]
    measured_synth_parquet_bytes_per_row: Decimal
    measured_csv_bytes_per_row: Decimal = Decimal(0)
    model_workspace_estimate_bytes: Annotated[int, Field(ge=0)]
    duckdb_memory_limit_bytes: Annotated[int, Field(gt=0)]
    include_csv: bool = False
    include_parquet_zip64: bool = False
    existing_immutable_artifacts: frozenset[ArtifactComponent] = frozenset()

    @field_validator(
        "measured_raw_ratio",
        "measured_normalized_bytes_per_row",
        "measured_synth_parquet_bytes_per_row",
        "measured_csv_bytes_per_row",
    )
    @classmethod
    def validate_measurement(cls, value: Decimal) -> Decimal:
        return _nonnegative_decimal(value, "measurement")


class DiskEstimate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    capacity_estimator_version: Literal[1] = CAPACITY_ESTIMATOR_VERSION
    raw_parquet_estimate_bytes: Annotated[int, Field(ge=0)]
    normalized_estimate_bytes: Annotated[int, Field(ge=0)]
    model_workspace_estimate_bytes: Annotated[int, Field(ge=0)]
    generated_parquet_estimate_bytes: Annotated[int, Field(ge=0)]
    csv_estimate_bytes: Annotated[int, Field(ge=0)]
    parquet_zip64_estimate_bytes: Annotated[int, Field(ge=0)]
    duckdb_spill_reserve_bytes: Annotated[int, Field(ge=0)]
    atomic_reserve_bytes: Annotated[int, Field(ge=0)]
    reports_logs_reserve_bytes: Annotated[int, Field(ge=0)]
    fixed_reserve_bytes: Annotated[int, Field(ge=0)]
    largest_single_future_artifact_bytes: Annotated[int, Field(ge=0)]
    required_additional_free_bytes: Annotated[int, Field(ge=0)]


def estimate_disk(input_: DiskEstimationInput) -> DiskEstimate:
    """Apply capacity estimator v1 exactly, using decimal arithmetic and integer ceilings."""

    raw = _ceil_product(input_.source_bytes, input_.measured_raw_ratio, Decimal("1.30"))
    normalized = _ceil_product(
        input_.rows, input_.measured_normalized_bytes_per_row, Decimal("1.30")
    )
    generated = _ceil_product(
        input_.output_rows,
        input_.measured_synth_parquet_bytes_per_row,
        Decimal("1.50"),
    )
    csv = (
        _ceil_product(input_.output_rows, input_.measured_csv_bytes_per_row, Decimal("1.50"))
        if input_.include_csv
        else 0
    )
    parquet_zip64 = _ceil_product(generated, Decimal("1.01")) if input_.include_parquet_zip64 else 0

    estimates = {
        ArtifactComponent.RAW_PARQUET: raw,
        ArtifactComponent.NORMALIZED_PARQUET: normalized,
        ArtifactComponent.MODEL_WORKSPACE: input_.model_workspace_estimate_bytes,
        ArtifactComponent.GENERATED_PARQUET: generated,
        ArtifactComponent.CSV: csv,
        ArtifactComponent.PARQUET_ZIP64: parquet_zip64,
    }
    pending = {
        component: size
        for component, size in estimates.items()
        if component not in input_.existing_immutable_artifacts
    }
    largest_future = max(pending.values(), default=0)
    spill_reserve = max(8 * GIB, 2 * input_.duckdb_memory_limit_bytes)
    atomic_reserve = max(largest_future, spill_reserve)
    reports_logs_reserve = 2 * GIB
    fixed_reserve = 20 * GIB
    required = sum(pending.values()) + atomic_reserve + reports_logs_reserve + fixed_reserve

    return DiskEstimate(
        raw_parquet_estimate_bytes=raw,
        normalized_estimate_bytes=normalized,
        model_workspace_estimate_bytes=input_.model_workspace_estimate_bytes,
        generated_parquet_estimate_bytes=generated,
        csv_estimate_bytes=csv,
        parquet_zip64_estimate_bytes=parquet_zip64,
        duckdb_spill_reserve_bytes=spill_reserve,
        atomic_reserve_bytes=atomic_reserve,
        reports_logs_reserve_bytes=reports_logs_reserve,
        fixed_reserve_bytes=fixed_reserve,
        largest_single_future_artifact_bytes=largest_future,
        required_additional_free_bytes=required,
    )


def admit_disk(estimate: DiskEstimate, free_space_bytes: int) -> DiskEstimate:
    if free_space_bytes < 0:
        raise ValueError("free_space_bytes must be nonnegative")
    required = estimate.required_additional_free_bytes
    if required > free_space_bytes:
        raise ResourceAdmissionError(
            ResourceErrorCode.DISK_QUOTA_EXCEEDED,
            f"job requires {required} free bytes but only {free_space_bytes} are available",
            required_bytes=required,
            available_bytes=free_space_bytes,
        )
    return estimate


class ResourceProfileName(StrEnum):
    M4 = "m4"
    L40S = "l40s"


class ResourceLease(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    capacity_estimator_version: Literal[1] = CAPACITY_ESTIMATOR_VERSION
    profile: ResourceProfileName
    duckdb_memory_limit_bytes: Annotated[int, Field(gt=0)]
    control_plane_browser_target_bytes: Annotated[int, Field(ge=0)]
    worker_rss_hard_bytes: Annotated[int, Field(gt=0)]
    process_tree_rss_hard_bytes: Annotated[int, Field(gt=0)]
    utility_training_default_max_rows: Annotated[int, Field(gt=0)]
    utility_generation_processes: Annotated[int, Field(gt=0)]
    dp_projected_state_hard_bytes: Annotated[int, Field(gt=0)] = 512 * MIB
    dp_modeled_columns_hard: Annotated[int, Field(gt=0)] = 32
    gpu_allocated_reserved_hard_bytes_exclusive: Annotated[int, Field(ge=0)] = 0
    host_ram_lease_bytes: Annotated[int, Field(ge=0)] | None = None


M4_LEASE = ResourceLease(
    profile=ResourceProfileName.M4,
    duckdb_memory_limit_bytes=8 * GIB,
    control_plane_browser_target_bytes=4 * GIB,
    worker_rss_hard_bytes=24 * GIB,
    process_tree_rss_hard_bytes=32 * GIB,
    utility_training_default_max_rows=250_000,
    utility_generation_processes=1,
)


def l40s_lease(host_ram_bytes: int, *, generation_processes: int = 1) -> ResourceLease:
    if host_ram_bytes < 32 * GIB:
        raise ResourceAdmissionError(
            ResourceErrorCode.RESOURCE_LIMIT,
            "L40S host must leave at least 32 GiB for the OS and file cache",
            required_bytes=32 * GIB,
            available_bytes=host_ram_bytes,
        )
    if generation_processes not in (1, 2, 4):
        raise ValueError("generation_processes must be one of 1, 2, or 4")
    eighty_percent = (host_ram_bytes * 80) // 100
    host_lease = min(eighty_percent, host_ram_bytes - 32 * GIB)
    return ResourceLease(
        profile=ResourceProfileName.L40S,
        duckdb_memory_limit_bytes=8 * GIB,
        control_plane_browser_target_bytes=4 * GIB,
        worker_rss_hard_bytes=host_lease,
        process_tree_rss_hard_bytes=host_lease,
        utility_training_default_max_rows=2_000_000,
        utility_generation_processes=generation_processes,
        gpu_allocated_reserved_hard_bytes_exclusive=40 * GIB,
        host_ram_lease_bytes=host_lease,
    )


class MemoryProjection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    worker_rss_bytes: Annotated[int, Field(ge=0)]
    process_tree_rss_bytes: Annotated[int, Field(ge=0)]
    gpu_allocated_reserved_bytes: Annotated[int, Field(ge=0)] = 0


def admit_memory(projection: MemoryProjection, lease: ResourceLease) -> MemoryProjection:
    checks = (
        ("worker RSS", projection.worker_rss_bytes, lease.worker_rss_hard_bytes, False),
        (
            "process-tree RSS",
            projection.process_tree_rss_bytes,
            lease.process_tree_rss_hard_bytes,
            False,
        ),
    )
    for label, projected, maximum, exclusive in checks:
        violates = projected >= maximum if exclusive else projected > maximum
        if violates:
            raise ResourceAdmissionError(
                ResourceErrorCode.RESOURCE_LIMIT,
                f"projected {label} of {projected} bytes exceeds the {maximum}-byte lease",
                required_bytes=projected,
                available_bytes=maximum,
            )
    gpu_limit = lease.gpu_allocated_reserved_hard_bytes_exclusive
    if gpu_limit and projection.gpu_allocated_reserved_bytes >= gpu_limit:
        raise ResourceAdmissionError(
            ResourceErrorCode.RESOURCE_LIMIT,
            f"projected allocated+reserved VRAM must be less than {gpu_limit} bytes",
            required_bytes=projection.gpu_allocated_reserved_bytes,
            available_bytes=gpu_limit - 1,
        )
    return projection


class DpStateEstimate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    capacity_estimator_version: Literal[1] = CAPACITY_ESTIMATOR_VERSION
    modeled_columns: Annotated[int, Field(ge=0)]
    one_way_cells: Annotated[int, Field(ge=0)]
    largest_pair_cells: Annotated[int, Field(ge=0)]
    selected_pair_cells: Annotated[int, Field(ge=0)]
    estimated_state_bytes: Annotated[int, Field(ge=0)]


def estimate_dp_state(domain_sizes: Iterable[int]) -> DpStateEstimate:
    sizes = tuple(domain_sizes)
    if not sizes:
        raise ValueError("at least one modeled column is required")
    if len(sizes) > 32:
        raise ResourceAdmissionError(
            ResourceErrorCode.RESOURCE_LIMIT,
            "formal-DP v1 supports at most 32 modeled columns",
        )
    if any(isinstance(size, bool) or size <= 0 for size in sizes):
        raise ValueError("domain sizes must be positive integers")
    if any(size > 256 for size in sizes):
        raise ResourceAdmissionError(
            ResourceErrorCode.RESOURCE_LIMIT,
            "formal-DP v1 supports at most 256 category/bin states per column",
        )

    pair_products = sorted((left * right for left, right in combinations(sizes, 2)), reverse=True)
    largest_pair = pair_products[0] if pair_products else 0
    if largest_pair > 1_000_000:
        raise ResourceAdmissionError(
            ResourceErrorCode.RESOURCE_LIMIT,
            "largest public-domain pair exceeds 1,000,000 cells",
            required_bytes=largest_pair,
            available_bytes=1_000_000,
        )
    selected_pairs = sum(pair_products[: max(0, len(sizes) - 1)])
    one_way = sum(sizes)
    estimated = 8 * (one_way + selected_pairs) * 8
    if estimated > 512 * MIB:
        raise ResourceAdmissionError(
            ResourceErrorCode.RESOURCE_LIMIT,
            "projected DP state exceeds the 512 MiB app gate",
            required_bytes=estimated,
            available_bytes=512 * MIB,
        )
    return DpStateEstimate(
        modeled_columns=len(sizes),
        one_way_cells=one_way,
        largest_pair_cells=largest_pair,
        selected_pair_cells=selected_pairs,
        estimated_state_bytes=estimated,
    )
