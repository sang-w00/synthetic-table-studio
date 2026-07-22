from __future__ import annotations

from collections.abc import Iterable
from itertools import combinations
from typing import Literal

from pydantic import Field

from sts.domain import CanonicalModel, DomainError, ErrorCode

MIB = 1024 * 1024
GIB = 1024 * MIB
MAX_MODELED_COLUMNS = 32
MAX_STATES_PER_COLUMN = 256
MAX_ESTIMATED_STATE_BYTES = 512 * MIB
MAX_LARGEST_PAIR_CELLS = 1_000_000
DEFAULT_DP_WORKER_RSS_LEASE_BYTES = 24 * GIB


class MstStateEstimate(CanonicalModel):
    capacity_estimator_version: Literal[1] = 1
    modeled_columns: int = Field(ge=1)
    domain_sizes: tuple[int, ...]
    one_way_cells: int = Field(ge=1)
    largest_pair_cells: int = Field(ge=0)
    selected_pair_cells: int = Field(ge=0)
    cell_bytes: Literal[8] = 8
    safety_factor: Literal[8] = 8
    estimated_state_bytes: int = Field(ge=0)


class MstDomainAdmission(CanonicalModel):
    estimate: MstStateEstimate
    projected_worker_rss_bytes: int = Field(ge=0)
    worker_rss_limit_bytes: int = Field(gt=0)
    upstream_max_model_size_mib: Literal[512] = 512


def estimate_mst_state(domain_sizes: Iterable[int]) -> MstStateEstimate:
    """Apply the app's conservative d-1-largest-pairs MST state formula."""

    sizes = tuple(domain_sizes)
    if not sizes:
        raise ValueError("at least one modeled column is required")
    if any(isinstance(size, bool) or not isinstance(size, int) or size <= 0 for size in sizes):
        raise ValueError("domain sizes must be positive integers")
    products = sorted((left * right for left, right in combinations(sizes, 2)), reverse=True)
    selected_pair_cells = sum(products[: max(0, len(sizes) - 1)])
    one_way_cells = sum(sizes)
    estimated_state_bytes = 8 * (one_way_cells + selected_pair_cells) * 8
    return MstStateEstimate(
        modeled_columns=len(sizes),
        domain_sizes=sizes,
        one_way_cells=one_way_cells,
        largest_pair_cells=products[0] if products else 0,
        selected_pair_cells=selected_pair_cells,
        estimated_state_bytes=estimated_state_bytes,
    )


def admit_mst_domain(
    domain_sizes: Iterable[int],
    *,
    projected_worker_rss_bytes: int,
    worker_rss_limit_bytes: int = DEFAULT_DP_WORKER_RSS_LEASE_BYTES,
) -> MstDomainAdmission:
    if isinstance(projected_worker_rss_bytes, bool) or projected_worker_rss_bytes < 0:
        raise ValueError("projected_worker_rss_bytes must be a non-negative integer")
    if isinstance(worker_rss_limit_bytes, bool) or worker_rss_limit_bytes <= 0:
        raise ValueError("worker_rss_limit_bytes must be a positive integer")
    estimate = estimate_mst_state(domain_sizes)
    failed_gates: list[str] = []
    if estimate.modeled_columns > MAX_MODELED_COLUMNS:
        failed_gates.append("modeled_columns")
    if estimate.largest_pair_cells > MAX_LARGEST_PAIR_CELLS:
        failed_gates.append("largest_pair_cells")
    if estimate.estimated_state_bytes > MAX_ESTIMATED_STATE_BYTES:
        failed_gates.append("estimated_state_bytes")
    if any(size > MAX_STATES_PER_COLUMN for size in estimate.domain_sizes):
        failed_gates.append("states_per_column")
    if failed_gates:
        raise DomainError(
            ErrorCode.DP_DOMAIN_TOO_LARGE,
            "public modeled domain exceeds the formal-DP MST admission gate",
            context={
                "failed_gates": failed_gates,
                "modeled_columns_limit": MAX_MODELED_COLUMNS,
                "states_per_column_limit": MAX_STATES_PER_COLUMN,
                "largest_pair_cells_limit": MAX_LARGEST_PAIR_CELLS,
                "estimated_state_bytes_limit": MAX_ESTIMATED_STATE_BYTES,
                "estimate": estimate.model_dump(mode="json"),
            },
        )
    if projected_worker_rss_bytes > worker_rss_limit_bytes:
        raise DomainError(
            ErrorCode.RESOURCE_LIMIT,
            "projected DP worker RSS exceeds its hard lease",
            context={
                "projected_worker_rss_bytes": projected_worker_rss_bytes,
                "worker_rss_limit_bytes": worker_rss_limit_bytes,
            },
        )
    return MstDomainAdmission(
        estimate=estimate,
        projected_worker_rss_bytes=projected_worker_rss_bytes,
        worker_rss_limit_bytes=worker_rss_limit_bytes,
    )


__all__ = [
    "DEFAULT_DP_WORKER_RSS_LEASE_BYTES",
    "MAX_ESTIMATED_STATE_BYTES",
    "MAX_LARGEST_PAIR_CELLS",
    "MAX_MODELED_COLUMNS",
    "MAX_STATES_PER_COLUMN",
    "MstDomainAdmission",
    "MstStateEstimate",
    "admit_mst_domain",
    "estimate_mst_state",
]
