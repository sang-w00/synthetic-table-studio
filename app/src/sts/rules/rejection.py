from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from sts.domain import DomainError, ErrorCode
from sts.rules.execution import (
    StructuralCodecs,
    attach_candidate_indices,
    full_validate,
    iter_arrow_batches,
    regenerate_identifiers,
    repair_and_validate_candidate,
)

if TYPE_CHECKING:
    from sts.rules.compiler import CompiledRules


@dataclass(frozen=True, slots=True)
class ShardAllocation:
    shard_id: int
    candidate_start: int
    candidate_stop: int
    candidate_quota: int
    seed: int

    def __post_init__(self) -> None:
        if self.candidate_start < 0 or self.candidate_stop < self.candidate_start:
            raise ValueError("candidate range must be nonnegative and ordered")
        if self.candidate_stop - self.candidate_start != self.candidate_quota:
            raise ValueError("candidate quota must equal the half-open range size")


@dataclass(frozen=True, slots=True)
class CandidateBatch:
    """A generated batch tied to its first global candidate index."""

    candidate_start: int
    batch: pa.Table | pa.RecordBatch


@dataclass(frozen=True, slots=True)
class RejectionResult:
    output_path: Path
    requested_rows: int
    actual_rows: int
    candidates_examined: int
    candidates_rejected: int
    post_violations: int
    allocations: tuple[ShardAllocation, ...]


type CandidateItem = CandidateBatch | pa.Table | pa.RecordBatch
type CandidateStream = Iterable[CandidateItem]
type CandidateProvider = Callable[[ShardAllocation], CandidateStream]
type ProviderSet = CandidateProvider | Mapping[int, CandidateProvider | CandidateStream]
type CandidateRepair = Callable[
    [pa.Table | pa.RecordBatch, "CompiledRules", StructuralCodecs | None],
    tuple[pa.Table, tuple[bool, ...]],
]


def _derive_seed(master_seed: int, allocation: ShardAllocation, nonce: int = 0) -> int:
    payload = (
        master_seed.to_bytes(32, "big", signed=False)
        + allocation.shard_id.to_bytes(4, "big", signed=False)
        + allocation.candidate_start.to_bytes(8, "big", signed=False)
        + allocation.candidate_stop.to_bytes(8, "big", signed=False)
        + nonce.to_bytes(4, "big", signed=False)
    )
    return int.from_bytes(hashlib.sha256(payload).digest()[:4], "big")


def plan_shards(
    output_rows: int,
    shard_count: int,
    master_seed: int,
    *,
    max_candidate_multiplier: int = 20,
) -> tuple[ShardAllocation, ...]:
    """Allocate one deterministic, disjoint half-open candidate range per shard."""

    if output_rows <= 0:
        raise ValueError("output_rows must be positive")
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    if master_seed < 0 or master_seed >= 2**256:
        raise ValueError("master_seed must be an unsigned 256-bit integer")
    if max_candidate_multiplier <= 0:
        raise ValueError("max_candidate_multiplier must be positive")
    max_candidates = output_rows * max_candidate_multiplier
    base, remainder = divmod(max_candidates, shard_count)
    allocations: list[ShardAllocation] = []
    cursor = 0
    used_seeds: set[int] = set()
    for shard_id in range(shard_count):
        quota = base + (1 if shard_id < remainder else 0)
        blank = ShardAllocation(
            shard_id=shard_id,
            candidate_start=cursor,
            candidate_stop=cursor + quota,
            candidate_quota=quota,
            seed=0,
        )
        nonce = 0
        seed = _derive_seed(master_seed, blank, nonce)
        while seed in used_seeds:
            nonce += 1
            seed = _derive_seed(master_seed, blank, nonce)
        used_seeds.add(seed)
        allocations.append(
            ShardAllocation(
                shard_id=shard_id,
                candidate_start=cursor,
                candidate_stop=cursor + quota,
                candidate_quota=quota,
                seed=seed,
            )
        )
        cursor += quota
    assert cursor == max_candidates
    return tuple(allocations)


def _default_repair(
    batch: pa.Table | pa.RecordBatch,
    compiled: CompiledRules,
    codecs: StructuralCodecs | None,
) -> tuple[pa.Table, tuple[bool, ...]]:
    result = repair_and_validate_candidate(batch, compiled, codecs=codecs)
    return result.table, result.report.invalid_rows


def _stream_for(providers: ProviderSet, allocation: ShardAllocation) -> CandidateStream:
    if callable(providers):
        return providers(allocation)
    try:
        stream = providers[allocation.shard_id]
    except KeyError as exc:
        raise ValueError(f"missing candidate provider for shard {allocation.shard_id}") from exc
    return stream(allocation) if callable(stream) else stream


class GlobalRejectionCoordinator:
    """Single owner of the global residual-rejection budget and publication."""

    def __init__(
        self,
        compiled: CompiledRules,
        *,
        output_rows: int,
        shard_count: int,
        master_seed: int,
        codecs: StructuralCodecs | None = None,
        max_candidate_multiplier: int = 20,
        candidate_repair: CandidateRepair | None = None,
    ) -> None:
        self.compiled = compiled
        self.output_rows = output_rows
        self.master_seed = master_seed
        self.codecs = codecs
        self.allocations = plan_shards(
            output_rows,
            shard_count,
            master_seed,
            max_candidate_multiplier=max_candidate_multiplier,
        )
        self.max_candidates = output_rows * max_candidate_multiplier
        self._repair = candidate_repair or _default_repair

    def run(self, providers: ProviderSet, output_path: str | Path) -> RejectionResult:
        """Repair, reject and atomically publish exactly the requested row count.

        Candidate batches are consumed by ascending global candidate index. A provider
        cannot cross its assigned range. On any failure or feasibility exhaustion the
        temporary file is removed and no output path is published.
        """

        destination = Path(output_path).resolve(strict=False)
        if destination.exists() or destination.is_symlink():
            raise DomainError(
                ErrorCode.IMMUTABLE_PATH_EXISTS,
                f"synthetic output already exists: {destination.name}",
            )
        if not callable(providers):
            missing = sorted(set(range(len(self.allocations))) - set(providers))
            extra = sorted(set(providers) - set(range(len(self.allocations))))
            if missing or extra:
                raise ValueError(
                    f"candidate providers do not match shard plan: missing={missing}, extra={extra}"
                )
        destination.parent.mkdir(parents=True, exist_ok=True)
        part = destination.with_name(f".{destination.name}.{uuid4().hex}.part")
        writer: pq.ParquetWriter | None = None
        accepted = examined = rejected = 0
        try:
            for allocation in self.allocations:
                expected_index = allocation.candidate_start
                for item in _stream_for(providers, allocation):
                    if accepted == self.output_rows:
                        break
                    if isinstance(item, CandidateBatch):
                        candidate_start = item.candidate_start
                        batch = item.batch
                    else:
                        candidate_start = expected_index
                        batch = item
                    table = (
                        pa.Table.from_batches([batch])
                        if isinstance(batch, pa.RecordBatch)
                        else batch
                    )
                    if candidate_start != expected_index:
                        raise DomainError(
                            ErrorCode.WORKER_FAILED,
                            "candidate stream is not contiguous within its assigned range",
                            context={
                                "shard_id": allocation.shard_id,
                                "expected_candidate_start": expected_index,
                                "actual_candidate_start": candidate_start,
                            },
                        )
                    candidate_stop = candidate_start + table.num_rows
                    if candidate_stop > allocation.candidate_stop:
                        raise DomainError(
                            ErrorCode.WORKER_FAILED,
                            "candidate stream exceeded its assigned global range",
                            context={
                                "shard_id": allocation.shard_id,
                                "candidate_stop": candidate_stop,
                                "allocated_stop": allocation.candidate_stop,
                            },
                        )
                    expected_index = candidate_stop
                    table = attach_candidate_indices(table, row_start=candidate_start)
                    examined += table.num_rows
                    repaired, invalid = self._repair(table, self.compiled, self.codecs)
                    if len(invalid) != repaired.num_rows:
                        raise DomainError(
                            ErrorCode.WORKER_FAILED,
                            "candidate repair returned a mask with the wrong length",
                            context={"shard_id": allocation.shard_id},
                        )
                    keep = pa.array([not value for value in invalid], type=pa.bool_())
                    rejected += sum(invalid)
                    valid = repaired.filter(keep)
                    remaining = self.output_rows - accepted
                    if valid.num_rows > remaining:
                        valid = valid.slice(0, remaining)
                    valid = regenerate_identifiers(valid, self.compiled, row_start=accepted)
                    if valid.num_rows:
                        if writer is None:
                            writer = pq.ParquetWriter(part, valid.schema, compression="zstd")
                        elif writer.schema != valid.schema:
                            raise DomainError(
                                ErrorCode.WORKER_FAILED,
                                "candidate shards produced incompatible output schemas",
                                context={"shard_id": allocation.shard_id},
                            )
                        writer.write_table(valid)
                        accepted += valid.num_rows
                if accepted == self.output_rows:
                    break
            if writer is not None:
                writer.close()
                writer = None
            if accepted != self.output_rows:
                raise DomainError(
                    ErrorCode.RULE_FEASIBILITY_EXHAUSTED,
                    "global rule-feasibility candidate budget was exhausted before the row target",
                    context={
                        "requested_rows": self.output_rows,
                        "accepted_rows": accepted,
                        "candidates_examined": examined,
                        "max_candidates": self.max_candidates,
                    },
                )
            post_rows = post_violations = 0
            for batch in iter_arrow_batches(part):
                checked = full_validate(batch, self.compiled, codecs=self.codecs)
                post_rows += checked.report.rows
                post_violations += checked.report.violation_union_count
            if post_rows != self.output_rows or post_violations:
                raise DomainError(
                    ErrorCode.OUTPUT_INVALID,
                    "staged synthetic output failed exact full validation",
                    context={
                        "requested_rows": self.output_rows,
                        "actual_rows": post_rows,
                        "post_violations": post_violations,
                    },
                )
            with part.open("rb") as handle:
                os.fsync(handle.fileno())
            os.replace(part, destination)
            directory_fd = os.open(destination.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            return RejectionResult(
                output_path=destination,
                requested_rows=self.output_rows,
                actual_rows=post_rows,
                candidates_examined=examined,
                candidates_rejected=rejected,
                post_violations=post_violations,
                allocations=self.allocations,
            )
        except Exception:
            if writer is not None:
                writer.close()
            part.unlink(missing_ok=True)
            raise
