from __future__ import annotations

import hashlib
import hmac
from typing import Annotated

from pydantic import Field, model_validator

from sts.domain.canonical import CanonicalModel

_UINT32_MAX = 2**32 - 1
_UINT256_MAX = 2**256 - 1
_HKDF_SALT = b"sts/argn/hkdf-sha256/v1"
_HKDF_INFO_PREFIX = b"sts/argn/seed/v1\x00"


class SeedCollisionError(ValueError):
    """Raised when distinct generation work items derive the same uint32 seed."""


class CandidateShardPlan(CanonicalModel):
    candidate_index: Annotated[int, Field(ge=0)]
    process_index: Annotated[int, Field(ge=0)]
    row_start: Annotated[int, Field(ge=0)]
    output_rows: Annotated[int, Field(gt=0)]
    engine_seed: Annotated[int, Field(ge=0, le=_UINT32_MAX)]


class GenerationProcessPlan(CanonicalModel):
    process_index: Annotated[int, Field(ge=0)]
    candidate_start: Annotated[int, Field(ge=0)]
    candidate_stop: Annotated[int, Field(gt=0)]
    output_rows: Annotated[int, Field(gt=0)]

    @model_validator(mode="after")
    def validate_candidate_range(self) -> GenerationProcessPlan:
        if self.candidate_stop <= self.candidate_start:
            raise ValueError("candidate_stop must be greater than candidate_start")
        return self


class GenerationSeedPlan(CanonicalModel):
    output_rows: Annotated[int, Field(gt=0)]
    rows_per_candidate: Annotated[int, Field(gt=0)]
    process_count: Annotated[int, Field(gt=0)]
    candidates: tuple[CandidateShardPlan, ...]
    processes: tuple[GenerationProcessPlan, ...]

    @model_validator(mode="after")
    def validate_exact_plan(self) -> GenerationSeedPlan:
        if len(self.processes) != self.process_count:
            raise ValueError("process plan count does not match process_count")
        if not self.candidates:
            raise ValueError("at least one candidate shard is required")
        if sum(item.output_rows for item in self.candidates) != self.output_rows:
            raise ValueError("candidate shard rows do not sum to output_rows")
        if sum(item.output_rows for item in self.processes) != self.output_rows:
            raise ValueError("process rows do not sum to output_rows")
        indices = [item.candidate_index for item in self.candidates]
        if indices != list(range(len(self.candidates))):
            raise ValueError("candidate indices must be contiguous and start at zero")
        starts = [item.row_start for item in self.candidates]
        expected_starts: list[int] = []
        next_start = 0
        for item in self.candidates:
            expected_starts.append(next_start)
            next_start += item.output_rows
        if starts != expected_starts:
            raise ValueError("candidate row ranges must be contiguous")
        seeds = [item.engine_seed for item in self.candidates]
        if len(seeds) != len(set(seeds)):
            raise ValueError("candidate engine seeds must be disjoint")
        return self


def _master_seed_bytes(master_seed: int | bytes) -> bytes:
    if isinstance(master_seed, bytes):
        if not master_seed:
            raise ValueError("master_seed bytes must not be empty")
        return master_seed
    if isinstance(master_seed, bool) or not isinstance(master_seed, int):
        raise TypeError("master_seed must be an integer or bytes")
    if not 0 <= master_seed <= _UINT256_MAX:
        raise ValueError("integer master_seed must fit in 256 bits")
    return master_seed.to_bytes(32, "big")


def hkdf_sha256(*, input_key_material: bytes, info: bytes, length: int) -> bytes:
    """RFC 5869 extract-and-expand using the utility seed domain salt."""

    if not input_key_material:
        raise ValueError("input_key_material must not be empty")
    if not info:
        raise ValueError("info must not be empty")
    if not 1 <= length <= 255 * hashlib.sha256().digest_size:
        raise ValueError("length is outside the RFC 5869 SHA-256 limit")

    pseudo_random_key = hmac.new(_HKDF_SALT, input_key_material, hashlib.sha256).digest()
    output = bytearray()
    previous = b""
    counter = 1
    while len(output) < length:
        previous = hmac.new(
            pseudo_random_key,
            previous + info + bytes((counter,)),
            hashlib.sha256,
        ).digest()
        output.extend(previous)
        counter += 1
    return bytes(output[:length])


def derive_uint32_seed(master_seed: int | bytes, *, purpose: str, index: int) -> int:
    """Derive one stable engine seed without reusing another purpose's HKDF stream."""

    if not purpose or "\x00" in purpose:
        raise ValueError("purpose must be nonempty and cannot contain NUL")
    if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index < 2**64:
        raise ValueError("index must be an unsigned 64-bit integer")
    info = _HKDF_INFO_PREFIX + purpose.encode("utf-8") + b"\x00" + index.to_bytes(8, "big")
    return int.from_bytes(
        hkdf_sha256(input_key_material=_master_seed_bytes(master_seed), info=info, length=4),
        "big",
    )


def derive_disjoint_uint32_seeds(
    master_seed: int | bytes,
    *,
    purpose: str,
    count: int,
) -> tuple[int, ...]:
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise ValueError("count must be a positive integer")
    seeds = tuple(derive_uint32_seed(master_seed, purpose=purpose, index=i) for i in range(count))
    if len(seeds) != len(set(seeds)):
        collisions: dict[int, list[int]] = {}
        for index, seed in enumerate(seeds):
            collisions.setdefault(seed, []).append(index)
        duplicate_indices = [indices for indices in collisions.values() if len(indices) > 1]
        raise SeedCollisionError(f"derived uint32 seed collision at indices {duplicate_indices}")
    return seeds


def plan_generation_seeds(
    *,
    master_seed: int | bytes,
    output_rows: int,
    rows_per_candidate: int,
    process_count: int,
) -> GenerationSeedPlan:
    """Plan exact candidate rows, contiguous process ranges, and collision-free seeds."""

    for name, value in (
        ("output_rows", output_rows),
        ("rows_per_candidate", rows_per_candidate),
        ("process_count", process_count),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")

    candidate_count = (output_rows + rows_per_candidate - 1) // rows_per_candidate
    if process_count > candidate_count:
        raise ValueError("process_count cannot exceed the number of candidate shards")
    seeds = derive_disjoint_uint32_seeds(
        master_seed,
        purpose="argn-generation-candidate",
        count=candidate_count,
    )

    process_candidate_counts = [candidate_count // process_count] * process_count
    for process_index in range(candidate_count % process_count):
        process_candidate_counts[process_index] += 1

    process_for_candidate: list[int] = []
    for process_index, count in enumerate(process_candidate_counts):
        process_for_candidate.extend([process_index] * count)

    candidates: list[CandidateShardPlan] = []
    remaining_rows = output_rows
    row_start = 0
    for candidate_index in range(candidate_count):
        candidate_rows = min(rows_per_candidate, remaining_rows)
        candidates.append(
            CandidateShardPlan(
                candidate_index=candidate_index,
                process_index=process_for_candidate[candidate_index],
                row_start=row_start,
                output_rows=candidate_rows,
                engine_seed=seeds[candidate_index],
            )
        )
        row_start += candidate_rows
        remaining_rows -= candidate_rows

    processes: list[GenerationProcessPlan] = []
    candidate_start = 0
    for process_index, count in enumerate(process_candidate_counts):
        candidate_stop = candidate_start + count
        processes.append(
            GenerationProcessPlan(
                process_index=process_index,
                candidate_start=candidate_start,
                candidate_stop=candidate_stop,
                output_rows=sum(
                    item.output_rows for item in candidates[candidate_start:candidate_stop]
                ),
            )
        )
        candidate_start = candidate_stop

    return GenerationSeedPlan(
        output_rows=output_rows,
        rows_per_candidate=rows_per_candidate,
        process_count=process_count,
        candidates=tuple(candidates),
        processes=tuple(processes),
    )
