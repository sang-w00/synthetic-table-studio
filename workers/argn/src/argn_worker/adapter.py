from __future__ import annotations

import hashlib
import importlib.metadata
import math
import os
import shutil
import stat
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .protocol import (
    ResolvedSnapshotFile,
    canonical_json_bytes,
    confined_output_path,
    fsync_directory,
    validate_workspace_relative_path,
)

ENGINE_DISTRIBUTION = "mostlyai-engine"
ENGINE_VERSION = "2.6.2"
ENGINE_WHEEL_SHA256 = "3ead3770c936919f8fce4e1f9fffd271ffdd490f0292c2ab9a42cb4bafe3caea"
CHECKPOINT_FORMAT_VERSION = "1.0"
CHECKPOINT_TAG_FILENAME = "sts-checkpoint.json"
BOUNDED_SAMPLE_SNAPSHOT_KEY = "bounded_training_sample"
MIN_CANDIDATE_ROWS = 10_000
MAX_CANDIDATE_ROWS = 250_000
MAX_BOUNDED_TRAINING_ROWS = 5_000_000
MAX_TRAINING_COLUMNS = 70
# The authoritative M4 Phase 0 gate has no CUDA devices, so clone fan-out is disabled locally.
MULTIPROCESS_CLONES_ENABLED = False

_SHA256_LENGTH = 64
_REQUIRED_COMPATIBILITY_KEYS = frozenset(
    ("source_manifest_sha256", "schema_sha256", "rules_sha256", "engine_sha256")
)


class AdapterError(RuntimeError):
    def __init__(
        self, code: str, message: str, *, details: Mapping[str, Any] | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})


class CancellationRequested(AdapterError):
    def __init__(self, stage: str) -> None:
        super().__init__(
            "CANCELLED", "ARGN operation was cancelled", details={"stage": stage}
        )


@dataclass(frozen=True)
class CheckpointCompatibility:
    source_manifest_sha256: str
    schema_sha256: str
    rules_sha256: str
    engine_sha256: str

    @classmethod
    def from_mapping(cls, raw: object) -> CheckpointCompatibility:
        value = _object(raw, "checkpoint_compatibility")
        _exact_keys(value, _REQUIRED_COMPATIBILITY_KEYS, "checkpoint_compatibility")
        compatibility = cls(
            **{key: _sha256(value[key], key) for key in _REQUIRED_COMPATIBILITY_KEYS}
        )
        if compatibility.engine_sha256 != ENGINE_WHEEL_SHA256:
            raise AdapterError(
                "BACKEND_INCOMPATIBLE",
                "checkpoint engine hash does not match the pinned mostlyai-engine wheel",
                details={"expected_engine_sha256": ENGINE_WHEEL_SHA256},
            )
        return compatibility

    def to_dict(self) -> dict[str, str]:
        return {
            "source_manifest_sha256": self.source_manifest_sha256,
            "schema_sha256": self.schema_sha256,
            "rules_sha256": self.rules_sha256,
            "engine_sha256": self.engine_sha256,
        }


@dataclass(frozen=True)
class DeterministicSplitConfig:
    callable_fraction: float
    fallback_fraction: float
    seed: int

    @classmethod
    def from_mapping(cls, raw: object) -> DeterministicSplitConfig:
        value = _object(raw, "deterministic_split")
        _exact_keys(
            value,
            {"callable_fraction", "fallback_fraction", "seed"},
            "deterministic_split",
        )
        callable_fraction = _number(value["callable_fraction"], "callable_fraction")
        fallback_fraction = _number(value["fallback_fraction"], "fallback_fraction")
        if callable_fraction != 0.9 or fallback_fraction != 0.9:
            raise AdapterError(
                "WORKER_REQUEST_INVALID",
                "ARGN training requires exact callable and fallback split fractions of 0.9",
            )
        return cls(
            callable_fraction=callable_fraction,
            fallback_fraction=fallback_fraction,
            seed=_uint32(value["seed"], "deterministic_split.seed"),
        )


@dataclass(frozen=True)
class FitConfig:
    checkpoint_path: str
    worker_memory_lease_bytes: int
    model_size: str
    max_epochs: int
    max_minutes: float
    device: str
    deterministic_split: DeterministicSplitConfig
    checkpoint_compatibility: CheckpointCompatibility

    @classmethod
    def from_limits(cls, limits: Mapping[str, Any]) -> FitConfig:
        value = _argn_limits(limits)
        _exact_keys(
            value,
            {
                "checkpoint_path",
                "max_epochs",
                "max_minutes",
                "model_size",
                "device",
                "deterministic_split",
                "checkpoint_compatibility",
            },
            "limits.argn for fit",
        )
        max_epochs = _positive_int(value["max_epochs"], "max_epochs")
        if max_epochs > 10_000:
            raise AdapterError(
                "WORKER_REQUEST_INVALID", "max_epochs is unreasonably large"
            )
        max_minutes = _number(value["max_minutes"], "max_minutes")
        if max_minutes <= 0:
            raise AdapterError("WORKER_REQUEST_INVALID", "max_minutes must be positive")
        return cls(
            checkpoint_path=validate_workspace_relative_path(
                _string(value["checkpoint_path"], "checkpoint_path")
            ),
            worker_memory_lease_bytes=_worker_memory_lease(limits),
            model_size=_string(value["model_size"], "model_size"),
            max_epochs=max_epochs,
            max_minutes=max_minutes,
            device=_string(value["device"], "device"),
            deterministic_split=DeterministicSplitConfig.from_mapping(
                value["deterministic_split"]
            ),
            checkpoint_compatibility=CheckpointCompatibility.from_mapping(
                value["checkpoint_compatibility"]
            ),
        )


@dataclass(frozen=True)
class GenerateConfig:
    checkpoint_path: str
    candidate_output_path: str
    candidate_rows: int
    batch_size: int
    engine_seed: int
    shard_index: int
    seed_count: int
    process_count: int
    device: str
    checkpoint_compatibility: CheckpointCompatibility

    @classmethod
    def from_limits(cls, limits: Mapping[str, Any]) -> GenerateConfig:
        value = _argn_limits(limits)
        _exact_keys(
            value,
            {
                "checkpoint_path",
                "candidate_output_path",
                "candidate_rows",
                "batch_size",
                "engine_seed",
                "shard_index",
                "seed_count",
                "process_count",
                "device",
                "checkpoint_compatibility",
            },
            "limits.argn for generate",
        )
        _worker_memory_lease(limits)
        candidate_rows = _positive_int(value["candidate_rows"], "candidate_rows")
        if not MIN_CANDIDATE_ROWS <= candidate_rows <= MAX_CANDIDATE_ROWS:
            raise AdapterError(
                "RESOURCE_LIMIT",
                f"candidate_rows must be between {MIN_CANDIDATE_ROWS} and {MAX_CANDIDATE_ROWS}",
                details={"candidate_rows": candidate_rows},
            )
        batch_size = _positive_int(value["batch_size"], "batch_size")
        if batch_size > candidate_rows:
            raise AdapterError(
                "WORKER_REQUEST_INVALID", "batch_size must not exceed candidate_rows"
            )
        process_count = _positive_int(value["process_count"], "process_count")
        if process_count != 1:
            raise AdapterError(
                "BACKEND_INCOMPATIBLE",
                "ARGN multi-process checkpoint clones are disabled by the local Phase 0 gate",
                details={
                    "requested_process_count": process_count,
                    "supported_process_count": 1,
                },
            )
        shard_index = value["shard_index"]
        if (
            isinstance(shard_index, bool)
            or not isinstance(shard_index, int)
            or shard_index < 0
        ):
            raise AdapterError(
                "WORKER_REQUEST_INVALID", "shard_index must be a nonnegative integer"
            )
        seed_count = _positive_int(value["seed_count"], "seed_count")
        if shard_index >= seed_count:
            raise AdapterError(
                "WORKER_REQUEST_INVALID", "shard_index must be less than seed_count"
            )
        return cls(
            checkpoint_path=validate_workspace_relative_path(
                _string(value["checkpoint_path"], "checkpoint_path")
            ),
            candidate_output_path=validate_workspace_relative_path(
                _string(value["candidate_output_path"], "candidate_output_path")
            ),
            candidate_rows=candidate_rows,
            batch_size=batch_size,
            engine_seed=_uint32(value["engine_seed"], "engine_seed"),
            shard_index=shard_index,
            seed_count=seed_count,
            process_count=process_count,
            device=_string(value["device"], "device"),
            checkpoint_compatibility=CheckpointCompatibility.from_mapping(
                value["checkpoint_compatibility"]
            ),
        )


class DeterministicSplitter:
    def __init__(self, config: DeterministicSplitConfig) -> None:
        self.config = config
        self.last_metrics: dict[str, Any] | None = None

    def __call__(self, keys: Any) -> tuple[Any, Any]:
        import numpy as np
        import pandas as pd

        if not isinstance(keys, pd.Series):
            keys = pd.Series(keys)
        if not keys.is_unique:
            raise AdapterError(
                "BACKEND_INCOMPATIBLE", "engine split keys must be unique"
            )
        count = len(keys)
        train_count = round(self.config.callable_fraction * count)
        hashed = pd.util.hash_pandas_object(
            keys, index=False, categorize=True
        ).to_numpy(dtype=np.uint64, copy=True)
        hashed ^= np.uint64(self.config.seed)
        state = hashed
        state ^= state >> np.uint64(30)
        state *= np.uint64(0xBF58476D1CE4E5B9)
        state ^= state >> np.uint64(27)
        state *= np.uint64(0x94D049BB133111EB)
        state ^= state >> np.uint64(31)
        order = np.argsort(state, kind="stable")
        train_positions = np.zeros(count, dtype=bool)
        train_positions[order[:train_count]] = True
        training = keys.iloc[train_positions]
        validation = keys.iloc[~train_positions]
        self.last_metrics = _split_metrics(training, validation, count, self.config)
        return training, validation

    def verify_parity(self, row_count: int) -> dict[str, Any]:
        import pandas as pd

        keys = pd.Series(range(row_count), dtype="int64")
        first_training, first_validation = self(keys)
        first_metrics = dict(self.last_metrics or {})
        first_training_rows = len(first_training)
        del first_training, first_validation
        self(keys)
        second_metrics = dict(self.last_metrics or {})
        deterministic = (
            first_metrics["training_sha256"] == second_metrics["training_sha256"]
            and first_metrics["validation_sha256"]
            == second_metrics["validation_sha256"]
        )
        expected_training = round(self.config.fallback_fraction * row_count)
        fallback_first = keys.sample(frac=1, random_state=self.config.seed)
        fallback_first_hashes = (
            hashlib.sha256(
                fallback_first.iloc[:expected_training].to_numpy(copy=False)
            ).hexdigest(),
            hashlib.sha256(
                fallback_first.iloc[expected_training:].to_numpy(copy=False)
            ).hexdigest(),
        )
        del fallback_first
        fallback_second = keys.sample(frac=1, random_state=self.config.seed)
        fallback_second_hashes = (
            hashlib.sha256(
                fallback_second.iloc[:expected_training].to_numpy(copy=False)
            ).hexdigest(),
            hashlib.sha256(
                fallback_second.iloc[expected_training:].to_numpy(copy=False)
            ).hexdigest(),
        )
        fallback_deterministic = fallback_first_hashes == fallback_second_hashes
        del fallback_second
        if (
            not deterministic
            or not fallback_deterministic
            or first_training_rows != expected_training
        ):
            raise AdapterError(
                "BACKEND_INCOMPATIBLE",
                "deterministic callable/fallback split parity failed",
                details={
                    "deterministic": deterministic,
                    "callable_training_rows": first_training_rows,
                    "fallback_training_rows": expected_training,
                    "fallback_deterministic": fallback_deterministic,
                },
            )
        first_metrics.update(
            {
                "deterministic": True,
                "fallback_training_rows": expected_training,
                "fallback_validation_rows": row_count - expected_training,
                "fallback_fraction": self.config.fallback_fraction,
                "fallback_deterministic": True,
                "fallback_training_sha256": fallback_first_hashes[0],
                "fallback_validation_sha256": fallback_first_hashes[1],
            }
        )
        return first_metrics


StageCallback = Callable[[str, dict[str, Any]], None]
CancellationCheck = Callable[[str], None]


def fit_checkpoint(
    *,
    workspace_root: Path,
    resolved_files: Mapping[str, ResolvedSnapshotFile],
    limits: Mapping[str, Any],
    stage: StageCallback,
    check_cancelled: CancellationCheck,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _verify_engine_installation()
    config = FitConfig.from_limits(limits)
    if set(resolved_files) != {BOUNDED_SAMPLE_SNAPSHOT_KEY}:
        raise AdapterError(
            "WORKER_REQUEST_INVALID",
            f"fit manifest must contain only {BOUNDED_SAMPLE_SNAPSHOT_KEY!r}",
        )
    sample = resolved_files[BOUNDED_SAMPLE_SNAPSHOT_KEY]
    checkpoint = confined_output_path(workspace_root, config.checkpoint_path)
    if checkpoint.exists():
        raise AdapterError(
            "ARTIFACT_ALREADY_EXISTS", "checkpoint output already exists"
        )

    staging = checkpoint.with_name(f".{checkpoint.name}.part-{uuid.uuid4().hex}")
    if staging.exists():
        raise AdapterError(
            "ARTIFACT_ALREADY_EXISTS", "checkpoint staging path already exists"
        )

    frame = None
    split_metrics: dict[str, Any] = {}
    try:
        check_cancelled("load_bounded_sample")
        stage("loading_sample", {"sample_size_bytes": sample.size_bytes})
        frame, sample_metrics = _load_bounded_sample(sample.path, config)
        check_cancelled("split")

        from mostlyai import engine
        from mostlyai.engine.random_state import set_random_state

        staging.mkdir(mode=0o700)
        splitter = DeterministicSplitter(config.deterministic_split)
        split_metrics = splitter.verify_parity(len(frame))
        stage(
            "split",
            {"rows": len(frame), "training_rows": split_metrics["training_rows"]},
        )
        set_random_state(config.deterministic_split.seed)
        engine.split(tgt_data=frame, trn_val_split=splitter, workspace_dir=staging)
        actual_split = _engine_split_rows(staging)
        if (
            actual_split["training_rows"] != split_metrics["training_rows"]
            or actual_split["validation_rows"] != split_metrics["validation_rows"]
        ):
            raise AdapterError(
                "BACKEND_INCOMPATIBLE",
                "engine split rows do not match deterministic split",
                details={"expected": split_metrics, "actual": actual_split},
            )
        split_metrics.update(actual_split)
        del frame
        frame = None

        check_cancelled("analyze")
        stage("analyze", {})
        engine.analyze(workspace_dir=staging)

        check_cancelled("encode")
        stage("encode", {})
        engine.encode(workspace_dir=staging)

        check_cancelled("train")
        stage("train", {"max_epochs": config.max_epochs, "device": config.device})
        set_random_state(config.deterministic_split.seed)
        engine.train(
            model=config.model_size,
            max_training_time=config.max_minutes * 60.0,
            max_epochs=config.max_epochs,
            device=config.device,
            workspace_dir=staging,
        )

        check_cancelled("publish_checkpoint")
        stage("publishing_checkpoint", {})
        engine_workspace = directory_digest(staging)
        tags = {
            "format_version": CHECKPOINT_FORMAT_VERSION,
            "engine": {
                "distribution": ENGINE_DISTRIBUTION,
                "version": ENGINE_VERSION,
                "wheel_sha256": ENGINE_WHEEL_SHA256,
            },
            "compatibility": config.checkpoint_compatibility.to_dict(),
            "fit": {
                "bounded_sample_sha256": sample.sha256,
                "bounded_sample_rows": sample_metrics["rows"],
                "model_size": config.model_size,
                "max_epochs": config.max_epochs,
                "device": config.device,
                "random_state": config.deterministic_split.seed,
                "split": split_metrics,
            },
            "engine_workspace": engine_workspace,
            "feature_gates": {"multiprocess_clones": False},
        }
        _write_atomic_file(
            staging / CHECKPOINT_TAG_FILENAME, canonical_json_bytes(tags)
        )
        _make_checkpoint_files_read_only(staging)
        _fsync_tree(staging)
        os.replace(staging, checkpoint)
        fsync_directory(checkpoint.parent)
        published = directory_digest(checkpoint)
        artifact = {
            "kind": "model_checkpoint",
            "path": config.checkpoint_path,
            "sha256": published["sha256"],
            "size_bytes": published["size_bytes"],
            "downloadable": False,
            "release_safe": False,
            "contains_private_source_information": True,
            "metadata": {
                "tags": config.checkpoint_compatibility.to_dict(),
                "format_version": CHECKPOINT_FORMAT_VERSION,
                "workspace_file_count": published["file_count"],
            },
        }
        usage = {
            "pid": os.getpid(),
            "bounded_training_sample": sample_metrics,
            "deterministic_split": split_metrics,
            "checkpoint_sha256": published["sha256"],
            "checkpoint_size_bytes": published["size_bytes"],
            "multiprocess_clones_enabled": False,
        }
        return [artifact], usage
    finally:
        del frame
        if staging.exists():
            _remove_tree(staging)


def generate_candidate(
    *,
    workspace_root: Path,
    resolved_files: Mapping[str, ResolvedSnapshotFile],
    limits: Mapping[str, Any],
    stage: StageCallback,
    check_cancelled: CancellationCheck,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _verify_engine_installation()
    config = GenerateConfig.from_limits(limits)
    checkpoint = _confined_existing_directory(workspace_root, config.checkpoint_path)
    _verify_checkpoint_snapshot(checkpoint, resolved_files)
    checkpoint_before = directory_digest(checkpoint)
    tags = _read_checkpoint_tags(checkpoint)
    if tags.get("compatibility") != config.checkpoint_compatibility.to_dict():
        raise AdapterError(
            "BACKEND_INCOMPATIBLE", "checkpoint compatibility tags do not match request"
        )
    if tags.get("feature_gates", {}).get("multiprocess_clones") is not False:
        raise AdapterError(
            "BACKEND_INCOMPATIBLE", "checkpoint does not carry the local clone gate"
        )

    candidate = confined_output_path(workspace_root, config.candidate_output_path)
    if candidate.exists():
        raise AdapterError("ARTIFACT_ALREADY_EXISTS", "candidate output already exists")
    clone = candidate.parent / f".argn-checkpoint-clone-{uuid.uuid4().hex}"
    candidate_staging = candidate.with_name(
        f".{candidate.name}.part-{uuid.uuid4().hex}"
    )
    try:
        check_cancelled("clone_checkpoint")
        stage(
            "cloning_checkpoint", {"checkpoint_files": checkpoint_before["file_count"]}
        )
        _clone_checkpoint(checkpoint, clone)
        clone_before = directory_digest(clone)
        if clone_before["sha256"] != checkpoint_before["sha256"]:
            raise AdapterError(
                "CHECKSUM_MISMATCH",
                "checkpoint clone does not match published checkpoint",
            )

        check_cancelled("generate")
        stage(
            "generate",
            {
                "candidate_rows": config.candidate_rows,
                "batch_size": config.batch_size,
                "shard_index": config.shard_index,
            },
        )
        from mostlyai import engine
        from mostlyai.engine.random_state import set_random_state

        set_random_state(config.engine_seed)
        engine.generate(
            sample_size=config.candidate_rows,
            batch_size=config.batch_size,
            device=config.device,
            workspace_dir=clone,
        )

        synthetic_data = clone / "SyntheticData"
        _move_generated_parquet(synthetic_data, candidate_staging)
        _drop_generated_index_column(candidate_staging)
        check_cancelled("verify_candidate")
        stage("verifying_candidate", {"parquet_files": 1})
        actual_rows = _parquet_file_rows(candidate_staging)
        if actual_rows != config.candidate_rows:
            raise AdapterError(
                "OUTPUT_INVALID",
                "generated candidate row count does not match request",
                details={
                    "requested_rows": config.candidate_rows,
                    "actual_rows": actual_rows,
                },
            )
        clone_after = directory_digest(clone)
        checkpoint_after = directory_digest(checkpoint)
        if clone_after["sha256"] != clone_before["sha256"]:
            raise AdapterError(
                "CHECKSUM_MISMATCH", "generation modified checkpoint files in its clone"
            )
        if checkpoint_after["sha256"] != checkpoint_before["sha256"]:
            raise AdapterError(
                "CHECKSUM_MISMATCH", "generation modified the published checkpoint"
            )

        check_cancelled("publish_candidate")
        stage("publishing_candidate", {"rows": actual_rows})
        _fsync_file(candidate_staging)
        os.replace(candidate_staging, candidate)
        fsync_directory(candidate.parent)
        output_digest = file_digest(candidate)
        artifact = {
            "kind": "synthetic_candidate_parquet",
            "path": config.candidate_output_path,
            "sha256": output_digest["sha256"],
            "size_bytes": output_digest["size_bytes"],
            "downloadable": False,
            "release_safe": False,
            "contains_private_source_information": False,
            "metadata": {
                "rows": actual_rows,
                "shard_index": config.shard_index,
                "seed_count": config.seed_count,
                "engine_seed": config.engine_seed,
                "checkpoint_sha256": checkpoint_before["sha256"],
            },
        }
        usage = {
            "pid": os.getpid(),
            "requested_rows": config.candidate_rows,
            "actual_rows": actual_rows,
            "candidate_parquet_files": 1,
            "checkpoint_sha256_before": checkpoint_before["sha256"],
            "checkpoint_sha256_after": checkpoint_after["sha256"],
            "checkpoint_unchanged": True,
            "clone_checkpoint_unchanged": True,
            "process_count": 1,
            "multiprocess_clones_enabled": False,
        }
        return [artifact], usage
    finally:
        candidate_staging.unlink(missing_ok=True)
        if clone.exists():
            _remove_tree(clone)


def directory_digest(root: Path) -> dict[str, Any]:
    if not root.is_dir():
        raise AdapterError(
            "CHECKSUM_MISMATCH", f"checkpoint directory is missing: {root.name}"
        )
    digest = hashlib.sha256()
    size_bytes = 0
    files = _regular_files(root)
    for path in files:
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        file_size = path.stat().st_size
        digest.update(file_size.to_bytes(8, "big"))
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
        size_bytes += file_size
    return {
        "sha256": digest.hexdigest(),
        "size_bytes": size_bytes,
        "file_count": len(files),
    }


def file_digest(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise AdapterError("CHECKSUM_MISMATCH", "candidate is not a regular file")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return {"sha256": digest.hexdigest(), "size_bytes": path.stat().st_size}


def _load_bounded_sample(path: Path, config: FitConfig) -> tuple[Any, dict[str, Any]]:
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    rows = parquet.metadata.num_rows
    if rows < 1 or rows > MAX_BOUNDED_TRAINING_ROWS:
        raise AdapterError(
            "RESOURCE_LIMIT",
            "bounded training sample exceeds the absolute worker row cap",
            details={"rows": rows, "max_training_rows": MAX_BOUNDED_TRAINING_ROWS},
        )
    columns = len(parquet.schema_arrow.names)
    if not 1 <= columns <= MAX_TRAINING_COLUMNS:
        raise AdapterError(
            "RESOURCE_LIMIT",
            "bounded training sample column count exceeds the worker cap",
            details={"columns": columns, "max_training_columns": MAX_TRAINING_COLUMNS},
        )
    if "__sts_row_id" in parquet.schema_arrow.names:
        raise AdapterError(
            "WORKER_REQUEST_INVALID",
            "bounded training sample must not expose __sts_row_id",
        )
    if len(parquet.schema_arrow.names) != len(set(parquet.schema_arrow.names)):
        raise AdapterError(
            "WORKER_REQUEST_INVALID", "bounded training sample has duplicate columns"
        )
    table = parquet.read()
    arrow_bytes = table.nbytes
    estimated_deep_bytes = _estimate_pandas_deep_bytes(table)
    projected_bytes = (estimated_deep_bytes * 3 + 1) // 2
    if projected_bytes > config.worker_memory_lease_bytes:
        raise AdapterError(
            "RESOURCE_LIMIT",
            "bounded training sample exceeds the pre-materialization memory lease",
            details={
                "estimated_pandas_deep_bytes": estimated_deep_bytes,
                "projected_bytes": projected_bytes,
                "worker_memory_lease_bytes": config.worker_memory_lease_bytes,
            },
        )
    frame = table.to_pandas()
    del table
    actual_deep_bytes = int(frame.memory_usage(index=True, deep=True).sum())
    actual_projected_bytes = (actual_deep_bytes * 3 + 1) // 2
    if actual_projected_bytes > config.worker_memory_lease_bytes:
        raise AdapterError(
            "RESOURCE_LIMIT",
            "materialized bounded training sample exceeds the memory lease",
            details={
                "actual_pandas_deep_bytes": actual_deep_bytes,
                "projected_bytes": actual_projected_bytes,
                "worker_memory_lease_bytes": config.worker_memory_lease_bytes,
            },
        )
    return frame, {
        "rows": len(frame),
        "columns": len(frame.columns),
        "arrow_bytes": arrow_bytes,
        "estimated_pandas_deep_bytes": estimated_deep_bytes,
        "actual_pandas_deep_bytes": actual_deep_bytes,
        "lease_projection_bytes": actual_projected_bytes,
        "worker_memory_lease_bytes": config.worker_memory_lease_bytes,
    }


def _estimate_pandas_deep_bytes(table: Any) -> int:
    import pyarrow as pa
    import pyarrow.compute as pc

    rows = table.num_rows
    total = 132
    for field, column in zip(table.schema, table.columns, strict=True):
        data_type = field.type
        nonnull = rows - column.null_count
        if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
            byte_lengths = pc.sum(pc.binary_length(column)).as_py() or 0
            total += rows * 8 + nonnull * 80 + byte_lengths * 4
        elif pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
            byte_lengths = pc.sum(pc.binary_length(column)).as_py() or 0
            total += rows * 8 + nonnull * 64 + byte_lengths
        elif pa.types.is_decimal(data_type):
            total += rows * 8 + nonnull * 128
        elif pa.types.is_boolean(data_type) and column.null_count:
            total += rows * 36
        elif pa.types.is_nested(data_type):
            raise AdapterError(
                "WORKER_REQUEST_INVALID",
                "nested Arrow types are not supported by the ARGN adapter",
            )
        else:
            total += max(rows * 8, column.nbytes)
    return max(total, table.nbytes)


def _engine_split_rows(workspace: Path) -> dict[str, int]:
    target_data = workspace / "OriginalData" / "tgt-data"
    training_rows = _parquet_rows(target_data, suffix="-trn.parquet")
    validation_rows = _parquet_rows(target_data, suffix="-val.parquet")
    return {"training_rows": training_rows, "validation_rows": validation_rows}


def _split_metrics(
    training: Any, validation: Any, total: int, config: DeterministicSplitConfig
) -> dict[str, Any]:
    training_values = training.to_numpy(copy=False)
    validation_values = validation.to_numpy(copy=False)
    if len(training) + len(validation) != total:
        raise AdapterError(
            "BACKEND_INCOMPATIBLE", "deterministic split is not complete"
        )
    return {
        "callable_fraction": config.callable_fraction,
        "seed": config.seed,
        "training_rows": len(training),
        "validation_rows": len(validation),
        "training_sha256": hashlib.sha256(training_values).hexdigest(),
        "validation_sha256": hashlib.sha256(validation_values).hexdigest(),
    }


def _verify_checkpoint_snapshot(
    checkpoint: Path, resolved_files: Mapping[str, ResolvedSnapshotFile]
) -> None:
    if not resolved_files:
        raise AdapterError(
            "WORKER_REQUEST_INVALID", "generate requires checkpoint snapshot files"
        )
    checkpoint_files = {
        path.resolve(strict=True) for path in _regular_files(checkpoint)
    }
    supplied_files = {
        entry.path.resolve(strict=True) for entry in resolved_files.values()
    }
    if supplied_files != checkpoint_files:
        raise AdapterError(
            "WORKER_REQUEST_INVALID",
            "generate manifest must contain every checkpoint file and no source capability",
            details={
                "expected_checkpoint_files": len(checkpoint_files),
                "supplied_checkpoint_files": len(supplied_files),
            },
        )


def _read_checkpoint_tags(checkpoint: Path) -> dict[str, Any]:
    import json

    path = checkpoint / CHECKPOINT_TAG_FILENAME
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as error:
        raise AdapterError(
            "CHECKSUM_MISMATCH", "checkpoint tags are missing or invalid"
        ) from error
    if (
        not isinstance(value, dict)
        or value.get("format_version") != CHECKPOINT_FORMAT_VERSION
    ):
        raise AdapterError("BACKEND_INCOMPATIBLE", "unsupported checkpoint tag format")
    engine = value.get("engine")
    if (
        not isinstance(engine, dict)
        or engine.get("version") != ENGINE_VERSION
        or engine.get("wheel_sha256") != ENGINE_WHEEL_SHA256
    ):
        raise AdapterError(
            "BACKEND_INCOMPATIBLE",
            "checkpoint engine identity does not match pinned engine",
        )
    return value


def _clone_checkpoint(source: Path, destination: Path) -> None:
    if destination.exists():
        raise AdapterError(
            "ARTIFACT_ALREADY_EXISTS", "checkpoint clone path already exists"
        )
    shutil.copytree(source, destination, symlinks=False, copy_function=shutil.copy2)
    _make_checkpoint_files_read_only(destination)


def _move_generated_parquet(synthetic_data: Path, candidate_staging: Path) -> None:
    if not synthetic_data.is_dir():
        raise AdapterError("OUTPUT_INVALID", "ARGN did not create SyntheticData")
    generated = sorted(synthetic_data.rglob("*.parquet"))
    all_regular = _regular_files(synthetic_data)
    if len(generated) != 1 or set(generated) != set(all_regular):
        raise AdapterError(
            "OUTPUT_INVALID", "ARGN must create exactly one candidate parquet shard"
        )
    os.replace(generated[0], candidate_staging)
    _remove_tree(synthetic_data)


def _drop_generated_index_column(candidate: Path) -> None:
    import pyarrow.parquet as pq

    schema = pq.read_schema(candidate)
    if "__index_level_0__" not in schema.names:
        return
    table = pq.read_table(candidate).drop(["__index_level_0__"])
    cleaned = candidate.with_suffix(candidate.suffix + ".cleaning")
    pq.write_table(table, cleaned)
    os.replace(cleaned, candidate)


def _parquet_file_rows(path: Path) -> int:
    import pyarrow.parquet as pq

    return pq.ParquetFile(path).metadata.num_rows


def _parquet_rows(root: Path, *, suffix: str | None = None) -> int:
    import pyarrow.parquet as pq

    total = 0
    for path in _regular_files(root):
        if path.suffix != ".parquet" or (
            suffix is not None and not path.name.endswith(suffix)
        ):
            continue
        total += pq.ParquetFile(path).metadata.num_rows
    return total


def _regular_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise AdapterError(
                "CHECKSUM_MISMATCH", f"workspace contains a symbolic link: {path.name}"
            )
        if path.is_file():
            files.append(path)
        elif not path.is_dir():
            raise AdapterError(
                "CHECKSUM_MISMATCH",
                f"workspace contains a non-regular entry: {path.name}",
            )
    return files


def _confined_existing_directory(root: Path, relative: str) -> Path:
    workspace = root.resolve(strict=True)
    candidate = (workspace / validate_workspace_relative_path(relative)).resolve(
        strict=True
    )
    if not candidate.is_relative_to(workspace) or not candidate.is_dir():
        raise AdapterError(
            "WORKER_REQUEST_INVALID", "checkpoint path is not a confined directory"
        )
    return candidate


def _write_atomic_file(path: Path, data: bytes) -> None:
    part = path.with_name(f".{path.name}.part-{uuid.uuid4().hex}")
    fd = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(part, path)
    fsync_directory(path.parent)


def _fsync_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_tree(root: Path) -> None:
    directories = {root}
    for file in _regular_files(root):
        fd = os.open(file, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        directories.add(file.parent)
    for directory in sorted(
        directories, key=lambda path: len(path.parts), reverse=True
    ):
        fsync_directory(directory)


def _make_checkpoint_files_read_only(root: Path) -> None:
    for path in _regular_files(root):
        path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)


def _remove_tree(path: Path) -> None:
    if not path.exists():
        return
    for entry in path.rglob("*"):
        if entry.is_dir():
            entry.chmod(0o700)
        elif entry.exists():
            entry.chmod(0o600)
    path.chmod(0o700)
    shutil.rmtree(path)


def _verify_engine_installation() -> None:
    try:
        installed = importlib.metadata.version(ENGINE_DISTRIBUTION)
    except importlib.metadata.PackageNotFoundError as error:
        raise AdapterError(
            "BACKEND_INCOMPATIBLE", "mostlyai-engine is not installed"
        ) from error
    if installed != ENGINE_VERSION:
        raise AdapterError(
            "BACKEND_INCOMPATIBLE",
            f"expected {ENGINE_DISTRIBUTION}=={ENGINE_VERSION}, observed {installed}",
        )


def _argn_limits(limits: Mapping[str, Any]) -> dict[str, Any]:
    expected = {"worker_rss_bytes", "max_process_tree_rss_bytes", "argn"}
    if set(limits) != expected:
        raise AdapterError(
            "WORKER_REQUEST_INVALID",
            "worker limits must contain worker and process-tree RSS leases plus limits.argn",
        )
    if limits["max_process_tree_rss_bytes"] != limits["worker_rss_bytes"]:
        raise AdapterError(
            "WORKER_REQUEST_INVALID",
            "worker and process-tree RSS leases must match",
        )
    return _object(limits["argn"], "limits.argn")


def _worker_memory_lease(limits: Mapping[str, Any]) -> int:
    return _positive_int(limits.get("worker_rss_bytes"), "worker_rss_bytes")


def _object(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise AdapterError("WORKER_REQUEST_INVALID", f"{field} must be a JSON object")
    return dict(value)


def _exact_keys(
    value: Mapping[str, Any], expected: set[str] | frozenset[str], field: str
) -> None:
    actual = set(value)
    if actual != set(expected):
        raise AdapterError(
            "WORKER_REQUEST_INVALID",
            f"invalid {field} fields",
            details={
                "missing": sorted(set(expected) - actual),
                "extra": sorted(actual - set(expected)),
            },
        )


def _string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise AdapterError(
            "WORKER_REQUEST_INVALID", f"{field} must be a nonempty string"
        )
    return value


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise AdapterError(
            "WORKER_REQUEST_INVALID", f"{field} must be a positive integer"
        )
    return value


def _uint32(value: object, field: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= 0xFFFFFFFF
    ):
        raise AdapterError(
            "WORKER_REQUEST_INVALID", f"{field} must be a uint32 integer"
        )
    return value


def _number(value: object, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise AdapterError("WORKER_REQUEST_INVALID", f"{field} must be a finite number")
    return float(value)


def _sha256(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise AdapterError(
            "WORKER_REQUEST_INVALID", f"{field} must be a lowercase SHA-256 digest"
        )
    return value
