from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import time
import traceback
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

from .evaluation import EVALUATION_CONFIG_VERSION, Table, evaluate_advanced
from .protocol import (
    PROTOCOL_VERSION,
    ResolvedSnapshotFile,
    WorkerEvent,
    WorkerEventWriter,
    WorkerRequestEnvelope,
    WorkerResultEnvelope,
    canonical_json_bytes,
    confined_output_path,
    fsync_directory,
    read_request,
    resolve_manifest_snapshot,
    validate_workspace_relative_path,
    write_result_atomic,
)

_EVALUATION_OPERATIONS = {
    "evaluate",
    "evaluate_advanced",
    "advanced_evaluation",
    "pairwise_evaluation",
    "c2st_evaluation",
    "downstream_utility_evaluation",
    "empirical_privacy_evaluation",
}


class EvaluationWorkerError(RuntimeError):
    def __init__(self, code: str, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


class EventEmitter:
    def __init__(self, path: Path) -> None:
        self.writer = WorkerEventWriter(path)
        self.sequence = 0

    def emit(
        self,
        stage: str,
        metrics: dict[str, Any],
        *,
        message_code: str | None = None,
    ) -> None:
        self.sequence += 1
        self.writer.append(
            WorkerEvent(
                version=PROTOCOL_VERSION,
                sequence=self.sequence,
                timestamp=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                stage=stage,
                completed=1 if stage in {"completed", "cancelled", "failed"} else 0,
                total=1,
                unit="operation",
                message_code=message_code or f"EVAL_{stage.upper()}",
                metrics=metrics,
            )
        )


class CancellationToken:
    def __init__(self, path: Path) -> None:
        self.path = path

    def check(self, stage: str) -> None:
        if self.path.exists():
            raise EvaluationWorkerError(
                "CANCELLED",
                "evaluation cancelled",
                details={"stage": stage},
            )


def _assert_supplied_path(root: Path, supplied: Path, *, existing: bool) -> Path:
    absolute = supplied.absolute()
    candidate = absolute.resolve(strict=True) if existing else absolute
    parent = candidate.parent.resolve(strict=True)
    if not parent.is_relative_to(root) or (existing and not candidate.is_relative_to(root)):
        raise ValueError(f"worker protocol path escapes workspace: {supplied}")
    if not existing and candidate.is_symlink():
        raise ValueError(f"worker protocol path must not be a symlink: {supplied}")
    return candidate


def _wait_for_cancellation(cancellation_path: Path) -> None:
    while not cancellation_path.exists():
        time.sleep(0.01)


def _error_result(status: str, error: EvaluationWorkerError) -> WorkerResultEnvelope:
    return WorkerResultEnvelope(
        version=PROTOCOL_VERSION,
        status=status,
        artifacts=[],
        resource_usage={"pid": os.getpid()},
        error={"code": error.code, "message": error.message, "details": error.details},
    )


def _protocol_result(
    status: str,
    *,
    code: str | None = None,
    message: str | None = None,
    resource_usage: dict[str, object] | None = None,
) -> WorkerResultEnvelope:
    error = None
    if code is not None:
        error = {"code": code, "message": message or code, "details": {}}
    return WorkerResultEnvelope(
        version=PROTOCOL_VERSION,
        status=status,
        artifacts=[],
        resource_usage=resource_usage or {},
        error=error,
    )


def _load_json_table(path: Path) -> tuple[Table, dict[str, str] | None]:
    payload = json.loads(path.read_bytes())
    embedded_types: dict[str, str] | None = None
    if isinstance(payload, dict) and "columns" in payload:
        raw_columns = payload["columns"]
        raw_types = payload.get("column_types")
        if raw_types is not None:
            if not isinstance(raw_types, dict):
                raise EvaluationWorkerError(
                    "WORKER_REQUEST_INVALID",
                    "embedded column_types must be an object",
                )
            embedded_types = {str(name): str(kind) for name, kind in raw_types.items()}
        payload = raw_columns
    if not isinstance(payload, (dict, list)):
        raise EvaluationWorkerError(
            "WORKER_REQUEST_INVALID",
            f"unsupported JSON table shape in {path.name}",
        )
    return Table.from_data(payload), embedded_types


def _load_csv_table(path: Path) -> tuple[Table, None]:
    columns: dict[str, list[Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise EvaluationWorkerError("WORKER_REQUEST_INVALID", "CSV table requires a header")
        columns = {name: [] for name in reader.fieldnames}
        for record in reader:
            for name in columns:
                value = record[name]
                columns[name].append(None if value == "" else value)
    return Table.from_data(columns), None


def _load_npz_table(path: Path) -> tuple[Table, None]:
    try:
        with np.load(path, allow_pickle=False) as payload:
            columns = {name: np.asarray(payload[name]) for name in payload.files}
    except ValueError as error:
        raise EvaluationWorkerError(
            "WORKER_REQUEST_INVALID",
            "NPZ evaluation input must not contain pickled object arrays",
        ) from error
    return Table.from_data(columns), None


def load_table(path: Path) -> tuple[Table, dict[str, str] | None]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return _load_json_table(path)
    if suffix == ".csv":
        return _load_csv_table(path)
    if suffix == ".npz":
        return _load_npz_table(path)
    raise EvaluationWorkerError(
        "WORKER_REQUEST_INVALID",
        f"evaluation input format is unsupported: {suffix or '<none>'}",
    )


def _mapping(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise EvaluationWorkerError("WORKER_REQUEST_INVALID", f"{field} must be an object")
    return dict(value)


def _configuration(
    request: WorkerRequestEnvelope,
    resolved_files: Mapping[str, ResolvedSnapshotFile],
) -> dict[str, Any]:
    raw = request.limits.get("evaluation_config", request.limits.get("config"))
    config_key = request.limits.get("evaluation_config_manifest_key")
    if raw is not None and config_key is not None:
        raise EvaluationWorkerError(
            "WORKER_REQUEST_INVALID",
            "supply evaluation config inline or by manifest key, not both",
        )
    if config_key is not None:
        if not isinstance(config_key, str) or config_key not in resolved_files:
            raise EvaluationWorkerError(
                "WORKER_REQUEST_INVALID",
                "evaluation_config_manifest_key does not name a snapshot file",
            )
        raw = json.loads(resolved_files[config_key].path.read_bytes())
    config = _mapping(raw or {}, "evaluation_config")
    version = config.pop("version", config.pop("evaluation_config_version", None))
    if version != EVALUATION_CONFIG_VERSION:
        raise EvaluationWorkerError(
            "WORKER_REQUEST_INVALID",
            f"evaluation config version must be {EVALUATION_CONFIG_VERSION}",
        )
    return config


def _input_keys(request: WorkerRequestEnvelope) -> dict[str, str]:
    raw = request.limits.get(
        "inputs",
        {
            "real_train_eval": "real_train_eval",
            "real_holdout": "real_holdout",
            "synthetic": "synthetic",
        },
    )
    mapping = _mapping(raw, "limits.inputs")
    required = {"real_train_eval", "real_holdout", "synthetic"}
    if set(mapping) != required or any(not isinstance(value, str) for value in mapping.values()):
        raise EvaluationWorkerError(
            "WORKER_REQUEST_INVALID",
            "limits.inputs must map real_train_eval, real_holdout, and synthetic to manifest keys",
        )
    return {name: str(value) for name, value in mapping.items()}


def _column_types(config: dict[str, Any], embedded: list[dict[str, str] | None]) -> dict[str, str]:
    raw = config.pop("column_types", None)
    if raw is None:
        available = [value for value in embedded if value is not None]
        if not available:
            raise EvaluationWorkerError(
                "WORKER_REQUEST_INVALID",
                "evaluation config requires column_types",
            )
        if any(value != available[0] for value in available[1:]):
            raise EvaluationWorkerError(
                "WORKER_REQUEST_INVALID",
                "embedded column_types do not match",
            )
        return available[0]
    mapping = _mapping(raw, "evaluation_config.column_types")
    if any(not isinstance(value, str) for value in mapping.values()):
        raise EvaluationWorkerError(
            "WORKER_REQUEST_INVALID",
            "column_types values must be strings",
        )
    return {name: str(kind) for name, kind in mapping.items()}


def _sections_for_operation(operation: str) -> list[str] | None:
    return {
        "pairwise_evaluation": ["pairwise"],
        "c2st_evaluation": ["c2st"],
        "downstream_utility_evaluation": ["downstream_utility"],
        "empirical_privacy_evaluation": ["empirical_privacy"],
    }.get(operation)


def _atomic_json(path: Path, payload: object) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = canonical_json_bytes(payload)
    part = path.with_name(f"{path.name}.part")
    try:
        with part.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(part, path)
        fsync_directory(path.parent)
    except BaseException:
        part.unlink(missing_ok=True)
        raise
    return {
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": len(data),
    }


def execute_evaluation(
    request: WorkerRequestEnvelope,
    resolved_files: Mapping[str, ResolvedSnapshotFile],
    root: Path,
    token: CancellationToken,
    emitter: EventEmitter,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    config = _configuration(request, resolved_files)
    input_keys = _input_keys(request)
    loaded: dict[str, Table] = {}
    embedded_types: list[dict[str, str] | None] = []
    for role, key in input_keys.items():
        if key not in resolved_files:
            raise EvaluationWorkerError(
                "WORKER_REQUEST_INVALID",
                f"limits.inputs.{role} does not name a snapshot file",
            )
        token.check(f"load_{role}")
        table, types = load_table(resolved_files[key].path)
        loaded[role] = table
        embedded_types.append(types)
    column_types = _column_types(config, embedded_types)
    supported = {
        "seed",
        "mode",
        "report_scope",
        "target",
        "task",
        "secret_groups",
        "auxiliary_groups",
        "public_categories",
        "public_bins",
        "sections",
        "output_path",
    }
    extra = set(config) - supported
    if extra:
        raise EvaluationWorkerError(
            "WORKER_REQUEST_INVALID",
            f"unsupported evaluation config fields: {sorted(extra)}",
        )
    output_path_value = config.pop(
        "output_path",
        f"jobs/{request.job_id}/attempt-{request.attempt}/advanced-evaluation.json",
    )
    if not isinstance(output_path_value, str):
        raise EvaluationWorkerError(
            "WORKER_REQUEST_INVALID",
            "evaluation output_path must be a string",
        )
    relative_output = validate_workspace_relative_path(output_path_value)
    output_path = confined_output_path(root, relative_output)
    requested_sections = _sections_for_operation(request.operation)
    config_sections = config.pop("sections", None)
    if requested_sections is not None and config_sections is not None:
        raise EvaluationWorkerError(
            "WORKER_REQUEST_INVALID",
            "section-specific operations must not also set sections",
        )
    if config_sections is not None:
        if not isinstance(config_sections, list) or any(
            not isinstance(value, str) for value in config_sections
        ):
            raise EvaluationWorkerError(
                "WORKER_REQUEST_INVALID",
                "evaluation sections must be a list of strings",
            )
        requested_sections = config_sections
    emitter.emit(
        "evaluating",
        {"sections": requested_sections or ["all"]},
        message_code="EVALUATION_STARTED",
    )
    token.check("evaluate")
    evaluation = evaluate_advanced(
        loaded["real_train_eval"],
        loaded["real_holdout"],
        loaded["synthetic"],
        column_types,
        sections=requested_sections,
        **config,
    )
    token.check("publish")
    digest = _atomic_json(output_path, evaluation)
    artifact = {
        "kind": "internal_diagnostic_report_json",
        "path": relative_output,
        "sha256": digest["sha256"],
        "size_bytes": digest["size_bytes"],
        "downloadable": False,
        "release_safe": False,
        "contains_private_source_information": True,
        "metadata": {
            "evaluation_config_version": EVALUATION_CONFIG_VERSION,
            "mode": evaluation["mode"],
            "report_scope": evaluation["report_scope"],
            "empirical_privacy_included": "empirical_privacy" in evaluation,
        },
    }
    usage = {
        "pid": os.getpid(),
        "real_train_eval_rows": loaded["real_train_eval"].row_count,
        "real_holdout_rows": loaded["real_holdout"].row_count,
        "synthetic_rows": loaded["synthetic"].row_count,
        "output_bytes": digest["size_bytes"],
    }
    return [artifact], usage


def run_worker(request_path: Path, events_path: Path, result_path: Path) -> int:
    try:
        request = read_request(request_path)
        root = Path(request.manifest_snapshot.workspace_root).resolve(strict=True)
        _assert_supplied_path(root, request_path, existing=True)
        events_path = _assert_supplied_path(root, events_path, existing=False)
        result_path = _assert_supplied_path(root, result_path, existing=False)
        cancellation_path = confined_output_path(root, request.cancellation_path)
    except Exception:
        traceback.print_exc()
        return 2

    emitter = EventEmitter(events_path)
    emitter.emit(
        "starting",
        {"pid": os.getpid(), "operation": request.operation},
        message_code="WORKER_STARTED",
    )
    token = CancellationToken(cancellation_path)
    try:
        if request.worker_kind != "eval":
            raise EvaluationWorkerError(
                "WORKER_REQUEST_INVALID",
                "request worker_kind must be eval",
            )
        if request.operation == "protocol_missing_result":
            return 0
        if request.operation == "protocol_nonzero_exit":
            return 7
        if request.operation == "protocol_wait_for_cancel":
            _wait_for_cancellation(cancellation_path)
            emitter.emit("cancelled", {}, message_code="WORKER_CANCELLED")
            write_result_atomic(
                result_path,
                _protocol_result("cancelled", code="CANCELLED"),
            )
            return 0
        if request.operation == "protocol_allocate":
            allocation = request.limits.get("probe_allocation_bytes", 0)
            if isinstance(allocation, bool) or not isinstance(allocation, int) or allocation < 1:
                raise EvaluationWorkerError(
                    "WORKER_REQUEST_INVALID",
                    "protocol_allocate requires positive limits.probe_allocation_bytes",
                )
            memory = bytearray(allocation)
            for offset in range(0, allocation, 4096):
                memory[offset] = 1
            _wait_for_cancellation(cancellation_path)
            emitter.emit("cancelled", {}, message_code="WORKER_CANCELLED")
            write_result_atomic(
                result_path,
                _protocol_result(
                    "cancelled",
                    code="CANCELLED",
                    resource_usage={"allocated_bytes": len(memory)},
                ),
            )
            return 0
        resolved_files = resolve_manifest_snapshot(request.manifest_snapshot)
        if request.operation == "protocol_probe":
            emitter.emit("completed", {}, message_code="WORKER_COMPLETED")
            write_result_atomic(
                result_path,
                _protocol_result(
                    "success",
                    resource_usage={
                        "pid": os.getpid(),
                        "resolved_snapshot_files": len(resolved_files),
                    },
                ),
            )
            return 0
        if request.operation not in _EVALUATION_OPERATIONS:
            raise EvaluationWorkerError(
                "OPERATION_UNSUPPORTED",
                f"unsupported eval worker operation: {request.operation}",
            )
        artifacts, usage = execute_evaluation(
            request,
            resolved_files,
            root,
            token,
            emitter,
        )
        emitter.emit(
            "completed",
            {"artifacts": len(artifacts)},
            message_code="EVALUATION_COMPLETED",
        )
        write_result_atomic(
            result_path,
            WorkerResultEnvelope(
                version=PROTOCOL_VERSION,
                status="success",
                artifacts=artifacts,
                resource_usage=usage,
                error=None,
            ),
        )
        return 0
    except EvaluationWorkerError as error:
        status = "cancelled" if error.code == "CANCELLED" else "failure"
        emitter.emit(status, {"error_code": error.code}, message_code=error.code)
        write_result_atomic(result_path, _error_result(status, error))
        return 0 if status == "cancelled" else 2
    except ValueError as error:
        code = "CHECKSUM_MISMATCH" if "mismatch" in str(error).lower() else "WORKER_REQUEST_INVALID"
        adapted = EvaluationWorkerError(code, str(error))
        emitter.emit("failed", {"error_code": code}, message_code=code)
        write_result_atomic(result_path, _error_result("failure", adapted))
        return 2
    except BaseException as error:
        traceback.print_exc()
        adapted = EvaluationWorkerError(
            "WORKER_FAILED",
            f"eval worker failed during {request.operation}",
            details={"exception_type": type(error).__name__},
        )
        emitter.emit("failed", {"error_code": adapted.code}, message_code=adapted.code)
        write_result_atomic(result_path, _error_result("failure", adapted))
        return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m eval_worker")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--request", type=Path, required=True)
    run_parser.add_argument("--events", type=Path, required=True)
    run_parser.add_argument("--result", type=Path, required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "run":
        return run_worker(args.request, args.events, args.result)
    raise AssertionError("unreachable")
