from __future__ import annotations

import argparse
import os
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .adapter import AdapterError, CancellationRequested, fit_checkpoint, generate_candidate
from .protocol import (
    PROTOCOL_VERSION,
    WorkerEvent,
    WorkerEventWriter,
    WorkerResultEnvelope,
    confined_output_path,
    read_request,
    resolve_manifest_snapshot,
    write_result_atomic,
)


class EventEmitter:
    def __init__(self, path: Path) -> None:
        self.writer = WorkerEventWriter(path)
        self.sequence = 0

    def emit(self, stage: str, metrics: dict[str, Any], *, message_code: str | None = None) -> None:
        self.sequence += 1
        self.writer.append(
            WorkerEvent(
                version=PROTOCOL_VERSION,
                sequence=self.sequence,
                timestamp=_timestamp(),
                stage=stage,
                completed=1 if stage in {"completed", "cancelled", "failed"} else 0,
                total=1,
                unit="operation",
                message_code=message_code or f"ARGN_{stage.upper()}",
                metrics=metrics,
            )
        )


class CancellationToken:
    def __init__(self, path: Path) -> None:
        self.path = path

    def check(self, stage: str) -> None:
        if self.path.exists():
            raise CancellationRequested(stage)


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
        message_code="ARGN_STARTED",
    )
    token = CancellationToken(cancellation_path)
    try:
        if request.worker_kind != "argn":
            raise AdapterError("WORKER_REQUEST_INVALID", "request worker_kind must be argn")
        token.check("resolve_manifest")
        resolved_files = resolve_manifest_snapshot(request.manifest_snapshot)
        token.check("dispatch")
        if request.operation == "fit":
            artifacts, usage = fit_checkpoint(
                workspace_root=root,
                resolved_files=resolved_files,
                limits=request.limits,
                stage=emitter.emit,
                check_cancelled=token.check,
            )
        elif request.operation == "generate":
            artifacts, usage = generate_candidate(
                workspace_root=root,
                resolved_files=resolved_files,
                limits=request.limits,
                stage=emitter.emit,
                check_cancelled=token.check,
            )
        else:
            raise AdapterError(
                "OPERATION_UNSUPPORTED", f"unsupported argn worker operation: {request.operation}"
            )
        emitter.emit("completed", {"artifacts": len(artifacts)}, message_code="ARGN_COMPLETED")
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
    except CancellationRequested as error:
        emitter.emit("cancelled", error.details, message_code="ARGN_CANCELLED")
        write_result_atomic(result_path, _error_result("cancelled", error))
        return 0
    except AdapterError as error:
        emitter.emit("failed", {"error_code": error.code}, message_code=error.code)
        write_result_atomic(result_path, _error_result("failure", error))
        return 2
    except ValueError as error:
        code = "CHECKSUM_MISMATCH" if "mismatch" in str(error).lower() else "WORKER_REQUEST_INVALID"
        adapted = AdapterError(code, str(error))
        emitter.emit("failed", {"error_code": adapted.code}, message_code=adapted.code)
        write_result_atomic(result_path, _error_result("failure", adapted))
        return 2
    except BaseException as error:
        traceback.print_exc()
        adapted = AdapterError(
            "WORKER_FAILED",
            f"ARGN worker failed during {request.operation}",
            details={"exception_type": type(error).__name__},
        )
        emitter.emit("failed", {"error_code": adapted.code}, message_code=adapted.code)
        write_result_atomic(result_path, _error_result("failure", adapted))
        return 2


def _error_result(status: str, error: AdapterError) -> WorkerResultEnvelope:
    return WorkerResultEnvelope(
        version=PROTOCOL_VERSION,
        status=status,
        artifacts=[],
        resource_usage={"pid": os.getpid()},
        error={"code": error.code, "message": error.message, "details": error.details},
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


def _timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m argn_worker")
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
