from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Literal

import psutil
from pydantic import BaseModel, ConfigDict

from .protocol import (
    WorkerError,
    WorkerRequestEnvelope,
    WorkerResultEnvelope,
    confined_output_path,
    fsync_directory,
    read_request,
    read_result,
    write_result_atomic,
)


class WorkerRuntime(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["argn", "dpmm", "eval"]
    interpreter: Path
    module: str
    source_root: Path


class WorkerExecution(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    request_id: str
    worker_kind: Literal["argn", "dpmm", "eval"]
    exit_code: int
    peak_process_tree_rss_bytes: int
    result: WorkerResultEnvelope
    stdout_path: Path
    stderr_path: Path


class WorkerSupervisor:
    def __init__(
        self,
        project_root: Path | None = None,
        *,
        poll_interval_seconds: float = 1.0,
        termination_grace_seconds: float = 2.0,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if termination_grace_seconds <= 0:
            raise ValueError("termination_grace_seconds must be positive")
        self.project_root = (
            project_root.resolve(strict=True)
            if project_root is not None
            else Path(__file__).resolve().parents[4]
        )
        self.poll_interval_seconds = poll_interval_seconds
        self.termination_grace_seconds = termination_grace_seconds

    def runtime_for(self, kind: Literal["argn", "dpmm", "eval"]) -> WorkerRuntime:
        worker_root = self.project_root / "workers" / kind
        interpreter = worker_root / ".venv" / "bin" / "python"
        source_root = worker_root / "src"
        if not interpreter.is_file():
            raise FileNotFoundError(f"locked {kind} worker interpreter not found: {interpreter}")
        if not source_root.is_dir():
            raise FileNotFoundError(f"{kind} worker source root not found: {source_root}")
        return WorkerRuntime(
            kind=kind,
            interpreter=interpreter,
            module=f"{kind}_worker",
            source_root=source_root,
        )

    def request_cancellation(
        self, request: WorkerRequestEnvelope, *, reason: str = "cancelled"
    ) -> Path:
        root = Path(request.manifest_snapshot.workspace_root).resolve(strict=True)
        path = confined_output_path(root, request.cancellation_path)
        _create_cancellation_file(path, reason)
        return path

    async def run(
        self,
        request_path: Path,
        events_path: Path,
        result_path: Path,
        *,
        stdout_path: Path | None = None,
        stderr_path: Path | None = None,
    ) -> WorkerExecution:
        request = read_request(request_path)
        root = Path(request.manifest_snapshot.workspace_root).resolve(strict=True)
        _confine_supplied_path(root, request_path, existing=True)
        events_path = _confine_supplied_path(root, events_path, existing=False)
        result_path = _confine_supplied_path(root, result_path, existing=False)
        stdout_path = _confine_supplied_path(
            root, stdout_path or result_path.with_name("worker.stdout"), existing=False
        )
        stderr_path = _confine_supplied_path(
            root, stderr_path or result_path.with_name("worker.stderr"), existing=False
        )
        cancellation_path = confined_output_path(root, request.cancellation_path)
        runtime = self.runtime_for(request.worker_kind)
        max_rss = _optional_positive_int(
            request.limits.get("max_process_tree_rss_bytes"),
            "limits.max_process_tree_rss_bytes",
        )

        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        stderr_path.parent.mkdir(parents=True, exist_ok=True)
        environment = os.environ.copy()
        existing_pythonpath = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            f"{runtime.source_root}{os.pathsep}{existing_pythonpath}"
            if existing_pythonpath
            else str(runtime.source_root)
        )

        with (
            stdout_path.open("ab", buffering=0) as stdout_handle,
            stderr_path.open("ab", buffering=0) as stderr_handle,
        ):
            process = await asyncio.create_subprocess_exec(
                str(runtime.interpreter),
                "-m",
                runtime.module,
                "run",
                "--request",
                str(request_path),
                "--events",
                str(events_path),
                "--result",
                str(result_path),
                cwd=runtime.source_root.parent,
                env=environment,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=stdout_handle,
                stderr=stderr_handle,
            )
            peak_rss = 0
            resource_exceeded = False
            cancellation_seen_at: float | None = (
                time.monotonic() if cancellation_path.exists() else None
            )

            try:
                while process.returncode is None:
                    await asyncio.sleep(self.poll_interval_seconds)
                    rss = _process_tree_rss(process.pid)
                    peak_rss = max(peak_rss, rss)
                    if max_rss is not None and rss > max_rss and not resource_exceeded:
                        resource_exceeded = True
                        _create_cancellation_file(cancellation_path, "resource_limit")
                        cancellation_seen_at = time.monotonic()
                    elif cancellation_path.exists() and cancellation_seen_at is None:
                        cancellation_seen_at = time.monotonic()

                    if (
                        cancellation_seen_at is not None
                        and time.monotonic() - cancellation_seen_at
                        >= self.termination_grace_seconds
                        and process.returncode is None
                    ):
                        await asyncio.to_thread(_terminate_process_tree, process.pid)
            finally:
                # Cancelling the supervising task must never orphan the worker: it still
                # holds this job's memory lease and would outlive the application.
                if process.returncode is None:
                    await asyncio.to_thread(_terminate_process_tree, process.pid)

            exit_code = await process.wait()

        if resource_exceeded:
            result = WorkerResultEnvelope(
                status="failure",
                artifacts=[],
                resource_usage={
                    "exit_code": exit_code,
                    "peak_process_tree_rss_bytes": peak_rss,
                    "process_tree_rss_limit_bytes": max_rss,
                },
                error=WorkerError(
                    code="RESOURCE_LIMIT",
                    message="worker process tree exceeded its RSS lease",
                    details={"peak_bytes": peak_rss, "limit_bytes": max_rss},
                ),
            )
            write_result_atomic(result_path, result)
        elif cancellation_seen_at is not None:
            result = _read_result_or_cancelled(result_path, exit_code, peak_rss)
            if result.status != "cancelled":
                result = WorkerResultEnvelope(
                    status="cancelled",
                    artifacts=[],
                    resource_usage={
                        "exit_code": exit_code,
                        "peak_process_tree_rss_bytes": peak_rss,
                    },
                    error=WorkerError(
                        code="CANCELLED",
                        message="worker was cancelled",
                        details={},
                    ),
                )
                write_result_atomic(result_path, result)
        elif exit_code != 0:
            result = _worker_failed_result(
                f"worker exited with status {exit_code}", exit_code, peak_rss
            )
            write_result_atomic(result_path, result)
        else:
            try:
                result = read_result(result_path)
            except (OSError, ValueError) as exc:
                result = _worker_failed_result(
                    f"worker exited without a valid result: {exc}", exit_code, peak_rss
                )
                write_result_atomic(result_path, result)

        return WorkerExecution(
            request_id=request.request_id,
            worker_kind=request.worker_kind,
            exit_code=exit_code,
            peak_process_tree_rss_bytes=peak_rss,
            result=result,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )


def _optional_positive_int(value: object, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _confine_supplied_path(root: Path, path: Path, *, existing: bool) -> Path:
    absolute = path.absolute()
    candidate = absolute.resolve(strict=True) if existing else absolute
    parent = candidate.parent.resolve(strict=True)
    if not parent.is_relative_to(root) or (existing and not candidate.is_relative_to(root)):
        raise ValueError(f"worker protocol path escapes workspace: {path}")
    if not existing and candidate.is_symlink():
        raise ValueError(f"worker protocol path must not be a symlink: {path}")
    if (
        not existing
        and candidate.exists()
        and not candidate.resolve(strict=True).is_relative_to(root)
    ):
        raise ValueError(f"worker protocol path escapes workspace: {path}")
    return candidate


def _create_cancellation_file(path: Path, reason: str) -> None:
    if path.exists():
        return
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        fd = os.open(path, flags, 0o600)
    except FileExistsError:
        return
    try:
        payload = f"{reason}\n".encode()
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    fsync_directory(path.parent)


def _process_tree_rss(pid: int) -> int:
    try:
        process = psutil.Process(pid)
        processes = [process, *process.children(recursive=True)]
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return 0
    total = 0
    for member in processes:
        try:
            total += member.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return total


def _terminate_process_tree(pid: int) -> None:
    try:
        parent = psutil.Process(pid)
        descendants = parent.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return
    processes = [*reversed(descendants), parent]
    for process in processes:
        try:
            process.terminate()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    _, alive = psutil.wait_procs(processes, timeout=1.0)
    for process in alive:
        try:
            process.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    if alive:
        psutil.wait_procs(alive, timeout=1.0)


def _worker_failed_result(message: str, exit_code: int, peak_rss: int) -> WorkerResultEnvelope:
    return WorkerResultEnvelope(
        status="failure",
        artifacts=[],
        resource_usage={
            "exit_code": exit_code,
            "peak_process_tree_rss_bytes": peak_rss,
        },
        error=WorkerError(code="WORKER_FAILED", message=message, details={}),
    )


def _read_result_or_cancelled(
    result_path: Path, exit_code: int, peak_rss: int
) -> WorkerResultEnvelope:
    try:
        return read_result(result_path)
    except (OSError, ValueError):
        return WorkerResultEnvelope(
            status="cancelled",
            artifacts=[],
            resource_usage={
                "exit_code": exit_code,
                "peak_process_tree_rss_bytes": peak_rss,
            },
            error=WorkerError(code="CANCELLED", message="worker was cancelled", details={}),
        )
