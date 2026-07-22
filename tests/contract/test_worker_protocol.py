from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from sts.jobs.protocol import (
    ManifestSnapshot,
    SnapshotFile,
    WorkerError,
    WorkerEvent,
    WorkerEventWriter,
    WorkerRequestEnvelope,
    WorkerResultEnvelope,
    canonical_json_bytes,
    confined_output_path,
    read_result,
    resolve_manifest_snapshot,
    write_result_atomic,
)
from sts.jobs.supervisor import WorkerSupervisor

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIB = 1024**2


def _workspace_request(
    workspace: Path,
    *,
    operation: str = "protocol_probe",
    limits: dict[str, object] | None = None,
) -> tuple[WorkerRequestEnvelope, Path, Path, Path]:
    source = workspace / "datasets" / "source.bin"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(b"bounded input\n")
    attempt = workspace / "jobs" / "job-1" / "attempt-1"
    attempt.mkdir(parents=True, exist_ok=True)
    request_path = attempt / "request.json"
    events_path = attempt / "events.jsonl"
    result_path = attempt / "result.json"
    request = WorkerRequestEnvelope(
        request_id="request-1",
        job_id="job-1",
        attempt=1,
        worker_kind="eval",
        operation=operation,
        manifest_snapshot=ManifestSnapshot(
            workspace_root=str(workspace),
            files={
                "source": SnapshotFile(
                    path="datasets/source.bin",
                    sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                    size_bytes=source.stat().st_size,
                )
            },
        ),
        limits=limits or {},
        cancellation_path="jobs/job-1/attempt-1/cancel",
    )
    request_path.write_bytes(canonical_json_bytes(request))
    return request, request_path, events_path, result_path


def test_request_event_result_schema_round_trips_in_every_locked_environment(
    tmp_path: Path,
) -> None:
    request, _, _, _ = _workspace_request(tmp_path)
    payload = {
        "request": request.model_dump(mode="json"),
        "event": WorkerEvent(
            sequence=1,
            timestamp=datetime(2026, 7, 22, tzinfo=UTC),
            stage="fit",
            completed=2,
            total=10,
            unit="rows",
            message_code="FIT_PROGRESS",
            metrics={"rss_bytes": 123},
        ).model_dump(mode="json"),
        "result": WorkerResultEnvelope(
            status="failure",
            artifacts=[],
            resource_usage={"rss_bytes": 123},
            error=WorkerError(code="EXPECTED", message="expected", details={}),
        ).model_dump(mode="json"),
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    script = """
import json, sys
from {module}.protocol import WorkerEvent, WorkerRequestEnvelope, WorkerResultEnvelope, canonical_json_bytes
payload = json.loads(sys.stdin.buffer.read())
roundtrip = {{
    'request': WorkerRequestEnvelope.from_dict(payload['request']).to_dict(),
    'event': WorkerEvent.from_dict(payload['event']).to_dict(),
    'result': WorkerResultEnvelope.from_dict(payload['result']).to_dict(),
}}
sys.stdout.buffer.write(canonical_json_bytes(roundtrip))
"""
    for kind in ("argn", "dpmm", "eval"):
        worker = PROJECT_ROOT / "workers" / kind
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(worker / "src")
        completed = subprocess.run(
            [
                str(worker / ".venv" / "bin" / "python"),
                "-c",
                script.format(module=f"{kind}_worker"),
            ],
            input=serialized.encode(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr.decode()
        assert json.loads(completed.stdout) == payload


def test_manifest_snapshot_denies_traversal_and_symlink_escape(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        SnapshotFile(path="../private.csv", sha256="0" * 64, size_bytes=0)
    with pytest.raises(ValueError):
        SnapshotFile(path="/private.csv", sha256="0" * 64, size_bytes=0)

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"private")
    (workspace / "escape.bin").symlink_to(outside)
    snapshot = ManifestSnapshot(
        workspace_root=str(workspace),
        files={
            "escape": SnapshotFile(
                path="escape.bin",
                sha256=hashlib.sha256(outside.read_bytes()).hexdigest(),
                size_bytes=outside.stat().st_size,
            )
        },
    )
    with pytest.raises(ValueError, match="escapes root"):
        resolve_manifest_snapshot(snapshot)

    broken_output = workspace / "cancel"
    broken_output.symlink_to(tmp_path / "not-created-outside")
    with pytest.raises(ValueError, match="must not be a symlink"):
        confined_output_path(workspace, "cancel")


def test_manifest_snapshot_verifies_size_and_sha(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")
    bad_size = ManifestSnapshot(
        workspace_root=str(tmp_path),
        files={
            "source": SnapshotFile(
                path="source.bin",
                sha256=hashlib.sha256(b"payload").hexdigest(),
                size_bytes=6,
            )
        },
    )
    with pytest.raises(ValueError, match="size mismatch"):
        resolve_manifest_snapshot(bad_size)

    bad_sha = bad_size.model_copy(
        update={
            "files": {
                "source": SnapshotFile(path="source.bin", sha256="0" * 64, size_bytes=7)
            }
        }
    )
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        resolve_manifest_snapshot(bad_sha)


def test_event_writer_is_monotonic_append_only_and_fsyncs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sts.jobs.protocol as protocol

    actual_fsync = protocol.os.fsync
    fsynced: list[int] = []

    def recording_fsync(fd: int) -> None:
        actual_fsync(fd)
        fsynced.append(fd)

    monkeypatch.setattr(protocol.os, "fsync", recording_fsync)
    path = tmp_path / "events.jsonl"
    writer = WorkerEventWriter(path)
    writer.append(
        WorkerEvent(
            sequence=1,
            timestamp=datetime.now(UTC),
            stage="fit",
            completed=0,
            total=1,
            unit="operation",
            message_code="STARTED",
            metrics={},
        )
    )
    writer.append(
        WorkerEvent(
            sequence=2,
            timestamp=datetime.now(UTC),
            stage="fit",
            completed=1,
            total=1,
            unit="operation",
            message_code="COMPLETED",
            metrics={},
        )
    )
    with pytest.raises(ValueError, match="sequence must be 3"):
        writer.append(
            WorkerEvent(
                sequence=2,
                timestamp=datetime.now(UTC),
                stage="fit",
                completed=1,
                total=1,
                unit="operation",
                message_code="DUPLICATE",
                metrics={},
            )
        )

    lines = path.read_bytes().splitlines()
    assert [json.loads(line)["sequence"] for line in lines] == [1, 2]
    assert len(fsynced) == 2


def test_result_publication_is_atomic_and_directory_fsynced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sts.jobs.protocol as protocol

    actual_fsync = protocol.os.fsync
    fsync_count = 0

    def recording_fsync(fd: int) -> None:
        nonlocal fsync_count
        actual_fsync(fd)
        fsync_count += 1

    monkeypatch.setattr(protocol.os, "fsync", recording_fsync)
    result_path = tmp_path / "result.json"
    result = WorkerResultEnvelope(
        status="success", artifacts=[], resource_usage={"rows": 10}, error=None
    )
    write_result_atomic(result_path, result)

    assert read_result(result_path) == result
    assert not (tmp_path / "result.json.part").exists()
    assert fsync_count == 2


@pytest.mark.asyncio
async def test_supervisor_success_uses_locked_interpreter(tmp_path: Path) -> None:
    request, request_path, events_path, result_path = _workspace_request(tmp_path)
    supervisor = WorkerSupervisor(
        PROJECT_ROOT, poll_interval_seconds=0.01, termination_grace_seconds=0.2
    )
    execution = await supervisor.run(request_path, events_path, result_path)

    assert execution.exit_code == 0
    assert execution.result.status == "success"
    assert execution.result.resource_usage["resolved_snapshot_files"] == 1
    assert execution.result.resource_usage["pid"] != os.getpid()
    assert read_result(result_path).status == "success"
    assert [
        json.loads(line)["sequence"] for line in events_path.read_bytes().splitlines()
    ] == [1, 2]
    assert supervisor.runtime_for(request.worker_kind).interpreter.samefile(
        PROJECT_ROOT / "workers" / "eval" / ".venv" / "bin" / "python"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "expected_fragment"),
    [
        ("protocol_missing_result", "without a valid result"),
        ("protocol_nonzero_exit", "status 7"),
    ],
)
async def test_supervisor_maps_missing_result_and_nonzero_exit_to_worker_failed(
    tmp_path: Path, operation: str, expected_fragment: str
) -> None:
    _, request_path, events_path, result_path = _workspace_request(
        tmp_path, operation=operation
    )
    supervisor = WorkerSupervisor(
        PROJECT_ROOT, poll_interval_seconds=0.01, termination_grace_seconds=0.2
    )
    execution = await supervisor.run(request_path, events_path, result_path)

    assert execution.result.status == "failure"
    assert execution.result.error is not None
    assert execution.result.error.code == "WORKER_FAILED"
    assert expected_fragment in execution.result.error.message
    assert read_result(result_path).error.code == "WORKER_FAILED"


async def _wait_until(predicate: object, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("condition not reached")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_supervisor_cancellation_file_is_observed(tmp_path: Path) -> None:
    request, request_path, events_path, result_path = _workspace_request(
        tmp_path, operation="protocol_wait_for_cancel"
    )
    supervisor = WorkerSupervisor(
        PROJECT_ROOT, poll_interval_seconds=0.01, termination_grace_seconds=0.5
    )
    running = asyncio.create_task(
        supervisor.run(request_path, events_path, result_path)
    )
    await _wait_until(lambda: events_path.exists() and events_path.stat().st_size > 0)
    cancellation_path = supervisor.request_cancellation(request)
    execution = await running

    assert cancellation_path.read_text() == "cancelled\n"
    assert execution.result.status == "cancelled"
    assert execution.result.error is not None
    assert execution.result.error.code == "CANCELLED"
    assert read_result(result_path).status == "cancelled"


@pytest.mark.asyncio
async def test_supervisor_enforces_process_tree_rss_limit(tmp_path: Path) -> None:
    limit = 48 * MIB
    _, request_path, events_path, result_path = _workspace_request(
        tmp_path,
        operation="protocol_allocate",
        limits={
            "max_process_tree_rss_bytes": limit,
            "probe_allocation_bytes": 96 * MIB,
        },
    )
    supervisor = WorkerSupervisor(
        PROJECT_ROOT, poll_interval_seconds=0.01, termination_grace_seconds=0.5
    )
    execution = await supervisor.run(request_path, events_path, result_path)

    assert execution.result.status == "failure"
    assert execution.result.error is not None
    assert execution.result.error.code == "RESOURCE_LIMIT"
    assert execution.peak_process_tree_rss_bytes > limit
    assert execution.result.resource_usage["process_tree_rss_limit_bytes"] == limit
    assert read_result(result_path).error.code == "RESOURCE_LIMIT"
    assert (
        tmp_path / "jobs" / "job-1" / "attempt-1" / "cancel"
    ).read_text() == "resource_limit\n"
