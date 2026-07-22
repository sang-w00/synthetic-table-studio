from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ARGN_ROOT = PROJECT_ROOT / "workers" / "argn"
ARGN_PYTHON = ARGN_ROOT / ".venv" / "bin" / "python"
ENGINE_SHA256 = "3ead3770c936919f8fce4e1f9fffd271ffdd490f0292c2ab9a42cb4bafe3caea"
MAX_CANDIDATE_ROWS = 250_000


@dataclass
class FittedWorkspace:
    root: Path
    sample: Path
    checkpoint: Path
    checkpoint_sha256: str
    compatibility: dict[str, str]
    fit_result: dict[str, Any]


@pytest.fixture(scope="module")
def fitted_workspace(tmp_path_factory: pytest.TempPathFactory) -> FittedWorkspace:
    root = tmp_path_factory.mktemp("argn-adapter")
    sample = root / "datasets" / "bounded.parquet"
    sample.parent.mkdir(parents=True)
    _write_mixed_fixture(sample, rows=10_000)
    (root / "checkpoints").mkdir()
    (root / "candidates").mkdir()
    compatibility = {
        "source_manifest_sha256": _digest_bytes(b"source-manifest-v1"),
        "schema_sha256": _digest_bytes(b"schema-v1"),
        "rules_sha256": _digest_bytes(b"rules-v1"),
        "engine_sha256": ENGINE_SHA256,
    }
    fit_config = {
        "checkpoint_path": "checkpoints/argn-fit",
        "model_size": "MOSTLY_AI/Small",
        "max_epochs": 2,
        "max_minutes": 5,
        "device": "cpu",
        "deterministic_split": {
            "callable_fraction": 0.9,
            "fallback_fraction": 0.9,
            "seed": 290_797,
        },
        "checkpoint_compatibility": compatibility,
    }
    request, events, result = _request_paths(root, "fit-real")
    payload = _request_payload(
        root,
        operation="fit",
        job_id="fit-real",
        files={"bounded_training_sample": _snapshot_file(root, sample)},
        argn_config=fit_config,
    )
    completed = _run_worker(request, events, result, payload)
    assert completed.returncode == 0, completed.stderr.decode()
    assert completed.stdout == b""
    fit_result = json.loads(result.read_bytes())
    assert fit_result["status"] == "success"
    assert fit_result["resource_usage"]["bounded_training_sample"]["rows"] == 10_000
    assert fit_result["resource_usage"]["deterministic_split"]["training_rows"] == 9_000
    assert (
        fit_result["resource_usage"]["deterministic_split"]["validation_rows"] == 1_000
    )
    assert fit_result["resource_usage"]["multiprocess_clones_enabled"] is False
    checkpoint = root / "checkpoints" / "argn-fit"
    assert checkpoint.is_dir()
    checkpoint_sha256 = fit_result["artifacts"][0]["sha256"]
    assert checkpoint_sha256 == _directory_digest(checkpoint)
    return FittedWorkspace(
        root=root,
        sample=sample,
        checkpoint=checkpoint,
        checkpoint_sha256=checkpoint_sha256,
        compatibility=compatibility,
        fit_result=fit_result,
    )


def test_real_fit_then_fresh_process_generate_without_source(
    fitted_workspace: FittedWorkspace,
) -> None:
    fitted = fitted_workspace
    tags = json.loads((fitted.checkpoint / "sts-checkpoint.json").read_bytes())
    assert tags["compatibility"] == fitted.compatibility
    assert tags["fit"]["bounded_sample_rows"] == 10_000
    assert tags["fit"]["max_epochs"] == 2
    assert tags["fit"]["model_size"] == "MOSTLY_AI/Small"
    assert tags["fit"]["split"]["deterministic"] is True
    assert tags["fit"]["split"]["fallback_deterministic"] is True
    assert tags["feature_gates"]["multiprocess_clones"] is False

    fitted.sample.unlink(missing_ok=True)
    assert not fitted.sample.exists()
    original_checksum = _directory_digest(fitted.checkpoint)
    request, events, result = _request_paths(fitted.root, "generate-real")
    payload = _request_payload(
        fitted.root,
        operation="generate",
        job_id="generate-real",
        files=_checkpoint_snapshot(fitted.root, fitted.checkpoint),
        argn_config=_generate_config(
            fitted, output="candidates/candidate-000000.parquet"
        ),
    )
    (fitted.root / "candidates").mkdir(exist_ok=True)
    completed = _run_worker(request, events, result, payload)
    assert completed.returncode == 0, completed.stderr.decode()
    assert completed.stdout == b""
    generated = json.loads(result.read_bytes())
    assert generated["status"] == "success"
    assert generated["resource_usage"]["requested_rows"] == 10_000
    assert generated["resource_usage"]["actual_rows"] == 10_000
    assert generated["resource_usage"]["checkpoint_unchanged"] is True
    assert generated["resource_usage"]["clone_checkpoint_unchanged"] is True
    assert generated["resource_usage"]["multiprocess_clones_enabled"] is False
    assert generated["resource_usage"]["checkpoint_sha256_before"] == original_checksum
    assert generated["resource_usage"]["checkpoint_sha256_after"] == original_checksum
    assert _directory_digest(fitted.checkpoint) == original_checksum
    candidate = fitted.root / "candidates" / "candidate-000000.parquet"
    assert candidate.is_file()
    assert pq.ParquetFile(candidate).metadata.num_rows == 10_000
    assert "__index_level_0__" not in pq.read_schema(candidate).names
    assert not any(
        path.name.startswith(".argn-checkpoint-clone")
        for path in candidate.parent.iterdir()
    )
    event_rows = [json.loads(line) for line in events.read_text().splitlines()]
    assert [event["sequence"] for event in event_rows] == list(
        range(1, len(event_rows) + 1)
    )
    assert event_rows[-1]["message_code"] == "ARGN_COMPLETED"
    assert not result.with_name(f"{result.name}.part").exists()


def test_generate_rejects_wrong_checkpoint_hash(
    fitted_workspace: FittedWorkspace,
) -> None:
    fitted = fitted_workspace
    files = _checkpoint_snapshot(fitted.root, fitted.checkpoint)
    first_file = next(iter(files.values()))
    first_file["sha256"] = "0" * 64
    request, events, result = _request_paths(fitted.root, "wrong-hash")
    payload = _request_payload(
        fitted.root,
        operation="generate",
        job_id="wrong-hash",
        files=files,
        argn_config=_generate_config(fitted, output="candidates/wrong-hash.parquet"),
    )
    completed = _run_worker(request, events, result, payload)
    assert completed.returncode == 2
    failure = json.loads(result.read_bytes())
    assert failure["status"] == "failure"
    assert failure["error"]["code"] == "CHECKSUM_MISMATCH"
    assert not (fitted.root / "candidates" / "wrong-hash.parquet").exists()


def test_generate_cancellation_is_atomic(fitted_workspace: FittedWorkspace) -> None:
    fitted = fitted_workspace
    request, events, result = _request_paths(fitted.root, "cancelled-generate")
    cancellation = request.parent / "cancel"
    payload = _request_payload(
        fitted.root,
        operation="generate",
        job_id="cancelled-generate",
        files=_checkpoint_snapshot(fitted.root, fitted.checkpoint),
        argn_config=_generate_config(fitted, output="candidates/cancelled.parquet"),
    )
    request.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    process = subprocess.Popen(
        _worker_command(request, events, result),
        cwd=PROJECT_ROOT,
        env=_worker_environment(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if events.exists() and '"stage":"generate"' in events.read_text():
            cancellation.write_text("user\n")
            break
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            pytest.fail(
                f"worker exited before cancellation point: stdout={stdout!r}, stderr={stderr!r}"
            )
        time.sleep(0.01)
    else:
        process.kill()
        process.communicate()
        pytest.fail("worker did not reach the generate stage")
    stdout, stderr = process.communicate(timeout=300)
    assert process.returncode == 0, stderr.decode()
    assert stdout == b""
    cancelled = json.loads(result.read_bytes())
    assert cancelled["status"] == "cancelled"
    assert cancelled["error"]["code"] == "CANCELLED"
    assert not (fitted.root / "candidates" / "cancelled.parquet").exists()
    assert not any(
        path.name.startswith(".cancelled.parquet.part")
        for path in (fitted.root / "candidates").iterdir()
    )
    assert (
        json.loads(events.read_text().splitlines()[-1])["message_code"]
        == "ARGN_CANCELLED"
    )


@pytest.mark.parametrize(
    ("overrides", "expected_code"),
    [
        ({"candidate_rows": MAX_CANDIDATE_ROWS + 1}, "RESOURCE_LIMIT"),
        ({"process_count": 2}, "BACKEND_INCOMPATIBLE"),
    ],
)
def test_generate_enforces_bounded_candidate_and_local_clone_gate(
    fitted_workspace: FittedWorkspace,
    overrides: dict[str, int],
    expected_code: str,
) -> None:
    fitted = fitted_workspace
    suffix = expected_code.lower()
    config = _generate_config(fitted, output=f"candidates/{suffix}.parquet")
    config.update(overrides)
    request, events, result = _request_paths(fitted.root, f"reject-{suffix}")
    payload = _request_payload(
        fitted.root,
        operation="generate",
        job_id=f"reject-{suffix}",
        files=_checkpoint_snapshot(fitted.root, fitted.checkpoint),
        argn_config=config,
    )
    completed = _run_worker(request, events, result, payload)
    assert completed.returncode == 2
    failure = json.loads(result.read_bytes())
    assert failure["error"]["code"] == expected_code
    assert not (fitted.root / "candidates" / f"{suffix}.parquet").exists()


def _generate_config(fitted: FittedWorkspace, *, output: str) -> dict[str, Any]:
    return {
        "checkpoint_path": fitted.checkpoint.relative_to(fitted.root).as_posix(),
        "candidate_output_path": output,
        "candidate_rows": 10_000,
        "batch_size": 1_000,
        "engine_seed": 3_063_687_064,
        "shard_index": 0,
        "seed_count": 1,
        "process_count": 1,
        "device": "cpu",
        "checkpoint_compatibility": fitted.compatibility,
    }


def _request_paths(root: Path, job_id: str) -> tuple[Path, Path, Path]:
    attempt = root / "jobs" / job_id / "attempt-1"
    attempt.mkdir(parents=True)
    return attempt / "request.json", attempt / "events.jsonl", attempt / "result.json"


def _request_payload(
    root: Path,
    *,
    operation: str,
    job_id: str,
    files: dict[str, dict[str, Any]],
    argn_config: dict[str, Any],
) -> dict[str, Any]:
    return {
        "version": "1.0",
        "request_id": f"request-{job_id}",
        "job_id": job_id,
        "attempt": 1,
        "worker_kind": "argn",
        "operation": operation,
        "manifest_snapshot": {
            "version": "1.0",
            "workspace_root": str(root),
            "files": files,
        },
        "limits": {"worker_rss_bytes": 512 * 1024**2, "argn": argn_config},
        "cancellation_path": f"jobs/{job_id}/attempt-1/cancel",
    }


def _snapshot_file(root: Path, path: Path) -> dict[str, Any]:
    content = path.read_bytes()
    return {
        "path": path.relative_to(root).as_posix(),
        "sha256": hashlib.sha256(content).hexdigest(),
        "size_bytes": len(content),
    }


def _checkpoint_snapshot(root: Path, checkpoint: Path) -> dict[str, dict[str, Any]]:
    return {
        f"checkpoint_file_{index:04d}": _snapshot_file(root, path)
        for index, path in enumerate(
            sorted(path for path in checkpoint.rglob("*") if path.is_file())
        )
    }


def _run_worker(
    request: Path,
    events: Path,
    result: Path,
    payload: dict[str, Any],
) -> subprocess.CompletedProcess[bytes]:
    request.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return subprocess.run(
        _worker_command(request, events, result),
        cwd=PROJECT_ROOT,
        env=_worker_environment(),
        capture_output=True,
        timeout=300,
        check=False,
    )


def _worker_command(request: Path, events: Path, result: Path) -> list[str]:
    return [
        str(ARGN_PYTHON),
        "-m",
        "argn_worker",
        "run",
        "--request",
        str(request),
        "--events",
        str(events),
        "--result",
        str(result),
    ]


def _worker_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ARGN_ROOT / "src")
    return environment


def _write_mixed_fixture(path: Path, *, rows: int) -> None:
    base = list(range(rows))
    table = pa.table(
        {
            "integer_value": pa.array(
                [None if value % 31 == 0 else value % 1000 for value in base]
            ),
            "float_value": pa.array(
                [None if value % 37 == 0 else (value % 173) / 11.0 for value in base],
                type=pa.float64(),
            ),
            "category": pa.array(
                [None if value % 29 == 0 else f"category-{value % 9}" for value in base]
            ),
            "flag": pa.array(
                [None if value % 41 == 0 else value % 2 == 0 for value in base]
            ),
            "event_time": pa.array(
                [
                    None
                    if value % 43 == 0
                    else 1_700_000_000_000_000 + value * 1_000_000
                    for value in base
                ],
                type=pa.timestamp("us"),
            ),
        }
    )
    pq.write_table(table, path, compression="zstd")


def _directory_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(path for path in root.rglob("*") if path.is_file()):
        relative = path.relative_to(root).as_posix().encode()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        size = path.stat().st_size
        digest.update(size.to_bytes(8, "big"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()
