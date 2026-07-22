from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from sts.api.app import create_app
from sts.jobs.runtime import DeterministicLightweightAdapter
from sts.storage.repository import OwnerType

BASE_URL = "http://127.0.0.1:8765"
ORIGIN_HEADERS = {"Origin": BASE_URL}


def _csv(rows: int) -> bytes:
    lines = ["group,amount,score,flag"]
    for index in range(rows):
        lines.append(
            f"{('a', 'b', 'c')[index % 3]},{index % 101},{(index % 37) / 10:.1f},"
            f"{'true' if index % 2 else 'false'}"
        )
    return ("\n".join(lines) + "\n").encode()


def _bootstrap(client: TestClient) -> None:
    response = client.get("/api/v1/bootstrap")
    assert response.status_code == 200, response.text


def _mutation(client: TestClient, method: str, path: str, **kwargs: Any):
    headers = dict(ORIGIN_HEADERS)
    headers.update(kwargs.pop("headers", {}))
    return client.request(method, path, headers=headers, **kwargs)


def _normalize_csv(
    client: TestClient, app: Any, *, rows: int = 1_000
) -> tuple[str, Any]:
    content = _csv(rows)
    created = _mutation(
        client,
        "POST",
        "/api/v1/datasets/uploads",
        json={
            "filename": "runtime.csv",
            "size_bytes": len(content),
            "source_format": "csv",
        },
    )
    assert created.status_code == 201, created.text
    dataset_id = created.json()["dataset_id"]
    midpoint = len(content) // 2
    for offset, chunk in ((0, content[:midpoint]), (midpoint, content[midpoint:])):
        uploaded = _mutation(
            client,
            "PATCH",
            f"/api/v1/datasets/{dataset_id}/content",
            content=chunk,
            headers={
                "Upload-Offset": str(offset),
                "Content-Type": "application/octet-stream",
            },
        )
        assert uploaded.status_code == 204, uploaded.text
    completed = _mutation(
        client,
        "POST",
        f"/api/v1/datasets/{dataset_id}/complete",
        json={"sha256": hashlib.sha256(content).hexdigest()},
    )
    assert completed.status_code == 202, completed.text
    if completed.json()["state"] == "parse_options_required":
        proposal = client.get(f"/api/v1/datasets/{dataset_id}/parse-options").json()[
            "proposal"
        ]["recommended"]
        confirmed = _mutation(
            client,
            "PUT",
            f"/api/v1/datasets/{dataset_id}/parse-options",
            json={
                "encoding": proposal["encoding"],
                "delimiter": proposal["delimiter"],
                "malformed": "fail",
            },
        )
        assert confirmed.status_code == 200, confirmed.text
    else:
        assert completed.json()["state"] == "raw_ready"
    profiled = _mutation(client, "POST", f"/api/v1/datasets/{dataset_id}/profile")
    assert profiled.status_code == 202, profiled.text
    schema = _mutation(
        client,
        "PUT",
        f"/api/v1/datasets/{dataset_id}/schema",
        json={
            "columns": [
                {
                    "name": "group",
                    "kind": "categorical",
                    "nullable": False,
                    "role": "model",
                },
                {
                    "name": "amount",
                    "kind": "integer",
                    "nullable": False,
                    "role": "model",
                },
                {"name": "score", "kind": "float", "nullable": False, "role": "model"},
                {"name": "flag", "kind": "boolean", "nullable": False, "role": "model"},
            ]
        },
    )
    assert schema.status_code == 200, schema.text
    rules = _mutation(
        client,
        "PUT",
        f"/api/v1/datasets/{dataset_id}/rules",
        json={
            "rules": [
                {
                    "id": "group-domain",
                    "kind": "allowed_values",
                    "provenance": "public",
                    "source_action": "block",
                    "column": "group",
                    "values": ["a", "b", "c"],
                }
            ]
        },
    )
    assert rules.status_code == 200, rules.text
    normalized = _mutation(client, "POST", f"/api/v1/datasets/{dataset_id}/normalize")
    assert normalized.status_code == 202, normalized.text
    assert normalized.json()["state"] == "normalized"
    manifest = app.state.repository.get_dataset_manifest(dataset_id)
    normalized_path = app.state.workspace.resolve_relative(
        manifest.normalized.relative_path, require_exists=True
    )
    assert pq.ParquetFile(normalized_path).metadata.num_rows == rows
    return dataset_id, manifest


def _job_request(
    dataset_id: str,
    manifest: Any,
    *,
    output_rows: int,
    training_rows: int,
    output_formats: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "version": "1.0",
        "dataset_id": dataset_id,
        "dataset_manifest_sha": manifest.canonical_sha256,
        "schema_version": manifest.schema_version,
        "rules_version": manifest.rules_version,
        "mode": "utility",
        "synthesizer": "tabular_argn",
        "output_rows": output_rows,
        "output_formats": output_formats or ["parquet"],
        "resource_profile": "m4-default",
        "evaluation_config_version": "1.0",
        "generation_seed": 20260723,
        "training": {
            "max_rows": training_rows,
            "max_epochs": 1,
            "max_minutes": 5,
            "model_size": "small",
            "device": "cpu",
        },
    }


def _create_job(
    client: TestClient, request: dict[str, Any], key: str
) -> dict[str, Any]:
    response = _mutation(
        client,
        "POST",
        "/api/v1/jobs",
        json=request,
        headers={"Idempotency-Key": key},
    )
    assert response.status_code == 201, response.text
    return response.json()


def _wait_for(
    client: TestClient,
    job_id: str,
    states: set[str],
    *,
    timeout: float = 300,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    seen: list[str] = []
    while time.monotonic() < deadline:
        response = client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200, response.text
        payload = response.json()
        seen.append(payload["state"])
        if payload["state"] in states:
            return payload
        if payload["state"] in {"succeeded", "cancelled", "failed"}:
            return payload
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not reach {states}; seen={seen[-20:]}")


def _assert_success_surface(
    client: TestClient, app: Any, job_id: str, output_rows: int
) -> None:
    report_response = client.get(f"/api/v1/jobs/{job_id}/reports/primary")
    assert report_response.status_code == 200, report_response.text
    report = report_response.json()
    assert report["mode"] == "utility"
    exact = report["evaluation"]["exact"]
    assert exact["requested_rows"] == output_rows
    assert exact["actual_rows"] == output_rows
    assert exact["hard_rule_violations"] == 0
    assert report["evaluation"]["rejection"]["post_violations"] == 0

    listed = client.get(f"/api/v1/jobs/{job_id}/artifacts?scope=downloadable")
    assert listed.status_code == 200, listed.text
    artifacts = listed.json()["artifacts"]
    kinds = {item["kind"] for item in artifacts}
    assert {
        "synthetic_parquet_manifest",
        "synthetic_parquet_zip",
        "primary_report_json",
    } <= kinds
    for artifact in artifacts:
        downloaded = client.get(f"/api/v1/artifacts/{artifact['artifact_id']}/download")
        assert downloaded.status_code == 200, downloaded.text
        assert len(downloaded.content) == artifact["size_bytes"]
        assert hashlib.sha256(downloaded.content).hexdigest() == artifact["sha256"]

    events = app.state.repository.replay_events(OwnerType.JOB, job_id)
    states = [event.payload["state"] for event in events]
    ordered = [
        "queued",
        "admitted",
        "preparing",
        "fitting",
        "generating",
        "repairing",
        "evaluating",
        "exporting",
        "publishing",
        "succeeded",
    ]
    assert all(state in states for state in ordered)
    assert [event.terminal for event in events].count(True) == 1
    sse = client.get(f"/api/v1/jobs/{job_id}/events")
    assert sse.status_code == 200
    assert sse.text.count("event: terminal") == 1
    assert not list(app.state.workspace.job_attempt_dir(job_id, 1).rglob("*.part"))


def test_real_argn_csv_runtime_end_to_end(tmp_path: Path) -> None:
    app = create_app(tmp_path / "real-workspace", public_port=8765)
    with TestClient(app, base_url=BASE_URL) as client:
        _bootstrap(client)
        dataset_id, manifest = _normalize_csv(client, app, rows=1_000)
        request = _job_request(
            dataset_id,
            manifest,
            output_rows=10_000,
            training_rows=1_000,
            output_formats=["parquet", "csv"],
        )
        assert client.portal is not None
        client.portal.call(app.state.job_runtime.stop)
        created = _create_job(client, request, "real-argn-system")
        assert created["state"] == "admitted"
        client.portal.call(app.state.job_runtime.start)
        finished = _wait_for(
            client, created["job_id"], {"succeeded", "failed"}, timeout=600
        )
        assert finished["state"] == "succeeded", finished
        _assert_success_surface(client, app, created["job_id"], 10_000)
        downloadable = client.get(
            f"/api/v1/jobs/{created['job_id']}/artifacts?scope=downloadable"
        ).json()["artifacts"]
        assert "synthetic_csv" in {item["kind"] for item in downloadable}


def test_injected_chunk_runtime_cancellation_resume_and_scale_loop(
    tmp_path: Path,
) -> None:
    adapter = DeterministicLightweightAdapter(batch_rows=4_096, pause_seconds=0.001)
    app = create_app(
        tmp_path / "lightweight-workspace",
        public_port=8765,
        utility_adapter=adapter,
    )
    with TestClient(app, base_url=BASE_URL) as client:
        _bootstrap(client)
        dataset_id, manifest = _normalize_csv(client, app, rows=2_000)

        cancel_request = _job_request(
            dataset_id,
            manifest,
            output_rows=300_000,
            training_rows=2_000,
        )
        created = _create_job(client, cancel_request, "lightweight-cancel")
        _wait_for(client, created["job_id"], {"generating"}, timeout=60)
        cancelled_response = _mutation(
            client, "POST", f"/api/v1/jobs/{created['job_id']}/cancel"
        )
        assert cancelled_response.status_code in {200, 202}, cancelled_response.text
        cancelled = _wait_for(
            client, created["job_id"], {"cancelled", "failed"}, timeout=60
        )
        assert cancelled["state"] == "cancelled", cancelled
        assert cancelled["resume_boundary"] == "validated_fit_checkpoint"

        resumed_response = _mutation(
            client,
            "POST",
            f"/api/v1/jobs/{created['job_id']}/resume",
            headers={"Idempotency-Key": "lightweight-resume"},
        )
        assert resumed_response.status_code == 201, resumed_response.text
        resumed_id = resumed_response.json()["job_id"]
        resumed = _wait_for(client, resumed_id, {"succeeded", "failed"}, timeout=180)
        assert resumed["state"] == "succeeded", resumed
        assert resumed["retry_of"] == created["job_id"]
        _assert_success_surface(client, app, resumed_id, 300_000)

        assert adapter.generated_batches > 1
        assert adapter.maximum_generated_batch_rows <= 4_096
        internal = client.get(
            f"/api/v1/jobs/{resumed_id}/artifacts?scope=internal"
        ).json()["artifacts"]
        shard = next(
            item for item in internal if item["kind"] == "synthetic_parquet_shard"
        )
        shard_path = app.state.workspace.resolve_relative(
            shard["relative_path"], require_exists=True
        )
        assert pq.ParquetFile(shard_path).metadata.num_rows == 300_000
