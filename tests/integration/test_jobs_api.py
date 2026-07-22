from __future__ import annotations

import hashlib
import json
from pathlib import Path
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from sts.api.artifacts import ArtifactService, create_artifact_router
from sts.api.jobs import JobService, create_job_router
from sts.api.problems import install_problem_handlers
from sts.domain import (
    ArtifactManifest,
    ColumnKind,
    ColumnRole,
    ColumnSchema,
    DatasetManifest,
    DatasetState,
    ErrorCode,
    JobState,
    ManifestFile,
    SynthesisRequest,
)
from sts.storage import CatalogRepository, WorkspaceLayout
from sts.storage.repository import OwnerType

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ARGN_PROBE = PROJECT_ROOT / "probes/results/argn_contract.json"
DPMM_PROBE = PROJECT_ROOT / "probes/results/dpmm_contract.json"


@pytest.fixture
def jobs_client(tmp_path: Path):
    layout = WorkspaceLayout(tmp_path / "workspace")
    repository = CatalogRepository.open_workspace(layout)
    dataset_id = uuid4()
    dataset_directory = layout.dataset_dir(dataset_id, create=True)
    normalized = dataset_directory / "normalized.parquet"
    pq.write_table(
        pa.table(
            {
                "__sts_row_id": pa.array(range(100), type=pa.int64()),
                "category": [["a", "b", "a", "c"][index % 4] for index in range(100)],
                "amount": list(range(100)),
            }
        ),
        normalized,
    )
    normalized_bytes = normalized.read_bytes()
    manifest = DatasetManifest(
        dataset_id=dataset_id,
        source=ManifestFile(
            relative_path=f"datasets/{dataset_id}/source.csv",
            sha256="0" * 64,
            size_bytes=0,
        ),
        schema_version="schema-1",
        rules_version="rules-1",
        rules_sha256="1" * 64,
        columns=(
            ColumnSchema(
                name="category",
                kind=ColumnKind.CATEGORICAL,
                nullable=False,
                role=ColumnRole.MODEL,
            ),
            ColumnSchema(
                name="amount",
                kind=ColumnKind.INTEGER,
                nullable=False,
                role=ColumnRole.MODEL,
            ),
        ),
        normalized=ManifestFile(
            relative_path=f"datasets/{dataset_id}/normalized.parquet",
            sha256=hashlib.sha256(normalized_bytes).hexdigest(),
            size_bytes=len(normalized_bytes),
        ),
        row_count=100,
    )
    dataset = repository.create_dataset(manifest, state=DatasetState.NORMALIZED)
    service = JobService(
        repository,
        layout,
        argn_probe_path=ARGN_PROBE,
        dpmm_probe_path=DPMM_PROBE,
        worker_lease_bytes=32 * 1024**2,
        utility_max_rows=100,
    )
    artifacts = ArtifactService(repository, layout)
    app = FastAPI()
    install_problem_handlers(app)
    app.include_router(create_job_router(service))
    app.include_router(create_artifact_router(artifacts))
    with TestClient(app) as client:
        yield client, service, artifacts, repository, layout, dataset
    repository.close()


def _utility_request(dataset: object, *, output_rows: int = 20) -> dict:
    return {
        "version": "1.0",
        "dataset_id": str(dataset.dataset_id),
        "dataset_manifest_sha": dataset.manifest_sha256,
        "schema_version": "schema-1",
        "rules_version": "rules-1",
        "mode": "utility",
        "synthesizer": "tabular_argn",
        "output_rows": output_rows,
        "output_formats": ["parquet"],
        "resource_profile": "m4-default",
        "evaluation_config_version": "1.0",
        "generation_seed": 42,
        "training": {
            "max_rows": 8,
            "max_epochs": 2,
            "max_minutes": 10,
            "model_size": "MOSTLY_AI/Small",
            "device": "cpu",
        },
    }


def _dp_request(synthesizer: str, *, dataset_id: UUID | None = None) -> dict:
    identifier = dataset_id or uuid4()
    return {
        "version": "1.0",
        "dataset_id": str(identifier),
        "dataset_manifest_sha": "a" * 64,
        "schema_version": "schema-1",
        "rules_version": "rules-1",
        "mode": "differential_privacy",
        "synthesizer": synthesizer,
        "output_rows": 10,
        "output_formats": ["parquet"],
        "resource_profile": "m4-default",
        "evaluation_config_version": "1.0",
        "privacy": {
            "adjacency": "add_remove_one_row",
            "privacy_unit": "row",
            "epsilon_model": "3",
            "delta": "0.000001",
            "epsilon_preprocess": 0,
            "public_metadata_manifest": {
                "relative_path": "public/codebook.json",
                "sha256": "b" * 64,
                "size_bytes": 10,
            },
            "public_target_count": 10000,
            "fit_sampling_rate": "1",
        },
    }


def _create_utility(client: TestClient, dataset: object, key: str) -> dict:
    response = client.post(
        "/api/v1/jobs",
        json=_utility_request(dataset),
        headers={"Idempotency-Key": key},
    )
    assert response.status_code == 201, response.text
    return response.json()


def _publish(
    repository: CatalogRepository,
    job: object,
    *,
    kind: str,
    filename: str,
    content: bytes,
    downloadable: bool,
    release_safe: bool,
    private: bool,
) -> ArtifactManifest:
    manifest = ArtifactManifest(
        artifact_id=uuid4(),
        kind=kind,
        relative_path=f"jobs/{job.job_id}/attempt-{job.attempt}/{filename}",
        sha256=hashlib.sha256(content).hexdigest(),
        size_bytes=len(content),
        downloadable=downloadable,
        release_safe=release_safe,
        contains_private_source_information=private,
        job_id=job.job_id,
        attempt=job.attempt,
    )
    return repository.publish_artifact_bytes(manifest, content)


def test_create_is_idempotent_admits_utility_and_builds_bounded_argn_fit_request(
    jobs_client,
) -> None:
    client, _, _, _, layout, dataset = jobs_client
    created = _create_utility(client, dataset, "utility-create-1")
    assert created["state"] == "admitted"
    assert created["planning_request"] == {
        "worker_kind": "argn",
        "operation": "fit",
        "ready": True,
    }

    repeated = client.post(
        "/api/v1/jobs",
        json=_utility_request(dataset),
        headers={"Idempotency-Key": "utility-create-1"},
    )
    assert repeated.status_code == 201
    assert repeated.json() == created

    attempt = layout.job_attempt_dir(created["job_id"], 1)
    request = json.loads((attempt / "argn-fit-request.json").read_bytes())
    assert request["worker_kind"] == "argn"
    assert request["operation"] == "fit"
    assert set(request["manifest_snapshot"]["files"]) == {"bounded_training_sample"}
    sample_path = layout.resolve_relative(
        request["manifest_snapshot"]["files"]["bounded_training_sample"]["path"],
        require_exists=True,
    )
    assert pq.read_table(sample_path).num_rows == 8
    assert "__sts_row_id" not in pq.read_schema(sample_path).names

    conflict = client.post(
        "/api/v1/jobs",
        json=_utility_request(dataset, output_rows=21),
        headers={"Idempotency-Key": "utility-create-1"},
    )
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "IDEMPOTENCY_CONFLICT"


def test_utility_snapshot_and_host_admission_fail_before_job_creation(
    jobs_client,
) -> None:
    client, _, _, repository, _, dataset = jobs_client
    wrong_snapshot = _utility_request(dataset)
    wrong_snapshot["dataset_manifest_sha"] = "f" * 64
    response = client.post(
        "/api/v1/jobs",
        json=wrong_snapshot,
        headers={"Idempotency-Key": "wrong-snapshot"},
    )
    assert response.status_code == 409
    assert response.json()["code"] == "INVALID_STATE"

    over_cap = _utility_request(dataset)
    over_cap["training"]["max_rows"] = 101
    response = client.post(
        "/api/v1/jobs",
        json=over_cap,
        headers={"Idempotency-Key": "over-cap"},
    )
    assert response.status_code == 507
    assert response.json()["code"] == "RESOURCE_LIMIT"
    assert repository.replay_events(OwnerType.JOB, uuid4()) == ()


@pytest.mark.parametrize("synthesizer", ["mst", "aim"])
def test_dp_backends_fail_closed_at_verified_probe_without_job_or_ledger_spend(
    jobs_client,
    monkeypatch: pytest.MonkeyPatch,
    synthesizer: str,
) -> None:
    client, _, _, repository, _, _ = jobs_client
    calls = {"create_job": 0, "reserve": 0}
    original_create = repository.create_job
    original_reserve = repository.reserve_ledger_run

    def create_job_spy(*args, **kwargs):
        calls["create_job"] += 1
        return original_create(*args, **kwargs)

    def reserve_spy(*args, **kwargs):
        calls["reserve"] += 1
        return original_reserve(*args, **kwargs)

    monkeypatch.setattr(repository, "create_job", create_job_spy)
    monkeypatch.setattr(repository, "reserve_ledger_run", reserve_spy)
    response = client.post(
        "/api/v1/jobs",
        json=_dp_request(synthesizer),
        headers={"Idempotency-Key": f"dp-{synthesizer}"},
    )
    assert response.status_code == 409
    assert response.json()["code"] == "BACKEND_INCOMPATIBLE"
    assert response.json()["context"]["probe_gate"] == "failed"
    assert response.json()["context"]["formal_dp_enabled"] is False
    assert calls == {"create_job": 0, "reserve": 0}


def test_cancel_idempotency_resume_new_identity_and_sse_replay(jobs_client) -> None:
    client, _, _, repository, _, dataset = jobs_client
    created = _create_utility(client, dataset, "cancel-resume-source")
    job_id = created["job_id"]

    first_cancel = client.post(f"/api/v1/jobs/{job_id}/cancel")
    assert first_cancel.status_code == 202
    assert first_cancel.json()["state"] == "cancelling"
    repeated_cancel = client.post(f"/api/v1/jobs/{job_id}/cancel")
    assert repeated_cancel.status_code == 200
    assert repeated_cancel.json()["state"] == "cancelling"

    repository.set_resume_boundary(job_id, "validated_fit_checkpoint")
    cancelled = repository.transition_job(
        job_id,
        JobState.CANCELLED,
        expected_state=JobState.CANCELLING,
    )
    terminal = repository.append_event(
        OwnerType.JOB,
        job_id,
        {
            "version": "1.0",
            "stage": "cancelled",
            "state": "cancelled",
            "completed": 1,
            "total": 1,
            "unit": "steps",
            "message_code": "JOB_CANCELLED",
            "metrics": {},
        },
        terminal=True,
    )
    assert cancelled.state is JobState.CANCELLED
    assert client.post(f"/api/v1/jobs/{job_id}/cancel").status_code == 200

    events = repository.replay_events(OwnerType.JOB, job_id)
    after_id = events[-2].event_id
    replay = client.get(
        f"/api/v1/jobs/{job_id}/events",
        headers={"Last-Event-ID": str(after_id)},
    )
    assert replay.status_code == 200
    assert replay.headers["content-type"].startswith("text/event-stream")
    assert f"id: {terminal.event_id}" in replay.text
    assert replay.text.count("event: terminal") == 1
    assert f"id: {after_id}\n" not in replay.text

    resumed = client.post(
        f"/api/v1/jobs/{job_id}/resume",
        headers={"Idempotency-Key": "resume-1"},
    )
    assert resumed.status_code == 201
    assert resumed.json()["job_id"] != job_id
    assert resumed.json()["retry_of"] == job_id
    assert resumed.json()["attempt"] == 2
    repeated = client.post(
        f"/api/v1/jobs/{job_id}/resume",
        headers={"Idempotency-Key": "resume-1"},
    )
    assert repeated.status_code == 201
    assert repeated.json()["job_id"] == resumed.json()["job_id"]


def test_report_safety_artifact_scopes_and_full_and_range_downloads(
    jobs_client,
) -> None:
    client, _, _, repository, _, dataset = jobs_client
    created = _create_utility(client, dataset, "artifacts-job")
    job = repository.get_job(created["job_id"])
    primary_payload = b'{"version":"1.0","quality":"internal-source"}'
    primary = _publish(
        repository,
        job,
        kind="primary_report_json",
        filename="primary.json",
        content=primary_payload,
        downloadable=True,
        release_safe=False,
        private=True,
    )
    internal = _publish(
        repository,
        job,
        kind="internal_diagnostic_report_json",
        filename="internal.json",
        content=b'{"version":"1.0","source_count":12}',
        downloadable=False,
        release_safe=False,
        private=True,
    )
    safe = _publish(
        repository,
        job,
        kind="privacy_ledger_json",
        filename="safe-ledger.json",
        content=b'{"version":"1.0","epsilon":"3"}',
        downloadable=True,
        release_safe=True,
        private=False,
    )

    downloadable = client.get(f"/api/v1/jobs/{job.job_id}/artifacts?scope=downloadable")
    assert downloadable.status_code == 200
    assert {item["artifact_id"] for item in downloadable.json()["artifacts"]} == {
        str(primary.artifact_id),
        str(safe.artifact_id),
    }
    dp_release = client.get(f"/api/v1/jobs/{job.job_id}/artifacts?scope=dp_release")
    assert [item["artifact_id"] for item in dp_release.json()["artifacts"]] == [
        str(safe.artifact_id)
    ]
    all_internal = client.get(f"/api/v1/jobs/{job.job_id}/artifacts?scope=internal")
    assert {item["artifact_id"] for item in all_internal.json()["artifacts"]} == {
        str(primary.artifact_id),
        str(internal.artifact_id),
        str(safe.artifact_id),
    }

    primary_report = client.get(f"/api/v1/jobs/{job.job_id}/reports/primary")
    assert primary_report.status_code == 200
    assert primary_report.json()["quality"] == "internal-source"
    release_report = client.get(f"/api/v1/jobs/{job.job_id}/reports/release")
    assert release_report.status_code == 403
    assert release_report.json()["code"] == "REPORT_NOT_RELEASE_SAFE"
    internal_report = client.get(f"/api/v1/jobs/{job.job_id}/reports/internal")
    assert internal_report.status_code == 200
    assert internal_report.json()["source_count"] == 12

    full = client.get(f"/api/v1/artifacts/{primary.artifact_id}/download")
    assert full.status_code == 200
    assert full.content == primary_payload
    assert full.headers["Accept-Ranges"] == "bytes"
    assert full.headers["Content-Length"] == str(len(primary_payload))
    partial = client.get(
        f"/api/v1/artifacts/{primary.artifact_id}/download",
        headers={"Range": "bytes=2-8"},
    )
    assert partial.status_code == 206
    assert partial.content == primary_payload[2:9]
    assert partial.headers["Content-Range"] == f"bytes 2-8/{len(primary_payload)}"
    unsatisfied = client.get(
        f"/api/v1/artifacts/{primary.artifact_id}/download",
        headers={"Range": "bytes=999-1000"},
    )
    assert unsatisfied.status_code == 416
    assert unsatisfied.headers["Content-Range"] == f"bytes */{len(primary_payload)}"


def test_dp_report_requires_exact_safety_predicate(jobs_client) -> None:
    client, _, _, repository, _, dataset = jobs_client
    request = _dp_request("mst", dataset_id=dataset.dataset_id)
    request["dataset_manifest_sha"] = dataset.manifest_sha256
    job = repository.create_job(
        SynthesisRequest.model_validate(request),
        idempotency_key="cataloged-dp-report",
    )
    safe = _publish(
        repository,
        job,
        kind="dp_release_report_json",
        filename="dp-release.json",
        content=b'{"version":"1.0","epsilon":"3"}',
        downloadable=True,
        release_safe=True,
        private=False,
    )
    response = client.get(f"/api/v1/jobs/{job.job_id}/reports/release")
    assert response.status_code == 200
    assert response.json() == {"version": "1.0", "epsilon": "3"}
    assert safe.release_safe is True

    _publish(
        repository,
        job,
        kind="dp_release_report_json",
        filename="unsafe-release.json",
        content=b'{"version":"1.0","source_count":12}',
        downloadable=False,
        release_safe=False,
        private=True,
    )
    rejected = client.get(f"/api/v1/jobs/{job.job_id}/reports/release")
    assert rejected.status_code == 403
    assert rejected.json()["code"] == "REPORT_NOT_RELEASE_SAFE"


def test_download_denies_unpublished_partial_and_owner_escape_paths(
    jobs_client,
) -> None:
    client, _, _, repository, layout, dataset = jobs_client
    created = _create_utility(client, dataset, "download-denials")
    job = repository.get_job(created["job_id"])
    unpublished = _publish(
        repository,
        job,
        kind="model_checkpoint",
        filename="checkpoint.bin",
        content=b"private",
        downloadable=False,
        release_safe=False,
        private=True,
    )
    response = client.get(f"/api/v1/artifacts/{unpublished.artifact_id}/download")
    assert response.status_code == 409
    assert response.json()["code"] == "ARTIFACT_NOT_READY"

    partial = _publish(
        repository,
        job,
        kind="synthetic_csv",
        filename="output.csv.part",
        content=b"unfinished",
        downloadable=True,
        release_safe=False,
        private=False,
    )
    response = client.get(f"/api/v1/artifacts/{partial.artifact_id}/download")
    assert response.status_code == 409
    assert response.json()["code"] == "ARTIFACT_NOT_READY"

    outside = layout.root / "misplaced.bin"
    outside.write_bytes(b"not-owned")
    escaped = ArtifactManifest(
        artifact_id=uuid4(),
        kind="synthetic_csv",
        relative_path="misplaced.bin",
        sha256=hashlib.sha256(b"not-owned").hexdigest(),
        size_bytes=len(b"not-owned"),
        downloadable=True,
        release_safe=False,
        contains_private_source_information=False,
        job_id=job.job_id,
    )
    repository.register_artifact(escaped)
    response = client.get(f"/api/v1/artifacts/{escaped.artifact_id}/download")
    assert response.status_code == 409
    assert response.json()["code"] == "ARTIFACT_NOT_READY"

    with pytest.raises(ValidationError):
        ArtifactManifest(
            artifact_id=uuid4(),
            kind="synthetic_csv",
            relative_path="../private.csv",
            sha256="0" * 64,
            size_bytes=0,
            downloadable=True,
            release_safe=False,
            contains_private_source_information=False,
            job_id=job.job_id,
        )


def test_cancel_succeeded_or_failed_job_is_conflict(jobs_client) -> None:
    client, _, _, repository, _, dataset = jobs_client
    created = _create_utility(client, dataset, "failed-cancel")
    failed = repository.transition_job(
        created["job_id"],
        JobState.FAILED,
        expected_state=JobState.ADMITTED,
        error_code=ErrorCode.WORKER_FAILED,
    )
    assert failed.state is JobState.FAILED
    response = client.post(f"/api/v1/jobs/{failed.job_id}/cancel")
    assert response.status_code == 409
    assert response.json()["code"] == "INVALID_STATE"
