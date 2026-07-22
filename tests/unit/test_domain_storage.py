from __future__ import annotations

import hashlib
import itertools
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from sts.domain import (
    ArtifactManifest,
    ColumnKind,
    ColumnRole,
    ColumnSchema,
    DATASET_TRANSITIONS,
    JOB_TRANSITIONS,
    DatasetManifest,
    DatasetState,
    DifferentialPrivacySynthesisRequest,
    DomainError,
    ErrorCode,
    JobState,
    ManifestFile,
    SynthesisRequest,
    UtilitySynthesisRequest,
    canonical_json_bytes,
    canonical_sha256,
    validate_dataset_transition,
    validate_job_transition,
)
from sts.storage import (
    ArtifactScope,
    AtomicPublisher,
    CatalogRepository,
    OwnerType,
    WorkspaceLayout,
)


EXPECTED_DATASET_TRANSITIONS = {
    DatasetState.UPLOADING: {DatasetState.STAGED},
    DatasetState.STAGED: {DatasetState.INSPECTING},
    DatasetState.INSPECTING: {
        DatasetState.PARSE_OPTIONS_REQUIRED,
        DatasetState.SHEET_REQUIRED,
        DatasetState.RAW_READY,
        DatasetState.FAILED,
    },
    DatasetState.PARSE_OPTIONS_REQUIRED: {DatasetState.INSPECTING},
    DatasetState.SHEET_REQUIRED: {DatasetState.INSPECTING},
    DatasetState.RAW_READY: {DatasetState.PROFILING},
    DatasetState.PROFILING: {DatasetState.PROFILED, DatasetState.FAILED},
    DatasetState.PROFILED: {DatasetState.SCHEMA_READY},
    DatasetState.SCHEMA_READY: {DatasetState.NORMALIZING},
    DatasetState.NORMALIZING: {DatasetState.NORMALIZED, DatasetState.FAILED},
    DatasetState.NORMALIZED: set(),
    DatasetState.FAILED: set(),
}

EXPECTED_JOB_TRANSITIONS = {
    JobState.QUEUED: {JobState.ADMITTED, JobState.CANCELLING, JobState.FAILED},
    JobState.ADMITTED: {JobState.PREPARING, JobState.CANCELLING, JobState.FAILED},
    JobState.PREPARING: {JobState.FITTING, JobState.CANCELLING, JobState.FAILED},
    JobState.FITTING: {JobState.GENERATING, JobState.CANCELLING, JobState.FAILED},
    JobState.GENERATING: {JobState.REPAIRING, JobState.CANCELLING, JobState.FAILED},
    JobState.REPAIRING: {JobState.EVALUATING, JobState.CANCELLING, JobState.FAILED},
    JobState.EVALUATING: {JobState.EXPORTING, JobState.CANCELLING, JobState.FAILED},
    JobState.EXPORTING: {JobState.PUBLISHING, JobState.CANCELLING, JobState.FAILED},
    JobState.PUBLISHING: {JobState.SUCCEEDED, JobState.CANCELLING, JobState.FAILED},
    JobState.CANCELLING: {JobState.CANCELLED, JobState.FAILED},
    JobState.SUCCEEDED: set(),
    JobState.CANCELLED: set(),
    JobState.FAILED: set(),
}


def make_dataset_manifest(dataset_id: UUID | None = None) -> DatasetManifest:
    identifier = dataset_id or uuid4()
    return DatasetManifest(
        dataset_id=identifier,
        source=ManifestFile(
            relative_path=f"datasets/{identifier}/source.csv",
            sha256="a" * 64,
            size_bytes=123,
        ),
        schema_version="1.0",
        rules_version="1.0",
        columns=(
            ColumnSchema(
                name="amount",
                kind=ColumnKind.FIXED_DECIMAL,
                nullable=False,
                role=ColumnRole.MODEL,
                decimal_places=2,
                public_min=Decimal("0.00"),
                public_max=Decimal("100.00"),
            ),
        ),
        metadata={"b": 2, "a": "Cafe\u0301"},
    )


def utility_request(
    manifest: DatasetManifest, *, output_rows: int = 100
) -> dict[str, object]:
    return {
        "version": "1.0",
        "dataset_id": str(manifest.dataset_id),
        "dataset_manifest_sha": manifest.canonical_sha256,
        "schema_version": "1.0",
        "rules_version": "1.0",
        "mode": "utility",
        "synthesizer": "tabular_argn",
        "output_rows": output_rows,
        "output_formats": ["parquet", "csv"],
        "resource_profile": "m4-default",
        "evaluation_config_version": "1.0",
        "training": {
            "max_rows": 250_000,
            "max_epochs": 5,
            "max_minutes": 60,
            "model_size": "medium",
            "device": "cpu",
        },
    }


def create_catalog(
    tmp_path: Path,
) -> tuple[WorkspaceLayout, CatalogRepository, DatasetManifest]:
    layout = WorkspaceLayout(tmp_path / "workspace")
    repository = CatalogRepository.open_workspace(layout)
    manifest = make_dataset_manifest()
    repository.create_dataset(manifest)
    return layout, repository, manifest


def test_dataset_transition_matrix_covers_every_legal_and_illegal_pair() -> None:
    assert {state: set(targets) for state, targets in DATASET_TRANSITIONS.items()} == (
        EXPECTED_DATASET_TRANSITIONS
    )
    for current, target in itertools.product(DatasetState, repeat=2):
        if target in EXPECTED_DATASET_TRANSITIONS[current]:
            assert validate_dataset_transition(current, target) is target
        else:
            with pytest.raises(DomainError) as raised:
                validate_dataset_transition(current, target)
            assert raised.value.code is ErrorCode.INVALID_STATE


def test_job_transition_matrix_covers_every_legal_and_illegal_pair() -> None:
    assert {state: set(targets) for state, targets in JOB_TRANSITIONS.items()} == (
        EXPECTED_JOB_TRANSITIONS
    )
    for current, target in itertools.product(JobState, repeat=2):
        if target in EXPECTED_JOB_TRANSITIONS[current]:
            assert validate_job_transition(current, target) is target
        else:
            with pytest.raises(DomainError) as raised:
                validate_job_transition(current, target)
            assert raised.value.code is ErrorCode.INVALID_STATE


def test_canonical_json_digest_is_stable_and_strict() -> None:
    left = {"version": "1.0", "z": [3, 2, 1], "nested": {"b": 2, "a": "Cafe\u0301"}}
    right = {"nested": {"a": "Café", "b": 2}, "z": [3, 2, 1], "version": "1.0"}
    assert canonical_json_bytes(left) == canonical_json_bytes(right)
    assert canonical_sha256(left) == canonical_sha256(right)
    assert canonical_json_bytes(left) == (
        b'{"nested":{"a":"Caf\xc3\xa9","b":2},"version":"1.0","z":[3,2,1]}'
    )
    with pytest.raises(ValueError):
        canonical_json_bytes({"version": "1.0", "bad": float("nan")})

    manifest = make_dataset_manifest(UUID("00000000-0000-0000-0000-000000000001"))
    round_trip = DatasetManifest.model_validate_json(manifest.canonical_bytes())
    assert round_trip.canonical_sha256 == manifest.canonical_sha256


def test_manifest_contracts_require_explicit_artifact_safety_and_valid_columns() -> (
    None
):
    base = {
        "artifact_id": uuid4(),
        "kind": "synthetic_csv",
        "relative_path": f"jobs/{uuid4()}/attempt-1/output.csv",
        "sha256": "0" * 64,
        "size_bytes": 0,
    }
    with pytest.raises(ValidationError):
        ArtifactManifest.model_validate(base)
    with pytest.raises(ValidationError):
        ArtifactManifest.model_validate(
            {
                **base,
                "downloadable": True,
                "release_safe": True,
                "contains_private_source_information": True,
            }
        )
    with pytest.raises(ValidationError):
        ColumnSchema(
            name="money",
            kind="fixed_decimal",
            nullable=False,
            role="model",
        )
    with pytest.raises(ValidationError):
        ColumnSchema(
            name="id",
            kind="identifier",
            nullable=False,
            role="identifier",
        )


def test_synthesis_request_is_discriminated_and_mode_specific() -> None:
    manifest = make_dataset_manifest()
    utility = SynthesisRequest.model_validate(utility_request(manifest))
    assert isinstance(utility.root, UtilitySynthesisRequest)
    assert utility.mode == "utility"

    dp_payload = {
        **{
            key: value
            for key, value in utility_request(manifest).items()
            if key not in {"mode", "synthesizer", "training", "generation_seed"}
        },
        "mode": "differential_privacy",
        "synthesizer": "mst",
        "privacy": {
            "adjacency": "add_remove_one_row",
            "privacy_unit": "row",
            "epsilon_model": "3",
            "delta": "0.000001",
            "epsilon_preprocess": 0,
            "public_metadata_manifest": {
                "relative_path": f"datasets/{manifest.dataset_id}/public-metadata.json",
                "sha256": "b" * 64,
                "size_bytes": 100,
            },
            "public_target_count": 10_000,
            "fit_sampling_rate": "1",
        },
    }
    dp = SynthesisRequest.model_validate(dp_payload)
    assert isinstance(dp.root, DifferentialPrivacySynthesisRequest)
    assert dp.root.privacy.epsilon_preprocess == 0
    with pytest.raises(ValidationError):
        SynthesisRequest.model_validate(
            {**utility_request(manifest), "mode": "differential_privacy"}
        )
    with pytest.raises(ValidationError):
        SynthesisRequest.model_validate(
            {**utility_request(manifest), "synthesizer": "mst"}
        )


def test_atomic_publication_fsync_contract_no_part_and_checksum_failure(
    tmp_path: Path,
) -> None:
    layout = WorkspaceLayout(tmp_path / "workspace")
    publisher = AtomicPublisher(layout)
    destination = f"datasets/{uuid4()}/manifest.json"
    value = {"version": "1.0", "b": 2, "a": 1}
    expected = canonical_json_bytes(value)
    published = publisher.publish_json(destination, value)
    assert published.path.read_bytes() == expected
    assert published.sha256 == hashlib.sha256(expected).hexdigest()
    assert not list(layout.root.rglob("*.part"))

    with pytest.raises(DomainError) as immutable:
        publisher.publish_json(destination, value)
    assert immutable.value.code is ErrorCode.IMMUTABLE_PATH_EXISTS
    assert published.path.read_bytes() == expected

    failed_destination = f"datasets/{uuid4()}/bad.bin"
    with pytest.raises(DomainError) as mismatch:
        publisher.publish_bytes(failed_destination, b"actual", expected_sha256="0" * 64)
    assert mismatch.value.code is ErrorCode.CHECKSUM_MISMATCH
    assert not layout.resolve_relative(failed_destination).exists()
    assert not list(layout.root.rglob("*.part"))


def test_sqlite_wal_schema_dataset_retry_and_monotonic_events(tmp_path: Path) -> None:
    _, repository, manifest = create_catalog(tmp_path)
    try:
        assert repository.journal_mode == "wal"
        assert {
            "datasets",
            "jobs",
            "attempts",
            "artifacts",
            "events",
            "resource_leases",
            "privacy_scopes",
            "ledger_runs",
        } <= repository.table_names()
        first = repository.get_dataset(manifest.dataset_id)
        repository.transition_dataset(manifest.dataset_id, DatasetState.STAGED)
        repository.transition_dataset(manifest.dataset_id, DatasetState.INSPECTING)
        updated_manifest = DatasetManifest.model_validate(
            {**manifest.model_dump(mode="json"), "row_count": 42}
        )
        with pytest.raises(DomainError) as stale_state:
            repository.update_dataset_manifest(
                manifest.dataset_id,
                updated_manifest,
                expected_state=DatasetState.PROFILING,
            )
        assert stale_state.value.code is ErrorCode.INVALID_STATE
        updated_record = repository.update_dataset_manifest(
            manifest.dataset_id,
            updated_manifest,
            expected_state=DatasetState.INSPECTING,
        )
        assert updated_record.manifest_sha256 == updated_manifest.canonical_sha256
        assert repository.get_dataset_manifest(manifest.dataset_id).row_count == 42
        failed = repository.transition_dataset(manifest.dataset_id, DatasetState.FAILED)
        assert failed.failed_from_state is DatasetState.INSPECTING
        retried = repository.retry_dataset(manifest.dataset_id)
        assert retried.dataset_id == first.dataset_id
        assert retried.attempt == 2
        assert retried.attempt_id != first.attempt_id
        assert retried.state is DatasetState.INSPECTING
        assert (
            repository.latest_attempt(OwnerType.DATASET, manifest.dataset_id).operation
            == "inspect"
        )
        with pytest.raises(DomainError):
            repository.retry_dataset(manifest.dataset_id)

        event1 = repository.append_event(
            OwnerType.DATASET,
            manifest.dataset_id,
            {"version": "1.0", "stage": "inspect"},
        )
        event2 = repository.append_event(
            OwnerType.DATASET,
            manifest.dataset_id,
            {"version": "1.0", "stage": "failed"},
            terminal=True,
        )
        assert event2.event_id > event1.event_id
        assert [
            event.sequence
            for event in repository.replay_events("dataset", manifest.dataset_id)
        ] == [
            1,
            2,
        ]
        with pytest.raises(DomainError):
            repository.append_event(
                "dataset", manifest.dataset_id, {"version": "1.0", "stage": "late"}
            )
        with pytest.raises(ValueError):
            repository.append_event(
                "dataset", manifest.dataset_id, {"stage": "unversioned"}
            )
    finally:
        repository.close()


def test_job_idempotency_new_resume_ids_and_terminal_rules(tmp_path: Path) -> None:
    _, repository, manifest = create_catalog(tmp_path)
    try:
        payload = utility_request(manifest)
        first = repository.create_job(payload, idempotency_key="create-1")
        reordered = dict(reversed(list(payload.items())))
        same = repository.create_job(reordered, idempotency_key="create-1")
        assert same.job_id == first.job_id
        assert same.attempt_id == first.attempt_id
        with pytest.raises(DomainError) as conflict:
            repository.create_job(
                utility_request(manifest, output_rows=101), idempotency_key="create-1"
            )
        assert conflict.value.code is ErrorCode.IDEMPOTENCY_CONFLICT

        with pytest.raises(DomainError) as running_resume:
            repository.resume_job(first.job_id)
        assert running_resume.value.code is ErrorCode.RESUME_UNAVAILABLE
        repository.set_resume_boundary(first.job_id, "validated_fit_checkpoint")
        repository.transition_job(first.job_id, JobState.FAILED)
        resumed = repository.resume_job(first.job_id, idempotency_key="resume-1")
        same_resume = repository.resume_job(first.job_id, idempotency_key="resume-1")
        assert resumed.job_id != first.job_id
        assert resumed.attempt_id != first.attempt_id
        assert resumed.attempt == 2
        assert resumed.retry_of == first.job_id
        assert resumed.state is JobState.QUEUED
        assert same_resume.job_id == resumed.job_id
        assert repository.get_job(first.job_id).state is JobState.FAILED

        no_boundary = repository.create_job(payload, idempotency_key="create-2")
        repository.transition_job(no_boundary.job_id, JobState.FAILED)
        with pytest.raises(DomainError) as unavailable:
            repository.resume_job(no_boundary.job_id)
        assert unavailable.value.code is ErrorCode.RESUME_UNAVAILABLE

        succeeded = repository.create_job(payload, idempotency_key="create-3")
        for state in (
            JobState.ADMITTED,
            JobState.PREPARING,
            JobState.FITTING,
            JobState.GENERATING,
            JobState.REPAIRING,
            JobState.EVALUATING,
            JobState.EXPORTING,
            JobState.PUBLISHING,
            JobState.SUCCEEDED,
        ):
            repository.transition_job(succeeded.job_id, state)
        with pytest.raises(DomainError) as terminal:
            repository.resume_job(succeeded.job_id)
        assert terminal.value.code is ErrorCode.RESUME_UNAVAILABLE
    finally:
        repository.close()


def test_artifact_catalog_verifies_checksum_and_filters_only_explicit_flags(
    tmp_path: Path,
) -> None:
    layout, repository, dataset = create_catalog(tmp_path)
    try:
        job = repository.create_job(
            utility_request(dataset), idempotency_key="artifact-job"
        )
        entries = [
            ("release_named_but_utility.csv", b"utility", True, False, False),
            ("unsafe-looking-internal-name.json", b"formal", False, True, False),
            ("public-looking-report.json", b"private", False, False, True),
        ]
        manifests: list[ArtifactManifest] = []
        for filename, content, downloadable, release_safe, contains_private in entries:
            relative_path = f"jobs/{job.job_id}/attempt-1/{filename}"
            manifest = ArtifactManifest(
                artifact_id=uuid4(),
                kind="synthetic_csv"
                if filename.endswith(".csv")
                else "primary_report_json",
                relative_path=relative_path,
                sha256=hashlib.sha256(content).hexdigest(),
                size_bytes=len(content),
                downloadable=downloadable,
                release_safe=release_safe,
                contains_private_source_information=contains_private,
                job_id=job.job_id,
            )
            repository.publish_artifact_bytes(manifest, content)
            manifests.append(manifest)
        assert {
            item.artifact_id for item in repository.list_artifacts(job_id=job.job_id)
        } == {manifests[0].artifact_id}
        assert {
            item.artifact_id
            for item in repository.list_artifacts(
                job_id=job.job_id, scope=ArtifactScope.DP_RELEASE
            )
        } == {manifests[1].artifact_id}
        assert {
            item.artifact_id
            for item in repository.list_artifacts(job_id=job.job_id, scope="internal")
        } == {item.artifact_id for item in manifests}

        corrupt_relative = f"jobs/{job.job_id}/attempt-1/corrupt.bin"
        AtomicPublisher(layout).publish_bytes(corrupt_relative, b"wrong")
        corrupt_manifest = ArtifactManifest(
            artifact_id=uuid4(),
            kind="model_checkpoint",
            relative_path=corrupt_relative,
            sha256=hashlib.sha256(b"expected").hexdigest(),
            size_bytes=len(b"expected"),
            downloadable=False,
            release_safe=False,
            contains_private_source_information=True,
            job_id=job.job_id,
        )
        with pytest.raises(DomainError) as mismatch:
            repository.register_artifact(corrupt_manifest)
        assert mismatch.value.code is ErrorCode.CHECKSUM_MISMATCH
        assert not list(layout.root.rglob("*.part"))
    finally:
        repository.close()
