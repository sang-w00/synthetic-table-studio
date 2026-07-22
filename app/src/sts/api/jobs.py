from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import duckdb
import pyarrow.parquet as pq
from fastapi import APIRouter, Header, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse

from sts.domain import (
    JOB_RUNNING_STATES,
    JOB_TERMINAL_STATES,
    DatasetState,
    DomainError,
    ErrorCode,
    JobState,
    SynthesisRequest,
    UtilitySynthesisRequest,
    canonical_sha256,
)
from sts.jobs.protocol import SnapshotFile, WorkerRequestEnvelope
from sts.jobs.utility import (
    CheckpointCompatibility,
    admit_training_sample,
    bounded_priority_sample,
    create_argn_fit_request,
    hmac_key_commitment,
    load_argn_feature_availability,
    require_argn_feature_configuration,
)
from sts.privacy import load_dp_availability
from sts.rules import RuleSpec, compile_rules
from sts.rules.execution import (
    StructuralCodecs,
    audit_and_filter_source,
    prepare_model_batch,
)
from sts.storage import CatalogRepository, WorkspaceLayout
from sts.storage.atomic import sha256_file
from sts.storage.repository import JobRecord, OwnerType

if TYPE_CHECKING:
    from sts.jobs.runtime import UtilityJobRuntime

SSE_MEDIA_TYPE = "text/event-stream"
DEFAULT_HEARTBEAT_SECONDS = 15.0
DEFAULT_RETENTION_DAYS = 30
DEFAULT_WORKER_LEASE_BYTES = 24 * 1024**3
DEFAULT_UTILITY_MAX_ROWS = 250_000
_ARGN_MODEL_ALIASES = {"small": "MOSTLY_AI/Small"}


def _default_probe_path(name: str) -> Path:
    return Path(__file__).resolve().parents[4] / "probes" / "results" / name


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.part")
    try:
        with temporary.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _write_parquet_atomic(path: Path, table: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.part")
    try:
        pq.write_table(table, temporary, compression="zstd")
        descriptor = os.open(temporary, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _event_payload(record: Any) -> str:
    data = {
        "attempt": record.attempt,
        "sequence": record.sequence,
        "timestamp": record.timestamp,
        "terminal": record.terminal,
        **record.payload,
    }
    event_name = "terminal" if record.terminal else "progress"
    encoded = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return f"id: {record.event_id}\nevent: {event_name}\ndata: {encoded}\n\n"


def _parse_last_event_id(request: Request) -> int:
    raw = request.headers.get("Last-Event-ID", "0").strip()
    try:
        value = int(raw or "0")
    except ValueError as error:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            "Last-Event-ID must be a non-negative integer",
        ) from error
    if value < 0:
        raise DomainError(ErrorCode.SCHEMA_INVALID, "Last-Event-ID must be non-negative")
    return value


async def job_event_stream(
    repository: CatalogRepository,
    job_id: UUID | str,
    *,
    after_event_id: int = 0,
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
    poll_seconds: float = 0.1,
    retention_days: int = DEFAULT_RETENTION_DAYS,
) -> AsyncIterator[str]:
    repository.get_job(job_id)
    cursor = after_event_id
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    heartbeat_at = asyncio.get_running_loop().time() + heartbeat_seconds
    while True:
        events = repository.replay_events(OwnerType.JOB, job_id, after_event_id=cursor)
        for event in events:
            cursor = event.event_id
            timestamp = datetime.fromisoformat(event.timestamp.replace("Z", "+00:00"))
            if timestamp < cutoff:
                continue
            yield _event_payload(event)
            if event.terminal:
                return
        now = asyncio.get_running_loop().time()
        if now >= heartbeat_at:
            yield ": heartbeat\n\n"
            heartbeat_at = now + heartbeat_seconds
        await asyncio.sleep(poll_seconds)


def job_event_response(
    repository: CatalogRepository,
    job_id: UUID | str,
    request: Request,
    *,
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
    poll_seconds: float = 0.1,
) -> StreamingResponse:
    stream = job_event_stream(
        repository,
        job_id,
        after_event_id=_parse_last_event_id(request),
        heartbeat_seconds=heartbeat_seconds,
        poll_seconds=poll_seconds,
    )
    return StreamingResponse(
        stream,
        media_type=SSE_MEDIA_TYPE,
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


class JobService:
    """Job control plane with a hard utility/formal-DP capability boundary."""

    def __init__(
        self,
        repository: CatalogRepository,
        workspace: WorkspaceLayout,
        *,
        argn_probe_path: str | Path | None = None,
        dpmm_probe_path: str | Path | None = None,
        worker_lease_bytes: int = DEFAULT_WORKER_LEASE_BYTES,
        utility_max_rows: int = DEFAULT_UTILITY_MAX_ROWS,
        argn_gate_required: bool = True,
    ) -> None:
        if worker_lease_bytes <= 0 or utility_max_rows <= 0:
            raise ValueError("worker lease and utility row cap must be positive")
        self.repository = repository
        self.workspace = workspace
        self.workspace.initialize()
        self.argn_probe_path = Path(argn_probe_path or _default_probe_path("argn_contract.json"))
        self.dpmm_probe_path = Path(dpmm_probe_path or _default_probe_path("dpmm_contract.json"))
        self.worker_lease_bytes = worker_lease_bytes
        self.utility_max_rows = utility_max_rows
        self.argn_gate_required = argn_gate_required
        self.runtime: UtilityJobRuntime | None = None

    def attach_runtime(self, runtime: UtilityJobRuntime) -> None:
        if self.runtime is not None and self.runtime is not runtime:
            raise RuntimeError("job service already has a utility runtime")
        self.runtime = runtime

    def _emit(
        self,
        job_id: UUID | str,
        *,
        stage: str,
        state: JobState,
        completed: int = 0,
        total: int = 1,
        terminal: bool = False,
        code: str | None = None,
        metrics: Mapping[str, Any] | None = None,
    ) -> None:
        self.repository.append_event(
            OwnerType.JOB,
            job_id,
            {
                "version": "1.0",
                "stage": stage,
                "state": state.value,
                "completed": completed,
                "total": total,
                "unit": "steps",
                "message_code": code or f"JOB_{state.value.upper()}",
                "metrics": dict(metrics or {}),
            },
            terminal=terminal,
        )

    def _verified_dp_gate(self, synthesizer: str) -> None:
        availability = load_dp_availability(self.dpmm_probe_path)
        selected_enabled = availability.formal_dp_enabled and (
            synthesizer != "aim" or availability.aim_enabled
        )
        if not selected_enabled:
            raise DomainError(
                ErrorCode.BACKEND_INCOMPATIBLE,
                f"{synthesizer} did not pass the required formal-DP Phase 0 gate",
                context={
                    "synthesizer": synthesizer,
                    "probe_gate": "failed",
                    "probe_status": availability.probe_status,
                    "formal_dp_enabled": availability.formal_dp_enabled,
                    "aim_enabled": availability.aim_enabled,
                    "failed_gates": list(availability.failed_gates),
                    "failure_reasons": list(availability.failure_reasons),
                },
            )
        # A passing probe alone must never silently expose a worker route. The app-side
        # fit/sample boundary remains disabled until its implementation is reviewed.
        raise DomainError(
            ErrorCode.BACKEND_INCOMPATIBLE,
            "formal-DP app execution route is not enabled",
            context={
                "synthesizer": synthesizer,
                "probe_gate": "passed",
                "app_route": "disabled",
            },
        )

    def _validate_utility(self, request: UtilitySynthesisRequest) -> tuple[Any, Any, Any]:
        dataset = self.repository.get_dataset(request.dataset_id)
        if dataset.state is not DatasetState.NORMALIZED:
            raise DomainError(
                ErrorCode.INVALID_STATE,
                "utility synthesis requires a normalized dataset",
                context={"dataset_state": dataset.state.value},
            )
        manifest = self.repository.get_dataset_manifest(request.dataset_id)
        mismatches: dict[str, dict[str, str]] = {}
        for name, expected, actual in (
            ("dataset_manifest_sha", request.dataset_manifest_sha, dataset.manifest_sha256),
            ("schema_version", request.schema_version, manifest.schema_version),
            ("rules_version", request.rules_version, manifest.rules_version),
        ):
            if expected != actual:
                mismatches[name] = {"expected": expected, "actual": actual}
        if mismatches:
            raise DomainError(
                ErrorCode.INVALID_STATE,
                "synthesis request does not match the immutable normalized dataset snapshot",
                context={"mismatches": mismatches},
            )
        if manifest.normalized is None:
            raise DomainError(ErrorCode.INVALID_STATE, "normalized dataset artifact is unavailable")
        if request.training.max_rows > self.utility_max_rows:
            raise DomainError(
                ErrorCode.RESOURCE_LIMIT,
                "utility training row cap exceeds this host's admitted limit",
                context={
                    "requested_max_rows": request.training.max_rows,
                    "maximum_rows": self.utility_max_rows,
                },
            )
        availability = load_argn_feature_availability(self.argn_probe_path)
        if self.argn_gate_required:
            require_argn_feature_configuration(
                availability,
                device=request.training.device,
                process_count=1,
            )
        return dataset, manifest, availability

    def _plan_path(self, record: JobRecord, *, create: bool = False) -> Path:
        return (
            self.workspace.job_attempt_dir(
                record.job_id,
                record.attempt,
                create=create,
            )
            / "argn-fit-request.json"
        )

    def _compiled_rules(self, manifest: Any) -> Any:
        relative = manifest.metadata.get("rules_relative_path")
        if relative is None:
            return compile_rules(manifest.columns, (), mode="utility")
        if not isinstance(relative, str):
            raise DomainError(ErrorCode.SCHEMA_INVALID, "rules manifest path is invalid")
        path = self.workspace.resolve_relative(relative, require_exists=True)
        try:
            payload = json.loads(path.read_bytes())
            rules = tuple(RuleSpec.model_validate(value) for value in payload["rules"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise DomainError(ErrorCode.SCHEMA_INVALID, "persisted rules are invalid") from error
        return compile_rules(manifest.columns, rules, mode="utility")

    def prepare_runtime_plan(self, job_id: UUID | str) -> WorkerRequestEnvelope:
        record = self.repository.get_job(job_id)
        if record.state is not JobState.PREPARING:
            raise DomainError(
                ErrorCode.INVALID_STATE,
                "utility plan preparation requires a preparing job",
            )
        request = self.repository.get_job_request(job_id).value
        if not isinstance(request, UtilitySynthesisRequest):
            raise DomainError(ErrorCode.BACKEND_INCOMPATIBLE, "runtime never prepares DP jobs")
        _, manifest, availability = self._validate_utility(request)
        return self._build_argn_plan(record, request, manifest, availability)

    def load_runtime_rule_context(
        self, job_id: UUID | str
    ) -> tuple[Any, StructuralCodecs, dict[str, Any]]:
        record = self.repository.get_job(job_id)
        manifest = self.repository.get_dataset_manifest(record.dataset_id)
        compiled = self._compiled_rules(manifest)
        metadata_path = self._plan_path(record).parent / "argn-plan-metadata.json"
        try:
            private_plan = json.loads(metadata_path.read_bytes())
            fixed = {
                rule_id: tuple(tuple(values) for values in tuples)
                for rule_id, tuples in private_plan.get("fixed_tuples", {}).items()
            }
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise DomainError(
                ErrorCode.WORKER_FAILED, "utility plan metadata is invalid"
            ) from error
        return compiled, StructuralCodecs(fixed_tuples=fixed), private_plan

    def _build_argn_plan(
        self,
        record: JobRecord,
        request: UtilitySynthesisRequest,
        manifest: Any,
        availability: Any,
    ) -> WorkerRequestEnvelope:
        plan_path = self._plan_path(record, create=True)
        if plan_path.exists():
            return WorkerRequestEnvelope.model_validate_json(plan_path.read_bytes())

        normalized = self.workspace.resolve_relative(
            manifest.normalized.relative_path,
            require_exists=True,
        )
        attempt_dir = plan_path.parent
        compiled = self._compiled_rules(manifest)
        if compiled.rules:
            filtered = attempt_dir / "rule-filtered-source.parquet"
            audit = audit_and_filter_source(normalized, filtered, compiled)
            rule_source = filtered
            codecs = audit.codecs
            audit_report: Mapping[str, Any] | None = audit.report.public_context()
        else:
            rule_source = normalized
            codecs = StructuralCodecs(fixed_tuples={})
            audit_report = None
        partition_key = secrets.token_bytes(32)
        with duckdb.connect() as connection:
            sample = bounded_priority_sample(
                connection,
                normalized_parquet=rule_source,
                partition_key=partition_key,
                max_rows=request.training.max_rows,
            )
        model_sample = prepare_model_batch(sample.table, compiled, codecs=codecs)
        memory = admit_training_sample(
            model_sample,
            worker_lease_bytes=self.worker_lease_bytes,
        )

        sample_path = attempt_dir / "argn-bounded-training.parquet"
        _write_parquet_atomic(sample_path, model_sample)
        digest, size = sha256_file(sample_path)
        sample_snapshot = SnapshotFile(
            path=self.workspace.as_relative(sample_path),
            sha256=digest,
            size_bytes=size,
        )
        checkpoint_path = self.workspace.as_relative(attempt_dir) + "/argn-checkpoint"
        cancellation_path = self.workspace.as_relative(attempt_dir) + "/cancel.requested"
        rules_sha256 = manifest.rules_sha256 or canonical_sha256(
            {"version": "1.0", "rules_version": manifest.rules_version}
        )
        compatibility = CheckpointCompatibility(
            source_manifest_sha256=request.dataset_manifest_sha,
            schema_sha256=canonical_sha256(manifest.columns),
            rules_sha256=rules_sha256,
            engine_sha256=availability.engine_sha256,
        )
        split_seed = request.generation_seed
        if split_seed is None:
            split_seed = int.from_bytes(
                hashlib.sha256(
                    f"{record.job_id}:{record.request_sha256}:argn-split".encode()
                ).digest()[:4],
                "big",
            )
        else:
            split_seed %= 2**32
        worker_request = create_argn_fit_request(
            workspace_root=self.workspace.root,
            request_id=str(uuid4()),
            job_id=str(record.job_id),
            attempt=record.attempt,
            cancellation_path=cancellation_path,
            bounded_training_sample=sample_snapshot,
            checkpoint_path=checkpoint_path,
            compatibility=compatibility,
            max_epochs=request.training.max_epochs,
            max_minutes=request.training.max_minutes,
            model_size=_ARGN_MODEL_ALIASES.get(
                request.training.model_size.casefold(),
                request.training.model_size,
            ),
            device=request.training.device,
            split_seed=split_seed,
            worker_lease_bytes=self.worker_lease_bytes,
        )
        _atomic_bytes(plan_path, worker_request.model_dump_json().encode("utf-8"))
        metadata_path = attempt_dir / "argn-plan-metadata.json"
        _atomic_bytes(
            metadata_path,
            json.dumps(
                {
                    "version": "1.0",
                    "hmac_key_commitment": hmac_key_commitment(partition_key),
                    "partition_key_hex": partition_key.hex(),
                    "train_threshold_u64": sample.partition.train_threshold_u64,
                    "rule_filtered_relative_path": self.workspace.as_relative(rule_source),
                    "selected_training_rows": model_sample.num_rows,
                    "memory_admission": memory.model_dump(mode="json"),
                    "source_audit": audit_report,
                    "fixed_tuples": {
                        rule_id: [list(values) for values in tuples]
                        for rule_id, tuples in codecs.fixed_tuples.items()
                    },
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
        )
        return worker_request

    def create(self, request: SynthesisRequest, idempotency_key: str) -> dict[str, Any]:
        value = request.value
        if value.mode == "differential_privacy":
            # This is intentionally before every repository, source, and ledger operation.
            self._verified_dp_gate(value.synthesizer)
        if not isinstance(value, UtilitySynthesisRequest):
            raise DomainError(ErrorCode.BACKEND_INCOMPATIBLE, "unsupported synthesis backend")
        _, manifest, availability = self._validate_utility(value)
        try:
            record = self.repository.create_job(request, idempotency_key=idempotency_key)
        except ValueError as error:
            raise DomainError(ErrorCode.SCHEMA_INVALID, str(error)) from error

        existing_events = self.repository.replay_events(OwnerType.JOB, record.job_id)
        if not existing_events:
            self._emit(
                record.job_id,
                stage="queued",
                state=JobState.QUEUED,
                code="JOB_QUEUED",
            )
        if record.state is JobState.QUEUED:
            try:
                worker_request = (
                    self._build_argn_plan(record, value, manifest, availability)
                    if self.runtime is None
                    else None
                )
                record = self.repository.transition_job(
                    record.job_id,
                    JobState.ADMITTED,
                    expected_state=JobState.QUEUED,
                )
                record = self.repository.set_resume_boundary(record.job_id, "normalized_dataset")
                self._emit(
                    record.job_id,
                    stage="admission",
                    state=record.state,
                    completed=1,
                    code="JOB_ADMITTED",
                    metrics={
                        "worker_kind": "argn",
                        "operation": "fit",
                        "plan_ready": worker_request is not None,
                    },
                )
            except Exception as error:
                current = self.repository.get_job(record.job_id)
                if current.state not in JOB_TERMINAL_STATES:
                    failed = self.repository.transition_job(
                        record.job_id,
                        JobState.FAILED,
                        expected_state=current.state,
                        error_code=(
                            error.code
                            if isinstance(error, DomainError)
                            else ErrorCode.WORKER_FAILED
                        ),
                    )
                    self._emit(
                        failed.job_id,
                        stage="admission",
                        state=failed.state,
                        completed=1,
                        terminal=True,
                        code=(
                            error.code.value
                            if isinstance(error, DomainError)
                            else ErrorCode.WORKER_FAILED.value
                        ),
                    )
                raise
        if (
            self.runtime is not None
            and self.repository.get_job(record.job_id).state is JobState.ADMITTED
        ):
            self.runtime.submit(record.job_id)
        return self.get(record.job_id)

    def get(self, job_id: UUID | str) -> dict[str, Any]:
        record = self.repository.get_job(job_id)
        events = self.repository.replay_events(OwnerType.JOB, job_id)
        latest = events[-1] if events else None
        actions: list[str] = []
        if record.state in JOB_RUNNING_STATES:
            actions.append("cancel")
        if record.state in {JobState.FAILED, JobState.CANCELLED} and record.resume_boundary:
            actions.append("resume")
        plan_ready = self._plan_path(record).is_file()
        return {
            "job_id": str(record.job_id),
            "dataset_id": str(record.dataset_id),
            "state": record.state.value,
            "attempt": record.attempt,
            "request_sha256": record.request_sha256,
            "retry_of": str(record.retry_of) if record.retry_of else None,
            "resume_boundary": record.resume_boundary,
            "progress": latest.payload if latest else None,
            "legal_actions": actions,
            "planning_request": {
                "worker_kind": "argn",
                "operation": "fit",
                "ready": plan_ready,
            }
            if plan_ready
            else None,
        }

    def cancel(self, job_id: UUID | str) -> tuple[int, dict[str, Any]]:
        record = self.repository.get_job(job_id)
        if record.state in {JobState.CANCELLING, JobState.CANCELLED}:
            return status.HTTP_200_OK, self.get(job_id)
        if record.state in {JobState.SUCCEEDED, JobState.FAILED}:
            raise DomainError(
                ErrorCode.INVALID_STATE,
                f"cannot cancel a terminal {record.state.value} job",
            )
        record = self.repository.transition_job(
            job_id,
            JobState.CANCELLING,
            expected_state=record.state,
        )
        cancellation_path = (
            self.workspace.job_attempt_dir(
                record.job_id,
                record.attempt,
                create=True,
            )
            / "cancel.requested"
        )
        if not cancellation_path.exists():
            _atomic_bytes(cancellation_path, b"cancel\n")
        self._emit(
            job_id,
            stage="cancelling",
            state=record.state,
            code="JOB_CANCELLING",
        )
        return status.HTTP_202_ACCEPTED, self.get(job_id)

    def resume(
        self,
        job_id: UUID | str,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        source_request = self.repository.get_job_request(job_id)
        if source_request.mode == "differential_privacy":
            self._verified_dp_gate(source_request.value.synthesizer)
        source = source_request.value
        if isinstance(source, UtilitySynthesisRequest):
            self._validate_utility(source)
        try:
            record = self.repository.resume_job(job_id, idempotency_key=idempotency_key)
        except ValueError as error:
            raise DomainError(ErrorCode.SCHEMA_INVALID, str(error)) from error
        if not self.repository.replay_events(OwnerType.JOB, record.job_id):
            self._emit(
                record.job_id,
                stage="resume",
                state=record.state,
                code="JOB_RESUMED",
                metrics={"retry_of": str(job_id), "resume_boundary": record.resume_boundary},
            )
        if self.runtime is not None:
            current = self.repository.get_job(record.job_id)
            if current.state is JobState.QUEUED:
                current = self.repository.transition_job(
                    current.job_id,
                    JobState.ADMITTED,
                    expected_state=JobState.QUEUED,
                )
                self._emit(
                    current.job_id,
                    stage="admission",
                    state=current.state,
                    completed=1,
                    code="JOB_ADMITTED",
                    metrics={"resume_boundary": current.resume_boundary},
                )
            if current.state is JobState.ADMITTED:
                self.runtime.submit(current.job_id)
        return self.get(record.job_id)


def create_job_router(service: JobService) -> APIRouter:
    router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])

    @router.post("", status_code=status.HTTP_201_CREATED)
    def create_job(
        body: SynthesisRequest,
        idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=255),
    ) -> dict[str, Any]:
        return service.create(body, idempotency_key)

    @router.get("/{job_id}")
    def get_job(job_id: UUID) -> dict[str, Any]:
        return service.get(job_id)

    @router.post("/{job_id}/cancel")
    def cancel_job(job_id: UUID) -> Response:
        response_status, payload = service.cancel(job_id)
        return JSONResponse(status_code=response_status, content=payload)

    @router.post("/{job_id}/resume", status_code=status.HTTP_201_CREATED)
    def resume_job(
        job_id: UUID,
        idempotency_key: str | None = Header(
            default=None,
            alias="Idempotency-Key",
            min_length=1,
            max_length=255,
        ),
    ) -> dict[str, Any]:
        return service.resume(job_id, idempotency_key=idempotency_key)

    @router.get("/{job_id}/events")
    def events(job_id: UUID, request: Request) -> Response:
        service.repository.get_job(job_id)
        return job_event_response(service.repository, job_id, request)

    return router
