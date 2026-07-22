from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import shutil
import sqlite3
import threading
from collections.abc import Iterable, Iterator, Mapping
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol
from uuid import UUID, uuid4

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from sts.domain import ColumnKind, DomainError, ErrorCode, JobState, UtilitySynthesisRequest
from sts.evaluation import (
    EvaluationConfig,
    deterministic_hmac_sample,
    evaluate_primary,
    exact_full_scan,
)
from sts.export import (
    build_parquet_shard_manifest,
    create_parquet_zip64_store,
    export_csv_from_parquet,
    publish_export_artifact,
    write_parquet_shard_manifest,
)
from sts.export.models import ExportedFile
from sts.jobs.protocol import (
    SnapshotFile,
    WorkerError,
    WorkerRequestEnvelope,
    WorkerResultEnvelope,
    confined_output_path,
    read_request,
    write_result_atomic,
)
from sts.jobs.seeds import CandidateShardPlan, derive_uint32_seed
from sts.jobs.supervisor import WorkerExecution, WorkerSupervisor
from sts.jobs.utility import (
    CheckpointCompatibility,
    create_argn_generate_request,
    register_hmac_partition_sql,
    rows_per_candidate,
)
from sts.reports import ArtifactSafety, build_utility_primary_report, publish_report_artifacts
from sts.rules.execution import (
    StructuralCodecs,
    prepare_model_batch,
    repair_and_validate_candidate,
)
from sts.rules.rejection import CandidateBatch, GlobalRejectionCoordinator
from sts.storage import CatalogRepository, WorkspaceLayout
from sts.storage.atomic import sha256_file
from sts.storage.repository import JobRecord

if TYPE_CHECKING:
    from sts.api.jobs import JobService
    from sts.domain import ArtifactManifest
    from sts.rules.compiler import CompiledRules


class UtilityWorkerAdapter(Protocol):
    async def run(
        self,
        request_path: Path,
        events_path: Path,
        result_path: Path,
        *,
        stdout_path: Path | None = None,
        stderr_path: Path | None = None,
    ) -> WorkerExecution: ...


class RuntimeCancellation(RuntimeError):
    pass


class DeterministicLightweightAdapter:
    """Deterministic bounded worker used only when explicitly injected by verification.

    It preserves the subprocess envelope's fit/generate separation and writes generation
    in bounded Parquet batches. Production application construction never selects it.
    """

    def __init__(self, *, batch_rows: int = 65_536, pause_seconds: float = 0.0) -> None:
        if batch_rows <= 0 or pause_seconds < 0:
            raise ValueError("batch_rows must be positive and pause_seconds non-negative")
        self.batch_rows = batch_rows
        self.pause_seconds = pause_seconds
        self.generated_batches = 0
        self.maximum_generated_batch_rows = 0

    async def run(
        self,
        request_path: Path,
        events_path: Path,
        result_path: Path,
        *,
        stdout_path: Path | None = None,
        stderr_path: Path | None = None,
    ) -> WorkerExecution:
        del events_path
        request = read_request(request_path)
        stdout = stdout_path or result_path.with_name("worker.stdout")
        stderr = stderr_path or result_path.with_name("worker.stderr")
        stdout.parent.mkdir(parents=True, exist_ok=True)
        stdout.touch(exist_ok=True)
        stderr.touch(exist_ok=True)
        try:
            if request.operation == "fit":
                artifacts, usage = await asyncio.to_thread(self._fit, request)
            elif request.operation == "generate":
                artifacts, usage = await asyncio.to_thread(self._generate, request)
            else:
                raise ValueError(f"unsupported lightweight operation: {request.operation}")
            result = WorkerResultEnvelope(
                status="success", artifacts=artifacts, resource_usage=usage, error=None
            )
            exit_code = 0
        except RuntimeCancellation:
            result = WorkerResultEnvelope(
                status="cancelled",
                artifacts=[],
                resource_usage={},
                error=WorkerError(code="CANCELLED", message="worker was cancelled", details={}),
            )
            exit_code = 0
        except Exception as error:
            result = WorkerResultEnvelope(
                status="failure",
                artifacts=[],
                resource_usage={},
                error=WorkerError(
                    code="WORKER_FAILED", message=str(error) or type(error).__name__, details={}
                ),
            )
            exit_code = 1
        write_result_atomic(result_path, result)
        return WorkerExecution(
            request_id=request.request_id,
            worker_kind=request.worker_kind,
            exit_code=exit_code,
            peak_process_tree_rss_bytes=0,
            result=result,
            stdout_path=stdout,
            stderr_path=stderr,
        )

    @staticmethod
    def _cancelled(request: WorkerRequestEnvelope) -> bool:
        root = Path(request.manifest_snapshot.workspace_root)
        return confined_output_path(root, request.cancellation_path).exists()

    def _fit(self, request: WorkerRequestEnvelope) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if self._cancelled(request):
            raise RuntimeCancellation
        sample = request.manifest_snapshot.files["bounded_training_sample"]
        root = Path(request.manifest_snapshot.workspace_root)
        source = root / sample.path
        config = request.limits["argn"]
        checkpoint = confined_output_path(root, config["checkpoint_path"])
        if checkpoint.exists():
            raise FileExistsError(checkpoint)
        staging = checkpoint.with_name(f".{checkpoint.name}.{uuid4().hex}.part")
        staging.mkdir(parents=True)
        try:
            shutil.copyfile(source, staging / "training.parquet")
            tags = {
                "version": "1.0",
                "compatibility": config["checkpoint_compatibility"],
                "feature_gates": {"multiprocess_clones": False},
                "adapter": "deterministic_lightweight",
            }
            (staging / "sts-checkpoint.json").write_text(
                json.dumps(tags, sort_keys=True, separators=(",", ":")), encoding="utf-8"
            )
            if self._cancelled(request):
                raise RuntimeCancellation
            os.replace(staging, checkpoint)
        finally:
            if staging.exists():
                shutil.rmtree(staging)
        digest, size = _directory_digest(checkpoint)
        return (
            [
                {
                    "kind": "model_checkpoint",
                    "path": config["checkpoint_path"],
                    "sha256": digest,
                    "size_bytes": size,
                    "downloadable": False,
                    "release_safe": False,
                    "contains_private_source_information": True,
                    "metadata": {"adapter": "deterministic_lightweight"},
                }
            ],
            {"bounded_training_rows": pq.ParquetFile(source).metadata.num_rows},
        )

    def _generate(
        self, request: WorkerRequestEnvelope
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if self._cancelled(request):
            raise RuntimeCancellation
        root = Path(request.manifest_snapshot.workspace_root)
        config = request.limits["argn"]
        checkpoint = root / config["checkpoint_path"]
        source = pq.read_table(checkpoint / "training.parquet")
        if source.num_rows == 0:
            raise ValueError("lightweight checkpoint has no training rows")
        destination = confined_output_path(root, config["candidate_output_path"])
        if destination.exists():
            raise FileExistsError(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        part = destination.with_name(f".{destination.name}.{uuid4().hex}.part")
        writer = pq.ParquetWriter(part, source.schema, compression="zstd")
        remaining = int(config["candidate_rows"])
        cursor = int(config["engine_seed"]) % source.num_rows
        try:
            while remaining:
                if self._cancelled(request):
                    raise RuntimeCancellation
                count = min(self.batch_rows, remaining)
                indices = pa.array(
                    ((cursor + index) % source.num_rows for index in range(count)),
                    type=pa.int64(),
                )
                batch = source.take(indices)
                writer.write_table(batch)
                self.generated_batches += 1
                self.maximum_generated_batch_rows = max(
                    self.maximum_generated_batch_rows, batch.num_rows
                )
                cursor = (cursor + count) % source.num_rows
                remaining -= count
                if self.pause_seconds:
                    import time

                    time.sleep(self.pause_seconds)
        finally:
            writer.close()
        if self._cancelled(request):
            part.unlink(missing_ok=True)
            raise RuntimeCancellation
        os.replace(part, destination)
        digest, size = sha256_file(destination)
        return (
            [
                {
                    "kind": "synthetic_candidate_parquet",
                    "path": config["candidate_output_path"],
                    "sha256": digest,
                    "size_bytes": size,
                    "downloadable": False,
                    "release_safe": False,
                    "contains_private_source_information": False,
                    "metadata": {"rows": config["candidate_rows"]},
                }
            ],
            {
                "requested_rows": config["candidate_rows"],
                "actual_rows": pq.ParquetFile(destination).metadata.num_rows,
                "adapter": "deterministic_lightweight",
            },
        )


class UtilityJobRuntime:
    """Single-host background runtime for admitted utility jobs only."""

    def __init__(
        self,
        repository: CatalogRepository,
        workspace: WorkspaceLayout,
        service: JobService,
        *,
        adapter: UtilityWorkerAdapter | None = None,
        worker_lease_bytes: int = 24 * 1024**3,
    ) -> None:
        if worker_lease_bytes <= 0:
            raise ValueError("worker_lease_bytes must be positive")
        self.repository = repository
        self.workspace = workspace
        self.service = service
        self.adapter: UtilityWorkerAdapter = adapter or WorkerSupervisor()
        self.worker_lease_bytes = worker_lease_bytes
        self._queue: asyncio.Queue[UUID | None] = asyncio.Queue()
        self._runner: asyncio.Task[None] | None = None
        self._active: dict[UUID, asyncio.Task[None]] = {}
        self._submitted: set[UUID] = set()
        self._pending: set[UUID] = set()
        self._submission_lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._worker_lock = asyncio.Lock()
        self._stopping = False

    async def start(self) -> None:
        if self._runner is not None:
            return
        loop = asyncio.get_running_loop()
        with self._submission_lock:
            self._loop = loop
            self._stopping = False
            pending = tuple(self._pending)
            self._pending.clear()
        self._runner = asyncio.create_task(self._dispatch(), name="sts-utility-runtime")
        for job_id in (*self._admitted_job_ids(), *pending):
            self._submit_on_loop(job_id)

    async def stop(self) -> None:
        if self._runner is None:
            return
        with self._submission_lock:
            self._stopping = True
        for job_id in tuple(self._active):
            self._request_cancel_file(job_id, "runtime_shutdown")
        await self._queue.put(None)
        await self._runner
        active = tuple(self._active.values())
        if active:
            try:
                await asyncio.wait_for(asyncio.gather(*active, return_exceptions=True), timeout=10)
            except TimeoutError:
                for task in active:
                    task.cancel()
                await asyncio.gather(*active, return_exceptions=True)
        with self._submission_lock:
            self._runner = None
            self._loop = None

    def submit(self, job_id: UUID | str) -> None:
        identifier = UUID(str(job_id))
        with self._submission_lock:
            loop = self._loop
            if loop is None or self._stopping:
                self._pending.add(identifier)
                return
        loop.call_soon_threadsafe(self._submit_on_loop, identifier)

    async def wait(self, job_id: UUID | str, *, timeout: float = 300.0) -> JobRecord:
        identifier = UUID(str(job_id))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            record = self.repository.get_job(identifier)
            if record.state in {JobState.SUCCEEDED, JobState.CANCELLED, JobState.FAILED}:
                return record
            if loop.time() >= deadline:
                raise TimeoutError(f"job {identifier} did not finish")
            await asyncio.sleep(0.05)

    def _admitted_job_ids(self) -> tuple[UUID, ...]:
        connection = sqlite3.connect(
            f"file:{self.workspace.catalog_path}?mode=ro",
            uri=True,
        )
        try:
            rows = connection.execute(
                "SELECT id FROM jobs WHERE state = 'admitted' ORDER BY created_at, id"
            ).fetchall()
        finally:
            connection.close()
        return tuple(UUID(str(row[0])) for row in rows)

    def _submit_on_loop(self, job_id: UUID) -> None:
        with self._submission_lock:
            if self._stopping:
                self._pending.add(job_id)
                return
        if job_id in self._submitted:
            return
        self._submitted.add(job_id)
        self._queue.put_nowait(job_id)

    async def _dispatch(self) -> None:
        while True:
            job_id = await self._queue.get()
            if job_id is None:
                self._queue.task_done()
                return
            task = asyncio.create_task(self._run_guarded(job_id), name=f"sts-job-{job_id}")
            self._active[job_id] = task
            task.add_done_callback(lambda _task, key=job_id: self._active.pop(key, None))
            self._queue.task_done()

    async def _run_guarded(self, job_id: UUID) -> None:
        lease = None
        try:
            request = self.repository.get_job_request(job_id).value
            if not isinstance(request, UtilitySynthesisRequest):
                raise DomainError(ErrorCode.BACKEND_INCOMPATIBLE, "runtime never executes DP jobs")
            async with self._worker_lock:
                lease = self.repository.acquire_resource_lease(
                    job_id,
                    "utility_worker_rss",
                    self.worker_lease_bytes,
                    details={"worker_kind": "argn", "process_count": 1},
                )
                await self._run_job(job_id, request)
        except RuntimeCancellation:
            self._finish_cancelled(job_id)
        except asyncio.CancelledError:
            self._finish_cancelled(job_id)
            raise
        except Exception as error:
            self._finish_failed(job_id, error)
        finally:
            if lease is not None:
                self.repository.release_resource_lease(lease.lease_id)
            self._submitted.discard(job_id)

    async def _run_job(self, job_id: UUID, request: UtilitySynthesisRequest) -> None:
        record = self.repository.get_job(job_id)
        if record.state is JobState.CANCELLING:
            raise RuntimeCancellation
        if record.state is not JobState.ADMITTED:
            raise DomainError(
                ErrorCode.INVALID_STATE,
                f"runtime requires admitted job, found {record.state.value}",
            )
        record = self._advance(record, JobState.PREPARING)
        self._check_cancelled(record)

        resume_source = self._validated_checkpoint_source(record)
        if resume_source is None:
            fit_request = await asyncio.to_thread(self.service.prepare_runtime_plan, job_id)
            plan_record = record
        else:
            plan_record, fit_request = resume_source
            self.service._emit(
                job_id,
                stage="preparing",
                state=JobState.PREPARING,
                completed=1,
                code="JOB_RESUME_CHECKPOINT_READY",
                metrics={"source_job_id": str(plan_record.job_id)},
            )
        compiled, codecs, private_plan = self.service.load_runtime_rule_context(plan_record.job_id)

        record = self._advance(record, JobState.FITTING)
        if resume_source is None:
            execution = await self._run_worker(record, fit_request, "fit")
            self._require_worker_success(execution)
            self.repository.set_resume_boundary(job_id, "validated_fit_checkpoint")
        else:
            self.service._emit(
                job_id,
                stage="fitting",
                state=JobState.FITTING,
                completed=1,
                code="JOB_FIT_CHECKPOINT_REUSED",
                metrics={"source_job_id": str(plan_record.job_id)},
            )
        self._check_cancelled(record)

        record = self._advance(record, JobState.GENERATING)
        candidate_paths = await self._generate_candidates(
            record, request, fit_request, compiled, codecs
        )
        self._check_cancelled(record)

        record = self._advance(record, JobState.REPAIRING)
        output_path, rejection = await asyncio.to_thread(
            self._repair_candidates,
            record,
            request,
            compiled,
            codecs,
            candidate_paths,
        )
        for path in candidate_paths:
            path.unlink(missing_ok=True)
        self.repository.set_resume_boundary(job_id, "published_generation_shard")
        self._check_cancelled(record)

        record = self._advance(record, JobState.EVALUATING)
        evaluation = await asyncio.to_thread(
            self._evaluate,
            record,
            request,
            compiled,
            codecs,
            private_plan,
            output_path,
            rejection,
        )
        evaluation_path = self.workspace.job_attempt_dir(job_id, record.attempt) / "evaluation.json"
        _atomic_bytes(
            evaluation_path,
            json.dumps(evaluation, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        )
        self.repository.set_resume_boundary(job_id, "completed_evaluation_json")
        self._check_cancelled(record)

        record = self._advance(record, JobState.EXPORTING)
        staged = await asyncio.to_thread(
            self._stage_exports, record, request, compiled, output_path, evaluation
        )
        self._check_cancelled(record)

        record = self._advance(record, JobState.PUBLISHING)
        artifacts = await asyncio.to_thread(self._publish_exports, record, staged)
        report = build_utility_primary_report(
            job_id=job_id,
            evaluation=evaluation,
            artifacts=[artifact.model_dump(mode="json") for artifact in artifacts],
        )
        report_artifacts = await asyncio.to_thread(
            publish_report_artifacts,
            self.repository,
            report,
            job_id=job_id,
            attempt=record.attempt,
        )
        artifacts.extend(report_artifacts)
        self.repository.set_resume_boundary(job_id, "completed_export")
        succeeded = self.repository.transition_job(
            job_id, JobState.SUCCEEDED, expected_state=JobState.PUBLISHING
        )
        self.service._emit(
            job_id,
            stage="publishing",
            state=succeeded.state,
            completed=1,
            terminal=True,
            code="JOB_SUCCEEDED",
            metrics={"artifact_count": len(artifacts), "actual_rows": request.output_rows},
        )

    def _advance(self, record: JobRecord, target: JobState) -> JobRecord:
        self._check_cancelled(record)
        updated = self.repository.transition_job(record.job_id, target, expected_state=record.state)
        self.service._emit(
            record.job_id,
            stage=target.value,
            state=updated.state,
            completed=0,
            code=f"JOB_{target.value.upper()}",
        )
        return updated

    def _check_cancelled(self, record: JobRecord) -> None:
        current = self.repository.get_job(record.job_id)
        cancellation = (
            self.workspace.job_attempt_dir(current.job_id, current.attempt, create=True)
            / "cancel.requested"
        )
        if current.state is JobState.CANCELLING or cancellation.exists():
            raise RuntimeCancellation

    def _request_cancel_file(self, job_id: UUID, reason: str) -> None:
        try:
            record = self.repository.get_job(job_id)
        except DomainError:
            return
        path = (
            self.workspace.job_attempt_dir(job_id, record.attempt, create=True) / "cancel.requested"
        )
        if not path.exists():
            _atomic_bytes(path, f"{reason}\n".encode())

    def _finish_cancelled(self, job_id: UUID) -> None:
        record = self.repository.get_job(job_id)
        if record.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED}:
            return
        if record.state is not JobState.CANCELLING:
            record = self.repository.transition_job(
                job_id, JobState.CANCELLING, expected_state=record.state
            )
        record = self.repository.transition_job(
            job_id, JobState.CANCELLED, expected_state=JobState.CANCELLING
        )
        self.service._emit(
            job_id,
            stage="cancelled",
            state=record.state,
            completed=1,
            terminal=True,
            code="JOB_CANCELLED",
        )

    def _finish_failed(self, job_id: UUID, error: Exception) -> None:
        record = self.repository.get_job(job_id)
        if record.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED}:
            return
        if record.state is JobState.CANCELLING:
            self._finish_cancelled(job_id)
            return
        code = error.code if isinstance(error, DomainError) else ErrorCode.WORKER_FAILED
        failed = self.repository.transition_job(
            job_id, JobState.FAILED, expected_state=record.state, error_code=code
        )
        self.service._emit(
            job_id,
            stage=record.state.value,
            state=failed.state,
            completed=1,
            terminal=True,
            code=code.value,
            metrics={"error": str(error)},
        )

    def _validated_checkpoint_source(
        self, record: JobRecord
    ) -> tuple[JobRecord, WorkerRequestEnvelope] | None:
        if record.retry_of is None or record.resume_boundary not in {
            "validated_fit_checkpoint",
            "published_generation_shard",
            "completed_evaluation_json",
            "completed_export",
        }:
            return None
        source = self.repository.get_job(record.retry_of)
        plan_path = self.service._plan_path(source)
        if not plan_path.is_file():
            return None
        request = WorkerRequestEnvelope.model_validate_json(plan_path.read_bytes())
        checkpoint = self.workspace.resolve_relative(
            request.limits["argn"]["checkpoint_path"], require_exists=True
        )
        if not checkpoint.is_dir():
            return None
        return source, request

    async def _run_worker(
        self, record: JobRecord, request: WorkerRequestEnvelope, label: str
    ) -> WorkerExecution:
        attempt_dir = self.workspace.job_attempt_dir(record.job_id, record.attempt, create=True)
        request_path = attempt_dir / f"argn-{label}-request-{request.request_id}.json"
        if not request_path.exists():
            _atomic_bytes(request_path, request.model_dump_json().encode("utf-8"))
        execution = await self.adapter.run(
            request_path,
            attempt_dir / f"argn-{label}-events-{request.request_id}.jsonl",
            attempt_dir / f"argn-{label}-result-{request.request_id}.json",
            stdout_path=attempt_dir / f"argn-{label}.stdout",
            stderr_path=attempt_dir / f"argn-{label}.stderr",
        )
        return execution

    @staticmethod
    def _require_worker_success(execution: WorkerExecution) -> None:
        result = execution.result
        if result.status == "cancelled":
            raise RuntimeCancellation
        if result.status != "success":
            error = result.error
            code = ErrorCode.WORKER_FAILED
            if error is not None:
                with suppress(ValueError):
                    code = ErrorCode(error.code)
            raise DomainError(
                code,
                error.message if error is not None else "worker failed",
                context=error.details if error is not None else {},
            )

    async def _generate_candidates(
        self,
        record: JobRecord,
        request: UtilitySynthesisRequest,
        fit_request: WorkerRequestEnvelope,
        compiled: CompiledRules,
        codecs: StructuralCodecs,
    ) -> list[Path]:
        checkpoint_relative = fit_request.limits["argn"]["checkpoint_path"]
        checkpoint = self.workspace.resolve_relative(checkpoint_relative, require_exists=True)
        compatibility = CheckpointCompatibility.model_validate(
            fit_request.limits["argn"]["checkpoint_compatibility"]
        )
        snapshots = _checkpoint_snapshots(self.workspace, checkpoint)
        sample = fit_request.manifest_snapshot.files["bounded_training_sample"]
        sample_rows = max(
            1, pq.ParquetFile(self.workspace.resolve_relative(sample.path)).metadata.num_rows
        )
        sample_bytes = max(1, sample.size_bytes)
        decoded_row_bytes = max(1, math.ceil(sample_bytes / sample_rows))
        chunk_rows = rows_per_candidate(decoded_row_bytes)
        maximum_candidates = request.output_rows * 20
        maximum_calls = max(1, math.ceil(maximum_candidates / chunk_rows))
        seed_master = request.generation_seed
        if seed_master is None:
            seed_master = int.from_bytes(
                hashlib.sha256(f"{record.job_id}:generation".encode()).digest(), "big"
            )
        candidate_paths: list[Path] = []
        valid_rows = 0
        examined = 0
        used_seeds: set[int] = set()
        for index in range(maximum_calls):
            self._check_cancelled(record)
            remaining_budget = maximum_candidates - examined
            if remaining_budget <= 0:
                break
            generated_rows = max(10_000, min(chunk_rows, remaining_budget))
            seed = derive_uint32_seed(seed_master, purpose="argn-generation-candidate", index=index)
            if seed in used_seeds:
                raise DomainError(ErrorCode.WORKER_FAILED, "derived ARGN generation seed collision")
            used_seeds.add(seed)
            (
                self.workspace.job_attempt_dir(record.job_id, record.attempt, create=True)
                / "candidates"
            ).mkdir(parents=True, exist_ok=True)
            relative = (
                self.workspace.as_relative(
                    self.workspace.job_attempt_dir(record.job_id, record.attempt, create=True)
                )
                + f"/candidates/candidate-{index:06d}.parquet"
            )
            candidate = CandidateShardPlan(
                candidate_index=index,
                process_index=0,
                row_start=examined,
                output_rows=generated_rows,
                engine_seed=seed,
            )
            envelope = create_argn_generate_request(
                workspace_root=self.workspace.root,
                request_id=str(uuid4()),
                job_id=str(record.job_id),
                attempt=record.attempt,
                cancellation_path=(
                    self.workspace.as_relative(
                        self.workspace.job_attempt_dir(record.job_id, record.attempt)
                    )
                    + "/cancel.requested"
                ),
                checkpoint_files=snapshots,
                checkpoint_path=checkpoint_relative,
                candidate_output_path=relative,
                candidate=candidate,
                seed_count=maximum_calls,
                batch_size=min(10_000, generated_rows),
                device=request.training.device,
                process_count=1,
                actual_compatibility=compatibility,
                expected_compatibility=compatibility,
                worker_lease_bytes=self.worker_lease_bytes,
            )
            execution = await self._run_worker(record, envelope, f"generate-{index:06d}")
            self._require_worker_success(execution)
            path = self.workspace.resolve_relative(relative, require_exists=True)
            candidate_paths.append(path)
            budget_for_file = min(generated_rows, remaining_budget)
            for batch in pq.ParquetFile(path).iter_batches(batch_size=65_536):
                if budget_for_file <= 0:
                    break
                table = pa.Table.from_batches([batch])
                table = _coerce_argn_batch(table, compiled)
                if table.num_rows > budget_for_file:
                    table = table.slice(0, budget_for_file)
                checked = repair_and_validate_candidate(table, compiled, codecs=codecs)
                valid_rows += checked.report.valid_rows
                examined += table.num_rows
                budget_for_file -= table.num_rows
                if valid_rows >= request.output_rows:
                    break
            self.service._emit(
                record.job_id,
                stage="generating",
                state=JobState.GENERATING,
                completed=min(valid_rows, request.output_rows),
                total=request.output_rows,
                code="JOB_GENERATION_CANDIDATE_READY",
                metrics={"candidate_index": index, "examined": examined},
            )
            if valid_rows >= request.output_rows:
                return candidate_paths
        raise DomainError(
            ErrorCode.RULE_FEASIBILITY_EXHAUSTED,
            "global rule-feasibility candidate budget was exhausted",
            context={
                "requested_rows": request.output_rows,
                "valid_rows": valid_rows,
                "candidates_examined": examined,
                "max_candidates": maximum_candidates,
            },
        )

    def _repair_candidates(
        self,
        record: JobRecord,
        request: UtilitySynthesisRequest,
        compiled: CompiledRules,
        codecs: StructuralCodecs,
        candidate_paths: list[Path],
    ) -> tuple[Path, Any]:
        output = (
            self.workspace.job_attempt_dir(record.job_id, record.attempt, create=True)
            / "generated.parquet"
        )
        maximum = request.output_rows * 20

        def provider(_allocation: Any) -> Iterator[CandidateBatch]:
            cursor = 0
            for path in candidate_paths:
                for batch in pq.ParquetFile(path).iter_batches(batch_size=65_536):
                    self._check_cancelled(record)
                    if cursor >= maximum:
                        return
                    table = pa.Table.from_batches([batch])
                    table = _coerce_argn_batch(table, compiled)
                    if table.num_rows > maximum - cursor:
                        table = table.slice(0, maximum - cursor)
                    yield CandidateBatch(candidate_start=cursor, batch=table)
                    cursor += table.num_rows

        master_seed = request.generation_seed or int.from_bytes(
            hashlib.sha256(f"{record.job_id}:rejection".encode()).digest(), "big"
        )
        coordinator = GlobalRejectionCoordinator(
            compiled,
            output_rows=request.output_rows,
            shard_count=1,
            master_seed=master_seed,
            codecs=codecs,
        )
        result = coordinator.run(provider, output)
        return output, result

    def _evaluate(
        self,
        record: JobRecord,
        request: UtilitySynthesisRequest,
        compiled: CompiledRules,
        codecs: StructuralCodecs,
        private_plan: Mapping[str, Any],
        output_path: Path,
        rejection: Any,
    ) -> dict[str, Any]:
        exact = exact_full_scan(
            output_path,
            expected_columns=compiled.columns,
            requested_rows=request.output_rows,
            compiled_rules=compiled,
            codecs=codecs,
            expected_hard_rule_violations=0,
        )
        source_relative = private_plan.get("rule_filtered_relative_path")
        key_hex = private_plan.get("partition_key_hex")
        if not isinstance(source_relative, str) or not isinstance(key_hex, str):
            raise DomainError(
                ErrorCode.WORKER_FAILED, "utility plan lacks evaluation partition state"
            )
        source = self.workspace.resolve_relative(source_relative, require_exists=True)
        key = bytes.fromhex(key_hex)
        with duckdb.connect() as connection:
            partition = register_hmac_partition_sql(connection, key=key, normalized_parquet=source)
            projection = "* EXCLUDE (__sts_row_id, __sts_partition_score, __sts_priority)"
            limit = min(200_000, max(1, request.output_rows))
            train = connection.execute(
                f"SELECT {projection} FROM {partition.source_sql} "
                f"WHERE {partition.train_predicate()} "
                "ORDER BY __sts_priority, __sts_row_id LIMIT ?",
                [limit],
            ).to_arrow_table()
            holdout = connection.execute(
                f"SELECT {projection} FROM {partition.source_sql} "
                f"WHERE {partition.holdout_predicate()} "
                "ORDER BY __sts_priority, __sts_row_id LIMIT ?",
                [limit],
            ).to_arrow_table()
        train_final = repair_and_validate_candidate(
            prepare_model_batch(train, compiled, codecs=codecs), compiled, codecs=codecs
        ).table
        holdout_final = repair_and_validate_candidate(
            prepare_model_batch(holdout, compiled, codecs=codecs), compiled, codecs=codecs
        ).table
        evaluation_seed = request.generation_seed or int.from_bytes(
            hashlib.sha256(f"{record.job_id}:evaluation".encode()).digest()[:8], "big"
        )
        synthetic, synthetic_manifest = deterministic_hmac_sample(
            output_path,
            max_rows=min(200_000, request.output_rows),
            seed=evaluation_seed,
            namespace="utility-runtime-synthetic",
        )
        config = EvaluationConfig(master_seed=evaluation_seed)
        primary = evaluate_primary(
            train_final,
            holdout_final,
            synthetic,
            columns=compiled.columns,
            config=config,
            grouping_scope="utility_internal",
        )
        return {
            "version": "1.0",
            "configuration": config.model_dump(mode="json"),
            "exact": exact.model_dump(mode="json"),
            "primary": primary.model_dump(mode="json"),
            "synthetic_sample": synthetic_manifest.model_dump(mode="json"),
            "rejection": {
                "requested_rows": rejection.requested_rows,
                "actual_rows": rejection.actual_rows,
                "candidates_examined": rejection.candidates_examined,
                "candidates_rejected": rejection.candidates_rejected,
                "post_violations": rejection.post_violations,
            },
            "partition": {
                "key_commitment_sha256": private_plan.get("hmac_key_commitment"),
                "train_threshold_u64": private_plan.get("train_threshold_u64"),
            },
        }

    def _stage_exports(
        self,
        record: JobRecord,
        request: UtilitySynthesisRequest,
        compiled: CompiledRules,
        output_path: Path,
        evaluation: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        stage = (
            self.workspace.job_attempt_dir(record.job_id, record.attempt, create=True)
            / ".export-stage"
        )
        stage.mkdir(parents=True, exist_ok=True)
        exact = evaluation["exact"]
        source_sha, source_size = sha256_file(output_path)
        shard_export = ExportedFile(
            path=str(output_path),
            sha256=source_sha,
            size_bytes=source_size,
            row_count=request.output_rows,
            canonical_content_sha256=exact["canonical_content_sha256"],
        )
        shard_manifest = build_parquet_shard_manifest(
            [output_path], compiled.columns, relative_paths=["synthetic-part-00000.parquet"]
        )
        manifest_export = write_parquet_shard_manifest(
            shard_manifest, stage / "synthetic-parquet-manifest.json"
        )
        zip_export = create_parquet_zip64_store(
            [output_path],
            stage / "synthetic-parquet.zip",
            archive_names=["synthetic-part-00000.parquet"],
            manifest_path=manifest_export.path,
            manifest=shard_manifest,
        )
        items: list[dict[str, Any]] = [
            {
                "exported": shard_export,
                "kind": "synthetic_parquet_shard",
                "filename": "synthetic-part-00000.parquet",
                "downloadable": False,
            },
            {
                "exported": manifest_export,
                "kind": "synthetic_parquet_manifest",
                "filename": "synthetic-parquet-manifest.json",
                "downloadable": True,
            },
            {
                "exported": zip_export,
                "kind": "synthetic_parquet_zip",
                "filename": "synthetic-parquet.zip",
                "downloadable": True,
            },
        ]
        if any(str(value) == "csv" for value in request.output_formats):
            csv_export = export_csv_from_parquet(
                [output_path], stage / "synthetic.csv", compiled.columns
            )
            items.append(
                {
                    "exported": csv_export,
                    "kind": "synthetic_csv",
                    "filename": "synthetic.csv",
                    "downloadable": True,
                }
            )
        return items

    def _publish_exports(
        self, record: JobRecord, staged: Iterable[Mapping[str, Any]]
    ) -> list[ArtifactManifest]:
        artifacts: list[ArtifactManifest] = []
        for item in staged:
            safety = ArtifactSafety(
                downloadable=bool(item["downloadable"]),
                release_safe=False,
                contains_private_source_information=False,
            )
            relative = f"jobs/{record.job_id}/attempt-{record.attempt}/exports/{item['filename']}"
            artifacts.append(
                publish_export_artifact(
                    self.repository,
                    item["exported"],
                    kind=str(item["kind"]),
                    relative_path=relative,
                    job_id=record.job_id,
                    attempt=record.attempt,
                    safety=safety,
                )
            )
        return artifacts


def _coerce_argn_batch(table: pa.Table, compiled: CompiledRules) -> pa.Table:
    """Normalize engine decoder scalars before normative rule dtype validation."""

    output = table
    for column in compiled.columns:
        if column.kind is not ColumnKind.BOOLEAN or column.name not in output.column_names:
            continue
        values: list[bool | None] = []
        for value in output.column(column.name).combine_chunks().to_pylist():
            if value is None or isinstance(value, bool):
                values.append(value)
            elif isinstance(value, str) and value.casefold() in {"true", "false"}:
                values.append(value.casefold() == "true")
            else:
                values.append(None)
        index = output.column_names.index(column.name)
        output = output.set_column(index, column.name, pa.array(values, type=pa.bool_()))
    return output


def _directory_digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    for file in sorted(item for item in path.rglob("*") if item.is_file()):
        relative = file.relative_to(path).as_posix().encode()
        content = file.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
        size += len(content)
    return digest.hexdigest(), size


def _checkpoint_snapshots(workspace: WorkspaceLayout, checkpoint: Path) -> dict[str, SnapshotFile]:
    snapshots: dict[str, SnapshotFile] = {}
    for index, path in enumerate(sorted(item for item in checkpoint.rglob("*") if item.is_file())):
        digest, size = sha256_file(path)
        snapshots[f"checkpoint_{index:06d}"] = SnapshotFile(
            path=workspace.as_relative(path), sha256=digest, size_bytes=size
        )
    if not snapshots:
        raise DomainError(ErrorCode.WORKER_FAILED, "validated checkpoint contains no files")
    return snapshots


def _atomic_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(f".{path.name}.{uuid4().hex}.part")
    try:
        with part.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(part, path)
    finally:
        part.unlink(missing_ok=True)


__all__ = [
    "DeterministicLightweightAdapter",
    "UtilityJobRuntime",
    "UtilityWorkerAdapter",
]
