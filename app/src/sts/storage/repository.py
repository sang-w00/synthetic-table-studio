from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import Field

from sts.domain import (
    DATASET_RETRY_STATES,
    JOB_TERMINAL_STATES,
    ArtifactManifest,
    CanonicalModel,
    DatasetManifest,
    DatasetState,
    DomainError,
    ErrorCode,
    JobState,
    SynthesisRequest,
    canonical_json_text,
    require_versioned_json,
    validate_dataset_transition,
    validate_job_transition,
)

from .atomic import AtomicPublisher, verify_regular_file
from .layout import WorkspaceLayout


class ArtifactScope(StrEnum):
    DOWNLOADABLE = "downloadable"
    DP_RELEASE = "dp_release"
    INTERNAL = "internal"


class OwnerType(StrEnum):
    DATASET = "dataset"
    JOB = "job"


class LedgerRunState(StrEnum):
    PENDING_RESERVED = "pending_reserved"
    ABORTED_BEFORE_PRIVATE_ACCESS = "aborted_before_private_access"
    SPENT_NOT_RELEASED = "spent_not_released"
    RELEASED = "released"


LEDGER_TRANSITIONS: dict[LedgerRunState, frozenset[LedgerRunState]] = {
    LedgerRunState.PENDING_RESERVED: frozenset(
        {
            LedgerRunState.ABORTED_BEFORE_PRIVATE_ACCESS,
            LedgerRunState.SPENT_NOT_RELEASED,
        }
    ),
    LedgerRunState.SPENT_NOT_RELEASED: frozenset({LedgerRunState.RELEASED}),
    LedgerRunState.ABORTED_BEFORE_PRIVATE_ACCESS: frozenset(),
    LedgerRunState.RELEASED: frozenset(),
}

RESUMABLE_BOUNDARIES = frozenset(
    {
        "normalized_dataset",
        "validated_fit_checkpoint",
        "published_generation_shard",
        "completed_evaluation_json",
        "completed_export",
    }
)


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _validate_digest(value: str) -> str:
    lowered = value.lower()
    if len(lowered) != 64 or any(character not in "0123456789abcdef" for character in lowered):
        raise ValueError("expected a SHA-256 hexadecimal digest")
    return lowered


class DatasetRecord(CanonicalModel):
    dataset_id: UUID
    state: DatasetState
    attempt: int = Field(ge=1)
    attempt_id: UUID
    manifest_sha256: str
    failed_from_state: DatasetState | None = None
    created_at: str
    updated_at: str


class JobRecord(CanonicalModel):
    job_id: UUID
    dataset_id: UUID
    state: JobState
    attempt: int = Field(ge=1)
    attempt_id: UUID
    request_sha256: str
    idempotency_key: str | None = None
    retry_of: UUID | None = None
    resume_boundary: str | None = None
    created_at: str
    updated_at: str


class AttemptRecord(CanonicalModel):
    attempt_id: UUID
    owner_type: OwnerType
    owner_id: UUID
    attempt: int = Field(ge=1)
    operation: str
    state: str
    started_at: str
    completed_at: str | None = None
    error_code: ErrorCode | None = None


class EventRecord(CanonicalModel):
    event_id: int
    owner_type: OwnerType
    owner_id: UUID
    attempt: int
    sequence: int
    timestamp: str
    payload: dict[str, Any]
    terminal: bool


class ResourceLeaseRecord(CanonicalModel):
    lease_id: UUID
    job_id: UUID
    kind: str
    limit_bytes: int
    state: Literal["active", "released"]
    details: dict[str, Any]
    acquired_at: str
    released_at: str | None = None


class PrivacyScopeRecord(CanonicalModel):
    privacy_scope_id: UUID
    dataset_manifest_sha256: str
    created_at: str


class LedgerRunRecord(CanonicalModel):
    run_id: UUID
    privacy_scope_id: UUID
    job_id: UUID
    model_id: UUID | None = None
    state: LedgerRunState
    epsilon_model: Decimal
    delta: Decimal
    record: dict[str, Any]
    created_at: str
    updated_at: str


_SCHEMA = """
CREATE TABLE IF NOT EXISTS datasets (
    id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    current_attempt_id TEXT NOT NULL UNIQUE,
    manifest_json TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    failed_from_state TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES datasets(id),
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    current_attempt_id TEXT NOT NULL UNIQUE,
    request_json TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    idempotency_key TEXT UNIQUE,
    retry_of TEXT REFERENCES jobs(id),
    resume_boundary TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS attempts (
    id TEXT PRIMARY KEY,
    owner_type TEXT NOT NULL CHECK (owner_type IN ('dataset', 'job')),
    owner_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    operation TEXT NOT NULL,
    state TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    error_code TEXT,
    UNIQUE(owner_type, owner_id, attempt)
);

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    dataset_id TEXT REFERENCES datasets(id),
    job_id TEXT REFERENCES jobs(id),
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    kind TEXT NOT NULL,
    relative_path TEXT NOT NULL UNIQUE,
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    downloadable INTEGER NOT NULL CHECK (downloadable IN (0, 1)),
    release_safe INTEGER NOT NULL CHECK (release_safe IN (0, 1)),
    contains_private_source_information INTEGER NOT NULL
        CHECK (contains_private_source_information IN (0, 1)),
    manifest_json TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    published_at TEXT NOT NULL,
    CHECK (NOT (release_safe = 1 AND contains_private_source_information = 1)),
    CHECK ((dataset_id IS NULL) != (job_id IS NULL))
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_type TEXT NOT NULL CHECK (owner_type IN ('dataset', 'job')),
    owner_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    timestamp TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    terminal INTEGER NOT NULL CHECK (terminal IN (0, 1)),
    UNIQUE(owner_type, owner_id, attempt, sequence)
);
CREATE UNIQUE INDEX IF NOT EXISTS events_one_terminal
    ON events(owner_type, owner_id, attempt) WHERE terminal = 1;

CREATE TABLE IF NOT EXISTS resource_leases (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id),
    kind TEXT NOT NULL,
    limit_bytes INTEGER NOT NULL CHECK (limit_bytes > 0),
    state TEXT NOT NULL CHECK (state IN ('active', 'released')),
    details_json TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    released_at TEXT
);

CREATE TABLE IF NOT EXISTS privacy_scopes (
    id TEXT PRIMARY KEY,
    dataset_manifest_sha256 TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger_runs (
    id TEXT PRIMARY KEY,
    privacy_scope_id TEXT NOT NULL REFERENCES privacy_scopes(id),
    job_id TEXT NOT NULL REFERENCES jobs(id),
    model_id TEXT,
    state TEXT NOT NULL CHECK (
        state IN (
            'pending_reserved', 'aborted_before_private_access',
            'spent_not_released', 'released'
        )
    ),
    epsilon_model TEXT NOT NULL,
    delta TEXT NOT NULL,
    record_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


class CatalogRepository:
    """SQLite WAL catalog for immutable workspace state and state-machine transitions."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        workspace: WorkspaceLayout | None = None,
    ) -> None:
        if str(database_path) == ":memory:":
            raise ValueError("the WAL catalog requires a filesystem database")
        path = Path(database_path).expanduser().resolve(strict=False)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.database_path = path
        self.workspace = workspace
        if workspace is not None:
            workspace.initialize()
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            path,
            isolation_level=None,
            check_same_thread=False,
            timeout=30,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA synchronous = FULL")
        self._connection.execute("PRAGMA busy_timeout = 30000")
        journal_mode = self._connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        if str(journal_mode).lower() != "wal":
            self._connection.close()
            raise RuntimeError("SQLite refused WAL journal mode")
        self._connection.executescript(_SCHEMA)

    @classmethod
    def open_workspace(cls, workspace: WorkspaceLayout) -> CatalogRepository:
        workspace.initialize()
        return cls(workspace.catalog_path, workspace=workspace)

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> CatalogRepository:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def journal_mode(self) -> str:
        return str(self._connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")

    def table_names(self) -> frozenset[str]:
        rows = self._connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        )
        return frozenset(row[0] for row in rows)

    def create_dataset(
        self,
        manifest: DatasetManifest,
        *,
        state: DatasetState = DatasetState.UPLOADING,
    ) -> DatasetRecord:
        dataset_id = str(manifest.dataset_id)
        manifest_json = canonical_json_text(manifest)
        attempt_id = str(uuid4())
        timestamp = _now()
        with self._transaction() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO datasets(
                        id, state, attempt, current_attempt_id, manifest_json,
                        manifest_sha256, failed_from_state, created_at, updated_at
                    ) VALUES (?, ?, 1, ?, ?, ?, NULL, ?, ?)
                    """,
                    (
                        dataset_id,
                        state.value,
                        attempt_id,
                        manifest_json,
                        manifest.canonical_sha256,
                        timestamp,
                        timestamp,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO attempts(
                        id, owner_type, owner_id, attempt, operation, state, started_at
                    ) VALUES (?, 'dataset', ?, 1, 'initial', ?, ?)
                    """,
                    (attempt_id, dataset_id, state.value, timestamp),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError(
                    ErrorCode.INVALID_STATE, f"dataset already exists: {dataset_id}"
                ) from exc
        return self.get_dataset(dataset_id)

    def get_dataset(self, dataset_id: UUID | str) -> DatasetRecord:
        row = self._connection.execute(
            "SELECT * FROM datasets WHERE id = ?", (str(dataset_id),)
        ).fetchone()
        if row is None:
            raise DomainError(ErrorCode.DATASET_NOT_FOUND, f"dataset not found: {dataset_id}")
        return self._dataset_record(row)

    def list_datasets(self, *, limit: int = 20) -> tuple[DatasetRecord, ...]:
        if limit < 1 or limit > 100:
            raise ValueError("dataset list limit must be in 1..100")
        rows = self._connection.execute(
            "SELECT * FROM datasets ORDER BY updated_at DESC, id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return tuple(self._dataset_record(row) for row in rows)

    def get_dataset_manifest(self, dataset_id: UUID | str) -> DatasetManifest:
        row = self._connection.execute(
            "SELECT manifest_json FROM datasets WHERE id = ?", (str(dataset_id),)
        ).fetchone()
        if row is None:
            raise DomainError(ErrorCode.DATASET_NOT_FOUND, f"dataset not found: {dataset_id}")
        return DatasetManifest.model_validate_json(row["manifest_json"])

    def update_dataset_manifest(
        self,
        dataset_id: UUID | str,
        manifest: DatasetManifest,
        *,
        expected_state: DatasetState | str | None = None,
    ) -> DatasetRecord:
        """Atomically advance the dataset's current immutable manifest identity."""

        identifier = str(dataset_id)
        if str(manifest.dataset_id) != identifier:
            raise ValueError("updated manifest dataset_id must match the catalog dataset")
        manifest_json = canonical_json_text(manifest)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT state FROM datasets WHERE id = ?", (identifier,)
            ).fetchone()
            if row is None:
                raise DomainError(ErrorCode.DATASET_NOT_FOUND, f"dataset not found: {identifier}")
            current = DatasetState(row["state"])
            if expected_state is not None and current is not DatasetState(expected_state):
                raise DomainError(
                    ErrorCode.INVALID_STATE,
                    f"dataset expected {DatasetState(expected_state).value}, found {current.value}",
                )
            connection.execute(
                """
                UPDATE datasets
                SET manifest_json = ?, manifest_sha256 = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    manifest_json,
                    manifest.canonical_sha256,
                    _now(),
                    identifier,
                ),
            )
        return self.get_dataset(identifier)

    def transition_dataset(
        self,
        dataset_id: UUID | str,
        target: DatasetState | str,
        *,
        expected_state: DatasetState | str | None = None,
        error_code: ErrorCode | None = None,
    ) -> DatasetRecord:
        identifier = str(dataset_id)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM datasets WHERE id = ?", (identifier,)
            ).fetchone()
            if row is None:
                raise DomainError(ErrorCode.DATASET_NOT_FOUND, f"dataset not found: {identifier}")
            current = DatasetState(row["state"])
            if expected_state is not None and current is not DatasetState(expected_state):
                raise DomainError(
                    ErrorCode.INVALID_STATE,
                    f"dataset expected {DatasetState(expected_state).value}, found {current.value}",
                )
            target_state = validate_dataset_transition(current, target)
            failed_from = current.value if target_state is DatasetState.FAILED else None
            timestamp = _now()
            connection.execute(
                """
                UPDATE datasets
                SET state = ?, failed_from_state = ?, updated_at = ?
                WHERE id = ?
                """,
                (target_state.value, failed_from, timestamp, identifier),
            )
            completed = (
                timestamp
                if target_state in {DatasetState.NORMALIZED, DatasetState.FAILED}
                else None
            )
            connection.execute(
                """
                UPDATE attempts
                SET state = ?, completed_at = COALESCE(?, completed_at), error_code = ?
                WHERE id = ?
                """,
                (
                    target_state.value,
                    completed,
                    error_code.value if error_code else None,
                    row["current_attempt_id"],
                ),
            )
        return self.get_dataset(identifier)

    def retry_dataset(self, dataset_id: UUID | str) -> DatasetRecord:
        identifier = str(dataset_id)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM datasets WHERE id = ?", (identifier,)
            ).fetchone()
            if row is None:
                raise DomainError(ErrorCode.DATASET_NOT_FOUND, f"dataset not found: {identifier}")
            failed_from_raw = row["failed_from_state"]
            failed_from = DatasetState(failed_from_raw) if failed_from_raw else None
            if (
                DatasetState(row["state"]) is not DatasetState.FAILED
                or failed_from not in DATASET_RETRY_STATES
            ):
                raise DomainError(
                    ErrorCode.INVALID_STATE,
                    "only failed inspect, profile, or normalize attempts can be retried",
                )
            attempt = int(row["attempt"]) + 1
            attempt_id = str(uuid4())
            timestamp = _now()
            operation = {
                DatasetState.INSPECTING: "inspect",
                DatasetState.PROFILING: "profile",
                DatasetState.NORMALIZING: "normalize",
            }[failed_from]
            connection.execute(
                """
                INSERT INTO attempts(
                    id, owner_type, owner_id, attempt, operation, state, started_at
                ) VALUES (?, 'dataset', ?, ?, ?, ?, ?)
                """,
                (attempt_id, identifier, attempt, operation, failed_from.value, timestamp),
            )
            connection.execute(
                """
                UPDATE datasets
                SET state = ?, attempt = ?, current_attempt_id = ?,
                    failed_from_state = NULL, updated_at = ?
                WHERE id = ?
                """,
                (failed_from.value, attempt, attempt_id, timestamp, identifier),
            )
        return self.get_dataset(identifier)

    def create_job(
        self,
        request: SynthesisRequest | Mapping[str, Any],
        *,
        idempotency_key: str,
        job_id: UUID | str | None = None,
    ) -> JobRecord:
        parsed = (
            request
            if isinstance(request, SynthesisRequest)
            else SynthesisRequest.model_validate(request)
        )
        key = self._validate_idempotency_key(idempotency_key)
        request_json = canonical_json_text(parsed)
        request_sha256 = parsed.canonical_sha256
        identifier = str(UUID(str(job_id))) if job_id is not None else str(uuid4())
        dataset_id = str(parsed.root.dataset_id)
        attempt_id = str(uuid4())
        timestamp = _now()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM jobs WHERE idempotency_key = ?", (key,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["request_sha256"] != request_sha256
                    or existing["request_json"] != request_json
                ):
                    raise DomainError(
                        ErrorCode.IDEMPOTENCY_CONFLICT,
                        "idempotency key was already used for a different synthesis request",
                    )
                return self._job_record(existing)
            if (
                connection.execute("SELECT 1 FROM datasets WHERE id = ?", (dataset_id,)).fetchone()
                is None
            ):
                raise DomainError(ErrorCode.DATASET_NOT_FOUND, f"dataset not found: {dataset_id}")
            connection.execute(
                """
                INSERT INTO jobs(
                    id, dataset_id, state, attempt, current_attempt_id, request_json,
                    request_sha256, idempotency_key, retry_of, resume_boundary,
                    created_at, updated_at
                ) VALUES (?, ?, 'queued', 1, ?, ?, ?, ?, NULL, NULL, ?, ?)
                """,
                (
                    identifier,
                    dataset_id,
                    attempt_id,
                    request_json,
                    request_sha256,
                    key,
                    timestamp,
                    timestamp,
                ),
            )
            connection.execute(
                """
                INSERT INTO attempts(
                    id, owner_type, owner_id, attempt, operation, state, started_at
                ) VALUES (?, 'job', ?, 1, 'synthesis', 'queued', ?)
                """,
                (attempt_id, identifier, timestamp),
            )
        return self.get_job(identifier)

    def get_job(self, job_id: UUID | str) -> JobRecord:
        row = self._connection.execute("SELECT * FROM jobs WHERE id = ?", (str(job_id),)).fetchone()
        if row is None:
            raise DomainError(ErrorCode.JOB_NOT_FOUND, f"job not found: {job_id}")
        return self._job_record(row)

    def list_jobs(
        self,
        *,
        limit: int = 20,
        dataset_id: UUID | str | None = None,
    ) -> tuple[JobRecord, ...]:
        if limit < 1 or limit > 100:
            raise ValueError("job list limit must be in 1..100")
        if dataset_id is None:
            rows = self._connection.execute(
                "SELECT * FROM jobs ORDER BY updated_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = self._connection.execute(
                """
                SELECT * FROM jobs
                WHERE dataset_id = ?
                ORDER BY updated_at DESC, id DESC LIMIT ?
                """,
                (str(dataset_id), limit),
            ).fetchall()
        return tuple(self._job_record(row) for row in rows)

    def get_job_request(self, job_id: UUID | str) -> SynthesisRequest:
        row = self._connection.execute(
            "SELECT request_json FROM jobs WHERE id = ?", (str(job_id),)
        ).fetchone()
        if row is None:
            raise DomainError(ErrorCode.JOB_NOT_FOUND, f"job not found: {job_id}")
        return SynthesisRequest.model_validate_json(row["request_json"])

    def transition_job(
        self,
        job_id: UUID | str,
        target: JobState | str,
        *,
        expected_state: JobState | str | None = None,
        error_code: ErrorCode | None = None,
    ) -> JobRecord:
        identifier = str(job_id)
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE id = ?", (identifier,)).fetchone()
            if row is None:
                raise DomainError(ErrorCode.JOB_NOT_FOUND, f"job not found: {identifier}")
            current = JobState(row["state"])
            if expected_state is not None and current is not JobState(expected_state):
                raise DomainError(
                    ErrorCode.INVALID_STATE,
                    f"job expected {JobState(expected_state).value}, found {current.value}",
                )
            target_state = validate_job_transition(current, target)
            timestamp = _now()
            connection.execute(
                "UPDATE jobs SET state = ?, updated_at = ? WHERE id = ?",
                (target_state.value, timestamp, identifier),
            )
            completed = timestamp if target_state in JOB_TERMINAL_STATES else None
            connection.execute(
                """
                UPDATE attempts
                SET state = ?, completed_at = COALESCE(?, completed_at), error_code = ?
                WHERE id = ?
                """,
                (
                    target_state.value,
                    completed,
                    error_code.value if error_code else None,
                    row["current_attempt_id"],
                ),
            )
        return self.get_job(identifier)

    def set_resume_boundary(self, job_id: UUID | str, boundary: str) -> JobRecord:
        if boundary not in RESUMABLE_BOUNDARIES:
            raise ValueError(f"unsupported resume boundary: {boundary}")
        identifier = str(job_id)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT state FROM jobs WHERE id = ?", (identifier,)
            ).fetchone()
            if row is None:
                raise DomainError(ErrorCode.JOB_NOT_FOUND, f"job not found: {identifier}")
            if JobState(row["state"]) in JOB_TERMINAL_STATES:
                raise DomainError(
                    ErrorCode.INVALID_STATE,
                    "resume boundary must be recorded before terminal state",
                )
            connection.execute(
                "UPDATE jobs SET resume_boundary = ?, updated_at = ? WHERE id = ?",
                (boundary, _now(), identifier),
            )
        return self.get_job(identifier)

    def resume_job(
        self,
        job_id: UUID | str,
        *,
        idempotency_key: str | None = None,
        new_job_id: UUID | str | None = None,
    ) -> JobRecord:
        source_id = str(job_id)
        key = self._validate_idempotency_key(idempotency_key) if idempotency_key else None
        with self._transaction() as connection:
            source = connection.execute("SELECT * FROM jobs WHERE id = ?", (source_id,)).fetchone()
            if source is None:
                raise DomainError(ErrorCode.JOB_NOT_FOUND, f"job not found: {source_id}")
            source_state = JobState(source["state"])
            if source_state not in {JobState.FAILED, JobState.CANCELLED}:
                raise DomainError(
                    ErrorCode.RESUME_UNAVAILABLE,
                    "only failed or cancelled terminal jobs can be resumed",
                )
            boundary = source["resume_boundary"]
            if boundary not in RESUMABLE_BOUNDARIES:
                raise DomainError(
                    ErrorCode.RESUME_UNAVAILABLE,
                    "job has no validated resumable boundary",
                )
            if key is not None:
                existing = connection.execute(
                    "SELECT * FROM jobs WHERE idempotency_key = ?", (key,)
                ).fetchone()
                if existing is not None:
                    if existing["retry_of"] != source_id:
                        raise DomainError(
                            ErrorCode.IDEMPOTENCY_CONFLICT,
                            "idempotency key was already used for a different resume request",
                        )
                    return self._job_record(existing)
            identifier = str(UUID(str(new_job_id))) if new_job_id is not None else str(uuid4())
            attempt = int(source["attempt"]) + 1
            attempt_id = str(uuid4())
            timestamp = _now()
            connection.execute(
                """
                INSERT INTO jobs(
                    id, dataset_id, state, attempt, current_attempt_id, request_json,
                    request_sha256, idempotency_key, retry_of, resume_boundary,
                    created_at, updated_at
                ) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    identifier,
                    source["dataset_id"],
                    attempt,
                    attempt_id,
                    source["request_json"],
                    source["request_sha256"],
                    key,
                    source_id,
                    boundary,
                    timestamp,
                    timestamp,
                ),
            )
            connection.execute(
                """
                INSERT INTO attempts(
                    id, owner_type, owner_id, attempt, operation, state, started_at
                ) VALUES (?, 'job', ?, ?, 'resume', 'queued', ?)
                """,
                (attempt_id, identifier, attempt, timestamp),
            )
        return self.get_job(identifier)

    def latest_attempt(self, owner_type: OwnerType | str, owner_id: UUID | str) -> AttemptRecord:
        owner = OwnerType(owner_type)
        row = self._connection.execute(
            """
            SELECT * FROM attempts
            WHERE owner_type = ? AND owner_id = ?
            ORDER BY attempt DESC LIMIT 1
            """,
            (owner.value, str(owner_id)),
        ).fetchone()
        if row is None:
            code = (
                ErrorCode.DATASET_NOT_FOUND
                if owner is OwnerType.DATASET
                else ErrorCode.JOB_NOT_FOUND
            )
            raise DomainError(code, f"{owner.value} not found: {owner_id}")
        return self._attempt_record(row)

    def register_artifact(self, manifest: ArtifactManifest) -> ArtifactManifest:
        if (manifest.dataset_id is None) == (manifest.job_id is None):
            raise ValueError("catalog artifacts must have exactly one dataset or job owner")
        if self.workspace is None:
            raise RuntimeError("artifact registration requires a workspace layout")
        artifact_path = self.workspace.resolve_relative(manifest.relative_path, require_exists=True)
        verify_regular_file(artifact_path, manifest.sha256, manifest.size_bytes)
        timestamp = _now()
        with self._transaction() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO artifacts(
                        id, dataset_id, job_id, attempt, kind, relative_path,
                        sha256, size_bytes, downloadable, release_safe,
                        contains_private_source_information, manifest_json,
                        manifest_sha256, published_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(manifest.artifact_id),
                        str(manifest.dataset_id) if manifest.dataset_id else None,
                        str(manifest.job_id) if manifest.job_id else None,
                        manifest.attempt,
                        manifest.kind,
                        manifest.relative_path,
                        manifest.sha256,
                        manifest.size_bytes,
                        int(manifest.downloadable),
                        int(manifest.release_safe),
                        int(manifest.contains_private_source_information),
                        canonical_json_text(manifest),
                        manifest.canonical_sha256,
                        timestamp,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError(
                    ErrorCode.INVALID_STATE,
                    "artifact identity or immutable path is already cataloged",
                ) from exc
        return manifest

    def publish_artifact_bytes(
        self, manifest: ArtifactManifest, content: bytes
    ) -> ArtifactManifest:
        if self.workspace is None:
            raise RuntimeError("artifact publication requires a workspace layout")
        publisher = AtomicPublisher(self.workspace)
        publisher.publish_bytes(
            manifest.relative_path,
            content,
            expected_sha256=manifest.sha256,
            expected_size=manifest.size_bytes,
        )
        return self.register_artifact(manifest)

    def publish_artifact_file(
        self, manifest: ArtifactManifest, source_path: str | Path
    ) -> ArtifactManifest:
        if self.workspace is None:
            raise RuntimeError("artifact publication requires a workspace layout")
        publisher = AtomicPublisher(self.workspace)
        publisher.publish_file(
            source_path,
            manifest.relative_path,
            expected_sha256=manifest.sha256,
            expected_size=manifest.size_bytes,
        )
        return self.register_artifact(manifest)

    def get_artifact(self, artifact_id: UUID | str) -> ArtifactManifest:
        row = self._connection.execute(
            "SELECT manifest_json FROM artifacts WHERE id = ?", (str(artifact_id),)
        ).fetchone()
        if row is None:
            raise DomainError(ErrorCode.ARTIFACT_NOT_FOUND, f"artifact not found: {artifact_id}")
        return ArtifactManifest.model_validate_json(row["manifest_json"])

    def list_artifacts(
        self,
        *,
        job_id: UUID | str | None = None,
        dataset_id: UUID | str | None = None,
        scope: ArtifactScope | str = ArtifactScope.DOWNLOADABLE,
    ) -> tuple[ArtifactManifest, ...]:
        if (job_id is None) == (dataset_id is None):
            raise ValueError("exactly one job_id or dataset_id is required")
        artifact_scope = ArtifactScope(scope)
        owner_column = "job_id" if job_id is not None else "dataset_id"
        owner_value = str(job_id if job_id is not None else dataset_id)
        predicate = {
            ArtifactScope.DOWNLOADABLE: "downloadable = 1",
            ArtifactScope.DP_RELEASE: (
                "release_safe = 1 AND contains_private_source_information = 0"
            ),
            ArtifactScope.INTERNAL: "1 = 1",
        }[artifact_scope]
        rows = self._connection.execute(
            f"""
            SELECT manifest_json FROM artifacts
            WHERE {owner_column} = ? AND {predicate}
            ORDER BY published_at, id
            """,
            (owner_value,),
        )
        return tuple(ArtifactManifest.model_validate_json(row["manifest_json"]) for row in rows)

    def append_event(
        self,
        owner_type: OwnerType | str,
        owner_id: UUID | str,
        payload: Mapping[str, Any],
        *,
        attempt: int | None = None,
        sequence: int | None = None,
        terminal: bool = False,
        timestamp: str | None = None,
    ) -> EventRecord:
        owner = OwnerType(owner_type)
        require_versioned_json(payload)
        payload_json = canonical_json_text(payload)
        identifier = str(owner_id)
        with self._transaction() as connection:
            owner_table = "datasets" if owner is OwnerType.DATASET else "jobs"
            owner_row = connection.execute(
                f"SELECT attempt FROM {owner_table} WHERE id = ?", (identifier,)
            ).fetchone()
            if owner_row is None:
                code = (
                    ErrorCode.DATASET_NOT_FOUND
                    if owner is OwnerType.DATASET
                    else ErrorCode.JOB_NOT_FOUND
                )
                raise DomainError(code, f"{owner.value} not found: {identifier}")
            event_attempt = attempt or int(owner_row["attempt"])
            if connection.execute(
                """
                SELECT 1 FROM events
                WHERE owner_type = ? AND owner_id = ? AND attempt = ? AND terminal = 1
                """,
                (owner.value, identifier, event_attempt),
            ).fetchone():
                raise DomainError(
                    ErrorCode.INVALID_STATE, "cannot append an event after terminal event"
                )
            next_sequence = int(
                connection.execute(
                    """
                    SELECT COALESCE(MAX(sequence), 0) + 1 FROM events
                    WHERE owner_type = ? AND owner_id = ? AND attempt = ?
                    """,
                    (owner.value, identifier, event_attempt),
                ).fetchone()[0]
            )
            if sequence is not None and sequence != next_sequence:
                raise DomainError(
                    ErrorCode.INVALID_STATE,
                    f"event sequence must be {next_sequence}, got {sequence}",
                )
            event_timestamp = timestamp or _now()
            cursor = connection.execute(
                """
                INSERT INTO events(
                    owner_type, owner_id, attempt, sequence, timestamp, payload_json, terminal
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    owner.value,
                    identifier,
                    event_attempt,
                    next_sequence,
                    event_timestamp,
                    payload_json,
                    int(terminal),
                ),
            )
            event_id = int(cursor.lastrowid)
        return EventRecord(
            event_id=event_id,
            owner_type=owner,
            owner_id=UUID(identifier),
            attempt=event_attempt,
            sequence=next_sequence,
            timestamp=event_timestamp,
            payload=json.loads(payload_json),
            terminal=terminal,
        )

    def replay_events(
        self,
        owner_type: OwnerType | str,
        owner_id: UUID | str,
        *,
        after_event_id: int = 0,
    ) -> tuple[EventRecord, ...]:
        owner = OwnerType(owner_type)
        rows = self._connection.execute(
            """
            SELECT * FROM events
            WHERE owner_type = ? AND owner_id = ? AND id > ?
            ORDER BY id
            """,
            (owner.value, str(owner_id), after_event_id),
        )
        return tuple(self._event_record(row) for row in rows)

    def acquire_resource_lease(
        self,
        job_id: UUID | str,
        kind: str,
        limit_bytes: int,
        *,
        details: Mapping[str, Any] | None = None,
        lease_id: UUID | str | None = None,
    ) -> ResourceLeaseRecord:
        if limit_bytes <= 0:
            raise ValueError("resource lease limit must be positive")
        identifier = str(UUID(str(lease_id))) if lease_id is not None else str(uuid4())
        payload = {"version": "1.0", "values": dict(details or {})}
        timestamp = _now()
        with self._transaction() as connection:
            if (
                connection.execute("SELECT 1 FROM jobs WHERE id = ?", (str(job_id),)).fetchone()
                is None
            ):
                raise DomainError(ErrorCode.JOB_NOT_FOUND, f"job not found: {job_id}")
            connection.execute(
                """
                INSERT INTO resource_leases(
                    id, job_id, kind, limit_bytes, state, details_json, acquired_at
                ) VALUES (?, ?, ?, ?, 'active', ?, ?)
                """,
                (
                    identifier,
                    str(job_id),
                    kind,
                    limit_bytes,
                    canonical_json_text(payload),
                    timestamp,
                ),
            )
        return self._resource_lease(identifier)

    def release_resource_lease(self, lease_id: UUID | str) -> ResourceLeaseRecord:
        identifier = str(lease_id)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT state FROM resource_leases WHERE id = ?", (identifier,)
            ).fetchone()
            if row is None:
                raise DomainError(
                    ErrorCode.RESOURCE_LIMIT, f"resource lease not found: {identifier}"
                )
            if row["state"] != "released":
                timestamp = _now()
                connection.execute(
                    """
                    UPDATE resource_leases
                    SET state = 'released', released_at = ? WHERE id = ?
                    """,
                    (timestamp, identifier),
                )
        return self._resource_lease(identifier)

    def create_privacy_scope(
        self,
        dataset_manifest_sha256: str,
        *,
        privacy_scope_id: UUID | str | None = None,
    ) -> PrivacyScopeRecord:
        digest = _validate_digest(dataset_manifest_sha256)
        identifier = (
            str(UUID(str(privacy_scope_id))) if privacy_scope_id is not None else str(uuid4())
        )
        timestamp = _now()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM privacy_scopes WHERE dataset_manifest_sha256 = ?", (digest,)
            ).fetchone()
            if existing is not None:
                return self._privacy_scope_record(existing)
            connection.execute(
                """
                INSERT INTO privacy_scopes(id, dataset_manifest_sha256, created_at)
                VALUES (?, ?, ?)
                """,
                (identifier, digest, timestamp),
            )
        return self.get_privacy_scope(identifier)

    def get_privacy_scope(self, privacy_scope_id: UUID | str) -> PrivacyScopeRecord:
        row = self._connection.execute(
            "SELECT * FROM privacy_scopes WHERE id = ?", (str(privacy_scope_id),)
        ).fetchone()
        if row is None:
            raise DomainError(
                ErrorCode.DP_METADATA_NOT_PUBLIC,
                f"privacy scope not found: {privacy_scope_id}",
            )
        return self._privacy_scope_record(row)

    def reserve_ledger_run(
        self,
        privacy_scope_id: UUID | str,
        job_id: UUID | str,
        *,
        epsilon_model: Decimal,
        delta: Decimal,
        record: Mapping[str, Any],
        run_id: UUID | str | None = None,
    ) -> LedgerRunRecord:
        if epsilon_model <= 0 or delta <= 0 or delta >= 1:
            raise ValueError(
                "ledger epsilon must be positive and delta must be between zero and one"
            )
        require_versioned_json(record)
        identifier = str(UUID(str(run_id))) if run_id is not None else str(uuid4())
        timestamp = _now()
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT INTO ledger_runs(
                    id, privacy_scope_id, job_id, model_id, state, epsilon_model,
                    delta, record_json, created_at, updated_at
                ) VALUES (?, ?, ?, NULL, 'pending_reserved', ?, ?, ?, ?, ?)
                """,
                (
                    identifier,
                    str(privacy_scope_id),
                    str(job_id),
                    str(epsilon_model),
                    str(delta),
                    canonical_json_text(record),
                    timestamp,
                    timestamp,
                ),
            )
        return self.get_ledger_run(identifier)

    def transition_ledger_run(
        self,
        run_id: UUID | str,
        target: LedgerRunState | str,
        *,
        model_id: UUID | str | None = None,
    ) -> LedgerRunRecord:
        identifier = str(run_id)
        target_state = LedgerRunState(target)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM ledger_runs WHERE id = ?", (identifier,)
            ).fetchone()
            if row is None:
                raise DomainError(ErrorCode.INVALID_STATE, f"ledger run not found: {identifier}")
            current = LedgerRunState(row["state"])
            if target_state not in LEDGER_TRANSITIONS[current]:
                raise DomainError(
                    ErrorCode.INVALID_STATE,
                    f"ledger run cannot transition from {current.value} to {target_state.value}",
                )
            resolved_model_id = str(model_id) if model_id is not None else row["model_id"]
            if (
                target_state in {LedgerRunState.SPENT_NOT_RELEASED, LedgerRunState.RELEASED}
                and resolved_model_id is None
            ):
                resolved_model_id = str(uuid4())
            connection.execute(
                """
                UPDATE ledger_runs SET state = ?, model_id = ?, updated_at = ? WHERE id = ?
                """,
                (target_state.value, resolved_model_id, _now(), identifier),
            )
        return self.get_ledger_run(identifier)

    def get_ledger_run(self, run_id: UUID | str) -> LedgerRunRecord:
        row = self._connection.execute(
            "SELECT * FROM ledger_runs WHERE id = ?", (str(run_id),)
        ).fetchone()
        if row is None:
            raise DomainError(ErrorCode.INVALID_STATE, f"ledger run not found: {run_id}")
        return self._ledger_run_record(row)

    @staticmethod
    def _validate_idempotency_key(value: str) -> str:
        if not value or value.isspace() or len(value.encode("utf-8")) > 255:
            raise ValueError("idempotency key must contain 1 to 255 UTF-8 bytes")
        return value

    @staticmethod
    def _dataset_record(row: sqlite3.Row) -> DatasetRecord:
        return DatasetRecord(
            dataset_id=UUID(row["id"]),
            state=DatasetState(row["state"]),
            attempt=row["attempt"],
            attempt_id=UUID(row["current_attempt_id"]),
            manifest_sha256=row["manifest_sha256"],
            failed_from_state=(
                DatasetState(row["failed_from_state"]) if row["failed_from_state"] else None
            ),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _job_record(row: sqlite3.Row) -> JobRecord:
        return JobRecord(
            job_id=UUID(row["id"]),
            dataset_id=UUID(row["dataset_id"]),
            state=JobState(row["state"]),
            attempt=row["attempt"],
            attempt_id=UUID(row["current_attempt_id"]),
            request_sha256=row["request_sha256"],
            idempotency_key=row["idempotency_key"],
            retry_of=UUID(row["retry_of"]) if row["retry_of"] else None,
            resume_boundary=row["resume_boundary"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _attempt_record(row: sqlite3.Row) -> AttemptRecord:
        return AttemptRecord(
            attempt_id=UUID(row["id"]),
            owner_type=OwnerType(row["owner_type"]),
            owner_id=UUID(row["owner_id"]),
            attempt=row["attempt"],
            operation=row["operation"],
            state=row["state"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            error_code=ErrorCode(row["error_code"]) if row["error_code"] else None,
        )

    @staticmethod
    def _event_record(row: sqlite3.Row) -> EventRecord:
        return EventRecord(
            event_id=row["id"],
            owner_type=OwnerType(row["owner_type"]),
            owner_id=UUID(row["owner_id"]),
            attempt=row["attempt"],
            sequence=row["sequence"],
            timestamp=row["timestamp"],
            payload=json.loads(row["payload_json"]),
            terminal=bool(row["terminal"]),
        )

    def _resource_lease(self, lease_id: UUID | str) -> ResourceLeaseRecord:
        row = self._connection.execute(
            "SELECT * FROM resource_leases WHERE id = ?", (str(lease_id),)
        ).fetchone()
        if row is None:
            raise DomainError(ErrorCode.RESOURCE_LIMIT, f"resource lease not found: {lease_id}")
        payload = json.loads(row["details_json"])
        return ResourceLeaseRecord(
            lease_id=UUID(row["id"]),
            job_id=UUID(row["job_id"]),
            kind=row["kind"],
            limit_bytes=row["limit_bytes"],
            state=row["state"],
            details=payload["values"],
            acquired_at=row["acquired_at"],
            released_at=row["released_at"],
        )

    @staticmethod
    def _privacy_scope_record(row: sqlite3.Row) -> PrivacyScopeRecord:
        return PrivacyScopeRecord(
            privacy_scope_id=UUID(row["id"]),
            dataset_manifest_sha256=row["dataset_manifest_sha256"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _ledger_run_record(row: sqlite3.Row) -> LedgerRunRecord:
        return LedgerRunRecord(
            run_id=UUID(row["id"]),
            privacy_scope_id=UUID(row["privacy_scope_id"]),
            job_id=UUID(row["job_id"]),
            model_id=UUID(row["model_id"]) if row["model_id"] else None,
            state=LedgerRunState(row["state"]),
            epsilon_model=Decimal(row["epsilon_model"]),
            delta=Decimal(row["delta"]),
            record=json.loads(row["record_json"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


SQLiteRepository = CatalogRepository
