from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import Field, field_validator

from sts.domain import CanonicalModel, DomainError, ErrorCode, canonical_json_text

from .rng import PrivateFitRngPolicy


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _digest(value: str) -> str:
    lowered = value.lower()
    if len(lowered) != 64 or any(character not in "0123456789abcdef" for character in lowered):
        raise ValueError("expected a SHA-256 hexadecimal digest")
    return lowered


def _contains_forbidden_rng_material(value: object, path: str = "internal_private") -> str | None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in {
                "seed",
                "fit_seed",
                "private_seed",
                "rng_seed",
                "rng_state",
                "random_state",
                "entropy",
                "entropy_bytes",
            }:
                return f"{path}.{key}"
            found = _contains_forbidden_rng_material(item, f"{path}.{key}")
            if found:
                return found
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            found = _contains_forbidden_rng_material(item, f"{path}[{index}]")
            if found:
                return found
    return None


class LedgerRunState(StrEnum):
    PENDING_RESERVED = "pending_reserved"
    ABORTED_BEFORE_PRIVATE_ACCESS = "aborted_before_private_access"
    SPENT_NOT_RELEASED = "spent_not_released"
    RELEASED = "released"


class PrivacyScope(CanonicalModel):
    privacy_scope_id: UUID
    created_at: str


class LedgerRun(CanonicalModel):
    run_id: UUID
    privacy_scope_id: UUID
    job_id: UUID
    model_id: UUID | None
    state: LedgerRunState
    epsilon_model: Decimal = Field(gt=0)
    delta: Decimal = Field(gt=0)
    release_count: int = Field(ge=0)
    first_private_access_at: str | None
    created_at: str
    updated_at: str


class PrivacyComposition(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    privacy_scope_id: UUID
    accountant: Literal["basic_sequential"] = "basic_sequential"
    epsilon_preprocess: Literal[0] = 0
    epsilon_total: Decimal = Field(ge=0)
    delta_total: Decimal = Field(ge=0)
    spent_runs: int = Field(ge=0)


class LedgerReleaseProjection(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    privacy_scope_id: UUID
    run_id: UUID
    model_id: UUID
    adjacency: Literal["add_remove_one_row"]
    privacy_unit: Literal["row"]
    epsilon_model: Decimal
    delta: Decimal
    mechanism: Literal["mst"]
    accountant: Literal["basic_sequential"]
    conversion: str
    wheel_sha256: str
    public_metadata_hashes: tuple[str, ...]
    rng_policy: PrivateFitRngPolicy
    public_target_count_provenance: str
    release_count: int = Field(ge=1)
    public_rule_postprocessing: tuple[dict[str, Any], ...]
    limitations: tuple[str, ...]

    _wheel_sha = field_validator("wheel_sha256")(_digest)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS privacy_scopes (
    id TEXT PRIMARY KEY,
    dataset_manifest_sha256 TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS privacy_ledger_runs (
    id TEXT PRIMARY KEY,
    privacy_scope_id TEXT NOT NULL REFERENCES privacy_scopes(id),
    job_id TEXT NOT NULL,
    model_id TEXT,
    state TEXT NOT NULL CHECK (state IN (
        'pending_reserved', 'aborted_before_private_access', 'spent_not_released', 'released'
    )),
    epsilon_model TEXT NOT NULL,
    delta TEXT NOT NULL,
    adjacency TEXT NOT NULL CHECK (adjacency = 'add_remove_one_row'),
    privacy_unit TEXT NOT NULL CHECK (privacy_unit = 'row'),
    mechanism TEXT NOT NULL CHECK (mechanism = 'mst'),
    package_version TEXT NOT NULL,
    wheel_sha256 TEXT NOT NULL,
    accountant TEXT NOT NULL CHECK (accountant = 'basic_sequential'),
    conversion TEXT NOT NULL,
    public_metadata_hashes_json TEXT NOT NULL,
    rng_policy_json TEXT NOT NULL,
    public_target_count INTEGER NOT NULL CHECK (public_target_count > 0),
    public_target_count_provenance TEXT NOT NULL,
    public_rule_postprocessing_json TEXT NOT NULL,
    limitations_json TEXT NOT NULL,
    internal_private_json TEXT NOT NULL,
    release_count INTEGER NOT NULL DEFAULT 0 CHECK (release_count >= 0),
    first_private_access_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS privacy_ledger_runs_scope
    ON privacy_ledger_runs(privacy_scope_id);
CREATE TABLE IF NOT EXISTS privacy_ledger_releases (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES privacy_ledger_runs(id),
    model_id TEXT NOT NULL,
    released_at TEXT NOT NULL
);
"""


class PrivacyLedger:
    """Curator-only SQLite ledger with a whitelist-based release projection."""

    def __init__(self, database_path: str | Path) -> None:
        if str(database_path) == ":memory:":
            raise ValueError("the privacy ledger requires a filesystem SQLite database")
        path = Path(database_path).expanduser().resolve(strict=False)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.database_path = path
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
        mode = self._connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        if str(mode).lower() != "wal":
            self._connection.close()
            raise RuntimeError("SQLite refused WAL journal mode for the privacy ledger")
        self._connection.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> PrivacyLedger:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @contextmanager
    def _transaction(self):
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")

    def create_privacy_scope(
        self,
        dataset_manifest_sha256: str,
        *,
        privacy_scope_id: UUID | str | None = None,
    ) -> PrivacyScope:
        digest = _digest(dataset_manifest_sha256)
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT id, created_at FROM privacy_scopes WHERE dataset_manifest_sha256 = ?",
                (digest,),
            ).fetchone()
            if existing is not None:
                return PrivacyScope(
                    privacy_scope_id=UUID(existing["id"]),
                    created_at=existing["created_at"],
                )
            identifier = UUID(str(privacy_scope_id)) if privacy_scope_id is not None else uuid4()
            created_at = _now()
            connection.execute(
                """
                INSERT INTO privacy_scopes(id, dataset_manifest_sha256, created_at)
                VALUES (?, ?, ?)
                """,
                (str(identifier), digest, created_at),
            )
        return PrivacyScope(privacy_scope_id=identifier, created_at=created_at)

    def reserve_run(
        self,
        privacy_scope_id: UUID | str,
        job_id: UUID | str,
        *,
        epsilon_model: Decimal | str,
        delta: Decimal | str,
        package_version: str,
        wheel_sha256: str,
        public_metadata_hashes: Sequence[str],
        rng_policy: PrivateFitRngPolicy,
        public_target_count: int,
        public_target_count_provenance: str,
        conversion: str,
        public_rule_postprocessing: Sequence[Mapping[str, Any]] = (),
        limitations: Sequence[str] = (),
        internal_private: Mapping[str, Any] | None = None,
        run_id: UUID | str | None = None,
    ) -> LedgerRun:
        epsilon = Decimal(epsilon_model)
        delta_value = Decimal(delta)
        if not epsilon.is_finite() or epsilon <= 0:
            raise ValueError("epsilon_model must be finite and positive")
        if not delta_value.is_finite() or not Decimal(0) < delta_value < Decimal(1):
            raise ValueError("delta must be finite and between zero and one")
        if not package_version.strip() or not conversion.strip():
            raise ValueError("package_version and conversion must not be empty")
        hashes = tuple(_digest(value) for value in public_metadata_hashes)
        if not hashes:
            raise ValueError("at least one public metadata hash is required")
        if isinstance(public_target_count, bool) or public_target_count <= 0:
            raise ValueError("public_target_count must be positive")
        if not public_target_count_provenance.strip():
            raise ValueError("public_target_count_provenance must not be empty")
        private_record = dict(internal_private or {})
        forbidden_path = _contains_forbidden_rng_material(private_record)
        if forbidden_path:
            raise ValueError(
                f"private fit RNG seed/state must never enter the ledger: {forbidden_path}"
            )
        resolved_limitations = list(limitations)
        if delta_value > Decimal(1) / Decimal(public_target_count):
            advisory = "delta_exceeds_inverse_public_target_count_advisory"
            if advisory not in resolved_limitations:
                resolved_limitations.append(advisory)
        identifier = UUID(str(run_id)) if run_id is not None else uuid4()
        timestamp = _now()
        with self._transaction() as connection:
            scope = connection.execute(
                "SELECT 1 FROM privacy_scopes WHERE id = ?", (str(privacy_scope_id),)
            ).fetchone()
            if scope is None:
                raise DomainError(ErrorCode.INVALID_STATE, "privacy scope does not exist")
            connection.execute(
                """
                INSERT INTO privacy_ledger_runs(
                    id, privacy_scope_id, job_id, model_id, state, epsilon_model, delta,
                    adjacency, privacy_unit, mechanism, package_version, wheel_sha256,
                    accountant, conversion, public_metadata_hashes_json, rng_policy_json,
                    public_target_count, public_target_count_provenance,
                    public_rule_postprocessing_json, limitations_json, internal_private_json,
                    release_count, first_private_access_at, created_at, updated_at
                ) VALUES (?, ?, ?, NULL, 'pending_reserved', ?, ?,
                    'add_remove_one_row', 'row', 'mst', ?, ?, 'basic_sequential', ?, ?, ?,
                    ?, ?, ?, ?, ?, 0, NULL, ?, ?)
                """,
                (
                    str(identifier),
                    str(privacy_scope_id),
                    str(UUID(str(job_id))),
                    str(epsilon),
                    str(delta_value),
                    package_version,
                    _digest(wheel_sha256),
                    conversion,
                    canonical_json_text(hashes),
                    canonical_json_text(rng_policy),
                    public_target_count,
                    public_target_count_provenance,
                    canonical_json_text(tuple(dict(item) for item in public_rule_postprocessing)),
                    canonical_json_text(tuple(resolved_limitations)),
                    canonical_json_text(private_record),
                    timestamp,
                    timestamp,
                ),
            )
        return self.get_run(identifier)

    def mark_private_access(self, run_id: UUID | str) -> LedgerRun:
        """Commit spending immediately before the first private source read."""

        with self._transaction() as connection:
            row = self._get_row(connection, run_id)
            state = LedgerRunState(row["state"])
            if state is LedgerRunState.ABORTED_BEFORE_PRIVATE_ACCESS:
                raise DomainError(
                    ErrorCode.INVALID_STATE, "aborted ledger run cannot access private rows"
                )
            if state is LedgerRunState.PENDING_RESERVED:
                timestamp = _now()
                connection.execute(
                    """
                    UPDATE privacy_ledger_runs
                    SET state = 'spent_not_released', first_private_access_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (timestamp, timestamp, str(run_id)),
                )
        return self.get_run(run_id)

    def record_abort(self, run_id: UUID | str, *, reason: str) -> LedgerRun:
        if not reason.strip():
            raise ValueError("abort reason must not be empty")
        with self._transaction() as connection:
            row = self._get_row(connection, run_id)
            state = LedgerRunState(row["state"])
            if state is LedgerRunState.RELEASED:
                raise DomainError(
                    ErrorCode.INVALID_STATE, "a released ledger run cannot be aborted"
                )
            private_record = json.loads(row["internal_private_json"])
            private_record.setdefault("abort_events", []).append({"reason": reason, "at": _now()})
            target = (
                LedgerRunState.ABORTED_BEFORE_PRIVATE_ACCESS
                if state is LedgerRunState.PENDING_RESERVED
                else state
            )
            connection.execute(
                """
                UPDATE privacy_ledger_runs
                SET state = ?, internal_private_json = ?, updated_at = ? WHERE id = ?
                """,
                (target.value, canonical_json_text(private_record), _now(), str(run_id)),
            )
        return self.get_run(run_id)

    def bind_model(self, run_id: UUID | str, model_id: UUID | str) -> LedgerRun:
        resolved_model_id = UUID(str(model_id))
        with self._transaction() as connection:
            row = self._get_row(connection, run_id)
            state = LedgerRunState(row["state"])
            if state not in {LedgerRunState.SPENT_NOT_RELEASED, LedgerRunState.RELEASED}:
                raise DomainError(
                    ErrorCode.INVALID_STATE, "model can be bound only after private access"
                )
            if row["model_id"] is not None and row["model_id"] != str(resolved_model_id):
                raise DomainError(
                    ErrorCode.INVALID_STATE, "ledger run is already bound to another model"
                )
            connection.execute(
                "UPDATE privacy_ledger_runs SET model_id = ?, updated_at = ? WHERE id = ?",
                (str(resolved_model_id), _now(), str(run_id)),
            )
        return self.get_run(run_id)

    def record_release(
        self,
        run_id: UUID | str,
        model_id: UUID | str,
        *,
        release_id: UUID | str | None = None,
    ) -> LedgerRun:
        resolved_model = UUID(str(model_id))
        resolved_release = UUID(str(release_id)) if release_id is not None else uuid4()
        with self._transaction() as connection:
            row = self._get_row(connection, run_id)
            state = LedgerRunState(row["state"])
            if state not in {LedgerRunState.SPENT_NOT_RELEASED, LedgerRunState.RELEASED}:
                raise DomainError(
                    ErrorCode.INVALID_STATE, "release requires a spent sanitized model"
                )
            if row["model_id"] is not None and row["model_id"] != str(resolved_model):
                raise DomainError(
                    ErrorCode.INVALID_STATE, "release model does not match the ledger run"
                )
            existing_release = connection.execute(
                "SELECT run_id, model_id FROM privacy_ledger_releases WHERE id = ?",
                (str(resolved_release),),
            ).fetchone()
            if existing_release is not None:
                if existing_release["run_id"] != str(run_id) or existing_release["model_id"] != str(
                    resolved_model
                ):
                    raise DomainError(
                        ErrorCode.INVALID_STATE, "release ID is bound to another run or model"
                    )
                return self._run_from_row(row)
            timestamp = _now()
            connection.execute(
                """
                INSERT INTO privacy_ledger_releases(id, run_id, model_id, released_at)
                VALUES (?, ?, ?, ?)
                """,
                (str(resolved_release), str(run_id), str(resolved_model), timestamp),
            )
            connection.execute(
                """
                UPDATE privacy_ledger_runs
                SET model_id = ?, state = 'released', release_count = release_count + 1,
                    updated_at = ? WHERE id = ?
                """,
                (str(resolved_model), timestamp, str(run_id)),
            )
        return self.get_run(run_id)

    def get_run(self, run_id: UUID | str) -> LedgerRun:
        with self._lock:
            row = self._get_row(self._connection, run_id)
            return self._run_from_row(row)

    def composition(self, privacy_scope_id: UUID | str) -> PrivacyComposition:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT epsilon_model, delta FROM privacy_ledger_runs
                WHERE privacy_scope_id = ? AND state IN ('spent_not_released', 'released')
                """,
                (str(privacy_scope_id),),
            ).fetchall()
        return PrivacyComposition(
            privacy_scope_id=UUID(str(privacy_scope_id)),
            epsilon_total=sum((Decimal(row["epsilon_model"]) for row in rows), Decimal(0)),
            delta_total=sum((Decimal(row["delta"]) for row in rows), Decimal(0)),
            spent_runs=len(rows),
        )

    def release_projection(self, run_id: UUID | str) -> LedgerReleaseProjection:
        """Return only the release-safe whitelist; private columns are never copied."""

        with self._lock:
            row = self._get_row(self._connection, run_id)
            if LedgerRunState(row["state"]) is not LedgerRunState.RELEASED:
                raise DomainError(
                    ErrorCode.INVALID_STATE, "release projection requires a released run"
                )
            return LedgerReleaseProjection(
                privacy_scope_id=UUID(row["privacy_scope_id"]),
                run_id=UUID(row["id"]),
                model_id=UUID(row["model_id"]),
                adjacency=row["adjacency"],
                privacy_unit=row["privacy_unit"],
                epsilon_model=Decimal(row["epsilon_model"]),
                delta=Decimal(row["delta"]),
                mechanism=row["mechanism"],
                accountant=row["accountant"],
                conversion=row["conversion"],
                wheel_sha256=row["wheel_sha256"],
                public_metadata_hashes=tuple(json.loads(row["public_metadata_hashes_json"])),
                rng_policy=PrivateFitRngPolicy.model_validate(json.loads(row["rng_policy_json"])),
                public_target_count_provenance=row["public_target_count_provenance"],
                release_count=row["release_count"],
                public_rule_postprocessing=tuple(
                    json.loads(row["public_rule_postprocessing_json"])
                ),
                limitations=tuple(json.loads(row["limitations_json"])),
            )

    def internal_run_details(self, run_id: UUID | str) -> dict[str, Any]:
        """Curator-only diagnostics, deliberately separate from release_projection."""

        with self._lock:
            row = self._get_row(self._connection, run_id)
            scope = self._connection.execute(
                "SELECT dataset_manifest_sha256 FROM privacy_scopes WHERE id = ?",
                (row["privacy_scope_id"],),
            ).fetchone()
            return {
                "dataset_manifest_sha256": scope["dataset_manifest_sha256"],
                "internal_private": json.loads(row["internal_private_json"]),
                "job_id": row["job_id"],
                "package_version": row["package_version"],
                "public_target_count": row["public_target_count"],
            }

    @staticmethod
    def _get_row(connection: sqlite3.Connection, run_id: UUID | str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM privacy_ledger_runs WHERE id = ?", (str(run_id),)
        ).fetchone()
        if row is None:
            raise DomainError(ErrorCode.INVALID_STATE, f"privacy ledger run not found: {run_id}")
        return row

    @staticmethod
    def _run_from_row(row: sqlite3.Row) -> LedgerRun:
        return LedgerRun(
            run_id=UUID(row["id"]),
            privacy_scope_id=UUID(row["privacy_scope_id"]),
            job_id=UUID(row["job_id"]),
            model_id=UUID(row["model_id"]) if row["model_id"] else None,
            state=LedgerRunState(row["state"]),
            epsilon_model=Decimal(row["epsilon_model"]),
            delta=Decimal(row["delta"]),
            release_count=row["release_count"],
            first_private_access_at=row["first_private_access_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


__all__ = [
    "LedgerReleaseProjection",
    "LedgerRun",
    "LedgerRunState",
    "PrivacyComposition",
    "PrivacyLedger",
    "PrivacyScope",
]
