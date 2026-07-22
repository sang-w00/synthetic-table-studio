from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import UUID


def _uuid_component(value: UUID | str) -> str:
    try:
        return str(UUID(str(value)))
    except ValueError as exc:
        raise ValueError("workspace identifiers must be UUIDs") from exc


def _safe_relative(value: str | PurePosixPath) -> PurePosixPath:
    raw = str(value)
    if "\\" in raw or "\x00" in raw:
        raise ValueError("workspace paths must be relative POSIX paths")
    path = PurePosixPath(raw)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("workspace path traversal is not allowed")
    if path.as_posix() != raw:
        raise ValueError("workspace paths must be normalized")
    return path


@dataclass(frozen=True, slots=True)
class WorkspaceLayout:
    root: Path

    def __init__(self, root: str | os.PathLike[str]) -> None:
        resolved = Path(root).expanduser().resolve(strict=False)
        object.__setattr__(self, "root", resolved)

    @property
    def datasets_root(self) -> Path:
        return self.root / "datasets"

    @property
    def jobs_root(self) -> Path:
        return self.root / "jobs"

    @property
    def catalog_path(self) -> Path:
        return self.root / "catalog.sqlite3"

    def initialize(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.datasets_root.mkdir(exist_ok=True)
        self.jobs_root.mkdir(exist_ok=True)

    def dataset_dir(self, dataset_id: UUID | str, *, create: bool = False) -> Path:
        path = self.datasets_root / _uuid_component(dataset_id)
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def job_dir(self, job_id: UUID | str, *, create: bool = False) -> Path:
        path = self.jobs_root / _uuid_component(job_id)
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def job_attempt_dir(self, job_id: UUID | str, attempt: int, *, create: bool = False) -> Path:
        if attempt < 1:
            raise ValueError("attempt must be at least 1")
        path = self.job_dir(job_id, create=create) / f"attempt-{attempt}"
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def resolve_relative(
        self, relative_path: str | PurePosixPath, *, require_exists: bool = False
    ) -> Path:
        relative = _safe_relative(relative_path)
        candidate = self.root.joinpath(*relative.parts).resolve(strict=require_exists)
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:
            raise ValueError("resolved workspace path escapes the workspace root") from exc
        return candidate

    def as_relative(self, path: str | os.PathLike[str]) -> str:
        resolved = Path(path).expanduser().resolve(strict=True)
        try:
            relative = resolved.relative_to(self.root)
        except ValueError as exc:
            raise ValueError("path is outside the workspace root") from exc
        return PurePosixPath(*relative.parts).as_posix()
