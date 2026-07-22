from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from sts.domain import ArtifactManifest
from sts.reports import ArtifactSafety
from sts.storage import CatalogRepository

from .models import ExportedFile


def publish_export_artifact(
    repository: CatalogRepository,
    exported: ExportedFile,
    *,
    kind: str,
    relative_path: str,
    job_id: UUID | str,
    attempt: int,
    safety: ArtifactSafety,
    metadata: Mapping[str, Any] | None = None,
) -> ArtifactManifest:
    """Publish one export with caller-supplied safety flags; kind never implies safety."""

    artifact_metadata = dict(metadata or {})
    if exported.canonical_content_sha256 is not None:
        artifact_metadata["canonical_content_sha256"] = exported.canonical_content_sha256
    if exported.row_count is not None:
        artifact_metadata["row_count"] = exported.row_count
    if exported.null_marker is not None:
        artifact_metadata["csv_null_marker"] = exported.null_marker
    manifest = ArtifactManifest(
        artifact_id=uuid4(),
        kind=kind,
        relative_path=relative_path,
        sha256=exported.sha256,
        size_bytes=exported.size_bytes,
        downloadable=safety.downloadable,
        release_safe=safety.release_safe,
        contains_private_source_information=safety.contains_private_source_information,
        job_id=UUID(str(job_id)),
        attempt=attempt,
        metadata=artifact_metadata,
    )
    return repository.publish_artifact_file(manifest, Path(exported.path))
