from __future__ import annotations

import hashlib
from pathlib import PurePosixPath
from uuid import UUID, uuid4

from sts.domain import ArtifactManifest
from sts.storage import CatalogRepository

from .builders import BuiltReport

_REPORT_FILENAMES = {
    "utility_primary": ("primary-report.json", "primary-report.html"),
    "dp_release": ("dp-release-report.json", "dp-release-report.html"),
    "curator_internal": ("internal-diagnostic-report.json", "internal-diagnostic-report.html"),
}


def publish_report_artifacts(
    repository: CatalogRepository,
    report: BuiltReport,
    *,
    job_id: UUID | str,
    attempt: int,
    relative_directory: str | None = None,
) -> tuple[ArtifactManifest, ArtifactManifest]:
    """Atomically publish and catalog JSON and HTML representations of one report.

    Safety flags come from the explicit report constructor, not the filename or artifact kind.
    """

    identifier = UUID(str(job_id))
    directory = PurePosixPath(relative_directory or f"jobs/{identifier}/attempt-{attempt}/reports")
    if directory.is_absolute() or any(part in {"", ".", ".."} for part in directory.parts):
        raise ValueError("relative_directory must be a normalized workspace-relative path")
    json_filename, html_filename = _REPORT_FILENAMES[report.report_kind]
    payloads = (
        (report.json_artifact_kind, directory / json_filename, report.json_bytes()),
        (report.html_artifact_kind, directory / html_filename, report.html_bytes()),
    )
    published: list[ArtifactManifest] = []
    for artifact_kind, relative_path, payload in payloads:
        manifest = ArtifactManifest(
            artifact_id=uuid4(),
            kind=artifact_kind,
            relative_path=relative_path.as_posix(),
            sha256=hashlib.sha256(payload).hexdigest(),
            size_bytes=len(payload),
            downloadable=report.safety.downloadable,
            release_safe=report.safety.release_safe,
            contains_private_source_information=(report.safety.contains_private_source_information),
            job_id=identifier,
            attempt=attempt,
            metadata={"report_kind": report.report_kind, "format": relative_path.suffix[1:]},
        )
        published.append(repository.publish_artifact_bytes(manifest, payload))
    return published[0], published[1]
