from __future__ import annotations

import hashlib
from pathlib import PurePosixPath
from uuid import UUID, uuid4

from sts.domain import ArtifactManifest
from sts.storage import CatalogRepository

from .builders import BuiltReport
from .plain import PlainLanguageReport

_REPORT_FILENAMES = {
    "utility_primary": ("primary-report.json", "primary-report.html"),
    "dp_curator": ("dp-curator-report.json", "dp-curator-report.html"),
    "dp_release": ("dp-release-report.json", "dp-release-report.html"),
    "curator_internal": ("internal-diagnostic-report.json", "internal-diagnostic-report.html"),
}


def _report_directory(
    job_id: UUID | str, attempt: int, relative_directory: str | None
) -> PurePosixPath:
    identifier = UUID(str(job_id))
    directory = PurePosixPath(relative_directory or f"jobs/{identifier}/attempt-{attempt}/reports")
    if directory.is_absolute() or any(part in {"", ".", ".."} for part in directory.parts):
        raise ValueError("relative_directory must be a normalized workspace-relative path")
    return directory


def publish_plain_report_artifact(
    repository: CatalogRepository,
    plain: PlainLanguageReport,
    *,
    job_id: UUID | str,
    attempt: int,
    relative_directory: str | None = None,
) -> ArtifactManifest:
    """Publish the lay-reader HWPX companion of an already-classified report.

    Safety flags are inherited verbatim from the source report, so this function
    can never widen a release boundary on its own.
    """

    identifier = UUID(str(job_id))
    directory = _report_directory(identifier, attempt, relative_directory)
    payload = plain.hwpx_bytes()
    manifest = ArtifactManifest(
        artifact_id=uuid4(),
        kind=plain.artifact_kind,
        relative_path=(directory / plain.filename).as_posix(),
        sha256=hashlib.sha256(payload).hexdigest(),
        size_bytes=len(payload),
        downloadable=plain.safety.downloadable,
        release_safe=plain.safety.release_safe,
        contains_private_source_information=plain.safety.contains_private_source_information,
        job_id=identifier,
        attempt=attempt,
        metadata={"report_kind": plain.report_kind, "format": "hwpx", "audience": "plain"},
    )
    return repository.publish_artifact_bytes(manifest, payload)


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
    directory = _report_directory(identifier, attempt, relative_directory)
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
