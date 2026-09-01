from __future__ import annotations

import hashlib
from typing import Any

from fastapi import APIRouter, status
from pydantic import RootModel

from sts.domain import ManifestFile, canonical_json_bytes
from sts.privacy import validate_public_metadata
from sts.storage import WorkspaceLayout
from sts.storage.atomic import AtomicPublisher


class PublicMetadataPayload(RootModel[dict[str, Any]]):
    pass


class PrivacyService:
    def __init__(self, workspace: WorkspaceLayout) -> None:
        self.workspace = workspace
        self.publisher = AtomicPublisher(workspace)

    def publish_public_metadata(self, payload: dict[str, Any]) -> ManifestFile:
        manifest = validate_public_metadata(payload)
        content = canonical_json_bytes(manifest.model_dump(mode="json"))
        digest = hashlib.sha256(content).hexdigest()
        published = self.publisher.publish_bytes(
            f"public/metadata-{digest}.json",
            content,
            expected_sha256=digest,
            expected_size=len(content),
        )
        return ManifestFile(
            relative_path=published.relative_path,
            sha256=published.sha256,
            size_bytes=published.size_bytes,
        )


def create_privacy_router(service: PrivacyService) -> APIRouter:
    router = APIRouter(prefix="/api/v1/privacy", tags=["privacy"])

    @router.post("/public-metadata", status_code=status.HTTP_201_CREATED)
    def publish_public_metadata(body: PublicMetadataPayload) -> ManifestFile:
        return service.publish_public_metadata(body.root)

    return router
