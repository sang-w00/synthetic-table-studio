from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import Field

from .canonical import CanonicalModel


class ErrorCode(StrEnum):
    SESSION_REQUIRED = "SESSION_REQUIRED"
    ORIGIN_REJECTED = "ORIGIN_REJECTED"
    HOST_REJECTED = "HOST_REJECTED"
    REPORT_NOT_RELEASE_SAFE = "REPORT_NOT_RELEASE_SAFE"
    DATASET_NOT_FOUND = "DATASET_NOT_FOUND"
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    ARTIFACT_NOT_FOUND = "ARTIFACT_NOT_FOUND"
    INVALID_STATE = "INVALID_STATE"
    SOURCE_RULE_VIOLATION = "SOURCE_RULE_VIOLATION"
    RULE_CONFLICT = "RULE_CONFLICT"
    RESUME_UNAVAILABLE = "RESUME_UNAVAILABLE"
    ARTIFACT_NOT_READY = "ARTIFACT_NOT_READY"
    BACKEND_INCOMPATIBLE = "BACKEND_INCOMPATIBLE"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    IMMUTABLE_PATH_EXISTS = "IMMUTABLE_PATH_EXISTS"
    UPLOAD_TOO_LARGE = "UPLOAD_TOO_LARGE"
    INPUT_FORMAT_UNSUPPORTED = "INPUT_FORMAT_UNSUPPORTED"
    CSV_DIALECT_AMBIGUOUS = "CSV_DIALECT_AMBIGUOUS"
    XLSX_UNSAFE = "XLSX_UNSAFE"
    SCHEMA_INVALID = "SCHEMA_INVALID"
    OUTPUT_INVALID = "OUTPUT_INVALID"
    RULE_FEASIBILITY_EXHAUSTED = "RULE_FEASIBILITY_EXHAUSTED"
    DP_METADATA_NOT_PUBLIC = "DP_METADATA_NOT_PUBLIC"
    DP_DOMAIN_TOO_LARGE = "DP_DOMAIN_TOO_LARGE"
    RESOURCE_LIMIT = "RESOURCE_LIMIT"
    DISK_QUOTA_EXCEEDED = "DISK_QUOTA_EXCEEDED"
    WORKER_FAILED = "WORKER_FAILED"
    CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"


_ERROR_STATUS: dict[ErrorCode, int] = {
    ErrorCode.SESSION_REQUIRED: 401,
    ErrorCode.ORIGIN_REJECTED: 403,
    ErrorCode.HOST_REJECTED: 403,
    ErrorCode.REPORT_NOT_RELEASE_SAFE: 403,
    ErrorCode.DATASET_NOT_FOUND: 404,
    ErrorCode.JOB_NOT_FOUND: 404,
    ErrorCode.ARTIFACT_NOT_FOUND: 404,
    ErrorCode.INVALID_STATE: 409,
    ErrorCode.SOURCE_RULE_VIOLATION: 409,
    ErrorCode.RULE_CONFLICT: 409,
    ErrorCode.RESUME_UNAVAILABLE: 409,
    ErrorCode.ARTIFACT_NOT_READY: 409,
    ErrorCode.BACKEND_INCOMPATIBLE: 409,
    ErrorCode.IDEMPOTENCY_CONFLICT: 409,
    ErrorCode.IMMUTABLE_PATH_EXISTS: 409,
    ErrorCode.UPLOAD_TOO_LARGE: 413,
    ErrorCode.INPUT_FORMAT_UNSUPPORTED: 422,
    ErrorCode.CSV_DIALECT_AMBIGUOUS: 422,
    ErrorCode.XLSX_UNSAFE: 422,
    ErrorCode.SCHEMA_INVALID: 422,
    ErrorCode.OUTPUT_INVALID: 422,
    ErrorCode.RULE_FEASIBILITY_EXHAUSTED: 422,
    ErrorCode.DP_METADATA_NOT_PUBLIC: 422,
    ErrorCode.DP_DOMAIN_TOO_LARGE: 422,
    ErrorCode.RESOURCE_LIMIT: 507,
    ErrorCode.DISK_QUOTA_EXCEEDED: 507,
    ErrorCode.WORKER_FAILED: 500,
    ErrorCode.CHECKSUM_MISMATCH: 500,
}


class ProblemDetails(CanonicalModel):
    """RFC 9457 problem details with a stable machine-readable application code."""

    type: str = "about:blank"
    title: str
    status: int = Field(ge=400, le=599)
    detail: str
    instance: str | None = None
    code: ErrorCode
    context: dict[str, Any] = Field(default_factory=dict)


class DomainError(Exception):
    def __init__(
        self,
        code: ErrorCode,
        detail: str,
        *,
        title: str | None = None,
        status: int | None = None,
        instance: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.problem = ProblemDetails(
            title=title or code.value.replace("_", " ").title(),
            status=status or _ERROR_STATUS[code],
            detail=detail,
            instance=instance,
            code=code,
            context=context or {},
        )
        super().__init__(f"{code.value}: {detail}")

    @property
    def code(self) -> ErrorCode:
        return self.problem.code

    @property
    def status(self) -> int:
        return self.problem.status


def problem_for(
    code: ErrorCode,
    detail: str,
    *,
    instance: str | None = None,
    context: dict[str, Any] | None = None,
) -> ProblemDetails:
    return DomainError(code, detail, instance=instance, context=context).problem
