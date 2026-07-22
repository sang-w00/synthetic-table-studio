from __future__ import annotations

import hmac
from hashlib import sha256
from typing import Literal

from pydantic import Field, model_validator

from sts.domain.canonical import CanonicalModel


class EvaluationConfig(CanonicalModel):
    """Immutable settings for the version 1.0 evaluation contract.

    ``master_seed`` is public evaluation randomness.  It is deliberately distinct
    from model fitting and DP mechanism randomness.
    """

    version: Literal["1.0"] = "1.0"
    master_seed: int = Field(ge=0, le=2**64 - 1)
    primary_sample_rows: int = Field(default=200_000, gt=0, le=200_000)
    categorical_top_k: Literal[100] = 100
    bootstrap_repetitions: Literal[500] = 500
    confidence_level: Literal[0.95] = 0.95

    @model_validator(mode="after")
    def validate_version_settings(self) -> EvaluationConfig:
        # Literal fields make persisted configurations fail closed instead of
        # silently changing the meaning of an EvaluationConfig 1.0 report.
        if self.version != "1.0":  # pragma: no cover - guarded by Pydantic
            raise ValueError("unsupported evaluation config version")
        return self

    def derive_seed(self, label: str) -> int:
        """Derive a stable unsigned 64-bit seed for one named operation."""

        if not label:
            raise ValueError("seed label must not be empty")
        key = self.master_seed.to_bytes(8, "big", signed=False)
        digest = hmac.new(key, b"sts-evaluation-1.0\x00" + label.encode("utf-8"), sha256)
        return int.from_bytes(digest.digest()[:8], "big", signed=False)

    def derive_hmac_key(self, label: str) -> bytes:
        """Derive a domain-separated HMAC key without persisting it in reports."""

        if not label:
            raise ValueError("HMAC key label must not be empty")
        key = self.master_seed.to_bytes(8, "big", signed=False)
        return hmac.new(
            key,
            b"sts-evaluation-hmac-key-1.0\x00" + label.encode("utf-8"),
            sha256,
        ).digest()
