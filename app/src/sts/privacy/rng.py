from __future__ import annotations

import hashlib
import secrets
from typing import Any, Literal

from pydantic import field_validator

from sts.domain import CanonicalModel

_COMMITMENT_DOMAIN = b"synthetic-table-studio/private-fit-rng/v1\x00"


class PrivateFitRngPolicy(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    entropy_source: Literal["OS CSPRNG (256-bit)"] = "OS CSPRNG (256-bit)"
    rng_implementation: Literal[
        "numpy.random.SeedSequence -> numpy.random.RandomState(MT19937)"
    ] = "numpy.random.SeedSequence -> numpy.random.RandomState(MT19937)"
    commitment_sha256: str

    @field_validator("commitment_sha256")
    @classmethod
    def validate_commitment(cls, value: str) -> str:
        lowered = value.lower()
        if len(lowered) != 64 or any(character not in "0123456789abcdef" for character in lowered):
            raise ValueError("commitment must be a SHA-256 hexadecimal digest")
        return lowered


class PrivateFitRng:
    """One-shot private fit RNG handle whose serializable surface is seed-free."""

    __slots__ = ("_entropy", "_consumed", "policy")

    def __init__(self, entropy: bytes) -> None:
        if not isinstance(entropy, bytes) or len(entropy) != 32:
            raise ValueError("private fit entropy must contain exactly 256 bits")
        self._entropy = bytearray(entropy)
        self._consumed = False
        self.policy = PrivateFitRngPolicy(
            commitment_sha256=hashlib.sha256(_COMMITMENT_DOMAIN + entropy).hexdigest()
        )

    def __repr__(self) -> str:
        return f"PrivateFitRng(policy={self.policy!r}, private_material=<redacted>)"

    def __reduce__(self) -> Any:
        raise TypeError("private fit RNG handles must never be serialized")

    def public_record(self) -> dict[str, str]:
        return self.policy.model_dump(mode="json")

    def take_numpy_random_state(self) -> Any:
        """Build the exact worker RNG once, then erase this handle's seed bytes.

        NumPy is imported lazily because the app environment creates the policy,
        while the locked dpmm worker environment owns the fit implementation.
        """

        if self._consumed:
            raise RuntimeError("private fit RNG material has already been consumed")
        try:
            import numpy as np
        except ImportError as error:  # pragma: no cover - exercised in the worker environment
            raise RuntimeError("NumPy is required in the locked dpmm worker environment") from error
        entropy_words = [
            int.from_bytes(self._entropy[offset : offset + 4], "little")
            for offset in range(0, 32, 4)
        ]
        seed_sequence = np.random.SeedSequence(entropy_words)
        seed_words = seed_sequence.generate_state(624, dtype=np.uint32)
        random_state = np.random.RandomState(seed_words)
        for index in range(len(self._entropy)):
            self._entropy[index] = 0
        self._consumed = True
        return random_state


def create_private_fit_rng() -> PrivateFitRng:
    """Acquire 256 bits directly from the operating system CSPRNG."""

    return PrivateFitRng(secrets.token_bytes(32))


__all__ = ["PrivateFitRng", "PrivateFitRngPolicy", "create_private_fit_rng"]
