from __future__ import annotations

import hashlib
import hmac
from collections.abc import Iterable, Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from sts.domain import DomainError, ErrorCode, canonical_json_bytes

_DOMAIN_SEPARATOR = b"synthetic-table-studio/public-fit-predicate/v1\x00"
_HASH_SPACE = 1 << 256


class PublicFitSamplingPredicate:
    """A deterministic content-HMAC row predicate with no amplification claim.

    Selection is a fixed function of one canonical row and a predeclared public
    key. It never ranks rows, derives a cutoff from the source, or truncates a
    selected set. Consequently add/remove neighboring inputs map to equal or
    add/remove-one selected inputs.
    """

    __slots__ = ("_public_key", "rate", "threshold")

    algorithm = "HMAC-SHA256"
    stability = "rowwise_1_stable"
    amplification_claimed = False
    truncation_allowed = False

    def __init__(self, fit_sampling_rate: Decimal | str, public_key: bytes) -> None:
        try:
            rate = Decimal(fit_sampling_rate)
        except (InvalidOperation, ValueError) as error:
            raise ValueError("fit_sampling_rate must be a decimal in (0, 1]") from error
        if not rate.is_finite() or not Decimal(0) < rate <= Decimal(1):
            raise ValueError("fit_sampling_rate must be a decimal in (0, 1]")
        if not isinstance(public_key, bytes) or len(public_key) < 16:
            raise ValueError("public HMAC key must contain at least 128 bits")
        numerator, denominator = rate.as_integer_ratio()
        self.rate = rate
        self.threshold = numerator * _HASH_SPACE // denominator
        self._public_key = public_key

    def __repr__(self) -> str:
        return (
            "PublicFitSamplingPredicate(algorithm='HMAC-SHA256', "
            f"rate={str(self.rate)!r}, stability='rowwise_1_stable')"
        )

    def selected(self, row: Mapping[str, Any]) -> bool:
        message = _DOMAIN_SEPARATOR + canonical_json_bytes(row)
        digest = hmac.digest(self._public_key, message, hashlib.sha256)
        return int.from_bytes(digest, "big") < self.threshold

    def select_rows(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        maximum_selected_rows: int,
    ) -> tuple[Mapping[str, Any], ...]:
        """Materialize the selected fit input or reject it without truncation."""

        if isinstance(maximum_selected_rows, bool) or maximum_selected_rows < 0:
            raise ValueError("maximum_selected_rows must be a non-negative integer")
        selected: list[Mapping[str, Any]] = []
        for row in rows:
            if self.selected(row):
                selected.append(row)
                if len(selected) > maximum_selected_rows:
                    # Do not return a prefix: a source-dependent truncation would
                    # violate the approved preprocessing contract.
                    raise DomainError(
                        ErrorCode.RESOURCE_LIMIT,
                        "public fit predicate selected more rows than the memory gate permits; "
                        "the selection was not truncated",
                        context={
                            "maximum_selected_rows": maximum_selected_rows,
                            "truncated": False,
                            "amplification_claimed": False,
                        },
                    )
        return tuple(selected)

    def public_contract(self) -> dict[str, object]:
        return {
            "version": "1.0",
            "algorithm": self.algorithm,
            "fit_sampling_rate": str(self.rate),
            "stability": self.stability,
            "truncation_allowed": False,
            "amplification_claimed": False,
        }


__all__ = ["PublicFitSamplingPredicate"]
