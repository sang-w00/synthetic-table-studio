from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Literal

from sts.domain import CanonicalModel

_REQUIRED_FORMAL_GATES = frozenset(
    {
        "environment",
        "fit",
        "persist",
        "fresh_process_repeated_sample",
        "trusted_curator_checkpoint_boundary",
        "public_false_source_audit",
        "add_remove_accounting",
        "conservative_state_estimates",
    }
)


class DpMechanismProvenance(CanonicalModel):
    """Release-safe identity of the mechanism build that produced a guarantee.

    Every field is copied from the verified Phase-0 probe result, so an external
    reader can check which wheel and which (epsilon, delta) -> rho conversion the
    released model was actually produced by. A field the probe does not carry is
    left as None and is then dropped by the release allowlist rather than guessed.
    """

    version: Literal["1.0"] = "1.0"
    package_version: str | None = None
    wheel_sha256: str | None = None
    lock_sha256: str | None = None
    accountant: str | None = None
    conversion: str | None = None


class FormalDpAvailability(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    formal_dp_enabled: bool
    aim_enabled: bool
    probe_status: str
    failed_gates: tuple[str, ...]
    failure_reasons: tuple[str, ...]
    probe_result_path: str


def default_dpmm_probe_result_path() -> Path:
    configured = os.environ.get("STS_DPMM_PROBE_RESULT")
    if configured:
        return Path(configured).expanduser().resolve(strict=False)
    project_root = Path(__file__).resolve().parents[4]
    return project_root / "probes" / "results" / "dpmm_contract.json"


def load_dp_mechanism_provenance(
    path: str | Path | None = None,
) -> DpMechanismProvenance:
    """Read the mechanism build identity, or return an empty record if unreadable."""

    result_path = (
        Path(path).expanduser().resolve(strict=False)
        if path is not None
        else default_dpmm_probe_result_path()
    )
    try:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        environment = payload.get("environment")
        audit = payload.get("accounting_audit")
        if not isinstance(environment, dict):
            environment = {}
        if not isinstance(audit, dict):
            audit = {}
        declared = environment.get("declared_versions")
        package_version = (
            str(declared["dpmm"]) if isinstance(declared, dict) and "dpmm" in declared else None
        )
        conversion = audit.get("conversion")
        conversion_label = None
        if isinstance(conversion, dict) and "rho" in conversion:
            # The zCDP rho the (epsilon, delta) pair was converted into, named so an
            # external reader can reproduce the conversion rather than trust a label.
            conversion_label = f"epsilon_delta_to_zcdp_rho={conversion['rho']}"
        return DpMechanismProvenance(
            package_version=package_version,
            wheel_sha256=_optional_digest(environment.get("dpmm_wheel_sha256")),
            lock_sha256=_optional_digest(environment.get("lock_sha256")),
            accountant="basic_sequential",
            conversion=conversion_label,
        )
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return DpMechanismProvenance()


def _optional_digest(value: object) -> str | None:
    text = str(value).lower() if value is not None else ""
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        return None
    return text


def load_dp_availability(path: str | Path | None = None) -> FormalDpAvailability:
    """Read the verified Phase-0 result and independently fail closed."""

    result_path = (
        Path(path).expanduser().resolve(strict=False)
        if path is not None
        else default_dpmm_probe_result_path()
    )
    try:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise TypeError("probe result must be a JSON object")
        gates = payload.get("formal_dp_gate")
        if not isinstance(gates, dict):
            raise TypeError("formal_dp_gate must be a JSON object")
        failed_gates = tuple(
            sorted(gate for gate in _REQUIRED_FORMAL_GATES if gates.get(gate) is not True)
        )
        status = str(payload.get("status", "invalid"))
        declared_enabled = payload.get("formal_dp_enabled") is True
        formal_enabled = declared_enabled and status == "passed" and not failed_gates
        raw_reasons = payload.get("failure_reasons", ())
        if not isinstance(raw_reasons, list) or any(
            not isinstance(item, str) for item in raw_reasons
        ):
            raise TypeError("failure_reasons must be a string array")
        aim = payload.get("aim")
        aim_declared = isinstance(aim, dict) and aim.get("enabled") is True
        aim_equivalent_gates = (
            isinstance(aim, dict) and aim.get("equivalent_gates_executed") is True
        )
        aim_enabled = formal_enabled and aim_declared and aim_equivalent_gates
        reasons = tuple(raw_reasons)
        if declared_enabled and not formal_enabled and not reasons:
            reasons = ("formal_dp_gate_inconsistent",)
        return FormalDpAvailability(
            formal_dp_enabled=formal_enabled,
            aim_enabled=aim_enabled,
            probe_status=status,
            failed_gates=failed_gates,
            failure_reasons=reasons,
            probe_result_path=str(result_path),
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
        return FormalDpAvailability(
            formal_dp_enabled=False,
            aim_enabled=False,
            probe_status="unavailable",
            failed_gates=tuple(sorted(_REQUIRED_FORMAL_GATES)),
            failure_reasons=(f"probe_result_unavailable:{type(error).__name__}",),
            probe_result_path=str(result_path),
        )


__all__ = [
    "DpMechanismProvenance",
    "FormalDpAvailability",
    "default_dpmm_probe_result_path",
    "load_dp_availability",
    "load_dp_mechanism_provenance",
]
