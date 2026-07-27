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
    "FormalDpAvailability",
    "default_dpmm_probe_result_path",
    "load_dp_availability",
]
