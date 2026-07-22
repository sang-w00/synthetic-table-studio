#!/usr/bin/env python3
"""Run the pinned dpmm MST persistence, sampling, and privacy contract gate."""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.metadata
import inspect
import itertools
import json
import math
import os
from pathlib import Path
import pickle
import platform
import re
import resource
import secrets
import subprocess
import sys
import tempfile
import textwrap
import time
import tomllib
from typing import Any, Callable


PROBE_SCHEMA_VERSION = "1.0"
EXPECTED_VERSIONS = {
    "dpmm": "0.1.9",
    "numpy": "1.26.4",
    "pandas": "2.1.0",
}
EXPECTED_DPMM_WHEEL_SHA256 = (
    "fbd71d26caa51733cf1d8382f140faa755d7157e7d471191fee2c4a862a2f51b"
)
EPSILON = 3.0
DELTA = 1e-6
BATCH_SIZE = 10_000
BATCH_COUNT = 2
APP_STATE_LIMIT_BYTES = 512 * 1024 * 1024
APP_LARGEST_PAIR_LIMIT_CELLS = 1_000_000
PRIVATE_KEY_PATTERN = re.compile(
    r"(?:source.*(?:path|manifest)|raw.*(?:data|frame|rows?)|"
    r"unnoised|private.*seed|fit.*seed)",
    re.IGNORECASE,
)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(f".{path.name}.{os.getpid()}.part")
    encoded = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8")
    with part.open("wb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(part, path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def _redact(message: str, replacements: dict[str, str]) -> str:
    redacted = message
    for original, replacement in replacements.items():
        redacted = redacted.replace(original, replacement)
    return redacted


def _public_fixture() -> tuple[Any, dict[str, int], dict[str, int]]:
    import numpy as np
    import pandas as pd

    rows = 4_096
    index = np.arange(rows, dtype=np.int64)
    domain = {
        "region": 4,
        "age_bin": 5,
        "segment": 3,
        "active": 3,
        "tenure_bin": 6,
    }
    missing = {
        "region": 3,
        "age_bin": 4,
        "segment": 2,
        "active": 2,
        "tenure_bin": 5,
    }
    frame = pd.DataFrame(
        {
            "region": (index * 7 + index // 11) % 3,
            "age_bin": (index // 5 + index // 17) % 4,
            "segment": ((index // 3) + (index % 3 == 0)) % 2,
            "active": ((index // 7) + (index % 5 == 0)) % 2,
            "tenure_bin": (index // 13 + index // 29) % 5,
        }
    ).astype("int64")

    # Missing is an explicit public category, never a privately discovered sentinel.
    frame.loc[index % 97 == 0, "region"] = missing["region"]
    frame.loc[index % 89 == 0, "age_bin"] = missing["age_bin"]
    frame.loc[index % 83 == 0, "segment"] = missing["segment"]
    frame.loc[index % 79 == 0, "active"] = missing["active"]
    frame.loc[index % 73 == 0, "tenure_bin"] = missing["tenure_bin"]
    return frame, domain, missing


def _lock_and_import_gate(repo_root: Path) -> dict[str, Any]:
    import dpmm_worker

    project_root = repo_root / "workers" / "dpmm"
    lock_path = project_root / "uv.lock"
    project_path = project_root / "pyproject.toml"
    lock = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    project = tomllib.loads(project_path.read_text(encoding="utf-8"))
    packages = {entry["name"]: entry for entry in lock.get("package", [])}

    resolved_versions = {
        name: packages.get(name, {}).get("version") for name in EXPECTED_VERSIONS
    }
    imported_versions = {
        name: importlib.metadata.version(name) for name in EXPECTED_VERSIONS
    }
    dpmm_wheels = packages.get("dpmm", {}).get("wheels", [])
    wheel_hashes = [item.get("hash", "") for item in dpmm_wheels]
    expected_dependencies = {
        f"{name}=={version}" for name, version in EXPECTED_VERSIONS.items()
    }
    declared_dependencies = set(project["project"]["dependencies"])
    checks = {
        "python_is_3_11": sys.version_info[:2] == (3, 11),
        "requires_python_is_3_11_only": project["project"].get("requires-python")
        == ">=3.11,<3.12",
        "declared_versions_exact": expected_dependencies <= declared_dependencies,
        "lock_versions_exact": resolved_versions == EXPECTED_VERSIONS,
        "imports_versions_exact": imported_versions == EXPECTED_VERSIONS,
        "worker_package_imports": dpmm_worker.DPMM_VERSION == EXPECTED_VERSIONS["dpmm"],
        "dpmm_wheel_hash_exact": (
            f"sha256:{EXPECTED_DPMM_WHEEL_SHA256}" in wheel_hashes
        ),
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "declared_versions": dict(sorted(EXPECTED_VERSIONS.items())),
        "resolved_versions": resolved_versions,
        "imported_versions": imported_versions,
        "dpmm_wheel_sha256": EXPECTED_DPMM_WHEEL_SHA256,
        "lock_sha256": _sha256_file(lock_path),
    }


def _compact_source(obj: Any) -> str:
    return "".join(inspect.getsource(obj).split())


def _source_file_record(obj: Any, dpmm_root: Path) -> dict[str, Any]:
    path = Path(inspect.getsourcefile(obj) or "").resolve()
    return {
        "path": path.relative_to(dpmm_root).as_posix(),
        "sha256": _sha256_file(path),
    }


def _source_audit() -> dict[str, Any]:
    import dpmm
    from dpmm.models.base.mechanisms.cdp2adp import cdp_delta, cdp_eps, cdp_rho
    from dpmm.models.base.mechanisms.mechanism import Mechanism
    from dpmm.models.mst import MST
    from dpmm.pipelines.base import GenerativePipeline

    dpmm_root = Path(dpmm.__file__).resolve().parent
    pipeline_fit = _compact_source(GenerativePipeline.fit)
    mechanism_fit = _compact_source(Mechanism.fit)
    private_measure = _compact_source(Mechanism._measure)
    mst_init = _compact_source(MST.__init__)
    mst_fit = _compact_source(MST._fit)
    mst_select = _compact_source(MST.select)

    pipeline_tree = ast.parse(textwrap.dedent(inspect.getsource(GenerativePipeline.fit)))
    pipeline_forwards_public = any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "fit"
        and any(
            keyword.arg == "public"
            and isinstance(keyword.value, ast.Name)
            and keyword.value.id == "public"
            for keyword in node.keywords
        )
        for node in ast.walk(pipeline_tree)
    )

    checks = {
        "pipeline_public_default_false": inspect.signature(
            GenerativePipeline.fit
        ).parameters["public"].default
        is False,
        "pipeline_forwards_public_argument": pipeline_forwards_public,
        "mechanism_public_default_false": inspect.signature(Mechanism.fit).parameters[
            "public"
        ].default
        is False,
        "mechanism_forwards_public_to_private_fit": (
            "self._fit(data=data,public=public" in mechanism_fit
        ),
        "private_measurement_adds_gaussian_noise": (
            "ifpublic:y=xelse:y=x+gaussian_noise(" in private_measure
        ),
        "public_measurement_is_exact_only_in_public_branch": "ifpublic:y=x" in private_measure,
        "mst_private_default_false": inspect.signature(MST._fit).parameters[
            "public"
        ].default
        is False,
        "mst_one_way_measure_forwards_public": (
            "self.measure(data,cliques=cliques_1,public=public)" in mst_fit
        ),
        "mst_two_way_measure_uses_private_default": (
            "self.measure(data,cliques=cliques_2,flatten=True)" in mst_fit
        ),
        "mst_selection_public_default_false": inspect.signature(MST.select).parameters[
            "public"
        ].default
        is False,
        "mst_selection_unit_sensitivity": (
            "self.exponential_mechanism(wgts,epsilon,sensitivity=1.0)" in mst_select
        ),
        "mst_sigma_allocates_one_third_rho": (
            "self.sigma=np.sqrt(3/(2*self.rho))" in mst_init
        ),
        "cdp_conversion_functions_callable": all(
            callable(item) for item in (cdp_rho, cdp_eps, cdp_delta)
        ),
    }
    records_by_path: dict[str, dict[str, Any]] = {}
    for obj in (
        GenerativePipeline,
        Mechanism,
        MST,
        cdp_rho,
    ):
        record = _source_file_record(obj, dpmm_root)
        records_by_path[record["path"]] = record
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "installed_source_files": list(records_by_path.values()),
        "audit_scope": [
            "public=False propagation",
            "private Gaussian measurement branch",
            "private exponential selection with sensitivity 1",
            "rho split constants",
            "cDP conversion implementation",
        ],
    }


def _independent_cdp_delta(rho: float, epsilon: float) -> float:
    if rho == 0:
        return 0.0
    alpha_min = 1.01
    alpha_max = (epsilon + 1.0) / (2.0 * rho) + 2.0
    alpha = alpha_min
    for _ in range(1_000):
        alpha = (alpha_min + alpha_max) / 2.0
        derivative = (
            (2.0 * alpha - 1.0) * rho
            - epsilon
            + math.log1p(-1.0 / alpha)
        )
        if derivative < 0:
            alpha_min = alpha
        else:
            alpha_max = alpha
    value = math.exp(
        (alpha - 1.0) * (alpha * rho - epsilon)
        + alpha * math.log1p(-1.0 / alpha)
    ) / (alpha - 1.0)
    return min(value, 1.0)


def _independent_cdp_rho(epsilon: float, delta: float) -> float:
    rho_min = 0.0
    rho_max = epsilon + 1.0
    for _ in range(1_000):
        rho = (rho_min + rho_max) / 2.0
        if _independent_cdp_delta(rho, epsilon) <= delta:
            rho_min = rho
        else:
            rho_max = rho
    return rho_min


def _accounting_audit() -> dict[str, Any]:
    import numpy as np
    from dpmm.models.base.mechanisms.cdp2adp import cdp_delta, cdp_eps, cdp_rho

    base_rows = np.array([[0, 0], [1, 0], [1, 1]], dtype=np.int64)
    added_rows = np.vstack([base_rows, np.array([[0, 1]], dtype=np.int64)])

    def histogram(rows: Any) -> Any:
        cells = rows[:, 0] * 2 + rows[:, 1]
        return np.bincount(cells, minlength=4).astype(np.float64)

    base_histogram = histogram(base_rows)
    added_histogram = histogram(added_rows)
    difference = added_histogram - base_histogram
    l1_sensitivity = float(np.linalg.norm(difference, ord=1))
    l2_sensitivity = float(np.linalg.norm(difference, ord=2))

    installed_rho = float(cdp_rho(EPSILON, DELTA))
    independent_rho = _independent_cdp_rho(EPSILON, DELTA)
    known_rho = 0.18506984065340149
    installed_delta = float(cdp_delta(installed_rho, EPSILON))
    roundtrip_epsilon = float(cdp_eps(installed_rho, DELTA))
    sigma = math.sqrt(3.0 / (2.0 * installed_rho))
    expected_sigma = 2.846936654408663
    one_third_rho = installed_rho / 3.0
    checks = {
        "add_one_histogram_l1_sensitivity_is_one": math.isclose(
            l1_sensitivity, 1.0, rel_tol=0.0, abs_tol=0.0
        ),
        "add_one_histogram_l2_sensitivity_is_one": math.isclose(
            l2_sensitivity, 1.0, rel_tol=0.0, abs_tol=0.0
        ),
        "remove_one_is_symmetric": bool(
            np.array_equal(base_histogram - added_histogram, -difference)
        ),
        "installed_rho_matches_known_vector": math.isclose(
            installed_rho, known_rho, rel_tol=1e-12, abs_tol=1e-15
        ),
        "installed_rho_matches_independent_conversion": math.isclose(
            installed_rho, independent_rho, rel_tol=1e-12, abs_tol=1e-15
        ),
        "rho_to_delta_roundtrip": math.isclose(
            installed_delta, DELTA, rel_tol=1e-10, abs_tol=1e-15
        ),
        "rho_to_epsilon_roundtrip": math.isclose(
            roundtrip_epsilon, EPSILON, rel_tol=1e-12, abs_tol=1e-12
        ),
        "mst_sigma_matches_known_vector": math.isclose(
            sigma, expected_sigma, rel_tol=1e-12, abs_tol=1e-15
        ),
        "three_equal_rho_allocations_sum_to_total": math.isclose(
            one_third_rho * 3.0, installed_rho, rel_tol=1e-15, abs_tol=0.0
        ),
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "adjacency": "add_remove_one_row",
        "privacy_unit": "row",
        "known_vector": {
            "base_histogram": base_histogram.astype(int).tolist(),
            "added_histogram": added_histogram.astype(int).tolist(),
            "difference": difference.astype(int).tolist(),
            "l1_sensitivity": l1_sensitivity,
            "l2_sensitivity": l2_sensitivity,
        },
        "conversion": {
            "epsilon": EPSILON,
            "delta": DELTA,
            "rho": installed_rho,
            "independent_rho": independent_rho,
            "delta_roundtrip": installed_delta,
            "epsilon_roundtrip": roundtrip_epsilon,
            "mst_sigma": sigma,
        },
        "rho_allocation": {
            "one_way_measurements": one_third_rho,
            "pair_selection": one_third_rho,
            "two_way_measurements": one_third_rho,
            "total": installed_rho,
        },
    }


def _state_estimate_gate() -> dict[str, Any]:
    from dpmm.models.base.memory import meas_size

    scenarios: list[dict[str, Any]] = []
    for column_count in (8, 16, 32):
        cardinalities = [256] * column_count
        one_way_cells = sum(cardinalities)
        pair_cells = sorted(
            (
                cardinalities[left] * cardinalities[right]
                for left, right in itertools.combinations(range(column_count), 2)
            ),
            reverse=True,
        )
        selected_pair_cells = pair_cells[: column_count - 1]
        modeled_cells = one_way_cells + sum(selected_pair_cells)
        raw_float64_bytes = modeled_cells * 8
        app_estimated_state_bytes = raw_float64_bytes * 8
        largest_pair_cells = pair_cells[0] if pair_cells else 0

        upstream_reported_mib = sum(meas_size((value,)) for value in cardinalities)
        upstream_reported_mib += sum(
            meas_size((256, 256)) for _ in selected_pair_cells
        )
        upstream_implied_bytes = int(math.ceil(upstream_reported_mib * 1024**2))
        checks = {
            "modeled_columns_at_most_32": column_count <= 32,
            "states_per_column_at_most_256": max(cardinalities) <= 256,
            "largest_pair_cells_within_limit": (
                largest_pair_cells <= APP_LARGEST_PAIR_LIMIT_CELLS
            ),
            "app_state_within_512_mib": (
                app_estimated_state_bytes <= APP_STATE_LIMIT_BYTES
            ),
            "app_estimate_not_less_than_raw_float64_state": (
                app_estimated_state_bytes >= raw_float64_bytes
            ),
        }
        scenarios.append(
            {
                "columns": column_count,
                "states_per_column": 256,
                "one_way_cells": one_way_cells,
                "selected_largest_pair_cells": sum(selected_pair_cells),
                "largest_pair_cells": largest_pair_cells,
                "app_estimated_state_bytes": app_estimated_state_bytes,
                "raw_float64_state_bytes": raw_float64_bytes,
                "upstream_model_size_reported_mib": float(upstream_reported_mib),
                "upstream_reported_mib_implied_bytes": upstream_implied_bytes,
                "app_to_upstream_implied_bytes_ratio": (
                    app_estimated_state_bytes / upstream_implied_bytes
                    if upstream_implied_bytes
                    else None
                ),
                "checks": checks,
                "passed": all(checks.values()),
            }
        )
    return {
        "passed": all(item["passed"] for item in scenarios),
        "capacity_estimator_version": 1,
        "formula": (
            "8*(one_way_cells+sum_largest_d_minus_1_pair_cells)*8_safety_factor"
        ),
        "state_limit_bytes": APP_STATE_LIMIT_BYTES,
        "largest_pair_limit_cells": APP_LARGEST_PAIR_LIMIT_CELLS,
        "admission_uses_upstream_model_size_alone": False,
        "scenarios": scenarios,
    }


def _random_state_equal(left: Any, right: Any) -> bool:
    import numpy as np

    return (
        left[0] == right[0]
        and np.array_equal(left[1], right[1])
        and left[2:] == right[2:]
    )


def _true_histograms(frame: Any, cliques: list[tuple[str, ...]], domain: dict[str, int]) -> list[Any]:
    import numpy as np

    histograms: list[Any] = []
    for clique in cliques:
        shape = tuple(domain[column] for column in clique)
        coordinates = tuple(frame[column].to_numpy() for column in clique)
        flattened = np.ravel_multi_index(coordinates, shape)
        histograms.append(np.bincount(flattened, minlength=math.prod(shape)))
    return histograms


class _CheckpointScanner:
    def __init__(
        self,
        *,
        source_marker: str,
        source_path: Path,
        raw_frame: Any,
        true_histograms: list[Any],
        private_random_state: Any,
    ) -> None:
        self.source_marker = source_marker
        self.source_path = str(source_path.resolve())
        self.raw_frame = raw_frame
        self.raw_matrix = raw_frame.to_numpy()
        self.true_histograms = true_histograms
        self.private_random_state = private_random_state
        self.findings: list[dict[str, Any]] = []
        self.visited: set[int] = set()
        self.nodes = 0
        self.truncated = False

    def _finding(self, code: str, path: str, detail: str) -> None:
        finding = {"code": code, "object_path": path, "detail": detail}
        if finding not in self.findings:
            self.findings.append(finding)

    def visit(self, value: Any, path: str) -> None:
        import numpy as np
        import pandas as pd

        if self.nodes >= 100_000:
            self.truncated = True
            return
        self.nodes += 1

        if value is None or isinstance(value, (bool, int, float, complex, bytes, str)):
            if isinstance(value, str) and (
                self.source_marker in value or self.source_path in value
            ):
                self._finding(
                    "SOURCE_PATH_OR_MARKER_SERIALIZED",
                    path,
                    "serialized string contains the denied source marker or path",
                )
            return

        identity = id(value)
        if identity in self.visited:
            return
        self.visited.add(identity)

        if isinstance(value, np.random.RandomState):
            matches_private = _random_state_equal(
                value.get_state(), self.private_random_state
            )
            self._finding(
                "RNG_STATE_SERIALIZED",
                path,
                (
                    "serialized RandomState matches the private fit RNG state"
                    if matches_private
                    else "serialized RandomState is present in the checkpoint"
                ),
            )
            return
        if isinstance(value, np.random.Generator):
            self._finding(
                "RNG_STATE_SERIALIZED",
                path,
                "serialized NumPy Generator is present in the checkpoint",
            )
            return
        if isinstance(value, pd.DataFrame):
            self._finding(
                "DATAFRAME_SERIALIZED",
                path,
                f"serialized DataFrame has shape {value.shape}",
            )
            return
        if isinstance(value, pd.Series):
            self._finding(
                "SERIES_SERIALIZED",
                path,
                f"serialized Series has length {len(value)}",
            )
            return
        if isinstance(value, np.ndarray):
            if value.shape == self.raw_matrix.shape and np.array_equal(
                value, self.raw_matrix
            ):
                self._finding(
                    "RAW_ROWS_SERIALIZED",
                    path,
                    "serialized ndarray exactly matches the public fixture rows",
                )
            for histogram in self.true_histograms:
                if value.shape == histogram.shape and np.array_equal(value, histogram):
                    self._finding(
                        "UNNOISED_MEASUREMENT_SERIALIZED",
                        path,
                        "serialized ndarray exactly matches a true fixture marginal",
                    )
                    break
            return
        if isinstance(value, dict):
            for key, item in value.items():
                key_text = str(key)
                child_path = f"{path}[{key_text!r}]"
                if PRIVATE_KEY_PATTERN.search(key_text):
                    self._finding(
                        "FORBIDDEN_PRIVATE_FIELD_NAME",
                        child_path,
                        "serialized field name matches a forbidden private/source pattern",
                    )
                self.visit(item, child_path)
            return
        if isinstance(value, (list, tuple, set, frozenset)):
            for index, item in enumerate(value):
                self.visit(item, f"{path}[{index}]")
            return
        if hasattr(value, "__dict__"):
            for key, item in vars(value).items():
                child_path = f"{path}.{key}"
                if PRIVATE_KEY_PATTERN.search(key):
                    self._finding(
                        "FORBIDDEN_PRIVATE_FIELD_NAME",
                        child_path,
                        "serialized attribute name matches a forbidden private/source pattern",
                    )
                self.visit(item, child_path)


def _checkpoint_audit(
    checkpoint: Path,
    *,
    source_marker: str,
    source_path: Path,
    raw_frame: Any,
    true_histograms: list[Any],
    private_random_state: Any,
    private_entropy: bytes,
    seed_material: Any,
) -> dict[str, Any]:
    import joblib

    files = sorted(path for path in checkpoint.rglob("*") if path.is_file())
    file_records = [
        {
            "path": path.relative_to(checkpoint).as_posix(),
            "size_bytes": path.stat().st_size,
            "sha256": _sha256_file(path),
        }
        for path in files
    ]
    byte_patterns = {
        "source_marker_utf8": source_marker.encode("utf-8"),
        "source_path_utf8": str(source_path.resolve()).encode("utf-8"),
        "private_entropy_raw": private_entropy,
        "private_entropy_hex": private_entropy.hex().encode("ascii"),
        "private_seed_material_raw": seed_material.tobytes(),
    }
    byte_findings: list[dict[str, str]] = []
    for path in files:
        payload = path.read_bytes()
        for name, pattern in byte_patterns.items():
            if pattern and pattern in payload:
                byte_findings.append(
                    {
                        "code": "FORBIDDEN_BYTE_PATTERN",
                        "path": path.relative_to(checkpoint).as_posix(),
                        "pattern": name,
                    }
                )

    scanner = _CheckpointScanner(
        source_marker=source_marker,
        source_path=source_path,
        raw_frame=raw_frame,
        true_histograms=true_histograms,
        private_random_state=private_random_state,
    )
    load_errors: list[dict[str, str]] = []
    for path in files:
        relative = path.relative_to(checkpoint).as_posix()
        try:
            if path.suffix == ".joblib":
                loaded = joblib.load(path)
            elif path.suffix in {".pickle", ".pkl"}:
                with path.open("rb") as handle:
                    loaded = pickle.load(handle)
            else:
                continue
            scanner.visit(loaded, relative)
        except Exception as error:  # A generated local checkpoint is trusted input here.
            load_errors.append(
                {
                    "path": relative,
                    "error_type": type(error).__name__,
                    "message": str(error),
                }
            )

    findings = byte_findings + scanner.findings
    passed = not findings and not load_errors and not scanner.truncated
    return {
        "passed": passed,
        "checkpoint_files": file_records,
        "checkpoint_bytes": sum(item["size_bytes"] for item in file_records),
        "recursive_objects_inspected": scanner.nodes,
        "recursive_scan_truncated": scanner.truncated,
        "forbidden_findings": findings,
        "deserialization_errors": load_errors,
        "checks": {
            "no_source_path_or_marker": not any(
                item["code"] in {"SOURCE_PATH_OR_MARKER_SERIALIZED", "FORBIDDEN_BYTE_PATTERN"}
                and (
                    item.get("pattern", "").startswith("source_")
                    or item["code"] == "SOURCE_PATH_OR_MARKER_SERIALIZED"
                )
                for item in findings
            ),
            "no_raw_dataframe_or_rows": not any(
                item["code"] in {"DATAFRAME_SERIALIZED", "SERIES_SERIALIZED", "RAW_ROWS_SERIALIZED"}
                for item in findings
            ),
            "no_unnoised_measurements": not any(
                item["code"] == "UNNOISED_MEASUREMENT_SERIALIZED"
                for item in findings
            ),
            "no_private_rng_state_or_seed_pattern": not any(
                item["code"] == "RNG_STATE_SERIALIZED"
                or item.get("pattern", "").startswith("private_")
                for item in findings
            ),
            "recursive_schema_inspection_complete": not scanner.truncated
            and not load_errors,
        },
    }


def _validate_generated(frame: Any, domain: dict[str, int]) -> tuple[bool, dict[str, bool]]:
    import numpy as np

    checks: dict[str, bool] = {
        "column_order_exact": list(frame.columns) == list(domain),
        "no_nulls": not bool(frame.isna().any().any()),
        "integer_values": True,
        "values_in_public_domain": True,
    }
    if checks["column_order_exact"]:
        for column, cardinality in domain.items():
            values = frame[column].to_numpy()
            checks["integer_values"] = checks["integer_values"] and bool(
                np.equal(values, np.floor(values)).all()
            )
            checks["values_in_public_domain"] = checks[
                "values_in_public_domain"
            ] and bool(((values >= 0) & (values < cardinality)).all())
    else:
        checks["integer_values"] = False
        checks["values_in_public_domain"] = False
    return all(checks.values()), checks


def _fresh_sample_child(args: argparse.Namespace) -> int:
    output_path = Path(args.output).resolve()
    denied_source = Path(args.denied_source).resolve()
    checkpoint = Path(args.checkpoint).resolve()
    domain = json.loads(args.domain_json)
    opened_denied_source = False

    def deny_source_open(event: str, audit_args: tuple[Any, ...]) -> None:
        nonlocal opened_denied_source
        if event != "open" or not audit_args:
            return
        candidate = audit_args[0]
        if not isinstance(candidate, (str, bytes, os.PathLike)):
            return
        try:
            resolved = Path(os.fsdecode(candidate)).resolve()
        except (OSError, TypeError, ValueError):
            return
        if resolved == denied_source:
            opened_denied_source = True
            raise PermissionError("fresh sample process attempted to open denied source manifest")

    sys.addaudithook(deny_source_open)
    started = time.perf_counter()
    payload: dict[str, Any] = {
        "passed": False,
        "pid": os.getpid(),
        "source_manifest_opened": False,
        "source_dataframe_available": False,
        "batch_size": int(args.batch_size),
        "batch_count": int(args.batch_count),
    }
    try:
        import pandas as pd
        from dpmm.pipelines.base import GenerativePipeline

        pipeline = GenerativePipeline.load(checkpoint)
        batches: list[dict[str, Any]] = []
        total_rows = 0
        for batch_index in range(int(args.batch_count)):
            sample_seed = 1_048_583 + batch_index * 65_537
            generated = pipeline.generate(
                n_records=int(args.batch_size), random_state=sample_seed
            )
            valid, checks = _validate_generated(generated, domain)
            digest = _sha256_bytes(
                pd.util.hash_pandas_object(generated, index=False).values.tobytes()
            )
            rows = int(generated.shape[0])
            total_rows += rows
            batches.append(
                {
                    "index": batch_index,
                    "sampling_seed": sample_seed,
                    "rows": rows,
                    "columns": int(generated.shape[1]),
                    "content_digest": digest,
                    "validation": checks,
                    "passed": valid and rows == int(args.batch_size),
                }
            )
            del generated
        payload.update(
            {
                "batches": batches,
                "total_rows": total_rows,
                "exact_total": total_rows
                == int(args.batch_size) * int(args.batch_count),
                "distinct_batch_digests": len(
                    {item["content_digest"] for item in batches}
                )
                == len(batches),
                "source_manifest_opened": opened_denied_source,
                "passed": (
                    all(item["passed"] for item in batches)
                    and total_rows
                    == int(args.batch_size) * int(args.batch_count)
                    and not opened_denied_source
                ),
            }
        )
    except Exception as error:
        payload["error"] = {
            "type": type(error).__name__,
            "message": str(error).replace(str(checkpoint), "<checkpoint>"),
        }
        payload["source_manifest_opened"] = opened_denied_source
    payload["wall_seconds"] = time.perf_counter() - started
    payload["peak_rss_bytes"] = _rss_bytes()
    _atomic_json(output_path, payload)
    return 0 if payload["passed"] else 1


def _functional_gate(repo_root: Path) -> dict[str, Any]:
    import numpy as np
    from dpmm.pipelines.mst import MSTPipeline

    frame, domain, missing = _public_fixture()
    source_marker = f"STS-DENIED-SOURCE-{secrets.token_hex(16)}"
    fit_entropy = secrets.token_bytes(32)
    seed_material = np.frombuffer(fit_entropy, dtype="<u4").copy()
    private_rng = np.random.RandomState(seed_material)
    private_seed_commitment = _sha256_bytes(
        b"sts-dpmm-private-fit-seed-v1\0" + fit_entropy
    )
    baseline_rss = _rss_bytes()

    with tempfile.TemporaryDirectory(prefix="sts-dpmm-probe-") as temp_name:
        temp_root = Path(temp_name)
        source_manifest = temp_root / "source-manifest.json"
        source_manifest.write_text(
            json.dumps({"marker": source_marker, "downloadable": False}),
            encoding="utf-8",
        )
        checkpoint = temp_root / "checkpoint"
        child_result_path = temp_root / "fresh-sample-result.json"

        pipeline = MSTPipeline(
            epsilon=EPSILON,
            delta=DELTA,
            disable_processing=True,
            n_jobs=1,
            max_model_size=512,
        )
        fit_started = time.perf_counter()
        pipeline.fit(
            frame,
            domain=domain,
            random_state=private_rng,
            public=False,
        )
        fit_seconds = time.perf_counter() - fit_started
        fit_random_state = private_rng.get_state()
        generator = pipeline.gen.generator
        fit_checks = {
            "pipeline_is_mst": type(pipeline).__name__ == "MSTPipeline",
            "processing_disabled": pipeline.proc is None,
            "n_jobs_is_one": generator.n_jobs == 1,
            "public_argument_is_false": True,
            "fit_state_trained": generator.fit_state == "trained",
            "public_domain_exact": generator._domain == domain,
            "private_seed_not_disclosed": True,
        }
        true_histograms = _true_histograms(frame, generator.cliques, domain)

        store_started = time.perf_counter()
        pipeline.store(checkpoint)
        store_seconds = time.perf_counter() - store_started
        persist_checks = {
            "checkpoint_directory_exists": checkpoint.is_dir(),
            "state_file_exists": (checkpoint / "generative_model" / "state.joblib").is_file(),
            "estimator_file_exists": (
                checkpoint / "generative_model" / "estimator.pickle"
            ).is_file(),
            "source_manifest_outside_checkpoint": checkpoint not in source_manifest.parents,
        }
        audit = _checkpoint_audit(
            checkpoint,
            source_marker=source_marker,
            source_path=source_manifest,
            raw_frame=frame,
            true_histograms=true_histograms,
            private_random_state=fit_random_state,
            private_entropy=fit_entropy,
            seed_material=seed_material,
        )

        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--fresh-sample",
            "--checkpoint",
            str(checkpoint),
            "--output",
            str(child_result_path),
            "--denied-source",
            str(source_manifest),
            "--domain-json",
            json.dumps(domain),
            "--batch-size",
            str(BATCH_SIZE),
            "--batch-count",
            str(BATCH_COUNT),
        ]
        child_started = time.perf_counter()
        completed = subprocess.run(
            command,
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=300,
        )
        child_seconds = time.perf_counter() - child_started
        if child_result_path.is_file():
            child_result = json.loads(child_result_path.read_text(encoding="utf-8"))
        else:
            child_result = {
                "passed": False,
                "error": {
                    "type": "MissingChildResult",
                    "message": "fresh process did not atomically write its result file",
                },
            }
        child_result["exit_code"] = completed.returncode
        child_result["stdout_empty"] = not completed.stdout
        child_result["stderr_empty"] = not completed.stderr
        if completed.stderr:
            child_result["stderr_sha256"] = _sha256_bytes(completed.stderr)
        child_result["fresh_process_pid_differs"] = child_result.get("pid") != os.getpid()
        child_result["wall_seconds_parent_observed"] = child_seconds
        child_result["passed"] = bool(
            child_result.get("passed")
            and completed.returncode == 0
            and child_result["fresh_process_pid_differs"]
            and child_result.get("source_manifest_opened") is False
        )

        # Remove mutable secret material from the live NumPy buffer after all comparisons.
        seed_material.fill(0)

        fit = {
            "passed": all(fit_checks.values()),
            "checks": fit_checks,
            "fixture": {
                "public": True,
                "rows": int(frame.shape[0]),
                "columns": int(frame.shape[1]),
                "domain": domain,
                "missing_sentinel": missing,
            },
            "configuration": {
                "pipeline": "MSTPipeline",
                "epsilon": EPSILON,
                "delta": DELTA,
                "disable_processing": True,
                "n_jobs": 1,
                "max_model_size_mib": 512,
                "public": False,
            },
            "private_fit_rng": {
                "entropy_source": "secrets.token_bytes(32)",
                "numpy_initialization": "Seed material array passed to numpy.random.RandomState",
                "commitment": private_seed_commitment,
                "seed_disclosed": False,
            },
            "fit_wall_seconds": fit_seconds,
            "peak_rss_bytes": _rss_bytes(),
            "baseline_peak_rss_bytes": baseline_rss,
            "dpmm_reported_model_size": float(pipeline.model_size),
        }
        persist = {
            "passed": all(persist_checks.values()),
            "checks": persist_checks,
            "store_wall_seconds": store_seconds,
        }
        return {
            "passed": (
                fit["passed"]
                and persist["passed"]
                and child_result["passed"]
                and audit["passed"]
            ),
            "fit": fit,
            "persist": persist,
            "fresh_process_repeated_generation": child_result,
            "checkpoint_audit": audit,
        }


def _failed_gate(error: Exception, replacements: dict[str, str]) -> dict[str, Any]:
    return {
        "passed": False,
        "error": {
            "type": type(error).__name__,
            "message": _redact(str(error), replacements),
        },
    }


def _run_probe(repo_root: Path) -> dict[str, Any]:
    started = time.perf_counter()
    replacements = {str(repo_root): "<repo>"}
    result: dict[str, Any] = {
        "schema_version": PROBE_SCHEMA_VERSION,
        "probe": "dpmm_contract",
        "status": "failed",
        "formal_dp_enabled": False,
        "aim": {
            "enabled": False,
            "equivalent_gates_executed": False,
            "reason_code": "AIM_EQUIVALENT_GATES_NOT_RUN",
        },
    }

    gates: list[tuple[str, Callable[[], dict[str, Any]]]] = [
        ("environment", lambda: _lock_and_import_gate(repo_root)),
        ("source_audit", _source_audit),
        ("accounting_audit", _accounting_audit),
        ("state_estimates", _state_estimate_gate),
        ("functional", lambda: _functional_gate(repo_root)),
    ]
    for name, gate in gates:
        try:
            result[name] = gate()
        except Exception as error:
            result[name] = _failed_gate(error, replacements)

    formal_gate_status = {
        "environment": bool(result["environment"].get("passed")),
        "public_false_source_audit": bool(result["source_audit"].get("passed")),
        "add_remove_accounting": bool(result["accounting_audit"].get("passed")),
        "conservative_state_estimates": bool(result["state_estimates"].get("passed")),
        "fit": bool(result["functional"].get("fit", {}).get("passed")),
        "persist": bool(result["functional"].get("persist", {}).get("passed")),
        "fresh_process_repeated_sample": bool(
            result["functional"]
            .get("fresh_process_repeated_generation", {})
            .get("passed")
        ),
        "checkpoint_schema_and_secret_audit": bool(
            result["functional"].get("checkpoint_audit", {}).get("passed")
        ),
    }
    result["formal_dp_gate"] = formal_gate_status
    result["formal_dp_enabled"] = all(formal_gate_status.values())
    result["failure_reasons"] = [
        name for name, passed in formal_gate_status.items() if not passed
    ]
    result["status"] = "passed" if result["formal_dp_enabled"] else "failed"
    result["wall_seconds"] = time.perf_counter() - started
    result["peak_rss_bytes"] = _rss_bytes()
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fresh-sample", action="store_true")
    parser.add_argument("--checkpoint")
    parser.add_argument("--output")
    parser.add_argument("--denied-source")
    parser.add_argument("--domain-json")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--batch-count", type=int, default=BATCH_COUNT)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.fresh_sample:
        required = (
            args.checkpoint,
            args.output,
            args.denied_source,
            args.domain_json,
        )
        if not all(required):
            raise SystemExit("fresh sample mode requires checkpoint/output/source/domain")
        return _fresh_sample_child(args)

    repo_root = Path(__file__).resolve().parents[1]
    result_path = repo_root / "probes" / "results" / "dpmm_contract.json"
    result = _run_probe(repo_root)
    _atomic_json(result_path, result)
    print(
        f"dpmm_contract status={result['status']} "
        f"formal_dp_enabled={str(result['formal_dp_enabled']).lower()} "
        f"result={result_path}"
    )
    return 0 if result["formal_dp_enabled"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
