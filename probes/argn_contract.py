#!/usr/bin/env python3
"""Real compatibility probe for the pinned MOSTLY AI TabularARGN engine.

The parent process owns fixture construction and fitting. Generation is always
performed by a newly started interpreter that receives only a workspace path,
seed, and bounded sample request in a JSON file. Stdout is intentionally not a
protocol surface; child results are written atomically to JSON files.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import hashlib
import hmac
import importlib.metadata
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

PROBE_SCHEMA_VERSION = "1.0"
ENGINE_VERSION = "2.6.2"
MAX_EPOCHS = 2
SCRIPT_PATH = Path(__file__).resolve()
REPOSITORY_ROOT = SCRIPT_PATH.parents[1]
ARGN_PROJECT_ROOT = REPOSITORY_ROOT / "workers" / "argn"
DEFAULT_RESULT_PATH = SCRIPT_PATH.parent / "results" / "argn_contract.json"
CHECKPOINT_EXCLUDED_ROOTS = {"SyntheticData"}


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".part")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _exception_record(stage: str, error: BaseException) -> dict[str, Any]:
    return {
        "stage": stage,
        "type": type(error).__name__,
        "message": str(error),
        "traceback": traceback.format_exc().splitlines()[-30:],
    }


def _path_digest(root: Path, *, checkpoint_only: bool = False) -> dict[str, Any]:
    digest = hashlib.sha256()
    file_count = 0
    total_bytes = 0
    entries: list[dict[str, Any]] = []
    if not root.exists():
        return {
            "sha256": digest.hexdigest(),
            "file_count": 0,
            "bytes": 0,
            "files": [],
        }
    for path in sorted((item for item in root.rglob("*") if item.is_file()), key=lambda item: item.as_posix()):
        relative = path.relative_to(root)
        if checkpoint_only and relative.parts and relative.parts[0] in CHECKPOINT_EXCLUDED_ROOTS:
            continue
        file_hash = hashlib.sha256()
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                file_hash.update(chunk)
        size = path.stat().st_size
        relative_bytes = relative.as_posix().encode("utf-8")
        digest.update(len(relative_bytes).to_bytes(8, "big"))
        digest.update(relative_bytes)
        digest.update(size.to_bytes(8, "big"))
        digest.update(bytes.fromhex(file_hash.hexdigest()))
        file_count += 1
        total_bytes += size
        entries.append({"path": relative.as_posix(), "size_bytes": size, "sha256": file_hash.hexdigest()})
    return {
        "sha256": digest.hexdigest(),
        "file_count": file_count,
        "bytes": total_bytes,
        "files": entries,
    }


def _directory_size(root: Path) -> dict[str, int]:
    file_count = 0
    total_bytes = 0
    if root.exists():
        for path in root.rglob("*"):
            if path.is_file():
                file_count += 1
                total_bytes += path.stat().st_size
    return {"file_count": file_count, "bytes": total_bytes}


def _distribution_identity(distribution_name: str) -> dict[str, Any]:
    identity: dict[str, Any] = {"distribution": distribution_name}
    try:
        distribution = importlib.metadata.distribution(distribution_name)
        identity.update(
            {
                "installed_version": distribution.version,
                "metadata_name": distribution.metadata.get("Name"),
                "license": distribution.metadata.get("License"),
                "location": str(distribution.locate_file("")),
                "requires_dist": distribution.metadata.get_all("Requires-Dist") or [],
            }
        )
        installed_digest = hashlib.sha256()
        installed_files = 0
        installed_bytes = 0
        for relative in sorted(distribution.files or [], key=lambda item: str(item)):
            path = distribution.locate_file(relative)
            if not path.is_file():
                continue
            file_hash = hashlib.sha256()
            try:
                with path.open("rb") as handle:
                    while chunk := handle.read(1024 * 1024):
                        file_hash.update(chunk)
            except OSError:
                continue
            relative_bytes = str(relative).encode("utf-8")
            size = path.stat().st_size
            installed_digest.update(len(relative_bytes).to_bytes(8, "big"))
            installed_digest.update(relative_bytes)
            installed_digest.update(size.to_bytes(8, "big"))
            installed_digest.update(bytes.fromhex(file_hash.hexdigest()))
            installed_files += 1
            installed_bytes += size
        identity["installed_distribution"] = {
            "sha256": installed_digest.hexdigest(),
            "file_count": installed_files,
            "bytes": installed_bytes,
        }
    except BaseException as error:
        identity["installed_metadata_error"] = {"type": type(error).__name__, "message": str(error)}

    lock_path = ARGN_PROJECT_ROOT / "uv.lock"
    identity["lock_path"] = str(lock_path)
    if lock_path.exists():
        identity["lock_sha256"] = hashlib.sha256(lock_path.read_bytes()).hexdigest()
        try:
            import tomllib

            lock = tomllib.loads(lock_path.read_text(encoding="utf-8"))
            package = next(
                item
                for item in lock.get("package", [])
                if item.get("name") == distribution_name and item.get("version") == ENGINE_VERSION
            )
            identity["locked_source"] = package.get("source")
            identity["locked_sdist"] = package.get("sdist")
            identity["locked_wheels"] = package.get("wheels", [])
        except BaseException as error:
            identity["lock_parse_error"] = {"type": type(error).__name__, "message": str(error)}
    else:
        identity["lock_error"] = "uv.lock not found"
    return identity


def _process_tree_rss_bytes(process: Any) -> int:
    try:
        import psutil

        total = process.memory_info().rss
        for child in process.children(recursive=True):
            with contextlib.suppress(psutil.Error):
                total += child.memory_info().rss
        return total
    except BaseException:
        return 0


class PeakRssSampler:
    def __init__(self, interval_seconds: float = 0.05) -> None:
        self.interval_seconds = interval_seconds
        self.baseline_bytes = 0
        self.peak_bytes = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._process: Any = None

    def __enter__(self) -> "PeakRssSampler":
        import psutil

        self._process = psutil.Process(os.getpid())
        self.baseline_bytes = _process_tree_rss_bytes(self._process)
        self.peak_bytes = self.baseline_bytes
        self._thread = threading.Thread(target=self._sample, name="argn-rss-sampler", daemon=True)
        self._thread.start()
        return self

    def _sample(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self.peak_bytes = max(self.peak_bytes, _process_tree_rss_bytes(self._process))

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.peak_bytes = max(self.peak_bytes, _process_tree_rss_bytes(self._process))
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1)

    def result(self) -> dict[str, int]:
        return {
            "baseline_bytes": self.baseline_bytes,
            "peak_process_tree_rss_bytes": self.peak_bytes,
            "peak_increase_bytes": max(0, self.peak_bytes - self.baseline_bytes),
        }


def _measure_stage(name: str, stages: dict[str, Any], operation: Callable[[], Any]) -> Any:
    started = time.perf_counter()
    try:
        with PeakRssSampler() as sampler:
            value = operation()
    except BaseException:
        wall = time.perf_counter() - started
        rss = sampler.result() if "sampler" in locals() else {}
        stages[name] = {"status": "failed", "wall_seconds": wall, "rss": rss}
        raise
    wall = time.perf_counter() - started
    stages[name] = {"status": "passed", "wall_seconds": wall, "rss": sampler.result()}
    return value


def _mixed_fixture(row_count: int, seed: int) -> Any:
    import numpy as np
    import pandas as pd

    rng = np.random.default_rng(seed)
    segment_values = np.array(["consumer", "business", "public", "nonprofit"], dtype=object)
    region_values = np.array(["north", "south", "east", "west", "central"], dtype=object)
    segments = segment_values[rng.integers(0, len(segment_values), size=row_count)]
    regions = region_values[rng.integers(0, len(region_values), size=row_count)]
    ages = np.clip(rng.normal(43, 14, size=row_count).round(), 18, 90).astype("int64")
    income = np.exp(rng.normal(10.7, 0.55, size=row_count))
    income *= np.where(segments == "business", 1.35, 1.0)
    active = rng.random(row_count) < np.where(segments == "consumer", 0.72, 0.61)
    start = np.datetime64("2021-01-01T00:00:00")
    event_at = start + rng.integers(0, 365 * 3 * 24 * 60, size=row_count).astype("timedelta64[m]")

    age_series = pd.Series(ages, dtype="Int64")
    income_series = pd.Series(income.round(2), dtype="Float64")
    segment_series = pd.Series(segments, dtype="string")
    region_series = pd.Series(regions, dtype="category")
    active_series = pd.Series(active, dtype="boolean")
    event_series = pd.Series(event_at).astype("datetime64[ns]")

    age_series.iloc[rng.choice(row_count, size=max(1, row_count // 31), replace=False)] = pd.NA
    income_series.iloc[rng.choice(row_count, size=max(1, row_count // 19), replace=False)] = pd.NA
    segment_series.iloc[rng.choice(row_count, size=max(1, row_count // 37), replace=False)] = pd.NA
    region_series.iloc[rng.choice(row_count, size=max(1, row_count // 43), replace=False)] = pd.NA
    active_series.iloc[rng.choice(row_count, size=max(1, row_count // 29), replace=False)] = pd.NA
    event_series.iloc[rng.choice(row_count, size=max(1, row_count // 23), replace=False)] = pd.NaT

    return pd.DataFrame(
        {
            "age": age_series,
            "income": income_series,
            "segment": segment_series,
            "region": region_series,
            "is_active": active_series,
            "event_at": event_series,
        }
    )


def _dataframe_digest(frame: Any) -> str:
    import pandas as pd

    hashed = pd.util.hash_pandas_object(frame, index=True, categorize=True)
    digest = hashlib.sha256()
    digest.update("|".join(str(dtype) for dtype in frame.dtypes).encode("utf-8"))
    digest.update(hashed.to_numpy(copy=False).tobytes())
    return digest.hexdigest()


def _deterministic_split(keys: Any) -> tuple[Any, Any]:
    import numpy as np

    selected = np.fromiter(
        (int.from_bytes(hashlib.blake2s(str(key).encode("utf-8"), digest_size=4).digest(), "big") % 10 != 0 for key in keys),
        dtype=bool,
        count=len(keys),
    )
    return keys[selected], keys[~selected]


def _split_check(row_count: int) -> dict[str, Any]:
    import pandas as pd

    keys = pd.Series(range(row_count))
    first_train, first_validation = _deterministic_split(keys)
    second_train, second_validation = _deterministic_split(keys)
    train_set = set(first_train.tolist())
    validation_set = set(first_validation.tolist())
    return {
        "reproducible": first_train.tolist() == second_train.tolist()
        and first_validation.tolist() == second_validation.tolist(),
        "disjoint": train_set.isdisjoint(validation_set),
        "complete": len(train_set | validation_set) == row_count,
        "training_rows": len(first_train),
        "validation_rows": len(first_validation),
        "training_sha256": hashlib.sha256(first_train.to_numpy().tobytes()).hexdigest(),
        "validation_sha256": hashlib.sha256(first_validation.to_numpy().tobytes()).hexdigest(),
    }


def _derived_seed(master_seed: int, purpose: str, index: int) -> int:
    key = master_seed.to_bytes(16, "big", signed=False)
    message = f"argn-contract/{purpose}/{index}".encode("utf-8")
    return int.from_bytes(hmac.new(key, message, hashlib.sha256).digest()[:4], "big")


def _seed_checks(master_seed: int, fixture_rows: int) -> dict[str, Any]:
    fixture_one = _mixed_fixture(fixture_rows, master_seed)
    fixture_two = _mixed_fixture(fixture_rows, master_seed)
    digest_one = _dataframe_digest(fixture_one)
    digest_two = _dataframe_digest(fixture_two)
    seeds_one = [_derived_seed(master_seed, "generation-shard", index) for index in range(7)]
    seeds_two = [_derived_seed(master_seed, "generation-shard", index) for index in range(7)]
    return {
        "fixture": {
            "first_sha256": digest_one,
            "second_sha256": digest_two,
            "reproducible": digest_one == digest_two,
        },
        "split": _split_check(fixture_rows),
        "derived_32bit_seeds": {
            "values": seeds_one,
            "within_uint32": all(0 <= seed <= 0xFFFFFFFF for seed in seeds_one),
            "reproducible": seeds_one == seeds_two,
            "collision_free": len(set(seeds_one)) == len(seeds_one),
        },
    }


def _parquet_rows(root: Path, suffix: str | None = None) -> int:
    import pyarrow.parquet as pq

    total = 0
    for path in root.rglob("*.parquet"):
        if suffix is None or path.name.endswith(suffix):
            total += pq.ParquetFile(path).metadata.num_rows
    return total


def _output_evidence(synthetic_data: Path) -> dict[str, Any]:
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.ipc as ipc

    files = sorted(synthetic_data.rglob("*.parquet")) if synthetic_data.exists() else []
    if not files:
        raise RuntimeError(f"generate produced no Parquet files under {synthetic_data}")
    table = ds.dataset([str(path) for path in files], format="parquet").to_table()
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    content_sha256 = hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()
    frame = table.to_pandas()
    columns: dict[str, Any] = {}
    for name in frame.columns:
        series = frame[name]
        column: dict[str, Any] = {
            "dtype": str(series.dtype),
            "null_count": int(series.isna().sum()),
            "null_rate": float(series.isna().mean()),
            "unique_nonnull": int(series.nunique(dropna=True)),
        }
        if str(series.dtype).startswith(("int", "Int", "float", "Float")):
            numeric = series.dropna().astype("float64")
            if len(numeric):
                column["numeric"] = {
                    "mean": float(numeric.mean()),
                    "std": float(numeric.std(ddof=0)),
                    "min": float(numeric.min()),
                    "max": float(numeric.max()),
                }
        elif str(series.dtype).startswith("datetime"):
            values = series.dropna().astype("int64")
            if len(values):
                column["numeric"] = {
                    "mean": float(values.mean()),
                    "std": float(values.std(ddof=0)),
                    "min": float(values.min()),
                    "max": float(values.max()),
                }
        else:
            counts = series.fillna("__NULL__").astype(str).value_counts(normalize=True, dropna=False)
            column["frequencies"] = {str(key): float(value) for key, value in counts.items()}
        columns[str(name)] = column
    parquet_digest = _path_digest(synthetic_data)
    return {
        "rows": table.num_rows,
        "columns": table.num_columns,
        "column_order": list(table.column_names),
        "arrow_schema": str(table.schema),
        "content_sha256": content_sha256,
        "parquet": parquet_digest,
        "summary": columns,
        "python_candidate_dataframe_rows": len(frame),
        "python_candidate_dataframe_deep_bytes": int(frame.memory_usage(index=True, deep=True).sum()),
    }


def _child_generate(request_path: Path, result_path: Path) -> int:
    started_at = _utc_now()
    started = time.perf_counter()
    result: dict[str, Any] = {
        "schema_version": PROBE_SCHEMA_VERSION,
        "operation": "fresh_process_generate",
        "started_at": started_at,
        "pid": os.getpid(),
        "parent_pid": os.getppid(),
        "status": "failed",
        "source_dataframe_materialized": False,
        "exceptions": [],
    }
    exit_code = 1
    try:
        request = json.loads(request_path.read_text(encoding="utf-8"))
        allowed_keys = {"workspace", "seed", "device", "sample_size", "batch_size"}
        unexpected = sorted(set(request) - allowed_keys)
        if unexpected:
            raise ValueError(f"child request contains forbidden/unexpected keys: {unexpected}")
        workspace = Path(request["workspace"]).resolve()
        if not workspace.is_dir():
            raise FileNotFoundError(f"workspace does not exist: {workspace}")
        synthetic_data = workspace / "SyntheticData"
        if synthetic_data.exists():
            shutil.rmtree(synthetic_data)
        checkpoint_before = _path_digest(workspace, checkpoint_only=True)

        from mostlyai import engine
        from mostlyai.engine.random_state import set_random_state

        set_random_state(int(request["seed"]))
        with PeakRssSampler() as sampler:
            engine.generate(
                sample_size=int(request["sample_size"]),
                batch_size=int(request["batch_size"]),
                device=str(request["device"]),
                workspace_dir=workspace,
            )
        evidence = _output_evidence(synthetic_data)
        checkpoint_after = _path_digest(workspace, checkpoint_only=True)
        result.update(
            {
                "status": "passed",
                "request": {
                    "seed": int(request["seed"]),
                    "device": str(request["device"]),
                    "sample_size": int(request["sample_size"]),
                    "batch_size": int(request["batch_size"]),
                },
                "output": evidence,
                "rss": sampler.result(),
                "checkpoint_checksum_before": checkpoint_before,
                "checkpoint_checksum_after": checkpoint_after,
                "checkpoint_files_unchanged": checkpoint_before["sha256"] == checkpoint_after["sha256"],
            }
        )
        exit_code = 0
    except BaseException as error:
        result["exceptions"].append(_exception_record("fresh_process_generate", error))
    finally:
        result["completed_at"] = _utc_now()
        result["wall_seconds"] = time.perf_counter() - started
        try:
            _atomic_json(result_path, result)
        except BaseException:
            traceback.print_exc(file=sys.stderr)
            return 2
    return exit_code


def _tail(path: Path, max_bytes: int = 16_384) -> str:
    if not path.exists():
        return ""
    with path.open("rb") as handle:
        size = path.stat().st_size
        if size > max_bytes:
            handle.seek(size - max_bytes)
        return handle.read().decode("utf-8", errors="replace")


def _run_generation_child(
    source_workspace: Path,
    clone_parent: Path,
    label: str,
    seed: int,
    device: str,
    sample_size: int,
    batch_size: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    import psutil

    clone = clone_parent / f"workspace-{label}"
    shutil.copytree(source_workspace, clone)
    request_path = clone_parent / f"request-{label}.json"
    result_path = clone_parent / f"result-{label}.json"
    stdout_path = clone_parent / f"stdout-{label}.log"
    stderr_path = clone_parent / f"stderr-{label}.log"
    _atomic_json(
        request_path,
        {
            "workspace": str(clone),
            "seed": seed,
            "device": device,
            "sample_size": sample_size,
            "batch_size": batch_size,
        },
    )
    command = [
        sys.executable,
        str(SCRIPT_PATH),
        "--child-generate",
        "--child-request",
        str(request_path),
        "--child-result",
        str(result_path),
    ]
    started = time.perf_counter()
    timed_out = False
    peak_rss = 0
    with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
        process = subprocess.Popen(command, stdout=stdout_handle, stderr=stderr_handle, cwd=REPOSITORY_ROOT)
        ps_process = psutil.Process(process.pid)
        deadline = time.monotonic() + timeout_seconds
        while process.poll() is None:
            peak_rss = max(peak_rss, _process_tree_rss_bytes(ps_process))
            if time.monotonic() >= deadline:
                timed_out = True
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                break
            time.sleep(0.05)
        return_code = process.wait()
        with contextlib.suppress(BaseException):
            peak_rss = max(peak_rss, _process_tree_rss_bytes(ps_process))
    result: dict[str, Any]
    if result_path.exists():
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except BaseException as error:
            result = {
                "status": "failed",
                "exceptions": [_exception_record("read_child_result", error)],
            }
    else:
        result = {
            "status": "failed",
            "exceptions": [
                {
                    "stage": "fresh_process_generate",
                    "type": "ChildProcessFailure",
                    "message": "fresh generation process exited without a result JSON",
                    "traceback": [],
                }
            ],
        }
    result["supervisor"] = {
        "label": label,
        "return_code": return_code,
        "timed_out": timed_out,
        "wall_seconds": time.perf_counter() - started,
        "observed_peak_process_tree_rss_bytes": peak_rss,
        "stdout_tail": _tail(stdout_path),
        "stderr_tail": _tail(stderr_path),
    }
    result["clone_workspace"] = str(clone)
    return result


def _compare_generation_summaries(cpu: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    cpu_output = cpu.get("output", {})
    candidate_output = candidate.get("output", {})
    comparison: dict[str, Any] = {
        "thresholds": {
            "maximum_null_rate_difference": 0.02,
            "maximum_categorical_tvd": 0.10,
            "maximum_standardized_numeric_mean_difference": 0.15,
            "maximum_numeric_std_ratio_difference": 0.25,
        },
        "same_rows": cpu_output.get("rows") == candidate_output.get("rows"),
        "same_column_order": cpu_output.get("column_order") == candidate_output.get("column_order"),
        "same_arrow_schema": cpu_output.get("arrow_schema") == candidate_output.get("arrow_schema"),
        "columns": {},
    }
    all_within = True
    cpu_columns = cpu_output.get("summary", {})
    candidate_columns = candidate_output.get("summary", {})
    if set(cpu_columns) != set(candidate_columns):
        all_within = False
    for name in sorted(set(cpu_columns) & set(candidate_columns)):
        left = cpu_columns[name]
        right = candidate_columns[name]
        null_difference = abs(float(left["null_rate"]) - float(right["null_rate"]))
        column: dict[str, Any] = {
            "null_rate_difference": null_difference,
            "null_rate_within_threshold": null_difference <= 0.02,
        }
        within = column["null_rate_within_threshold"]
        if "frequencies" in left and "frequencies" in right:
            categories = set(left["frequencies"]) | set(right["frequencies"])
            tvd = 0.5 * sum(
                abs(float(left["frequencies"].get(category, 0.0)) - float(right["frequencies"].get(category, 0.0)))
                for category in categories
            )
            column["categorical_tvd"] = tvd
            column["categorical_tvd_within_threshold"] = tvd <= 0.10
            within = within and column["categorical_tvd_within_threshold"]
        elif "numeric" in left and "numeric" in right:
            left_numeric = left["numeric"]
            right_numeric = right["numeric"]
            scale = max(abs(float(left_numeric["std"])), 1.0)
            standardized_mean_difference = abs(float(left_numeric["mean"]) - float(right_numeric["mean"])) / scale
            left_std = abs(float(left_numeric["std"]))
            right_std = abs(float(right_numeric["std"]))
            std_ratio_difference = abs(left_std - right_std) / max(left_std, 1.0)
            column.update(
                {
                    "standardized_numeric_mean_difference": standardized_mean_difference,
                    "standardized_numeric_mean_within_threshold": standardized_mean_difference <= 0.15,
                    "numeric_std_ratio_difference": std_ratio_difference,
                    "numeric_std_ratio_within_threshold": std_ratio_difference <= 0.25,
                }
            )
            within = (
                within
                and column["standardized_numeric_mean_within_threshold"]
                and column["numeric_std_ratio_within_threshold"]
            )
        column["within_thresholds"] = within
        all_within = all_within and within
        comparison["columns"][name] = column
    comparison["passed"] = bool(
        cpu.get("status") == "passed"
        and candidate.get("status") == "passed"
        and comparison["same_rows"]
        and comparison["same_column_order"]
        and comparison["same_arrow_schema"]
        and all_within
    )
    return comparison


def _fit_workspace(
    workspace: Path,
    fixture_rows: int,
    fixture_seed: int,
    model: str,
    train_device: str,
    stages: dict[str, Any],
) -> dict[str, Any]:
    from mostlyai import engine
    from mostlyai.engine.random_state import set_random_state

    frame = _mixed_fixture(fixture_rows, fixture_seed)
    frame_metrics = {
        "rows": len(frame),
        "columns": len(frame.columns),
        "column_order": list(frame.columns),
        "dtypes": {str(name): str(dtype) for name, dtype in frame.dtypes.items()},
        "null_counts": {str(name): int(value) for name, value in frame.isna().sum().items()},
        "deep_bytes": int(frame.memory_usage(index=True, deep=True).sum()),
        "sha256": _dataframe_digest(frame),
    }
    set_random_state(fixture_seed)
    _measure_stage(
        "split",
        stages,
        lambda: engine.split(tgt_data=frame, trn_val_split=_deterministic_split, workspace_dir=workspace),
    )
    split_rows = {
        "training": _parquet_rows(workspace / "OriginalData", "-trn.parquet"),
        "validation": _parquet_rows(workspace / "OriginalData", "-val.parquet"),
    }
    _measure_stage("analyze", stages, lambda: engine.analyze(workspace_dir=workspace))
    _measure_stage("encode", stages, lambda: engine.encode(workspace_dir=workspace))
    set_random_state(fixture_seed)
    _measure_stage(
        "train",
        stages,
        lambda: engine.train(
            model=model,
            max_epochs=MAX_EPOCHS,
            device=train_device,
            workspace_dir=workspace,
        ),
    )
    checkpoint = _path_digest(workspace, checkpoint_only=True)
    return {
        "fixture": frame_metrics,
        "split_rows": split_rows,
        "checkpoint": checkpoint,
        "model_store": _directory_size(workspace / "ModelStore"),
        "encoded_data": _directory_size(workspace / "EncodedData"),
        "maximum_materialized_python_dataframe": {
            "kind": "bounded_training_dataframe",
            "rows": len(frame),
            "deep_bytes": frame_metrics["deep_bytes"],
        },
    }


def _child_fit(request_path: Path, result_path: Path) -> int:
    started = time.perf_counter()
    result: dict[str, Any] = {
        "schema_version": PROBE_SCHEMA_VERSION,
        "operation": "fit_checkpoint",
        "started_at": _utc_now(),
        "pid": os.getpid(),
        "parent_pid": os.getppid(),
        "status": "failed",
        "exceptions": [],
    }
    exit_code = 1
    try:
        request = json.loads(request_path.read_text(encoding="utf-8"))
        allowed_keys = {"workspace", "fixture_rows", "fixture_seed", "model", "train_device"}
        unexpected = sorted(set(request) - allowed_keys)
        if unexpected:
            raise ValueError(f"fit child request contains forbidden/unexpected keys: {unexpected}")
        workspace = Path(request["workspace"]).resolve()
        workspace.mkdir(parents=True, exist_ok=False)
        stages: dict[str, Any] = {}
        with PeakRssSampler() as sampler:
            evidence = _fit_workspace(
                workspace=workspace,
                fixture_rows=int(request["fixture_rows"]),
                fixture_seed=int(request["fixture_seed"]),
                model=str(request["model"]),
                train_device=str(request["train_device"]),
                stages=stages,
            )
        result.update(
            {
                "status": "passed",
                "request": {
                    "fixture_rows": int(request["fixture_rows"]),
                    "fixture_seed": int(request["fixture_seed"]),
                    "model": str(request["model"]),
                    "train_device": str(request["train_device"]),
                },
                "stages": stages,
                "evidence": evidence,
                "rss": sampler.result(),
            }
        )
        exit_code = 0
    except BaseException as error:
        result["exceptions"].append(_exception_record("fit_checkpoint", error))
    finally:
        result["completed_at"] = _utc_now()
        result["wall_seconds"] = time.perf_counter() - started
        try:
            _atomic_json(result_path, result)
        except BaseException:
            traceback.print_exc(file=sys.stderr)
            return 2
    return exit_code


def _run_fit_child(
    work_root: Path,
    workspace: Path,
    fixture_rows: int,
    fixture_seed: int,
    model: str,
    train_device: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    import psutil

    request_path = work_root / f"fit-request-{fixture_rows}.json"
    result_path = work_root / f"fit-result-{fixture_rows}.json"
    stdout_path = work_root / f"fit-stdout-{fixture_rows}.log"
    stderr_path = work_root / f"fit-stderr-{fixture_rows}.log"
    _atomic_json(
        request_path,
        {
            "workspace": str(workspace),
            "fixture_rows": fixture_rows,
            "fixture_seed": fixture_seed,
            "model": model,
            "train_device": train_device,
        },
    )
    command = [
        sys.executable,
        str(SCRIPT_PATH),
        "--child-fit",
        "--child-request",
        str(request_path),
        "--child-result",
        str(result_path),
    ]
    started = time.perf_counter()
    timed_out = False
    peak_rss = 0
    with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
        process = subprocess.Popen(command, stdout=stdout_handle, stderr=stderr_handle, cwd=REPOSITORY_ROOT)
        ps_process = psutil.Process(process.pid)
        deadline = time.monotonic() + timeout_seconds
        while process.poll() is None:
            peak_rss = max(peak_rss, _process_tree_rss_bytes(ps_process))
            if time.monotonic() >= deadline:
                timed_out = True
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                break
            time.sleep(0.05)
        return_code = process.wait()
    if result_path.exists():
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except BaseException as error:
            result = {
                "status": "failed",
                "exceptions": [_exception_record("read_fit_child_result", error)],
            }
    else:
        result = {
            "status": "failed",
            "exceptions": [
                {
                    "stage": "fit_checkpoint",
                    "type": "ChildProcessFailure",
                    "message": "fit process exited without a result JSON",
                    "traceback": [],
                }
            ],
        }
    result["supervisor"] = {
        "return_code": return_code,
        "timed_out": timed_out,
        "wall_seconds": time.perf_counter() - started,
        "observed_peak_process_tree_rss_bytes": peak_rss,
        "process_exited_before_generation": process.poll() is not None,
        "stdout_tail": _tail(stdout_path),
        "stderr_tail": _tail(stderr_path),
    }
    return result


def _cuda_information() -> dict[str, Any]:
    try:
        import torch

        count = torch.cuda.device_count() if torch.cuda.is_available() else 0
        return {
            "torch_cuda_available": bool(torch.cuda.is_available()),
            "cuda_device_count": count,
            "devices": [torch.cuda.get_device_name(index) for index in range(count)],
        }
    except BaseException as error:
        return {
            "torch_cuda_available": False,
            "cuda_device_count": 0,
            "devices": [],
            "error": {"type": type(error).__name__, "message": str(error)},
        }


def _mps_information() -> dict[str, Any]:
    try:
        import torch

        built = bool(torch.backends.mps.is_built())
        available = bool(torch.backends.mps.is_available())
        return {"torch_mps_built": built, "torch_mps_available": available}
    except BaseException as error:
        return {
            "torch_mps_built": False,
            "torch_mps_available": False,
            "error": {"type": type(error).__name__, "message": str(error)},
        }


def _run_multiprocess_clone_probe(
    workspace: Path,
    work_root: Path,
    counts: list[int],
    cuda: dict[str, Any],
    master_seed: int,
    sample_size: int,
    batch_size: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {"requested_process_counts": counts, "levels": []}
    available = int(cuda.get("cuda_device_count", 0))
    all_passed = True
    any_attempted = False
    original_before = _path_digest(workspace)
    for process_count in counts:
        if process_count > available:
            evidence["levels"].append(
                {
                    "process_count": process_count,
                    "attempted": False,
                    "passed": False,
                    "reason": f"requires {process_count} CUDA devices; observed {available}",
                }
            )
            all_passed = False
            continue
        any_attempted = True
        level_root = work_root / f"cuda-clones-{process_count}"
        level_root.mkdir(parents=True, exist_ok=True)
        seeds = [_derived_seed(master_seed, f"cuda-{process_count}", index) for index in range(process_count)]
        specifications = [
            (workspace, level_root, f"rank-{index}", seeds[index], f"cuda:{index}", sample_size, batch_size, timeout_seconds)
            for index in range(process_count)
        ]
        with ThreadPoolExecutor(max_workers=process_count) as executor:
            results = list(executor.map(lambda values: _run_generation_child(*values), specifications))
        exact_sum = sum(int(item.get("output", {}).get("rows", 0)) for item in results) == process_count * sample_size
        checkpoints_unchanged = all(bool(item.get("checkpoint_files_unchanged")) for item in results)
        no_contention = all(item.get("status") == "passed" for item in results) and checkpoints_unchanged
        passed = bool(
            len(set(seeds)) == len(seeds)
            and exact_sum
            and no_contention
            and _path_digest(workspace)["sha256"] == original_before["sha256"]
        )
        all_passed = all_passed and passed
        evidence["levels"].append(
            {
                "process_count": process_count,
                "attempted": True,
                "passed": passed,
                "derived_seeds": seeds,
                "seeds_disjoint": len(set(seeds)) == len(seeds),
                "exact_row_sum": exact_sum,
                "checkpoint_files_unchanged": checkpoints_unchanged,
                "write_contention_absent": no_contention,
                "results": results,
            }
        )
    original_after = _path_digest(workspace)
    evidence["original_workspace_checksum_before"] = original_before
    evidence["original_workspace_checksum_after"] = original_after
    evidence["original_workspace_unchanged"] = original_before["sha256"] == original_after["sha256"]
    evidence["passed"] = bool(any_attempted and all_passed and evidence["original_workspace_unchanged"])
    if not any_attempted:
        evidence["disabled_reason"] = f"no requested process level is supported by {available} observed CUDA devices"
    elif not evidence["passed"]:
        evidence["disabled_reason"] = "one or more requested 1/2/4 CUDA clone levels failed or were unavailable"
    return evidence


def _parse_int_list(value: str) -> list[int]:
    values = [int(item.strip()) for item in value.split(",") if item.strip()]
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError("expected a comma-separated list of positive integers")
    return values


def _environment_versions() -> dict[str, Any]:
    packages = ["mostlyai-engine", "numpy", "pandas", "pyarrow", "torch", "psutil"]
    versions: dict[str, Any] = {}
    for name in packages:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = None
    return {
        "python": sys.version,
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "packages": versions,
        "cuda": _cuda_information(),
        "mps": _mps_information(),
    }


def _probe(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    started_at = _utc_now()
    started = time.perf_counter()
    exceptions: list[dict[str, Any]] = []
    result: dict[str, Any] = {
        "schema_version": PROBE_SCHEMA_VERSION,
        "probe": "argn_contract",
        "started_at": started_at,
        "status": "failed",
        "config": {
            "fixture_rows": args.fixture_rows,
            "generation_rows": args.generation_rows,
            "generation_batch_size": args.generation_batch_size,
            "max_epochs": MAX_EPOCHS,
            "model": args.model,
            "train_device": args.train_device,
            "master_seed": args.master_seed,
            "clone_process_counts": args.clone_process_counts,
            "run_staircase": args.run_staircase,
            "staircase_fit_rows": args.staircase_fit_rows,
            "staircase_generation_rows": args.staircase_generation_rows,
            "child_timeout_seconds": args.child_timeout_seconds,
        },
        "exceptions": exceptions,
        "feature_gates": {
            "bounded_generation": {"enabled": False, "reason": "probe not completed"},
            "fresh_process_checkpoint_generation": {"enabled": False, "reason": "probe not completed"},
            "deterministic_generation": {"enabled": False, "reason": "probe not completed"},
            "multiprocess_clones": {"enabled": False, "reason": "probe not completed"},
            "mps_parity": {"enabled": False, "reason": "probe not completed"},
            "utility_backend": {"enabled": False, "reason": "bounded generation gate not completed"},
        },
    }
    try:
        result["environment"] = _environment_versions()
        result["package_identity"] = _distribution_identity("mostlyai-engine")
        installed_version = result["environment"]["packages"].get("mostlyai-engine")
        if installed_version != ENGINE_VERSION:
            raise RuntimeError(f"expected mostlyai-engine=={ENGINE_VERSION}, observed {installed_version!r}")

        result["seed_checks"] = _seed_checks(args.master_seed, args.fixture_rows)
        seed_prerequisites = result["seed_checks"]
        if not (
            seed_prerequisites["fixture"]["reproducible"]
            and seed_prerequisites["split"]["reproducible"]
            and seed_prerequisites["split"]["disjoint"]
            and seed_prerequisites["split"]["complete"]
            and seed_prerequisites["derived_32bit_seeds"]["within_uint32"]
            and seed_prerequisites["derived_32bit_seeds"]["reproducible"]
            and seed_prerequisites["derived_32bit_seeds"]["collision_free"]
        ):
            raise RuntimeError("deterministic fixture, split, or derived seed prerequisite failed")

        if args.workspace_root:
            work_root = Path(args.workspace_root).resolve()
            if work_root.exists():
                shutil.rmtree(work_root)
            work_root.mkdir(parents=True)
            cleanup_context: contextlib.AbstractContextManager[Any] = contextlib.nullcontext(work_root)
        else:
            cleanup_context = tempfile.TemporaryDirectory(prefix="sts-argn-contract-")

        with cleanup_context as temporary:
            work_root = Path(temporary)
            workspace = work_root / "fit-workspace"
            fit_process = _run_fit_child(
                work_root=work_root,
                workspace=workspace,
                fixture_rows=args.fixture_rows,
                fixture_seed=args.master_seed,
                model=args.model,
                train_device=args.train_device,
                timeout_seconds=args.child_timeout_seconds,
            )
            result["contract"] = {
                "fit_process": fit_process,
                "stages": fit_process.get("stages", {}),
            }
            if fit_process.get("status") != "passed" or fit_process.get("supervisor", {}).get("return_code") != 0:
                raise RuntimeError(
                    "fit checkpoint process failed: "
                    + json.dumps(fit_process.get("exceptions", []), ensure_ascii=False)
                )
            if not fit_process.get("supervisor", {}).get("process_exited_before_generation"):
                raise RuntimeError("fit process did not exit before generation")
            fit_evidence = fit_process["evidence"]
            result["contract"].update(fit_evidence)
            original_before_generations = _path_digest(workspace)
            result["contract"]["workspace_checksum_before_generations"] = original_before_generations

            generation_root = work_root / "generation-clones"
            generation_root.mkdir(parents=True)
            baseline_seed = _derived_seed(args.master_seed, "fresh-generation", 0)
            different_seed = _derived_seed(args.master_seed, "fresh-generation", 1)
            baseline = _run_generation_child(
                workspace,
                generation_root,
                "cpu-baseline",
                baseline_seed,
                "cpu",
                args.generation_rows,
                args.generation_batch_size,
                args.child_timeout_seconds,
            )
            repeat = _run_generation_child(
                workspace,
                generation_root,
                "cpu-repeat",
                baseline_seed,
                "cpu",
                args.generation_rows,
                args.generation_batch_size,
                args.child_timeout_seconds,
            )
            changed_seed = _run_generation_child(
                workspace,
                generation_root,
                "cpu-different-seed",
                different_seed,
                "cpu",
                args.generation_rows,
                args.generation_batch_size,
                args.child_timeout_seconds,
            )
            result["contract"]["fresh_process_generations"] = {
                "baseline": baseline,
                "same_seed_repeat": repeat,
                "different_seed": changed_seed,
            }
            baseline_exact_rows = baseline.get("output", {}).get("rows") == args.generation_rows
            baseline_bounded = (
                baseline.get("request", {}).get("batch_size") == args.generation_batch_size
                and int(baseline.get("output", {}).get("python_candidate_dataframe_rows", 0)) <= args.generation_rows
            )
            fit_exited = bool(fit_process.get("supervisor", {}).get("process_exited_before_generation"))
            distinct_fit_and_generate = (
                fit_process.get("pid") is not None
                and baseline.get("pid") is not None
                and fit_process.get("pid") != baseline.get("pid")
            )
            fresh_passed = bool(
                fit_exited
                and distinct_fit_and_generate
                and baseline.get("status") == "passed"
                and baseline.get("supervisor", {}).get("return_code") == 0
                and baseline.get("source_dataframe_materialized") is False
                and baseline.get("checkpoint_files_unchanged")
            )
            bounded_passed = bool(fresh_passed and baseline_exact_rows and baseline_bounded)
            result["feature_gates"]["fresh_process_checkpoint_generation"] = {
                "enabled": fresh_passed,
                "reason": "fit process exited, then a distinct fresh interpreter generated from the checkpoint without a source DataFrame"
                if fresh_passed
                else "fit-process exit or fresh interpreter checkpoint generation checks failed; see process evidence",
                "fit_process_pid": fit_process.get("pid"),
                "generation_process_pid": baseline.get("pid"),
                "parent_probe_pid": os.getpid(),
                "fit_process_exited_before_generation": fit_exited,
                "fit_and_generation_are_distinct": distinct_fit_and_generate,
            }
            result["feature_gates"]["bounded_generation"] = {
                "enabled": bounded_passed,
                "reason": "fresh generation produced the exact bounded row request"
                if bounded_passed
                else "bounded generation failed exact rows, process, batch, or checkpoint immutability checks",
                "requested_rows": args.generation_rows,
                "actual_rows": baseline.get("output", {}).get("rows"),
                "engine_batch_size": args.generation_batch_size,
                "candidate_dataframe_rows": baseline.get("output", {}).get("python_candidate_dataframe_rows"),
            }

            same_seed_equal = bool(
                baseline.get("status") == "passed"
                and repeat.get("status") == "passed"
                and baseline.get("output", {}).get("content_sha256") == repeat.get("output", {}).get("content_sha256")
            )
            different_seed_differs = bool(
                baseline.get("status") == "passed"
                and changed_seed.get("status") == "passed"
                and baseline.get("output", {}).get("content_sha256")
                != changed_seed.get("output", {}).get("content_sha256")
            )
            deterministic_passed = same_seed_equal and different_seed_differs
            result["seed_checks"]["engine_generation"] = {
                "same_seed_content_equal": same_seed_equal,
                "different_seed_content_differs": different_seed_differs,
                "baseline_seed": baseline_seed,
                "different_seed": different_seed,
                "baseline_content_sha256": baseline.get("output", {}).get("content_sha256"),
                "repeat_content_sha256": repeat.get("output", {}).get("content_sha256"),
                "different_seed_content_sha256": changed_seed.get("output", {}).get("content_sha256"),
            }
            result["feature_gates"]["deterministic_generation"] = {
                "enabled": deterministic_passed,
                "reason": "same checkpoint and seed reproduced content; a different seed changed content"
                if deterministic_passed
                else "same-seed reproducibility or different-seed separation failed",
            }

            mps = result["environment"]["mps"]
            if args.skip_mps_parity:
                result["feature_gates"]["mps_parity"] = {
                    "enabled": False,
                    "reason": "MPS parity was explicitly skipped",
                    "attempted": False,
                }
            elif not mps.get("torch_mps_available"):
                result["feature_gates"]["mps_parity"] = {
                    "enabled": False,
                    "reason": "torch reports MPS unavailable on this host",
                    "attempted": False,
                }
            elif baseline.get("status") != "passed":
                result["feature_gates"]["mps_parity"] = {
                    "enabled": False,
                    "reason": "CPU baseline failed, so MPS parity cannot be established",
                    "attempted": False,
                }
            else:
                mps_result = _run_generation_child(
                    workspace,
                    generation_root,
                    "mps-parity",
                    baseline_seed,
                    "mps",
                    args.generation_rows,
                    args.generation_batch_size,
                    args.child_timeout_seconds,
                )
                comparison = _compare_generation_summaries(baseline, mps_result)
                result["contract"]["mps_generation"] = mps_result
                result["contract"]["mps_cpu_comparison"] = comparison
                result["feature_gates"]["mps_parity"] = {
                    "enabled": comparison["passed"],
                    "reason": "MPS and CPU generation passed the recorded schema/distribution parity thresholds"
                    if comparison["passed"]
                    else "MPS generation failed or exceeded a recorded CPU parity threshold",
                    "attempted": True,
                }

            clone_evidence = _run_multiprocess_clone_probe(
                workspace=workspace,
                work_root=work_root,
                counts=args.clone_process_counts,
                cuda=result["environment"]["cuda"],
                master_seed=args.master_seed,
                sample_size=args.generation_rows,
                batch_size=args.generation_batch_size,
                timeout_seconds=args.child_timeout_seconds,
            )
            result["contract"]["multiprocess_clone_probe"] = clone_evidence
            result["feature_gates"]["multiprocess_clones"] = {
                "enabled": clone_evidence["passed"],
                "reason": "all requested 1/2/4 CUDA clone levels passed"
                if clone_evidence["passed"]
                else clone_evidence.get("disabled_reason", "CUDA clone checks failed"),
                "attempted_levels": [
                    level["process_count"] for level in clone_evidence["levels"] if level["attempted"]
                ],
            }

            original_after_generations = _path_digest(workspace)
            result["contract"]["workspace_checksum_after_generations"] = original_after_generations
            result["contract"]["original_workspace_unchanged"] = (
                original_before_generations["sha256"] == original_after_generations["sha256"]
            )
            maximum_candidate_rows = max(
                [
                    int(item.get("output", {}).get("python_candidate_dataframe_rows", 0))
                    for item in (baseline, repeat, changed_seed, result["contract"].get("mps_generation", {}))
                ],
                default=0,
            )
            maximum_candidate_bytes = max(
                [
                    int(item.get("output", {}).get("python_candidate_dataframe_deep_bytes", 0))
                    for item in (baseline, repeat, changed_seed, result["contract"].get("mps_generation", {}))
                ],
                default=0,
            )
            result["contract"]["bounded_python_objects"] = {
                "maximum_training_dataframe_rows": fit_evidence["fixture"]["rows"],
                "maximum_training_dataframe_deep_bytes": fit_evidence["fixture"]["deep_bytes"],
                "maximum_generation_candidate_dataframe_rows": maximum_candidate_rows,
                "maximum_generation_candidate_dataframe_deep_bytes": maximum_candidate_bytes,
                "claim": "no probe-created DataFrame exceeded the bounded training fixture or one generation candidate",
            }

            if args.run_staircase:
                staircase: dict[str, Any] = {"fit": [], "generation": []}
                for rows in args.staircase_fit_rows:
                    if rows == args.fixture_rows:
                        staircase["fit"].append({"rows": rows, "reused_primary_contract": True, "evidence": fit_evidence})
                        continue
                    staircase_workspace = work_root / f"staircase-fit-{rows}"
                    staircase_workspace.mkdir(parents=True)
                    staircase_stages: dict[str, Any] = {}
                    try:
                        evidence = _fit_workspace(
                            staircase_workspace,
                            rows,
                            args.master_seed,
                            args.model,
                            args.train_device,
                            staircase_stages,
                        )
                        staircase["fit"].append(
                            {"rows": rows, "status": "passed", "stages": staircase_stages, "evidence": evidence}
                        )
                    except BaseException as error:
                        record = _exception_record(f"staircase_fit_{rows}", error)
                        exceptions.append(record)
                        staircase["fit"].append(
                            {"rows": rows, "status": "failed", "stages": staircase_stages, "exception": record}
                        )
                for index, rows in enumerate(args.staircase_generation_rows):
                    staircase_result = _run_generation_child(
                        workspace,
                        generation_root,
                        f"staircase-generate-{rows}",
                        _derived_seed(args.master_seed, "staircase-generation", index),
                        "cpu",
                        rows,
                        min(args.generation_batch_size, rows),
                        args.child_timeout_seconds,
                    )
                    staircase["generation"].append({"rows": rows, "evidence": staircase_result})
                result["staircase"] = staircase
            else:
                result["staircase"] = {
                    "executed": False,
                    "reason": "expensive staircase is opt-in via --run-staircase",
                    "configured_fit_rows": args.staircase_fit_rows,
                    "configured_generation_rows": args.staircase_generation_rows,
                }

            utility_enabled = bool(result["feature_gates"]["bounded_generation"]["enabled"])
            result["feature_gates"]["utility_backend"] = {
                "enabled": utility_enabled,
                "reason": "bounded generation contract passed"
                if utility_enabled
                else "bounded generation contract failed; utility backend must not ship",
            }
            result["status"] = "passed" if utility_enabled else "failed"
    except BaseException as error:
        exceptions.append(_exception_record("parent_probe", error))
        result["feature_gates"]["bounded_generation"] = {
            "enabled": False,
            "reason": "fit or generation contract raised an exception",
        }
        result["feature_gates"]["utility_backend"] = {
            "enabled": False,
            "reason": "bounded generation contract did not pass; utility backend must not ship",
        }
        result["status"] = "failed"
    finally:
        result["completed_at"] = _utc_now()
        result["wall_seconds"] = time.perf_counter() - started
    return result, 0 if result["status"] == "passed" else 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the pinned MOSTLY AI Engine contract probe")
    parser.add_argument("--output", type=Path, default=DEFAULT_RESULT_PATH)
    parser.add_argument("--fixture-rows", type=int, default=int(os.environ.get("ARGN_PROBE_FIXTURE_ROWS", "10000")))
    parser.add_argument(
        "--generation-rows", type=int, default=int(os.environ.get("ARGN_PROBE_GENERATION_ROWS", "10000"))
    )
    parser.add_argument(
        "--generation-batch-size",
        type=int,
        default=int(os.environ.get("ARGN_PROBE_GENERATION_BATCH_SIZE", "1000")),
    )
    parser.add_argument("--master-seed", type=int, default=290797)
    parser.add_argument("--model", default=os.environ.get("ARGN_PROBE_MODEL", "MOSTLY_AI/Small"))
    parser.add_argument("--train-device", default=os.environ.get("ARGN_PROBE_TRAIN_DEVICE", "cpu"))
    parser.add_argument("--clone-process-counts", type=_parse_int_list, default=_parse_int_list("1,2,4"))
    parser.add_argument("--child-timeout-seconds", type=int, default=3600)
    parser.add_argument("--run-staircase", action="store_true")
    parser.add_argument("--staircase-fit-rows", type=_parse_int_list, default=_parse_int_list("10000,50000,250000"))
    parser.add_argument(
        "--staircase-generation-rows", type=_parse_int_list, default=_parse_int_list("10000,100000,1000000")
    )
    parser.add_argument("--skip-mps-parity", action="store_true")
    parser.add_argument("--workspace-root", type=Path)
    parser.add_argument("--child-fit", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--child-generate", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--child-request", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--child-result", type=Path, help=argparse.SUPPRESS)
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if args.fixture_rows <= 0 or args.generation_rows <= 0 or args.generation_batch_size <= 0:
        parser.error("row counts and generation batch size must be positive")
    if args.generation_batch_size > args.generation_rows:
        args.generation_batch_size = args.generation_rows
    if args.child_fit:
        if args.child_request is None or args.child_result is None:
            parser.error("--child-fit requires --child-request and --child-result")
        return _child_fit(args.child_request, args.child_result)
    if args.child_generate:
        if args.child_request is None or args.child_result is None:
            parser.error("--child-generate requires --child-request and --child-result")
        return _child_generate(args.child_request, args.child_result)
    result, exit_code = _probe(args)
    _atomic_json(args.output.resolve(), result)
    print(json.dumps({"status": result["status"], "result": str(args.output.resolve())}, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
