from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from .protocol import (
    WorkerEvent,
    WorkerEventWriter,
    WorkerRequestEnvelope,
    WorkerResultEnvelope,
    confined_output_path,
    read_request,
    resolve_manifest_snapshot,
    write_result_atomic,
)


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _directory_digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    for item in sorted(
        candidate for candidate in path.rglob("*") if candidate.is_file()
    ):
        relative = item.relative_to(path).as_posix().encode("utf-8")
        file_digest, file_size = _sha256_file(item)
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(bytes.fromhex(file_digest))
        digest.update(file_size.to_bytes(8, "big"))
        size += file_size
    return digest.hexdigest(), size


def _cancelled(root: Path, request: WorkerRequestEnvelope) -> bool:
    return confined_output_path(root, request.cancellation_path).exists()


def _event(
    writer: WorkerEventWriter, sequence: int, stage: str, completed: int
) -> None:
    writer.append(
        WorkerEvent(
            version="1.0",
            sequence=sequence,
            timestamp=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            stage=stage,
            completed=completed,
            total=1,
            unit="steps",
            message_code=f"DPMM_{stage.upper()}",
            metrics={},
        )
    )


def _fit(
    request: WorkerRequestEnvelope, writer: WorkerEventWriter
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    import pandas as pd
    from dpmm.pipelines.mst import MSTPipeline

    root = Path(request.manifest_snapshot.workspace_root).resolve(strict=True)
    snapshots = resolve_manifest_snapshot(request.manifest_snapshot)
    source = snapshots["private_fit_sample"].path
    config = request.limits["dpmm"]
    domain = {str(name): int(size) for name, size in config["domain"].items()}
    if set(domain) != set(pd.read_csv(source, nrows=0).columns):
        raise ValueError("encoded private fit columns do not match the public domain")
    if _cancelled(root, request):
        raise InterruptedError("cancelled before private fit")
    frame = pd.read_csv(source, dtype={name: "int64" for name in domain})
    for name, size in domain.items():
        values = frame[name]
        if values.isna().any() or not ((values >= 0) & (values < size)).all():
            raise ValueError(f"encoded values exceed the public domain for {name}")
    checkpoint = confined_output_path(root, config["checkpoint_path"])
    if checkpoint.exists():
        raise FileExistsError(checkpoint)
    staging = checkpoint.with_name(f".{checkpoint.name}.{uuid4().hex}.part")
    private_rng, rng_policy = _private_fit_rng()
    pipeline = MSTPipeline(
        epsilon=float(config["epsilon_model"]),
        delta=float(config["delta"]),
        disable_processing=True,
        n_jobs=1,
        max_model_size=int(config["max_model_size"]),
    )
    _event(writer, 1, "fitting", 0)
    try:
        pipeline.fit(frame, domain=domain, random_state=private_rng, public=False)
        pipeline.store(staging)
        os.replace(staging, checkpoint)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    _event(writer, 2, "fitting", 1)
    digest, size = _directory_digest(checkpoint)
    return (
        [
            {
                "kind": "model_checkpoint",
                "path": config["checkpoint_path"],
                "sha256": digest,
                "size_bytes": size,
                "downloadable": False,
                "release_safe": False,
                "contains_private_source_information": True,
                "metadata": {
                    "engine": "dpmm",
                    "version": "0.1.9",
                    "private_rng_persisted": False,
                    "rng_policy": rng_policy,
                },
            }
        ],
        {
            # The number of private rows the fit predicate selected is source-derived and
            # is deliberately not reported; the modeled column count is public schema.
            "modeled_columns": int(frame.shape[1]),
            "rng_policy": rng_policy,
        },
    )


_RNG_COMMITMENT_DOMAIN = b"synthetic-table-studio/private-fit-rng/v1\x00"


def _private_fit_rng() -> tuple[Any, dict[str, str]]:
    """Build the private fit RNG and a seed-free public record of it.

    This mirrors ``sts.privacy.rng.PrivateFitRng`` byte for byte: the worker runs
    in its own locked environment and cannot import the application package, but
    the ledger must be able to verify that the RNG it records is the RNG that
    produced the model. 256 bits come from the OS CSPRNG, the only thing that
    leaves this function is a domain-separated commitment to them, and the bytes
    are zeroed as soon as the RandomState has been seeded.
    """

    import numpy as np

    entropy = bytearray(os.urandom(32))
    commitment = hashlib.sha256(_RNG_COMMITMENT_DOMAIN + bytes(entropy)).hexdigest()
    words = [
        int.from_bytes(entropy[offset : offset + 4], "little")
        for offset in range(0, 32, 4)
    ]
    seed_words = np.random.SeedSequence(words).generate_state(624, dtype=np.uint32)
    random_state = np.random.RandomState(seed_words)
    for index in range(len(entropy)):
        entropy[index] = 0
    policy = {
        "version": "1.0",
        "entropy_source": "OS CSPRNG (256-bit)",
        "rng_implementation": "numpy.random.SeedSequence -> numpy.random.RandomState(MT19937)",
        "commitment_sha256": commitment,
    }
    return random_state, policy


def _sample(
    request: WorkerRequestEnvelope, writer: WorkerEventWriter
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from dpmm.pipelines.mst import MSTPipeline

    root = Path(request.manifest_snapshot.workspace_root).resolve(strict=True)
    resolve_manifest_snapshot(request.manifest_snapshot)
    config = request.limits["dpmm"]
    checkpoint = confined_output_path(root, config["checkpoint_path"])
    if not checkpoint.is_dir():
        raise FileNotFoundError(checkpoint)
    output = confined_output_path(root, config["encoded_output_path"])
    if output.exists():
        raise FileExistsError(output)
    if _cancelled(root, request):
        raise InterruptedError("cancelled before public sampling")
    _event(writer, 1, "generating", 0)
    pipeline = MSTPipeline.load(checkpoint)
    generated = pipeline.generate(
        n_records=int(config["target_count"]),
        random_state=int(config["sampling_seed"]),
    )
    domain = {str(name): int(size) for name, size in config["domain"].items()}
    if list(generated.columns) != list(domain):
        raise ValueError("generated columns do not match the public domain")
    for name, size in domain.items():
        values = generated[name].to_numpy()
        if not ((values >= 0) & (values < size)).all():
            raise ValueError(f"generated values exceed the public domain for {name}")
    part = output.with_name(f".{output.name}.{uuid4().hex}.part")
    generated.to_csv(part, index=False)
    os.replace(part, output)
    _event(writer, 2, "generating", 1)
    digest, size = _sha256_file(output)
    return (
        [
            {
                "kind": "dp_discrete_sample",
                "path": config["encoded_output_path"],
                "sha256": digest,
                "size_bytes": size,
                "downloadable": False,
                "release_safe": False,
                "contains_private_source_information": False,
                "metadata": {"sampling_seed": int(config["sampling_seed"])},
            }
        ],
        {
            "generated_rows": int(generated.shape[0]),
            "modeled_columns": int(generated.shape[1]),
        },
    )


def run(request_path: Path, events_path: Path, result_path: Path) -> int:
    request: WorkerRequestEnvelope | None = None
    try:
        request = read_request(request_path)
        if request.worker_kind != "dpmm" or request.operation not in {"fit", "sample"}:
            raise ValueError("unsupported dpmm worker request")
        writer = WorkerEventWriter(events_path)
        if request.operation == "fit":
            artifacts, usage = _fit(request, writer)
        else:
            artifacts, usage = _sample(request, writer)
        result = WorkerResultEnvelope("1.0", "success", artifacts, usage, None)
        exit_code = 0
    except InterruptedError as error:
        result = WorkerResultEnvelope(
            "1.0",
            "cancelled",
            [],
            {},
            {"code": "CANCELLED", "message": str(error), "details": {}},
        )
        exit_code = 0
    except Exception as error:
        result = WorkerResultEnvelope(
            "1.0",
            "failure",
            [],
            {},
            {
                "code": "WORKER_FAILED",
                "message": str(error) or type(error).__name__,
                "details": {},
            },
        )
        exit_code = 1
    write_result_atomic(result_path, result)
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    command = subparsers.add_parser("run")
    command.add_argument("--request", type=Path, required=True)
    command.add_argument("--events", type=Path, required=True)
    command.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()
    return run(args.request, args.events, args.result)


if __name__ == "__main__":
    sys.exit(main())
