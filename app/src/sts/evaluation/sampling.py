from __future__ import annotations

import hashlib
import heapq
import hmac
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Literal

import pyarrow as pa
from pydantic import Field

from sts.domain.canonical import CanonicalModel


class SampleManifest(CanonicalModel):
    """Auditable identity of a deterministic bounded evaluation sample."""

    version: Literal["1.0"] = "1.0"
    namespace: str = Field(min_length=1)
    seed: int = Field(ge=0, le=2**64 - 1)
    population_rows: int = Field(ge=0)
    requested_rows: int = Field(gt=0)
    selected_rows: int = Field(ge=0)
    sample_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    identity: Literal["column", "logical_ordinal"]
    identity_column: str | None = None


class DeterministicSample(CanonicalModel):
    """Manifest wrapper used for JSON contracts; the Arrow table stays in memory."""

    manifest: SampleManifest


SampleSource = pa.Table | pa.RecordBatch | Iterable[pa.RecordBatch] | str | Path


def _iter_path(path: Path) -> Iterator[pa.RecordBatch]:
    import duckdb

    connection = duckdb.connect()
    try:
        suffix = path.suffix.lower()
        if suffix in {".parquet", ".pq"}:
            query = "SELECT * FROM read_parquet(?)"
        elif suffix == ".csv":
            query = "SELECT * FROM read_csv_auto(?, header = true)"
        else:
            raise ValueError(f"unsupported evaluation sample source: {path.suffix}")
        reader = connection.execute(query, [str(path)]).to_arrow_reader(batch_size=65_536)
        yield from reader
    finally:
        connection.close()


def iter_sample_batches(source: SampleSource) -> Iterator[pa.RecordBatch]:
    if isinstance(source, pa.RecordBatch):
        yield source
    elif isinstance(source, pa.Table):
        batches = source.to_batches(max_chunksize=65_536)
        if batches:
            yield from batches
        else:
            yield pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in source.schema],
                schema=source.schema,
            )
    elif isinstance(source, (str, Path)):
        yield from _iter_path(Path(source))
    else:
        for batch in source:
            if not isinstance(batch, pa.RecordBatch):
                raise TypeError("sample iterables must contain only pyarrow.RecordBatch values")
            yield batch


def _sample_table_sha256(table: pa.Table) -> str:
    """Hash a canonical single-chunk Arrow IPC stream for exact sample identity."""

    canonical = table.combine_chunks()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, canonical.schema) as writer:
        writer.write_table(canonical)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def deterministic_hmac_sample(
    source: SampleSource,
    *,
    max_rows: int,
    seed: int,
    namespace: str,
    id_column: str = "__sts_row_id",
    hmac_key: bytes | None = None,
) -> tuple[pa.Table, SampleManifest]:
    """Select the globally smallest HMAC priorities in one bounded pass.

    Selection is independent of input batch boundaries.  A stable identity column
    is preferred; sources without one use their decoded logical row ordinal.  The
    returned table retains source order, while selection itself is uniform under
    the keyed SHA-256 PRF.
    """

    if max_rows <= 0:
        raise ValueError("max_rows must be positive")
    if not 0 <= seed <= 2**64 - 1:
        raise ValueError("seed must be an unsigned 64-bit integer")
    if not namespace:
        raise ValueError("namespace must not be empty")
    key = (
        hmac_key
        or hashlib.sha256(b"sts-evaluation-sample-key-1.0\x00" + seed.to_bytes(8, "big")).digest()
    )
    prefix = namespace.encode("utf-8") + b"\x00" + seed.to_bytes(8, "big") + b"\x00"

    heap: list[tuple[int, int, tuple[object, ...]]] = []
    schema: pa.Schema | None = None
    identity_index: int | None = None
    population_rows = 0
    for batch in iter_sample_batches(source):
        if schema is None:
            schema = batch.schema
            identity_index = schema.get_field_index(id_column)
            if identity_index < 0:
                identity_index = None
        elif batch.schema != schema:
            raise ValueError("all sample batches must have exactly the same Arrow schema")
        columns = [batch.column(index) for index in range(batch.num_columns)]
        for row_index in range(batch.num_rows):
            ordinal = population_rows
            if identity_index is None:
                identity = ordinal.to_bytes(8, "big", signed=False)
            else:
                scalar = columns[identity_index][row_index]
                if not scalar.is_valid:
                    raise ValueError(f"sample identity column {id_column!r} contains null")
                value = scalar.as_py()
                identity = value if isinstance(value, bytes) else str(value).encode("utf-8")
            priority = int.from_bytes(
                hmac.new(key, prefix + identity, hashlib.sha256).digest(), "big"
            )
            values = tuple(column[row_index].as_py() for column in columns)
            entry = (-priority, -ordinal, values)
            if len(heap) < max_rows:
                heapq.heappush(heap, entry)
            elif entry[:2] > heap[0][:2]:
                heapq.heapreplace(heap, entry)
            population_rows += 1

    if schema is None:
        schema = pa.schema([])
    selected = sorted(heap, key=lambda entry: -entry[1])
    if schema:
        arrays = [
            pa.array((entry[2][index] for entry in selected), type=field.type)
            for index, field in enumerate(schema)
        ]
        table = pa.Table.from_arrays(arrays, schema=schema)
    else:
        table = pa.Table.from_arrays([], schema=schema)
    manifest = SampleManifest(
        namespace=namespace,
        seed=seed,
        population_rows=population_rows,
        requested_rows=max_rows,
        selected_rows=table.num_rows,
        sample_sha256=_sample_table_sha256(table),
        identity="column" if identity_index is not None else "logical_ordinal",
        identity_column=id_column if identity_index is not None else None,
    )
    return table, manifest
