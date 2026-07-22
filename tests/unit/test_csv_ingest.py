from __future__ import annotations

import hashlib
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq
import pytest

from sts.domain import DomainError, ErrorCode
from sts.ingest.csv import (
    CSV_MAX_COLUMNS,
    CSV_MAX_FIELD_BYTES,
    CSV_MAX_LOGICAL_RECORD_BYTES,
    CSV_SAMPLE_BYTES,
    MalformedPolicy,
    CsvLimits,
    CsvParseOptions,
    convert_csv_to_raw_parquet,
    inspect_csv,
    propose_csv_parse_options,
)
from sts.ingest.upload import (
    MAX_UPLOAD_BYTES,
    MAX_UPLOAD_CHUNK_BYTES,
    UploadManager,
    UploadState,
)


def _options(
    *,
    encoding: str = "utf-8",
    delimiter: str = ",",
    malformed: MalformedPolicy = MalformedPolicy.FAIL,
) -> CsvParseOptions:
    return CsvParseOptions(
        encoding=encoding,
        delimiter=delimiter,
        malformed=malformed,
        confirmed=True,
    )


def _write_encoded(path: Path, text: str, encoding: str) -> None:
    path.write_bytes(text.encode(encoding))


def test_upload_exact_offsets_hash_gate_and_atomic_staging(tmp_path: Path) -> None:
    manager = UploadManager(tmp_path / "workspace")
    dataset_id = uuid4()
    content = b"abcdef"

    created = manager.create(dataset_id, "people.csv", len(content))
    assert created.state is UploadState.UPLOADING
    assert manager.head_offset(dataset_id) == 0
    assert manager.head_offset(dataset_id) == 0

    assert manager.append(dataset_id, 0, content[:3]) == 3
    assert manager.head_offset(dataset_id) == 3
    with pytest.raises(DomainError) as wrong_offset:
        manager.append(dataset_id, 0, b"x")
    assert wrong_offset.value.code is ErrorCode.INVALID_STATE
    assert wrong_offset.value.problem.context == {
        "provided_offset": 0,
        "upload_offset": 3,
    }
    assert manager.head_offset(dataset_id) == 3

    assert manager.append(dataset_id, 3, content[3:]) == len(content)
    with pytest.raises(DomainError) as wrong_hash:
        manager.complete(dataset_id, "0" * 64)
    assert wrong_hash.value.code is ErrorCode.CHECKSUM_MISMATCH
    assert not (
        tmp_path / "workspace" / "datasets" / str(dataset_id) / "source.csv"
    ).exists()
    assert manager.head_offset(dataset_id) == len(content)

    digest = hashlib.sha256(content).hexdigest()
    published = manager.complete(dataset_id, digest)
    assert published.path.read_bytes() == content
    assert published.sha256 == digest
    assert manager.head_offset(dataset_id) == len(content)
    assert UploadManager(tmp_path / "workspace").head_offset(dataset_id) == len(content)
    assert manager.complete(dataset_id, digest) == published
    with pytest.raises(DomainError) as completed_append:
        manager.append(dataset_id, len(content), b"")
    assert completed_append.value.code is ErrorCode.INVALID_STATE
    assert not list(published.path.parent.glob("*.part"))


def test_upload_enforces_declared_size_chunk_and_upload_limits(tmp_path: Path) -> None:
    assert MAX_UPLOAD_CHUNK_BYTES == 64 * 1024**2
    assert MAX_UPLOAD_BYTES == 8 * 1024**3
    manager = UploadManager(tmp_path / "workspace", max_chunk_bytes=4)
    dataset_id = uuid4()
    manager.create(dataset_id, "data.csv", 5, "csv")

    with pytest.raises(DomainError) as chunk_error:
        manager.append(dataset_id, 0, b"12345")
    assert chunk_error.value.code is ErrorCode.UPLOAD_TOO_LARGE
    assert manager.head_offset(dataset_id) == 0
    assert manager.append(dataset_id, 0, b"1234") == 4
    with pytest.raises(DomainError) as declared_error:
        manager.append(dataset_id, 4, b"xx")
    assert declared_error.value.code is ErrorCode.UPLOAD_TOO_LARGE
    assert manager.head_offset(dataset_id) == 4
    with pytest.raises(DomainError) as incomplete:
        manager.complete(dataset_id, hashlib.sha256(b"1234").hexdigest())
    assert incomplete.value.code is ErrorCode.CHECKSUM_MISMATCH

    with pytest.raises(DomainError) as too_large:
        manager.create(uuid4(), "huge.csv", MAX_UPLOAD_BYTES + 1)
    assert too_large.value.code is ErrorCode.UPLOAD_TOO_LARGE
    with pytest.raises(ValueError):
        UploadManager(tmp_path / "other", max_chunk_bytes=MAX_UPLOAD_CHUNK_BYTES + 1)


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "cp949", "euc-kr"])
def test_all_supported_encodings_preserve_logical_strings(
    tmp_path: Path, encoding: str
) -> None:
    source = tmp_path / f"source-{encoding}.csv"
    destination = tmp_path / f"raw-{encoding}.parquet"
    _write_encoded(source, "name,city\n홍길동,서울\n김영희,부산\n", encoding)

    result = convert_csv_to_raw_parquet(
        source, destination, _options(encoding=encoding)
    )
    table = pq.read_table(destination)
    assert result.rows == 2
    assert result.columns == 2
    assert result.skipped == 0
    assert table.schema.field("name").type.__str__() == "string"
    assert table.to_pylist() == [
        {"name": "홍길동", "city": "서울", "__sts_row_id": 0},
        {"name": "김영희", "city": "부산", "__sts_row_id": 1},
    ]


def test_utf8_bom_before_quoted_header_and_large_valid_field(tmp_path: Path) -> None:
    source = tmp_path / "quoted-bom.csv"
    destination = tmp_path / "quoted-bom.parquet"
    value = "x" * 200_000
    source.write_bytes(
        b'\xef\xbb\xbf"name","note"\n' + f"alice,{value}\n".encode("utf-8")
    )

    result = convert_csv_to_raw_parquet(
        source, destination, _options(encoding="utf-8-sig")
    )
    assert result.row_count == 1
    assert pq.read_table(destination).column("note").to_pylist() == [value]


def test_multiline_records_and_empty_values_keep_logical_row_ids(
    tmp_path: Path,
) -> None:
    source = tmp_path / "multiline.csv"
    destination = tmp_path / "raw.parquet"
    source.write_bytes(b'kind,note\nfirst,"line one\nline two"\nsecond,\n')

    result = convert_csv_to_raw_parquet(source, destination, _options())
    assert result.row_count == 2
    assert pq.read_table(destination).to_pylist() == [
        {"kind": "first", "note": "line one\nline two", "__sts_row_id": 0},
        {"kind": "second", "note": "", "__sts_row_id": 1},
    ]


def test_ambiguous_dialect_requires_explicit_confirmation(tmp_path: Path) -> None:
    source = tmp_path / "ambiguous.csv"
    source.write_text("a,b;c\n1,2;3\n", encoding="utf-8")

    inspection = inspect_csv(source)
    assert inspection.ambiguous
    assert inspection.requires_confirmation
    assert inspection.options is None
    with pytest.raises(DomainError) as ambiguous:
        convert_csv_to_raw_parquet(source, tmp_path / "unconfirmed.parquet")
    assert ambiguous.value.code is ErrorCode.CSV_DIALECT_AMBIGUOUS
    with pytest.raises(DomainError) as not_confirmed:
        convert_csv_to_raw_parquet(
            source,
            tmp_path / "still-unconfirmed.parquet",
            CsvParseOptions(encoding="utf-8", delimiter=",", confirmed=False),
        )
    assert not_confirmed.value.code is ErrorCode.CSV_DIALECT_AMBIGUOUS

    confirmed = inspect_csv(source, _options(delimiter=","))
    assert confirmed.header == ("a", "b;c")
    result = convert_csv_to_raw_parquet(
        source, tmp_path / "confirmed.parquet", confirmed.options
    )
    assert result.row_count == 1


def test_malformed_fail_or_skip_and_preserve_source_positions(tmp_path: Path) -> None:
    source = tmp_path / "malformed.csv"
    source.write_bytes(b'a,b\n1,2\n3"oops,4\n5\n6,7\n')

    with pytest.raises(DomainError) as failed:
        convert_csv_to_raw_parquet(source, tmp_path / "failed.parquet", _options())
    assert failed.value.code is ErrorCode.SCHEMA_INVALID
    assert failed.value.problem.context["logical_row_id"] == 1
    assert not (tmp_path / "failed.parquet").exists()
    assert not list(tmp_path.glob("*.part"))

    result = convert_csv_to_raw_parquet(
        source,
        tmp_path / "skipped.parquet",
        _options(malformed=MalformedPolicy.SKIP),
    )
    assert result.row_count == 2
    assert result.skipped_rows == 2
    assert dict(result.malformed_reasons) == {
        "column_count_mismatch": 1,
        "quote_in_unquoted_field": 1,
    }
    assert pq.read_table(tmp_path / "skipped.parquet").to_pylist() == [
        {"a": "1", "b": "2", "__sts_row_id": 0},
        {"a": "6", "b": "7", "__sts_row_id": 3},
    ]


@pytest.mark.parametrize(
    ("payload", "expected_detail"),
    [
        (b"", "empty"),
        (b"a,a\n1,2\n", "unique"),
        ((",".join(f"c{i}" for i in range(71)) + "\n").encode(), "70-column"),
    ],
)
def test_empty_duplicate_and_71_column_inputs_are_rejected(
    tmp_path: Path, payload: bytes, expected_detail: str
) -> None:
    source = tmp_path / f"bad-{expected_detail}.csv"
    source.write_bytes(payload)
    with pytest.raises(DomainError) as error:
        convert_csv_to_raw_parquet(
            source, tmp_path / f"bad-{expected_detail}.parquet", _options()
        )
    assert error.value.code is ErrorCode.SCHEMA_INVALID
    assert expected_detail in error.value.problem.detail


def test_record_and_field_byte_limits_are_hard_even_in_skip_mode(
    tmp_path: Path,
) -> None:
    assert CSV_SAMPLE_BYTES == 8 * 1024**2
    assert CSV_MAX_COLUMNS == 70
    assert CSV_MAX_LOGICAL_RECORD_BYTES == 16 * 1024**2
    assert CSV_MAX_FIELD_BYTES == 8 * 1024**2

    record_source = tmp_path / "record.csv"
    record_source.write_bytes(b"a\n12345678901\n")
    record_limits = CsvLimits(max_logical_record_bytes=10, max_field_bytes=10)
    with pytest.raises(DomainError) as record_error:
        convert_csv_to_raw_parquet(
            record_source,
            tmp_path / "record.parquet",
            _options(malformed=MalformedPolicy.SKIP),
            limits=record_limits,
        )
    assert "logical record" in record_error.value.problem.detail

    field_source = tmp_path / "field.csv"
    field_source.write_bytes(b"a\n12345\n")
    field_limits = CsvLimits(max_logical_record_bytes=20, max_field_bytes=4)
    with pytest.raises(DomainError) as field_error:
        convert_csv_to_raw_parquet(
            field_source,
            tmp_path / "field.parquet",
            _options(malformed=MalformedPolicy.SKIP),
            limits=field_limits,
        )
    assert "field exceeds" in field_error.value.problem.detail


def test_parse_proposal_never_samples_more_than_eight_mib(tmp_path: Path) -> None:
    source = tmp_path / "large.csv"
    block = b"a,b\n1,2\n" * 100_000
    with source.open("wb") as stream:
        for _ in range(12):
            stream.write(block)
        stream.write(b"\xff")

    proposal = propose_csv_parse_options(source)
    assert source.stat().st_size > CSV_SAMPLE_BYTES
    assert proposal.sample_size_bytes == CSV_SAMPLE_BYTES
    assert proposal.recommended.encoding == "utf-8"


def test_streaming_batches_and_exact_raw_rows(tmp_path: Path) -> None:
    source = tmp_path / "rows.csv"
    source.write_text("a,b\n0,x\n1,y\n2,z\n3,w\n4,v\n", encoding="utf-8")
    destination = tmp_path / "rows.parquet"
    limits = CsvLimits(batch_rows=2, batch_value_bytes=64)

    proposal = propose_csv_parse_options(source, limits=limits)
    assert proposal.sample_size_bytes == source.stat().st_size
    result = convert_csv_to_raw_parquet(
        source,
        destination,
        _options(),
        proposal=proposal,
        limits=limits,
    )
    parquet = pq.ParquetFile(destination)
    assert result.row_count == 5
    assert result.batches_written == 3
    assert result.largest_batch_rows == 2
    assert parquet.metadata.num_rows == 5
    assert parquet.num_row_groups == 3
    assert pq.read_table(destination).column("__sts_row_id").to_pylist() == [
        0,
        1,
        2,
        3,
        4,
    ]
    assert hashlib.sha256(destination.read_bytes()).hexdigest() == result.sha256
