from __future__ import annotations

import codecs
import contextlib
import csv as csv_module
import fcntl
import hashlib
import io
import os
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from sts.domain import DomainError, ErrorCode

CSV_SAMPLE_BYTES = 8 * 1024**2
CSV_MAX_COLUMNS = 70
CSV_MAX_LOGICAL_RECORD_BYTES = 16 * 1024**2
CSV_MAX_FIELD_BYTES = 8 * 1024**2
CSV_BATCH_ROWS = 65_536
CSV_BATCH_VALUE_BYTES = 64 * 1024**2
SUPPORTED_CSV_ENCODINGS = ("utf-8", "utf-8-sig", "cp949", "euc-kr")
SUPPORTED_CSV_DELIMITERS = (",", "\t", ";", "|")
ROW_ID_COLUMN = "__sts_row_id"

# Python's parser otherwise rejects valid fields around 128 KiB, well below
# the product's explicit byte limits. The application never accepts records
# larger than this value.
csv_module.field_size_limit(CSV_MAX_LOGICAL_RECORD_BYTES)


class MalformedPolicy(StrEnum):
    FAIL = "fail"
    SKIP = "skip"


@dataclass(frozen=True, slots=True)
class CsvLimits:
    sample_bytes: int = CSV_SAMPLE_BYTES
    max_columns: int = CSV_MAX_COLUMNS
    max_logical_record_bytes: int = CSV_MAX_LOGICAL_RECORD_BYTES
    max_field_bytes: int = CSV_MAX_FIELD_BYTES
    batch_rows: int = CSV_BATCH_ROWS
    batch_value_bytes: int = CSV_BATCH_VALUE_BYTES

    def __post_init__(self) -> None:
        upper_bounds = (
            ("sample_bytes", self.sample_bytes, CSV_SAMPLE_BYTES),
            ("max_columns", self.max_columns, CSV_MAX_COLUMNS),
            (
                "max_logical_record_bytes",
                self.max_logical_record_bytes,
                CSV_MAX_LOGICAL_RECORD_BYTES,
            ),
            ("max_field_bytes", self.max_field_bytes, CSV_MAX_FIELD_BYTES),
        )
        for name, value, maximum in upper_bounds:
            if value <= 0 or value > maximum:
                raise ValueError(f"{name} must be in 1..{maximum}")
        if self.batch_rows <= 0 or self.batch_value_bytes <= 0:
            raise ValueError("CSV batch bounds must be positive")


DEFAULT_CSV_LIMITS = CsvLimits()


@dataclass(frozen=True, slots=True)
class CsvParseCandidate:
    encoding: str
    delimiter: str
    detected_columns: int
    sampled_records: int
    consistent_records: int


@dataclass(frozen=True, slots=True)
class CsvParseProposal:
    sample_size_bytes: int
    candidates: tuple[CsvParseCandidate, ...]
    recommended: CsvParseCandidate
    ambiguous: bool

    @property
    def requires_confirmation(self) -> bool:
        return self.ambiguous


@dataclass(frozen=True, slots=True)
class CsvParseOptions:
    encoding: str
    delimiter: str
    malformed: MalformedPolicy | str = MalformedPolicy.FAIL
    confirmed: bool = False

    def __post_init__(self) -> None:
        if self.encoding not in SUPPORTED_CSV_ENCODINGS:
            raise ValueError(
                "encoding must be exactly one of " + ", ".join(SUPPORTED_CSV_ENCODINGS)
            )
        if self.delimiter not in SUPPORTED_CSV_DELIMITERS:
            raise ValueError("delimiter must be one of comma, tab, semicolon, or pipe")
        object.__setattr__(self, "malformed", MalformedPolicy(self.malformed))


@dataclass(frozen=True, slots=True)
class CsvInspection:
    proposal: CsvParseProposal
    options: CsvParseOptions | None
    header: tuple[str, ...] | None

    @property
    def requires_confirmation(self) -> bool:
        return self.proposal.requires_confirmation and self.options is None

    @property
    def ambiguous(self) -> bool:
        return self.proposal.ambiguous


@dataclass(frozen=True, slots=True)
class CsvConversionResult:
    output_path: Path
    row_count: int
    column_count: int
    skipped_rows: int
    header: tuple[str, ...]
    sha256: str
    size_bytes: int
    batches_written: int
    largest_batch_rows: int
    largest_batch_value_bytes: int
    malformed_reasons: tuple[tuple[str, int], ...]

    @property
    def rows(self) -> int:
        return self.row_count

    @property
    def columns(self) -> int:
        return self.column_count

    @property
    def skipped(self) -> int:
        return self.skipped_rows


@dataclass(frozen=True, slots=True)
class _LogicalRecord:
    content: bytes
    malformed_reason: str | None


def propose_csv_parse_options(
    source_path: str | os.PathLike[str], *, limits: CsvLimits = DEFAULT_CSV_LIMITS
) -> CsvParseProposal:
    source = Path(source_path)
    size = source.stat().st_size
    with source.open("rb") as stream:
        sample = stream.read(limits.sample_bytes)
    if not sample:
        raise DomainError(ErrorCode.SCHEMA_INVALID, "CSV input is empty")

    decoded_candidates = _decode_sample_candidates(sample, complete=size <= len(sample))
    if not decoded_candidates:
        raise DomainError(
            ErrorCode.INPUT_FORMAT_UNSUPPORTED,
            "CSV is not valid in any supported encoding",
            context={"supported_encodings": list(SUPPORTED_CSV_ENCODINGS)},
        )

    ranked: list[tuple[tuple[int, int, int, int], int, int, CsvParseCandidate]] = []
    for encoding_index, (encoding, text) in enumerate(decoded_candidates):
        for delimiter_index, delimiter in enumerate(SUPPORTED_CSV_DELIMITERS):
            score, detected_columns, sampled_records, consistent_records = _score_dialect(
                text, delimiter
            )
            candidate = CsvParseCandidate(
                encoding=encoding,
                delimiter=delimiter,
                detected_columns=detected_columns,
                sampled_records=sampled_records,
                consistent_records=consistent_records,
            )
            ranked.append((score, -encoding_index, -delimiter_index, candidate))

    ranked.sort(reverse=True, key=lambda item: (item[0], item[1], item[2]))
    best_score = ranked[0][0]
    if ranked[0][3].sampled_records == 0:
        raise DomainError(ErrorCode.SCHEMA_INVALID, "CSV sample contains no parseable records")
    best = [entry[3] for entry in ranked if entry[0] == best_score]
    ordered_candidates = tuple(entry[3] for entry in ranked)
    return CsvParseProposal(
        sample_size_bytes=len(sample),
        candidates=ordered_candidates,
        recommended=best[0],
        ambiguous=len(best) > 1,
    )


def inspect_csv(
    source_path: str | os.PathLike[str],
    confirmed_options: CsvParseOptions | None = None,
    *,
    limits: CsvLimits = DEFAULT_CSV_LIMITS,
) -> CsvInspection:
    proposal = propose_csv_parse_options(source_path, limits=limits)
    if confirmed_options is None and proposal.requires_confirmation:
        return CsvInspection(proposal=proposal, options=None, header=None)
    options = _resolve_options(proposal, confirmed_options)
    header = _read_and_validate_header(Path(source_path), options, limits)
    return CsvInspection(proposal=proposal, options=options, header=header)


def convert_csv_to_raw_parquet(
    source_path: str | os.PathLike[str],
    output_path: str | os.PathLike[str],
    options: CsvParseOptions | None = None,
    *,
    proposal: CsvParseProposal | None = None,
    limits: CsvLimits = DEFAULT_CSV_LIMITS,
) -> CsvConversionResult:
    """Stream one CSV into an atomically published all-string raw Parquet file."""
    source = Path(source_path)
    destination = Path(output_path)
    if source.resolve() == destination.resolve(strict=False):
        raise ValueError("CSV source and Parquet destination must differ")
    current_proposal = proposal or propose_csv_parse_options(source, limits=limits)
    resolved_options = _resolve_options(current_proposal, options)

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.part")
    writer: pq.ParquetWriter | None = None
    schema: pa.Schema | None = None
    reasons: Counter[str] = Counter()
    rows_written = 0
    skipped_rows = 0
    batches_written = 0
    largest_batch_rows = 0
    largest_batch_value_bytes = 0
    header: tuple[str, ...] | None = None
    columns: list[list[str]] = []
    row_ids: list[int] = []
    buffered_value_bytes = 0

    def flush_batch() -> None:
        nonlocal batches_written, largest_batch_rows, largest_batch_value_bytes
        nonlocal buffered_value_bytes
        if not row_ids:
            return
        assert writer is not None
        assert schema is not None
        arrays = [pa.array(row_ids, type=pa.int64())]
        arrays.extend(pa.array(values, type=pa.string()) for values in columns)
        batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
        writer.write_batch(batch)
        batches_written += 1
        largest_batch_rows = max(largest_batch_rows, len(row_ids))
        largest_batch_value_bytes = max(largest_batch_value_bytes, buffered_value_bytes)
        for values in columns:
            values.clear()
        row_ids.clear()
        buffered_value_bytes = 0

    try:
        with source.open("rb") as stream:
            _consume_utf8_bom(stream, resolved_options.encoding)
            records = _iter_logical_records(
                stream,
                ord(resolved_options.delimiter),
                limits.max_logical_record_bytes,
            )
            try:
                header_record = next(records)
            except StopIteration:
                raise DomainError(ErrorCode.SCHEMA_INVALID, "CSV input is empty") from None
            header = _parse_header_record(header_record, resolved_options, limits)
            columns = [[] for _ in header]
            schema = pa.schema(
                [pa.field(ROW_ID_COLUMN, pa.int64(), nullable=False)]
                + [pa.field(name, pa.string(), nullable=False) for name in header],
                metadata={
                    b"sts.raw_semantics": b"all-varchar",
                    b"sts.row_id": b"0-based-logical-data-record",
                    b"sts.csv_encoding": resolved_options.encoding.encode("ascii"),
                    b"sts.csv_delimiter": resolved_options.delimiter.encode("ascii"),
                },
            )
            writer = pq.ParquetWriter(
                temporary,
                schema,
                compression="zstd",
                use_dictionary=True,
                write_statistics=True,
            )

            logical_row_id = 0
            for record in records:
                current_row_id = logical_row_id
                logical_row_id += 1
                parsed, malformed_reason = _parse_data_record(
                    record, resolved_options, len(header), limits
                )
                if malformed_reason is not None:
                    if resolved_options.malformed is MalformedPolicy.FAIL:
                        raise DomainError(
                            ErrorCode.SCHEMA_INVALID,
                            "malformed CSV record",
                            context={
                                "logical_row_id": current_row_id,
                                "reason": malformed_reason,
                            },
                        )
                    skipped_rows += 1
                    reasons[malformed_reason] += 1
                    continue
                assert parsed is not None
                value_bytes = sum(
                    _encoded_length(value, resolved_options.encoding) for value in parsed
                )
                if row_ids and (
                    len(row_ids) >= limits.batch_rows
                    or buffered_value_bytes + value_bytes > limits.batch_value_bytes
                ):
                    flush_batch()
                for values, value in zip(columns, parsed, strict=True):
                    values.append(value)
                row_ids.append(current_row_id)
                buffered_value_bytes += value_bytes
                rows_written += 1
            flush_batch()
            writer.close()
            writer = None

        descriptor = os.open(temporary, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _fsync_directory(destination.parent)
        _publish_immutable(temporary, destination)
    except Exception:
        if writer is not None:
            with contextlib.suppress(Exception):
                writer.close()
        temporary.unlink(missing_ok=True)
        raise

    digest, size_bytes = _sha256_file(destination)
    assert header is not None
    return CsvConversionResult(
        output_path=destination,
        row_count=rows_written,
        column_count=len(header),
        skipped_rows=skipped_rows,
        header=header,
        sha256=digest,
        size_bytes=size_bytes,
        batches_written=batches_written,
        largest_batch_rows=largest_batch_rows,
        largest_batch_value_bytes=largest_batch_value_bytes,
        malformed_reasons=tuple(sorted(reasons.items())),
    )


def _decode_sample_candidates(sample: bytes, *, complete: bool) -> list[tuple[str, str]]:
    if sample.startswith(b"\xef\xbb\xbf"):
        encodings = ("utf-8-sig",)
    else:
        try:
            text = _decode_sample(sample, "utf-8", complete=complete)
        except UnicodeDecodeError:
            encodings = ("cp949", "euc-kr")
        else:
            return [("utf-8", text)]

    decoded: list[tuple[str, str]] = []
    for encoding in encodings:
        try:
            decoded.append((encoding, _decode_sample(sample, encoding, complete=complete)))
        except UnicodeDecodeError:
            continue
    return decoded


def _decode_sample(sample: bytes, encoding: str, *, complete: bool) -> str:
    decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
    return decoder.decode(sample, final=complete)


def _score_dialect(text: str, delimiter: str) -> tuple[tuple[int, int, int, int], int, int, int]:
    widths: list[int] = []
    reader = csv_module.reader(
        io.StringIO(text, newline=""),
        delimiter=delimiter,
        quotechar='"',
        doublequote=True,
        strict=True,
    )
    try:
        for row in reader:
            widths.append(len(row))
            if len(widths) >= 256:
                break
    except csv_module.Error:
        # A bounded proposal sample may end in the middle of a quoted logical
        # record. Complete records already observed remain useful evidence.
        pass
    if not widths:
        return (0, 0, 0, 0), 0, 0, 0
    counts = Counter(widths)
    detected_columns, consistent = max(counts.items(), key=lambda item: (item[1], item[0]))
    sampled = len(widths)
    multi_column = int(detected_columns > 1)
    consistency_per_mille = consistent * 1000 // sampled
    score = (multi_column, consistency_per_mille, detected_columns, consistent)
    return score, detected_columns, sampled, consistent


def _resolve_options(
    proposal: CsvParseProposal, options: CsvParseOptions | None
) -> CsvParseOptions:
    if options is None:
        if proposal.requires_confirmation:
            raise DomainError(
                ErrorCode.CSV_DIALECT_AMBIGUOUS,
                "CSV encoding or dialect is ambiguous and must be confirmed",
                context={
                    "candidates": [
                        {"encoding": item.encoding, "delimiter": item.delimiter}
                        for item in proposal.candidates
                        if (
                            item.detected_columns == proposal.recommended.detected_columns
                            and item.consistent_records == proposal.recommended.consistent_records
                            and item.sampled_records == proposal.recommended.sampled_records
                        )
                    ]
                },
            )
        item = proposal.recommended
        return CsvParseOptions(encoding=item.encoding, delimiter=item.delimiter)

    supported_candidate = any(
        candidate.encoding == options.encoding and candidate.delimiter == options.delimiter
        for candidate in proposal.candidates
    )
    if not supported_candidate:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            "confirmed CSV options do not match a parseable proposal candidate",
        )
    if proposal.requires_confirmation and not options.confirmed:
        raise DomainError(
            ErrorCode.CSV_DIALECT_AMBIGUOUS,
            "ambiguous CSV options require explicit confirmation",
        )
    return options


def _consume_utf8_bom(stream: BinaryIO, encoding: str) -> None:
    if encoding != "utf-8-sig":
        return
    if stream.read(3) != b"\xef\xbb\xbf":
        stream.seek(0)


def _read_and_validate_header(
    source: Path, options: CsvParseOptions, limits: CsvLimits
) -> tuple[str, ...]:
    with source.open("rb") as stream:
        _consume_utf8_bom(stream, options.encoding)
        records = _iter_logical_records(
            stream, ord(options.delimiter), limits.max_logical_record_bytes
        )
        try:
            record = next(records)
        except StopIteration:
            raise DomainError(ErrorCode.SCHEMA_INVALID, "CSV input is empty") from None
    return _parse_header_record(record, options, limits)


def _parse_header_record(
    record: _LogicalRecord, options: CsvParseOptions, limits: CsvLimits
) -> tuple[str, ...]:
    if record.malformed_reason is not None:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            "CSV header is malformed",
            context={"reason": record.malformed_reason},
        )
    try:
        text = record.content.decode(options.encoding, errors="strict")
        fields = _parse_record_text(text, options.delimiter)
    except (UnicodeDecodeError, csv_module.Error) as exc:
        raise DomainError(ErrorCode.SCHEMA_INVALID, "CSV header is malformed") from exc
    if not fields or all(field == "" for field in fields):
        raise DomainError(ErrorCode.SCHEMA_INVALID, "CSV header is empty")
    _validate_field_limits(fields, options.encoding, limits)
    if len(fields) > limits.max_columns:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            f"CSV exceeds the {limits.max_columns}-column limit",
            context={"columns": len(fields), "limit": limits.max_columns},
        )
    if any(field == "" for field in fields):
        raise DomainError(ErrorCode.SCHEMA_INVALID, "CSV header names must be non-empty")
    duplicate = next((name for name, count in Counter(fields).items() if count > 1), None)
    if duplicate is not None:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            "CSV header names must be unique",
            context={"duplicate_header": duplicate},
        )
    if ROW_ID_COLUMN in fields:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            f"CSV header uses reserved column {ROW_ID_COLUMN}",
        )
    return tuple(fields)


def _parse_data_record(
    record: _LogicalRecord,
    options: CsvParseOptions,
    expected_columns: int,
    limits: CsvLimits,
) -> tuple[list[str] | None, str | None]:
    if record.malformed_reason is not None:
        return None, record.malformed_reason
    try:
        text = record.content.decode(options.encoding, errors="strict")
    except UnicodeDecodeError:
        return None, "invalid_encoding"
    try:
        fields = _parse_record_text(text, options.delimiter)
    except csv_module.Error:
        return None, "invalid_csv_syntax"
    if len(fields) != expected_columns:
        return None, "column_count_mismatch"
    _validate_field_limits(fields, options.encoding, limits)
    return fields, None


def _parse_record_text(text: str, delimiter: str) -> list[str]:
    reader = csv_module.reader(
        io.StringIO(text, newline=""),
        delimiter=delimiter,
        quotechar='"',
        doublequote=True,
        strict=True,
    )
    try:
        row = next(reader)
    except StopIteration:
        return []
    try:
        next(reader)
    except StopIteration:
        return row
    raise csv_module.Error("logical record parsed as multiple rows")


def _validate_field_limits(fields: list[str], encoding: str, limits: CsvLimits) -> None:
    for index, field in enumerate(fields):
        field_bytes = _encoded_length(field, encoding)
        if field_bytes > limits.max_field_bytes:
            raise DomainError(
                ErrorCode.SCHEMA_INVALID,
                f"CSV field exceeds the {limits.max_field_bytes}-byte limit",
                context={"column_index": index, "field_bytes": field_bytes},
            )


def _encoded_length(value: str, encoding: str) -> int:
    logical_encoding = "utf-8" if encoding == "utf-8-sig" else encoding
    return len(value.encode(logical_encoding))


def _iter_logical_records(
    stream: BinaryIO,
    delimiter: int,
    max_record_bytes: int,
    *,
    read_size: int = 64 * 1024,
) -> Iterator[_LogicalRecord]:
    # States make quote recognition field-aware. An unexpected quote in an
    # unquoted field marks that record malformed but does not steal subsequent
    # physical lines, which permits deterministic fail|skip recovery.
    FIELD_START, UNQUOTED, QUOTED, AFTER_QUOTE = range(4)
    state = FIELD_START
    record = bytearray()
    malformed_reason: str | None = None
    skip_lf = False

    while chunk := stream.read(read_size):
        for byte in chunk:
            if skip_lf:
                skip_lf = False
                if byte == 0x0A:
                    continue
            record.append(byte)
            boundary = False

            if state == QUOTED:
                if byte == 0x22:
                    state = AFTER_QUOTE
            elif state == AFTER_QUOTE:
                if byte == 0x22:
                    state = QUOTED
                elif byte == delimiter:
                    state = FIELD_START
                elif byte in (0x0A, 0x0D):
                    boundary = True
                else:
                    malformed_reason = malformed_reason or "characters_after_closing_quote"
                    state = UNQUOTED
            elif state == FIELD_START:
                if byte == 0x22:
                    state = QUOTED
                elif byte == delimiter:
                    pass
                elif byte in (0x0A, 0x0D):
                    boundary = True
                else:
                    state = UNQUOTED
            else:  # UNQUOTED
                if byte == delimiter:
                    state = FIELD_START
                elif byte in (0x0A, 0x0D):
                    boundary = True
                elif byte == 0x22:
                    malformed_reason = malformed_reason or "quote_in_unquoted_field"

            if boundary:
                terminator = record.pop()
                if len(record) > max_record_bytes:
                    _raise_record_limit(len(record), max_record_bytes)
                yield _LogicalRecord(bytes(record), malformed_reason)
                record.clear()
                malformed_reason = None
                state = FIELD_START
                skip_lf = terminator == 0x0D
            elif len(record) > max_record_bytes:
                _raise_record_limit(len(record), max_record_bytes)

    if record:
        if state == QUOTED:
            malformed_reason = malformed_reason or "unterminated_quoted_field"
        yield _LogicalRecord(bytes(record), malformed_reason)


def _raise_record_limit(actual: int, limit: int) -> None:
    raise DomainError(
        ErrorCode.SCHEMA_INVALID,
        f"CSV logical record exceeds the {limit}-byte limit",
        context={"record_bytes": actual, "limit": limit},
    )


def _publish_immutable(temporary: Path, destination: Path) -> None:
    lock_path = destination.parent / ".publication.lock"
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        if destination.exists() or destination.is_symlink():
            raise DomainError(
                ErrorCode.IMMUTABLE_PATH_EXISTS,
                f"immutable artifact path already exists: {destination.name}",
            )
        os.rename(temporary, destination)
        _fsync_directory(destination.parent)
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size
