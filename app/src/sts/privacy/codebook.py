from __future__ import annotations

import math
import random
from bisect import bisect_right
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from sts.domain import DomainError, ErrorCode

from .metadata import (
    PublicBinnedCodebook,
    PublicCategoricalCodebook,
    PublicColumnCodebook,
    PublicMetadataManifest,
)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _category_key(value: object) -> tuple[type[object], object]:
    return type(value), value


def _ordered_value(column: PublicBinnedCodebook, value: object) -> Decimal | date | datetime:
    if column.kind in {"integer", "fixed_decimal", "float"}:
        if isinstance(value, bool):
            raise ValueError("boolean values are not numeric modeled values")
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("numeric modeled values must be finite")
        try:
            parsed = Decimal(str(value))
        except Exception as error:
            raise ValueError(f"value is not numeric: {value!r}") from error
        if not parsed.is_finite():
            raise ValueError("numeric modeled values must be finite")
        if column.kind == "integer" and parsed != parsed.to_integral_value():
            raise ValueError("integer modeled values must be integral")
        return parsed
    if column.kind == "date":
        if isinstance(value, datetime):
            raise ValueError("datetime values are not valid date values")
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return date.fromisoformat(value)
        raise ValueError("date modeled values must be dates or ISO-8601 strings")
    if isinstance(value, str):
        parsed_datetime = datetime.fromisoformat(value.replace("Z", "+00:00"))
    elif isinstance(value, datetime):
        parsed_datetime = value
    else:
        raise ValueError("datetime modeled values must be datetimes or ISO-8601 strings")
    if parsed_datetime.tzinfo is None or parsed_datetime.utcoffset() is None:
        raise ValueError("datetime modeled values require an explicit timezone")
    return parsed_datetime.astimezone(UTC)


def _datetime_micros(value: datetime) -> int:
    delta = value - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


class DiscreteCodebook:
    """Public-only reversible encoder for the discrete dpmm domain.

    Category order, bin boundaries, missing states, and within-bin sampling are all
    taken from the validated public manifest. No source-dependent discovery occurs.
    """

    def __init__(self, manifest: PublicMetadataManifest) -> None:
        self.manifest = manifest
        self._columns = {column.name: column for column in manifest.columns}
        self._category_codes = {
            column.name: {
                _category_key(value): code for code, value in enumerate(column.categories)
            }
            for column in manifest.columns
            if isinstance(column, PublicCategoricalCodebook)
        }

    @property
    def domain(self) -> dict[str, int]:
        return {column.name: column.state_count for column in self.manifest.columns}

    def encode_value(self, column_name: str, value: object) -> int:
        column = self._column(column_name)
        if value is None:
            if not column.nullable:
                raise DomainError(
                    ErrorCode.OUTPUT_INVALID,
                    f"null is outside the public codebook for {column_name!r}",
                )
            return column.state_count - 1
        if isinstance(column, PublicCategoricalCodebook):
            code = self._category_codes[column_name].get(_category_key(value))
            if code is None:
                raise DomainError(
                    ErrorCode.OUTPUT_INVALID,
                    f"value is outside the public categories for {column_name!r}",
                )
            return code
        try:
            ordered = _ordered_value(column, value)
        except (ValueError, TypeError) as error:
            raise DomainError(
                ErrorCode.OUTPUT_INVALID,
                f"value is invalid for the public bins of {column_name!r}: {error}",
            ) from error
        edges = column.ordered_bins
        if ordered < edges[0] or ordered > edges[-1]:
            raise DomainError(
                ErrorCode.OUTPUT_INVALID,
                f"value is outside the public bins for {column_name!r}",
            )
        code = bisect_right(edges, ordered) - 1
        return min(code, len(edges) - 2)

    def encode_row(self, row: Mapping[str, object]) -> dict[str, int]:
        missing = [name for name in self._columns if name not in row]
        if missing:
            raise DomainError(
                ErrorCode.OUTPUT_INVALID,
                "modeled row is missing public-codebook columns",
                context={"missing_columns": missing},
            )
        return {name: self.encode_value(name, row[name]) for name in self._columns}

    def encode_rows(self, rows: Iterable[Mapping[str, object]]) -> list[dict[str, int]]:
        return [self.encode_row(row) for row in rows]

    def decode_value(self, column_name: str, code: int, *, rng: random.Random) -> object:
        column = self._column(column_name)
        if (
            isinstance(code, bool)
            or not isinstance(code, int)
            or not 0 <= code < column.state_count
        ):
            raise DomainError(
                ErrorCode.OUTPUT_INVALID,
                f"discrete code is outside the public domain for {column_name!r}",
            )
        if column.nullable and code == column.state_count - 1:
            return None
        if isinstance(column, PublicCategoricalCodebook):
            return column.categories[code]
        return self._sample_public_bin(column, code, rng)

    def decode_row(self, row: Mapping[str, int], *, sampling_seed: int) -> dict[str, object]:
        if isinstance(sampling_seed, bool) or not 0 <= sampling_seed <= 2**64 - 1:
            raise ValueError("sampling_seed must be an unsigned 64-bit integer")
        missing = [name for name in self._columns if name not in row]
        if missing:
            raise DomainError(
                ErrorCode.OUTPUT_INVALID,
                "discrete row is missing public-codebook columns",
                context={"missing_columns": missing},
            )
        rng = random.Random(sampling_seed)
        return {name: self.decode_value(name, row[name], rng=rng) for name in self._columns}

    def decode_rows(
        self,
        rows: Iterable[Mapping[str, int]],
        *,
        sampling_seed: int,
    ) -> list[dict[str, object]]:
        if isinstance(sampling_seed, bool) or not 0 <= sampling_seed <= 2**64 - 1:
            raise ValueError("sampling_seed must be an unsigned 64-bit integer")
        rng = random.Random(sampling_seed)
        return [
            {name: self.decode_value(name, row[name], rng=rng) for name in self._columns}
            for row in rows
        ]

    def _column(self, name: str) -> PublicColumnCodebook:
        try:
            return self._columns[name]
        except KeyError as error:
            raise KeyError(f"unknown public codebook column: {name}") from error

    @staticmethod
    def _sample_public_bin(
        column: PublicBinnedCodebook,
        code: int,
        rng: random.Random,
    ) -> object:
        # Uniform is currently the only accepted public distribution. Keeping the
        # dispatch explicit prevents an unvalidated source-derived fallback.
        if column.within_bin.kind != "uniform":  # pragma: no cover - model is fail-closed
            raise RuntimeError("unsupported public within-bin distribution")
        lower = column.ordered_bins[code]
        upper = column.ordered_bins[code + 1]
        final_bin = code == len(column.bins) - 2
        if column.kind == "integer":
            low_int = int(lower)
            high_int = int(upper) if final_bin else int(upper) - 1
            if high_int < low_int:
                raise DomainError(ErrorCode.OUTPUT_INVALID, "public integer bin has no values")
            return rng.randint(low_int, high_int)
        if column.kind == "fixed_decimal":
            scale = 10 ** (column.decimal_places or 0)
            low_scaled = int(Decimal(lower) * scale)
            high_scaled = int(Decimal(upper) * scale)
            if not final_bin:
                high_scaled -= 1
            if high_scaled < low_scaled:
                raise DomainError(
                    ErrorCode.OUTPUT_INVALID, "public fixed-decimal bin has no values"
                )
            return Decimal(rng.randint(low_scaled, high_scaled)) / Decimal(scale)
        if column.kind == "float":
            low_float = float(lower)
            high_float = float(upper)
            return low_float + (high_float - low_float) * rng.random()
        if column.kind == "date":
            low_ordinal = lower.toordinal()
            high_ordinal = upper.toordinal() if final_bin else upper.toordinal() - 1
            if high_ordinal < low_ordinal:
                raise DomainError(ErrorCode.OUTPUT_INVALID, "public date bin has no values")
            return date.fromordinal(rng.randint(low_ordinal, high_ordinal))
        low_micros = _datetime_micros(lower)
        high_micros = _datetime_micros(upper)
        if not final_bin:
            high_micros -= 1
        if high_micros < low_micros:
            raise DomainError(ErrorCode.OUTPUT_INVALID, "public datetime bin has no values")
        return _EPOCH + timedelta(microseconds=rng.randint(low_micros, high_micros))


__all__ = ["DiscreteCodebook"]
