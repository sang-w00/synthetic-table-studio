from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import PurePosixPath
from typing import Any
from uuid import UUID

from pydantic import BaseModel


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("canonical JSON does not support non-finite decimals")
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    rendered = format(normalized, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _canonical_value(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _canonical_value(value.model_dump(mode="json", by_alias=True, exclude_none=False))
    if isinstance(value, Enum):
        return _canonical_value(value.value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, PurePosixPath):
        return value.as_posix()
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("canonical datetimes must be timezone-aware")
        utc = value.astimezone(UTC)
        rendered = utc.isoformat(timespec="microseconds").replace("+00:00", "Z")
        return rendered
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return _canonical_decimal(value)
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical JSON does not support non-finite floats")
        if value == 0:
            return 0
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical JSON object keys must be strings")
            canonical_key = unicodedata.normalize("NFC", key)
            if canonical_key in normalized:
                raise ValueError("object keys collide after Unicode normalization")
            normalized[canonical_key] = _canonical_value(item)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, memoryview)):
        return [_canonical_value(item) for item in value]
    raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize JSON deterministically for persisted identities.

    Object keys and strings are NFC-normalized, object keys are sorted, insignificant
    whitespace is omitted, and non-finite numeric values are rejected.
    """

    normalized = _canonical_value(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_json_text(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def require_versioned_json(value: Any) -> None:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", by_alias=True, exclude_none=False)
    if not isinstance(value, Mapping) or not isinstance(value.get("version"), str):
        raise ValueError("persisted JSON must be an object with a string version field")
    if not value["version"]:
        raise ValueError("persisted JSON version must not be empty")


class CanonicalModel(BaseModel):
    model_config = {"extra": "forbid", "frozen": True, "str_strip_whitespace": True}

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self)

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()
