from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi import Request
from fastapi.responses import StreamingResponse
from starlette.concurrency import run_in_threadpool

from sts.domain import DomainError, ErrorCode
from sts.storage import CatalogRepository
from sts.storage.repository import EventRecord, OwnerType

SSE_MEDIA_TYPE = "text/event-stream"
DEFAULT_HEARTBEAT_SECONDS = 15.0
DEFAULT_RETENTION_DAYS = 30


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _event_payload(record: EventRecord) -> str:
    data = {
        "attempt": record.attempt,
        "sequence": record.sequence,
        "timestamp": record.timestamp,
        "terminal": record.terminal,
        **record.payload,
    }
    event_name = "terminal" if record.terminal else "progress"
    encoded = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return f"id: {record.event_id}\nevent: {event_name}\ndata: {encoded}\n\n"


def parse_last_event_id(request: Request) -> int:
    raw = request.headers.get("Last-Event-ID", "0").strip()
    if not raw:
        return 0
    try:
        value = int(raw)
    except ValueError as exc:
        raise DomainError(
            ErrorCode.SCHEMA_INVALID,
            "Last-Event-ID must be a non-negative integer",
        ) from exc
    if value < 0:
        raise DomainError(ErrorCode.SCHEMA_INVALID, "Last-Event-ID must be non-negative")
    return value


async def dataset_event_stream(
    repository: CatalogRepository,
    dataset_id: UUID | str,
    *,
    after_event_id: int = 0,
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
    poll_seconds: float = 0.1,
    retention_days: int = DEFAULT_RETENTION_DAYS,
) -> AsyncIterator[str]:
    repository.get_dataset(dataset_id)
    cursor = after_event_id
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    heartbeat_at = asyncio.get_running_loop().time() + heartbeat_seconds
    while True:
        events = await run_in_threadpool(
            repository.replay_events,
            OwnerType.DATASET,
            dataset_id,
            after_event_id=cursor,
        )
        for event in events:
            cursor = event.event_id
            if _timestamp(event.timestamp) < cutoff:
                continue
            yield _event_payload(event)
            if event.terminal:
                return

        now = asyncio.get_running_loop().time()
        if now >= heartbeat_at:
            yield ": heartbeat\n\n"
            heartbeat_at = now + heartbeat_seconds
        await asyncio.sleep(poll_seconds)


def dataset_event_response(
    repository: CatalogRepository,
    dataset_id: UUID | str,
    request: Request,
    *,
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
    poll_seconds: float = 0.1,
) -> StreamingResponse:
    last_event_id = parse_last_event_id(request)
    stream = dataset_event_stream(
        repository,
        dataset_id,
        after_event_id=last_event_id,
        heartbeat_seconds=heartbeat_seconds,
        poll_seconds=poll_seconds,
    )
    return StreamingResponse(
        stream,
        media_type=SSE_MEDIA_TYPE,
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
