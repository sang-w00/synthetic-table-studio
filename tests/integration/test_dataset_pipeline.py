from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from sts.api import (
    DatasetService,
    create_dataset_router,
    dataset_event_stream,
    install_problem_handlers,
)
from sts.storage import CatalogRepository, WorkspaceLayout


@pytest.fixture
def dataset_client(tmp_path: Path):
    layout = WorkspaceLayout(tmp_path / "workspace")
    repository = CatalogRepository.open_workspace(layout)
    service = DatasetService(repository, layout, duckdb_memory_limit="256MB")
    app = FastAPI()
    install_problem_handlers(app)
    app.include_router(create_dataset_router(service))
    with TestClient(app) as client:
        yield client, service, layout
    repository.close()


def _create_and_inspect_csv(client: TestClient, content: bytes) -> tuple[str, dict]:
    created = client.post(
        "/api/v1/datasets/uploads",
        json={
            "filename": "fixture.csv",
            "size_bytes": len(content),
            "source_format": "csv",
        },
    )
    assert created.status_code == 201
    dataset_id = created.json()["dataset_id"]

    head = client.head(f"/api/v1/datasets/{dataset_id}/content")
    assert head.status_code == 204
    assert head.headers["Upload-Offset"] == "0"

    split = len(content) // 2
    first = client.patch(
        f"/api/v1/datasets/{dataset_id}/content",
        content=content[:split],
        headers={"Upload-Offset": "0", "Content-Type": "application/octet-stream"},
    )
    assert first.status_code == 204
    assert first.headers["Upload-Offset"] == str(split)
    second = client.patch(
        f"/api/v1/datasets/{dataset_id}/content",
        content=content[split:],
        headers={
            "Upload-Offset": str(split),
            "Content-Type": "application/octet-stream",
        },
    )
    assert second.status_code == 204
    assert second.headers["Upload-Offset"] == str(len(content))

    completed = client.post(
        f"/api/v1/datasets/{dataset_id}/complete",
        json={"sha256": hashlib.sha256(content).hexdigest()},
    )
    assert completed.status_code == 202
    state = completed.json()["state"]
    if state == "parse_options_required":
        proposal_response = client.get(f"/api/v1/datasets/{dataset_id}/parse-options")
        assert proposal_response.status_code == 200
        proposal = proposal_response.json()["proposal"]["recommended"]
        confirmed = client.put(
            f"/api/v1/datasets/{dataset_id}/parse-options",
            json={
                "encoding": proposal["encoding"],
                "delimiter": proposal["delimiter"],
                "malformed": "fail",
            },
        )
        assert confirmed.status_code == 200
        assert confirmed.json()["state"] == "raw_ready"
        parse_proposal = proposal_response.json()
    else:
        assert state == "raw_ready"
        parse_proposal = {}
    return dataset_id, parse_proposal


def _valid_schema(*, bad_amount: bool = False) -> list[dict]:
    return [
        {"name": "code", "kind": "categorical", "nullable": False, "role": "model"},
        {
            "name": "amount",
            "kind": "boolean" if bad_amount else "integer",
            "nullable": True,
            "role": "model",
        },
        {"name": "ratio", "kind": "float", "nullable": False, "role": "model"},
        {"name": "when", "kind": "date", "nullable": False, "role": "model"},
        {"name": "flag", "kind": "boolean", "nullable": False, "role": "model"},
        {"name": "name", "kind": "categorical", "nullable": False, "role": "model"},
    ]


def test_high_cardinality_values_are_proposed_as_identifiers_for_confirmation(
    dataset_client,
) -> None:
    client, _, _ = dataset_client
    content = b"Index,note\n" + b"".join(
        f"Row{index},group-{index % 3}\n".encode() for index in range(400)
    )
    dataset_id, _ = _create_and_inspect_csv(client, content)

    assert client.post(f"/api/v1/datasets/{dataset_id}/profile").status_code == 202
    profile = client.get(f"/api/v1/datasets/{dataset_id}/profile?view=raw").json()
    columns = {column["name"]: column for column in profile["columns"]}

    assert columns["Index"]["candidate_type"] == "identifier"
    assert columns["Index"]["candidate_requires_confirmation"] is True
    assert columns["Index"]["candidate_alternatives"] == ["text"]
    assert columns["note"]["candidate_type"] == "categorical"


def test_real_csv_upload_profile_schema_normalize_and_sse(dataset_client) -> None:
    client, service, layout = dataset_client
    content = (
        b"code,amount,ratio,when,flag,name\n"
        b"001,10,1.5,2026-01-01,true,Ana\n"
        b"002,20,2.5,2026-01-02,false,Bob\n"
        b"003,,3.5,2026-01-03,true,Ana\n"
    )
    dataset_id, _ = _create_and_inspect_csv(client, content)

    invalid_state = client.post(f"/api/v1/datasets/{dataset_id}/normalize")
    assert invalid_state.status_code == 409
    assert invalid_state.headers["content-type"].startswith("application/problem+json")
    assert invalid_state.json()["code"] == "INVALID_STATE"

    started = client.post(f"/api/v1/datasets/{dataset_id}/profile")
    assert started.status_code == 202
    assert started.json()["state"] == "profiled"
    raw_profile = client.get(f"/api/v1/datasets/{dataset_id}/profile?view=raw")
    assert raw_profile.status_code == 200
    profile = raw_profile.json()
    assert profile["row_count"] == 3
    assert profile["column_count"] == 6
    columns = {column["name"]: column for column in profile["columns"]}
    assert columns["amount"]["null_count"] == 1
    assert columns["amount"]["parse_success"]["integer"] == 2
    assert columns["name"]["exact_low_cardinality"] == [
        {"value": "Ana", "count": 2},
        {"value": "Bob", "count": 1},
    ]
    assert columns["code"]["fixed_length"] == 3
    assert columns["code"]["candidate_type"] == "categorical"
    assert columns["code"]["candidate_requires_confirmation"] is True
    assert "integer" in columns["code"]["candidate_alternatives"]

    wrong_order = _valid_schema()
    wrong_order[0], wrong_order[1] = wrong_order[1], wrong_order[0]
    rejected = client.put(
        f"/api/v1/datasets/{dataset_id}/schema",
        json={"columns": wrong_order},
    )
    assert rejected.status_code == 422
    assert rejected.headers["content-type"].startswith("application/problem+json")
    assert rejected.json()["code"] == "SCHEMA_INVALID"

    schema = client.put(
        f"/api/v1/datasets/{dataset_id}/schema",
        json={"columns": _valid_schema()},
    )
    assert schema.status_code == 200
    assert schema.json()["state"] == "schema_ready"

    rules_body = {
        "rules": [
            {
                "id": "code-domain",
                "kind": "allowed_values",
                "provenance": "public",
                "source_action": "block",
                "column": "code",
                "values": ["001", "002", "003"],
            }
        ]
    }
    rules = client.put(f"/api/v1/datasets/{dataset_id}/rules", json=rules_body)
    assert rules.status_code == 200
    assert rules.json()["state"] == "schema_ready"
    assert rules.json()["rule_count"] == 1
    assert len(rules.json()["rules_sha256"]) == 64

    normalized = client.post(f"/api/v1/datasets/{dataset_id}/normalize")
    assert normalized.status_code == 202
    assert normalized.json()["state"] == "normalized"
    status_response = client.get(f"/api/v1/datasets/{dataset_id}")
    assert status_response.json()["state"] == "normalized"
    assert status_response.json()["legal_actions"] == []
    rules_after_normalize = client.put(
        f"/api/v1/datasets/{dataset_id}/rules",
        json=rules_body,
    )
    assert rules_after_normalize.status_code == 409
    assert rules_after_normalize.headers["content-type"].startswith(
        "application/problem+json"
    )
    assert rules_after_normalize.json()["code"] == "INVALID_STATE"

    typed = client.get(f"/api/v1/datasets/{dataset_id}/profile?view=typed")
    assert typed.status_code == 200
    typed_columns = {column["name"]: column for column in typed.json()["columns"]}
    assert typed_columns["amount"]["storage_type"] == "BIGINT"
    assert typed_columns["ratio"]["storage_type"] == "DOUBLE"
    assert typed_columns["when"]["storage_type"] == "DATE"

    manifest = service.repository.get_dataset_manifest(dataset_id)
    assert manifest.normalized is not None
    assert manifest.rules_sha256 == rules.json()["rules_sha256"]
    assert manifest.rules_version == rules.json()["rules_version"]
    rules_path = layout.resolve_relative(
        str(manifest.metadata["rules_relative_path"]),
        require_exists=True,
    )
    assert rules_path.name == f"rules-{manifest.rules_sha256}.json"
    normalized_path = layout.resolve_relative(
        manifest.normalized.relative_path, require_exists=True
    )
    parquet = pq.ParquetFile(normalized_path)
    assert parquet.schema_arrow.names == [
        "__sts_row_id",
        "code",
        "amount",
        "ratio",
        "when",
        "flag",
        "name",
    ]
    assert parquet.metadata.num_rows == 3
    assert parquet.metadata.row_group(0).column(0).compression == "ZSTD"

    replay = client.get(f"/api/v1/datasets/{dataset_id}/events")
    assert replay.status_code == 200
    assert replay.headers["content-type"].startswith("text/event-stream")
    terminal_events = re.findall(r"^event: terminal$", replay.text, flags=re.MULTILINE)
    assert len(terminal_events) == 1
    ids = [
        int(value)
        for value in re.findall(r"^id: (\d+)$", replay.text, flags=re.MULTILINE)
    ]
    assert ids == sorted(ids)
    assert len(ids) >= 4

    resumed = client.get(
        f"/api/v1/datasets/{dataset_id}/events",
        headers={"Last-Event-ID": str(ids[-3])},
    )
    resumed_ids = [
        int(value)
        for value in re.findall(r"^id: (\d+)$", resumed.text, flags=re.MULTILINE)
    ]
    assert resumed_ids
    assert all(value > ids[-3] for value in resumed_ids)
    assert resumed_ids[-1] == ids[-1]


def test_normalization_failure_is_retryable_and_validation_is_problem_json(
    dataset_client,
) -> None:
    client, _, _ = dataset_client
    content = (
        b"code,amount,ratio,when,flag,name\n"
        b"001,10,1.5,2026-01-01,true,Ana\n"
        b"002,20,2.5,2026-01-02,false,Bob\n"
    )
    dataset_id, _ = _create_and_inspect_csv(client, content)
    assert client.post(f"/api/v1/datasets/{dataset_id}/profile").status_code == 202

    malformed_request = client.put(
        f"/api/v1/datasets/{dataset_id}/schema",
        json={
            "columns": [
                {
                    "name": "code",
                    "kind": "fixed_decimal",
                    "nullable": False,
                    "role": "model",
                }
            ]
        },
    )
    assert malformed_request.status_code == 422
    assert malformed_request.headers["content-type"].startswith(
        "application/problem+json"
    )
    assert malformed_request.json()["code"] == "SCHEMA_INVALID"

    accepted = client.put(
        f"/api/v1/datasets/{dataset_id}/schema",
        json={"columns": _valid_schema(bad_amount=True)},
    )
    assert accepted.status_code == 200
    invalid_rules = client.put(
        f"/api/v1/datasets/{dataset_id}/rules",
        json={
            "rules": [
                {
                    "id": "missing-provenance",
                    "kind": "not_null",
                    "column": "code",
                }
            ]
        },
    )
    assert invalid_rules.status_code == 422
    assert invalid_rules.headers["content-type"].startswith("application/problem+json")
    assert invalid_rules.json()["code"] == "SCHEMA_INVALID"
    failed = client.post(f"/api/v1/datasets/{dataset_id}/normalize")
    assert failed.status_code == 422
    assert failed.headers["content-type"].startswith("application/problem+json")
    assert failed.json()["code"] == "SCHEMA_INVALID"
    status_response = client.get(f"/api/v1/datasets/{dataset_id}")
    assert status_response.json()["state"] == "failed"
    assert status_response.json()["attempt"] == 1

    retried = client.post(f"/api/v1/datasets/{dataset_id}/retry")
    assert retried.status_code == 202
    assert retried.json()["state"] == "normalizing"
    assert retried.json()["attempt"] == 2

    illegal_retry = client.post(f"/api/v1/datasets/{dataset_id}/retry")
    assert illegal_retry.status_code == 409
    assert illegal_retry.headers["content-type"].startswith("application/problem+json")
    assert illegal_retry.json()["code"] == "INVALID_STATE"


@pytest.mark.asyncio
async def test_sse_heartbeat_after_replay_cursor(dataset_client) -> None:
    client, service, _ = dataset_client
    created = client.post(
        "/api/v1/datasets/uploads",
        json={"filename": "pending.csv", "size_bytes": 1, "source_format": "csv"},
    )
    assert created.status_code == 201
    dataset_id = created.json()["dataset_id"]
    latest = service.repository.replay_events("dataset", dataset_id)[-1].event_id
    stream = dataset_event_stream(
        service.repository,
        dataset_id,
        after_event_id=latest,
        heartbeat_seconds=0.001,
        poll_seconds=0.001,
    )
    try:
        assert await anext(stream) == ": heartbeat\n\n"
    finally:
        await stream.aclose()

    invalid_cursor = client.get(
        f"/api/v1/datasets/{dataset_id}/events",
        headers={"Last-Event-ID": "not-an-integer"},
    )
    assert invalid_cursor.status_code == 422
    assert invalid_cursor.headers["content-type"].startswith("application/problem+json")
