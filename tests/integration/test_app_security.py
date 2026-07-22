from __future__ import annotations
import asyncio
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader

from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from sts.api.app import create_app
from sts.api.security import CSP_POLICY, SESSION_COOKIE_NAME, validate_bind_host

PORT = 8765
BASE_URL = f"http://127.0.0.1:{PORT}"
ORIGIN = BASE_URL


def _build_app(tmp_path: Path, *, static_dir: Path | None = None):
    return create_app(
        tmp_path / "workspace",
        public_port=PORT,
        static_dir=static_dir or tmp_path / "missing-dist",
    )


def _bootstrap(client: TestClient) -> str:
    response = client.get("/api/v1/bootstrap")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    return client.cookies[SESSION_COOKIE_NAME]


def test_bootstrap_sets_only_host_cookie_and_never_exposes_token(
    tmp_path: Path,
) -> None:
    app = _build_app(tmp_path)
    with TestClient(app, base_url=BASE_URL) as client:
        response = client.get("/api/v1/bootstrap")
        token = client.cookies[SESSION_COOKIE_NAME]

        assert response.status_code == 200
        assert len(token) >= 32
        assert token not in response.text
        assert token not in str(response.url)
        assert "location" not in response.headers
        cookie = response.headers["set-cookie"]
        assert cookie.startswith(f"{SESSION_COOKIE_NAME}=")
        assert "HttpOnly" in cookie
        assert "SameSite=strict" in cookie
        assert "Path=/" in cookie
        assert "Domain=" not in cookie
        assert response.headers["content-security-policy"] == CSP_POLICY
        assert "access-control-allow-origin" not in response.headers


def test_session_cookie_is_invalid_after_application_restart(tmp_path: Path) -> None:
    first = _build_app(tmp_path)
    with TestClient(first, base_url=BASE_URL) as client:
        stale_token = _bootstrap(client)

    second = _build_app(tmp_path)
    with TestClient(second, base_url=BASE_URL) as client:
        response = client.get(
            f"/api/v1/datasets/{uuid4()}",
            headers={"Cookie": f"{SESSION_COOKIE_NAME}={stale_token}"},
        )
        assert response.status_code == 401
        assert response.json()["code"] == "SESSION_REQUIRED"
        assert response.headers["content-security-policy"] == CSP_POLICY


def test_host_and_exact_origin_are_enforced_before_router_dispatch(
    tmp_path: Path,
) -> None:
    app = _build_app(tmp_path)
    upload = {"filename": "fixture.csv", "size_bytes": 0, "source_format": "csv"}
    with TestClient(app, base_url=BASE_URL) as client:
        rejected_host = client.get(
            "/api/v1/bootstrap",
            headers={"Host": "attacker.example"},
        )
        assert rejected_host.status_code == 403
        assert rejected_host.json()["code"] == "HOST_REJECTED"

        _bootstrap(client)
        missing_origin = client.post("/api/v1/datasets/uploads", json=upload)
        assert missing_origin.status_code == 403
        assert missing_origin.json()["code"] == "ORIGIN_REJECTED"

        prefix_origin = client.post(
            "/api/v1/datasets/uploads",
            json=upload,
            headers={"Origin": f"{ORIGIN}.attacker.example"},
        )
        assert prefix_origin.status_code == 403
        assert prefix_origin.json()["code"] == "ORIGIN_REJECTED"

        valid = client.post(
            "/api/v1/datasets/uploads",
            json=upload,
            headers={"Origin": ORIGIN},
        )
        assert valid.status_code == 201
        assert valid.json()["state"] == "uploading"


def test_eventsource_and_download_routes_require_the_cookie_session(
    tmp_path: Path,
) -> None:
    app = _build_app(tmp_path)
    job_id = uuid4()
    artifact_id = uuid4()
    with TestClient(app, base_url=BASE_URL) as client:
        token = _bootstrap(client)
        client.cookies.clear()

        event_rejected = client.get(f"/api/v1/jobs/{job_id}/events")
        download_rejected = client.get(f"/api/v1/artifacts/{artifact_id}/download")
        assert event_rejected.status_code == 401
        assert download_rejected.status_code == 401
        assert event_rejected.json()["code"] == "SESSION_REQUIRED"
        assert download_rejected.json()["code"] == "SESSION_REQUIRED"

        cookie = {"Cookie": f"{SESSION_COOKIE_NAME}={token}"}
        event_dispatched = client.get(f"/api/v1/jobs/{job_id}/events", headers=cookie)
        download_dispatched = client.get(
            f"/api/v1/artifacts/{artifact_id}/download",
            headers=cookie,
        )
        assert event_dispatched.status_code == 404
        assert event_dispatched.json()["code"] == "JOB_NOT_FOUND"
        assert download_dispatched.status_code == 404
        assert download_dispatched.json()["code"] == "ARTIFACT_NOT_FOUND"


def test_security_headers_no_cors_and_all_router_families_are_mounted(
    tmp_path: Path,
) -> None:
    app = _build_app(tmp_path)
    paths: set[str | None] = set()
    for route in app.routes:
        paths.add(getattr(route, "path", None))
        included = getattr(route, "original_router", None)
        if included is not None:
            paths.update(getattr(child, "path", None) for child in included.routes)
    assert "/api/v1/datasets/uploads" in paths
    assert "/api/v1/datasets/{dataset_id}/rules" in paths
    assert "/api/v1/jobs" in paths
    assert "/api/v1/jobs/{job_id}/artifacts" in paths
    assert "/api/v1/artifacts/{artifact_id}/download" in paths
    assert "/api/v1/jobs/{job_id}/events" in paths

    with TestClient(app, base_url=BASE_URL) as client:
        response = client.get(
            "/api/v1/bootstrap",
            headers={"Origin": "http://attacker.example"},
        )
        assert response.status_code == 200
        assert response.headers["content-security-policy"] == CSP_POLICY
        assert response.headers["x-content-type-options"] == "nosniff"
        assert response.headers["referrer-policy"] == "no-referrer"
        assert response.headers["x-frame-options"] == "DENY"
        assert not any(name.startswith("access-control-") for name in response.headers)


def test_built_web_files_and_spa_routes_use_the_static_fallback(tmp_path: Path) -> None:
    static_dir = tmp_path / "dist"
    static_dir.mkdir()
    (static_dir / "index.html").write_text(
        "<!doctype html><title>Studio</title>", encoding="utf-8"
    )
    (static_dir / "app.js").write_text("window.studio = true;", encoding="utf-8")
    app = _build_app(tmp_path, static_dir=static_dir)

    with TestClient(app, base_url=BASE_URL) as client:
        root = client.get("/")
        client_route = client.get("/reports/current")
        asset = client.get("/app.js")
        missing_asset = client.get("/missing.js")
        unknown_api = client.get("/api/v1/not-a-route")

        assert root.status_code == 200
        assert client_route.status_code == 200
        assert "<title>Studio</title>" in client_route.text
        assert asset.status_code == 200
        assert asset.text == "window.studio = true;"
        assert missing_asset.status_code == 404
        assert unknown_api.status_code == 401
        assert root.headers["content-security-policy"] == CSP_POLICY


def test_bind_policy_is_loopback_only_without_explicit_unsafe_flag() -> None:
    assert validate_bind_host("127.0.0.1") == "127.0.0.1"
    assert validate_bind_host("::1") == "::1"
    assert validate_bind_host("[::1]") == "[::1]"

    with pytest.raises(ValueError, match="non-loopback"):
        validate_bind_host("0.0.0.0")
    with pytest.raises(ValueError, match="loopback IP"):
        validate_bind_host("localhost")

    assert validate_bind_host("0.0.0.0", unsafe_allow_non_loopback=True) == "0.0.0.0"
    assert (
        validate_bind_host("studio.internal", unsafe_allow_non_loopback=True)
        == "studio.internal"
    )


@pytest.mark.asyncio
async def test_serve_runner_honors_asgi_lifespan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STS_SERVE_VENV", "1")
    serve_path = Path(__file__).resolve().parents[2] / "scripts" / "serve"
    loader = SourceFileLoader("_sts_serve_lifespan_test", str(serve_path))
    spec = spec_from_loader(loader.name, loader)
    assert spec is not None
    serve_module = module_from_spec(spec)
    loader.exec_module(serve_module)

    lifecycle: list[str] = []
    started = False

    async def tiny_app(scope, receive, send) -> None:
        nonlocal started
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    started = True
                    lifecycle.append("startup")
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    lifecycle.append("shutdown")
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        else:
            assert scope["type"] == "http"
            assert started
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-length", b"2")],
                }
            )
            await send({"type": "http.response.body", "body": b"ok"})

    server = serve_module.ASGIHTTPServer(tiny_app, "127.0.0.1", 0)
    await server.start()
    try:
        assert lifecycle == ["startup"]
        assert server._server is not None
        socket_address = server._server.sockets[0].getsockname()
        reader, writer = await asyncio.open_connection("127.0.0.1", socket_address[1])
        writer.write(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        await writer.drain()
        response = await reader.read()
        writer.close()
        await writer.wait_closed()
        assert response.startswith(b"HTTP/1.1 200 OK\r\n")
        assert response.endswith(b"\r\n\r\nok")
        assert lifecycle == ["startup"]
    finally:
        await server.close()

    assert lifecycle == ["startup", "shutdown"]
