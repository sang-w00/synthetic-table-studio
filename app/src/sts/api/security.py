from __future__ import annotations

import hmac
import ipaddress
import secrets
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from http.cookies import SimpleCookie
from typing import Any

from fastapi.responses import JSONResponse

from sts.domain import DomainError, ErrorCode

SESSION_COOKIE_NAME = "sts_session"
MUTATION_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
CSP_POLICY = (
    "default-src 'self'; connect-src 'self'; img-src 'self' data:; "
    "style-src 'self' 'unsafe-inline'; object-src 'none'; frame-ancestors 'none'"
)
SECURITY_HEADERS: tuple[tuple[bytes, bytes], ...] = (
    (b"content-security-policy", CSP_POLICY.encode("ascii")),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"no-referrer"),
    (b"permissions-policy", b"camera=(), microphone=(), geolocation=()"),
    (b"cross-origin-opener-policy", b"same-origin"),
    (b"x-frame-options", b"DENY"),
)


def loopback_hosts(port: int) -> frozenset[str]:
    _validate_port(port)
    return frozenset({f"127.0.0.1:{port}", f"[::1]:{port}"})


def loopback_origins(port: int) -> frozenset[str]:
    _validate_port(port)
    return frozenset({f"http://127.0.0.1:{port}", f"http://[::1]:{port}"})


def validate_bind_host(host: str, *, unsafe_allow_non_loopback: bool = False) -> str:
    """Reject non-IP and non-loopback binds unless the operator opts into the risk."""

    candidate = host.strip()
    address_text = (
        candidate[1:-1] if candidate.startswith("[") and candidate.endswith("]") else candidate
    )
    try:
        address = ipaddress.ip_address(address_text)
    except ValueError as error:
        if unsafe_allow_non_loopback and candidate:
            return candidate
        raise ValueError(
            "bind host must be a loopback IP; pass --unsafe-allow-non-loopback to override"
        ) from error
    if not address.is_loopback and not unsafe_allow_non_loopback:
        raise ValueError(
            "non-loopback bind refused; pass --unsafe-allow-non-loopback to accept the risk"
        )
    return candidate


def _validate_port(port: int) -> None:
    if not 1 <= port <= 65535:
        raise ValueError("port must be between 1 and 65535")


def _normalize_values(values: Iterable[str], *, field_name: str) -> frozenset[str]:
    normalized = frozenset(value.strip().lower() for value in values if value.strip())
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty")
    return normalized


@dataclass(frozen=True, slots=True)
class LocalSecurityConfig:
    allowed_hosts: frozenset[str]
    allowed_origins: frozenset[str]
    session_token: str = field(default_factory=lambda: secrets.token_urlsafe(32), repr=False)
    cookie_name: str = SESSION_COOKIE_NAME

    def __init__(
        self,
        *,
        allowed_hosts: Iterable[str],
        allowed_origins: Iterable[str],
        session_token: str | None = None,
        cookie_name: str = SESSION_COOKIE_NAME,
    ) -> None:
        hosts = _normalize_values(allowed_hosts, field_name="allowed_hosts")
        origins = frozenset(value.strip() for value in allowed_origins if value.strip())
        if not origins:
            raise ValueError("allowed_origins cannot be empty")
        token = session_token or secrets.token_urlsafe(32)
        if not token or not cookie_name or any(character.isspace() for character in cookie_name):
            raise ValueError("session token and cookie name must be non-empty")
        object.__setattr__(self, "allowed_hosts", hosts)
        object.__setattr__(self, "allowed_origins", origins)
        object.__setattr__(self, "session_token", token)
        object.__setattr__(self, "cookie_name", cookie_name)


def bootstrap_response(config: LocalSecurityConfig) -> JSONResponse:
    response = JSONResponse({"status": "ready"})
    response.set_cookie(
        config.cookie_name,
        config.session_token,
        httponly=True,
        samesite="strict",
        path="/",
        secure=False,
    )
    return response


def _headers(scope: Mapping[str, Any], name: bytes) -> list[bytes]:
    return [value for key, value in scope.get("headers", ()) if key.lower() == name]


def _cookie_value(scope: Mapping[str, Any], name: str) -> str | None:
    values = _headers(scope, b"cookie")
    if not values:
        return None
    cookie = SimpleCookie()
    try:
        cookie.load(b"; ".join(values).decode("latin-1"))
    except Exception:
        return None
    morsel = cookie.get(name)
    return morsel.value if morsel is not None else None


def _problem(error: DomainError) -> JSONResponse:
    return JSONResponse(
        status_code=error.status,
        content=error.problem.model_dump(mode="json", exclude_none=True),
        media_type="application/problem+json",
    )


def _secure_response_start(message: dict[str, Any]) -> dict[str, Any]:
    existing = [
        (key, value)
        for key, value in message.get("headers", ())
        if key.lower() not in {name for name, _ in SECURITY_HEADERS}
    ]
    return {**message, "headers": [*existing, *SECURITY_HEADERS]}


class LocalSecurityMiddleware:
    """ASGI middleware enforcing the localhost browser trust boundary."""

    def __init__(self, app: Any, config: LocalSecurityConfig) -> None:
        self.app = app
        self.config = config

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return

        async def secure_send(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                message = _secure_response_start(message)
            await send(message)

        host_values = _headers(scope, b"host")
        host = host_values[0].decode("latin-1").strip().lower() if len(host_values) == 1 else ""
        if host not in self.config.allowed_hosts:
            await _problem(DomainError(ErrorCode.HOST_REJECTED, "request Host is not allowlisted"))(
                scope, receive, secure_send
            )
            return

        method = str(scope.get("method", "GET")).upper()
        if method in MUTATION_METHODS:
            origin_values = _headers(scope, b"origin")
            origin = origin_values[0].decode("latin-1") if len(origin_values) == 1 else ""
            if origin not in self.config.allowed_origins:
                await _problem(
                    DomainError(ErrorCode.ORIGIN_REJECTED, "mutation Origin is not allowlisted")
                )(scope, receive, secure_send)
                return

        path = str(scope.get("path", ""))
        is_api = path == "/api" or path.startswith("/api/")
        is_bootstrap = path == "/api/v1/bootstrap" and method in {"GET", "HEAD"}
        if is_api and not is_bootstrap:
            supplied = _cookie_value(scope, self.config.cookie_name)
            if supplied is None or not hmac.compare_digest(supplied, self.config.session_token):
                await _problem(
                    DomainError(
                        ErrorCode.SESSION_REQUIRED,
                        "a current localhost session is required",
                    )
                )(scope, receive, secure_send)
                return

        await self.app(scope, receive, secure_send)
