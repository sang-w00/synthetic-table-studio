from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from sts.domain import DomainError, ErrorCode, ProblemDetails

PROBLEM_CONTENT_TYPE = "application/problem+json"


def problem_response(problem: ProblemDetails) -> JSONResponse:
    return JSONResponse(
        status_code=problem.status,
        content=problem.model_dump(mode="json", exclude_none=True),
        media_type=PROBLEM_CONTENT_TYPE,
    )


async def domain_error_handler(request: Request, exc: DomainError) -> JSONResponse:
    problem = exc.problem.model_copy(
        update={"instance": exc.problem.instance or str(request.url.path)}
    )
    return problem_response(problem)


async def validation_error_handler(
    request: Request, exc: RequestValidationError | ValidationError
) -> JSONResponse:
    context: dict[str, Any] = {
        "errors": [
            {
                "location": [str(part) for part in error.get("loc", ())],
                "message": error.get("msg", "invalid value"),
                "type": error.get("type", "validation_error"),
            }
            for error in exc.errors()
        ]
    }
    problem = DomainError(
        ErrorCode.SCHEMA_INVALID,
        "request body failed validation",
        instance=str(request.url.path),
        context=context,
    ).problem
    return problem_response(problem)


def install_problem_handlers(app: FastAPI) -> None:
    app.add_exception_handler(DomainError, domain_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(RequestValidationError, validation_error_handler)  # type: ignore[arg-type]
