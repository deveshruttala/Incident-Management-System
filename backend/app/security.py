"""Cross-cutting security & observability middleware.

Three small concerns live together because they all wrap the request:

    require_api_key       — FastAPI dependency for `/ingest*` endpoints
    correlation_id_mw     — assigns/propagates X-Request-ID for tracing
    body_size_limit_mw    — defence-in-depth cap on request body size

Auth is intentionally **optional** (controlled by `INGEST_API_KEY`).
Production deployments must set the env var; local dev leaves it empty.
"""

from __future__ import annotations

import logging
import secrets
import time
import uuid

from fastapi import Header, HTTPException, Request, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from app.config import settings

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# 1. API-key authentication for the ingest endpoints                          #
# --------------------------------------------------------------------------- #
async def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """FastAPI dependency: enforce `X-API-Key` when `INGEST_API_KEY` is set.

    `secrets.compare_digest` is used to make the comparison constant-time and
    avoid leaking the key length / prefix via timing side-channels.
    """
    expected = settings.ingest_api_key
    if not expected:  # auth disabled in dev
        return
    if not x_api_key or not secrets.compare_digest(x_api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid X-API-Key",
            headers={"WWW-Authenticate": "ApiKey"},
        )


# --------------------------------------------------------------------------- #
# 2. Request correlation ID                                                   #
# --------------------------------------------------------------------------- #
class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Echo / generate `X-Request-ID` so logs can be stitched across services.

    If the caller already supplied an ID we reuse it (the value flows through
    a load balancer or other upstream). Otherwise we mint a fresh UUID.
    """

    HEADER = "x-request-id"

    async def dispatch(self, request: Request, call_next) -> Response:
        rid = request.headers.get(self.HEADER) or uuid.uuid4().hex
        request.state.request_id = rid
        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = (time.monotonic() - start) * 1000
        response.headers[self.HEADER] = rid
        # Single structured access-log line per request — easy to grep / ship.
        log.info(
            "req=%s %s %s -> %d in %.1fms",
            rid, request.method, request.url.path, response.status_code, elapsed_ms,
        )
        return response


# --------------------------------------------------------------------------- #
# 3. Request body size cap                                                    #
# --------------------------------------------------------------------------- #
class BodySizeLimitMiddleware(BaseHTTPMiddleware):
    """Reject obviously oversized bodies before we even read them.

    Relies on the `Content-Length` header so we can fail fast (HTTP 413).
    A streaming body without Content-Length is allowed through — Pydantic
    validation will still cap individual fields.
    """

    def __init__(self, app, max_bytes: int) -> None:
        super().__init__(app)
        self.max_bytes = max_bytes

    async def dispatch(self, request: Request, call_next) -> Response:
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                if int(content_length) > self.max_bytes:
                    return Response(
                        content=f"request body exceeds {self.max_bytes} bytes",
                        status_code=413,
                    )
            except ValueError:
                pass  # malformed header — let downstream handle it
        return await call_next(request)
