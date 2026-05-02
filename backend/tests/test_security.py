"""Security middleware: API-key auth + body-size cap behaviour."""

from __future__ import annotations

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app import config as cfg_mod
from app.security import BodySizeLimitMiddleware, require_api_key


@pytest.fixture
def app_no_auth(monkeypatch):
    """Build a fresh app with auth disabled."""
    monkeypatch.setattr(cfg_mod.settings, "ingest_api_key", "")
    app = FastAPI()

    @app.post("/protected", dependencies=[Depends(require_api_key)])
    async def protected() -> dict:
        return {"ok": True}

    return app


@pytest.fixture
def app_with_auth(monkeypatch):
    """Build a fresh app with a known API key configured."""
    monkeypatch.setattr(cfg_mod.settings, "ingest_api_key", "secret-token-123")
    app = FastAPI()

    @app.post("/protected", dependencies=[Depends(require_api_key)])
    async def protected() -> dict:
        return {"ok": True}

    return app


def test_no_key_required_when_unset(app_no_auth):
    client = TestClient(app_no_auth)
    assert client.post("/protected").status_code == 200


def test_missing_key_is_rejected_when_required(app_with_auth):
    client = TestClient(app_with_auth)
    r = client.post("/protected")
    assert r.status_code == 401
    assert r.headers.get("www-authenticate") == "ApiKey"


def test_wrong_key_is_rejected(app_with_auth):
    client = TestClient(app_with_auth)
    r = client.post("/protected", headers={"X-API-Key": "wrong"})
    assert r.status_code == 401


def test_correct_key_passes(app_with_auth):
    client = TestClient(app_with_auth)
    r = client.post("/protected", headers={"X-API-Key": "secret-token-123"})
    assert r.status_code == 200


def test_body_size_limit_enforced():
    app = FastAPI()
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=100)

    @app.post("/echo")
    async def echo() -> dict:
        return {"ok": True}

    client = TestClient(app)
    # Force a Content-Length header above the cap
    r = client.post(
        "/echo",
        headers={"Content-Length": "1000"},
        content=b"x" * 1000,
    )
    assert r.status_code == 413
