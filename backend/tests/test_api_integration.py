"""
Integration tests for the Data Intelligence Platform API endpoints.
Run with: pytest tests/test_api_integration.py -v
"""

from __future__ import annotations

import io
import os

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

# Set test API key before importing app
os.environ["DATA_INTEL_API_KEY"] = "test-key-for-ci"
os.environ["DEBUG"] = "false"

from main import app

API_KEY = "test-key-for-ci"
HEADERS = {"X-API-Key": API_KEY}


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def client():
    """Async HTTP client wired to the FastAPI app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _csv_bytes(content: str = "name,age\nAlice,30\nBob,25\n") -> bytes:
    """Return a minimal CSV as bytes."""
    return content.encode("utf-8")


# ── Root & Health ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_root(client):
    resp = await client.get("/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Data Intelligence Platform"
    assert "version" in data
    assert data["status"] == "operational"


@pytest.mark.asyncio
async def test_health(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "healthy"


# ── Auth ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_upload_requires_api_key(client):
    """Endpoints should reject requests without a valid API key."""
    resp = await client.post(
        "/api/upload",
        files=[("files", ("test.csv", io.BytesIO(_csv_bytes()), "text/csv"))],
    )
    assert resp.status_code in (401, 403)


@pytest.mark.asyncio
async def test_upload_wrong_api_key(client):
    resp = await client.post(
        "/api/upload",
        files=[("files", ("test.csv", io.BytesIO(_csv_bytes()), "text/csv"))],
        headers={"X-API-Key": "wrong-key"},
    )
    assert resp.status_code in (401, 403)


# ── Upload ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_upload_single_csv(client):
    """Upload a valid CSV and get an ingestion result back."""
    resp = await client.post(
        "/api/upload",
        files=[("files", ("test.csv", io.BytesIO(_csv_bytes()), "text/csv"))],
        headers=HEADERS,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("success") is True
    assert "file_id" in data


@pytest.mark.asyncio
async def test_upload_rejected_extension(client):
    """Upload with a disallowed extension should be rejected."""
    resp = await client.post(
        "/api/upload",
        files=[("files", ("malware.exe", io.BytesIO(b"MZ..."), "application/octet-stream"))],
        headers=HEADERS,
    )
    assert resp.status_code == 400
    assert "not allowed" in resp.json()["detail"].lower()


# ── Profiling ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_profile_nonexistent_file(client):
    """Profiling a non-existent file should return 404."""
    resp = await client.get("/api/profile/nonexistent-id", headers=HEADERS)
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_profile_after_upload(client):
    """Upload then profile should succeed."""
    # Upload first
    upload = await client.post(
        "/api/upload",
        files=[("files", ("data.csv", io.BytesIO(_csv_bytes()), "text/csv"))],
        headers=HEADERS,
    )
    assert upload.status_code == 200
    file_id = upload.json()["file_id"]

    # Profile
    resp = await client.get(f"/api/profile/{file_id}", headers=HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("success") is True


# ── Cleaning ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_cleaning_analyze_nonexistent(client):
    resp = await client.get("/api/cleaning/nonexistent/analyze", headers=HEADERS)
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_cleaning_analyze_after_upload(client):
    """Upload → analyze cleaning should return a plan."""
    upload = await client.post(
        "/api/upload",
        files=[("files", ("clean_test.csv", io.BytesIO(_csv_bytes()), "text/csv"))],
        headers=HEADERS,
    )
    file_id = upload.json()["file_id"]

    resp = await client.get(f"/api/cleaning/{file_id}/analyze", headers=HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert "total_actions" in data
    assert "actions" in data


# ── SQL ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_sql_tables_list(client):
    """The /api/sql/tables endpoint should return a list."""
    resp = await client.get("/api/sql/tables", headers=HEADERS)
    assert resp.status_code == 200
    assert isinstance(resp.json(), (list, dict))


@pytest.mark.asyncio
async def test_sql_execute_no_body(client):
    """SQL execute without a body should return 422."""
    resp = await client.post("/api/sql/execute", headers=HEADERS)
    assert resp.status_code == 422


# ── Reporting ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_export_formats(client):
    resp = await client.get("/api/reporting/export-formats", headers=HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert "report_formats" in data
    assert "data_formats" in data


# ── Rate limiting ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_root_is_rate_limit_exempt(client):
    """Root endpoint should not be rate-limited."""
    for _ in range(5):
        resp = await client.get("/")
        assert resp.status_code == 200
