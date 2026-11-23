"""
Security tests for SSRF and local file access prevention in /api/monitor/schedule.
"""

import pytest
from fastapi.testclient import TestClient
from main import create_app
from config import AppConfig
import state
import pandas as pd


@pytest.fixture
def client():
    app = create_app()
    return TestClient(app)


@pytest.fixture
def test_dataset():
    file_id = "test-ssrf-dataset"
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    state.set_df(file_id, df)
    state.profile_store[file_id] = {"overall_score": 90}
    return file_id


def test_monitor_blocks_file_scheme(client, test_dataset):
    """file:// schemes must be rejected with HTTP 400 Bad Request."""
    headers = {"X-API-Key": AppConfig.API_KEY}
    resp = client.post(
        "/api/monitor/schedule",
        json={
            "file_id": test_dataset,
            "origin_url": "file:///etc/passwd",
            "cron_expr": "*/10 * * * *",
        },
        headers=headers,
    )
    assert resp.status_code == 400
    assert "origin_url" in resp.json().get("detail", "").lower() or "disallowed" in resp.json().get("detail", "").lower()


def test_monitor_blocks_loopback_and_metadata_ip(client, test_dataset):
    """Loopback addresses (127.0.0.1, localhost) and cloud metadata (169.254.169.254) must be blocked with HTTP 400."""
    headers = {"X-API-Key": AppConfig.API_KEY}
    
    # Metadata IP
    resp_meta = client.post(
        "/api/monitor/schedule",
        json={
            "file_id": test_dataset,
            "origin_url": "http://169.254.169.254/latest/meta-data/",
            "cron_expr": "*/10 * * * *",
        },
        headers=headers,
    )
    assert resp_meta.status_code == 400

    # Localhost
    resp_local = client.post(
        "/api/monitor/schedule",
        json={
            "file_id": test_dataset,
            "origin_url": "http://127.0.0.1:8000/internal",
            "cron_expr": "*/10 * * * *",
        },
        headers=headers,
    )
    assert resp_local.status_code == 400
