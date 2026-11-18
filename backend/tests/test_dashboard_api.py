"""
Integration tests for Flagship AI Dashboard API endpoints:
- POST /api/dashboard/interpret
- POST /api/dashboard/query
- POST /api/dashboard/auto
- POST /api/dashboard/suggest
- Dashboard CRUD (/api/dashboards)
"""

import io
import pytest
from fastapi.testclient import TestClient
import pandas as pd
from main import app
from config import AppConfig
from dashboard.chart_config_models import ChartConfig, ChartType, AggregationType, TimeGrain


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def auth_headers():
    return {"X-API-Key": AppConfig.API_KEY}


@pytest.fixture
def uploaded_file_id(client, auth_headers):
    # Upload test CSV
    csv_content = """date,category,revenue,cost
2023-01-10,Electronics,100,50
2023-01-20,Clothing,200,80
2023-02-05,Electronics,150,70
2023-02-18,Clothing,250,90
2023-03-12,Electronics,300,100
2023-03-25,Clothing,400,120
"""
    files = [("files", ("test_sales.csv", io.BytesIO(csv_content.encode("utf-8")), "text/csv"))]
    resp = client.post("/api/upload", files=files, headers=auth_headers)
    assert resp.status_code == 200, f"Upload failed: {resp.text}"
    return resp.json()["file_id"]


def test_dashboard_interpret_monthly_prompt(client, auth_headers, uploaded_file_id):
    """Interpret 'Monthly revenue by date' must set time grain to month and aggregate data."""
    payload = {
        "file_id": uploaded_file_id,
        "message": "Monthly revenue by date",
    }
    resp = client.post("/api/dashboard/interpret", json=payload, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["success"] is True
    assert "config" in data
    config = data["config"]
    assert config["x_axis_time_grain"] == "month"
    assert config["y_axis"] == "revenue"
    assert config["aggregation"] == "sum"

    # SQL should group by the month expression
    assert "sql" in data
    sql = data["sql"]
    assert "DATE_TRUNC" in sql or "strftime" in sql
    assert "GROUP BY" in sql

    # Executed data should have 3 distinct months: 2023-01, 2023-02, 2023-03
    assert "data" in data
    rows = data["data"]
    assert len(rows) == 3, f"Expected 3 monthly rows, got: {rows}"
    dates = [r["date"] for r in rows]
    assert "2023-01" in dates
    assert "2023-02" in dates
    assert "2023-03" in dates


def test_dashboard_query_quarterly_grain(client, auth_headers, uploaded_file_id):
    """Direct ChartConfig query with quarterly grain."""
    chart_config = {
        "chart_type": "bar",
        "title": "Quarterly Revenue",
        "x_axis": "date",
        "y_axis": "revenue",
        "aggregation": "sum",
        "x_axis_time_grain": "quarter",
        "filters": [],
        "sort_direction": "asc",
        "color_scheme": "default",
        "show_legend": True,
        "show_grid": True,
        "show_values": False,
        "stacked": False,
        "trend_line": False,
        "smooth": False,
    }
    payload = {
        "file_id": uploaded_file_id,
        "config": chart_config,
    }
    resp = client.post("/api/dashboard/query", json=payload, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    res_data = resp.json()
    assert res_data["row_count"] == 1  # All rows are 2023 Q1
    assert "2023-Q1" in res_data["data"][0]["date"]
    assert res_data["data"][0]["revenue"] == 1400  # 100+200+150+250+300+400


def test_dashboard_auto_generate(client, auth_headers, uploaded_file_id):
    """Auto-generate initial dashboard widgets."""
    resp = client.post("/api/dashboard/auto", json={"file_id": uploaded_file_id}, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "widgets" in data
    widgets = data["widgets"]
    assert len(widgets) >= 2
    # Verify at least one KPI and one time series / breakdown
    chart_types = [w["config"]["chart_type"] for w in widgets if "config" in w]
    assert "kpi" in chart_types


def test_dashboard_crud_lifecycle(client, auth_headers, uploaded_file_id):
    """Save, list, retrieve, and delete a dashboard."""
    dashboard_payload = {
        "title": "Executive Sales Dashboard",
        "description": "Quarterly and monthly revenue breakdown",
        "file_id": uploaded_file_id,
        "widgets": [],
        "global_filters": [],
    }
    # Create / Save
    save_resp = client.post("/api/dashboards", json=dashboard_payload, headers=auth_headers)
    assert save_resp.status_code == 200, save_resp.text
    saved = save_resp.json()
    assert saved["status"] == "saved"
    dash_id = saved["id"]

    # List
    list_resp = client.get("/api/dashboards", headers=auth_headers)
    assert list_resp.status_code == 200
    dashboards = list_resp.json()["dashboards"]
    assert any(d["id"] == dash_id for d in dashboards)

    # Get by ID
    get_resp = client.get(f"/api/dashboards/{dash_id}", headers=auth_headers)
    assert get_resp.status_code == 200
    retrieved = get_resp.json()
    assert retrieved["title"] == "Executive Sales Dashboard"

    # Delete
    del_resp = client.delete(f"/api/dashboards/{dash_id}", headers=auth_headers)
    assert del_resp.status_code == 200
    assert del_resp.json()["status"] == "deleted"

    # Verify deleted
    get_del = client.get(f"/api/dashboards/{dash_id}", headers=auth_headers)
    assert get_del.status_code == 404


def test_dashboard_multi_page_persistence_and_share(client, auth_headers, uploaded_file_id):
    """Test multi-page dashboard report persistence with bookmarks, measures, and read-only share view."""
    multi_page_payload = {
        "title": "Multi-Page Corporate BI Report",
        "description": "Cross-page executive analytics",
        "file_id": uploaded_file_id,
        "pages": [
            {
                "id": "p1",
                "title": "Executive Summary",
                "widgets": [
                    {
                        "id": "w1",
                        "config": {
                            "chart_type": "kpi",
                            "title": "Total Net Revenue",
                            "x_axis": "",
                            "y_axis": "net_revenue",
                            "color_scheme": "default",
                            "show_legend": False,
                            "show_grid": False,
                            "show_values": True,
                        },
                    }
                ],
            },
            {
                "id": "p2",
                "title": "Product Decomposition",
                "widgets": [],
            },
        ],
        "measures": [
            {
                "name": "Margin_Rate",
                "expression": "SUM(net_revenue) / SUM(gross_revenue)",
                "format": "percentage",
                "is_calculated": True,
            }
        ],
        "bookmarks": [
            {
                "id": "bm-1",
                "name": "Q1 Performance View",
                "activePageId": "p1",
                "crossFilters": {"region": "North America"},
                "visibleVisualIds": ["w1"],
                "createdAt": 1700000000,
            }
        ],
    }

    # Save multi-page report
    save_resp = client.post("/api/dashboards", json=multi_page_payload, headers=auth_headers)
    assert save_resp.status_code == 200, save_resp.text
    saved = save_resp.json()
    assert saved["status"] == "saved"
    dash_id = saved["id"]

    # Load via standard endpoint
    get_resp = client.get(f"/api/dashboards/{dash_id}", headers=auth_headers)
    assert get_resp.status_code == 200
    report = get_resp.json()
    assert report["title"] == "Multi-Page Corporate BI Report"
    assert len(report["pages"]) == 2
    assert report["pages"][0]["title"] == "Executive Summary"
    assert len(report["measures"]) == 1
    assert report["measures"][0]["name"] == "Margin_Rate"
    assert len(report["bookmarks"]) == 1
    assert report["bookmarks"][0]["name"] == "Q1 Performance View"

    # Load via read-only share view
    share_resp = client.get(f"/api/dashboards/share/{dash_id}", headers=auth_headers)
    assert share_resp.status_code == 200
    share_data = share_resp.json()
    assert share_data["readonly"] is True
    assert share_data["id"] == dash_id
    assert len(share_data["pages"]) == 2

    # Cleanup
    del_resp = client.delete(f"/api/dashboards/{dash_id}", headers=auth_headers)
    assert del_resp.status_code == 200
