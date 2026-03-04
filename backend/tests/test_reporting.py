"""Test Pillar 4 — Report generator, code exporter, data exporter."""
import pandas as pd
import json

from reporting.report_generator import ReportGenerator, ReportExporter
from reporting.code_exporter import CodeExporter
from reporting.data_exporter import DataExporter


def test_report_generator():
    profile = {
        "total_rows": 1000,
        "total_columns": 10,
        "estimated_domain": "E-Commerce",
        "memory_size_bytes": 2_000_000,
        "duplicate_row_count": 5,
        "columns": [
            {"name": "id", "dtype": "int64", "null_percentage": 0, "unique_count": 1000, "semantic_type": "numeric"},
            {"name": "price", "dtype": "float64", "null_percentage": 2.5, "unique_count": 200, "semantic_type": "numeric"},
            {"name": "category", "dtype": "object", "null_percentage": 0, "unique_count": 8, "semantic_type": "categorical"},
        ],
    }

    insights = {
        "quality_score": {
            "overall_score": 85,
            "grade": "B+",
            "completeness": 95,
            "uniqueness": 88,
            "validity": 82,
            "consistency": 80,
            "timeliness": None,
        },
        "feature_rankings": [
            {"feature": "price", "importance_score": 92, "method": "mutual_info", "rationale": "Strong predictor"},
        ],
        "anomalies": [
            {"feature": "price", "severity": "high", "anomaly_type": "outlier", "description": "3 extreme outliers"},
        ],
    }

    audit_log = [
        {"step_name": "Drop Duplicates", "action_type": "drop_duplicates", "status": "applied",
         "trigger_reason": "5 duplicate rows found", "columns_affected": [], "rows_before": 1000, "rows_after": 995},
        {"step_name": "Fill Missing Prices", "action_type": "fill_nulls_median", "status": "applied",
         "trigger_reason": "2.5% null values", "columns_affected": ["price"], "rows_before": 995, "rows_after": 995},
        {"step_name": "Text Cleaning", "action_type": "text_clean", "status": "skipped",
         "trigger_reason": "No text columns", "columns_affected": [], "rows_before": 995, "rows_after": 995},
    ]

    report = ReportGenerator.generate(
        profile_data=profile,
        insights_data=insights,
        audit_log=audit_log,
    )

    assert len(report.sections) >= 6, f"Expected >=6 sections, got {len(report.sections)}"
    d = report.to_dict()
    assert "title" in d
    assert "sections" in d
    print(f"  report_generator: {len(report.sections)} sections")

    # HTML
    html = ReportExporter.to_html(report)
    assert "<html" in html
    assert "Executive Summary" in html
    print(f"  html_export: {len(html)} chars")

    # Notebook
    nb_str = ReportExporter.to_notebook(report)
    nb = json.loads(nb_str)
    assert nb["nbformat"] == 4
    print(f"  notebook_export: {len(nb['cells'])} cells")


def test_code_exporter():
    audit_log = [
        {"step_name": "Drop Duplicates", "action_type": "drop_duplicates", "status": "applied",
         "trigger_reason": "5 duplicate rows", "columns_affected": []},
        {"step_name": "Fill Nulls", "action_type": "fill_nulls_median", "status": "applied",
         "trigger_reason": "Missing values", "columns_affected": ["price", "quantity"]},
    ]

    py = CodeExporter.to_python_script(audit_log, "sales.csv")
    assert "import pandas" in py
    assert "drop_duplicates" in py
    print(f"  python_script: {len(py)} chars")

    nb = CodeExporter.to_notebook(audit_log, "sales.csv")
    nb_parsed = json.loads(nb)
    assert nb_parsed["nbformat"] == 4
    print(f"  notebook: {len(nb_parsed['cells'])} cells")

    spec = CodeExporter.to_json_pipeline(audit_log)
    pipeline = json.loads(spec)
    assert "steps" in pipeline
    assert len(pipeline["steps"]) == 2
    print(f"  json_pipeline: {len(pipeline['steps'])} steps")

    sql = CodeExporter.to_sql_file([
        {"name": "Top Products", "description": "By revenue", "sql": "SELECT * FROM products ORDER BY revenue DESC LIMIT 10"},
    ])
    assert "Top Products" in sql
    print(f"  sql_file: {len(sql)} chars")


def test_data_exporter():
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [95.5, 87.3, 91.0],
    })

    csv = DataExporter.to_csv(df)
    assert b"Alice" in csv
    print(f"  csv: {len(csv)} bytes")

    js = DataExporter.to_json(df)
    parsed = json.loads(js)
    assert len(parsed) == 3
    print(f"  json: {len(parsed)} records")

    sql = DataExporter.to_sql_inserts(df, "students")
    assert "CREATE TABLE" in sql
    assert "INSERT INTO" in sql
    print(f"  sql_insert: {len(sql)} chars")

    formats = DataExporter.get_supported_formats()
    assert len(formats) == 6
    print(f"  formats: {len(formats)} supported")


if __name__ == "__main__":
    tests = [
        ("report_generator", test_report_generator),
        ("code_exporter", test_code_exporter),
        ("data_exporter", test_data_exporter),
    ]

    passed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  PASS: {name}")
            passed += 1
        except Exception as e:
            print(f"  FAIL: {name} — {e}")

    print(f"\n{'='*40}")
    print(f"  {passed}/{len(tests)} passed")
    if passed == len(tests):
        print("  ALL PILLAR 4 TESTS PASSED")
    print(f"{'='*40}")
