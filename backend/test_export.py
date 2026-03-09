from reporting.report_generator import ReportGenerator, FullReport, ReportExporter

profile_mock = {
    "total_rows": 5000,
    "total_columns": 21,
    "memory_size_bytes": 1563648,
    "estimated_domain": "Education / Academic",
    "duplicate_row_count": 0,
    "columns": [
        {"name": "student_id", "inferred_dtype": "int64", "distinct_count": 5000, "null_percentage": 0.0, "semantic_type": "id_key",
         "numeric": {"mean": 2500.5, "std": 1443.52, "min": 1, "max": 5000, "median": 2500.5, "percentile_25": 1250.75, "percentile_75": 3750.25, "skewness": 0.0, "kurtosis": -1.2}},
        {"name": "age", "inferred_dtype": "int64", "distinct_count": 10, "null_percentage": 0.0, "semantic_type": "numeric_discrete",
         "numeric": {"mean": 21.5, "std": 2.87, "min": 18, "max": 26, "median": 22.0, "percentile_25": 19.0, "percentile_75": 24.0, "skewness": 0.12, "kurtosis": -1.19}},
        {"name": "study_hours", "inferred_dtype": "float64", "distinct_count": 840, "null_percentage": 0.0, "semantic_type": "numeric_continuous",
         "numeric": {"mean": 5.01, "std": 2.88, "min": 0.0, "max": 10.0, "median": 5.01, "percentile_25": 2.52, "percentile_75": 7.51, "skewness": 0.001, "kurtosis": -1.2}},
        {"name": "caffeine_intake_mg", "inferred_dtype": "int64", "distinct_count": 500, "null_percentage": 0.0, "semantic_type": "numeric_discrete",
         "numeric": {"mean": 250.1, "std": 144.3, "min": 0, "max": 500, "median": 250.0, "percentile_25": 125.0, "percentile_75": 375.0, "skewness": 0.002, "kurtosis": -1.2}},
        {"name": "productivity_score", "inferred_dtype": "float64", "distinct_count": 3410, "null_percentage": 0.0, "semantic_type": "numeric_continuous",
         "numeric": {"mean": 50.2, "std": 13.5, "min": 5.1, "max": 95.8, "median": 50.1, "percentile_25": 39.8, "percentile_75": 60.5, "skewness": 0.03, "kurtosis": -0.55}},
        {"name": "burnout_level", "inferred_dtype": "float64", "distinct_count": 3256, "null_percentage": 0.0, "semantic_type": "numeric_continuous",
         "numeric": {"mean": 50.5, "std": 15.2, "min": 0.5, "max": 99.9, "median": 50.8, "percentile_25": 38.1, "percentile_75": 63.2, "skewness": -0.01, "kurtosis": -0.62}},
        {"name": "mental_health_score", "inferred_dtype": "int64", "distinct_count": 10, "null_percentage": 0.0, "semantic_type": "numeric_discrete",
         "numeric": {"mean": 5.5, "std": 2.87, "min": 1, "max": 10, "median": 5.0, "percentile_25": 3.0, "percentile_75": 8.0, "skewness": 0.0, "kurtosis": -1.2}},
        {"name": "gender", "inferred_dtype": "object", "distinct_count": 3, "null_percentage": 0.0, "semantic_type": "categorical_nominal"},
        {"name": "academic_level", "inferred_dtype": "object", "distinct_count": 3, "null_percentage": 0.0, "semantic_type": "categorical_nominal"},
    ],
    "cross_analysis": {
        "correlations": {
            "study_hours vs productivity_score": 0.82,
            "burnout_level vs mental_health_score": -0.75,
            "caffeine_intake_mg vs burnout_level": 0.45,
            "age vs study_hours": 0.12,
        }
    }
}

insights_mock = {
    "hypotheses": [
        {"observation": "Feature 'student_id' exhibits near-perfect cardinality", "evidence": "100.0% of values are strictly unique", "impact": "medium", "confidence": 1.0, "question": "Should this feature be excluded from predictive modeling?"},
    ],
    "quality_score": {
        "grade": "A",
        "overall_score": 95,
        "completeness": 100,
        "uniqueness": 90,
        "validity": 95,
        "consistency": 92,
    }
}

report = ReportGenerator.generate(profile_data=profile_mock, insights_data=insights_mock)
print(f"Sections generated: {len(report.sections)}")
for s in report.sections:
    charts_count = len(s.charts) if s.charts else 0
    tables_count = len(s.tables) if s.tables else 0
    print(f"  - {s.title}: {tables_count} tables, {charts_count} charts")

try:
    pdf_bytes = ReportExporter.to_pdf(report)
    print(f"\nPDF: {len(pdf_bytes)} bytes - SUCCESS")
except Exception as e:
    print(f"\nPDF FAIL: {e}")

try:
    html_text = ReportExporter.to_html(report)
    print(f"HTML: {len(html_text)} chars - SUCCESS")
except Exception as e:
    print(f"HTML FAIL: {e}")
