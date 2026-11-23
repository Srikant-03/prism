"""
tests/test_categorical_association.py
======================================
Tests for the categorical association engine and datetime handling:
1. Datetime with many unique values (excluded from raw categorical Cramér's V)
2. Datetime-derived month (Deterministic Derivation)
3. Datetime-derived quarter (Deterministic Derivation)
4. Datetime-derived year (Deterministic Derivation)
5. Datetime vs nominal category (Temporal Association Grouped, score < 0.50)
6. High-cardinality nominals (Bergsma bias correction prevents spurious ~1.0)
7. End-to-end: zero false Data Leakage warnings on temporal features
"""

import numpy as np
import pandas as pd
import pytest

from profiling.correlation_analyzer import CorrelationAnalyzer
from profiling.profiling_models import DatasetProfile, ColumnProfile, SemanticType
from profiling.type_detector import SemanticTypeDetector
from insights.anomaly_detector import AnomalyDetector
from insights.insight_models import AnomalySeverity
from insights.relationship_classifier import RelationshipClassifier, RelationshipType


@pytest.fixture
def sample_temporal_df():
    """Create a DataFrame with 143 rows, 111 unique dates, and derived + nominal columns."""
    np.random.seed(42)
    # Generate 111 unique dates spanning ~18 months
    base_dates = pd.date_range("2023-01-01", "2024-06-30", periods=111)
    # Replicate 32 dates to get 143 rows
    extra_dates = np.random.choice(base_dates, size=32, replace=True)
    all_dates = np.concatenate([base_dates.values, extra_dates])
    np.random.shuffle(all_dates)

    date_series = pd.to_datetime(all_dates)
    df = pd.DataFrame({
        "date": date_series.strftime("%Y-%m-%d"),
        "month_name": date_series.strftime("%B"),
        "quarter": "Q" + date_series.quarter.astype(str),
        "year": date_series.year,
        "channel": np.random.choice(["Online", "Retail", "Wholesale"], size=143),
        "category": np.random.choice(["Electronics", "Clothing", "Home", "Garden", "Food"], size=143),
        "revenue": np.random.uniform(100, 10000, size=143),
    })
    return df


def test_1_datetime_with_many_unique_values(sample_temporal_df):
    """1. Raw datetime with 111 unique dates is detected and NOT fed into ordinary categorical Cramér's V."""
    df = sample_temporal_df
    analyzer = CorrelationAnalyzer()

    # Detect datetime columns
    dt_cols = analyzer._detect_datetime_columns(df)
    assert "date" in dt_cols

    # Run analysis
    analysis = analyzer.analyze(df)
    pairs = analysis.strongest_pairs

    # Verify that date is NOT paired with channel or category via raw Cramér's V with score ~1.0
    for p in pairs:
        if (p.col1 == "date" and p.col2 in ("channel", "category")) or (p.col2 == "date" and p.col1 in ("channel", "category")):
            assert p.metric != "Cramér's V", "Raw Cramér's V must NOT be computed for raw datetime column"
            assert p.score < 0.60, f"Spurious high association detected: {p.col1} ↔ {p.col2} = {p.score}"


def test_2_datetime_derived_month(sample_temporal_df):
    """2. Datetime-derived month_name is detected as Deterministic Derivation."""
    df = sample_temporal_df
    analyzer = CorrelationAnalyzer()

    analysis = analyzer.analyze(df)
    pairs = analysis.strongest_pairs

    month_pair = next(
        (p for p in pairs if (p.col1 == "date" and p.col2 == "month_name") or (p.col2 == "date" and p.col1 == "month_name")),
        None
    )
    assert month_pair is not None
    assert month_pair.metric == "Deterministic Derivation"
    assert month_pair.score == 1.0


def test_3_datetime_derived_quarter(sample_temporal_df):
    """3. Datetime-derived quarter is detected as Deterministic Derivation."""
    df = sample_temporal_df
    analyzer = CorrelationAnalyzer()

    analysis = analyzer.analyze(df)
    pairs = analysis.strongest_pairs

    quarter_pair = next(
        (p for p in pairs if (p.col1 == "date" and p.col2 == "quarter") or (p.col2 == "date" and p.col1 == "quarter")),
        None
    )
    assert quarter_pair is not None
    assert quarter_pair.metric == "Deterministic Derivation"
    assert quarter_pair.score == 1.0


def test_4_datetime_derived_year(sample_temporal_df):
    """4. Datetime-derived year is detected as Deterministic Derivation."""
    df = sample_temporal_df
    analyzer = CorrelationAnalyzer()

    analysis = analyzer.analyze(df)
    pairs = analysis.strongest_pairs

    year_pair = next(
        (p for p in pairs if (p.col1 == "date" and p.col2 == "year") or (p.col2 == "date" and p.col1 == "year")),
        None
    )
    assert year_pair is not None
    assert year_pair.metric == "Deterministic Derivation"
    assert year_pair.score == 1.0


def test_5_datetime_vs_nominal_category_grouped_analysis(sample_temporal_df):
    """5. Datetime vs nominal category uses grouped temporal analysis with realistic score."""
    df = sample_temporal_df
    analyzer = CorrelationAnalyzer()

    analysis = analyzer.analyze(df)
    pairs = analysis.strongest_pairs

    channel_pair = next(
        (p for p in pairs if (p.col1 == "date" and p.col2 == "channel") or (p.col2 == "date" and p.col1 == "channel")),
        None
    )
    # Channel was randomly assigned, so grouped association should be weak/moderate (< 0.50)
    if channel_pair is not None:
        assert channel_pair.metric == "Temporal Association (Grouped)"
        assert channel_pair.score < 0.50


def test_6_high_cardinality_bias_corrected_cramers_v():
    """6. Bergsma bias-corrected Cramér's V suppresses finite-sample degrees-of-freedom inflation."""
    np.random.seed(42)
    n = 100
    # A high-cardinality variable with 70 levels in 100 rows
    high_card = pd.Series([f"cat_{i % 70}" for i in range(n)])
    # Unrelated binary variable
    binary = pd.Series(np.random.choice(["Yes", "No"], size=n))

    analyzer = CorrelationAnalyzer()
    score = analyzer._cramers_v(high_card, binary)

    # With uncorrected Cramér's V, this would be ~0.85+. With bias correction, it drops near 0
    assert score is not None
    assert score < 0.35, f"Expected bias-corrected score < 0.35, got {score}"


def test_7_no_false_data_leakage_on_temporal_features(sample_temporal_df):
    """7. None of the datetime or calendar-derived pairs are classified as Data Leakage."""
    df = sample_temporal_df
    analyzer = CorrelationAnalyzer()
    analysis = analyzer.analyze(df)

    col_date = ColumnProfile(
        name="date", position=0, original_dtype="object", inferred_dtype="datetime",
        semantic_type=SemanticType.DATETIME, total_count=len(df), null_count=0,
        null_percentage=0.0, distinct_count=df["date"].nunique(), distinct_percentage=df["date"].nunique() / len(df) * 100,
    )
    col_month = ColumnProfile(
        name="month_name", position=1, original_dtype="object", inferred_dtype="string",
        semantic_type=SemanticType.CATEGORICAL_NOMINAL, total_count=len(df), null_count=0,
        null_percentage=0.0, distinct_count=12, distinct_percentage=12 / len(df) * 100,
    )
    col_quarter = ColumnProfile(
        name="quarter", position=2, original_dtype="object", inferred_dtype="string",
        semantic_type=SemanticType.CATEGORICAL_NOMINAL, total_count=len(df), null_count=0,
        null_percentage=0.0, distinct_count=4, distinct_percentage=4 / len(df) * 100,
    )
    col_year = ColumnProfile(
        name="year", position=3, original_dtype="int64", inferred_dtype="integer",
        semantic_type=SemanticType.NUMERIC_DISCRETE, total_count=len(df), null_count=0,
        null_percentage=0.0, distinct_count=2, distinct_percentage=2 / len(df) * 100,
    )

    profile = DatasetProfile(
        dataset_id="test_ds", filename="test.csv", total_rows=len(df), total_columns=len(df.columns),
        columns=[col_date, col_month, col_quarter, col_year], memory_usage_bytes=1024, memory_size_bytes=1024,
        duplicate_rows_count=0, duplicate_rows_percentage=0.0,
        cross_analysis={
            "correlations": {
                "strongest_pairs": [p.model_dump() for p in analysis.strongest_pairs]
            },
            "target": {"target_column": None},
        },
    )

    warnings = AnomalyDetector.detect(profile)

    # Check that NO warning involving date or derived features has category "Data Leakage" or severity "Critical"
    for w in warnings:
        if w.feature in ("date", "month_name", "quarter", "year"):
            assert w.category != "Data Leakage", f"Unexpected Data Leakage category on {w.feature}: {w.description}"
            assert w.severity != AnomalySeverity.CRITICAL, f"Unexpected Critical severity on {w.feature}: {w.description}"
