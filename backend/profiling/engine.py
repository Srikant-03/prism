"""
Main profiling orchestrator.
Coordinates type detection, per-column profiling, and dataset-level analysis.
Auto-triggers after ingestion completes.
"""

from __future__ import annotations

import time
import sys
from typing import Optional

import pandas as pd
import numpy as np

from profiling.profiling_models import (
    ColumnProfile,
    DatasetProfile,
    ProfilingResult,
    SemanticType,
    ValueFrequency,
)
from profiling.type_detector import SemanticTypeDetector
from profiling.numeric_profiler import NumericProfiler
from profiling.categorical_profiler import CategoricalProfiler
from profiling.datetime_profiler import DatetimeProfiler
from profiling.boolean_profiler import BooleanProfiler
from profiling.text_profiler import TextProfiler
from profiling.domain_detector import DomainDetector
from profiling.key_detector import KeyDetector
from profiling.correlation_analyzer import CorrelationAnalyzer
from profiling.target_detector import TargetDetector
from profiling.temporal_analyzer import TemporalAnalyzer
from profiling.geo_analyzer import GeoAnalyzer
from profiling.cross_column_models import CrossColumnProfile
from insights.quality_scorer import QualityScorer
from insights.anomaly_detector import AnomalyDetector
from insights.feature_ranker import FeatureRanker
from insights.briefing_generator import BriefingGenerator
from insights.insight_models import DatasetInsights


class DataProfiler:
    """
    Orchestrates the complete profiling pipeline.
    For every column: universal stats + type-specific deep profiling.
    For the dataset: metadata, domain inference, key detection, completeness scoring.
    """

    @staticmethod
    def profile(
        df: pd.DataFrame,
        file_id: str,
        disk_size_bytes: int = 0,
    ) -> ProfilingResult:
        """
        Run the full profiling pipeline on a DataFrame.

        Args:
            df: The ingested DataFrame to profile.
            file_id: Unique file identifier.
            disk_size_bytes: Original file size on disk.

        Returns:
            ProfilingResult with complete dataset and column profiles.
        """
        start_time = time.time()
        warnings: list[str] = []

        if df.empty:
            return ProfilingResult(
                success=True,
                file_id=file_id,
                profile=DatasetProfile(),
                warnings=["Dataset is empty — profiling skipped."],
            )

        # ── Dataset-Level Metadata ──
        dataset = DatasetProfile()
        dataset.total_rows = len(df)
        dataset.total_columns = len(df.columns)

        # Memory size
        mem_bytes = int(df.memory_usage(deep=True).sum())
        dataset.memory_size_bytes = mem_bytes
        dataset.memory_size_readable = _format_bytes(mem_bytes)
        dataset.disk_size_bytes = disk_size_bytes
        dataset.disk_size_readable = _format_bytes(disk_size_bytes)

        # ── Inferred Schema ──
        for col in df.columns:
            sem_type, conf = SemanticTypeDetector.detect(df[col], col)
            dataset.inferred_schema[col] = sem_type.value
            dataset.schema_confidence[col] = round(conf, 3)

        # ── Domain Detection ──
        dataset.estimated_domain, dataset.domain_confidence, dataset.domain_justification = (
            DomainDetector.detect(df)
        )

        # ── Structural Completeness ──
        total_cells = dataset.total_rows * dataset.total_columns
        non_null_cells = int(df.notna().sum().sum())
        dataset.structural_completeness = round(
            non_null_cells / total_cells * 100, 2
        ) if total_cells > 0 else 0.0

        # ── Schema Consistency ──
        consistency_scores = {}
        for col in df.columns:
            consistency_scores[col] = _check_type_consistency(df[col])
        avg_consistency = (
            sum(float(v.split("%")[0]) for v in consistency_scores.values())
            / len(consistency_scores)
            if consistency_scores
            else 100.0
        )
        dataset.schema_consistency = round(avg_consistency, 2)
        dataset.schema_consistency_details = consistency_scores

        # ── Temporal Coverage ──
        temporal_cols = [
            col for col, st in dataset.inferred_schema.items()
            if st in ("datetime", "duration")
        ]
        dataset.temporal_columns = temporal_cols
        if temporal_cols:
            first_dt = temporal_cols[0]
            try:
                dt_series = pd.to_datetime(df[first_dt], errors="coerce")
                earliest = dt_series.min()
                latest = dt_series.max()
                if pd.notna(earliest) and pd.notna(latest):
                    dataset.temporal_coverage = {
                        "column": first_dt,
                        "earliest": str(earliest),
                        "latest": str(latest),
                        "span_days": (latest - earliest).days,
                    }
            except Exception:
                pass

        # ── Key Detection ──
        try:
            dataset.primary_key_candidates = KeyDetector.detect_primary_keys(df)
        except Exception as e:
            warnings.append(f"Primary key detection failed: {str(e)}")

        try:
            dataset.foreign_key_candidates = KeyDetector.detect_foreign_keys(df)
        except Exception as e:
            warnings.append(f"Foreign key detection failed: {str(e)}")

        try:
            dataset.id_columns = KeyDetector.detect_id_columns(df)
        except Exception as e:
            warnings.append(f"ID column detection failed: {str(e)}")

        # ── Per-Column Profiling ──
        column_profiles: list[ColumnProfile] = []
        for idx, col in enumerate(df.columns):
            try:
                col_profile = DataProfiler._profile_column(
                    df[col], col, idx, dataset.inferred_schema.get(col, "unknown"),
                    dataset.schema_confidence.get(col, 0.0),
                )
                column_profiles.append(col_profile)
            except Exception as e:
                warnings.append(f"Profiling failed for column '{col}': {str(e)}")
                # Add minimal profile
                column_profiles.append(ColumnProfile(
                    name=col,
                    position=idx,
                    inferred_dtype=str(df[col].dtype),
                    semantic_type=SemanticType.UNKNOWN,
                    quality_score=0,
                    quality_justification=f"Profiling error: {str(e)}",
                ))

        dataset.columns = column_profiles
        
        # ── Cross-Column Analysis ──
        try:
            # 1. Correlations
            corr_analyzer = CorrelationAnalyzer()
            correlations = corr_analyzer.analyze(df, dataset)
            
            # 2. Target Detection
            target_detector = TargetDetector()
            target_analysis = target_detector.analyze(df, dataset, correlations)
            
            # 3. Temporal Patterns
            temporal_analyzer = TemporalAnalyzer()
            temporal_analysis = temporal_analyzer.analyze(df, dataset)
            
            # 4. Geo Patterns
            geo_analyzer = GeoAnalyzer()
            geo_analysis = geo_analyzer.analyze(df, dataset)
            
            cross_profile = CrossColumnProfile(
                correlations=correlations,
                target=target_analysis,
                temporal=temporal_analysis,
                geo=geo_analysis
            )
            dataset.cross_analysis = cross_profile.model_dump()
        except Exception as e:
            warnings.append(f"Cross-column analysis failed: {str(e)}")

        # ── Insights Generation ──
        try:
            quality = QualityScorer.calculate_scores(dataset)
            anomalies = AnomalyDetector.detect(dataset)
            rankings = FeatureRanker.rank_features(dataset)
            briefing = BriefingGenerator.generate(dataset, quality, anomalies, rankings)

            dataset.insights = DatasetInsights(
                quality_score=quality,
                anomalies=anomalies,
                feature_ranking=rankings,
                analyst_briefing=briefing
            )
        except Exception as e:
            warnings.append(f"Insight Generation failed: {str(e)}")

        dataset.profiling_time_seconds = round(time.time() - start_time, 3)

        return ProfilingResult(
            success=True,
            file_id=file_id,
            profile=dataset,
            warnings=warnings,
        )

    @staticmethod
    def _profile_column(
        series: pd.Series,
        col_name: str,
        position: int,
        semantic_type_str: str,
        type_confidence: float,
    ) -> ColumnProfile:
        """Profile a single column with universal + type-specific stats."""
        sem_type = SemanticType(semantic_type_str)
        non_null = series.dropna()
        n = len(series)
        n_non_null = len(non_null)

        profile = ColumnProfile(
            name=col_name,
            position=position,
            inferred_dtype=str(series.dtype),
            semantic_type=sem_type,
            semantic_type_confidence=type_confidence,
        )

        # ── Universal Stats ──
        profile.null_count = n - n_non_null
        profile.null_percentage = round((n - n_non_null) / n * 100, 2) if n > 0 else 0.0
        profile.non_null_count = n_non_null
        profile.distinct_count = int(non_null.nunique())
        profile.distinct_percentage = (
            round(profile.distinct_count / n_non_null * 100, 2) if n_non_null > 0 else 0.0
        )

        # Most/least frequent
        if n_non_null > 0:
            value_counts = non_null.value_counts()
            most_freq_val = value_counts.index[0]
            most_freq_count = int(value_counts.iloc[0])
            profile.most_frequent = ValueFrequency(
                value=str(most_freq_val)[:200],
                count=most_freq_count,
                percentage=round(most_freq_count / n_non_null * 100, 2),
            )

            least_freq_val = value_counts.index[-1]
            least_freq_count = int(value_counts.iloc[-1])
            profile.least_frequent = ValueFrequency(
                value=str(least_freq_val)[:200],
                count=least_freq_count,
                percentage=round(least_freq_count / n_non_null * 100, 2),
            )

        # Sample values
        sample_n = min(10, n_non_null)
        if sample_n > 0:
            sampled = non_null.sample(n=sample_n, random_state=42)
            profile.sample_values = [str(v)[:200] for v in sampled.tolist()]

        # ── Type-Specific Profiling ──
        if SemanticTypeDetector.is_numeric_type(sem_type):
            try:
                numeric_series = pd.to_numeric(non_null, errors="coerce").dropna()
                profile.numeric = NumericProfiler.profile(
                    numeric_series,
                    original_series=series if pd.api.types.is_object_dtype(series) else None,
                )
            except Exception:
                pass

        elif SemanticTypeDetector.is_categorical_type(sem_type):
            profile.categorical = CategoricalProfiler.profile(non_null)

        elif SemanticTypeDetector.is_datetime_type(sem_type):
            profile.datetime = DatetimeProfiler.profile(series)

        elif SemanticTypeDetector.is_boolean_type(sem_type):
            profile.boolean = BooleanProfiler.profile(series)

        elif SemanticTypeDetector.is_text_type(sem_type):
            profile.text = TextProfiler.profile(series, col_name)

        elif SemanticTypeDetector.is_id_type(sem_type):
            # ID columns get categorical profiling for cardinality but no deep analysis
            profile.categorical = CategoricalProfiler.profile(non_null)

        else:
            # Unknown — try numeric first, then categorical
            if pd.api.types.is_numeric_dtype(series):
                try:
                    profile.numeric = NumericProfiler.profile(non_null)
                except Exception:
                    pass
            else:
                profile.categorical = CategoricalProfiler.profile(non_null)

        # ── Quality Score ──
        profile.quality_score, profile.quality_justification = (
            _compute_quality_score(profile, n)
        )

        return profile


def _compute_quality_score(profile: ColumnProfile, total_rows: int) -> tuple[float, str]:
    """
    Compute a data quality score (0-100) for a column.
    Factors: completeness, consistency, uniqueness appropriateness.
    """
    issues: list[str] = []
    score = 100.0

    # Completeness penalty (null ratio)
    null_pct = profile.null_percentage
    if null_pct > 50:
        score -= 30
        issues.append(f"Very high null rate ({null_pct:.1f}%)")
    elif null_pct > 20:
        score -= 15
        issues.append(f"High null rate ({null_pct:.1f}%)")
    elif null_pct > 5:
        score -= 5
        issues.append(f"Moderate null rate ({null_pct:.1f}%)")

    # Categorical quality checks
    if profile.categorical:
        cat = profile.categorical
        if cat.case_inconsistencies:
            penalty = min(len(cat.case_inconsistencies) * 3, 15)
            score -= penalty
            issues.append(f"{len(cat.case_inconsistencies)} case inconsistencies")

        if cat.whitespace_issues:
            score -= 5
            issues.append("Whitespace padding detected")

        if cat.special_char_contamination:
            score -= 10
            issues.append("Special character contamination")

    # Numeric quality checks
    if profile.numeric:
        num = profile.numeric
        if num.formatting_issues:
            score -= len(num.formatting_issues) * 5
            issues.append(f"{len(num.formatting_issues)} formatting issues")

    # Datetime quality checks
    if profile.datetime:
        dt = profile.datetime
        if dt.future_dates_count > 0:
            score -= 5
            issues.append(f"{dt.future_dates_count} future dates")
        if dt.implausible_dates_count > 0:
            score -= 10
            issues.append(f"{dt.implausible_dates_count} implausible dates")
        if dt.mixed_formats:
            score -= 5
            issues.append("Mixed date formats")

    # Text quality checks
    if profile.text:
        if profile.text.has_pii_risk:
            score -= 10
            issues.append("PII risk detected")
        if profile.text.html_contamination:
            score -= 5
            issues.append("HTML contamination")

    score = max(0.0, min(100.0, score))
    justification = (
        f"Quality score: {score:.0f}/100. "
        + (("Issues: " + "; ".join(issues) + ".") if issues else "No quality issues detected.")
    )

    return round(score, 1), justification


def _check_type_consistency(series: pd.Series) -> str:
    """Check if a column has mixed types."""
    if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_bool_dtype(series):
        return "100% consistent"

    non_null = series.dropna()
    if len(non_null) == 0:
        return "100% consistent (empty)"

    if pd.api.types.is_object_dtype(series):
        # Try to detect mixed types in object columns
        sample = non_null.head(1000)
        type_counts: dict[str, int] = {}
        for val in sample:
            t = type(val).__name__
            type_counts[t] = type_counts.get(t, 0) + 1

        if len(type_counts) == 1:
            return "100% consistent"

        total = sum(type_counts.values())
        dominant = max(type_counts.values())
        pct = round(dominant / total * 100, 1)
        return f"{pct}% dominant type ({max(type_counts, key=type_counts.get)})"

    return "100% consistent"


def _format_bytes(size_bytes: int) -> str:
    """Format bytes to human-readable string."""
    if size_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    size = float(size_bytes)
    while size >= 1024 and i < len(units) - 1:
        size /= 1024
        i += 1
    return f"{size:.1f} {units[i]}"
