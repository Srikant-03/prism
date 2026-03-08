"""
Main profiling orchestrator.
Coordinates type detection, per-column profiling, and dataset-level analysis.
Auto-triggers after ingestion completes.

Architecture: Uses a ProfilingPipeline with discrete PipelineStage objects
for explicit, testable ordering. Each stage is isolated and failure in one
stage does not kill the rest of the pipeline.
"""

from __future__ import annotations

import time
import sys
import logging
from typing import Optional, Callable, Any
from dataclasses import dataclass, field

# File-based logger to avoid uvicorn swallowing output
_log = logging.getLogger("profiling.engine")
if not _log.handlers:
    _fh = logging.FileHandler("profiling_debug.log", mode="a")
    _fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _log.addHandler(_fh)
    _sh = logging.StreamHandler(sys.stderr)
    _sh.setFormatter(logging.Formatter("%(message)s"))
    _log.addHandler(_sh)
    _log.setLevel(logging.DEBUG)

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


# ──────────────────────────────────────────
# Pipeline Infrastructure
# ──────────────────────────────────────────

@dataclass
class PipelineStage:
    """A single stage in the profiling pipeline."""
    name: str
    run: Callable[["PipelineContext"], None]
    required: bool = False  # If True, failure aborts the pipeline


@dataclass
class PipelineContext:
    """Shared mutable context passed through all pipeline stages."""
    df: pd.DataFrame
    file_id: str
    disk_size_bytes: int
    dataset: DatasetProfile = field(default_factory=DatasetProfile)
    warnings: list[str] = field(default_factory=list)
    column_profiles: list[ColumnProfile] = field(default_factory=list)
    # Cross-column results (populated by later stages)
    correlations: Any = None
    target_analysis: Any = None
    temporal_analysis: Any = None
    geo_analysis: Any = None


# ──────────────────────────────────────────
# Pipeline Stage Implementations
# ──────────────────────────────────────────

def _stage_metadata(ctx: PipelineContext) -> None:
    """Compute dataset-level metadata: shape, memory, schema, domain."""
    ds = ctx.dataset
    ds.total_rows = len(ctx.df)
    ds.total_columns = len(ctx.df.columns)

    mem_bytes = int(ctx.df.memory_usage(deep=True).sum())
    ds.memory_size_bytes = mem_bytes
    ds.memory_size_readable = _format_bytes(mem_bytes)
    ds.disk_size_bytes = ctx.disk_size_bytes
    ds.disk_size_readable = _format_bytes(ctx.disk_size_bytes)

    # Inferred schema
    for col in ctx.df.columns:
        sem_type, conf = SemanticTypeDetector.detect(ctx.df[col], col)
        ds.inferred_schema[col] = sem_type.value
        ds.schema_confidence[col] = round(conf, 3)

    # Domain detection
    ds.estimated_domain, ds.domain_confidence, ds.domain_justification = (
        DomainDetector.detect(ctx.df)
    )

    # Structural completeness
    total_cells = ds.total_rows * ds.total_columns
    non_null_cells = int(ctx.df.notna().sum().sum())
    ds.structural_completeness = round(
        non_null_cells / total_cells * 100, 2
    ) if total_cells > 0 else 0.0

    # Schema consistency
    consistency_scores = {}
    for col in ctx.df.columns:
        consistency_scores[col] = _check_type_consistency(ctx.df[col])
    avg_consistency = (
        sum(float(v.split("%")[0]) for v in consistency_scores.values())
        / len(consistency_scores)
        if consistency_scores
        else 100.0
    )
    ds.schema_consistency = round(avg_consistency, 2)
    ds.schema_consistency_details = consistency_scores

    # Temporal coverage
    temporal_cols = [
        col for col, st in ds.inferred_schema.items()
        if st in ("datetime", "duration")
    ]
    ds.temporal_columns = temporal_cols
    if temporal_cols:
        first_dt = temporal_cols[0]
        try:
            dt_series = pd.to_datetime(ctx.df[first_dt], errors="coerce")
            earliest = dt_series.min()
            latest = dt_series.max()
            if pd.notna(earliest) and pd.notna(latest):
                ds.temporal_coverage = {
                    "column": first_dt,
                    "earliest": str(earliest),
                    "latest": str(latest),
                    "span_days": (latest - earliest).days,
                }
        except Exception:
            pass


def _stage_key_detection(ctx: PipelineContext) -> None:
    """Detect primary keys, foreign keys, and ID columns."""
    ds = ctx.dataset
    try:
        ds.primary_key_candidates = KeyDetector.detect_primary_keys(ctx.df)
    except Exception as e:
        ctx.warnings.append(f"Primary key detection failed: {str(e)}")
    try:
        ds.foreign_key_candidates = KeyDetector.detect_foreign_keys(ctx.df)
    except Exception as e:
        ctx.warnings.append(f"Foreign key detection failed: {str(e)}")
    try:
        ds.id_columns = KeyDetector.detect_id_columns(ctx.df)
    except Exception as e:
        ctx.warnings.append(f"ID column detection failed: {str(e)}")


def _stage_column_profiling(ctx: PipelineContext) -> None:
    """Profile each column with universal + type-specific stats."""
    for idx, col in enumerate(ctx.df.columns):
        try:
            col_profile = _profile_column(
                ctx.df[col], col, idx,
                ctx.dataset.inferred_schema.get(col, "unknown"),
                ctx.dataset.schema_confidence.get(col, 0.0),
            )
            ctx.column_profiles.append(col_profile)
        except Exception as e:
            ctx.warnings.append(f"Profiling failed for column '{col}': {str(e)}")
            ctx.column_profiles.append(ColumnProfile(
                name=col,
                position=idx,
                inferred_dtype=str(ctx.df[col].dtype),
                semantic_type=SemanticType.UNKNOWN,
                quality_score=0,
                quality_justification=f"Profiling error: {str(e)}",
            ))
    ctx.dataset.columns = ctx.column_profiles


def _stage_correlations(ctx: PipelineContext) -> None:
    """Compute inter-column correlations."""
    corr_analyzer = CorrelationAnalyzer()
    ctx.correlations = corr_analyzer.analyze(ctx.df, ctx.dataset)


def _stage_target_detection(ctx: PipelineContext) -> None:
    """Detect potential target/label columns."""
    if ctx.correlations:
        target_detector = TargetDetector()
        ctx.target_analysis = target_detector.analyze(ctx.df, ctx.dataset, ctx.correlations)


def _stage_temporal_analysis(ctx: PipelineContext) -> None:
    """Detect temporal patterns in the dataset."""
    temporal_analyzer = TemporalAnalyzer()
    ctx.temporal_analysis = temporal_analyzer.analyze(ctx.df, ctx.dataset)


def _stage_geo_analysis(ctx: PipelineContext) -> None:
    """Detect geographic patterns in the dataset."""
    geo_analyzer = GeoAnalyzer()
    ctx.geo_analysis = geo_analyzer.analyze(ctx.df, ctx.dataset)


def _stage_cross_column_assembly(ctx: PipelineContext) -> None:
    """Assemble cross-column profile from available results."""
    from profiling.cross_column_models import CorrelationAnalysis, TargetAnalysis, TemporalAnalysis, GeoAnalysis
    cross_profile = CrossColumnProfile(
        correlations=ctx.correlations or CorrelationAnalysis(correlation_matrix={}, strongest_pairs=[], multicollinearity=None, mutual_information={}),
        target=ctx.target_analysis or TargetAnalysis(is_target_detected=False),
        temporal=ctx.temporal_analysis or TemporalAnalysis(has_temporal_patterns=False),
        geo=ctx.geo_analysis or GeoAnalysis(has_geo_patterns=False),
    )
    ctx.dataset.cross_analysis = cross_profile.model_dump()


def _stage_insights(ctx: PipelineContext) -> None:
    """Generate quality scores, anomaly detection, feature ranking, and briefing."""
    quality = QualityScorer.calculate_scores(ctx.dataset)
    anomalies = AnomalyDetector.detect(ctx.dataset)
    rankings = FeatureRanker.rank_features(ctx.dataset)
    briefing = BriefingGenerator.generate(ctx.dataset, quality, anomalies, rankings)
    ctx.dataset.insights = DatasetInsights(
        quality_score=quality,
        anomalies=anomalies,
        feature_ranking=rankings,
        analyst_briefing=briefing,
    )


# ──────────────────────────────────────────
# Pipeline Definition
# ──────────────────────────────────────────

PROFILING_PIPELINE: list[PipelineStage] = [
    PipelineStage(name="Metadata & Schema",      run=_stage_metadata,              required=True),
    PipelineStage(name="Key Detection",           run=_stage_key_detection),
    PipelineStage(name="Column Profiling",        run=_stage_column_profiling,      required=True),
    PipelineStage(name="Correlation Analysis",    run=_stage_correlations),
    PipelineStage(name="Target Detection",        run=_stage_target_detection),
    PipelineStage(name="Temporal Analysis",       run=_stage_temporal_analysis),
    PipelineStage(name="Geo Analysis",            run=_stage_geo_analysis),
    PipelineStage(name="Cross-Column Assembly",   run=_stage_cross_column_assembly),
    PipelineStage(name="Insights Generation",     run=_stage_insights),
]


# ──────────────────────────────────────────
# Public API (backward-compatible)
# ──────────────────────────────────────────

class DataProfiler:
    """
    Orchestrates the complete profiling pipeline.
    For every column: universal stats + type-specific deep profiling.
    For the dataset: metadata, domain inference, key detection, completeness scoring.

    Uses PROFILING_PIPELINE — a list of PipelineStage objects — for explicit,
    testable ordering. Each stage is isolated; failure in one non-required
    stage is recorded as a warning but does not kill the pipeline.
    """

    @staticmethod
    def profile(
        df: pd.DataFrame,
        file_id: str,
        disk_size_bytes: int = 0,
        pipeline: list[PipelineStage] | None = None,
    ) -> ProfilingResult:
        """
        Run the full profiling pipeline on a DataFrame.

        Args:
            df: The ingested DataFrame to profile.
            file_id: Unique file identifier.
            disk_size_bytes: Original file size on disk.
            pipeline: Optional custom pipeline stages (defaults to PROFILING_PIPELINE).

        Returns:
            ProfilingResult with complete dataset and column profiles.
        """
        start_time = time.time()

        if df.empty:
            return ProfilingResult(
                success=True,
                file_id=file_id,
                profile=DatasetProfile(),
                warnings=["Dataset is empty — profiling skipped."],
            )

        ctx = PipelineContext(
            df=df,
            file_id=file_id,
            disk_size_bytes=disk_size_bytes,
        )

        stages = pipeline or PROFILING_PIPELINE

        for stage in stages:
            t0 = time.time()
            print(f"[PROFILING] Starting {stage.name}...")
            try:
                stage.run(ctx)
                print(f"[PROFILING] {stage.name} done in {time.time() - t0:.1f}s")
            except Exception as e:
                elapsed = time.time() - t0
                print(f"[PROFILING ERROR] {stage.name} failed after {elapsed:.1f}s: {e}")
                ctx.warnings.append(f"{stage.name} failed: {str(e)}")
                if stage.required:
                    import traceback
                    traceback.print_exc()
                    # Still return partial result for required failures
                    break

        ctx.dataset.profiling_time_seconds = round(time.time() - start_time, 3)

        return ProfilingResult(
            success=True,
            file_id=file_id,
            profile=ctx.dataset,
            warnings=ctx.warnings,
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
        return _profile_column(series, col_name, position, semantic_type_str, type_confidence)


# ──────────────────────────────────────────
# Column Profiling (extracted from class for reuse)
# ──────────────────────────────────────────

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
        profile.categorical = CategoricalProfiler.profile(non_null)

    else:
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


# ──────────────────────────────────────────
# Utility Functions
# ──────────────────────────────────────────

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
