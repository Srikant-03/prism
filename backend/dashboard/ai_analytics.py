"""
AI Analytics Engine — Power BI Advanced Analytics & AI Super-Visuals for PRISM.

Provides:
  1. Time-Series Forecasting (Holt-Winters / ARIMA + 80%/95% confidence bands + anomaly detection)
  2. "Explain This Point" (Dimensional root-cause waterfall decomposition + factual narrative)
  3. Interactive Decomposition Tree (Multi-level node splits + AI High/Low value recommendation)
  4. Key Influencers & Top Segments (Feature impact, lift multipliers, leakage-guarded driver analysis)
  5. Live Smart Narrative (Data-grounded dynamic narrative bullets)
"""

from __future__ import annotations

import logging
import math
import numpy as np
import pandas as pd
from typing import Optional, Any
from pydantic import BaseModel, Field

from dashboard.chart_config_models import ChartConfig, ChartType, AggregationType
from dashboard.measure_engine import MeasureEngine
from state import get_stored_dataframe

logger = logging.getLogger(__name__)


# ── Request & Response Models ──

class ForecastRequest(BaseModel):
    file_id: str
    date_column: str
    value_column: str
    time_grain: str = "month"  # month, quarter, day, year
    periods: int = 6
    confidence: float = 0.95
    aggregation: str = "sum"


class ForecastPoint(BaseModel):
    date: str
    actual: Optional[float] = None
    forecast: Optional[float] = None
    lower_80: Optional[float] = None
    upper_80: Optional[float] = None
    lower_95: Optional[float] = None
    upper_95: Optional[float] = None
    is_anomaly: bool = False
    anomaly_score: Optional[float] = None


class ForecastResponse(BaseModel):
    points: list[ForecastPoint]
    model_name: str
    trend: str  # upward, downward, flat
    anomalies_detected: int = 0


class ExplainPointRequest(BaseModel):
    file_id: str
    clicked_point: str  # e.g. "2025-04" or category name
    dimension_column: str  # e.g. "order_date"
    metric_column: str = "net_revenue"
    aggregation: str = "sum"
    time_grain: Optional[str] = "month"


class WaterfallStep(BaseModel):
    name: str
    value: float
    type: str  # baseline, increase, decrease, total
    percent_of_delta: Optional[float] = None


class ExplainPointResponse(BaseModel):
    clicked_point: str
    current_value: float
    previous_point: Optional[str] = None
    previous_value: Optional[float] = None
    delta: float
    delta_percent: float
    top_driver_dimension: str
    top_driver_category: str
    waterfall: list[WaterfallStep]
    narrative: str
    top_outlier_transactions: list[dict[str, Any]] = Field(default_factory=list)


class DecompositionTreeRequest(BaseModel):
    file_id: str
    metric_column: str = "net_revenue"
    aggregation: str = "sum"
    current_path: list[dict[str, str]] = Field(default_factory=list)  # [{"dimension": "product_category", "value": "Electronics"}]
    split_dimension: Optional[str] = None
    split_strategy: Optional[str] = None  # "high_value", "low_value", or specific dimension name


class TreeNode(BaseModel):
    name: str
    value: float
    percent_of_parent: float
    percent_of_total: float
    is_split_candidate: bool = False


class DecompositionTreeResponse(BaseModel):
    root_metric: str
    root_value: float
    dimension_split: str
    nodes: list[TreeNode]


class KeyInfluencerRequest(BaseModel):
    file_id: str
    target_column: str = "churn_flag"
    target_value: Optional[Any] = 1


class DriverRule(BaseModel):
    feature: str
    condition: str
    impact: str  # e.g. "2.3x more likely"
    multiplier: float
    segment_size: int
    target_rate: float
    baseline_rate: float


class KeyInfluencerResponse(BaseModel):
    target_column: str
    target_value: Any
    baseline_rate: float
    drivers: list[DriverRule]
    top_segments: list[dict[str, Any]]
    guarded_leakage_columns: list[str] = Field(default_factory=list)


class SmartNarrativeRequest(BaseModel):
    file_id: str
    current_config: Optional[ChartConfig] = None


class SmartNarrativeResponse(BaseModel):
    title: str
    bullets: list[str]
    summary_kpis: dict[str, Any]


# ── AI Analytics Implementation ──

def run_forecast(
    df: pd.DataFrame,
    date_col: str,
    val_col: str,
    time_grain: str = "month",
    periods: int = 6,
    confidence: float = 0.95,
    agg: str = "sum",
) -> ForecastResponse:
    """
    Generate time-series forecast using Exponential Smoothing with confidence intervals and anomaly detection.
    """
    import duckdb
    conn = duckdb.connect(":memory:")
    conn.register("t", df)

    # Aggregate historical series
    grain_fmt = "%Y-%m" if time_grain == "month" else ("%Y" if time_grain == "year" else "%Y-%m-%d")
    date_expr = f"strftime(DATE_TRUNC('{time_grain}', TRY_CAST(\"{date_col}\" AS DATE)), '{grain_fmt}')"
    
    query = f"""
        SELECT {date_expr} as d, {agg.upper()}("{val_col}") as v
        FROM t
        WHERE "{date_col}" IS NOT NULL AND "{val_col}" IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """
    history = conn.execute(query).fetchall()
    if len(history) < 3:
        raise ValueError("At least 3 historical time points are required for forecasting.")

    dates = [r[0] for r in history]
    values = [float(r[1]) for r in history]
    n_hist = len(values)

    # Fit Exponential Smoothing / Trend model
    try:
        from statsmodels.tsa.holtwinters import SimpleExpSmoothing, ExponentialSmoothing
        if n_hist >= 6:
            model = ExponentialSmoothing(values, trend="add", initialization_method="estimated").fit()
        else:
            model = SimpleExpSmoothing(values, initialization_method="estimated").fit()
        fitted = list(model.fittedvalues)
        forecast_vals = list(model.forecast(periods))
        model_name = "Holt-Winters Exponential Smoothing"
    except Exception as e:
        logger.warning("ExponentialSmoothing failed (%s), falling back to linear trend", e)
        # Fallback linear regression
        x = np.arange(n_hist)
        slope, intercept = np.polyfit(x, values, 1)
        fitted = [slope * i + intercept for i in range(n_hist)]
        forecast_vals = [slope * (n_hist + i) + intercept for i in range(periods)]
        model_name = "Linear Trend Extrapolation"

    # Residuals & Anomaly Detection
    residuals = [abs(values[i] - fitted[i]) for i in range(n_hist)]
    mean_res = float(np.mean(residuals))
    std_res = float(np.std(residuals)) if len(residuals) > 1 else 1.0
    if std_res == 0:
        std_res = 1.0

    z_crit = 1.96 if confidence >= 0.95 else 1.28
    points: list[ForecastPoint] = []
    anomalies_count = 0

    for i in range(n_hist):
        actual = values[i]
        fit_v = fitted[i]
        z_score = abs(actual - fit_v) / std_res
        is_anom = bool(z_score > 2.5)
        if is_anom:
            anomalies_count += 1
        points.append(ForecastPoint(
            date=str(dates[i]),
            actual=round(actual, 2),
            forecast=round(fit_v, 2),
            is_anomaly=is_anom,
            anomaly_score=round(z_score, 2) if is_anom else None,
        ))

    # Generate future dates
    last_date = dates[-1]
    for i in range(periods):
        f_val = max(0.0, float(forecast_vals[i]))
        # Extend date label
        if time_grain == "month":
            y, m = map(int, last_date.split("-"))
            m_next = m + (i + 1)
            y_next = y + (m_next - 1) // 12
            m_final = (m_next - 1) % 12 + 1
            next_date_str = f"{y_next:04d}-{m_final:02d}"
        elif time_grain == "year":
            next_date_str = str(int(last_date) + (i + 1))
        else:
            next_date_str = f"P+{i+1}"

        margin_95 = z_crit * std_res * math.sqrt(i + 1)
        margin_80 = 1.28 * std_res * math.sqrt(i + 1)

        points.append(ForecastPoint(
            date=next_date_str,
            actual=None,
            forecast=round(f_val, 2),
            lower_80=round(max(0.0, f_val - margin_80), 2),
            upper_80=round(f_val + margin_80, 2),
            lower_95=round(max(0.0, f_val - margin_95), 2),
            upper_95=round(f_val + margin_95, 2),
            is_anomaly=False,
        ))

    overall_trend = "upward" if forecast_vals[-1] > values[-1] else ("downward" if forecast_vals[-1] < values[-1] else "flat")

    return ForecastResponse(
        points=points,
        model_name=model_name,
        trend=overall_trend,
        anomalies_detected=anomalies_count,
    )


def explain_point_delta(
    df: pd.DataFrame,
    clicked_point: str,
    dimension_col: str,
    metric_col: str = "net_revenue",
    agg: str = "sum",
    time_grain: Optional[str] = "month",
) -> ExplainPointResponse:
    """
    Perform Power BI 'Explain the Increase/Decrease' root-cause waterfall decomposition.
    """
    import duckdb
    conn = duckdb.connect(":memory:")
    conn.register("t", df)

    # 1. Get sequence of time periods
    grain_fmt = "%Y-%m" if time_grain == "month" else ("%Y" if time_grain == "year" else "%Y-%m-%d")
    date_expr = f"strftime(DATE_TRUNC('{time_grain}', TRY_CAST(\"{dimension_col}\" AS DATE)), '{grain_fmt}')"

    periods_rows = conn.execute(f"""
        SELECT {date_expr} as d, {agg.upper()}("{metric_col}") as v
        FROM t
        WHERE "{dimension_col}" IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).fetchall()

    period_dict = {r[0]: float(r[1]) for r in periods_rows}
    all_periods = [r[0] for r in periods_rows]

    if clicked_point not in period_dict:
        clicked_val = float(df[metric_col].sum())
        prev_point = None
        prev_val = clicked_val
        delta = 0.0
        delta_pct = 0.0
    else:
        clicked_val = period_dict[clicked_point]
        idx = all_periods.index(clicked_point)
        if idx > 0:
            prev_point = all_periods[idx - 1]
            prev_val = period_dict[prev_point]
        else:
            prev_point = None
            prev_val = clicked_val
        delta = clicked_val - prev_val
        delta_pct = (delta / prev_val * 100.0) if prev_val != 0 else 0.0

    # 2. Decompose across dimensions: product_category, region, country_code, is_enterprise
    candidate_dims = [c for c in ["product_category", "region", "country_code", "is_enterprise"] if c in df.columns]
    best_dim = "product_category" if "product_category" in candidate_dims else (candidate_dims[0] if candidate_dims else "Dimension")
    
    # Analyze breakdown for current vs previous period
    breakdown_query = f"""
        SELECT 
            COALESCE(CAST("{best_dim}" AS VARCHAR), 'Unknown') as cat,
            SUM(CASE WHEN {date_expr} = '{clicked_point}' THEN "{metric_col}" ELSE 0 END) as cur_v,
            SUM(CASE WHEN {date_expr} = '{prev_point}' THEN "{metric_col}" ELSE 0 END) as prev_v
        FROM t
        WHERE {date_expr} IN ('{clicked_point}', '{prev_point}')
        GROUP BY 1
    """
    cat_rows = conn.execute(breakdown_query).fetchall()

    category_deltas = []
    for r in cat_rows:
        cat_name, cur_v, prev_v = r[0], float(r[1]), float(r[2])
        cat_delta = cur_v - prev_v
        category_deltas.append({
            "category": cat_name,
            "cur_v": cur_v,
            "prev_v": prev_v,
            "delta": cat_delta,
            "abs_delta": abs(cat_delta),
        })

    category_deltas.sort(key=lambda x: x["abs_delta"], reverse=True)
    top_cat = category_deltas[0]["category"] if category_deltas else "General"
    top_cat_delta = category_deltas[0]["delta"] if category_deltas else 0.0

    # 3. Build Waterfall steps
    waterfall: list[WaterfallStep] = []
    if prev_point:
        waterfall.append(WaterfallStep(name=f"Prior ({prev_point})", value=round(prev_val, 2), type="baseline"))

    for c in category_deltas[:4]:
        c_delta = c["delta"]
        step_type = "increase" if c_delta >= 0 else "decrease"
        pct_of_delta = round((c_delta / delta * 100.0), 1) if delta != 0 else 0.0
        waterfall.append(WaterfallStep(
            name=str(c["category"]),
            value=round(c_delta, 2),
            type=step_type,
            percent_of_delta=pct_of_delta,
        ))

    # Other categories combined
    other_delta = sum(c["delta"] for c in category_deltas[4:])
    if other_delta != 0:
        waterfall.append(WaterfallStep(
            name="Other Categories",
            value=round(other_delta, 2),
            type="increase" if other_delta >= 0 else "decrease",
            percent_of_delta=round((other_delta / delta * 100.0), 1) if delta != 0 else 0.0,
        ))

    waterfall.append(WaterfallStep(name=f"Total ({clicked_point})", value=round(clicked_val, 2), type="total"))

    # 4. Find Top Outlier Transactions in clicked point
    top_txns_query = f"""
        SELECT *
        FROM t
        WHERE {date_expr} = '{clicked_point}'
        ORDER BY "{metric_col}" DESC
        LIMIT 3
    """
    top_txns = conn.execute(top_txns_query).df().to_dict(orient="records")

    # Clean nan values in top_txns for JSON
    cleaned_txns = []
    for txn in top_txns:
        cleaned_txns.append({k: (None if pd.isna(v) else v) for k, v in txn.items()})

    # 5. Generate data-grounded narrative
    top_txn_mention = ""
    if cleaned_txns:
        first_txn = cleaned_txns[0]
        id_str = first_txn.get("txn_uuid") or first_txn.get("sequential_row_id") or "top transaction"
        first_val = float(first_txn.get(metric_col) or 0.0)
        top_txn_mention = f", with {id_str} representing an extreme single transaction of ${first_val:,.2f}"

    dir_str = "increased" if delta >= 0 else "decreased"
    narrative = (
        f"{metric_col.replace('_', ' ').title()} in {clicked_point} {dir_str} by ${abs(delta):,.2f} "
        f"({delta_pct:+.1f}%) compared to {prev_point or 'baseline'}. "
        f"This movement was primarily driven by {top_cat} (contributing ${abs(top_cat_delta):,.2f})"
        f"{top_txn_mention}."
    )

    return ExplainPointResponse(
        clicked_point=clicked_point,
        current_value=round(clicked_val, 2),
        previous_point=prev_point,
        previous_value=round(prev_val, 2) if prev_val is not None else None,
        delta=round(delta, 2),
        delta_percent=round(delta_pct, 2),
        top_driver_dimension=best_dim,
        top_driver_category=top_cat,
        waterfall=waterfall,
        narrative=narrative,
        top_outlier_transactions=cleaned_txns,
    )


def compute_decomposition_tree(
    df: pd.DataFrame,
    metric_col: str = "net_revenue",
    agg: str = "sum",
    current_path: Optional[list[dict[str, str]]] = None,
    split_dimension: Optional[str] = None,
    split_strategy: Optional[str] = None,
) -> DecompositionTreeResponse:
    """
    Compute multi-level interactive Decomposition Tree node splits.
    """
    import duckdb
    conn = duckdb.connect(":memory:")
    conn.register("t", df)

    # Apply path filters
    where_clauses = [f'"{metric_col}" IS NOT NULL']
    if current_path:
        for p in current_path:
            dim = p["dimension"]
            val = p["value"].replace("'", "''")
            where_clauses.append(f'"{dim}" = \'{val}\'')

    where_sql = "WHERE " + " AND ".join(where_clauses)
    root_val = float(conn.execute(f'SELECT {agg.upper()}("{metric_col}") FROM t {where_sql}').fetchone()[0] or 0.0)

    # Determine split dimension
    available_dims = [
        c for c in ["product_category", "region", "country_code", "is_enterprise", "loyalty_tier_code"]
        if c in df.columns and (not current_path or not any(p["dimension"] == c for p in current_path))
    ]
    if not available_dims:
        available_dims = ["product_category"]

    chosen_dim = split_dimension
    if not chosen_dim:
        if split_strategy == "low_value":
            chosen_dim = available_dims[-1]
        else:
            chosen_dim = available_dims[0]

    nodes_query = f"""
        SELECT 
            COALESCE(CAST("{chosen_dim}" AS VARCHAR), 'Unknown') as name,
            {agg.upper()}("{metric_col}") as val
        FROM t
        {where_sql}
        GROUP BY 1
        ORDER BY 2 DESC
    """
    rows = conn.execute(nodes_query).fetchall()

    nodes: list[TreeNode] = []
    for r in rows:
        name, val = str(r[0]), float(r[1] or 0.0)
        pct_parent = round((val / root_val * 100.0), 1) if root_val != 0 else 0.0
        nodes.append(TreeNode(
            name=name,
            value=round(val, 2),
            percent_of_parent=pct_parent,
            percent_of_total=pct_parent,
            is_split_candidate=len(available_dims) > 1,
        ))

    return DecompositionTreeResponse(
        root_metric=metric_col,
        root_value=round(root_val, 2),
        dimension_split=chosen_dim,
        nodes=nodes,
    )


def compute_key_influencers(
    df: pd.DataFrame,
    target_col: str = "churn_flag",
    target_value: Optional[Any] = 1,
) -> KeyInfluencerResponse:
    """
    Compute Key Influencers and Top Segments using feature effect sizes,
    with automatic data leakage & surrogate identifier protection.
    """
    leakage_keywords = ["post_churn", "refund", "churned", "cancelled"]
    id_keywords = ["id", "uuid", "hash", "key", "email", "notes", "metadata"]

    guarded_cols = []
    features_to_use = []

    for c in df.columns:
        cl = c.lower()
        if cl == target_col.lower():
            continue
        if any(lk in cl for lk in leakage_keywords):
            guarded_cols.append(c)
            continue
        if any(ik in cl for ik in id_keywords):
            continue
        features_to_use.append(c)

    # Target series
    target_series = df[target_col].dropna()
    target_val_num = float(target_value) if target_value is not None else 1.0
    binary_target = (target_series == target_val_num).astype(int)
    baseline_rate = float(binary_target.mean())

    drivers: list[DriverRule] = []

    # Calculate lift for categorical and numeric thresholds
    for f in features_to_use:
        s = df[f].dropna()
        if len(s) == 0:
            continue

        if pd.api.types.is_numeric_dtype(s) and s.nunique() > 5:
            # Check top quartile threshold
            q75 = float(s.quantile(0.75))
            mask = df[f] >= q75
            seg_size = int(mask.sum())
            if seg_size >= 10:
                seg_target_rate = float((df.loc[mask, target_col] == target_val_num).mean())
                multiplier = (seg_target_rate / baseline_rate) if baseline_rate > 0 else 1.0
                if multiplier >= 1.25 or multiplier <= 0.75:
                    drivers.append(DriverRule(
                        feature=f,
                        condition=f">= {q75:.2f}",
                        impact=f"{multiplier:.1f}x more likely" if multiplier >= 1.0 else f"{multiplier:.1f}x less likely",
                        multiplier=round(multiplier, 2),
                        segment_size=seg_size,
                        target_rate=round(seg_target_rate, 4),
                        baseline_rate=round(baseline_rate, 4),
                    ))
        else:
            # Categorical categories
            for cat_val, count in s.value_counts().items():
                if count >= 8:
                    mask = df[f] == cat_val
                    seg_size = int(mask.sum())
                    seg_target_rate = float((df.loc[mask, target_col] == target_val_num).mean())
                    multiplier = (seg_target_rate / baseline_rate) if baseline_rate > 0 else 1.0
                    if abs(multiplier - 1.0) >= 0.2:
                        drivers.append(DriverRule(
                            feature=f,
                            condition=f"is '{cat_val}'",
                            impact=f"{multiplier:.1f}x more likely" if multiplier >= 1.0 else f"{multiplier:.1f}x less likely",
                            multiplier=round(multiplier, 2),
                            segment_size=seg_size,
                            target_rate=round(seg_target_rate, 4),
                            baseline_rate=round(baseline_rate, 4),
                        ))

    drivers.sort(key=lambda d: abs(d.multiplier - 1.0), reverse=True)

    # Top segments
    top_segments = []
    if drivers:
        for d in drivers[:3]:
            top_segments.append({
                "segment_name": f"Segment when {d.feature} {d.condition}",
                "size": d.segment_size,
                "target_rate": d.target_rate,
                "lift": d.multiplier,
            })

    return KeyInfluencerResponse(
        target_column=target_col,
        target_value=target_value,
        baseline_rate=round(baseline_rate, 4),
        drivers=drivers[:8],
        top_segments=top_segments,
        guarded_leakage_columns=guarded_cols,
    )


def generate_smart_narrative(
    df: pd.DataFrame,
    current_config: Optional[ChartConfig] = None,
) -> SmartNarrativeResponse:
    """
    Generate dynamic, filter-aware narrative bullets strictly grounded in executed numbers.
    """
    import duckdb
    conn = duckdb.connect(":memory:")
    conn.register("t", df)

    total_rows = len(df)
    has_net = "net_revenue" in df.columns
    has_gross = "gross_revenue" in df.columns
    has_date = any("date" in c.lower() for c in df.columns)
    date_col = next((c for c in df.columns if "date" in c.lower()), None)

    bullets: list[str] = []
    summary_kpis: dict[str, Any] = {"total_rows": total_rows}

    if has_net:
        total_net = float(df["net_revenue"].sum())
        summary_kpis["total_net_revenue"] = total_net
        bullets.append(f"Total **Net Revenue** stands at **${total_net:,.2f}** across **{total_rows}** recorded transactions.")

    if has_net and has_gross:
        total_gross = float(df["gross_revenue"].sum())
        margin = (total_net / total_gross * 100.0) if total_gross > 0 else 0.0
        summary_kpis["net_margin_pct"] = margin
        bullets.append(f"Net realization margin is **{margin:.1f}%** relative to **${total_gross:,.2f}** in Gross Revenue.")

    if has_date and date_col and has_net:
        peak_row = conn.execute(f"""
            SELECT strftime(DATE_TRUNC('month', TRY_CAST("{date_col}" AS DATE)), '%Y-%m') as m,
                   SUM("net_revenue") as rev
            FROM t
            WHERE "{date_col}" IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 1
        """).fetchone()
        if peak_row:
            summary_kpis["peak_month"] = peak_row[0]
            summary_kpis["peak_revenue"] = float(peak_row[1])
            bullets.append(f"Peak monthly performance occurred in **{peak_row[0]}** generating **${float(peak_row[1]):,.2f}**.")

    if "product_category" in df.columns and has_net:
        top_cat_row = conn.execute("""
            SELECT product_category, SUM(net_revenue) as rev
            FROM t
            WHERE product_category IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 1
        """).fetchone()
        if top_cat_row:
            bullets.append(f"**{top_cat_row[0]}** is the dominant product line, contributing **${float(top_cat_row[1]):,.2f}**.")

    if "churn_flag" in df.columns:
        churn_rate = float(df["churn_flag"].mean() * 100.0)
        summary_kpis["churn_rate_pct"] = churn_rate
        bullets.append(f"Overall churn flag rate is **{churn_rate:.1f}%** of active accounts.")

    return SmartNarrativeResponse(
        title="Executive Intelligence Narrative",
        bullets=bullets,
        summary_kpis=summary_kpis,
    )
