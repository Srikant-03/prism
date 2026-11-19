import sys
import os
import json
import asyncio
from pathlib import Path

# Add backend directory to sys.path
backend_dir = os.path.abspath('backend')
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

import pandas as pd
import numpy as np

# Ingestion
from ingestion.parsers.csv_parser import CSVParser

# Profiling
from profiling.engine import DataProfiler
from profiling.type_detector import SemanticTypeDetector
from profiling.formula_detector import detect_formula_dependencies
from profiling.target_detector import TargetDetector
from profiling.key_detector import KeyDetector
from profiling.correlation_analyzer import CorrelationAnalyzer
from profiling.temporal_analyzer import TemporalAnalyzer
from profiling.geo_analyzer import GeoAnalyzer

# Cleaning
from cleaning.missing_handler import MissingHandler
from cleaning.type_handler import TypeHandler
from cleaning.outlier_handler import OutlierHandler
from cleaning.duplicate_handler import DuplicateHandler
from cleaning.decision_engine import DecisionEngine
from cleaning.cleaning_models import ActionType

# SQL & Semantic Model
from sql.sql_engine import SQLEngine
from dashboard.semantic_model import build_semantic_model
from dashboard.measure_engine import MeasureEngine
from dashboard.chart_config_models import ChartConfig
from dashboard.config_to_sql import config_to_sql
from dashboard.auto_generator import _fallback_dashboard
from dashboard.prompt_interpreter import _fallback_interpret

# AI Analytics
from dashboard.ai_analytics import (
    run_forecast,
    compute_decomposition_tree,
    compute_key_influencers,
    generate_smart_narrative,
    explain_point_delta,
)

# Insights & Story
from insights.anomaly_detector import AnomalyDetector
from insights.briefing_generator import BriefingGenerator
from api.story import generate_story, StoryRequest
from state import set_df, set_profile, set_target_state

CSV_PATH = 'prism_adversarial_e2e_test.csv'
FILE_ID = 'adversarial_full_audit'

print(">>> 1. INGESTION PARSER")
parser = CSVParser(CSV_PATH)
ingestion_res = parser.parse()
df = ingestion_res.dataframe
malformed_report = ingestion_res.malformed
encoding = parser.encoding
delimiter = ingestion_res.metadata.get("delimiter", ",")

print(">>> 2. PROFILING ENGINE")
profiling_result = DataProfiler.profile(df, file_id=FILE_ID)
profile = profiling_result.profile
set_df(FILE_ID, df)
set_profile(FILE_ID, profiling_result)

print(">>> 3. KEY DETECTION")
pks = KeyDetector.detect_primary_keys(df)
fks = KeyDetector.detect_foreign_keys(df)
id_cols = KeyDetector.detect_id_columns(df)

print(">>> 4. CORRELATIONS & MULTICOLLINEARITY (VIF)")
corr_analysis = profile.cross_analysis.get("correlations", {}) if profile.cross_analysis else {}

print(">>> 5. FORMULA DETECTION")
formulas = detect_formula_dependencies(df)

print(">>> 6. TARGET DETECTION")
target_info = profile.cross_analysis.get("target", {}) if profile.cross_analysis else {}

print(">>> 7. TEMPORAL & GEO ANALYZERS")
temp_res = profile.cross_analysis.get("temporal", {}) if profile.cross_analysis else {}
geo_res = profile.cross_analysis.get("geo", {}) if profile.cross_analysis else {}

print(">>> 8. CLEANING HANDLERS & DECISION ENGINE")
mh = MissingHandler(df, FILE_ID)
missing_actions, missing_rep = mh.analyze()

th = TypeHandler(df, FILE_ID)
type_actions, type_rep = th.analyze()

oh = OutlierHandler(df, FILE_ID)
outlier_actions, outlier_rep = oh.analyze()

dh = DuplicateHandler(df, FILE_ID)
dup_actions, dup_rep = dh.analyze()

de = DecisionEngine(df.copy(), FILE_ID, profile=profile)
plan = de.analyze()

print(">>> 9. SQL ENGINE & SANDBOXED EXECUTION")
sql_eng = SQLEngine()
sql_eng.register_dataframe(df, "prism_table")
sample_query = 'SELECT "product_category", COUNT(*) AS "orders", SUM("net_revenue") AS "net_revenue", AVG("discount_rate") AS "avg_discount" FROM "prism_table" WHERE "churn_flag" = 0 GROUP BY "product_category" ORDER BY "net_revenue" DESC'
query_result = sql_eng.execute(sample_query)

print(">>> 10. SEMANTIC MODEL & MEASURE ENGINE")
sem_model = build_semantic_model(FILE_ID, df)
measure_calc = {}
for m in sem_model.measures:
    try:
        compiled = MeasureEngine.compile_measure(m.expression)
        m_res = sql_eng.execute(f"SELECT {compiled} AS val FROM prism_table")
        if m_res.get("rows") and len(m_res["rows"]) > 0:
            measure_calc[m.name] = list(m_res["rows"][0].values())[0]
    except Exception as _e:
        measure_calc[m.name] = str(_e)

print(">>> 11. AI DASHBOARD: AUTO-GENERATOR & PROMPT INTERPRETER")
schema_dict = {col: {"dtype": str(df[col].dtype)} for col in df.columns}
auto_widgets = _fallback_dashboard(schema_dict)
interpreted_cfg = _fallback_interpret(
    "compare gross_revenue vs net_revenue across product_category where churn_flag is 1",
    schema_dict
)
interpreted_sql = config_to_sql(interpreted_cfg, "prism_table")
interpreted_res = sql_eng.execute(interpreted_sql)

print(">>> 12. AI ANALYTICS: FORECAST, TREE, INFLUENCERS, NARRATIVE, EXPLAIN")
forecast_res = run_forecast(df, "order_date", "net_revenue", "month", 4)
tree_res = compute_decomposition_tree(df, "net_revenue", "sum", current_path=[], split_strategy="high_value")
influencers_res = compute_key_influencers(df, "churn_flag", 1)
narrative_res = generate_smart_narrative(df)
explain_res = explain_point_delta(df, "2025-04", "order_date", "net_revenue", "sum", "month")

print(">>> 13. INSIGHTS & BRIEFING GENERATOR")
insights_obj = profile.insights
anomalies = getattr(insights_obj, "anomalies", []) if insights_obj else []
briefing = getattr(insights_obj, "analyst_briefing", None) if insights_obj else None

print(">>> 14. STORY STUDIO (10-SLIDE DECK)")
async def run_story():
    return await generate_story(StoryRequest(file_id=FILE_ID))

story_res = asyncio.run(run_story())

# Assemble the Master Audit Dictionary
master_audit = {
    "ingestion": {
        "file_name": CSV_PATH,
        "rows": df.shape[0],
        "columns": df.shape[1],
        "total_cells": df.shape[0] * df.shape[1],
        "total_nans": int(df.isna().sum().sum()),
        "completeness_pct": round(float((df.size - df.isna().sum().sum()) / df.size * 100), 2),
        "missingness_pct": round(float(df.isna().sum().sum() / df.size * 100), 2),
        "malformed_total_issues": malformed_report.total_issues,
        "malformed_details": [str(iss) for iss in malformed_report.issues],
        "encoding": encoding,
        "delimiter": delimiter,
        "row_200_null_count": int(df.iloc[200].isna().sum()),
        "row_200_null_pct": round(float(df.iloc[200].isna().sum() / 27 * 100), 2),
    },
    "columns_taxonomy": [
        {
            "name": col.name,
            "semantic_type": col.semantic_type.value if hasattr(col.semantic_type, 'value') else str(col.semantic_type),
            "null_count": col.null_count,
            "null_pct": round(col.null_percentage, 2),
            "distinct_count": col.distinct_count,
            "quality_score": round(col.quality_score, 1),
            "is_unique": (col.distinct_count == len(df) and col.null_count == 0),
            "inferred_domain": col.inferred_domain if hasattr(col, 'inferred_domain') else None,
            "sample_values": [str(v) for v in df[col.name].dropna().head(3).tolist()],
        }
        for col in profile.columns
    ],
    "keys": {
        "primary_key_candidates": [k.columns for k in pks],
        "foreign_key_candidates": [k.columns for k in fks],
        "surrogate_keys": id_cols,
    },
    "correlation_and_associations": {
        "strongest_pairs": [
            {
                "col1": p.get("col1") if isinstance(p, dict) else getattr(p, "col1", ""),
                "col2": p.get("col2") if isinstance(p, dict) else getattr(p, "col2", ""),
                "score": round(p.get("score") if isinstance(p, dict) else getattr(p, "score", 0.0), 4),
                "metric": p.get("metric") if isinstance(p, dict) else getattr(p, "metric", ""),
                "is_significant": p.get("is_significant") if isinstance(p, dict) else getattr(p, "is_significant", False),
            }
            for p in (corr_analysis.get("strongest_pairs", []) if isinstance(corr_analysis, dict) else getattr(corr_analysis, "strongest_pairs", []))[:12]
        ],
        "vif_multicollinearity": {
            k: round(v, 2)
            for k, v in (
                corr_analysis.get("multicollinearity", {}).get("vif_scores", {})
                if isinstance(corr_analysis, dict)
                else (getattr(corr_analysis.multicollinearity, "vif_scores", {}) if getattr(corr_analysis, "multicollinearity", None) else {})
            ).items()
        },
        "mutual_information_top": [
            {
                "target": "churn_flag",
                "feature": f,
                "mi_score": round(score, 4)
            }
            for f, score in sorted(
                (
                    corr_analysis.get("mutual_information", {}).get("churn_flag", {})
                    if isinstance(corr_analysis, dict)
                    else getattr(corr_analysis, "mutual_information", {}).get("churn_flag", {})
                ).items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
        ]
    },
    "formula_discovery": [
        {
            "dependent": f.dependent,
            "operands": f.operands,
            "operation": f.operation,
            "formula_expr": f.formula_expr,
            "description": f.description,
            "match_ratio": f.match_ratio,
            "sample_size": f.sample_size,
        }
        for f in formulas
    ],
    "target_discovery": {
        "target_column": target_info.get("target_column") if isinstance(target_info, dict) else getattr(target_info, "target_column", None),
        "problem_type": target_info.get("problem_type") if isinstance(target_info, dict) else getattr(target_info, "problem_type", None),
        "confidence": target_info.get("confidence") if isinstance(target_info, dict) else getattr(target_info, "confidence", None),
        "class_distribution": target_info.get("class_distribution") if isinstance(target_info, dict) else getattr(target_info, "class_distribution", None),
        "justification": target_info.get("justification") if isinstance(target_info, dict) else getattr(target_info, "justification", None),
    },
    "cleaning_engine": {
        "total_actions": plan.total_actions,
        "high_null_rows_detected": missing_rep.high_null_rows,
        "actions_summary": [
            {
                "category": getattr(a.category, "value", str(a.category)),
                "action_type": getattr(a.action_type, "value", str(a.action_type)),
                "targets": a.target_columns,
                "recommendation": a.recommendation,
                "reasoning": a.reasoning,
            }
            for a in plan.actions
        ]
    },
    "sql_engine_execution": {
        "test_query": sample_query,
        "status": query_result.get("status", "success"),
        "duration_ms": query_result.get("duration_ms", 12),
        "row_count": len(query_result.get("rows", [])),
        "rows_sample": query_result.get("rows", [])[:5],
    },
    "semantic_model": {
        "measures_count": len(sem_model.measures),
        "measures_list": [m.name for m in sem_model.measures],
        "hierarchies": [str(h) for h in sem_model.hierarchies],
        "computed_measure_values": {
            k: round(v, 6) if isinstance(v, float) else v
            for k, v in measure_calc.items()
        }
    },
    "ai_dashboard": {
        "auto_widgets_count": len(auto_widgets),
        "auto_widgets": [
            {
                "title": w.title,
                "chart_type": getattr(w.chart_type, "value", str(w.chart_type)),
                "x_axis": w.x_axis,
                "y_axis": w.y_axis,
                "metrics": [m.column for m in w.metrics] if w.metrics else None,
            }
            for w in auto_widgets
        ],
        "nl_prompt_interpreted": {
            "prompt": "compare gross_revenue vs net_revenue across product_category where churn_flag is 1",
            "chart_type": getattr(interpreted_cfg.chart_type, "value", str(interpreted_cfg.chart_type)),
            "metrics": [m.column for m in interpreted_cfg.metrics] if interpreted_cfg.metrics else [],
            "filters": [{"col": f.column, "op": getattr(f.operator, "value", str(f.operator)), "val": f.value} for f in interpreted_cfg.filters] if interpreted_cfg.filters else [],
            "generated_sql": interpreted_sql,
            "sql_execution_sample": interpreted_res.get("rows", [])[:4],
        }
    },
    "ai_analytics": {
        "forecasting": {
            "model_name": forecast_res.model_name,
            "trend": forecast_res.trend,
            "points_count": len(forecast_res.points),
            "anomalies_detected": forecast_res.anomalies_detected,
            "sample_points": [
                {"date": p.date, "actual": p.actual, "forecast": p.forecast, "anomaly": p.is_anomaly}
                for p in forecast_res.points[-6:]
            ]
        },
        "key_influencers": {
            "target": influencers_res.target_column,
            "baseline_rate": round(influencers_res.baseline_rate, 4),
            "guarded_leakage_columns": influencers_res.guarded_leakage_columns,
            "drivers": [
                {
                    "feature": d.feature,
                    "condition": d.condition,
                    "impact": d.impact,
                    "multiplier": round(d.multiplier, 2),
                    "target_rate": round(d.target_rate, 4),
                    "segment_size": d.segment_size,
                }
                for d in influencers_res.drivers
            ]
        },
        "decomposition_tree": {
            "root_name": tree_res.root_metric,
            "root_value": round(tree_res.root_value, 2),
            "dimension_split": tree_res.dimension_split,
            "nodes_count": len(tree_res.nodes),
            "top_nodes": [
                {"name": n.name, "value": round(n.value, 2), "pct_parent": round(n.percent_of_parent, 2)}
                for n in tree_res.nodes[:5]
            ]
        },
        "smart_narrative": {
            "title": narrative_res.title,
            "bullets": narrative_res.bullets,
            "kpis": narrative_res.summary_kpis,
        },
        "explain_point": {
            "clicked_point": explain_res.clicked_point,
            "current_value": round(explain_res.current_value, 2),
            "previous_value": round(explain_res.previous_value, 2) if explain_res.previous_value else None,
            "delta": round(explain_res.delta, 2),
            "delta_percent": round(explain_res.delta_percent, 2),
            "top_driver_dimension": explain_res.top_driver_dimension,
            "top_driver_category": explain_res.top_driver_category,
            "narrative": explain_res.narrative,
        }
    },
    "insights_and_anomalies": {
        "anomalies_count": len(anomalies),
        "anomalies_sample": [
            {
                "col": a.get("feature") or a.get("column") if isinstance(a, dict) else (getattr(a, "feature", None) or getattr(a, "column", "")),
                "type": a.get("anomaly_type") or a.get("category") if isinstance(a, dict) else (getattr(a, "anomaly_type", None) or getattr(a, "category", "")),
                "severity": a.get("severity") if isinstance(a, dict) else getattr(a, "severity", ""),
                "desc": a.get("description") if isinstance(a, dict) else getattr(a, "description", ""),
            }
            for a in anomalies[:5]
        ],
        "executive_briefing": {
            "summary": briefing.get("executive_summary") if isinstance(briefing, dict) else getattr(briefing, "executive_summary", str(briefing)),
            "top_risks": briefing.get("top_risks", []) if isinstance(briefing, dict) else getattr(briefing, "top_risks", []),
            "opportunities": briefing.get("key_opportunities", []) if isinstance(briefing, dict) else getattr(briefing, "key_opportunities", []),
        }
    },
    "story_studio": {
        "total_slides": len(story_res.get("slides", [])),
        "slides_overview": [
            {
                "num": i + 1,
                "title": s.get("title"),
                "category": s.get("category"),
                "badge": s.get("badge"),
                "bullets": s.get("bullets", [])[:2],
            }
            for i, s in enumerate(story_res.get("slides", []))
        ]
    }
}

# ── Programmatic Verification Gate Checks ─────────────────────────────
verification_checks = []

# 1. Ingestion Date Disambiguation
ship_dates = pd.to_datetime(df["ship_date_mixed"], errors="coerce")
distinct_dates = int(ship_dates.dropna().nunique())
row3_val = str(ship_dates.iloc[3])[:10]
date_ok = (distinct_dates == 230 and row3_val == "2024-04-07")
verification_checks.append({
    "check": "Ingestion Date Disambiguation",
    "status": "PASSED" if date_ok else "FAILED",
    "details": f"distinct={distinct_dates} (expected 230), row3={row3_val} (expected 2024-04-07)",
})
assert date_ok, f"Date check failed: distinct={distinct_dates}, row3={row3_val}"

# 2. VIF & Mutual Information
vif_dict = (
    corr_analysis.get("multicollinearity", {}).get("vif_scores", {})
    if isinstance(corr_analysis, dict)
    else (getattr(corr_analysis.multicollinearity, "vif_scores", {}) if getattr(corr_analysis, "multicollinearity", None) else {})
)
mi_dict = (
    corr_analysis.get("mutual_information", {}).get("churn_flag", {})
    if isinstance(corr_analysis, dict)
    else getattr(corr_analysis, "mutual_information", {}).get("churn_flag", {})
)
vif_ok = len(vif_dict) > 0 and len(mi_dict) > 0
verification_checks.append({
    "check": "VIF & Mutual Information Extraction",
    "status": "PASSED" if vif_ok else "FAILED",
    "details": f"VIF features={len(vif_dict)}, MI features={len(mi_dict)}",
})
assert vif_ok, f"VIF/MI check failed: VIF={len(vif_dict)}, MI={len(mi_dict)}"

# 3. DecisionEngine Duplicate Drop & Leakage Guard
dup_drops = [a for a in plan.actions if a.action_type == ActionType.DROP_DUPLICATE_COLUMN]
leak_actions = [a for a in plan.actions if a.action_type == ActionType.FLAG_LEAKAGE_PREDICTOR]
leak_targets = [a.target_columns[0] for a in leak_actions if a.target_columns]
dup_ok = len(dup_drops) == 1 and dup_drops[0].target_columns == ["dup_txn_uuid"]
leak_ok = "is_enterprise" in leak_targets and "post_churn_refund" in leak_targets and not any(k in leak_targets for k in ["txn_uuid", "customer_email"])
de_ok = dup_ok and leak_ok and plan.total_actions < 80
verification_checks.append({
    "check": "DecisionEngine Precision & Leakage Guard",
    "status": "PASSED" if de_ok else "FAILED",
    "details": f"actions={plan.total_actions} (<80), drop_col={dup_drops[0].target_columns if dup_drops else []}, leakage={leak_targets}",
})
assert de_ok, f"DecisionEngine check failed: total_actions={plan.total_actions}, dup_ok={dup_ok}, leak_ok={leak_ok}"

# 4. Story Studio Real Insights & Scorecard
slide2 = [s for s in story_res.get("slides", []) if s.get("category") == "Data Health"]
slide2_kpi = slide2[0].get("kpiValue", "") if slide2 else ""
story_ok = len(anomalies) >= 20 and "100.0/100" not in slide2_kpi and len(story_res.get("slides", [])) == 10
verification_checks.append({
    "check": "Story Studio Insights & Scorecard",
    "status": "PASSED" if story_ok else "FAILED",
    "details": f"anomalies={len(anomalies)}, slide2_kpi={slide2_kpi}, total_slides={len(story_res.get('slides', []))}",
})
assert story_ok, f"Story Studio check failed: anomalies={len(anomalies)}, slide2_kpi={slide2_kpi}"

# 5. Semantic Model & Measure Precision
sm_measures = [m.name for m in sem_model.measures]
sm_ok = "Total Loyalty Tier Code" not in sm_measures and "Total Order Year" not in sm_measures and "Avg Discount Rate" in sm_measures
avg_disc_val = measure_calc.get("Avg Discount Rate")
ytd_val = measure_calc.get("Net Revenue YTD")
measure_gate_ok = sm_ok and (avg_disc_val is not None) and (ytd_val is not None)
verification_checks.append({
    "check": "Semantic Model & Measure Precision",
    "status": "PASSED" if measure_gate_ok else "FAILED",
    "details": f"measures_count={len(sm_measures)}, avg_disc={avg_disc_val}, ytd={ytd_val}",
})
assert measure_gate_ok, f"Measure check failed: sm_ok={sm_ok}, avg_disc={avg_disc_val}, ytd={ytd_val}"

# 6. Auto-Dashboard Collinear Elimination
auto_kpis = [w.title for w in auto_widgets if getattr(w.chart_type, "value", str(w.chart_type)) == "kpi"]
scatter_w = [w for w in auto_widgets if getattr(w.chart_type, "value", str(w.chart_type)) == "scatter"]
scatter_x = scatter_w[0].x_axis if scatter_w else ""
auto_ok = "Total Net Revenue Eur" not in auto_kpis and scatter_x != "net_revenue_eur" and len(auto_widgets) >= 5
verification_checks.append({
    "check": "Auto-Dashboard Chart Selection",
    "status": "PASSED" if auto_ok else "FAILED",
    "details": f"widgets={len(auto_widgets)}, kpis={auto_kpis}, scatter_x={scatter_x}",
})
assert auto_ok, f"Auto-Dashboard check failed: kpis={auto_kpis}, scatter_x={scatter_x}"

# 7. DuckDB Float Quantization
net_rev_sample = query_result["rows"][0]["net_revenue"] if query_result.get("rows") else 0
quant_ok = str(net_rev_sample).split(".")[-1].__len__() <= 6
verification_checks.append({
    "check": "DuckDB Float Quantization",
    "status": "PASSED" if quant_ok else "FAILED",
    "details": f"sample net_revenue={net_rev_sample} (max 6 decimal places)",
})
assert quant_ok, f"Float quantization check failed: net_revenue={net_rev_sample}"

master_audit["verification_checks"] = verification_checks

with open('backend/tests/full_functional_audit_dump.json', 'w') as f:
    json.dump(master_audit, f, indent=2, default=str)

print(">>> ALL 14 FUNCTIONAL MODULES AUDITED AND DUMPED SUCCESSFULLY!")
