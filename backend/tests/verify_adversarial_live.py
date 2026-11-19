"""
Live Adversarial E2E Verification Script.
Evaluates PRISM's AI-Powered Dashboard upgrades against prism_adversarial_e2e_test.csv.
"""
import sys
import os
import pandas as pd

# Add backend directory to sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import asyncio
from dashboard.semantic_model import build_semantic_model
from dashboard.measure_engine import MeasureEngine
from dashboard.auto_generator import generate_initial_dashboard
from dashboard.prompt_interpreter import interpret_prompt
from dashboard.chart_config_models import ChartConfig
from dashboard.config_to_sql import config_to_sql
from dashboard.ai_analytics import (
    run_forecast,
    compute_decomposition_tree,
    compute_key_influencers,
    generate_smart_narrative,
)

csv_path = os.path.abspath('prism_adversarial_e2e_test.csv')
if not os.path.exists(csv_path):
    csv_path = os.path.abspath(os.path.join('..', 'prism_adversarial_e2e_test.csv'))

print(f"Loading {csv_path}...")
df = pd.read_csv(csv_path)
print(f"Dataset Shape: {df.shape}")
assert df.shape == (245, 27), f"Unexpected shape {df.shape}"

# 1. Semantic Model
print("\n--- 1. Semantic Model Verification ---")
sm = build_semantic_model("test_file_id", df)
print(f"Measures: {len(sm.measures)}, Categoricals: {len(sm.categorical_columns)}, Numerics: {len(sm.numeric_columns)}, Hierarchies: {len(sm.hierarchies)}")
assert len(sm.measures) >= 3, "Measures count insufficient"
measure_names = [m.name for m in sm.measures]
print(f"Detected Measures: {measure_names}")

# 2. Auto Dashboard
print("\n--- 2. Auto-Dashboard Generation ---")
schema = {c: {"dtype": str(df[c].dtype), "sample_values": df[c].dropna().head(3).tolist()} for c in df.columns}
auto_configs = asyncio.run(generate_initial_dashboard(schema, "E-commerce Retail Sales and Margin Performance"))
print(f"Auto-Dashboard Configs: {len(auto_configs)}")
assert len(auto_configs) >= 4, f"Expected at least 4 widgets, got {len(auto_configs)}"
widget_types = [c.chart_type for c in auto_configs]
print(f"Generated Widget Types: {widget_types}")

# 3. Prompt Interpretation (Multi-metric comparison)
print("\n--- 3. Prompt Interpretation & Filtering ---")
res = asyncio.run(interpret_prompt("compare gross_revenue vs net_revenue across product_category where churn_flag is 1", schema))
assert isinstance(res, ChartConfig), f"Expected ChartConfig, got {type(res)}: {res}"
assert len(res.metrics) == 2, f"Expected 2 metrics, got {len(res.metrics)}"
assert len(res.filters) == 1, "Expected filter on churn_flag"
print(f"Interpreted Metrics: {[m.column for m in res.metrics]}")
print(f"Interpreted Filter: {res.filters[0].column} {res.filters[0].operator} {res.filters[0].value}")

# 4. Config to SQL
print("\n--- 4. Config to SQL Generation ---")
sql = config_to_sql(res, "test_table")
print(f"Generated SQL: {sql}")
assert "churn_flag" in sql and "1" in sql, "Filter omitted in SQL!"
assert "gross_revenue" in sql and "net_revenue" in sql, "Metrics missing in SQL!"

# 5. Key Influencers with Data Leakage Guard
print("\n--- 5. Key Influencers with Leakage Guard ---")
ki = compute_key_influencers(df, "churn_flag", 1)
print(f"Key Influencers: {len(ki.drivers)} drivers, Guarded Columns: {ki.guarded_leakage_columns}")
print(f"Top Driver Features: {[d.feature + ' ' + d.condition for d in ki.drivers[:5]]}")
assert "post_churn_refund" in ki.guarded_leakage_columns, "Data leakage column post_churn_refund not guarded!"
assert len(ki.drivers) >= 3, "Expected at least 3 statistically significant drivers"

# 6. Forecasting
print("\n--- 6. Time Series Forecasting ---")
fc = run_forecast(df, "order_date", "net_revenue", "month", 4)
print(f"Forecast points: {len(fc.points)}, Model: {fc.model_name}, Trend: {fc.trend}, Anomalies: {fc.anomalies_detected}")
assert len(fc.points) > 0, "No forecast points produced"

# 7. Smart Narrative
print("\n--- 7. Smart Narrative Generation ---")
sn = generate_smart_narrative(df)
print(f"Narrative Title: {sn.title}")
print(f"Bullets Count: {len(sn.bullets)}")
for b in sn.bullets:
    print(f"  * {b}")
assert len(sn.bullets) >= 3, "Insufficient narrative bullets"
assert "total_net_revenue" in sn.summary_kpis, "Missing summary KPI"

print("\n=======================================================")
print("ALL LIVE ADVERSARIAL ASSERTIONS PASSED WITH 0 REGRESSIONS!")
print("=======================================================")
