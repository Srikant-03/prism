"""
Generate a comprehensive test CSV for the AI Dashboard project.
Covers: bar, line, area, pie, donut, scatter, heatmap, treemap, funnel, KPI, table charts.
Tests: SUM/AVG/COUNT/MIN/MAX/MEDIAN aggs, filters (>, <, =, in, between, is_null),
       sort+limit (top/bottom N), group_by, secondary y-axis, stacked, trend line.
"""
import csv
import random
import math

random.seed(42)

# ── Schema ──────────────────────────────────────────────────────────────────
COLUMNS = [
    "date", "month_name", "quarter", "year",
    "category", "subcategory",
    "region", "country", "channel", "salesperson",
    "revenue", "cost", "profit",
    "units_sold", "avg_order_value",
    "customer_satisfaction", "response_time_days",
    "discount_pct",        # nullable — for IS NULL / IS NOT NULL filter tests
    "return_rate",
    "funnel_stage",        # for funnel charts
    "prospect_count",      # leads at each funnel stage
    "weekday",             # for heatmap (weekday × region)
    "hour_bucket",         # for heatmap (hour × channel)
]

# ── Lookup tables ─────────────────────────────────────────────────────────────
CATEGORIES = {
    "Electronics":    {"subs": ["Smartphones", "Laptops", "Tablets", "Accessories", "Wearables"],
                       "revenue_base": 40000, "revenue_spread": 35000,
                       "cost_pct": 0.62, "units_base": 12, "units_spread": 14,
                       "price_per_unit": 2800},
    "Clothing":       {"subs": ["Casual Wear", "Formal Wear", "Sportswear", "Outdoor", "Kids Wear"],
                       "revenue_base": 5000,  "revenue_spread": 6000,
                       "cost_pct": 0.50, "units_base": 150, "units_spread": 180,
                       "price_per_unit": 32},
    "Food":           {"subs": ["Beverages", "Snacks", "Fresh Produce", "Dairy", "Frozen Foods"],
                       "revenue_base": 2000,  "revenue_spread": 2500,
                       "cost_pct": 0.40, "units_base": 400, "units_spread": 500,
                       "price_per_unit": 5},
    "Sports":         {"subs": ["Fitness Equipment", "Outdoor Gear", "Team Sports", "Water Sports", "Cycling"],
                       "revenue_base": 7000,  "revenue_spread": 9000,
                       "cost_pct": 0.60, "units_base": 70,  "units_spread": 80,
                       "price_per_unit": 110},
    "Home & Garden":  {"subs": ["Furniture", "Decor", "Garden Tools", "Lighting", "Bedding"],
                       "revenue_base": 8000,  "revenue_spread": 8000,
                       "cost_pct": 0.50, "units_base": 40,  "units_spread": 45,
                       "price_per_unit": 220},
}

REGIONS  = ["North", "South", "East", "West"]
COUNTRIES = {"North": "USA", "South": "Mexico", "East": "Canada", "West": "UK"}
CHANNELS = ["Online", "Offline", "Partner"]
SALESPEOPLE = ["Alice", "Bob", "Carol", "Dave", "Eve"]
FUNNEL_STAGES = ["Awareness", "Interest", "Consideration", "Intent", "Purchase"]
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
HOUR_BUCKETS = ["00-06", "06-09", "09-12", "12-15", "15-18", "18-21", "21-24"]

MONTHS = [
    ("January", "Q1", 1), ("February", "Q1", 2), ("March", "Q1", 3),
    ("April",   "Q2", 4), ("May",      "Q2", 5), ("June",   "Q2", 6),
    ("July",    "Q3", 7), ("August",   "Q3", 8), ("September", "Q3", 9),
    ("October", "Q4", 10), ("November", "Q4", 11), ("December", "Q4", 12),
]

# Seasonal multiplier: amplify certain months (Nov-Dec holiday spike, summer peak)
SEASON_MULT = {1:0.85, 2:0.88, 3:0.92, 4:0.95, 5:1.00, 6:1.02,
               7:1.05, 8:1.08, 9:1.00, 10:1.05, 11:1.30, 12:1.45}

rows = []

# ══════════════════════════════════════════════════════════════════════════════
# BLOCK 1 — Main sales data (2023 + 2024 H1)
# 5 categories × 4 regions × 18 months (12 months 2023 + 6 months Jan-Jun 2024)
# = 360 rows but we'll do 5 × 18 = 90 rows (one row per category per month,
#   rotating region/channel/salesperson)
# ══════════════════════════════════════════════════════════════════════════════
row_idx = 0
for year in [2023, 2024]:
    months_for_year = MONTHS if year == 2023 else MONTHS[:6]
    for month_name, quarter, month_num in months_for_year:
        for cat_idx, (cat, cinfo) in enumerate(CATEGORIES.items()):
            # Rotate supporting dimensions
            region     = REGIONS[row_idx % 4]
            country    = COUNTRIES[region]
            channel    = CHANNELS[row_idx % 3]
            salesperson = SALESPEOPLE[(cat_idx + row_idx) % 5]
            sub        = cinfo["subs"][row_idx % len(cinfo["subs"])]
            funnel     = FUNNEL_STAGES[row_idx % 5]
            weekday    = WEEKDAYS[row_idx % 7]
            hour_bkt   = HOUR_BUCKETS[row_idx % 7]

            # Revenue with seasonality + year growth + noise
            season = SEASON_MULT[month_num]
            growth = 1.0 if year == 2023 else 1.18    # 18% YoY growth
            noise  = 1 + (random.random() - 0.5) * 0.25
            revenue = round(cinfo["revenue_base"] * season * growth * noise, 2)
            cost    = round(revenue * cinfo["cost_pct"] * (1 + (random.random() - 0.5) * 0.1), 2)
            profit  = round(revenue - cost, 2)

            units   = max(1, int(cinfo["units_base"] * season * growth * noise))
            aov     = round(revenue / units, 2)

            sat     = round(random.uniform(3.0, 5.0), 1)
            rtime   = round(random.uniform(0.5, 7.0), 1)

            # discount_pct: NULL every ~7th row to test IS NULL filters
            if row_idx % 7 == 0:
                discount = ""        # NULL
            else:
                discount = round(random.uniform(0, 25), 1)

            return_rate  = round(random.uniform(1.0, 12.0), 1)

            # Prospect count: higher for early funnel stages
            stage_mult = {"Awareness": 12, "Interest": 7, "Consideration": 4, "Intent": 2, "Purchase": 1}
            prospect = int(units * stage_mult[funnel] * random.uniform(0.8, 1.2))

            day = 10 + (row_idx % 20)   # days 10-29
            date_str = f"{year}-{month_num:02d}-{day:02d}"

            rows.append({
                "date": date_str,
                "month_name": month_name,
                "quarter": quarter,
                "year": year,
                "category": cat,
                "subcategory": sub,
                "region": region,
                "country": country,
                "channel": channel,
                "salesperson": salesperson,
                "revenue": revenue,
                "cost": cost,
                "profit": profit,
                "units_sold": units,
                "avg_order_value": aov,
                "customer_satisfaction": sat,
                "response_time_days": rtime,
                "discount_pct": discount,
                "return_rate": return_rate,
                "funnel_stage": funnel,
                "prospect_count": prospect,
                "weekday": weekday,
                "hour_bucket": hour_bkt,
            })
            row_idx += 1

# ══════════════════════════════════════════════════════════════════════════════
# BLOCK 2 — Explicit funnel-stage rows for clean funnel chart testing
# One row per stage (5 stages), aggregated across all categories.
# These represent a full pipeline snapshot at a point in time.
# ══════════════════════════════════════════════════════════════════════════════
funnel_template = [
    # (stage, avg_revenue, avg_cost, avg_units, avg_sat, prospect_count)
    ("Awareness",     500,   300,   5,  3.5, 12500),
    ("Interest",     1800,  1000,  15,  3.8,  7200),
    ("Consideration", 4200, 2400,  30,  4.0,  4100),
    ("Intent",       8500,  4800,  55,  4.2,  1800),
    ("Purchase",    18000, 10000, 110,  4.6,   750),
]
for f_idx, (stage, rev, cost, units, sat, prospects) in enumerate(funnel_template):
    rows.append({
        "date": "2024-06-30",
        "month_name": "June",
        "quarter": "Q2",
        "year": 2024,
        "category": "Electronics",
        "subcategory": "Smartphones",
        "region": REGIONS[f_idx % 4],
        "country": COUNTRIES[REGIONS[f_idx % 4]],
        "channel": "Online",
        "salesperson": SALESPEOPLE[f_idx % 5],
        "revenue": rev,
        "cost": cost,
        "profit": rev - cost,
        "units_sold": units,
        "avg_order_value": round(rev / units, 2),
        "customer_satisfaction": sat,
        "response_time_days": round(8 - f_idx * 1.2, 1),
        "discount_pct": "",   # NULL — tests IS NULL
        "return_rate": round(12 - f_idx * 1.8, 1),
        "funnel_stage": stage,
        "prospect_count": prospects,
        "weekday": "Monday",
        "hour_bucket": "09-12",
    })

# ══════════════════════════════════════════════════════════════════════════════
# BLOCK 3 — Scatter test rows: avg_order_value vs customer_satisfaction
# Two clearly numeric columns, no obvious category dimension.
# ══════════════════════════════════════════════════════════════════════════════
for s_idx in range(20):
    sat  = round(1.0 + (s_idx / 19.0) * 4.0, 1)   # 1.0 → 5.0
    aov  = round(50 + sat * 400 + random.uniform(-80, 80), 2)
    rev  = round(aov * random.randint(5, 30), 2)
    cost = round(rev * 0.55, 2)
    rows.append({
        "date": f"2024-03-{(s_idx % 28) + 1:02d}",
        "month_name": "March",
        "quarter": "Q1",
        "year": 2024,
        "category": CATEGORIES_LIST[s_idx % 5] if False else list(CATEGORIES.keys())[s_idx % 5],
        "subcategory": "Scatter Test",
        "region": REGIONS[s_idx % 4],
        "country": COUNTRIES[REGIONS[s_idx % 4]],
        "channel": CHANNELS[s_idx % 3],
        "salesperson": SALESPEOPLE[s_idx % 5],
        "revenue": rev,
        "cost": cost,
        "profit": round(rev - cost, 2),
        "units_sold": random.randint(1, 50),
        "avg_order_value": aov,
        "customer_satisfaction": sat,
        "response_time_days": round(6.0 - sat * 0.8 + random.uniform(-0.5, 0.5), 1),
        "discount_pct": round(random.uniform(0, 30), 1) if s_idx % 4 != 0 else "",
        "return_rate": round(max(0.1, 15 - sat * 2.5 + random.uniform(-1, 1)), 1),
        "funnel_stage": FUNNEL_STAGES[s_idx % 5],
        "prospect_count": random.randint(10, 500),
        "weekday": WEEKDAYS[s_idx % 7],
        "hour_bucket": HOUR_BUCKETS[s_idx % 7],
    })

# ══════════════════════════════════════════════════════════════════════════════
# BLOCK 4 — Heatmap rows: every (weekday × region) combination
# Revenue at each intersection → region × weekday heatmap
# ══════════════════════════════════════════════════════════════════════════════
for wd in WEEKDAYS:
    for rgn in REGIONS:
        rev  = round(random.uniform(2000, 15000), 2)
        cost = round(rev * 0.55, 2)
        rows.append({
            "date": "2024-01-15",
            "month_name": "January",
            "quarter": "Q1",
            "year": 2024,
            "category": "Electronics",
            "subcategory": "Heatmap Test",
            "region": rgn,
            "country": COUNTRIES[rgn],
            "channel": "Online",
            "salesperson": "Alice",
            "revenue": rev,
            "cost": cost,
            "profit": round(rev - cost, 2),
            "units_sold": random.randint(5, 80),
            "avg_order_value": round(rev / random.randint(5, 80), 2),
            "customer_satisfaction": round(random.uniform(3.0, 5.0), 1),
            "response_time_days": round(random.uniform(1, 5), 1),
            "discount_pct": round(random.uniform(0, 20), 1) if random.random() > 0.2 else "",
            "return_rate": round(random.uniform(2, 10), 1),
            "funnel_stage": "Purchase",
            "prospect_count": random.randint(20, 200),
            "weekday": wd,
            "hour_bucket": HOUR_BUCKETS[WEEKDAYS.index(wd) % 7],
        })

# ── Write CSV ──────────────────────────────────────────────────────────────
output_path = r"c:\abbu\dashboard_test_data.csv"
with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=COLUMNS)
    writer.writeheader()
    writer.writerows(rows)

print(f"Written {len(rows)} rows to {output_path}")
print(f"Columns ({len(COLUMNS)}): {', '.join(COLUMNS)}")
print("\nRow breakdown:")
print(f"  Block 1 — Main sales data (2023 full + 2024 H1):  {90} rows")
print(f"  Block 2 — Funnel pipeline snapshots:               {5} rows")
print(f"  Block 3 — Scatter test (sat vs order value):       {20} rows")
print(f"  Block 4 — Heatmap (weekday x region):              {7*4} rows")
print(f"  Total:                                              {len(rows)} rows")
