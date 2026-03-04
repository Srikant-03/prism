"""
Tests for Template Generator and NL Query schema context.
"""

import pandas as pd
import numpy as np


def test_template_generator_data_audit():
    """Templates are generated for any table."""
    from sql.sql_engine import SQLEngine
    from sql.template_generator import TemplateGenerator

    engine = SQLEngine()
    df = pd.DataFrame({
        "id": range(100),
        "name": [f"item_{i}" for i in range(100)],
        "price": np.random.uniform(5, 500, 100),
        "category": np.random.choice(["A", "B", "C", "D"], 100),
        "created_at": pd.date_range("2024-01-01", periods=100, freq="D"),
        "city": np.random.choice(["NYC", "LA", "SF", "CHI"], 100),
    })
    engine.register_dataframe(df, "products", "raw")

    gen = TemplateGenerator(engine)
    templates = gen.generate_templates("products")

    categories = set(t["category"] for t in templates)
    assert "Data Audit" in categories
    assert "Top-N Analysis" in categories
    assert "Distribution Analysis" in categories
    assert "Completeness" in categories
    assert "Trend Analysis" in categories  # has date col
    assert "Segmentation" in categories   # has geo col

    # Check each template has required fields
    for t in templates:
        assert "title" in t
        assert "description" in t
        assert "sql" in t
        assert "params" in t

    print(f"  ✓ template_generator_data_audit: {len(templates)} templates, {len(categories)} categories")


def test_template_generator_funnel():
    """Funnel templates generated when user+event columns exist."""
    from sql.sql_engine import SQLEngine
    from sql.template_generator import TemplateGenerator

    engine = SQLEngine()
    df = pd.DataFrame({
        "user_id": [1, 2, 3, 1, 2],
        "event_type": ["view", "cart", "view", "checkout", "view"],
        "timestamp": pd.date_range("2024-01-01", periods=5, freq="h"),
    })
    engine.register_dataframe(df, "events", "raw")

    gen = TemplateGenerator(engine)
    templates = gen.generate_templates("events")
    categories = set(t["category"] for t in templates)

    assert "Funnel & Conversion" in categories
    assert "Cohort Analysis" in categories
    print(f"  ✓ template_generator_funnel: {len(templates)} templates, funnel+cohort detected")


def test_template_generator_outlier():
    """Outlier templates generated when amount columns exist."""
    from sql.sql_engine import SQLEngine
    from sql.template_generator import TemplateGenerator

    engine = SQLEngine()
    df = pd.DataFrame({
        "order_id": range(50),
        "amount": np.random.normal(100, 20, 50),
        "revenue": np.random.normal(500, 50, 50),
    })
    engine.register_dataframe(df, "orders", "raw")

    gen = TemplateGenerator(engine)
    templates = gen.generate_templates("orders")
    categories = set(t["category"] for t in templates)

    assert "Outlier Detection" in categories
    print(f"  ✓ template_generator_outlier: outlier detection found")


def test_template_generator_cross_table():
    """Cross-table relationships detected when common columns exist."""
    from sql.sql_engine import SQLEngine
    from sql.template_generator import TemplateGenerator

    engine = SQLEngine()
    df1 = pd.DataFrame({"product_id": [1, 2, 3], "name": ["A", "B", "C"]})
    df2 = pd.DataFrame({"order_id": [1, 2], "product_id": [1, 2], "qty": [5, 3]})
    engine.register_dataframe(df1, "products", "raw")
    engine.register_dataframe(df2, "orders", "raw")

    gen = TemplateGenerator(engine)
    templates = gen.generate_templates("products")
    categories = set(t["category"] for t in templates)

    assert "Relationships" in categories
    print(f"  ✓ template_generator_cross_table: relationship templates generated")


def test_template_sql_executable():
    """Generated template SQL should be executable."""
    from sql.sql_engine import SQLEngine
    from sql.template_generator import TemplateGenerator

    engine = SQLEngine()
    df = pd.DataFrame({
        "id": range(20),
        "name": [f"x{i}" for i in range(20)],
        "score": np.random.uniform(0, 100, 20),
        "group": np.random.choice(["A", "B"], 20),
    })
    engine.register_dataframe(df, "scores", "raw")

    gen = TemplateGenerator(engine)
    templates = gen.generate_templates("scores")

    executed = 0
    for t in templates:
        sql = t["sql"]
        for p in t["params"]:
            placeholder = "{{" + p["name"] + "}}"
            sql = sql.replace(placeholder, str(p["default"] if p["default"] is not None else 10))
        result = engine.execute(sql)
        assert result["success"], f"Template '{t['title']}' failed: {result.get('error')}"
        executed += 1

    print(f"  ✓ template_sql_executable: {executed}/{len(templates)} templates executed successfully")


def test_nl_schema_context():
    """Schema context builder should include all table/column info."""
    from sql.sql_engine import SQLEngine
    from sql.nl_query import build_schema_context

    engine = SQLEngine()
    df = pd.DataFrame({
        "user_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "balance": [100.0, 200.0, 300.0],
    })
    engine.register_dataframe(df, "users", "raw")

    ctx = build_schema_context(engine)
    assert "users" in ctx
    assert "user_id" in ctx
    assert "balance" in ctx
    assert "float" in ctx or "integer" in ctx
    assert "rows=3" in ctx
    print(f"  ✓ nl_schema_context: context built ({len(ctx)} chars)")


# ── Run ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import traceback
    tests = [
        test_template_generator_data_audit,
        test_template_generator_funnel,
        test_template_generator_outlier,
        test_template_generator_cross_table,
        test_template_sql_executable,
        test_nl_schema_context,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  ✗ {test.__name__} FAILED: {e}")
            traceback.print_exc()
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed, {len(tests)} total")
    if failed == 0:
        print("ALL TESTS PASSED ✓")
