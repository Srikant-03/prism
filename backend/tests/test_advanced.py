"""
Tests for advanced SQLEngine capabilities: explain, views, and caching.
"""
import pandas as pd
import numpy as np
import pytest
from sql.sql_engine import SQLEngine

def test_sql_engine_advanced_features():
    engine = SQLEngine()
    df = pd.DataFrame({"id": range(20), "name": ["a"]*20, "val": [1.0]*20})
    engine.register_dataframe(df, "test", "raw")

    # Test explain
    r = engine.explain_query("SELECT * FROM test LIMIT 5")
    assert r["success"] is True, f"explain failed: {r}"
    assert len(r["nodes"]) > 0

    # Test views
    r = engine.create_view("top5", "SELECT * FROM test LIMIT 5")
    assert r["success"] is True, f"create_view failed: {r}"
    assert r.get("view_name") == "top5"
    views = engine.list_views()
    assert "top5" in views
    r2 = engine.execute("SELECT * FROM top5")
    assert r2["row_count"] == 5

    # Test cache
    r3 = engine.execute_cached("SELECT COUNT(*) FROM test")
    assert r3["row_count"] == 1
    assert r3.get("from_cache", False) is False
    r4 = engine.execute_cached("SELECT COUNT(*) FROM test")
    assert r4["row_count"] == 1
    assert r4.get("from_cache", False) is True
    n = engine.clear_cache()
    assert n >= 1

    # Test drop view
    engine.drop_view("top5")
    views2 = engine.list_views()
    assert "top5" not in views2
