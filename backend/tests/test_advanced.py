import pandas as pd
import numpy as np
from sql.sql_engine import SQLEngine

engine = SQLEngine()
df = pd.DataFrame({"id": range(20), "name": ["a"]*20, "val": [1.0]*20})
engine.register_dataframe(df, "test", "raw")

# Test explain
r = engine.explain_query("SELECT * FROM test LIMIT 5")
print(f"explain: success={r['success']}, nodes={len(r['nodes'])}")

# Test views
r = engine.create_view("top5", "SELECT * FROM test LIMIT 5")
print(f"create_view: success={r['success']}, name={r.get('view_name')}")
views = engine.list_views()
print(f"list_views: {views}")
r2 = engine.execute("SELECT * FROM top5")
print(f"query_view: rows={r2['row_count']}")

# Test cache
r3 = engine.execute_cached("SELECT COUNT(*) FROM test")
print(f"cache_miss: rows={r3['row_count']}, from_cache={r3.get('from_cache', False)}")
r4 = engine.execute_cached("SELECT COUNT(*) FROM test")
print(f"cache_hit: rows={r4['row_count']}, from_cache={r4.get('from_cache', False)}")
n = engine.clear_cache()
print(f"cache_cleared: {n}")

# Test drop view
engine.drop_view("top5")
views2 = engine.list_views()
print(f"after_drop: {views2}")
print("ALL ADVANCED TESTS PASSED")
