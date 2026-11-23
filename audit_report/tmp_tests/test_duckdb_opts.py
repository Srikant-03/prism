import duckdb

try:
    c = duckdb.connect(database=":memory:", config={"enable_external_access": "false", "max_memory": "2GB"})
    print("max_memory 2GB in config: OK")
except Exception as e:
    print("max_memory in config error:", e)

try:
    c.execute("SET statement_timeout = '30s'")
    print("SET statement_timeout = '30s': OK")
except Exception as e:
    print("SET statement_timeout error:", e)

try:
    c.execute("SET preserve_insertion_order = false")
    print("preserve_insertion_order: OK")
except Exception as e:
    print("preserve_insertion_order error:", e)
