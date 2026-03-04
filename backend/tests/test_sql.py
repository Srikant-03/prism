"""
SQL Engine & Query Builder Tests.
Tests DuckDB table registration, schema introspection, query execution,
JSON → SQL generation, and injection prevention.
"""

import pandas as pd
import numpy as np


# ── SQL Engine tests ──────────────────────────────────────────────────

def test_engine_register_and_list():
    from sql.sql_engine import SQLEngine
    engine = SQLEngine()
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    name = engine.register_dataframe(df, "test_data.csv", "raw", "f1")

    tables = engine.list_tables()
    assert len(tables) == 1
    assert tables[0]["name"] == name
    assert tables[0]["n_rows"] == 3
    assert tables[0]["n_cols"] == 2
    print(f"  ✓ engine_register_and_list passed (name={name})")


def test_engine_columns():
    from sql.sql_engine import SQLEngine
    engine = SQLEngine()
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [9.5, 8.0, 7.3],
        "active": [True, False, True],
    })
    engine.register_dataframe(df, "users", "raw")
    columns = engine.get_columns("users")

    assert len(columns) == 4
    types = {c["name"]: c["ui_type"] for c in columns}
    assert types["id"] == "integer"
    assert types["score"] == "float"
    assert types["active"] == "boolean"
    print(f"  ✓ engine_columns passed ({types})")


def test_engine_execute_select():
    from sql.sql_engine import SQLEngine
    engine = SQLEngine()
    df = pd.DataFrame({"x": [10, 20, 30], "y": [1, 2, 3]})
    engine.register_dataframe(df, "nums", "raw")

    result = engine.execute('SELECT x, y, x + y AS total FROM "nums" WHERE x > 10')
    assert result["success"] is True
    assert result["row_count"] == 2
    assert result["rows"][0]["total"] == 22
    print(f"  ✓ engine_execute_select passed ({result['row_count']} rows, {result['execution_time_s']}s)")


def test_engine_block_writes():
    from sql.sql_engine import SQLEngine
    engine = SQLEngine()
    df = pd.DataFrame({"a": [1]})
    engine.register_dataframe(df, "temp", "raw")

    result = engine.execute('DROP TABLE "temp"')
    assert result["success"] is False
    assert "not allowed" in result["error"].lower()
    print(f"  ✓ engine_block_writes passed (blocked: {result['error']})")


def test_engine_column_values():
    from sql.sql_engine import SQLEngine
    engine = SQLEngine()
    df = pd.DataFrame({"color": ["red", "blue", "red", "green", "blue", "red"]})
    engine.register_dataframe(df, "colors", "raw")

    values = engine.get_column_values("colors", "color")
    assert set(values) == {"red", "blue", "green"}
    print(f"  ✓ engine_column_values passed ({values})")


def test_engine_collision_free_names():
    from sql.sql_engine import SQLEngine
    engine = SQLEngine()
    df1 = pd.DataFrame({"a": [1]})
    df2 = pd.DataFrame({"b": [2]})

    n1 = engine.register_dataframe(df1, "data.csv", "raw")
    n2 = engine.register_dataframe(df2, "data.csv", "cleaned")

    assert n1 != n2
    tables = engine.list_tables()
    assert len(tables) == 2
    print(f"  ✓ engine_collision_free_names passed ({n1}, {n2})")


# ── Query Builder tests ──────────────────────────────────────────────

def test_builder_simple_select():
    from sql.query_builder import QueryBuilder
    spec = {
        "from": {"table": "users"},
        "columns": [{"column": "name"}, {"column": "age"}],
        "limit": 10,
    }
    sql = QueryBuilder.build(spec)
    assert '"users"' in sql
    assert '"name"' in sql
    assert "LIMIT 10" in sql
    print(f"  ✓ builder_simple_select passed: {sql.replace(chr(10), ' | ')}")


def test_builder_where_clause():
    from sql.query_builder import QueryBuilder
    spec = {
        "from": {"table": "orders"},
        "columns": [],
        "where": [
            {"column": "status", "op": "=", "value": "active"},
            {"column": "amount", "op": ">", "value": 100, "logic": "AND"},
        ],
    }
    sql = QueryBuilder.build(spec)
    assert "WHERE" in sql
    assert "'active'" in sql
    assert "> 100" in sql
    print(f"  ✓ builder_where_clause passed")


def test_builder_aggregation():
    from sql.query_builder import QueryBuilder
    spec = {
        "from": {"table": "sales"},
        "columns": [
            {"column": "region"},
            {"aggregate": "SUM", "column": "revenue", "alias": "total_rev"},
            {"aggregate": "COUNT", "column": "*"},
        ],
        "group_by": ["region"],
        "having": [{"column": "total_rev", "op": ">", "value": 1000}],
    }
    sql = QueryBuilder.build(spec)
    assert "GROUP BY" in sql
    assert "SUM" in sql
    assert "HAVING" in sql
    print(f"  ✓ builder_aggregation passed")


def test_builder_join():
    from sql.query_builder import QueryBuilder
    spec = {
        "from": {"table": "orders", "alias": "o"},
        "columns": [{"column": "order_id", "table": "o"}],
        "joins": [
            {
                "type": "LEFT",
                "table": "customers",
                "alias": "c",
                "on": [{"left": "o.customer_id", "right": "c.id"}],
            },
        ],
    }
    sql = QueryBuilder.build(spec)
    assert "LEFT JOIN" in sql
    assert '"customers"' in sql
    print(f"  ✓ builder_join passed")


def test_builder_window_function():
    from sql.query_builder import QueryBuilder
    spec = {
        "from": {"table": "sales"},
        "columns": [
            {"column": "date"},
            {"column": "revenue"},
            {
                "window": {
                    "func": "ROW_NUMBER",
                    "partition_by": ["region"],
                    "order_by": [{"column": "date", "direction": "DESC"}],
                },
                "alias": "rn",
            },
        ],
    }
    sql = QueryBuilder.build(spec)
    assert "ROW_NUMBER()" in sql
    assert "PARTITION BY" in sql
    assert "ORDER BY" in sql
    print(f"  ✓ builder_window_function passed")


def test_builder_operators():
    from sql.query_builder import QueryBuilder
    # BETWEEN
    spec = {
        "from": {"table": "t"},
        "where": [{"column": "age", "op": "BETWEEN", "values": [18, 65]}],
    }
    sql = QueryBuilder.build(spec)
    assert "BETWEEN 18 AND 65" in sql

    # CONTAINS
    spec["where"] = [{"column": "name", "op": "CONTAINS", "value": "john"}]
    sql = QueryBuilder.build(spec)
    assert "LIKE '%john%'" in sql

    # IS NULL
    spec["where"] = [{"column": "email", "op": "IS NULL"}]
    sql = QueryBuilder.build(spec)
    assert "IS NULL" in sql

    print(f"  ✓ builder_operators passed (BETWEEN, CONTAINS, IS NULL)")


def test_builder_cte():
    from sql.query_builder import QueryBuilder
    spec = {
        "ctes": [
            {
                "name": "top_users",
                "query": {
                    "from": {"table": "users"},
                    "columns": [{"column": "id"}, {"column": "score"}],
                    "order_by": [{"column": "score", "direction": "DESC"}],
                    "limit": 10,
                },
            },
        ],
        "from": {"table": "top_users"},
        "columns": [],
    }
    sql = QueryBuilder.build(spec)
    assert "WITH" in sql
    assert '"top_users"' in sql
    print(f"  ✓ builder_cte passed")


# ── Integration test ──────────────────────────────────────────────────

def test_engine_with_builder():
    from sql.sql_engine import SQLEngine
    from sql.query_builder import QueryBuilder

    engine = SQLEngine()
    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "Alice", "Bob"],
        "dept": ["Eng", "Sales", "Eng", "Eng", "Sales"],
        "salary": [100, 80, 120, 90, 85],
    })
    engine.register_dataframe(df, "employees", "raw")

    spec = {
        "from": {"table": "employees"},
        "columns": [
            {"column": "dept"},
            {"aggregate": "AVG", "column": "salary", "alias": "avg_salary"},
            {"aggregate": "COUNT", "column": "*", "alias": "cnt"},
        ],
        "group_by": ["dept"],
        "order_by": [{"column": "avg_salary", "direction": "DESC"}],
    }
    sql = QueryBuilder.build(spec)
    result = engine.execute(sql)

    assert result["success"] is True
    assert result["row_count"] == 2
    assert result["rows"][0]["dept"] in ("Eng", "Sales")
    print(f"  ✓ engine_with_builder passed ({result['row_count']} rows)")


# ── Run all tests ────────────────────────────────────────────────────

if __name__ == "__main__":
    import traceback
    tests = [
        test_engine_register_and_list,
        test_engine_columns,
        test_engine_execute_select,
        test_engine_block_writes,
        test_engine_column_values,
        test_engine_collision_free_names,
        test_builder_simple_select,
        test_builder_where_clause,
        test_builder_aggregation,
        test_builder_join,
        test_builder_window_function,
        test_builder_operators,
        test_builder_cte,
        test_engine_with_builder,
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
    else:
        print("SOME TESTS FAILED ✗")
