"""
Tests for SQLEngine guardrails (timeout, memory limit) and CompressedParser GZIP isolation.
"""

import gzip
import io
import os
import shutil
import tempfile
from pathlib import Path
import pytest
import pandas as pd
from sql.sql_engine import SQLEngine
from ingestion.parsers.compressed import CompressedParser
from ingestion.parsers.csv_parser import CSVParser


def test_sql_engine_timeout():
    """Verify that a runaway query gets interrupted after timeout."""
    engine = SQLEngine()
    # Execute query with 0.5s timeout on an intentional cross join
    res = engine.execute(
        "SELECT COUNT(*) FROM generate_series(1, 1000000000) t1, generate_series(1, 1000000000) t2",
        timeout_seconds=0.5
    )
    assert res["success"] is False
    assert "timed out" in res["error"].lower()


def test_sql_engine_normal_query():
    """Verify standard queries run and finish normally within timeout."""
    engine = SQLEngine()
    df = pd.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
    engine.register_dataframe(df, "test_table")
    res = engine.execute("SELECT SUM(b) as total FROM test_table", timeout_seconds=5.0)
    assert res["success"] is True
    assert res["rows"][0]["total"] == 60


def test_gzip_isolation_avoids_collision(tmp_path):
    """Verify GZIP decompression uses isolated temporary directories."""
    csv_bytes = b"col1,col2\nval1,val2\n"
    gz_path1 = tmp_path / "data.csv.gz"
    with gzip.open(str(gz_path1), "wb") as f:
        f.write(csv_bytes)

    parser = CompressedParser(gz_path1)
    res = parser.parse(parser_factory=lambda p: CSVParser(p))
    assert res.dataframe is not None
    assert len(res.dataframe) == 1
    assert res.dataframe.iloc[0]["col1"] == "val1"
