"""
Phase 1 Verification Gate — Ingestion, Malformed Rows & Null Preservation
"""
import sys
from pathlib import Path
import pytest
import pandas as pd
import numpy as np

# Ensure backend is on sys.path
backend_dir = Path(__file__).resolve().parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from ingestion.parsers.csv_parser import CSVParser
from cleaning.missing_handler import MissingHandler
from cleaning.cleaning_models import ActionType


def test_phase1_verification_gate():
    csv_path = Path("prism_adversarial_e2e_test.csv")
    assert csv_path.exists(), "prism_adversarial_e2e_test.csv must exist in workspace root"

    parser = CSVParser(csv_path)
    result = parser.parse()

    # 1. Malformed rows count check
    assert result.malformed.total_issues == 0, f"Expected 0 malformed rows, got {result.malformed.total_issues}"

    # 2. DataFrame shape
    df = result.dataframe
    assert df.shape == (245, 27), f"Expected shape (245, 27), got {df.shape}"

    # 3. Total NaN count
    total_nans = int(df.isna().sum().sum())
    assert total_nans == 127, f"Expected 127 NaNs, got {total_nans}"

    # 4. Completeness and missingness percentages
    total_cells = 245 * 27
    missingness_pct = total_nans / total_cells * 100.0
    completeness_pct = (total_cells - total_nans) / total_cells * 100.0
    assert abs(missingness_pct - 1.92) < 0.05, f"Expected ~1.92% missingness, got {missingness_pct:.2f}%"
    assert abs(completeness_pct - 98.08) < 0.05, f"Expected ~98.08% completeness, got {completeness_pct:.2f}%"

    # 5. Specific column null counts
    assert df["region"].isna().sum() == 21, f"Expected 21 nulls in region, got {df['region'].isna().sum()}"
    assert df["customer_notes"].isna().sum() == 29, f"Expected 29 nulls in customer_notes, got {df['customer_notes'].isna().sum()}"

    # 6. constant_status unique non-null values
    non_null_constant = df["constant_status"].dropna().unique().tolist()
    assert non_null_constant == ["ACTIVE"], f"Expected ['ACTIVE'], got {non_null_constant}"

    # 7. Row 200 null density
    row_200 = df.iloc[200]
    row_200_nulls = int(row_200.isna().sum())
    assert row_200_nulls == 20, f"Expected 20 nulls in Row 200, got {row_200_nulls}"
    assert abs((row_200_nulls / 27) - 0.741) < 0.01, f"Expected 74.1% nulls in Row 200, got {row_200_nulls / 27:.1%}"

    # 8. MissingHandler high-null row detection & action consolidation
    handler = MissingHandler(df, "adversarial-test")
    actions, report = handler.analyze()

    assert report.high_null_rows == 1, f"Expected 1 high null row, got {report.high_null_rows}"
    
    # Assert single-row DROP_ROWS are consolidated into the row action
    drop_row_actions = [a for a in actions if a.action_type == ActionType.DROP_ROWS]
    assert len(drop_row_actions) == 1, f"Expected exactly 1 consolidated DROP_ROWS action, got {len(drop_row_actions)}"
    assert "consolidated_columns" in drop_row_actions[0].metadata
    assert len(drop_row_actions[0].metadata["consolidated_columns"]) >= 10
