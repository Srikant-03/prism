"""
Tests for cross-column profiling capabilities.
"""
import pytest
import pandas as pd
import numpy as np
from profiling.engine import DataProfiler

def test_cross_column_profiling():
    np.random.seed(42)
    data = {
        'id': range(100),
        'age': np.random.normal(40, 10, 100),
        'income': np.random.normal(60000, 15000, 100),
        'is_fraud': np.zeros(100, dtype=int),
        'device_type': np.random.choice(['mobile', 'desktop', 'tablet'], size=100),
        'country': np.random.choice(['US', 'CA', 'UK'], size=100),
        'timestamp': pd.date_range(start='2023-01-01', periods=100, freq='D')
    }
    
    # Induce a correlation
    data['transaction_amount'] = data['income'] * 0.05 + np.random.normal(0, 500, 100)
    fraud_idx = data['transaction_amount'] > 4000
    data['is_fraud'][fraud_idx] = 1

    df = pd.DataFrame(data)

    # Run Profiler
    result = DataProfiler.profile(df, file_id="test_geo_target_corr")

    assert result is not None
    assert result.profile is not None
    assert result.profile.cross_analysis is not None
    assert "target" in result.profile.cross_analysis
    assert "geo" in result.profile.cross_analysis
    assert "correlations" in result.profile.cross_analysis
    assert "temporal" in result.profile.cross_analysis
