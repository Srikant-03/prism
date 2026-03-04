import json
import pandas as pd
import numpy as np
from profiling.engine import DataProfiler

def run_test():
    # 1. Create a quick mock dataset
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

    # 2. Run Engine
    result = DataProfiler.profile(df, file_id="test_geo_target_corr")

    # 3. Print Results
    print("Dataset Profile completed successfully.")
    print("Warnings:", result.warnings)
    print("Target Detected:", result.profile.cross_analysis['target']['target_column'])
    print("Has Geo:", result.profile.cross_analysis['geo']['has_geo_patterns'])
    print("Strongest Correlated Pairs:", [p['col1'] + "-" + p['col2'] + ":" + str(round(p['score'], 2)) for p in result.profile.cross_analysis['correlations']['strongest_pairs'][:3]])
    print("Has Temporal:", result.profile.cross_analysis['temporal']['has_temporal_patterns'])

if __name__ == "__main__":
    run_test()
