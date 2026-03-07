import pandas as pd
import numpy as np
import json
import asyncio
from cleaning.decision_engine import DecisionEngine
from profiling.engine import DataProfiler
def make_tough_dataset():
    np.random.seed(42)
    n = 1000
    df = pd.DataFrame({
        "id": range(n),
        "target": np.random.choice([0, 1], n),
        
        # Datetimes (will cause pairwise and extraction explosions)
        "created_at": pd.date_range("2023-01-01", periods=n, freq="D"),
        "updated_at": pd.date_range("2023-01-15", periods=n, freq="1.5D"),
        "deleted_at": pd.date_range("2024-01-01", periods=n, freq="W"),
        "login_at": pd.date_range("2022-01-01", periods=n, freq="H")[:n],
        
        # Categoricals (will cause encoding explosions)
        "cat1": np.random.choice(["A", "B", "C", "D", "E"], n),
        "cat2": np.random.choice(["High", "Medium", "Low"], n),
        "cat3": np.random.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), n),
        "cat_mixed": np.random.choice(["1", "2", "Unknown", None, "5"], n),
        
        # Text (will cause NLP/Text explosion)
        "text_notes": ["This is a random note " + str(i) for i in range(n)],
        "user_bio": ["Bio " + chr(65 + i%26) for i in range(n)],
        
        # Missing values (will cause imputation explosions)
        "miss_num1": np.where(np.random.rand(n) > 0.3, np.random.randn(n), np.nan),
        "miss_num2": np.where(np.random.rand(n) > 0.5, np.random.randn(n) * 10, np.nan),
        "miss_cat": np.where(np.random.rand(n) > 0.2, np.random.choice(["X", "Y"], n), None),
        
        # Outliers & Skew (will cause outlier/scaling explosions)
        "skewed_val": np.random.exponential(scale=10, size=n),
        "outlier_val": np.append(np.random.normal(0, 1, n-5), [1000, 2000, -1000, 5000, 9999]),
        
        # Duplicates
        "dup_col": ["Same Value"] * n,
        
        # High cardinality IDs
        "uuid": [f"user_{i}" for i in range(n)]
    })
    
    # Introduce row duplicates
    df = pd.concat([df, df.head(50)])
    
    return df

async def main():
    import traceback
    try:
        df = make_tough_dataset()
        file_id = "tough_test_123"
        
        # Profile first to get the insights
        profile_result = DataProfiler.profile(df, file_id)
        profile = profile_result.profile
        
        print("Profiling done. Running cleaning engine...")
        
        # Run Decision Engine
        engine = DecisionEngine(df, file_id, profile)
        plan = engine.analyze()
        
        with open("action_counts.txt", "w") as f:
            f.write(f"\\nTotal Actions Suggested: {plan.total_actions}\\n")
            f.write(f"Definitive Actions: {plan.definitive_count}\\n")
            f.write(f"Judgment Calls: {plan.judgment_call_count}\\n")
            
            cats = {}
            for a in plan.actions:
                cat = a.category.value
                cats[cat] = cats.get(cat, 0) + 1
                
            f.write("\\nAction Breakdown by Category:\\n")
            for k, v in sorted(cats.items(), key=lambda x: -x[1]):
                f.write(f"  {k}: {v}\\n")
                
            f.write("\\nAction Details:\\n")
            for a in plan.actions:
                f.write(f"[{a.category.name}] {a.action_type.name} on {a.target_columns} - {a.confidence.name}\\n")
                
    except Exception as e:
        with open("traceback_engine.txt", "w") as f:
            f.write(traceback.format_exc())
            
if __name__ == "__main__":
    asyncio.run(main())
