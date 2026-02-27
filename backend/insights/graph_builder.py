"""
Graph Builder — Generates column relationship graphs from correlations and mutual information.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def build_relationship_graph(df: pd.DataFrame, correlation_threshold: float = 0.3) -> dict:
    """Build a graph of column relationships."""
    nodes = []
    links = []

    # Columns as nodes
    for i, col in enumerate(df.columns):
        dtype = str(df[col].dtype)
        category = "numeric" if pd.api.types.is_numeric_dtype(df[col]) else "categorical"
        nodes.append({
            "id": col,
            "name": col,
            "symbolSize": _calculate_node_size(df[col]),
            "value": len(df[col].dropna()),
            "category": category,
            "label": {"show": True}
        })

    # Correlations as links (numeric only)
    numeric_df = df.select_dtypes(include=[np.number])
    if not numeric_df.empty:
        corr_matrix = numeric_df.corr().fillna(0)
        cols = corr_matrix.columns
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                val = corr_matrix.iloc[i, j]
                if abs(val) >= correlation_threshold:
                    links.append({
                        "source": cols[i],
                        "target": cols[j],
                        "value": round(float(val), 3),
                        "lineStyle": {
                            "width": abs(val) * 5,
                            "opacity": 0.6,
                            "color": "#6366f1" if val > 0 else "#ef4444"
                        },
                        "label": {"show": False, "formatter": "{c}"}
                    })

    # Categories for legend
    categories = [{"name": "numeric"}, {"name": "categorical"}]

    return {
        "nodes": nodes,
        "links": links,
        "categories": categories
    }


def _calculate_node_size(series: pd.Series) -> float:
    """Calculate node size based on importance/variance or just constant."""
    # Simple logic: bigger nodes for columns with more unique values relative to others
    unique_ratio = series.nunique() / max(len(series), 1)
    return 10 + (unique_ratio * 40)
