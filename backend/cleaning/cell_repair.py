"""
Cell Repair — Intelligent cell-level repair suggestions with confidence scores.
"""

from __future__ import annotations

import uuid
from typing import Optional
import numpy as np
import pandas as pd


def generate_repairs(df: pd.DataFrame, max_suggestions: int = 200) -> list[dict]:
    """Scan dataset and generate cell-level repair suggestions."""
    suggestions = []

    for col in df.columns:
        if len(suggestions) >= max_suggestions:
            break

        series = df[col]

        # Typo detection for categorical columns
        if series.dtype == "object":
            repairs = _detect_typos(series, col)
            suggestions.extend(repairs[:20])

        # Impossible numeric values
        if pd.api.types.is_numeric_dtype(series):
            repairs = _detect_numeric_anomalies(series, col)
            suggestions.extend(repairs[:20])

        # Date format inconsistencies
        if series.dtype == "object":
            repairs = _detect_date_issues(series, col)
            suggestions.extend(repairs[:10])

    # Sort by confidence descending
    suggestions.sort(key=lambda s: s.get("confidence", 0), reverse=True)
    return suggestions[:max_suggestions]


def _detect_typos(series: pd.Series, col_name: str) -> list[dict]:
    """Detect likely typos using edit distance to frequent values."""
    suggestions = []
    value_counts = series.value_counts()

    if len(value_counts) < 2 or len(value_counts) > 500:
        return []

    # Get frequent values (appearing more than once)
    frequent = value_counts[value_counts > 1].index.tolist()
    rare = value_counts[value_counts == 1].index.tolist()

    if not frequent or not rare:
        return []

    for rare_val in rare[:50]:
        if pd.isna(rare_val) or not isinstance(rare_val, str):
            continue
        best_match = None
        best_dist = float("inf")
        for freq_val in frequent[:100]:
            if pd.isna(freq_val) or not isinstance(freq_val, str):
                continue
            dist = _levenshtein(str(rare_val).lower(), str(freq_val).lower())
            max_len = max(len(str(rare_val)), len(str(freq_val)), 1)
            if dist < best_dist and dist <= max(2, max_len * 0.3):
                best_dist = dist
                best_match = freq_val

        if best_match and best_dist > 0:
            confidence = max(0.5, 1 - best_dist / max(len(str(rare_val)), len(str(best_match)), 1))
            row_indices = series[series == rare_val].index.tolist()
            for idx in row_indices[:5]:
                suggestions.append({
                    "id": str(uuid.uuid4())[:8],
                    "row_index": int(idx),
                    "column": col_name,
                    "current_value": str(rare_val),
                    "suggested_value": str(best_match),
                    "confidence": round(confidence, 2),
                    "type": "typo",
                    "reasoning": f"Did you mean '{best_match}'? (edit distance: {best_dist}, appears {int(value_counts[best_match])}× in dataset)",
                    "status": "pending",
                })

    return suggestions


def _detect_numeric_anomalies(series: pd.Series, col_name: str) -> list[dict]:
    """Detect impossible/extreme numeric values."""
    suggestions = []
    clean = series.dropna()
    if len(clean) < 10:
        return []

    mean = clean.mean()
    std = clean.std()
    if std == 0:
        return []

    # Z-score based detection
    z_scores = abs((clean - mean) / std)
    outliers = z_scores[z_scores > 4].index

    for idx in list(outliers)[:20]:
        val = clean[idx]
        # Try to suggest a fix
        suggested = None
        reasoning = ""

        # Check if it's a 10x error
        if val != 0:
            for factor in [10, 100, 1000]:
                corrected = val / factor
                if abs(corrected - mean) < 2 * std:
                    suggested = round(corrected, 2)
                    reasoning = f"Value {val} appears to be a {factor}× entry error — likely should be {suggested}"
                    break

        if not suggested:
            suggested = round(mean, 2)
            reasoning = f"Value {val} is {z_scores[idx]:.1f} standard deviations from the mean ({mean:.2f})"

        suggestions.append({
            "id": str(uuid.uuid4())[:8],
            "row_index": int(idx),
            "column": col_name,
            "current_value": float(val),
            "suggested_value": float(suggested),
            "confidence": round(max(0.5, 1 - 1 / z_scores[idx]), 2),
            "type": "outlier",
            "reasoning": reasoning,
            "status": "pending",
        })

    return suggestions


def _detect_date_issues(series: pd.Series, col_name: str) -> list[dict]:
    """Detect date format inconsistencies."""
    import re
    suggestions = []

    # Check for date-like patterns
    date_patterns = {
        "us": re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$"),
        "eu": re.compile(r"^\d{1,2}-\d{1,2}-\d{2,4}$"),
        "iso": re.compile(r"^\d{4}-\d{2}-\d{2}"),
    }

    format_counts = {"us": 0, "eu": 0, "iso": 0, "other": 0}
    for val in series.dropna().head(200):
        s = str(val).strip()
        matched = False
        for fmt, pattern in date_patterns.items():
            if pattern.match(s):
                format_counts[fmt] += 1
                matched = True
                break
        if not matched:
            format_counts["other"] += 1

    # If mixed formats detected
    active_formats = {k: v for k, v in format_counts.items() if v > 0 and k != "other"}
    if len(active_formats) > 1:
        dominant = max(active_formats, key=active_formats.get)
        for fmt, count in active_formats.items():
            if fmt != dominant and count < active_formats[dominant] * 0.3:
                for idx, val in series.items():
                    s = str(val).strip()
                    if date_patterns.get(fmt, re.compile("^$")).match(s):
                        suggestions.append({
                            "id": str(uuid.uuid4())[:8],
                            "row_index": int(idx),
                            "column": col_name,
                            "current_value": str(val),
                            "suggested_value": f"Convert to {dominant} format",
                            "confidence": 0.7,
                            "type": "date_format",
                            "reasoning": f"Mixed date formats detected — majority uses {dominant} format",
                            "status": "pending",
                        })
                        if len(suggestions) >= 10:
                            break

    return suggestions


def _levenshtein(s1: str, s2: str) -> int:
    """Compute Levenshtein edit distance."""
    if len(s1) < len(s2):
        return _levenshtein(s2, s1)
    if len(s2) == 0:
        return len(s1)

    prev = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr = [i + 1]
        for j, c2 in enumerate(s2):
            curr.append(min(
                prev[j + 1] + 1,
                curr[j] + 1,
                prev[j] + (0 if c1 == c2 else 1),
            ))
        prev = curr
    return prev[-1]
