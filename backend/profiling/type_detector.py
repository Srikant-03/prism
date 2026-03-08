"""
Semantic type detection engine.
Classifies columns into 18+ semantic types using regex patterns,
statistical analysis, and value distribution heuristics.
All rules are evidence-based with confidence scores.
"""

from __future__ import annotations

import re
import math
from collections import Counter
from typing import Optional

import pandas as pd
import numpy as np

from profiling.profiling_models import SemanticType


# ──────────────────────────────────────────
# Pattern Registry
# ──────────────────────────────────────────

# Order matters: more specific patterns first, fallback patterns last
EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
URL_RE = re.compile(r"^https?://[^\s]+$|^www\.[^\s]+$", re.IGNORECASE)
PHONE_RE = re.compile(r"^[\+]?[0-9\s\-\(\)\.]{7,20}$")
IP_RE = re.compile(r"^(\d{1,3}\.){3}\d{1,3}$|^([0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}$")
GEO_RE = re.compile(r"^[\-+]?\d+\.?\d*[,\s]+[\-+]?\d+\.?\d*$")
CURRENCY_RE = re.compile(r"^[\$€£¥₹₽][\s]?[\d,]+\.?\d*$|^[\d,]+\.?\d*[\s]?[\$€£¥₹₽]$")
PERCENTAGE_RE = re.compile(r"^[\-+]?\d+\.?\d*\s*%$")
HASH_RE = re.compile(r"^[0-9a-fA-F]{32,128}$")
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
BINARY_RE = re.compile(r"^[01]+$")

# Ordinal patterns — case-insensitive
ORDINAL_PATTERNS = [
    {"values": {"low", "medium", "high"}, "label": "Low/Medium/High"},
    {"values": {"small", "medium", "large"}, "label": "Small/Medium/Large"},
    {"values": {"bronze", "silver", "gold", "platinum"}, "label": "Bronze/Silver/Gold/Platinum"},
    {"values": {"poor", "fair", "good", "very good", "excellent"}, "label": "Rating Scale"},
    {"values": {"none", "mild", "moderate", "severe"}, "label": "Severity Scale"},
    {"values": {"beginner", "intermediate", "advanced", "expert"}, "label": "Skill Level"},
    {"values": {"very low", "low", "medium", "high", "very high"}, "label": "5-Level Scale"},
    {"values": {"freshman", "sophomore", "junior", "senior"}, "label": "Academic Year"},
    {"values": {"xs", "s", "m", "l", "xl", "xxl"}, "label": "Size Scale"},
]

# Disguised boolean patterns
BOOLEAN_PAIRS = [
    {"yes", "no"}, {"y", "n"}, {"true", "false"}, {"t", "f"},
    {"1", "0"}, {"on", "off"}, {"active", "inactive"},
    {"enabled", "disabled"}, {"m", "f"}, {"male", "female"},
    {"pass", "fail"}, {"positive", "negative"},
]


class SemanticTypeDetector:
    """
    Classifies a pandas Series into one of 18+ semantic types.
    Uses a multi-signal approach: regex matching, value distribution analysis,
    and column name heuristics. Returns type + confidence score.
    """

    @staticmethod
    def detect(series: pd.Series, column_name: str = "") -> tuple[SemanticType, float]:
        """
        Detect the semantic type of a column.

        Returns:
            (SemanticType, confidence) where confidence is 0.0-1.0
        """
        if series.empty or series.isna().all():
            return SemanticType.UNKNOWN, 0.0

        # Work with non-null values
        non_null = series.dropna()
        if len(non_null) == 0:
            return SemanticType.UNKNOWN, 0.0

        n = len(non_null)
        col_lower = column_name.lower().strip()

        # ── 1. Check native dtypes first ──
        if pd.api.types.is_bool_dtype(series):
            return SemanticType.BOOLEAN, 0.95

        if pd.api.types.is_datetime64_any_dtype(series):
            return SemanticType.DATETIME, 0.95

        if pd.api.types.is_timedelta64_dtype(series):
            return SemanticType.DURATION, 0.95

        # ── 2. String-based pattern detection ──
        if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
            result = SemanticTypeDetector._detect_string_type(non_null, col_lower, n)
            if result:
                return result

        # ── 3. Numeric type classification ──
        if pd.api.types.is_numeric_dtype(series):
            return SemanticTypeDetector._classify_numeric(non_null, col_lower, n)

        # ── 4. Fallback ──
        return SemanticType.UNKNOWN, 0.3

    @staticmethod
    def _detect_string_type(
        non_null: pd.Series, col_lower: str, n: int
    ) -> Optional[tuple[SemanticType, float]]:
        """Detect type for string/object columns using pattern matching."""

        # Sample for efficiency on large datasets
        sample_size = min(n, 1000)
        sample = non_null.head(sample_size).astype(str)

        # ── Email ──
        email_match = sample.str.match(EMAIL_RE.pattern, na=False).sum()
        if email_match / sample_size > 0.7:
            return SemanticType.EMAIL, min(email_match / sample_size, 0.99)

        # ── URL ──
        url_match = sample.str.match(URL_RE.pattern, na=False).sum()
        if url_match / sample_size > 0.7:
            return SemanticType.URL, min(url_match / sample_size, 0.99)

        # ── IP Address ──
        ip_match = sample.str.match(IP_RE.pattern, na=False).sum()
        if ip_match / sample_size > 0.7:
            return SemanticType.IP_ADDRESS, min(ip_match / sample_size, 0.99)

        # ── Phone ──
        phone_hints = any(h in col_lower for h in ["phone", "tel", "mobile", "cell", "fax"])
        phone_match = sample.str.match(PHONE_RE.pattern, na=False).sum()
        if phone_match / sample_size > 0.6 and phone_hints:
            return SemanticType.PHONE, min(phone_match / sample_size * 0.9, 0.95)
        if phone_match / sample_size > 0.85:
            return SemanticType.PHONE, min(phone_match / sample_size * 0.8, 0.90)

        # ── Currency ──
        currency_match = sample.str.match(CURRENCY_RE.pattern, na=False).sum()
        currency_hints = any(h in col_lower for h in ["price", "cost", "amount", "salary", "revenue", "fee", "total", "payment"])
        if currency_match / sample_size > 0.5:
            return SemanticType.CURRENCY, min(currency_match / sample_size, 0.95)
        if currency_hints and _try_numeric_ratio(sample) > 0.7:
            return SemanticType.CURRENCY, 0.6

        # ── Percentage ──
        pct_match = sample.str.match(PERCENTAGE_RE.pattern, na=False).sum()
        pct_hints = any(h in col_lower for h in ["percent", "pct", "rate", "ratio", "_pct", "%"])
        if pct_match / sample_size > 0.5:
            return SemanticType.PERCENTAGE, min(pct_match / sample_size, 0.95)

        # ── Hash / Encoded ──
        hash_match = sample.str.match(HASH_RE.pattern, na=False).sum()
        uuid_match = sample.str.match(UUID_RE.pattern, na=False).sum()
        if uuid_match / sample_size > 0.7:
            return SemanticType.ID_KEY, min(uuid_match / sample_size, 0.95)
        if hash_match / sample_size > 0.7:
            return SemanticType.HASHED, min(hash_match / sample_size, 0.90)

        # ── Geo Coordinates ──
        geo_match = sample.str.match(GEO_RE.pattern, na=False).sum()
        geo_hints = bool(re.search(r'\b(lat|lon|lng|coord|geo|latitude|longitude)\b', col_lower))
        if geo_match / sample_size > 0.5 and geo_hints:
            return SemanticType.GEO_COORDINATE, 0.85
        if geo_match / sample_size > 0.8:
            return SemanticType.GEO_COORDINATE, 0.70

        # ── Disguised Boolean ──
        unique_lower = set(sample.str.lower().str.strip().unique())
        for pair in BOOLEAN_PAIRS:
            if unique_lower <= pair or (len(unique_lower) == 2 and unique_lower <= pair | {""}):
                return SemanticType.BOOLEAN, 0.90

        # ── Binary Encoded ──
        if all(len(str(v)) > 10 for v in sample.head(20)):
            binary_match = sample.head(100).str.match(BINARY_RE.pattern, na=False).sum()
            if binary_match > 80:
                return SemanticType.BINARY_ENCODED, 0.80

        # ── Datetime (string-encoded) ──
        try:
            parsed = pd.to_datetime(sample, errors="coerce", infer_datetime_format=True)
            dt_ratio = parsed.notna().sum() / sample_size
            if dt_ratio > 0.7:
                date_hints = any(h in col_lower for h in [
                    "date", "time", "timestamp", "created", "updated", "modified",
                    "born", "dob", "start", "end", "expir",
                ])
                conf = dt_ratio * (0.95 if date_hints else 0.80)
                return SemanticType.DATETIME, min(conf, 0.95)
        except Exception:
            pass

        # ── ID / Key ──
        distinct_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 0
        id_hints = any(h in col_lower for h in ["_id", "id_", "key", "code", "sku", "uuid", "guid"])
        if distinct_ratio > 0.95 and id_hints:
            return SemanticType.ID_KEY, 0.90
        if distinct_ratio == 1.0 and len(non_null) > 10 and id_hints:
            return SemanticType.ID_KEY, 0.95

        # ── Ordinal ──
        for pattern in ORDINAL_PATTERNS:
            if unique_lower <= pattern["values"]:
                return SemanticType.CATEGORICAL_ORDINAL, 0.85

        # ── Free Text vs Categorical ──
        avg_len = sample.str.len().mean()
        unique_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 0

        if avg_len > 50 and unique_ratio > 0.5:
            return SemanticType.FREE_TEXT, 0.80

        if unique_ratio > 0.95 and avg_len > 20:
            return SemanticType.FREE_TEXT, 0.65

        # Default to categorical
        return SemanticType.CATEGORICAL_NOMINAL, 0.70

    @staticmethod
    def _classify_numeric(
        non_null: pd.Series, col_lower: str, n: int
    ) -> tuple[SemanticType, float]:
        """Classify a numeric column as continuous, discrete, geo, percentage, etc."""

        # ── Geo coordinate check ──
        geo_hints = bool(re.search(r'\b(lat|lon|lng|latitude|longitude)\b', col_lower))
        if geo_hints:
            vals = non_null.astype(float)
            if (vals.abs() <= 180).all():
                return SemanticType.GEO_COORDINATE, 0.90

        # ── Percentage check ──
        pct_hints = any(h in col_lower for h in ["percent", "pct", "rate", "ratio"])
        if pct_hints:
            vals = non_null.astype(float)
            if (vals >= 0).all() and (vals <= 100).all():
                return SemanticType.PERCENTAGE, 0.80

        # ── ID / Key ──
        id_hints = any(h in col_lower for h in ["_id", "id_", "id", "key", "code", "index", "number", "num", "no"])
        distinct_ratio = non_null.nunique() / n if n > 0 else 0

        if distinct_ratio > 0.95 and id_hints and n > 10:
            return SemanticType.ID_KEY, 0.85

        # ── Boolean (0/1) ──
        unique_vals = set(non_null.unique())
        if unique_vals <= {0, 1, 0.0, 1.0}:
            return SemanticType.BOOLEAN, 0.80

        # ── Continuous vs Discrete ──
        if pd.api.types.is_integer_dtype(non_null):
            n_unique = non_null.nunique()
            if n_unique <= 20 and n > 50:
                return SemanticType.NUMERIC_DISCRETE, 0.85
            return SemanticType.NUMERIC_DISCRETE, 0.75
        else:
            # Float — check if actually integer-valued
            try:
                is_int = (non_null == non_null.astype(int)).all()
                if is_int:
                    n_unique = non_null.nunique()
                    if n_unique <= 20 and n > 50:
                        return SemanticType.NUMERIC_DISCRETE, 0.80
            except (ValueError, OverflowError):
                pass

            return SemanticType.NUMERIC_CONTINUOUS, 0.80

    @staticmethod
    def is_numeric_type(sem_type: SemanticType) -> bool:
        return sem_type in {
            SemanticType.NUMERIC_CONTINUOUS,
            SemanticType.NUMERIC_DISCRETE,
            SemanticType.CURRENCY,
            SemanticType.PERCENTAGE,
            SemanticType.GEO_COORDINATE,
        }

    @staticmethod
    def is_categorical_type(sem_type: SemanticType) -> bool:
        return sem_type in {
            SemanticType.CATEGORICAL_NOMINAL,
            SemanticType.CATEGORICAL_ORDINAL,
        }

    @staticmethod
    def is_datetime_type(sem_type: SemanticType) -> bool:
        return sem_type in {SemanticType.DATETIME, SemanticType.DURATION}

    @staticmethod
    def is_boolean_type(sem_type: SemanticType) -> bool:
        return sem_type == SemanticType.BOOLEAN

    @staticmethod
    def is_text_type(sem_type: SemanticType) -> bool:
        return sem_type in {
            SemanticType.FREE_TEXT,
            SemanticType.EMAIL,
            SemanticType.URL,
            SemanticType.PHONE,
            SemanticType.IP_ADDRESS,
        }

    @staticmethod
    def is_id_type(sem_type: SemanticType) -> bool:
        return sem_type in {
            SemanticType.ID_KEY,
            SemanticType.HASHED,
            SemanticType.BINARY_ENCODED,
        }


def _try_numeric_ratio(sample: pd.Series) -> float:
    """Check what ratio of string values can be parsed as numbers."""
    try:
        cleaned = sample.str.replace(r"[\$€£¥₹₽,%\s]", "", regex=True)
        numeric = pd.to_numeric(cleaned, errors="coerce")
        return numeric.notna().sum() / len(sample)
    except Exception:
        return 0.0
