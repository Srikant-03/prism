"""
Type Handler — Detects and corrects data type mismatches without user configuration.
Handles dates, currency, booleans, mixed-type columns, JSON, URLs, emails, IPs, UUIDs.
"""

from __future__ import annotations

import re
import json
from typing import Any, Optional

import numpy as np
import pandas as pd

from cleaning.cleaning_models import (
    CleaningAction, ActionType, ActionConfidence, ActionCategory,
    ImpactEstimate, PreviewSample, UserOption,
    TypeCorrectionReport, TypeCorrectionDetail, DetectedType,
)


# ── Boolean mappings ──────────────────────────────────────────────────
_BOOL_TRUE = {
    "true", "yes", "y", "1", "t", "on", "active", "enabled",
    "checked", "pass", "ok", "correct", "positive",
}
_BOOL_FALSE = {
    "false", "no", "n", "0", "f", "off", "inactive", "disabled",
    "unchecked", "fail", "wrong", "negative",
}

# ── Currency patterns ─────────────────────────────────────────────────
_CURRENCY_SYMBOLS = r"[\$€£¥₹₽₩₪₫¢₴₦]"
_CURRENCY_PATTERN = re.compile(
    rf"^\s*{_CURRENCY_SYMBOLS}?\s*[\-\+]?\s*[\d,]+\.?\d*\s*[KkMmBbTt]?\s*$"
    r"|"
    r"^\s*\([\d,]+\.?\d*\)\s*$"  # Negative in parens: (1,200)
    r"|"
    rf"^\s*[\-\+]?\s*[\d,]+\.?\d*\s*{_CURRENCY_SYMBOLS}\s*$"  # Symbol after
    r"|"
    r"^\s*[\-\+]?\s*[\d,]+\.?\d*\s*%\s*$"  # Percentage
)

# ── Pseudo-null values ────────────────────────────────────────────────
_PSEUDO_NULLS = {
    "n/a", "na", "nan", "null", "none", "--", "-", ".", "..",
    "missing", "undefined", "not available", "not applicable",
    "no data", "empty", "#n/a", "#ref!", "#value!", "#null!",
    "?", "unknown", "tbd", "tba", "nil",
}

# ── Structured data patterns ─────────────────────────────────────────
_URL_PATTERN = re.compile(r'^https?://\S+$', re.IGNORECASE)
_EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')
_IP_PATTERN = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
_UUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)


class TypeHandler:
    """Detect and correct data type mismatches."""

    def __init__(self, df: pd.DataFrame, file_id: str, profile: Any = None):
        self.df = df
        self.file_id = file_id
        self.n_rows = len(df)
        self.n_cols = len(df.columns)
        self.profile = profile

    # ── Public entry ──────────────────────────────────────────────────
    def analyze(self) -> tuple[list[CleaningAction], TypeCorrectionReport]:
        actions: list[CleaningAction] = []
        report = TypeCorrectionReport(total_columns_analyzed=self.n_cols)

        for col in self.df.columns:
            series = self.df[col]

            # Skip columns that are already numeric/datetime and correct
            if pd.api.types.is_datetime64_any_dtype(series):
                continue

            # Only analyze object/string columns for type mismatches
            if series.dtype == object or str(series.dtype) == "string":
                detections = self._analyze_string_column(col, series)
                for detail in detections:
                    report.corrections.append(detail)
                    action = self._detail_to_action(detail)
                    if action:
                        actions.append(action)

            # Check for integer columns that are actually categories
            elif pd.api.types.is_integer_dtype(series):
                cat_detail = self._check_integer_category(col, series)
                if cat_detail:
                    report.corrections.append(cat_detail)
                    action = self._detail_to_action(cat_detail)
                    if action:
                        actions.append(action)

            # Check for pseudo-nulls in any column
            if series.dtype == object:
                pseudo_detail = self._check_pseudo_nulls(col, series)
                if pseudo_detail:
                    report.corrections.append(pseudo_detail)
                    action = self._detail_to_action(pseudo_detail)
                    if action:
                        actions.append(action)

        report.total_corrections_found = len(report.corrections)
        report.mixed_type_columns = self._find_mixed_type_columns()
        report.structured_data_columns = self._find_structured_columns()

        return actions, report

    # ── String Column Analysis ────────────────────────────────────────
    def _analyze_string_column(self, col: str, series: pd.Series) -> list[TypeCorrectionDetail]:
        detections: list[TypeCorrectionDetail] = []
        non_null = series.dropna()
        if len(non_null) == 0:
            return detections

        values = non_null.astype(str)
        total = len(values)

        # 1. Date detection
        date_detail = self._detect_dates(col, values, total)
        if date_detail and date_detail.parse_success_rate > 0.7:
            detections.append(date_detail)
            return detections  # If it's a date column, don't check other types

        # 2. Currency / numeric-with-symbols
        curr_detail = self._detect_currency(col, values, total)
        if curr_detail and curr_detail.parse_success_rate > 0.7:
            detections.append(curr_detail)
            return detections

        # 3. Boolean
        bool_detail = self._detect_booleans(col, values, total)
        if bool_detail and bool_detail.parse_success_rate > 0.8:
            detections.append(bool_detail)
            return detections

        # 4. Structured data (JSON, URL, email, IP, UUID)
        struct_detail = self._detect_structured(col, values, total)
        if struct_detail:
            detections.append(struct_detail)

        return detections

    # ── Date Detection ────────────────────────────────────────────────
    def _detect_dates(self, col: str, values: pd.Series, total: int) -> Optional[TypeCorrectionDetail]:
        # Sample for speed
        sample = values.head(200) if len(values) > 200 else values
        parsed = 0
        sample_before: list[str] = []
        sample_after: list[str] = []
        unparseable: list[str] = []

        for val in sample:
            val_str = str(val).strip()
            if not val_str:
                continue

            # Try epoch detection (seconds or milliseconds)
            try:
                num = float(val_str)
                if 1e9 < num < 2e10:  # epoch seconds (1970–2033)
                    parsed += 1
                    sample_before.append(val_str)
                    dt = pd.Timestamp(num, unit='s')
                    sample_after.append(str(dt))
                    continue
                elif 1e12 < num < 2e13:  # epoch milliseconds
                    parsed += 1
                    sample_before.append(val_str)
                    dt = pd.Timestamp(num, unit='ms')
                    sample_after.append(str(dt))
                    continue
            except (ValueError, OverflowError):
                pass

            # Try standard date parsing
            try:
                dt = pd.to_datetime(val_str, dayfirst=False)
                parsed += 1
                if len(sample_before) < 5:
                    sample_before.append(val_str)
                    sample_after.append(str(dt))
            except Exception:
                # Try European format
                try:
                    dt = pd.to_datetime(val_str, dayfirst=True)
                    parsed += 1
                    if len(sample_before) < 5:
                        sample_before.append(val_str)
                        sample_after.append(str(dt))
                except Exception:
                    if len(unparseable) < 5:
                        unparseable.append(val_str)

        rate = parsed / len(sample) if len(sample) > 0 else 0
        if rate < 0.5:
            return None

        return TypeCorrectionDetail(
            column=col,
            current_dtype=str(self.df[col].dtype),
            detected_type=DetectedType.DATETIME,
            confidence=round(rate, 3),
            parse_success_rate=round(rate, 3),
            sample_before=sample_before[:5],
            sample_after=sample_after[:5],
            unparseable_count=len(values) - int(rate * total),
            unparseable_samples=unparseable[:5],
        )

    # ── Currency / Numeric Detection ──────────────────────────────────
    def _detect_currency(self, col: str, values: pd.Series, total: int) -> Optional[TypeCorrectionDetail]:
        sample = values.head(200) if len(values) > 200 else values
        matches = 0
        sample_before: list[str] = []
        sample_after: list[str] = []
        unparseable: list[str] = []

        for val in sample:
            val_str = str(val).strip()
            if not val_str:
                continue

            if _CURRENCY_PATTERN.match(val_str):
                parsed_val = self._parse_currency_value(val_str)
                if parsed_val is not None:
                    matches += 1
                    if len(sample_before) < 5:
                        sample_before.append(val_str)
                        sample_after.append(str(parsed_val))
                    continue

            if len(unparseable) < 5:
                unparseable.append(val_str)

        rate = matches / len(sample) if len(sample) > 0 else 0
        if rate < 0.5:
            return None

        detected = DetectedType.CURRENCY
        # Check if it's a percentage column
        pct_count = sum(1 for v in sample if "%" in str(v))
        if pct_count / len(sample) > 0.5:
            detected = DetectedType.PERCENTAGE

        return TypeCorrectionDetail(
            column=col,
            current_dtype=str(self.df[col].dtype),
            detected_type=detected,
            confidence=round(rate, 3),
            parse_success_rate=round(rate, 3),
            sample_before=sample_before[:5],
            sample_after=sample_after[:5],
            unparseable_count=total - int(rate * total),
            unparseable_samples=unparseable[:5],
        )

    def _parse_currency_value(self, val_str: str) -> Optional[float]:
        """Parse a currency/numeric string to float."""
        try:
            s = val_str.strip()

            # Handle parentheses for negatives: (1,200) → -1200
            is_negative = False
            if s.startswith("(") and s.endswith(")"):
                is_negative = True
                s = s[1:-1]

            # Handle leading minus
            if s.startswith("-"):
                is_negative = True
                s = s[1:]

            # Remove currency symbols
            s = re.sub(r'[\$€£¥₹₽₩₪₫¢₴₦]', '', s).strip()

            # Handle percentage
            if s.endswith("%"):
                s = s[:-1].strip()
                s = s.replace(",", "")
                return -float(s) / 100 if is_negative else float(s) / 100

            # Handle suffixes K/M/B/T
            multiplier = 1
            if s and s[-1].upper() in "KMBT":
                suffix = s[-1].upper()
                s = s[:-1]
                multiplier = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[suffix]

            # Remove thousand separators
            s = s.replace(",", "").strip()
            if not s:
                return None

            result = float(s) * multiplier
            return -result if is_negative else result
        except (ValueError, KeyError):
            return None

    # ── Boolean Detection ─────────────────────────────────────────────
    def _detect_booleans(self, col: str, values: pd.Series, total: int) -> Optional[TypeCorrectionDetail]:
        unique = values.str.lower().str.strip().unique()
        if len(unique) > 10:
            return None

        # Check if all values are in boolean mappings
        bool_matches = 0
        sample_before: list[str] = []
        sample_after: list[str] = []

        for val in values.head(100):
            val_lower = str(val).lower().strip()
            if val_lower in _BOOL_TRUE:
                bool_matches += 1
                if len(sample_before) < 5:
                    sample_before.append(str(val))
                    sample_after.append("True")
            elif val_lower in _BOOL_FALSE:
                bool_matches += 1
                if len(sample_before) < 5:
                    sample_before.append(str(val))
                    sample_after.append("False")

        rate = bool_matches / min(total, 100) if total > 0 else 0

        if rate < 0.8:
            return None

        return TypeCorrectionDetail(
            column=col,
            current_dtype=str(self.df[col].dtype),
            detected_type=DetectedType.BOOLEAN,
            confidence=round(rate, 3),
            parse_success_rate=round(rate, 3),
            sample_before=sample_before[:5],
            sample_after=sample_after[:5],
        )

    # ── Integer Category Detection ────────────────────────────────────
    def _check_integer_category(self, col: str, series: pd.Series) -> Optional[TypeCorrectionDetail]:
        """Detect integer columns that represent categories."""
        nunique = series.nunique()
        if nunique > 20 or nunique < 2:
            return None

        # Integer with few unique values → likely categorical
        values = sorted(series.dropna().unique())
        # Check if values are sequential small integers
        if all(isinstance(v, (int, np.integer)) for v in values):
            max_val = max(values)
            if max_val <= 20:
                return TypeCorrectionDetail(
                    column=col,
                    current_dtype=str(series.dtype),
                    detected_type=DetectedType.CATEGORICAL_INT,
                    confidence=0.7,
                    parse_success_rate=1.0,
                    sample_before=[str(v) for v in values[:5]],
                    sample_after=[f"cat_{v}" for v in values[:5]],
                )
        return None

    # ── Pseudo-Null Detection ─────────────────────────────────────────
    def _check_pseudo_nulls(self, col: str, series: pd.Series) -> Optional[TypeCorrectionDetail]:
        """Detect string representations of null values."""
        non_null = series.dropna()
        if len(non_null) == 0:
            return None

        pseudo_mask = non_null.astype(str).str.lower().str.strip().isin(_PSEUDO_NULLS)
        pseudo_count = int(pseudo_mask.sum())

        if pseudo_count == 0:
            return None

        pseudo_samples = non_null[pseudo_mask].head(5).astype(str).tolist()

        return TypeCorrectionDetail(
            column=col,
            current_dtype=str(series.dtype),
            detected_type=DetectedType.UNKNOWN,  # Will re-infer after replacement
            confidence=0.95,
            parse_success_rate=1.0,
            sample_before=pseudo_samples,
            sample_after=["NaN"] * len(pseudo_samples),
            unparseable_count=0,
        )

    # ── Structured Data Detection ─────────────────────────────────────
    def _detect_structured(self, col: str, values: pd.Series, total: int) -> Optional[TypeCorrectionDetail]:
        sample = values.head(100)
        sample_str = sample.astype(str).str.strip()

        # JSON detection
        json_count = sum(1 for v in sample_str if self._is_json(v))
        if json_count / len(sample) > 0.7:
            return TypeCorrectionDetail(
                column=col,
                current_dtype=str(self.df[col].dtype),
                detected_type=DetectedType.JSON_BLOB,
                confidence=round(json_count / len(sample), 3),
                parse_success_rate=round(json_count / len(sample), 3),
                sample_before=sample_str.head(3).tolist(),
                sample_after=["(expanded to sub-columns)"] * min(3, len(sample)),
            )

        # URL detection
        url_count = sum(1 for v in sample_str if _URL_PATTERN.match(v))
        if url_count / len(sample) > 0.7:
            return TypeCorrectionDetail(
                column=col,
                current_dtype=str(self.df[col].dtype),
                detected_type=DetectedType.URL,
                confidence=round(url_count / len(sample), 3),
                parse_success_rate=round(url_count / len(sample), 3),
                sample_before=sample_str.head(3).tolist(),
                sample_after=["(validated URL)"] * min(3, len(sample)),
            )

        # Email detection
        email_count = sum(1 for v in sample_str if _EMAIL_PATTERN.match(v))
        if email_count / len(sample) > 0.7:
            return TypeCorrectionDetail(
                column=col,
                current_dtype=str(self.df[col].dtype),
                detected_type=DetectedType.EMAIL,
                confidence=round(email_count / len(sample), 3),
                parse_success_rate=round(email_count / len(sample), 3),
                sample_before=sample_str.head(3).tolist(),
                sample_after=sample_str.head(3).str.lower().tolist(),
            )

        # IP address detection
        ip_count = sum(1 for v in sample_str if _IP_PATTERN.match(v))
        if ip_count / len(sample) > 0.7:
            return TypeCorrectionDetail(
                column=col,
                current_dtype=str(self.df[col].dtype),
                detected_type=DetectedType.IP_ADDRESS,
                confidence=round(ip_count / len(sample), 3),
                parse_success_rate=round(ip_count / len(sample), 3),
                sample_before=sample_str.head(3).tolist(),
                sample_after=["(validated IP)"] * min(3, len(sample)),
            )

        # UUID detection
        uuid_count = sum(1 for v in sample_str if _UUID_PATTERN.match(v))
        if uuid_count / len(sample) > 0.7:
            return TypeCorrectionDetail(
                column=col,
                current_dtype=str(self.df[col].dtype),
                detected_type=DetectedType.UUID,
                confidence=round(uuid_count / len(sample), 3),
                parse_success_rate=round(uuid_count / len(sample), 3),
                sample_before=sample_str.head(3).tolist(),
                sample_after=sample_str.head(3).str.lower().tolist(),
            )

        return None

    # ── Mixed-Type Columns ────────────────────────────────────────────
    def _find_mixed_type_columns(self) -> list[str]:
        """Find columns with mixed data types."""
        mixed: list[str] = []
        for col in self.df.columns:
            if self.df[col].dtype != object:
                continue
            non_null = self.df[col].dropna()
            if len(non_null) == 0:
                continue

            # Check if column has a mix of numeric and string values
            numeric_count = 0
            string_count = 0
            for val in non_null.head(100):
                try:
                    float(val)
                    numeric_count += 1
                except (ValueError, TypeError):
                    val_lower = str(val).strip().lower()
                    if val_lower not in _PSEUDO_NULLS:
                        string_count += 1

            if numeric_count > 0 and string_count > 0:
                ratio = min(numeric_count, string_count) / (numeric_count + string_count)
                if ratio > 0.05:  # At least 5% of the minority type
                    mixed.append(col)

        return mixed

    # ── Structured Column Detection ───────────────────────────────────
    def _find_structured_columns(self) -> list[dict[str, Any]]:
        """Identify columns containing structured data (JSON, lists)."""
        structured: list[dict] = []
        for col in self.df.columns:
            if self.df[col].dtype != object:
                continue
            sample = self.df[col].dropna().head(20).astype(str)
            json_count = sum(1 for v in sample if self._is_json(v))
            if json_count / max(len(sample), 1) > 0.5:
                structured.append({
                    "column": col,
                    "type": "json",
                    "sample": sample.head(2).tolist(),
                })
        return structured

    # ── Convert TypeCorrectionDetail → CleaningAction ─────────────────
    def _detail_to_action(self, detail: TypeCorrectionDetail) -> Optional[CleaningAction]:
        """Create a CleaningAction for a type correction."""
        col = detail.column
        dt = detail.detected_type

        # Map detected type to ActionType
        action_type_map = {
            DetectedType.DATETIME: ActionType.PARSE_DATES,
            DetectedType.CURRENCY: ActionType.PARSE_CURRENCY,
            DetectedType.PERCENTAGE: ActionType.PARSE_CURRENCY,
            DetectedType.BOOLEAN: ActionType.STANDARDIZE_BOOLEANS,
            DetectedType.CATEGORICAL_INT: ActionType.FLAG_INTEGER_CATEGORIES,
            DetectedType.JSON_BLOB: ActionType.EXPAND_JSON,
            DetectedType.LIST: ActionType.EXPAND_JSON,
            DetectedType.URL: ActionType.VALIDATE_STRUCTURED,
            DetectedType.EMAIL: ActionType.VALIDATE_STRUCTURED,
            DetectedType.IP_ADDRESS: ActionType.VALIDATE_STRUCTURED,
            DetectedType.UUID: ActionType.VALIDATE_STRUCTURED,
            DetectedType.UNKNOWN: ActionType.REPLACE_PSEUDO_NULLS,
        }

        at = action_type_map.get(dt)
        if at is None:
            return None

        # Confidence determines definitive vs judgment call
        is_definitive = detail.confidence > 0.95 and dt in (
            DetectedType.BOOLEAN, DetectedType.UNKNOWN,  # pseudo-nulls
        )

        # Build preview
        preview = PreviewSample(
            before=[{col: v} for v in detail.sample_before[:3]],
            after=[{col: v} for v in detail.sample_after[:3]],
            columns_before=[col],
            columns_after=[col] if dt != DetectedType.JSON_BLOB else [f"{col}_*"],
        )

        # Evidence text
        evidence_map = {
            DetectedType.DATETIME: f"Column '{col}' contains date strings ({detail.parse_success_rate * 100:.0f}% parseable). Mixed formats handled per-cell.",
            DetectedType.CURRENCY: f"Column '{col}' contains currency/numeric strings with symbols ({detail.parse_success_rate * 100:.0f}% parseable).",
            DetectedType.PERCENTAGE: f"Column '{col}' contains percentage strings ({detail.parse_success_rate * 100:.0f}% parseable).",
            DetectedType.BOOLEAN: f"Column '{col}' contains boolean representations ({detail.parse_success_rate * 100:.0f}% match). Values: {detail.sample_before[:3]}",
            DetectedType.CATEGORICAL_INT: f"Column '{col}' is integer type with only {len(detail.sample_before)} unique values — likely categorical codes.",
            DetectedType.JSON_BLOB: f"Column '{col}' contains JSON objects ({detail.parse_success_rate * 100:.0f}% valid JSON).",
            DetectedType.URL: f"Column '{col}' contains URLs ({detail.parse_success_rate * 100:.0f}% valid).",
            DetectedType.EMAIL: f"Column '{col}' contains email addresses ({detail.parse_success_rate * 100:.0f}% valid format).",
            DetectedType.IP_ADDRESS: f"Column '{col}' contains IP addresses ({detail.parse_success_rate * 100:.0f}% valid).",
            DetectedType.UUID: f"Column '{col}' contains UUIDs ({detail.parse_success_rate * 100:.0f}% valid format).",
            DetectedType.UNKNOWN: f"Column '{col}' contains pseudo-null strings: {detail.sample_before[:3]}",
        }

        recommendation_map = {
            DetectedType.DATETIME: f"Parse '{col}' as datetime, handling mixed formats per-cell.",
            DetectedType.CURRENCY: f"Parse '{col}' to numeric, removing currency symbols and separators.",
            DetectedType.PERCENTAGE: f"Parse '{col}' to numeric (0–1 scale), removing % signs.",
            DetectedType.BOOLEAN: f"Standardize '{col}' to True/False boolean type.",
            DetectedType.CATEGORICAL_INT: f"Convert '{col}' to categorical type for correct downstream treatment.",
            DetectedType.JSON_BLOB: f"Parse '{col}' JSON and expand into sub-columns.",
            DetectedType.URL: f"Validate and standardize URLs in '{col}'.",
            DetectedType.EMAIL: f"Validate and lowercase email addresses in '{col}'.",
            DetectedType.IP_ADDRESS: f"Validate IP address format in '{col}'.",
            DetectedType.UUID: f"Validate and lowercase UUIDs in '{col}'.",
            DetectedType.UNKNOWN: f"Replace pseudo-null strings in '{col}' with actual NaN, then re-infer type.",
        }

        options = [
            UserOption(key="apply", label="Apply Correction", is_default=True),
            UserOption(key="skip", label="Skip — Keep Current Type"),
        ]
        if dt == DetectedType.JSON_BLOB:
            options.insert(1, UserOption(key="expand", label="Expand to Sub-Columns"))
        if dt == DetectedType.CATEGORICAL_INT:
            options[0] = UserOption(key="apply", label="Convert to Categorical", is_default=True)

        return CleaningAction(
            category=ActionCategory.TYPE_CORRECTION,
            action_type=at,
            confidence=ActionConfidence.DEFINITIVE if is_definitive else ActionConfidence.JUDGMENT_CALL,
            evidence=evidence_map.get(dt, f"Type mismatch in '{col}'."),
            recommendation=recommendation_map.get(dt, f"Correct type for '{col}'."),
            reasoning=(
                f"Current dtype is '{detail.current_dtype}' but content matches "
                f"{dt.value} pattern with {detail.confidence * 100:.0f}% confidence. "
                f"{detail.unparseable_count} values cannot be parsed."
            ),
            target_columns=[col],
            preview=preview,
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols,
                columns_after=self.n_cols + (5 if dt == DetectedType.JSON_BLOB else 0),
                rows_affected=int(detail.parse_success_rate * self.n_rows),
                rows_affected_pct=round(detail.parse_success_rate * 100, 1),
                description=f"Converts {int(detail.parse_success_rate * self.n_rows):,} values to {dt.value}.",
            ),
            options=options,
            metadata={
                "detected_type": dt.value,
                "parse_rate": detail.parse_success_rate,
                "unparseable": detail.unparseable_count,
            },
        )

    def _is_json(self, val: str) -> bool:
        s = val.strip()
        if not (s.startswith("{") or s.startswith("[")):
            return False
        try:
            json.loads(s)
            return True
        except (json.JSONDecodeError, ValueError):
            return False
