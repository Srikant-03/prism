"""
Standardization Handler — Detects and fixes data consistency issues:
casing, whitespace, unicode, synonyms, units, precision, phone, email, URL, currency.
"""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from typing import Any, Optional

import numpy as np
import pandas as pd

from cleaning.cleaning_models import (
    CleaningAction, ActionType, ActionConfidence, ActionCategory,
    ImpactEstimate, PreviewSample, UserOption,
)

# ── Column-name pattern detectors ─────────────────────────────────────
_PHONE_PATTERNS = re.compile(r"(?i)(phone|tel|mobile|fax|cell|contact_number)")
_EMAIL_PATTERNS = re.compile(r"(?i)(email|e[-_]?mail|mail_address)")
_URL_PATTERNS = re.compile(r"(?i)(url|link|website|homepage|webpage|uri)")
_CURRENCY_COL_PATTERNS = re.compile(r"(?i)(price|cost|amount|fee|revenue|salary|income|payment|charge)")


class StandardizationHandler:
    """Data standardization and consistency engine."""

    def __init__(self, df: pd.DataFrame, file_id: str, profile: Any = None):
        self.df = df
        self.file_id = file_id
        self.n_rows = len(df)
        self.n_cols = len(df.columns)
        self.profile = profile

    # ── Public entry ──────────────────────────────────────────────────
    def analyze(self) -> tuple[list[CleaningAction], dict]:
        actions: list[CleaningAction] = []
        report: dict[str, Any] = {
            "casing_issues": [],
            "whitespace_issues": [],
            "unicode_issues": [],
            "synonym_clusters": [],
            "unit_issues": [],
            "precision_issues": [],
            "phone_columns": [],
            "email_columns": [],
            "url_columns": [],
            "currency_format_issues": [],
        }

        str_cols = self.df.select_dtypes(include=["object", "string"]).columns.tolist()
        num_cols = self.df.select_dtypes(include=[np.number]).columns.tolist()

        for col in str_cols:
            series = self.df[col].dropna()
            if len(series) < 3:
                continue

            # 1. Casing inconsistency
            casing = self._check_casing(col, series)
            if casing:
                report["casing_issues"].append(casing)
                actions.append(self._build_casing_action(col, casing))

            # 2. Whitespace issues
            ws = self._check_whitespace(col, series)
            if ws:
                report["whitespace_issues"].append(ws)
                actions.append(self._build_whitespace_action(col, ws))

            # 3. Unicode issues
            uni = self._check_unicode(col, series)
            if uni:
                report["unicode_issues"].append(uni)
                actions.append(self._build_unicode_action(col, uni))

            # 4. Synonyms / alias clusters
            synonyms = self._detect_synonyms(col, series)
            if synonyms:
                report["synonym_clusters"].extend(synonyms)
                actions.append(self._build_synonym_action(col, synonyms))

            # 5. Phone columns
            if _PHONE_PATTERNS.search(col):
                phone_info = self._check_phone(col, series)
                if phone_info:
                    report["phone_columns"].append(phone_info)
                    actions.append(self._build_phone_action(col, phone_info))

            # 6. Email columns
            if _EMAIL_PATTERNS.search(col):
                email_info = self._check_email(col, series)
                if email_info:
                    report["email_columns"].append(email_info)
                    actions.append(self._build_email_action(col, email_info))

            # 7. URL columns
            if _URL_PATTERNS.search(col):
                url_info = self._check_url(col, series)
                if url_info:
                    report["url_columns"].append(url_info)
                    actions.append(self._build_url_action(col, url_info))

            # 8. Currency format in string columns
            currency_info = self._check_currency_format(col, series)
            if currency_info:
                report["currency_format_issues"].append(currency_info)
                actions.append(self._build_currency_format_action(col, currency_info))

        # 9. Numeric precision issues
        for col in num_cols:
            prec = self._check_precision(col)
            if prec:
                report["precision_issues"].append(prec)
                actions.append(self._build_precision_action(col, prec))

        # 10. Unit inconsistency (numeric columns)
        for col in num_cols:
            unit = self._check_unit_inconsistency(col)
            if unit:
                report["unit_issues"].append(unit)
                actions.append(self._build_unit_action(col, unit))

        return actions, report

    # ── 1. Casing check ───────────────────────────────────────────────
    def _check_casing(self, col: str, series: pd.Series) -> Optional[dict]:
        unique_vals = series.unique()
        if len(unique_vals) > 500:
            return None

        casings = {"upper": 0, "lower": 0, "title": 0, "mixed": 0}
        for val in unique_vals:
            s = str(val)
            if s.isupper():
                casings["upper"] += 1
            elif s.islower():
                casings["lower"] += 1
            elif s.istitle():
                casings["title"] += 1
            else:
                casings["mixed"] += 1

        active = {k: v for k, v in casings.items() if v > 0}
        if len(active) <= 1:
            return None  # Consistent casing

        dominant = max(active, key=active.get)
        inconsistent = sum(v for k, v in active.items() if k != dominant)
        if inconsistent < 2:
            return None

        return {
            "column": col,
            "casings": casings,
            "dominant": dominant,
            "inconsistent_count": inconsistent,
        }

    def _build_casing_action(self, col: str, info: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATA_STANDARDIZATION,
            action_type=ActionType.STANDARDIZE_CASING,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Column '{col}' has inconsistent casing: {info['casings']}. "
                f"Dominant style: {info['dominant']} ({info['inconsistent_count']} inconsistent values)."
            ),
            recommendation=f"Standardize '{col}' to {info['dominant']} case.",
            reasoning="Inconsistent casing causes duplicate categories and matching failures.",
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                description=f"Normalizes {info['inconsistent_count']} values to {info['dominant']} case.",
            ),
            options=[
                UserOption(key="lower", label="lowercase", is_default=(info["dominant"] == "lower")),
                UserOption(key="upper", label="UPPERCASE", is_default=(info["dominant"] == "upper")),
                UserOption(key="title", label="Title Case", is_default=(info["dominant"] == "title")),
                UserOption(key="skip", label="Skip"),
            ],
            metadata=info,
        )

    # ── 2. Whitespace check ───────────────────────────────────────────
    def _check_whitespace(self, col: str, series: pd.Series) -> Optional[dict]:
        leading = int(series.str.match(r"^\s").sum())
        trailing = int(series.str.match(r".*\s$").sum())
        multi_space = int(series.str.contains(r"\s{2,}", regex=True).sum())
        total = leading + trailing + multi_space

        if total < 2:
            return None

        return {
            "column": col,
            "leading": leading,
            "trailing": trailing,
            "multi_space": multi_space,
            "total_affected": total,
        }

    def _build_whitespace_action(self, col: str, info: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATA_STANDARDIZATION,
            action_type=ActionType.STANDARDIZE_WHITESPACE,
            confidence=ActionConfidence.DEFINITIVE,
            evidence=(
                f"Column '{col}': {info['leading']} values with leading whitespace, "
                f"{info['trailing']} with trailing, {info['multi_space']} with multiple internal spaces."
            ),
            recommendation=f"Strip and normalize whitespace in '{col}'.",
            reasoning="Whitespace inconsistencies cause silent matching failures and duplicate categories.",
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                description=f"Fixes {info['total_affected']} whitespace issues.",
            ),
        )

    # ── 3. Unicode check ──────────────────────────────────────────────
    def _check_unicode(self, col: str, series: pd.Series) -> Optional[dict]:
        issues = 0
        fullwidth = 0
        lookalike = 0

        for val in series.head(200):
            s = str(val)
            nfc = unicodedata.normalize("NFC", s)
            if s != nfc:
                issues += 1

            # Check for full-width characters (common in CJK)
            for ch in s:
                cp = ord(ch)
                if 0xFF01 <= cp <= 0xFF5E:  # Full-width ASCII
                    fullwidth += 1
                    break

            # Simple lookalike check (Cyrillic/Latin)
            has_latin = any("LATIN" in unicodedata.name(c, "") for c in s if c.isalpha())
            has_cyrillic = any("CYRILLIC" in unicodedata.name(c, "") for c in s if c.isalpha())
            if has_latin and has_cyrillic:
                lookalike += 1

        total = issues + fullwidth + lookalike
        if total < 1:
            return None

        return {
            "column": col,
            "nfc_issues": issues,
            "fullwidth_chars": fullwidth,
            "lookalike_chars": lookalike,
        }

    def _build_unicode_action(self, col: str, info: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATA_STANDARDIZATION,
            action_type=ActionType.NORMALIZE_UNICODE,
            confidence=ActionConfidence.DEFINITIVE,
            evidence=(
                f"Column '{col}': {info['nfc_issues']} NFC normalization issues, "
                f"{info['fullwidth_chars']} full-width characters, "
                f"{info['lookalike_chars']} mixed-script lookalikes."
            ),
            recommendation=f"Apply Unicode NFC normalization to '{col}'.",
            reasoning="Unicode inconsistencies cause invisible string mismatches.",
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                description="Normalizes Unicode representations.",
            ),
        )

    # ── 4. Synonym detection ──────────────────────────────────────────
    def _detect_synonyms(self, col: str, series: pd.Series) -> list[dict]:
        unique_vals = series.unique()
        if len(unique_vals) > 200 or len(unique_vals) < 3:
            return []

        # Normalize for comparison
        clusters: list[dict] = []
        seen: set[str] = set()
        val_list = [str(v) for v in unique_vals]

        for i, a in enumerate(val_list):
            if a.lower() in seen:
                continue
            group = [a]
            a_norm = re.sub(r"[^a-z0-9]", "", a.lower())

            for j, b in enumerate(val_list):
                if i == j or b.lower() in seen:
                    continue
                b_norm = re.sub(r"[^a-z0-9]", "", b.lower())

                # Exact match after normalization
                if a_norm == b_norm and a != b:
                    group.append(b)
                    continue

                # Common abbreviation patterns
                if self._is_abbreviation(a, b):
                    group.append(b)

            if len(group) >= 2:
                for v in group:
                    seen.add(v.lower())
                counts = {v: int(series[series == v].count()) for v in group if v in series.values}
                canonical = max(counts, key=counts.get) if counts else group[0]
                clusters.append({
                    "column": col,
                    "variants": group,
                    "canonical": canonical,
                    "counts": counts,
                })

        return clusters

    @staticmethod
    def _is_abbreviation(a: str, b: str) -> bool:
        """Check if one string is a known abbreviation of the other."""
        # Common abbreviation pairs
        pairs = {
            ("usa", "us"), ("united states", "us"), ("united states of america", "usa"),
            ("uk", "united kingdom"), ("gb", "great britain"),
            ("ca", "california"), ("ny", "new york"),
            ("mr", "mister"), ("mrs", "missus"), ("dr", "doctor"),
            ("jan", "january"), ("feb", "february"), ("mar", "march"),
        }
        al, bl = a.lower().strip("."), b.lower().strip(".")
        return (al, bl) in pairs or (bl, al) in pairs

    def _build_synonym_action(self, col: str, clusters: list[dict]) -> CleaningAction:
        total_variants = sum(len(c["variants"]) for c in clusters)
        preview = "; ".join(
            f"{c['variants']} → '{c['canonical']}'" for c in clusters[:3]
        )

        return CleaningAction(
            category=ActionCategory.DATA_STANDARDIZATION,
            action_type=ActionType.CONSOLIDATE_SYNONYMS,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Column '{col}': detected {len(clusters)} synonym clusters "
                f"with {total_variants} total variants. Examples: {preview}"
            ),
            recommendation="Consolidate synonym variants into canonical forms.",
            reasoning="Multiple representations of the same entity inflate cardinality and reduce model accuracy.",
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                description=f"Consolidates {total_variants} variants into {len(clusters)} canonical forms.",
            ),
            options=[
                UserOption(key="auto", label="Auto-consolidate (most frequent)", is_default=True),
                UserOption(key="review", label="Review each cluster"),
                UserOption(key="skip", label="Skip"),
            ],
            metadata={"clusters": clusters},
        )

    # ── 5. Phone check ────────────────────────────────────────────────
    def _check_phone(self, col: str, series: pd.Series) -> Optional[dict]:
        formats: Counter = Counter()
        for val in series.head(100):
            s = str(val).strip()
            if re.match(r"^\+\d{1,3}\d{7,14}$", re.sub(r"[\s\-\(\)]", "", s)):
                formats["e164"] += 1
            elif re.match(r"^\(\d{3}\)\s?\d{3}[-.]?\d{4}$", s):
                formats["us_parens"] += 1
            elif re.match(r"^\d{3}[-.]?\d{3}[-.]?\d{4}$", s):
                formats["us_dashed"] += 1
            elif re.match(r"^\+", s):
                formats["intl"] += 1
            else:
                formats["other"] += 1

        if len(formats) <= 1:
            return None

        return {"column": col, "formats": dict(formats)}

    def _build_phone_action(self, col: str, info: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATA_STANDARDIZATION,
            action_type=ActionType.STANDARDIZE_PHONE,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=f"Column '{col}': inconsistent phone formats: {info['formats']}.",
            recommendation=f"Standardize phone numbers in '{col}' to E.164 format.",
            reasoning="Consistent phone formats enable matching, deduplication, and validation.",
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                description="Normalizes phone numbers.",
            ),
            options=[
                UserOption(key="e164", label="E.164 Format (+1234567890)", is_default=True),
                UserOption(key="national", label="National Format ((123) 456-7890)"),
                UserOption(key="skip", label="Skip"),
            ],
        )

    # ── 6. Email check ────────────────────────────────────────────────
    def _check_email(self, col: str, series: pd.Series) -> Optional[dict]:
        has_upper = int(series.str.contains(r"[A-Z]", regex=True).sum())
        has_whitespace = int(series.str.contains(r"\s", regex=True).sum())
        invalid = int((~series.str.contains(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", regex=True, na=False)).sum())

        disposable_domains = {"mailinator.com", "tempmail.com", "guerrillamail.com",
                              "throwaway.email", "10minutemail.com", "yopmail.com"}
        disposable_count = 0
        for val in series:
            s = str(val).lower().strip()
            domain = s.split("@")[-1] if "@" in s else ""
            if domain in disposable_domains:
                disposable_count += 1

        total_issues = has_upper + has_whitespace + invalid + disposable_count
        if total_issues < 2:
            return None

        return {
            "column": col,
            "uppercase_emails": has_upper,
            "whitespace_emails": has_whitespace,
            "invalid_format": invalid,
            "disposable_domains": disposable_count,
        }

    def _build_email_action(self, col: str, info: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATA_STANDARDIZATION,
            action_type=ActionType.STANDARDIZE_EMAIL,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Column '{col}': {info['uppercase_emails']} with uppercase, "
                f"{info['whitespace_emails']} with whitespace, {info['invalid_format']} invalid, "
                f"{info['disposable_domains']} disposable domains."
            ),
            recommendation=f"Standardize emails in '{col}' (lowercase, strip, validate).",
            reasoning="Email standardization prevents duplicate accounts and improves data quality.",
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                description="Normalizes email addresses.",
            ),
            options=[
                UserOption(key="full", label="Full Standardization", is_default=True),
                UserOption(key="lowercase_only", label="Lowercase Only"),
                UserOption(key="flag_disposable", label="Flag Disposable Domains"),
                UserOption(key="skip", label="Skip"),
            ],
            metadata=info,
        )

    # ── 7. URL check ──────────────────────────────────────────────────
    def _check_url(self, col: str, series: pd.Series) -> Optional[dict]:
        has_http = int(series.str.startswith("http://", na=False).sum())
        has_https = int(series.str.startswith("https://", na=False).sum())
        trailing_slash = int(series.str.endswith("/", na=False).sum())
        has_utm = int(series.str.contains(r"utm_", regex=True, na=False).sum())
        has_encoding = int(series.str.contains(r"%[0-9a-fA-F]{2}", regex=True, na=False).sum())

        total = has_http + trailing_slash + has_utm + has_encoding
        if total < 2:
            return None

        return {
            "column": col,
            "http_count": has_http,
            "https_count": has_https,
            "trailing_slashes": trailing_slash,
            "utm_parameters": has_utm,
            "percent_encoded": has_encoding,
        }

    def _build_url_action(self, col: str, info: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATA_STANDARDIZATION,
            action_type=ActionType.STANDARDIZE_URL,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Column '{col}': {info['http_count']} http://, {info['https_count']} https://, "
                f"{info['trailing_slashes']} trailing slashes, {info['utm_parameters']} with UTM params."
            ),
            recommendation=f"Standardize URLs in '{col}'.",
            reasoning="URL normalization enables matching and deduplication of web addresses.",
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                description="Normalizes URLs.",
            ),
            options=[
                UserOption(key="full", label="Full (https + strip slash + decode)", is_default=True),
                UserOption(key="strip_utm", label="Also Remove UTM Parameters"),
                UserOption(key="skip", label="Skip"),
            ],
        )

    # ── 8. Currency format check ──────────────────────────────────────
    def _check_currency_format(self, col: str, series: pd.Series) -> Optional[dict]:
        symbols: Counter = Counter()
        for val in series.head(200):
            s = str(val).strip()
            if re.match(r"^[\$€£¥₹₽₩]", s):
                symbols[s[0]] += 1
            elif re.search(r"[A-Z]{3}$", s):  # ISO code at end
                match = re.search(r"([A-Z]{3})$", s)
                if match:
                    symbols[match.group(1)] += 1

        if len(symbols) <= 1:
            return None

        return {"column": col, "symbols": dict(symbols)}

    def _build_currency_format_action(self, col: str, info: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATA_STANDARDIZATION,
            action_type=ActionType.STANDARDIZE_CURRENCY_FORMAT,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=f"Column '{col}': multiple currency symbols detected: {info['symbols']}.",
            recommendation=f"Separate or convert mixed currencies in '{col}'.",
            reasoning="Mixed currencies in a single column prevent numeric analysis.",
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                description="Flags multi-currency values for separation or conversion.",
            ),
            options=[
                UserOption(key="separate", label="Separate into Currency + Amount columns", is_default=True),
                UserOption(key="flag", label="Flag Rows with Different Currency"),
                UserOption(key="skip", label="Skip"),
            ],
            metadata=info,
        )

    # ── 9. Precision check ────────────────────────────────────────────
    def _check_precision(self, col: str) -> Optional[dict]:
        series = self.df[col].dropna()
        if not pd.api.types.is_float_dtype(series):
            return None

        # Count decimal places
        decimals: Counter = Counter()
        for val in series.head(200):
            s = f"{val:.20f}".rstrip("0")
            if "." in s:
                dec = len(s.split(".")[1])
            else:
                dec = 0
            decimals[dec] += 1

        if len(decimals) <= 1:
            return None

        # Find most common precision
        dominant = decimals.most_common(1)[0]
        inconsistent = sum(v for k, v in decimals.items() if k != dominant[0])
        if inconsistent < 3:
            return None

        return {
            "column": col,
            "decimal_counts": dict(decimals),
            "dominant_precision": dominant[0],
            "inconsistent_count": inconsistent,
        }

    def _build_precision_action(self, col: str, info: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATA_STANDARDIZATION,
            action_type=ActionType.STANDARDIZE_PRECISION,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Column '{col}': inconsistent float precision: {info['decimal_counts']}. "
                f"Dominant precision: {info['dominant_precision']} decimal places."
            ),
            recommendation=f"Round '{col}' to {info['dominant_precision']} decimal places.",
            reasoning="Inconsistent precision can affect comparisons and aggregations.",
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                description=f"Rounds {info['inconsistent_count']} values.",
            ),
            options=[
                UserOption(key=str(info["dominant_precision"]), label=f"Round to {info['dominant_precision']} decimals", is_default=True),
                UserOption(key="2", label="Round to 2 decimals"),
                UserOption(key="4", label="Round to 4 decimals"),
                UserOption(key="skip", label="Skip"),
            ],
            metadata=info,
        )

    # ── 10. Unit inconsistency ────────────────────────────────────────
    def _check_unit_inconsistency(self, col: str) -> Optional[dict]:
        series = self.df[col].dropna()
        if len(series) < 20:
            return None

        # Check for bimodal distribution suggesting mixed units
        try:
            from scipy.stats import gaussian_kde
            kde = gaussian_kde(series.values)
            x_range = np.linspace(float(series.min()), float(series.max()), 200)
            density = kde(x_range)

            peaks = []
            for i in range(1, len(density) - 1):
                if density[i] > density[i - 1] and density[i] > density[i + 1]:
                    peaks.append({"x": float(x_range[i]), "density": float(density[i])})

            if len(peaks) < 2:
                return None

            # Check if peak ratio suggests unit conversion (common ratios)
            common_ratios = {
                2.2046: "kg/lbs", 0.4536: "lbs/kg",
                2.54: "inches/cm", 0.3937: "cm/inches",
                1.6093: "miles/km", 0.6214: "km/miles",
                33.814: "liters/oz", 0.02957: "oz/liters",
                1.8: "celsius/fahrenheit",
            }

            ratio = peaks[0]["x"] / peaks[1]["x"] if peaks[1]["x"] != 0 else 0
            inv_ratio = peaks[1]["x"] / peaks[0]["x"] if peaks[0]["x"] != 0 else 0
            detected_unit = None

            for r, label in common_ratios.items():
                if 0.9 < ratio / r < 1.1 or 0.9 < inv_ratio / r < 1.1:
                    detected_unit = label
                    break

            return {
                "column": col,
                "n_peaks": len(peaks),
                "peak_positions": [p["x"] for p in peaks[:3]],
                "suspected_units": detected_unit,
                "ratio": round(ratio, 4),
            }

        except (ImportError, Exception):
            return None

    def _build_unit_action(self, col: str, info: dict) -> CleaningAction:
        unit_text = f" (suspected: {info['suspected_units']})" if info["suspected_units"] else ""
        return CleaningAction(
            category=ActionCategory.DATA_STANDARDIZATION,
            action_type=ActionType.FIX_UNIT_INCONSISTENCY,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Column '{col}': bimodal distribution with {info['n_peaks']} peaks "
                f"at {info['peak_positions']}{unit_text}."
            ),
            recommendation=f"Investigate potential unit inconsistency in '{col}'.",
            reasoning="Bimodal distribution in a numeric column may indicate mixed units.",
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                description="May require manual review of unit conversion.",
            ),
            options=[
                UserOption(key="flag", label="Flag Rows for Review", is_default=True),
                UserOption(key="skip", label="Skip"),
            ],
            metadata=info,
        )

    # ── Static execution methods ──────────────────────────────────────

    @staticmethod
    def apply_casing(df: pd.DataFrame, col: str, mode: str) -> pd.DataFrame:
        if col not in df.columns:
            return df
        if mode == "lower":
            df[col] = df[col].astype(str).str.lower()
        elif mode == "upper":
            df[col] = df[col].astype(str).str.upper()
        elif mode == "title":
            df[col] = df[col].astype(str).str.title()
        return df

    @staticmethod
    def apply_whitespace(df: pd.DataFrame, col: str) -> pd.DataFrame:
        if col not in df.columns:
            return df
        df[col] = df[col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
        return df

    @staticmethod
    def apply_unicode(df: pd.DataFrame, col: str) -> pd.DataFrame:
        if col not in df.columns:
            return df
        df[col] = df[col].apply(
            lambda v: unicodedata.normalize("NFC", str(v)) if pd.notna(v) else v
        )
        return df

    @staticmethod
    def apply_synonyms(df: pd.DataFrame, col: str, clusters: list[dict]) -> pd.DataFrame:
        if col not in df.columns:
            return df
        mapping = {}
        for cluster in clusters:
            canonical = cluster["canonical"]
            for variant in cluster["variants"]:
                if variant != canonical:
                    mapping[variant] = canonical
        df[col] = df[col].replace(mapping)
        return df

    @staticmethod
    def apply_email_standardize(df: pd.DataFrame, col: str) -> pd.DataFrame:
        if col not in df.columns:
            return df
        df[col] = df[col].astype(str).str.strip().str.lower()
        return df

    @staticmethod
    def apply_precision(df: pd.DataFrame, col: str, decimals: int) -> pd.DataFrame:
        if col not in df.columns:
            return df
        df[col] = df[col].round(decimals)
        return df
