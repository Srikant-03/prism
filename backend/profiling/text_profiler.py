"""
Free-text column profiler.
Analyses text length, language detection, named entity patterns,
HTML/Markdown contamination, and PII risk assessment.
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any

import pandas as pd
import numpy as np

from profiling.profiling_models import TextProfile


# PII Detection Patterns
PII_PATTERNS = {
    "email": re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"),
    "phone": re.compile(r"(?:\+?\d{1,3}[\s\-]?)?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{4}"),
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "credit_card": re.compile(r"\b(?:\d{4}[\s\-]?){3}\d{4}\b"),
    "ip_address": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
    "us_zip": re.compile(r"\b\d{5}(?:-\d{4})?\b"),
}

# Common first/last names and name-like column identifiers
NAME_HINTS = {"name", "first_name", "last_name", "full_name", "firstname", "lastname",
              "fname", "lname", "author", "user", "username", "customer_name"}

# Language character range patterns
LANGUAGE_PATTERNS = {
    "English": re.compile(r"[a-zA-Z]"),
    "Chinese": re.compile(r"[\u4e00-\u9fff]"),
    "Japanese": re.compile(r"[\u3040-\u309f\u30a0-\u30ff]"),
    "Korean": re.compile(r"[\uac00-\ud7af]"),
    "Arabic": re.compile(r"[\u0600-\u06ff]"),
    "Cyrillic": re.compile(r"[\u0400-\u04ff]"),
    "Devanagari": re.compile(r"[\u0900-\u097f]"),
    "Thai": re.compile(r"[\u0e00-\u0e7f]"),
    "Latin Extended": re.compile(r"[\u00c0-\u024f]"),
}


class TextProfiler:
    """Deep profiling for free-text columns."""

    @staticmethod
    def profile(series: pd.Series, column_name: str = "") -> TextProfile:
        profile = TextProfile()
        non_null = series.dropna()
        if len(non_null) == 0:
            return profile

        str_series = non_null.astype(str)
        n = len(str_series)

        # ── Length Stats ──
        lengths = str_series.str.len()
        profile.avg_length = float(lengths.mean())
        profile.min_length = int(lengths.min())
        profile.max_length = int(lengths.max())

        # Token count
        token_counts = str_series.str.split().str.len()
        profile.avg_token_count = float(token_counts.mean())

        # ── Language Detection ──
        profile.detected_language, profile.language_confidence = (
            TextProfiler._detect_language(str_series)
        )

        # ── Entity Type Detection ──
        profile.entity_types_found = TextProfiler._detect_entities(str_series)

        # ── HTML / Markdown Contamination ──
        sample = str_series.head(2000)
        html_count = sample.str.contains(r"<[a-zA-Z][^>]*>", regex=True, na=False).sum()
        if html_count > len(sample) * 0.05:
            profile.html_contamination = True

        md_patterns = r"(\*\*|__|#+\s|```|\[.*\]\(.*\)|!\[)"
        md_count = sample.str.contains(md_patterns, regex=True, na=False).sum()
        if md_count > len(sample) * 0.05:
            profile.markdown_contamination = True

        # ── PII Risk Assessment ──
        profile.pii_risks, profile.has_pii_risk = TextProfiler._assess_pii(
            str_series, column_name
        )

        return profile

    @staticmethod
    def _detect_language(series: pd.Series) -> tuple[str, float]:
        """Detect dominant language using character n-gram analysis."""
        # Concatenate a sample of text
        sample_text = " ".join(series.head(500).tolist())
        if not sample_text.strip():
            return "unknown", 0.0

        total_chars = len(sample_text)
        lang_scores: dict[str, int] = {}

        for lang, pattern in LANGUAGE_PATTERNS.items():
            matches = len(pattern.findall(sample_text))
            if matches > 0:
                lang_scores[lang] = matches

        if not lang_scores:
            return "unknown", 0.0

        # Find dominant language
        dominant = max(lang_scores, key=lang_scores.get)  # type: ignore
        confidence = lang_scores[dominant] / total_chars

        return dominant, min(confidence * 2, 0.99)  # Scale up confidence

    @staticmethod
    def _detect_entities(series: pd.Series) -> list[str]:
        """Detect common named entity types embedded in text."""
        entities: list[str] = []
        sample = series.head(1000)
        sample_text = " ".join(sample.tolist())

        # Date mentions
        date_pattern = re.compile(
            r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2},?\s*\d{4}|\b\d{1,2}/\d{1,2}/\d{2,4}\b"
        )
        if len(date_pattern.findall(sample_text)) > 5:
            entities.append("dates")

        # Currency amounts
        currency_pattern = re.compile(r"[\$€£¥₹]\s?\d+(?:[,\.]\d+)*")
        if len(currency_pattern.findall(sample_text)) > 3:
            entities.append("currency_amounts")

        # Location-like patterns (title case multi-word)
        location_pattern = re.compile(r"\b[A-Z][a-z]+(?:\s[A-Z][a-z]+)+\b")
        locations = location_pattern.findall(sample_text)
        if len(locations) > 10:
            entities.append("possible_locations_or_names")

        # Email in text
        emails = PII_PATTERNS["email"].findall(sample_text)
        if len(emails) > 2:
            entities.append("emails")

        # Phone in text
        phones = PII_PATTERNS["phone"].findall(sample_text)
        if len(phones) > 2:
            entities.append("phone_numbers")

        return entities

    @staticmethod
    def _assess_pii(
        series: pd.Series, column_name: str
    ) -> tuple[list[dict[str, Any]], bool]:
        """Assess PII risk across the column."""
        risks: list[dict[str, Any]] = []
        sample = series.head(3000)
        col_lower = column_name.lower().strip()

        # Check column name for PII hints
        if col_lower in NAME_HINTS or any(h in col_lower for h in ["name", "person"]):
            risks.append({
                "type": "personal_name",
                "confidence": "medium",
                "evidence": f"Column name '{column_name}' suggests personal names",
                "recommendation": "Consider pseudonymization",
            })

        # Pattern-based PII scanning
        for pii_type, pattern in PII_PATTERNS.items():
            matches = sample.str.contains(pattern, na=False)
            match_count = int(matches.sum())

            if match_count > 0:
                sample_matches = sample[matches].head(3).tolist()
                # Mask samples for safety
                masked = [TextProfiler._mask_pii(str(v)) for v in sample_matches]

                risks.append({
                    "type": pii_type,
                    "count": match_count,
                    "percentage": round(match_count / len(sample) * 100, 2),
                    "confidence": "high" if match_count / len(sample) > 0.1 else "medium",
                    "masked_examples": masked,
                    "recommendation": f"Column may contain {pii_type.replace('_', ' ')}. Review for data governance.",
                })

        has_risk = len(risks) > 0
        return risks, has_risk

    @staticmethod
    def _mask_pii(text: str) -> str:
        """Partially mask PII values for safe display."""
        if len(text) <= 4:
            return "****"
        return text[:2] + "*" * (len(text) - 4) + text[-2:]
