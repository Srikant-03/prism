"""
Dataset domain detector.
Infers the likely domain/subject of a dataset using column name semantics
and value patterns. Never asserts as fact — always states with confidence.
"""

from __future__ import annotations

from collections import Counter
from typing import Optional

import pandas as pd


# Domain signal patterns: (domain, column name keywords, value keywords)
DOMAIN_SIGNALS = [
    {
        "domain": "E-commerce / Retail Transaction",
        "col_keywords": ["product", "price", "order", "cart", "sku", "quantity", "discount",
                         "shipping", "customer", "invoice", "item", "purchase", "payment"],
        "value_keywords": ["shipped", "delivered", "pending", "refund", "credit card"],
        "weight": 1.0,
    },
    {
        "domain": "Financial / Banking",
        "col_keywords": ["account", "balance", "transaction", "debit", "credit", "interest",
                         "loan", "mortgage", "deposit", "withdrawal", "portfolio", "stock",
                         "dividend", "forex", "exchange_rate"],
        "value_keywords": ["savings", "checking", "wire transfer", "ach"],
        "weight": 1.0,
    },
    {
        "domain": "Healthcare / Medical",
        "col_keywords": ["patient", "diagnosis", "symptom", "medication", "dosage", "hospital",
                         "doctor", "icd", "blood", "heart_rate", "bmi", "cholesterol",
                         "prescription", "treatment", "vitals"],
        "value_keywords": ["mg", "tablet", "injection", "outpatient", "inpatient"],
        "weight": 1.0,
    },
    {
        "domain": "Human Resources / Employee",
        "col_keywords": ["employee", "salary", "department", "hire_date", "position", "manager",
                         "performance", "leave", "attendance", "bonus", "designation",
                         "experience", "qualification"],
        "value_keywords": ["full-time", "part-time", "contract", "terminated", "active"],
        "weight": 1.0,
    },
    {
        "domain": "Marketing / Customer Analytics",
        "col_keywords": ["campaign", "click", "impression", "conversion", "bounce", "session",
                         "referrer", "utm", "channel", "engagement", "ctr", "cpc", "roi",
                         "lead", "funnel"],
        "value_keywords": ["organic", "paid", "social", "email campaign"],
        "weight": 1.0,
    },
    {
        "domain": "Logistics / Supply Chain",
        "col_keywords": ["warehouse", "shipment", "tracking", "carrier", "freight", "inventory",
                         "supply", "vendor", "supplier", "delivery", "route", "fleet"],
        "value_keywords": ["in transit", "delivered", "out for delivery"],
        "weight": 1.0,
    },
    {
        "domain": "Education / Academic",
        "col_keywords": ["student", "grade", "course", "enrollment", "gpa", "semester",
                         "professor", "faculty", "exam", "score", "assignment", "class"],
        "value_keywords": ["freshman", "sophomore", "junior", "senior", "graduate"],
        "weight": 1.0,
    },
    {
        "domain": "IoT / Sensor Data",
        "col_keywords": ["sensor", "device_id", "temperature", "humidity", "pressure",
                         "acceleration", "voltage", "current", "signal", "beacon",
                         "reading", "measurement"],
        "value_keywords": [],
        "weight": 0.9,
    },
    {
        "domain": "Social Media / User Activity",
        "col_keywords": ["user", "post", "like", "comment", "share", "follower", "following",
                         "tweet", "hashtag", "mention", "profile", "feed", "story"],
        "value_keywords": [],
        "weight": 0.9,
    },
    {
        "domain": "Genomics / Bioinformatics",
        "col_keywords": ["gene", "chromosome", "sequence", "mutation", "allele", "genome",
                         "protein", "dna", "rna", "variant", "snp", "expression"],
        "value_keywords": ["A", "T", "G", "C"],
        "weight": 1.0,
    },
    {
        "domain": "Real Estate / Property",
        "col_keywords": ["property", "bedroom", "bathroom", "sqft", "square_feet", "lot_size",
                         "listing", "rent", "mortgage", "appraisal", "zoning", "address"],
        "value_keywords": ["residential", "commercial", "condo", "townhouse"],
        "weight": 1.0,
    },
    {
        "domain": "Survey / Questionnaire",
        "col_keywords": ["respondent", "response", "answer", "question", "survey", "rating",
                         "satisfaction", "feedback", "likert", "agree", "disagree"],
        "value_keywords": ["strongly agree", "agree", "neutral", "disagree", "strongly disagree"],
        "weight": 1.0,
    },
]


class DomainDetector:
    """
    Infers the likely domain of a dataset.
    Uses column name matching and value pattern analysis.
    Always expressed with confidence — never asserted as fact.
    """

    @staticmethod
    def detect(df: pd.DataFrame) -> tuple[str, float, str]:
        """
        Detect the likely domain of the dataset.

        Returns:
            (domain_description, confidence, justification)
        """
        if df.empty:
            return "Unknown", 0.0, "Dataset is empty — cannot infer domain."

        col_names = [c.lower().replace(" ", "_") for c in df.columns]
        col_set = set(col_names)

        # Score each domain
        scores: list[tuple[str, float, list[str]]] = []

        for signal in DOMAIN_SIGNALS:
            col_matches = []
            for keyword in signal["col_keywords"]:
                for col in col_names:
                    if keyword in col:
                        col_matches.append(f"'{col}' matches '{keyword}'")
                        break

            # Value-based matching (sample first 1000 rows)
            value_matches = []
            if signal["value_keywords"]:
                sample_text = ""
                for col in df.select_dtypes(include=["object"]).columns:
                    sample_text += " ".join(df[col].dropna().head(200).astype(str).tolist()) + " "
                sample_lower = sample_text.lower()

                for vk in signal["value_keywords"]:
                    if vk.lower() in sample_lower:
                        value_matches.append(f"value '{vk}' found in data")

            total_signals = len(signal["col_keywords"]) + len(signal["value_keywords"])
            matched = len(col_matches) + len(value_matches)

            if matched > 0:
                score = (matched / total_signals) * signal["weight"]
                evidence = col_matches + value_matches
                scores.append((signal["domain"], score, evidence))

        if not scores:
            return (
                "General / Mixed-Purpose Dataset",
                0.2,
                "No strong domain signals detected from column names or value patterns. "
                "This dataset may be general-purpose or from an uncommon domain.",
            )

        # Sort by score descending
        scores.sort(key=lambda x: x[1], reverse=True)
        best_domain, best_score, best_evidence = scores[0]

        # Calibrate confidence
        confidence = min(best_score * 1.5, 0.95)

        # Check if second-best is close (ambiguous domain)
        if len(scores) > 1 and scores[1][1] > best_score * 0.7:
            second_domain = scores[1][0]
            confidence *= 0.8
            justification = (
                f"This appears to be a {best_domain} dataset "
                f"(confidence: {confidence:.0%}). Evidence: "
                + "; ".join(best_evidence[:5])
                + f". Note: could also be {second_domain}."
            )
        else:
            justification = (
                f"This appears to be a {best_domain} dataset "
                f"(confidence: {confidence:.0%}). Evidence: "
                + "; ".join(best_evidence[:5])
                + "."
            )

        return best_domain, round(confidence, 3), justification
