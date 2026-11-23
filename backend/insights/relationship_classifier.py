"""
insights/relationship_classifier.py
====================================
Classifies the *nature* of a high-association pair of features, distinguishing
five semantically distinct relationship types so that downstream anomaly
detection, leakage handlers, and reporting components can communicate the
correct severity and recommendation to the user.

The five types
--------------
1. DERIVED_FEATURE          — column B is mathematically / deterministically
                              derived from column A (e.g. month_name ← date).
                              Severity: LOW.  Category: "Feature Redundancy".
2. DUPLICATE_FEATURE        — the two columns carry identical or near-identical
                              information with no clear derivation direction.
                              Severity: MEDIUM. Category: "Redundant Feature".
3. TARGET_LEAKAGE           — a feature directly encodes the target or is a
                              mathematical proxy of it.
                              Severity: CRITICAL. Category: "Data Leakage".
4. FUTURE_INFORMATION       — a feature encodes data that would not be available
                              at inference time (post-event timestamps, outcomes).
                              Severity: HIGH. Category: "Future Information Leakage".
5. ORDINARY_ASSOCIATION     — both columns are legitimate inputs that happen to
                              correlate strongly; no leakage risk.
                              Severity: INFO. Category: "Strong Association".

Usage
-----
    from insights.relationship_classifier import RelationshipClassifier, RelationshipType

    rc = RelationshipClassifier.classify_pair(
        col1="month_name", col2="date",
        df=df,
        col1_semantic="categorical_nominal",
        col2_semantic="datetime",
        target_column="revenue",
        score=1.0,
    )
    print(rc.type)          # RelationshipType.DERIVED_FEATURE
    print(rc.severity)      # "Low"
    print(rc.description)   # "month_name is deterministically derived from date. ..."
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Relationship taxonomy
# ─────────────────────────────────────────────────────────────────────────────

class RelationshipType(str, Enum):
    DERIVED_FEATURE      = "derived_feature"
    DUPLICATE_FEATURE    = "duplicate_feature"
    FUNCTIONAL_DEPENDENCY = "functional_dependency"
    MATHEMATICAL_DEPENDENCY = "mathematical_dependency"
    TARGET_LEAKAGE       = "target_leakage"
    FUTURE_INFORMATION   = "future_information"
    ORDINARY_ASSOCIATION = "ordinary_association"


# Anomaly severity strings — mirror AnomalySeverity constants so callers can
# use the output directly without importing insight_models.
_SEV_CRITICAL = "Critical"
_SEV_HIGH     = "High"
_SEV_MEDIUM   = "Medium"
_SEV_LOW      = "Low"
_SEV_INFO     = "Informational"


@dataclass
class RelationshipClassification:
    type: RelationshipType
    severity: str
    category: str
    col1: str
    col2: str
    score: float
    description: str
    recommendation: str
    derivation_direction: Optional[str] = None   # "col1_from_col2" | "col2_from_col1" | None
    operands: Optional[list[str]] = None
    formula_expr: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# Name-pattern helpers
# ─────────────────────────────────────────────────────────────────────────────

# Canonical date-derivation components.  A column whose *normalised name*
# matches one of these is presumed derivable from any datetime column.
_DATE_DERIVED_NAMES: frozenset[str] = frozenset({
    "year", "month", "quarter", "week", "weeknum", "week_number",
    "weekday", "day", "dayofweek", "day_of_week", "dayofyear", "day_of_year",
    "hour", "minute", "second",
    "month_name", "month_num", "monthname",
    "quarter_name", "quarter_num",
    "hour_bucket", "hour_band",
    "season", "fiscal_year", "fiscal_quarter",
    "date_part",
})

# Prefixes / suffixes that indicate a column is derived from a date
_DATE_DERIVED_AFFIXES = re.compile(
    r"(^|_)(year|month|quarter|week|day|hour|season|fiscal)($|_)",
    re.IGNORECASE,
)

# Semantic types that mark a column as a datetime source
_DATETIME_SEMANTICS: frozenset[str] = frozenset({
    "datetime", "duration", "date",
})

# Future-information patterns (match on column name)
_FUTURE_PATTERNS = [
    re.compile(r"(?i)(outcome|result|disposition|resolution|final|conclusion)"),
    re.compile(r"(?i)(follow[-_]?up|followup|next[-_]?step|response)"),
    re.compile(r"(?i)(cancel|churn|return|refund|complaint)[-_]?(date|time|at|ts)"),
    re.compile(r"(?i)(close|closed|resolved|completed|finished)[-_]?(date|time|at|ts)"),
    re.compile(r"(?i)(end[-_]?date|end[-_]?time|expiry|expiration)"),
]


def _normalise(name: str) -> str:
    """Lowercase, strip leading/trailing whitespace, collapse separators."""
    return re.sub(r"[\s\-]+", "_", name.strip().lower())


def _is_date_derived_name(col: str) -> bool:
    """Return True if col's normalised name is a recognised date component."""
    n = _normalise(col)
    return n in _DATE_DERIVED_NAMES or bool(_DATE_DERIVED_AFFIXES.search(n))


def _is_datetime_semantic(semantic: Any) -> bool:
    if not semantic:
        return False
    if hasattr(semantic, "value"):
        semantic = semantic.value
    s = str(semantic).lower().replace("-", "_")
    if "." in s:
        s = s.split(".")[-1]
    return s in _DATETIME_SEMANTICS


def _looks_like_date_col(col: str, df: Optional[pd.DataFrame] = None) -> bool:
    """Return True if column name or contents look like a primary datetime column."""
    n = _normalise(col)
    if n in ("date", "datetime", "timestamp", "ts", "time", "created_at", "updated_at"):
        return True
    if n.endswith("_date") or n.endswith("_datetime") or n.endswith("_timestamp") or n.startswith("date_"):
        return True
    if df is not None and col in df.columns:
        try:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                return True
        except Exception:
            pass
    return False


def _is_future_name(col: str) -> bool:
    for pat in _FUTURE_PATTERNS:
        if pat.search(col):
            return True
    return False


def _columns_are_identical(s1: pd.Series, s2: pd.Series) -> bool:
    """Return True if two series contain exactly the same values (in order)."""
    try:
        if len(s1) != len(s2):
            return False
        eq = s1.reset_index(drop=True).equals(s2.reset_index(drop=True))
        return bool(eq)
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Classifier
# ─────────────────────────────────────────────────────────────────────────────

class RelationshipClassifier:
    """
    Classifies the semantic nature of a high-association feature pair.

    Design principles
    -----------------
    * A near-perfect association score alone is NOT sufficient evidence of
      data leakage.  The classifier requires additional structural evidence.
    * Date-derived columns (month_name, year, quarter, …) are ALWAYS
      classified as DERIVED_FEATURE when paired with a datetime column,
      regardless of the association score.
    * Target leakage is only raised when one of the two columns IS the known
      target column AND the other column has a score above a high threshold.
    """

    @staticmethod
    def classify_pair(
        col1: str,
        col2: str,
        df: Optional[pd.DataFrame] = None,
        col1_semantic: Optional[str] = None,
        col2_semantic: Optional[str] = None,
        target_column: Optional[str] = None,
        score: float = 1.0,
        metric: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        business_rules: Optional[list[str] | dict[str, Any]] = None,
        prediction_timestamp: Optional[Any] = None,
        feature_timestamps: Optional[dict[str, Any]] = None,
        target_timestamp: Optional[Any] = None,
    ) -> RelationshipClassification:
        """
        Classify the relationship between col1 and col2.

        Parameters
        ----------
        col1, col2      : column names
        df              : the actual DataFrame (used for value-level checks);
                          may be None — name-pattern checks still apply
        col1_semantic   : SemanticType value string for col1
        col2_semantic   : SemanticType value string for col2
        target_column   : the known target column (if any); used for leakage
        score           : the raw association score (correlation / MI / purity)
        metric          : the statistical metric name (e.g. 'Deterministic Derivation')

        Returns
        -------
        RelationshipClassification
        """

        # ── Guard ────────────────────────────────────────────────────────────
        if col1 == col2:
            return RelationshipClassification(
                type=RelationshipType.DUPLICATE_FEATURE,
                severity=_SEV_MEDIUM,
                category="Redundant Feature",
                col1=col1, col2=col2, score=score,
                description=f"'{col1}' and '{col2}' are the same column.",
                recommendation="Remove the duplicate column.",
            )

        # Explicit metric overrides
        if metric == "Deterministic Derivation":
            return _derived_result(source=col1, derived=col2, score=score)
        if metric in ("Functional Dependency", "Deterministic Redundancy"):
            return _functional_dependency_result(determinant=col1, dependent=col2, score=score)
        if metric in ("Deterministic Formula", "Mathematical Dependency"):
            return RelationshipClassification(
                type=RelationshipType.MATHEMATICAL_DEPENDENCY,
                severity=_SEV_LOW,
                category="Deterministic Redundancy",
                col1=col1, col2=col2, score=score,
                description=f"`{col1}` is deterministically derived from `{col2}`.",
                recommendation="Keeping both may introduce redundant information and exact multicollinearity.",
            )
        if metric in ("Temporal Association (Grouped)", "Temporal Trend"):
            return RelationshipClassification(
                type=RelationshipType.ORDINARY_ASSOCIATION,
                severity=_SEV_INFO,
                category="Strong Association",
                col1=col1, col2=col2, score=score,
                description=f"Temporal association ({metric}, score={score:.2f}) between '{col1}' and '{col2}'.",
                recommendation="Monitor temporal distribution and trend dynamics.",
            )

        # ── 1. Future-information leakage ─────────────────────────────────
        is_conf1, ev1 = _verify_future_confirmed(
            col1, metadata, business_rules, prediction_timestamp, feature_timestamps, target_timestamp
        )
        is_conf2, ev2 = _verify_future_confirmed(
            col2, metadata, business_rules, prediction_timestamp, feature_timestamps, target_timestamp
        )

        if is_conf1:
            return _future_result(col1, col2, score, is_confirmed=True, evidence_detail=ev1)
        if is_conf2:
            return _future_result(col2, col1, score, is_confirmed=True, evidence_detail=ev2)

        if _is_future_name(col1):
            return _future_result(col1, col2, score, is_confirmed=False)
        if _is_future_name(col2):
            return _future_result(col2, col1, score, is_confirmed=False)

        # ── 2. Target leakage ────────────────────────────────────────────
        if target_column:
            if col1 == target_column and score >= 0.95:
                return _target_leakage_result(col2, col1, score)
            if col2 == target_column and score >= 0.95:
                return _target_leakage_result(col1, col2, score)

        # ── 3. Deterministic date-derivation ─────────────────────────────
        #
        # Rule: if one column is a datetime source AND the other is a known
        # date-component name (year, month_name, quarter, …), classify as
        # DERIVED_FEATURE regardless of score.
        c1_is_dt = _is_datetime_semantic(col1_semantic) or _looks_like_date_col(col1, df)
        c2_is_dt = _is_datetime_semantic(col2_semantic) or _looks_like_date_col(col2, df)
        c1_is_derived = _is_date_derived_name(col1)
        c2_is_derived = _is_date_derived_name(col2)

        if c1_is_dt and c2_is_derived:
            return _derived_result(source=col1, derived=col2, score=score)
        if c2_is_dt and c1_is_derived:
            return _derived_result(source=col2, derived=col1, score=score)

        # Both columns are date-component names → mutual derivation from a
        # shared (possibly absent) datetime source.
        if c1_is_derived and c2_is_derived:
            return RelationshipClassification(
                type=RelationshipType.DERIVED_FEATURE,
                severity=_SEV_LOW,
                category="Feature Redundancy",
                col1=col1, col2=col2, score=score,
                description=(
                    f"'{col1}' and '{col2}' are both deterministically derived from the same "
                    f"datetime source. This is feature redundancy, not data leakage."
                ),
                recommendation=(
                    f"Keep at most one of '{col1}'/'{col2}' or encode them as a single "
                    f"cyclic/ordinal feature."
                ),
                derivation_direction="mutual",
            )

        # ── 4. Value-level checks with df ────────────────────────────────
        if df is not None and col1 in df.columns and col2 in df.columns:
            if _columns_are_identical(df[col1], df[col2]):
                return RelationshipClassification(
                    type=RelationshipType.DUPLICATE_FEATURE,
                    severity=_SEV_MEDIUM,
                    category="Redundant Feature",
                    col1=col1, col2=col2, score=score,
                    description=(
                        f"'{col1}' and '{col2}' contain identical values. "
                        f"One is an exact duplicate of the other."
                    ),
                    recommendation=f"Drop one of '{col1}' / '{col2}' to eliminate the redundancy.",
                )

            # Check for functional dependency (1-to-1 or many-to-1)
            try:
                from profiling.functional_dependency import detect_functional_dependency
                fd = detect_functional_dependency(df, col1, col2)
                if fd:
                    return _functional_dependency_result(
                        determinant=fd.determinant,
                        dependent=fd.dependent,
                        score=score,
                        is_bijective=fd.is_bijective,
                    )
            except Exception:
                pass

            # Features have distinct values in the dataframe without functional dependency
            return RelationshipClassification(
                type=RelationshipType.ORDINARY_ASSOCIATION,
                severity=_SEV_INFO,
                category="Strong Association",
                col1=col1, col2=col2, score=score,
                description=(
                    f"Strong statistical association (score={score:.2f}) between "
                    f"'{col1}' and '{col2}'. Both features appear to be legitimate inputs."
                ),
                recommendation=(
                    f"Monitor for multicollinearity. Consider VIF analysis if both features "
                    f"are included in a linear model."
                ),
            )

        # ── 5. Exact duplicate by name or perfect score without df ──────
        if _normalise(col1) == _normalise(col2):
            return RelationshipClassification(
                type=RelationshipType.DUPLICATE_FEATURE,
                severity=_SEV_MEDIUM,
                category="Redundant Feature",
                col1=col1, col2=col2, score=score,
                description=(
                    f"'{col1}' and '{col2}' share the same normalized name. "
                    f"One column appears redundant, but there is no evidence of data leakage."
                ),
                recommendation="Keep only one instance of this column.",
            )

        if score >= 0.9999:
            return _functional_dependency_result(
                determinant=col1,
                dependent=col2,
                score=score,
            )

        # ── 6. Ordinary strong association ───────────────────────────────
        return RelationshipClassification(
            type=RelationshipType.ORDINARY_ASSOCIATION,
            severity=_SEV_INFO,
            category="Strong Association",
            col1=col1, col2=col2, score=score,
            description=(
                f"Strong association (score={score:.2f}) between "
                f"'{col1}' and '{col2}'. Both features appear to be legitimate inputs."
            ),
            recommendation=(
                f"Monitor for multicollinearity. Consider VIF analysis if both features "
                f"are included in a linear model."
            ),
        )

    @staticmethod
    def classify_formula(
        formula: Any,
        target_column: Optional[str] = None,
    ) -> RelationshipClassification:
        """
        Classify a deterministic formula dependency.

        Interpretation:
        - If target_column == formula.dependent:
          Direct target leakage: using operands to predict the target.
        - If target_column != formula.dependent:
          Deterministic redundancy / mathematical dependency. NOT data leakage.
        """
        dependent = getattr(formula, "dependent", str(formula))
        operands = getattr(formula, "operands", [])
        op1 = operands[0] if len(operands) > 0 else "feature_1"
        op2 = operands[1] if len(operands) > 1 else "feature_2"
        formula_expr = getattr(formula, "formula_expr", f"{dependent} = f({op1}, {op2})")
        score = getattr(formula, "match_ratio", 1.0)

        is_target = bool(target_column and dependent == target_column)

        if is_target:
            return RelationshipClassification(
                type=RelationshipType.TARGET_LEAKAGE,
                severity=_SEV_CRITICAL,
                category="Data Leakage",
                col1=dependent,
                col2=op1,
                score=score,
                description=(
                    f"`{dependent}` is deterministically derived from `{op1}` and `{op2}`. "
                    f"Using `{op1}` and `{op2}` to predict `{dependent}` should be classified as direct target leakage."
                ),
                recommendation=(
                    f"Using `{op1}` and `{op2}` to predict `{dependent}` is direct target leakage because the target "
                    f"is deterministically derived from these features. Drop one or both predictor features."
                ),
                derivation_direction=f"{dependent}_from_{op1}_{op2}",
                operands=operands,
                formula_expr=formula_expr,
            )
        else:
            return RelationshipClassification(
                type=RelationshipType.MATHEMATICAL_DEPENDENCY,
                severity=_SEV_LOW,
                category="Deterministic Redundancy",
                col1=dependent,
                col2=op1,
                score=score,
                description=f"`{dependent}` is deterministically derived from `{op1}` and `{op2}`.",
                recommendation=(
                    f"Keeping all three may introduce redundant information and exact multicollinearity. "
                    f"Consider dropping `{dependent}` (or one of its components) to eliminate exact mathematical dependency."
                ),
                derivation_direction=f"{dependent}_from_{op1}_{op2}",
                operands=operands,
                formula_expr=formula_expr,
            )



# ─────────────────────────────────────────────────────────────────────────────
# Private result builders
# ─────────────────────────────────────────────────────────────────────────────

def _functional_dependency_result(
    determinant: str,
    dependent: str,
    score: float,
    is_bijective: bool = True,
) -> RelationshipClassification:
    return RelationshipClassification(
        type=RelationshipType.FUNCTIONAL_DEPENDENCY,
        severity=_SEV_LOW,
        category="Deterministic Redundancy",
        col1=determinant,
        col2=dependent,
        score=score,
        description=(
            f"`{dependent}` is deterministically determined by `{determinant}` in this dataset. "
            f"Keeping both may introduce redundant information."
        ),
        recommendation=(
            f"Consider dropping `{dependent}` (or `{determinant}`) to eliminate redundant "
            f"information and reduce dimensionality."
        ),
        derivation_direction=f"{dependent}_from_{determinant}",
    )

def _derived_result(source: str, derived: str, score: float) -> RelationshipClassification:
    return RelationshipClassification(
        type=RelationshipType.DERIVED_FEATURE,
        severity=_SEV_LOW,
        category="Feature Redundancy",
        col1=derived, col2=source, score=score,
        description=(
            f"'{derived}' is deterministically derived from '{source}'. "
            f"This is feature redundancy, not data leakage."
        ),
        recommendation=(
            f"Consider dropping '{derived}' if '{source}' already encodes the same "
            f"information, or if the model can extract temporal components directly."
        ),
        derivation_direction="col1_from_col2",
    )


def _target_leakage_result(leaking_col: str, target: str, score: float) -> RelationshipClassification:
    return RelationshipClassification(
        type=RelationshipType.TARGET_LEAKAGE,
        severity=_SEV_CRITICAL,
        category="Data Leakage",
        col1=leaking_col, col2=target, score=score,
        description=(
            f"'{leaking_col}' has a near-perfect association (score={score:.2f}) with "
            f"the target '{target}'. This is a strong indicator of target leakage — "
            f"the feature may encode the target or be computed after the event."
        ),
        recommendation=(
            f"Remove '{leaking_col}' immediately. Verify whether it is computed before "
            f"or after '{target}' is determined. If it is post-hoc, it must be excluded."
        ),
    )


def _verify_future_confirmed(
    col: str,
    metadata: Optional[dict[str, Any]] = None,
    business_rules: Optional[list[str] | dict[str, Any]] = None,
    prediction_timestamp: Optional[Any] = None,
    feature_timestamps: Optional[dict[str, Any]] = None,
    target_timestamp: Optional[Any] = None,
) -> tuple[bool, Optional[str]]:
    if isinstance(metadata, dict):
        col_meta = metadata.get(col, {})
        if isinstance(col_meta, dict):
            if col_meta.get("is_post_prediction") is True:
                return True, "explicit metadata declares column as post-prediction"
            if col_meta.get("available_at_inference") is False:
                return True, "explicit metadata declares column not available at inference"
            if col_meta.get("timing") in ("future", "post_event", "post_prediction"):
                return True, f"metadata timing marked as '{col_meta.get('timing')}'"

    if isinstance(business_rules, list):
        for rule in business_rules:
            r_str = str(rule).lower()
            if col.lower() in r_str and any(k in r_str for k in ("post-prediction", "future leakage", "after prediction", "after target", "not available at inference")):
                return True, f"documented business rule: '{rule}'"
    elif isinstance(business_rules, dict):
        rule_val = business_rules.get(col)
        if rule_val and any(k in str(rule_val).lower() for k in ("post-prediction", "future", "leakage", "after")):
            return True, f"documented business rule for '{col}': {rule_val}"

    if prediction_timestamp is not None and feature_timestamps and col in feature_timestamps:
        try:
            p_dt = pd.to_datetime(prediction_timestamp)
            f_dt = pd.to_datetime(feature_timestamps[col])
            if f_dt > p_dt:
                return True, f"feature timestamp ({f_dt}) occurs after prediction timestamp ({p_dt})"
        except Exception:
            pass

    if target_timestamp is not None and feature_timestamps and col in feature_timestamps:
        try:
            t_dt = pd.to_datetime(target_timestamp)
            f_dt = pd.to_datetime(feature_timestamps[col])
            if f_dt > t_dt:
                return True, f"feature timestamp ({f_dt}) occurs after target timestamp ({t_dt})"
        except Exception:
            pass

    return False, None


def _future_result(
    future_col: str,
    other_col: str,
    score: float,
    is_confirmed: bool = False,
    evidence_detail: Optional[str] = None,
) -> RelationshipClassification:
    if is_confirmed:
        return RelationshipClassification(
            type=RelationshipType.FUTURE_INFORMATION,
            severity=_SEV_HIGH,
            category="Confirmed Future Leakage",
            col1=future_col, col2=other_col, score=score,
            description=(
                f"'{future_col}' is confirmed to occur after the prediction event "
                f"({evidence_detail or 'verified by timestamps/business rules'}). "
                f"This information will not be available at prediction time."
            ),
            recommendation=(
                f"Remove '{future_col}' from the feature set before training. "
                f"Confirmed post-prediction leakage."
            ),
        )

    return RelationshipClassification(
        type=RelationshipType.FUTURE_INFORMATION,
        severity=_SEV_MEDIUM,
        category="Conditional Future Leakage Warning",
        col1=future_col, col2=other_col, score=score,
        description=(
            f"'{future_col}' may cause target/future leakage if this value is only known after the "
            f"prediction timestamp. Confirm feature availability at inference time."
        ),
        recommendation=(
            f"'{future_col}' may cause target/future leakage if this value is only known after the "
            f"prediction timestamp. Confirm feature availability at inference time."
        ),
    )





# ─────────────────────────────────────────────────────────────────────────────
# Convenience helper used by leakage_handler and anomaly_detector
# ─────────────────────────────────────────────────────────────────────────────

def is_date_derived_pair(
    col1: str,
    col2: str,
    col1_semantic: Optional[str] = None,
    col2_semantic: Optional[str] = None,
    df: Optional[pd.DataFrame] = None,
) -> bool:
    """
    Return True if either col1 or col2 is a date-derived component of the other.

    This is used as a fast guard in leakage_handler and anomaly_detector
    without constructing a full RelationshipClassification.
    """
    c1_dt = _is_datetime_semantic(col1_semantic) or _looks_like_date_col(col1, df)
    c2_dt = _is_datetime_semantic(col2_semantic) or _looks_like_date_col(col2, df)
    c1_derived = _is_date_derived_name(col1)
    c2_derived = _is_date_derived_name(col2)

    return (
        (c1_dt and c2_derived)
        or (c2_dt and c1_derived)
        or (c1_derived and c2_derived)
    )
