from typing import List
from profiling.profiling_models import DatasetProfile
from insights.insight_models import AnomalyWarning, AnomalySeverity
from insights.relationship_classifier import RelationshipClassifier, RelationshipType

class AnomalyDetector:
    """
    Scans a DatasetProfile to generate a categorized, severity-ranked list of business/data anomalies.
    """

    @staticmethod
    def detect(profile: DatasetProfile) -> List[AnomalyWarning]:
        warnings: List[AnomalyWarning] = []

        # 1. Dataset Level Anomalies
        if profile.total_rows == 0:
            warnings.append(AnomalyWarning(
                severity=AnomalySeverity.CRITICAL,
                category="Structural",
                description="The dataset contains zero rows.",
                recommendation="Verify ingestion source and file integrity."
            ))
            return warnings

        # 2. Column Nullability
        for col in profile.columns:
            if col.null_percentage > 95:
                warnings.append(AnomalyWarning(
                    feature=col.name,
                    severity=AnomalySeverity.HIGH,
                    category="Missingness",
                    description=f"Column is {col.null_percentage:.1f}% null.",
                    recommendation="Consider dropping this feature as it provides minimal variance."
                ))
            elif col.null_percentage > 50:
                warnings.append(AnomalyWarning(
                    feature=col.name,
                    severity=AnomalySeverity.MEDIUM,
                    category="Missingness",
                    description=f"Column is {col.null_percentage:.1f}% null.",
                    recommendation="Imputation may introduce significant bias. Proceed with caution."
                ))

        # 3. Categorical Issues (Constant, Imbalance, Inconsistencies)
        for col in profile.columns:
            if col.distinct_count == 1 and col.non_null_count > 0:
                warnings.append(AnomalyWarning(
                    feature=col.name,
                    severity=AnomalySeverity.MEDIUM,
                    category="Variance",
                    description="Column contains only a single constant value.",
                    recommendation="Drop this column as it carries zero information gain."
                ))
            
            if col.categorical:
                inconsistencies = col.categorical.case_inconsistencies or []
                if len(inconsistencies) > 0:
                    warnings.append(AnomalyWarning(
                        feature=col.name,
                        severity=AnomalySeverity.LOW,
                        category="Formatting",
                        description=f"Found {len(inconsistencies)} casing inconsistencies (e.g. 'Apple' vs 'apple').",
                        recommendation="Apply a lowercase/uppercase transform to standardize."
                    ))

        # 4. Numeric Outliers
        for col in profile.columns:
            if col.numeric:
                outliers = col.numeric.box_outliers or []
                if len(outliers) > (profile.total_rows * 0.05):
                    pct = (len(outliers) / profile.total_rows) * 100 if profile.total_rows > 0 else 0
                    warnings.append(AnomalyWarning(
                        feature=col.name,
                        severity=AnomalySeverity.INFO,
                        category="Distribution",
                        description=f"{pct:.1f}% of records are statistical outliers ({len(outliers)} observations). Statistically unusual observation; manual review recommended.",
                        recommendation="Review extreme values against business domain context before applying robust scaling or winsorization."
                    ))

        # 5. Cross-Column Anomalies
        if profile.cross_analysis:
            cross = profile.cross_analysis
            correlations = cross.get("correlations", {})
            target_analysis = cross.get("target", {})
            
            # Multicollinearity
            if correlations.get("multicollinearity", {}).get("has_multicollinearity"):
                warnings.append(AnomalyWarning(
                    severity=AnomalySeverity.HIGH,
                    category="Correlation",
                    description="High Multicollinearity detected among numerical features.",
                    recommendation="Apply PCA or drop redundant columns (check Matrix for VIF scores)."
                ))
            
            # High Association Analysis (Classified via RelationshipClassifier)
            target_col = target_analysis.get("target_column") if target_analysis else None
            semantic_map = {col.name: getattr(col, "semantic_type", None) for col in profile.columns}

            for pair in correlations.get("strongest_pairs", []):
                score = pair.get("score", 0.0)
                col1 = pair.get("col1")
                col2 = pair.get("col2")
                if col1 is not None and col2 is not None and abs(score) >= 0.99 and col1 != col2:
                    c1_s = semantic_map.get(col1)
                    c2_s = semantic_map.get(col2)
                    col1_semantic = c1_s.value if hasattr(c1_s, "value") else str(c1_s or "")
                    col2_semantic = c2_s.value if hasattr(c2_s, "value") else str(c2_s or "")
                    cls = RelationshipClassifier.classify_pair(
                        col1=col1,
                        col2=col2,
                        col1_semantic=col1_semantic,
                        col2_semantic=col2_semantic,
                        target_column=target_col,
                        score=score,
                        metric=pair.get("metric"),
                    )
                    warn_feature = cls.col2 if getattr(cls, "type", None) == RelationshipType.FUNCTIONAL_DEPENDENCY else col1
                    warnings.append(AnomalyWarning(
                        feature=warn_feature,
                        severity=cls.severity,
                        category=cls.category,
                        description=cls.description,
                        recommendation=cls.recommendation
                    ))

            # Deterministic Formula / Mathematical Dependency Analysis
            det_formulas = correlations.get("deterministic_formulas", [])
            for formula_dict in det_formulas:
                dep = formula_dict.get("dependent")
                operands = formula_dict.get("operands", [])
                op1 = operands[0] if len(operands) > 0 else "feature_1"
                op2 = operands[1] if len(operands) > 1 else "feature_2"
                is_target_leakage = bool(target_col and dep == target_col)
                if is_target_leakage:
                    warnings.append(AnomalyWarning(
                        feature=dep,
                        severity=AnomalySeverity.CRITICAL,
                        category="Data Leakage",
                        description=(
                            f"`{dep}` is deterministically derived from `{op1}` and `{op2}`. "
                            f"Using `{op1}` and `{op2}` to predict `{dep}` should be classified as direct target leakage."
                        ),
                        recommendation=(
                            f"Do not use `{op1}` and `{op2}` together to predict `{dep}` as they deterministically "
                            f"reconstruct the target. Drop one or both predictor features."
                        )
                    ))
                else:
                    warnings.append(AnomalyWarning(
                        feature=dep,
                        severity=AnomalySeverity.LOW,
                        category="Deterministic Redundancy",
                        description=f"`{dep}` is deterministically derived from `{op1}` and `{op2}`.",
                        recommendation=(
                            f"Keeping all three may introduce redundant information and multicollinearity. "
                            f"Consider dropping `{dep}` (or one of its components) for linear models."
                        )
                    ))

            # Target Imbalance
            problem_type = target_analysis.get("problem_type")
            class_dist = target_analysis.get("class_distribution", {})
            if problem_type == 'binary_classification' and class_dist:
                dist = list(class_dist.values())
                if len(dist) == 2 and (min(dist) < 0.1):
                    warnings.append(AnomalyWarning(
                        feature=target_analysis.get("target_column"),
                        severity=AnomalySeverity.HIGH,
                        category="Target Imbalance",
                        description=f"Highly imbalanced target distribution ({min(dist)*100:.1f}% vs {max(dist)*100:.1f}%).",
                        recommendation="Use SMOTE, class weighting, or stratified sampling."
                    ))
                    
        # 6. PII Risk
        for col in profile.columns:
            if col.text and col.text.has_pii_risk:
                risks = col.text.pii_risks or []
                for risk in risks:
                    warnings.append(AnomalyWarning(
                        feature=col.name,
                        severity=AnomalySeverity.CRITICAL,
                        category="Security",
                        description=f"Detected potential {risk['type']} PII footprint.",
                        recommendation="Anonymize, hash, or drop this column before downstream storage."
                    ))

        # Sort by severity
        severity_map = {
            AnomalySeverity.CRITICAL: 0,
            AnomalySeverity.HIGH: 1,
            AnomalySeverity.MEDIUM: 2,
            AnomalySeverity.LOW: 3,
            AnomalySeverity.INFO: 4
        }
        warnings.sort(key=lambda w: severity_map.get(w.severity, 5))
        return warnings
