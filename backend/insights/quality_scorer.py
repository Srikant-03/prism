from typing import Dict, Any, List
import pandas as pd
from profiling.profiling_models import DatasetProfile
from insights.insight_models import DataQualityScore

class QualityScorer:
    """
    Computes a 0-100 data quality score based on 5 dimensions:
    Completeness, Uniqueness, Validity, Consistency, Timeliness.
    """

    @staticmethod
    def calculate_scores(profile: DatasetProfile) -> DataQualityScore:
        total_rows = profile.total_rows
        if total_rows == 0:
            return DataQualityScore(
                completeness=0, uniqueness=0, validity=0, consistency=0, timeliness=0,
                overall_score=0, grade="F"
            )

        # 1. Completeness: Inverse of overall null percentage
        total_cells = total_rows * profile.total_columns
        total_nulls = sum(col.null_count for col in profile.columns)
        completeness = max(0.0, 100.0 * (1.0 - (total_nulls / total_cells))) if total_cells > 0 else 0.0

        # 2. Uniqueness: Average unique count vs row count ratio across ID/Categorical columns
        uniqueness_scores = []
        for col in profile.columns:
            if col.semantic_type in ['id_key', 'categorical_nominal', 'email', 'phone']:
                unique_ratio = col.distinct_count / total_rows if total_rows > 0 else 0
                uniqueness_scores.append(unique_ratio * 100)
        
        uniqueness = sum(uniqueness_scores) / len(uniqueness_scores) if uniqueness_scores else 100.0
        
        # 3. Validity: Penalize for implausible dates, formatting issues
        validity_penalties = 0
        for col in profile.columns:
            if col.datetime:
                validity_penalties += min(5, (col.datetime.implausible_dates_count / max(1, col.non_null_count)) * 100)
            if col.numeric and len(col.numeric.formatting_issues) > 0:
                validity_penalties += 2
            if col.text and col.text.has_pii_risk:
                validity_penalties += 5
                
        validity = max(0.0, 100.0 - validity_penalties)

        # 4. Consistency: Penalize for mixed types, varied case, whitespace issues
        consistency_penalties = 0
        for col in profile.columns:
            if col.categorical:
                consistency_penalties += len(col.categorical.case_inconsistencies) * 2
                consistency_penalties += len(col.categorical.whitespace_issues) * 1
            if col.datetime and col.datetime.mixed_formats:
                consistency_penalties += 10
        consistency = max(0.0, 100.0 - consistency_penalties)

        # 5. Timeliness: (Optional) If there are date columns, check gaps and recency
        timeliness = None
        datetime_cols = [c for c in profile.columns if c.datetime]
        if datetime_cols:
            timeliness_scores = []
            for col in datetime_cols:
                gap_penalty = min(20, col.datetime.gap_count * 2)
                timeliness_scores.append(max(0.0, 100.0 - gap_penalty))
            timeliness = sum(timeliness_scores) / len(timeliness_scores)

        # Weighted Average
        weights = {'completeness': 0.35, 'uniqueness': 0.15, 'validity': 0.25, 'consistency': 0.25}
        overall_score = (
            (completeness * weights['completeness']) +
            (uniqueness * weights['uniqueness']) +
            (validity * weights['validity']) +
            (consistency * weights['consistency'])
        )

        # Grade Assignment
        if overall_score >= 90:
            grade = "A"
        elif overall_score >= 80:
            grade = "B"
        elif overall_score >= 70:
            grade = "C"
        elif overall_score >= 60:
            grade = "D"
        else:
            grade = "F"

        return DataQualityScore(
            completeness=round(completeness, 2),
            uniqueness=round(uniqueness, 2),
            validity=round(validity, 2),
            consistency=round(consistency, 2),
            timeliness=round(timeliness, 2) if timeliness is not None else None,
            overall_score=round(overall_score, 2),
            grade=grade
        )
