import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from scipy import stats
from sklearn.feature_selection import mutual_info_regression, mutual_info_classif
from statsmodels.stats.outliers_influence import variance_inflation_factor

from profiling.cross_column_models import (
    CorrelationAnalysis,
    CorrelationPair,
    MulticollinearityReport
)
from profiling.profiling_models import DatasetProfile

logger = logging.getLogger(__name__)


def _to_clean_num(series: pd.Series) -> pd.Series:
    """
    Robustly convert a series to numeric, stripping currency symbols,
    ISO currency codes (e.g. USD, EUR, GBP), commas, and percent signs.
    """
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors='coerce')
    clean_s = series.astype(str).str.strip()
    clean_s = clean_s.str.replace(r'^[A-Z]{3}\s*', '', regex=True)
    clean_s = clean_s.str.replace(r'[$€£¥,]', '', regex=True)
    clean_s = clean_s.str.replace(r'%', '', regex=True)
    clean_s = clean_s.str.strip()
    return pd.to_numeric(clean_s, errors='coerce')


class CorrelationAnalyzer:
    """
    Computes pair-wise correlations, associations, and multicollinearity
    across columns in a dataset.
    """

    def __init__(self, top_k: int = 50, vif_threshold: float = 5.0):
        self.top_k = top_k
        self.vif_threshold = vif_threshold

    def analyze(self, df: pd.DataFrame, dataset_profile: Optional[DatasetProfile] = None) -> CorrelationAnalysis:
        try:
            # 1. Detect datetime columns before any categorical partitioning
            datetime_cols = self._detect_datetime_columns(df, dataset_profile)
            dt_set = set(datetime_cols)

            # 2. Gather remaining column types
            numeric_cols = []
            categorical_cols = []
            binary_cols = []

            for col in df.columns:
                if col in dt_set:
                    continue
                s_clean = _to_clean_num(df[col])
                non_null_ratio = s_clean.notna().sum() / len(df) if len(df) > 0 else 0
                if non_null_ratio > 0.4 and s_clean.nunique() > 1:
                    numeric_cols.append(col)
                elif df[col].nunique() == 2 or pd.api.types.is_bool_dtype(df[col]):
                    binary_cols.append(col)
                elif df[col].nunique() > 1:
                    # Categorical: must NOT be high-cardinality near-unique ID
                    n_unique = df[col].nunique()
                    unique_ratio = n_unique / len(df) if len(df) > 0 else 0
                    if n_unique <= 50 and unique_ratio <= 0.4:
                        categorical_cols.append(col)

            all_features = list(dict.fromkeys(numeric_cols + categorical_cols + binary_cols + datetime_cols))
            all_pairs = []
            matrix = {c: {c2: (1.0 if c == c2 else 0.0) for c2 in all_features} for c in all_features}

            # 3. Numeric - Numeric: Pearson & Spearman
            for i in range(len(numeric_cols)):
                col1 = numeric_cols[i]
                s1 = _to_clean_num(df[col1])
                valid1 = s1.notna()

                for j in range(i + 1, len(numeric_cols)):
                    col2 = numeric_cols[j]
                    s2 = _to_clean_num(df[col2])
                    valid = valid1 & s2.notna()
                    v1, v2 = s1[valid], s2[valid]

                    if len(v1) > 2 and v1.nunique() > 1 and v2.nunique() > 1:
                        # Pearson
                        r, p_val = stats.pearsonr(v1, v2)
                        if not np.isnan(r) and not np.isinf(r):
                            r_f = float(r)
                            p_f = float(p_val) if not np.isnan(p_val) else 1.0
                            matrix[col1][col2] = r_f
                            matrix[col2][col1] = r_f
                            all_pairs.append(CorrelationPair(
                                col1=col1, col2=col2, score=r_f, p_value=p_f,
                                metric="Pearson", is_significant=(p_f < 0.05)
                            ))
                        
                        # Spearman for rank
                        rho, p_val_s = stats.spearmanr(v1, v2)
                        if not np.isnan(rho) and not np.isinf(rho):
                            rho_f = float(rho)
                            p_s_f = float(p_val_s) if not np.isnan(p_val_s) else 1.0
                            all_pairs.append(CorrelationPair(
                                col1=col1, col2=col2, score=rho_f, p_value=p_s_f,
                                metric="Spearman", is_significant=(p_s_f < 0.05)
                            ))

            # 4. Categorical - Categorical: Bias-corrected Cramér's V (raw datetimes excluded)
            from profiling.functional_dependency import detect_functional_dependency
            for i in range(len(categorical_cols)):
                col1 = categorical_cols[i]
                for j in range(i + 1, len(categorical_cols)):
                    col2 = categorical_cols[j]
                    fd = detect_functional_dependency(df, col1, col2)
                    if fd:
                        matrix[col1][col2] = 1.0
                        matrix[col2][col1] = 1.0
                        all_pairs.append(CorrelationPair(
                            col1=fd.determinant,
                            col2=fd.dependent,
                            score=1.0,
                            metric="Functional Dependency",
                            is_significant=True,
                        ))
                    else:
                        c_v = self._cramers_v(df[col1], df[col2])
                        if c_v is not None:
                            matrix[col1][col2] = c_v
                            matrix[col2][col1] = c_v
                            all_pairs.append(CorrelationPair(
                                col1=col1, col2=col2, score=c_v, metric="Cramér's V",
                                is_significant=(c_v > 0.25)
                            ))

            # 5. Datetime Analysis: Deterministic Derivations, Grouped Temporal Associations & Numeric Trends
            for dt_col in datetime_cols:
                dt_pairs = self._analyze_datetime_relationships(
                    df=df,
                    dt_col=dt_col,
                    other_cols=list(df.columns),
                    categorical_cols=categorical_cols,
                    numeric_cols=numeric_cols,
                )
                for p in dt_pairs:
                    matrix[p.col1][p.col2] = p.score
                    matrix[p.col2][p.col1] = p.score
                    all_pairs.append(p)

            # 6. Numeric - Categorical: Eta-squared approximation / Point-biserial
            for num_col in numeric_cols:
                for cat_col in categorical_cols:
                    eta_sq = self._eta_squared(df[num_col], df[cat_col])
                    if eta_sq is not None:
                        matrix[num_col][cat_col] = eta_sq
                        matrix[cat_col][num_col] = eta_sq
                        all_pairs.append(CorrelationPair(
                            col1=num_col, col2=cat_col, score=eta_sq, metric="Eta-squared"
                        ))
                
                for bin_col in binary_cols:
                    pb, p_val = self._point_biserial(df[num_col], df[bin_col])
                    if pb is not None:
                        matrix[num_col][bin_col] = pb
                        matrix[bin_col][num_col] = pb
                        all_pairs.append(CorrelationPair(
                            col1=num_col, col2=bin_col, score=pb, p_value=p_val,
                            metric="Point-Biserial", is_significant=(p_val < 0.05) if p_val else False
                        ))

            # 7. Mutual Information (Numeric & Categorical combined sample)
            try:
                mi_dict = self._compute_mutual_information(df, numeric_cols, categorical_cols, binary_cols)
            except Exception:
                mi_dict = {}

            # 8. Multicollinearity (VIF)
            try:
                vif_report = self._compute_vif(df, numeric_cols)
            except Exception:
                vif_report = MulticollinearityReport(has_multicollinearity=False, vif_scores={}, warnings=[])

            # 8b. Deterministic Formula Dependencies
            det_formulas = []
            try:
                from profiling.formula_detector import detect_formula_dependencies
                target_col = None
                if dataset_profile and hasattr(dataset_profile, "cross_analysis"):
                    ca = getattr(dataset_profile, "cross_analysis", None)
                    if isinstance(ca, dict):
                        target_col = ca.get("target", {}).get("target_column")
                found_formulas = detect_formula_dependencies(df, columns=numeric_cols, target_column=target_col)
                for f in found_formulas:
                    det_formulas.append({
                        "dependent": f.dependent,
                        "operands": f.operands,
                        "operation": f.operation,
                        "formula_expr": f.formula_expr,
                        "description": f.description,
                        "match_ratio": f.match_ratio,
                    })
            except Exception:
                det_formulas = []

            # 9. Sort and select top pairs
            best_pairs = {}
            for p in all_pairs:
                key = tuple(sorted([p.col1, p.col2]))
                if key not in best_pairs or abs(p.score) > abs(best_pairs[key].score):
                    best_pairs[key] = p

            sorted_pairs = sorted(best_pairs.values(), key=lambda x: abs(x.score), reverse=True)
            top_pairs = sorted_pairs[:self.top_k]

            return CorrelationAnalysis(
                correlation_matrix=matrix,
                strongest_pairs=top_pairs,
                multicollinearity=vif_report,
                mutual_information=mi_dict,
                deterministic_formulas=det_formulas,
            )
        except Exception as e:
            # Generate fallback matrix from present columns instead of returning empty
            fallback_cols = list(df.columns[:10])
            fallback_matrix = {c: {c2: (1.0 if c == c2 else 0.0) for c2 in fallback_cols} for c in fallback_cols}
            return CorrelationAnalysis(
                correlation_matrix=fallback_matrix,
                strongest_pairs=[],
                multicollinearity=MulticollinearityReport(has_multicollinearity=False, vif_scores={}, warnings=[]),
                mutual_information={}
            )

    @staticmethod
    def _detect_datetime_columns(df: pd.DataFrame, dataset_profile: Optional[DatasetProfile] = None) -> list[str]:
        datetime_cols = set()

        # 1. From dataset_profile metadata if available
        if dataset_profile:
            if getattr(dataset_profile, "temporal_columns", None):
                datetime_cols.update(dataset_profile.temporal_columns)
            if getattr(dataset_profile, "inferred_schema", None):
                for col, sem in dataset_profile.inferred_schema.items():
                    if str(sem).lower() in ("datetime", "date", "duration"):
                        datetime_cols.add(col)
            if getattr(dataset_profile, "columns", None):
                for c in dataset_profile.columns:
                    sem = getattr(c, "semantic_type", None)
                    sem_str = sem.value if hasattr(sem, "value") else str(sem or "")
                    if sem_str.lower() in ("datetime", "date", "duration"):
                        datetime_cols.add(c.name)

        # 2. Inspect DataFrame series directly
        for col in df.columns:
            if col in datetime_cols:
                continue
            s = df[col]
            if pd.api.types.is_datetime64_any_dtype(s) or pd.api.types.is_timedelta64_dtype(s):
                datetime_cols.add(col)
                continue

            col_lower = str(col).lower().strip()
            date_hints = any(h in col_lower for h in ["date", "time", "timestamp", "dob", "created_at", "updated_at"])
            if date_hints and (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
                non_null = s.dropna()
                if len(non_null) > 0:
                    sample = non_null.head(min(len(non_null), 200))
                    try:
                        parsed = pd.to_datetime(sample, errors="coerce")
                        if parsed.notna().mean() > 0.7:
                            datetime_cols.add(col)
                    except Exception:
                        pass

        return [c for c in df.columns if c in datetime_cols]

    def _analyze_datetime_relationships(
        self,
        df: pd.DataFrame,
        dt_col: str,
        other_cols: List[str],
        categorical_cols: List[str],
        numeric_cols: List[str],
    ) -> List[CorrelationPair]:
        pairs: List[CorrelationPair] = []
        try:
            s_dt = pd.to_datetime(df[dt_col], errors='coerce')
        except Exception:
            return pairs

        valid_dt = s_dt.notna()
        if valid_dt.sum() < 3:
            return pairs

        # 1. Deterministic calendar derivations
        derived_cols = set()
        for col in other_cols:
            if col == dt_col:
                continue
            target_s = df[col]
            is_derived = False

            col_lower = str(col).lower().strip()
            if any(k in col_lower for k in ["month", "quarter", "year", "weekday", "day"]):
                try:
                    if "month_name" in col_lower or col_lower == "monthname":
                        match_ratio = (s_dt.dt.month_name() == target_s).mean()
                        if match_ratio > 0.90:
                            is_derived = True
                    elif "quarter" in col_lower:
                        q_str = "Q" + s_dt.dt.quarter.astype(str)
                        match_ratio = max(
                            (q_str == target_s.astype(str)).mean(),
                            (s_dt.dt.quarter == target_s).mean()
                        )
                        if match_ratio > 0.90:
                            is_derived = True
                    elif col_lower == "year" or col_lower.endswith("_year"):
                        match_ratio = (s_dt.dt.year == target_s).mean()
                        if match_ratio > 0.90:
                            is_derived = True
                    elif "weekday" in col_lower or "day_name" in col_lower:
                        match_ratio = max(
                            (s_dt.dt.day_name() == target_s).mean(),
                            (s_dt.dt.weekday == target_s).mean()
                        )
                        if match_ratio > 0.90:
                            is_derived = True
                except Exception:
                    pass

            if not is_derived:
                try:
                    from insights.relationship_classifier import RelationshipClassifier, RelationshipType
                    rel = RelationshipClassifier.classify_pair(dt_col, col, df=df, score=1.0)
                    if rel.type == RelationshipType.DERIVED_FEATURE:
                        is_derived = True
                except Exception:
                    pass

            if is_derived:
                derived_cols.add(col)
                pairs.append(CorrelationPair(
                    col1=dt_col, col2=col, score=1.0,
                    metric="Deterministic Derivation",
                    is_significant=True,
                ))

        # 2. Grouped temporal association with nominal categories (not derived)
        span_days = 0
        try:
            span_days = (s_dt.max() - s_dt.min()).days
        except Exception:
            pass

        try:
            if span_days > 365:
                dt_grouped = s_dt.dt.to_period('M').astype(str)
            elif span_days >= 60:
                dt_grouped = s_dt.dt.to_period('W').astype(str)
            else:
                dt_grouped = s_dt.dt.day_name()
        except Exception:
            dt_grouped = s_dt.dt.to_period('M').astype(str)

        for cat_col in categorical_cols:
            if cat_col in derived_cols or cat_col == dt_col:
                continue
            c_v = self._cramers_v(dt_grouped, df[cat_col])
            if c_v is not None:
                pairs.append(CorrelationPair(
                    col1=dt_col, col2=cat_col, score=c_v,
                    metric="Temporal Association (Grouped)",
                    is_significant=(c_v > 0.25)
                ))

        # 3. Temporal trend with continuous numeric variables
        try:
            epoch_s = s_dt.astype('int64') // 10**9
            for num_col in numeric_cols:
                if num_col == dt_col or num_col in derived_cols:
                    continue
                s_num = pd.to_numeric(df[num_col], errors='coerce')
                valid = valid_dt & s_num.notna()
                if valid.sum() > 2:
                    r, p = stats.pearsonr(epoch_s[valid], s_num[valid])
                    if not np.isnan(r) and not np.isinf(r):
                        pairs.append(CorrelationPair(
                            col1=dt_col, col2=num_col, score=float(r), p_value=float(p),
                            metric="Temporal Trend",
                            is_significant=(p < 0.05)
                        ))
        except Exception:
            pass

        return pairs

    def _cramers_v(self, x: pd.Series, y: pd.Series) -> Optional[float]:
        try:
            df_temp = pd.DataFrame({'x': x, 'y': y}).dropna()
            n = len(df_temp)
            if n < 3:
                return None

            if n > 2000:
                df_temp = df_temp.sample(2000, random_state=42)
                n = len(df_temp)

            confusion = pd.crosstab(df_temp['x'], df_temp['y'])
            r, k = confusion.shape
            if r < 2 or k < 2:
                return None

            chi2, _, _, _ = stats.chi2_contingency(confusion, correction=False)
            phi2 = chi2 / n

            # Bergsma (2013) bias correction for finite-sample degrees-of-freedom inflation
            phi2_corr = max(0.0, phi2 - ((k - 1) * (r - 1)) / (n - 1))
            r_corr = r - ((r - 1) ** 2) / (n - 1)
            k_corr = k - ((k - 1) ** 2) / (n - 1)
            min_dim = min(r_corr - 1, k_corr - 1)

            if min_dim <= 0:
                return 0.0

            res = float(np.sqrt(phi2_corr / min_dim))
            return min(max(res, 0.0), 1.0) if not np.isnan(res) and not np.isinf(res) else None
        except Exception:
            return None

    def _eta_squared(self, num_s: pd.Series, cat_s: pd.Series) -> Optional[float]:
        try:
            clean_n = num_s.astype(str).str.replace(',', '', regex=False).str.replace('$', '', regex=False).str.strip() if pd.api.types.is_object_dtype(num_s) else num_s
            df_curr = pd.DataFrame({'n': pd.to_numeric(clean_n, errors='coerce'), 'c': cat_s}).dropna()
            if len(df_curr) < 3 or df_curr['c'].nunique() < 2:
                return None
            
            # ANOVA logic for eta-squared: SS_between / SS_total
            mean_total = df_curr['n'].mean()
            ss_total = ((df_curr['n'] - mean_total)**2).sum()
            
            if ss_total == 0:
                return 0.0
                
            group_means = df_curr.groupby('c')['n'].mean()
            group_counts = df_curr.groupby('c')['n'].count()
            ss_between = (group_counts * (group_means - mean_total)**2).sum()
            
            eta2 = ss_between / ss_total
            res = float(min(max(eta2, 0.0), 1.0))
            return res if not np.isnan(res) and not np.isinf(res) else None
        except Exception:
            return None

    def _point_biserial(self, num_s: pd.Series, bin_s: pd.Series) -> tuple[Optional[float], Optional[float]]:
        try:
            # Check if boolean
            clean_n = num_s.astype(str).str.replace(',', '', regex=False).str.replace('$', '', regex=False).str.strip() if pd.api.types.is_object_dtype(num_s) else num_s
            df_curr = pd.DataFrame({'n': pd.to_numeric(clean_n, errors='coerce'), 'b': bin_s}).dropna()
            
            # Convert binary to 0/1
            b_vals = df_curr['b'].unique()
            if len(b_vals) != 2:
                return None, None
            
            mapping = {b_vals[0]: 0, b_vals[1]: 1}
            numeric_bin = df_curr['b'].map(mapping)
            
            if df_curr['n'].nunique() < 2:
                return None, None
                
            r, p = stats.pointbiserialr(numeric_bin, df_curr['n'])
            if np.isnan(r) or np.isinf(r):
                return None, None
            p_f = float(p) if not np.isnan(p) else 1.0
            return float(r), p_f
        except Exception:
            return None, None

    def _compute_mutual_information(self, df: pd.DataFrame, num_cols: List[str], cat_cols: List[str], bin_cols: List[str]) -> Dict[str, Dict[str, float]]:
        # Take a subset to avoid excessive compute time, MI is expensive
        # Maximum 2000 rows, maximum 10 features
        mi_dict: Dict[str, Dict[str, float]] = {c: {} for c in (num_cols + cat_cols + bin_cols)}
        
        all_features = num_cols + cat_cols + bin_cols
        if not all_features:
            return mi_dict
            
        # Select features to compare (cap at 10 most complete)
        null_counts = df[all_features].isnull().sum()
        selected_features = null_counts.nsmallest(10).index.tolist()
        
        if len(selected_features) < 2:
            return mi_dict

        sub_df = df[selected_features].copy()
        if len(sub_df) > 2000:
            sub_df = sub_df.sample(2000, random_state=42)

        # Label encode cat columns
        for c in selected_features:
            if c in cat_cols or c in bin_cols:
                sub_df[c] = sub_df[c].astype(str).astype('category').cat.codes
            else:
                num_s = _to_clean_num(sub_df[c])
                med_val = num_s.median() if not num_s.empty else 0.0
                sub_df[c] = num_s.fillna(med_val)

        sub_df = sub_df.fillna(-1) # For safety

        try:
            for i, target in enumerate(selected_features):
                # Is target continuous or discrete?
                discrete_target = target in cat_cols or target in bin_cols
                
                features = [f for f in selected_features if f != target]
                X = sub_df[features]
                y = sub_df[target]
                
                # Identify discrete features for the MI computer
                discrete_mask = np.array([f in cat_cols or f in bin_cols for f in features], dtype=bool)

                if discrete_target:
                    mi_scores = mutual_info_classif(X, y, discrete_features=discrete_mask, random_state=42)
                else:
                    mi_scores = mutual_info_regression(X, y, discrete_features=discrete_mask, random_state=42)
                
                for feat, score in zip(features, mi_scores):
                    s_f = float(score) if not np.isnan(score) and not np.isinf(score) else 0.0
                    mi_dict[feat][target] = s_f
                    mi_dict[target][feat] = s_f

        except Exception as e:
            logger.warning("Mutual information calculation failed: %s", e, exc_info=True)

        return mi_dict

    def _compute_vif(self, df: pd.DataFrame, num_cols: List[str]) -> MulticollinearityReport:
        try:
            if len(num_cols) < 2:
                return MulticollinearityReport(has_multicollinearity=False, vif_scores={}, warnings=[])

            # Use subset of numeric columns without a lot of nulls
            null_pct = df[num_cols].isnull().mean()
            valid_cols = null_pct[null_pct < 0.2].index.tolist()

            # Cap columns to 15 to prevent slow VIF
            if len(valid_cols) > 15:
                valid_cols = valid_cols[:15]
            
            if len(valid_cols) < 2:
                return MulticollinearityReport(has_multicollinearity=False, vif_scores={}, warnings=[])

            # Dropna for VIF compute
            X = pd.DataFrame({c: _to_clean_num(df[c]) for c in valid_cols}).dropna()

            # Cap rows
            if len(X) > 3000:
                X = X.sample(3000, random_state=42)
            
            if len(X) < max(10, len(valid_cols) + 2):
                return MulticollinearityReport(has_multicollinearity=False, vif_scores={}, warnings=[])

            # VIF needs intercept
            X['__intercept__'] = 1.0

            vif_scores = {}
            for i, col in enumerate(X.columns):
                # We skip computing VIF for the intercept but need it in the dataset
                if col == '__intercept__':
                    continue
                v = _safe_vif(X.values, i)
                if not np.isnan(v) and not np.isinf(v):
                    vif_scores[col] = float(v)

            high_vif = [c for c, v in vif_scores.items() if v > self.vif_threshold]
            warnings = []
            if len(high_vif) > 0:
                warnings.append(f"High multicollinearity detected in: {', '.join(high_vif)}")

            return MulticollinearityReport(
                has_multicollinearity=len(high_vif) > 0,
                vif_scores=vif_scores,
                warnings=warnings
            )
        except Exception as e:
            logger.warning("VIF multicollinearity calculation failed: %s", e, exc_info=True)
            return MulticollinearityReport(has_multicollinearity=False, vif_scores={}, warnings=[])


def _safe_vif(X_values: np.ndarray, exog_idx: int) -> float:
    """Compute VIF with R^2 clamped to 0.9999 to prevent divide-by-zero RuntimeWarning."""
    try:
        from statsmodels.regression.linear_model import OLS
        k_vars = X_values.shape[1]
        x_i = X_values[:, exog_idx]
        mask = np.arange(k_vars) != exog_idx
        x_noti = X_values[:, mask]
        ols_res = OLS(x_i, x_noti).fit()
        r_squared = float(ols_res.rsquared)
        r_squared = min(max(r_squared, 0.0), 0.9999)
        return float(1.0 / (1.0 - r_squared))
    except Exception:
        return 1.0

