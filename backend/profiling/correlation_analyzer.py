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

class CorrelationAnalyzer:
    """
    Computes pair-wise correlations, associations, and multicollinearity
    across columns in a dataset.
    """

    def __init__(self, top_k: int = 50, vif_threshold: float = 5.0):
        self.top_k = top_k
        self.vif_threshold = vif_threshold

    def analyze(self, df: pd.DataFrame, dataset_profile: DatasetProfile) -> CorrelationAnalysis:
        try:
            def _to_clean_num(series: pd.Series) -> pd.Series:
                if pd.api.types.is_numeric_dtype(series):
                    return pd.to_numeric(series, errors='coerce')
                clean_s = series.astype(str).str.replace(',', '', regex=False).str.replace('$', '', regex=False).str.strip()
                return pd.to_numeric(clean_s, errors='coerce')

            # 1. Gather column types by directly testing series content
            numeric_cols = []
            categorical_cols = []
            binary_cols = []

            for col in df.columns:
                s_clean = _to_clean_num(df[col])
                non_null_ratio = s_clean.notna().sum() / len(df) if len(df) > 0 else 0
                if non_null_ratio > 0.4 and s_clean.nunique() > 1:
                    numeric_cols.append(col)
                elif df[col].nunique() == 2 or pd.api.types.is_bool_dtype(df[col]):
                    binary_cols.append(col)
                elif df[col].nunique() > 1 and df[col].nunique() <= 500:
                    categorical_cols.append(col)

            all_features = list(dict.fromkeys(numeric_cols + categorical_cols + binary_cols))
            all_pairs = []
            matrix = {c: {c2: (1.0 if c == c2 else 0.0) for c2 in all_features} for c in all_features}

            # 2. Numeric - Numeric: Pearson & Spearman
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

            # 3. Categorical - Categorical: Cramér's V
            for i in range(len(categorical_cols)):
                col1 = categorical_cols[i]
                for j in range(i + 1, len(categorical_cols)):
                    col2 = categorical_cols[j]
                    c_v = self._cramers_v(df[col1], df[col2])
                    if c_v is not None:
                        matrix[col1][col2] = c_v
                        matrix[col2][col1] = c_v
                        all_pairs.append(CorrelationPair(
                            col1=col1, col2=col2, score=c_v, metric="Cramér's V"
                        ))

            # 4. Numeric - Categorical: Eta-squared approximation / Point-biserial
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

            # 5. Mutual Information (Numeric & Categorical combined sample)
            try:
                mi_dict = self._compute_mutual_information(df, numeric_cols, categorical_cols, binary_cols)
            except Exception:
                mi_dict = {}

            # 6. Multicollinearity (VIF)
            try:
                vif_report = self._compute_vif(df, numeric_cols)
            except Exception:
                vif_report = MulticollinearityReport(has_multicollinearity=False, vif_scores={}, warnings=[])

            # 7. Sort and select top pairs
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
                mutual_information=mi_dict
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

    def _cramers_v(self, x: pd.Series, y: pd.Series) -> Optional[float]:
        try:
            if len(x) > 2000:
                df_temp = pd.DataFrame({'x': x, 'y': y}).dropna()
                if len(df_temp) > 2000:
                    df_temp = df_temp.sample(2000, random_state=42)
                x, y = df_temp['x'], df_temp['y']

            confusion = pd.crosstab(x, y)
            if confusion.shape[0] < 2 or confusion.shape[1] < 2:
                return None
            chi2, _, _, _ = stats.chi2_contingency(confusion, correction=False)
            n = confusion.sum().sum()
            min_dim = min(confusion.shape) - 1
            if min_dim == 0 or n == 0:
                return 0.0
            res = float(np.sqrt(chi2 / (n * min_dim)))
            return res if not np.isnan(res) and not np.isinf(res) else None
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
                discrete_features_idx = [j for j, f in enumerate(features) if f in cat_cols or f in bin_cols]

                if discrete_target:
                    mi_scores = mutual_info_classif(X, y, discrete_features=discrete_features_idx, random_state=42)
                else:
                    mi_scores = mutual_info_regression(X, y, discrete_features=discrete_features_idx, random_state=42)
                
                for feat, score in zip(features, mi_scores):
                    s_f = float(score) if not np.isnan(score) and not np.isinf(score) else 0.0
                    mi_dict[feat][target] = s_f
                    mi_dict[target][feat] = s_f

        except Exception as e:
            # Fallback if MI fails
            pass

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
                v = variance_inflation_factor(X.values, i)
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
        except Exception:
            return MulticollinearityReport(has_multicollinearity=False, vif_scores={}, warnings=[])

