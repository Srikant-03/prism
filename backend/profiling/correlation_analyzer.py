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
        # 1. Gather column types from profile
        numeric_cols = []
        categorical_cols = []
        binary_cols = []

        for col in dataset_profile.columns:
            if col.name not in df.columns:
                continue
            
            stype = col.semantic_type
            if stype in ('numeric_continuous', 'numeric_discrete', 'percentage', 'currency'):
                numeric_cols.append(col.name)
            elif stype in ('categorical_nominal', 'categorical_ordinal'):
                categorical_cols.append(col.name)
            elif stype == 'boolean':
                binary_cols.append(col.name)

        all_pairs = []
        matrix = {c: {} for c in numeric_cols}

        # 2. Numeric - Numeric: Pearson & Spearman
        for i in range(len(numeric_cols)):
            col1 = numeric_cols[i]
            s1 = pd.to_numeric(df[col1], errors='coerce')
            valid1 = s1.notna()

            for j in range(i, len(numeric_cols)):
                col2 = numeric_cols[j]
                if col1 == col2:
                    matrix[col1][col2] = 1.0
                    continue
                
                s2 = pd.to_numeric(df[col2], errors='coerce')
                valid = valid1 & s2.notna()
                v1, v2 = s1[valid], s2[valid]

                if len(v1) > 2 and v1.nunique() > 1 and v2.nunique() > 1:
                    # Pearson
                    r, p_val = stats.pearsonr(v1, v2)
                    if not np.isnan(r):
                        matrix[col1][col2] = float(r)
                        matrix[col2][col1] = float(r)
                        all_pairs.append(CorrelationPair(
                            col1=col1, col2=col2, score=float(r), p_value=float(p_val),
                            metric="Pearson", is_significant=(p_val < 0.05)
                        ))
                    
                    # Spearman for rank
                    rho, p_val_s = stats.spearmanr(v1, v2)
                    if not np.isnan(rho):
                        all_pairs.append(CorrelationPair(
                            col1=col1, col2=col2, score=float(rho), p_value=float(p_val_s),
                            metric="Spearman", is_significant=(p_val_s < 0.05)
                        ))

        # 3. Categorical - Categorical: Cramér's V
        for i in range(len(categorical_cols)):
            col1 = categorical_cols[i]
            for j in range(i + 1, len(categorical_cols)):
                col2 = categorical_cols[j]
                c_v = self._cramers_v(df[col1], df[col2])
                if c_v is not None:
                    # Association is positive [0, 1]
                    all_pairs.append(CorrelationPair(
                        col1=col1, col2=col2, score=c_v, metric="Cramér's V"
                    ))

        # 4. Numeric - Categorical: Eta-squared approximation / Point-biserial
        for num_col in numeric_cols:
            for cat_col in categorical_cols:
                # Eta-squared basically involves checking variance explained by groups
                eta_sq = self._eta_squared(df[num_col], df[cat_col])
                if eta_sq is not None:
                    all_pairs.append(CorrelationPair(
                        col1=num_col, col2=cat_col, score=eta_sq, metric="Eta-squared"
                    ))
            
            for bin_col in binary_cols:
                pb, p_val = self._point_biserial(df[num_col], df[bin_col])
                if pb is not None:
                    all_pairs.append(CorrelationPair(
                        col1=num_col, col2=bin_col, score=pb, p_value=p_val,
                        metric="Point-Biserial", is_significant=(p_val < 0.05) if p_val else False
                    ))

        # 5. Mutual Information (Numeric & Categorical combined sample)
        mi_dict = self._compute_mutual_information(df, numeric_cols, categorical_cols, binary_cols)

        # 6. Multicollinearity (VIF)
        vif_report = self._compute_vif(df, numeric_cols)

        # 7. Sort and select top pairs
        # Deduplicate, prioritizing strongest score magnitude
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

    def _cramers_v(self, x: pd.Series, y: pd.Series) -> Optional[float]:
        try:
            confusion = pd.crosstab(x, y)
            if confusion.shape[0] < 2 or confusion.shape[1] < 2:
                return None
            chi2, _, _, _ = stats.chi2_contingency(confusion, correction=False)
            n = confusion.sum().sum()
            min_dim = min(confusion.shape) - 1
            if min_dim == 0 or n == 0:
                return 0.0
            return float(np.sqrt(chi2 / (n * min_dim)))
        except Exception:
            return None

    def _eta_squared(self, num_s: pd.Series, cat_s: pd.Series) -> Optional[float]:
        try:
            df_curr = pd.DataFrame({'n': pd.to_numeric(num_s, errors='coerce'), 'c': cat_s}).dropna()
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
            return float(min(max(eta2, 0.0), 1.0))
        except Exception:
            return None

    def _point_biserial(self, num_s: pd.Series, bin_s: pd.Series) -> tuple[Optional[float], Optional[float]]:
        try:
            # Check if boolean
            df_curr = pd.DataFrame({'n': pd.to_numeric(num_s, errors='coerce'), 'b': bin_s}).dropna()
            
            # Convert binary to 0/1
            b_vals = df_curr['b'].unique()
            if len(b_vals) != 2:
                return None, None
            
            mapping = {b_vals[0]: 0, b_vals[1]: 1}
            numeric_bin = df_curr['b'].map(mapping)
            
            if df_curr['n'].nunique() < 2:
                return None, None
                
            r, p = stats.pointbiserialr(numeric_bin, df_curr['n'])
            if np.isnan(r):
                return None, None
            return float(r), float(p)
        except Exception:
            return None, None

    def _compute_mutual_information(self, df: pd.DataFrame, num_cols: List[str], cat_cols: List[str], bin_cols: List[str]) -> Dict[str, Dict[str, float]]:
        # Take a subset to avoid excessive compute time, MI is expensive
        # Maximum 5000 rows, maximum 20 features
        mi_dict: Dict[str, Dict[str, float]] = {c: {} for c in (num_cols + cat_cols + bin_cols)}
        
        all_features = num_cols + cat_cols + bin_cols
        if not all_features:
            return mi_dict
            
        # Select features to compare (cap at 20 most complete)
        null_counts = df[all_features].isnull().sum()
        selected_features = null_counts.nsmallest(20).index.tolist()
        
        if len(selected_features) < 2:
            return mi_dict

        sub_df = df[selected_features].copy()
        if len(sub_df) > 5000:
            sub_df = sub_df.sample(5000, random_state=42)

        # Label encode cat columns
        for c in selected_features:
            if c in cat_cols or c in bin_cols:
                sub_df[c] = sub_df[c].astype(str).astype('category').cat.codes
            else:
                sub_df[c] = pd.to_numeric(sub_df[c], errors='coerce').fillna(sub_df[c].median())

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
                    mi_dict[feat][target] = float(score)
                    mi_dict[target][feat] = float(score)

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
            
            if len(valid_cols) < 2:
                return MulticollinearityReport(has_multicollinearity=False, vif_scores={}, warnings=[])

            # Dropna for VIF compute
            X = df[valid_cols].apply(pd.to_numeric, errors='coerce').dropna()
            
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

