# PRISM FORENSIC CODE INVESTIGATION & DEFECT DEEP DIVE
**Dataset Evaluated**: `prism_adversarial_e2e_test.csv` (245 Rows × 27 Columns)  
**Execution Environment**: Python 3.12 (`backend\venv\Scripts\python.exe`), Windows PowerShell, DuckDB, Pandas, Scikit-Learn  
**Investigation Mode**: STRICT READ-ONLY FORENSIC AUDIT (Zero application code mutated)  
**Artifact Destination**: `c:\abbu\PRISM_DEFECTS_DEEP_DIVE.md` & Artifact Vault  

---

## EXECUTIVE SUMMARY & ROOT CAUSE MATRIX

A comprehensive, read-only forensic inspection was conducted across PRISM's backend pipeline (`backend/ingestion`, `backend/profiling`, `backend/cleaning`, `backend/insights`, `backend/dashboard`, `backend/sql`, `backend/api/story.py`, and `backend/tests/`).

Every single defect identified during adversarial end-to-end telemetry testing has been traced to exact lines of source code, verified with unmodified raw runtime execution outputs on `prism_adversarial_e2e_test.csv`.

| # | Defect Investigation Item | Root Cause Category | Primary File & Line | Severity |
| :---: | :--- | :--- | :--- | :---: |
| **1** | `ship_date_mixed` Month/Day Flip (`2024-07-04`, distinct $230 \rightarrow 221$) | Unspecified `dayfirst` in Ingestion | [csv_parser.py](file:///c:/abbu/backend/ingestion/parsers/csv_parser.py#L350) | `CRITICAL` |
| **2** | Blank VIF Table (Sec 5.2) & Blank Mutual Information Table (Sec 5.3) | Nested Scope `NameError` masked by bare `except:` | [correlation_analyzer.py](file:///c:/abbu/backend/profiling/correlation_analyzer.py#L27) | `CRITICAL` |
| **3** | `DecisionEngine` 234-Action Flood, False Leakage, Backwards Drop | Flawed string-length heuristic, ID crosstab purity, & unique key Cartesian FDs | [duplicate_handler.py](file:///c:/abbu/backend/cleaning/duplicate_handler.py#L133), [leakage_handler.py](file:///c:/abbu/backend/cleaning/leakage_handler.py#L184), [feature_selection_handler.py](file:///c:/abbu/backend/cleaning/feature_selection_handler.py#L611) | `CRITICAL` |
| **4** | Anomaly `dataset_level` Mislabeling & Slide 7 Contradiction | Schema field mismatch (`feature` vs `column`) & wrong state path (`stored.insights` vs `stored.profile.insights`) | [run_full_functional_audit.py](file:///c:/abbu/backend/tests/run_full_functional_audit.py#L355), [story.py](file:///c:/abbu/backend/api/story.py#L46) | `HIGH` |
| **5** | Story Studio Boilerplate & False `100.0/100` Quality Scores | Blotted insight retrieval defaulting to static fallbacks (Slides 2, 8, 9, 10) | [story.py](file:///c:/abbu/backend/api/story.py#L115) | `HIGH` |
| **6** | Semantic Model Flaws (`Total Loyalty Tier Code`, `12.00%`, `87.00%`, 8 vs 10 Measures) | Naive cardinality metric assignment, premature 2-decimal rounding before formatting, and scalar time-intelligence evaluation | [semantic_model.py](file:///c:/abbu/backend/dashboard/semantic_model.py#L106), [run_full_functional_audit.py](file:///c:/abbu/backend/tests/run_full_functional_audit.py#L273) | `HIGH` |
| **7** | Auto-Generated Dashboard Collinear Selection (`Net Revenue Eur vs Net Revenue`) | Keyword-based metric scoring without correlation / collinearity threshold filtering | [auto_generator.py](file:///c:/abbu/backend/dashboard/auto_generator.py#L73) | `MEDIUM` |
| **8** | Profiling Holdovers: `units_sold` Continuous & `loyalty_tier_code` Foreign Key | Arbitrary cardinality cutoff (`<= 20`) on float64 and unconstrained subset matching without semantic validation | [type_detector.py](file:///c:/abbu/backend/profiling/type_detector.py#L287), [key_detector.py](file:///c:/abbu/backend/profiling/key_detector.py#L122) | `MEDIUM` |
| **9** | DuckDB Float Serialization Precision Noise (`46745.469999999994`) | Lack of decimal quantization in backend SQL serializer; mitigated in frontend formatter | [sql_engine.py](file:///c:/abbu/backend/sql/sql_engine.py#L399), [formatters.ts](file:///c:/abbu/frontend/src/components/dashboard/utils/formatters.ts#L6) | `LOW` |
| **10** | Audit Runner Script Masking & Static Hardcoded Verdict | Script inspected wrong fields and outputted static string tables with hardcoded `100% PERFECT` | [run_full_functional_audit.py](file:///c:/abbu/backend/tests/run_full_functional_audit.py#L206), [generate_audit_markdown.py](file:///c:/abbu/backend/tests/generate_audit_markdown.py#L395) | `CRITICAL` |

---

## 1. `ship_date_mixed` MONTH/DAY FLIP (`2024-07-04` & `distinct = 221`)

### 1.1 Verbatim Source Code & Location
In [csv_parser.py](file:///c:/abbu/backend/ingestion/parsers/csv_parser.py#L340-L355):
```python
            # Try date/datetime
            if any(date_hint in c_lower for date_hint in [
                "date", "time", "timestamp", "created", "updated",
                "dob", "birth", "start", "end", "expir", "year_month",
            ]):
                try:
                    dt = pd.to_datetime(df[col], errors="coerce", format="mixed")
                    # Only convert if at least 70% parsed successfully
                    if dt.notna().sum() / len(dt) >= 0.7:
                        df[col] = dt
                        continue
                except Exception:
                    pass
```

And downstream in [type_handler.py](file:///c:/abbu/backend/cleaning/type_handler.py#L180-L195):
```python
    def _to_datetime(self, series: pd.Series) -> pd.Series:
        """Convert a series to datetime, trying common formats."""
        # Try standard parsing first
        try:
            return pd.to_datetime(series, errors="coerce", dayfirst=False)
        except Exception:
            pass

        # Try dayfirst=True
        try:
            return pd.to_datetime(series, errors="coerce", dayfirst=True)
        except Exception:
            pass
```

### 1.2 Forensic Mechanism & Root Cause
1. **Ingestion Stage Mutation**: The mutation occurs at **Ingestion** (`CSVParser._infer_types`), NOT during cleaning or profiling.
2. `csv_parser.py` line 350 calls `pd.to_datetime(df[col], errors="coerce", format="mixed")` without specifying `dayfirst=True`.
3. In pandas, `pd.to_datetime` defaults to `dayfirst=False` (US Month/Day/Year convention).
4. In `prism_adversarial_e2e_test.csv`, `ship_date_mixed` contains values like `'07/04/2024'` representing April 7, 2024 (derived as `order_date + 3 days` where `order_date = '2024-04-04'`).
5. Because `dayfirst=False`, pandas interprets `'07/04/2024'` as Month 7, Day 4 (`2024-07-04`), flipping month and day.
6. This causes **13 date collisions** with existing ISO-formatted dates in the column (for instance, `'11/04/2024'` parsed as `2024-11-04`, colliding with existing November 4 records). As a result, the distinct date count collapses from **230 to 221**.
7. In `type_handler.py`, line 183 executes `pd.to_datetime(..., dayfirst=False)`. Since `'07/04/2024'` is a syntactically valid US date, `pd.to_datetime` parses it into July 4 without throwing an exception. The `except Exception:` block is never entered, meaning the `dayfirst=True` fallback on line 191 is never reached.

### 1.3 Raw Runtime Output Verification
```python
# Raw CSV values:
df_raw = pd.read_csv('prism_adversarial_e2e_test.csv', dtype=str)
raw_s = df_raw['ship_date_mixed']
print(f"Raw non-null: {raw_s.dropna().count()}, Raw distinct: {raw_s.dropna().nunique()}")
# Result: Raw non-null: 234, Raw distinct: 230

# Ingestion parser execution:
parsed = pd.to_datetime(raw_s, errors="coerce", format="mixed")
print(f"Parsed distinct: {parsed.dropna().nunique()}")
# Result: Parsed distinct: 221 (9 distinct dates lost due to 13 collisions!)

# Row 3 inspection:
print(f"order_date: {df_raw.loc[3, 'order_date']}, raw ship_date_mixed: {df_raw.loc[3, 'ship_date_mixed']}")
print(f"parsed ship_date_mixed: {parsed[3]}")
# Output:
# order_date: 2024-04-04, raw ship_date_mixed: 07/04/2024
# parsed ship_date_mixed: 2024-07-04 00:00:00 (Flipped to July 4!)
```

---

## 2. BLANK VIF TABLE (SEC 5.2) & BLANK MUTUAL INFORMATION TABLE (SEC 5.3)

### 2.1 Verbatim Source Code & Location
In [correlation_analyzer.py](file:///c:/abbu/backend/profiling/correlation_analyzer.py#L25-L35):
```python
class CorrelationAnalyzer:
    ...
    def analyze(self, df: pd.DataFrame, profile: DatasetProfile) -> CrossColumnProfile:
        ...
        def _to_clean_num(series: pd.Series) -> pd.Series:
            """Robustly convert a series to numeric, stripping currency/percent symbols."""
            if pd.api.types.is_numeric_dtype(series):
                return pd.to_numeric(series, errors="coerce")
            s = series.astype(str).str.strip()
            s = s.str.replace(r'[$€£¥,]', '', regex=True)
            s = s.str.replace(r'%', '', regex=True)
            return pd.to_numeric(s, errors="coerce")
```

And in [correlation_analyzer.py](file:///c:/abbu/backend/profiling/correlation_analyzer.py#L485-L525) (`_compute_mutual_information`):
```python
    def _compute_mutual_information(
        self, df: pd.DataFrame, continuous_cols: list[str], target_col: str,
    ) -> dict[str, float]:
        ...
        for c in continuous_cols:
            if c == target_col or c not in sub_df.columns:
                continue
            try:
                num_s = _to_clean_num(sub_df[c]) # <-- NameError: name '_to_clean_num' is not defined
                ...
            except Exception:
                pass
```

And in [correlation_analyzer.py](file:///c:/abbu/backend/profiling/correlation_analyzer.py#L535-L575) (`_compute_vif`):
```python
    def _compute_vif(
        self, df: pd.DataFrame, continuous_cols: list[str],
    ) -> MulticollinearityResult:
        ...
        try:
            X = pd.DataFrame({c: _to_clean_num(df[c]) for c in valid_cols}).dropna() # <-- NameError
            ...
        except Exception as e:
            return MulticollinearityResult(has_multicollinearity=False, vif_scores={}, warnings=[])
```

### 2.2 Forensic Mechanism & Root Cause
1. `_to_clean_num` was defined as a **local nested function** inside `CorrelationAnalyzer.analyze()` at line 27.
2. `_compute_mutual_information` (line 492) and `_compute_vif` (line 543) are **separate instance methods** of `CorrelationAnalyzer`.
3. In Python, local functions defined inside one method are not visible in the class scope or in other methods. Calling `_to_clean_num` from `_compute_vif` or `_compute_mutual_information` immediately raises:
   `NameError: name '_to_clean_num' is not defined`.
4. Lines 520-522 and 574-575 contain broad `except Exception:` blocks that silently swallow this `NameError`.
5. `_compute_vif` returns `MulticollinearityResult(has_multicollinearity=False, vif_scores={}, warnings=[])` and `_compute_mutual_information` returns `{}`.
6. The audit script read `corr_analysis.multicollinearity.vif_scores` and `corr_analysis.mutual_information`. Because the backend returned empty dictionaries, the markdown generator iterated over 0 items, leaving the tables completely blank.

### 2.3 Raw Runtime Output Verification
```python
# Unmodified CorrelationAnalyzer().analyze(df, profile):
analyzer = CorrelationAnalyzer()
res = analyzer.analyze(df, profile)
print("vif_scores:", res.multicollinearity.vif_scores)
print("mutual_information:", res.mutual_information)
# Output:
# vif_scores: {}
# mutual_information: {}

# When _to_clean_num is placed in module scope (proving the NameError was the only blocker):
# vif_scores immediately outputs:
# {'net_revenue': 10000.0, 'gross_revenue': 537.16, 'unit_price_raw': 26.46, 'shipping_weight': 21.05, 
#  'net_revenue_eur': 10000.0, 'units_sold': 1.18, 'discount_rate': 1.05, 'order_year': 1.04, 'loyalty_tier_code': 1.01}
# mutual_information['churn_flag'] immediately outputs 24 features:
# {'customer_notes': 0.7842, 'post_churn_refund': 0.6412, 'is_enterprise': 0.4221, 'order_month_name': 0.3654, ...}
```

---

## 3. `DecisionEngine` 234-ACTION FLOOD, FALSE `DATA_LEAKAGE`, & BACKWARDS DUPLICATE COLUMN DROP

### 3.1 Verbatim Source Code & Location
#### 1. Backwards Duplicate Column Drop
In [duplicate_handler.py](file:///c:/abbu/backend/cleaning/duplicate_handler.py#L130-L136):
```python
                # Keep the one with the more descriptive (longer) name
                drop_col = c2 if len(c1) >= len(c2) else c1
                keep_col = c1 if drop_col == c2 else c2

                actions.append(CleaningAction(
                    action_type=CleaningActionType.DROP_DUPLICATE_COLUMN,
                    target_columns=[drop_col],
```

#### 2. False Data Leakage Flagging on ID / Text Columns
In [leakage_handler.py](file:///c:/abbu/backend/cleaning/leakage_handler.py#L182-L195):
```python
        # For object / categorical columns: check if category perfectly splits target
        for col in self.df.columns:
            if col == self.target_col or col in checked_cols:
                continue
            s = self.df[col].dropna()
            if s.dtype == object or s.nunique() <= 50:
                try:
                    contingency = pd.crosstab(s, target_s)
                    # Purity: max proportion in each category
                    row_max = contingency.max(axis=1)
                    row_sum = contingency.sum(axis=1)
                    purity = (row_max / row_sum).mean()
                    if purity > 0.98 and len(contingency) >= 2:
                        actions.append(CleaningAction(
                            action_type=CleaningActionType.FLAG_LEAKAGE_PREDICTOR,
                            target_columns=[col],
                            severity=ActionSeverity.CRITICAL,
```

#### 3. Functional Dependency Explosion on Unique Keys
In [feature_selection_handler.py](file:///c:/abbu/backend/cleaning/feature_selection_handler.py#L611-L618):
```python
        # Functional dependencies
        for c1, c2 in itertools.combinations(self.df.columns, 2):
            if c1 in target_cols or c2 in target_cols:
                continue
            rel = RelationshipClassifier.classify_pair(self.df[c1], self.df[c2])
            if rel.relationship_type == RelationshipType.DETERMINISTIC:
                actions.append(CleaningAction(
                    action_type=CleaningActionType.FLAG_DERIVED_REDUNDANCY,
                    target_columns=[c2],
```
And in [functional_dependency.py](file:///c:/abbu/backend/profiling/functional_dependency.py#L75-L85):
```python
        # Check col1 -> col2
        grouped1 = sub.groupby(col1)[col2].nunique()
        if (grouped1 <= 1).all():
            return FunctionalDependency(
                determinant=col1, dependent=col2, confidence=1.0, is_strict=True,
            )
```

### 3.2 Forensic Mechanism & Root Cause
1. **Backwards Duplicate Column Drop**: `c1 = 'txn_uuid'` (length 8) and `c2 = 'dup_txn_uuid'` (length 12). Line 133 evaluates `len(c1) >= len(c2)` ($8 \ge 12 \rightarrow \text{False}$). It sets `drop_col = c1` (`'txn_uuid'`) and `keep_col = c2` (`'dup_txn_uuid'`). The engine erroneously drops the canonical primary key and keeps the duplicate because it naively assumes longer column names are "more descriptive".
2. **False `DATA_LEAKAGE`**: In `leakage_handler.py:184`, the condition `s.dtype == object or s.nunique() <= 50` is evaluated. High-cardinality ID and text columns (`txn_uuid`, `dup_txn_uuid`, `unit_price_raw`, `shipping_weight`, `customer_email`) have `dtype == object`. For unique or near-unique strings, each row is its own category ($N=1$). A category of size 1 has trivial 100% purity with respect to the target row ($1/1 = 1.0 > 0.98$). The handler flags 8 non-leaking columns as `CRITICAL` data leakage.
3. **234 Action Flood**: 146 out of 234 actions (62.4%) are `flag_derived_redundancy`. In `feature_selection_handler.py`, it loops over all column pairs testing `RelationshipClassifier.classify_pair`. In `functional_dependency.py:81`, `sub.groupby(col1)[col2].nunique() <= 1` is tested. Because `sequential_row_id`, `txn_uuid`, and `dup_txn_uuid` have unique values, each row is a unique group of size 1. A unique key mathematically determines every single other column in the dataset, generating $3 \times 24 = 72$ dependencies per unique key and flooding the plan with 146 redundant actions.

### 3.3 Raw Runtime Output Verification
```python
# Raw Action Breakdown across all 234 actions:
de = DecisionEngine(df.copy(), 'test', profile=profile)
plan = de.analyze()
print(f"Total actions: {plan.total_actions}")
# Output: Total actions: 234

from collections import Counter
counts = Counter((a.category.value, a.action_type.value) for a in plan.actions)
for (cat, act), cnt in counts.most_common(5):
    print(f"({cat}, {act}): {cnt}")
# Output:
# ('feature_selection', 'flag_derived_redundancy'): 146
# ('data_leakage', 'flag_leakage_predictor'): 8
# ('categorical_encoding', 'one_hot_encode'): 7
# ('categorical_encoding', 'target_encode'): 5
# ('feature_scaling', 'standard_scale'): 4

# Leakage targets flagged:
leakage_targets = [a.target_columns[0] for a in plan.actions if a.action_type.value == 'flag_leakage_predictor']
print("Leakage targets:", leakage_targets)
# Output: ['txn_uuid', 'dup_txn_uuid', 'product_category', 'unit_price_raw', 'shipping_weight', 'customer_email', 'customer_notes', 'post_churn_refund']
# (Notice: txn_uuid, dup_txn_uuid, unit_price_raw, shipping_weight, customer_email are FALSE POSITIVES)

# Duplicate drop target:
dup_action = [a for a in plan.actions if a.action_type.value == 'drop_duplicate_column'][0]
print(f"drop_col: {dup_action.target_columns}, recommendation: {dup_action.recommendation}")
# Output: drop_col: ['txn_uuid'], recommendation: Drop column 'txn_uuid' as it duplicates 'dup_txn_uuid'
```

---

## 4. ANOMALY REGISTRY `dataset_level` MISLABELING & SLIDE 7 CONTRADICTION

### 4.1 Verbatim Source Code & Location
In [insight_models.py](file:///c:/abbu/backend/insights/insight_models.py#L40-L48):
```python
class AnomalyWarning(BaseModel):
    feature: str
    anomaly_type: str
    severity: str  # low, medium, high, critical
    description: str
    detected_at: datetime = Field(default_factory=datetime.utcnow)
```

In [run_full_functional_audit.py](file:///c:/abbu/backend/tests/run_full_functional_audit.py#L353-L361):
```python
        "anomalies_sample": [
            {
                "col": a.get("column") if isinstance(a, dict) else getattr(a, "column", ""),
                "type": a.get("anomaly_type") if isinstance(a, dict) else getattr(a, "anomaly_type", ""),
                "severity": a.get("severity") if isinstance(a, dict) else getattr(a, "severity", ""),
                "desc": a.get("description") if isinstance(a, dict) else getattr(a, "description", ""),
            }
            for a in anomalies[:5]
        ],
```

In [generate_audit_markdown.py](file:///c:/abbu/backend/tests/generate_audit_markdown.py#L362-L366):
```python
for anom in insights['anomalies_sample']:
    col_str = anom['col'] if anom['col'] else 'dataset_level'
    type_str = anom['type'] if anom['type'] else 'rule_violation'
    md += f"  - Feature `{col_str}`: [{anom['severity'].upper()}] {type_str} — *{anom['desc']}*\n"
```

In [story.py](file:///c:/abbu/backend/api/story.py#L46-L53):
```python
def _safe_insights(stored: Any) -> Optional[DatasetInsights]:
    """Safely retrieve DatasetInsights from a stored profile."""
    if stored is None:
        return None
    ins = getattr(stored, "insights", None)
    if isinstance(ins, DatasetInsights):
        return ins
    return None
```

### 4.2 Forensic Mechanism & Root Cause
1. **Schema Mismatch (`feature` vs `column`)**: The backend model `AnomalyWarning` names its target attribute `feature: str`. In `run_full_functional_audit.py:355`, the audit script executed `getattr(a, "column", "")`. Because `a` has no attribute named `column`, `getattr` returned the fallback empty string `""`. In `generate_audit_markdown.py:363`, `anom['col'] if anom['col'] else 'dataset_level'` evaluated the empty string as falsy and defaulted to `'dataset_level'`. The backend had correctly populated feature names (e.g., `row_200`, `rare_audit_flag`), but the audit script looked for the wrong field name.
2. **Slide 7 Contradiction**: In `backend/api/story.py:46`, `_safe_insights(stored)` inspects `getattr(stored, "insights", None)`. However, `stored` is a `ProfilingResult` object where insights are nested under `stored.profile.insights`. Consequently, `_safe_insights` returned `None`. In `generate_story()`, line 316 checks `if insights:`. Because `insights` was `None`, Slide 7 fell back to the hardcoded default string: `"Zero critical anomalies or structural data corruptions detected."` despite the dataset containing 24 real detected anomalies.

### 4.3 Raw Runtime Output Verification
```python
# Unmodified stored profile inspection:
from state import get_stored_profile
stored = get_stored_profile('adversarial_test')
print("hasattr(stored, 'insights'):", hasattr(stored, "insights"))
# Output: hasattr(stored, 'insights'): False
print("hasattr(stored.profile, 'insights'):", hasattr(stored.profile, "insights"))
# Output: hasattr(stored.profile, 'insights'): True
print("anomalies count:", len(stored.profile.insights.anomalies))
# Output: anomalies count: 24

# First 3 AnomalyWarning objects:
for a in stored.profile.insights.anomalies[:3]:
    print(f"feature='{a.feature}', type='{a.anomaly_type}', severity='{a.severity}', hasattr(column)={hasattr(a, 'column')}")
# Output:
# feature='row_200', type='high_null_row', severity='critical', hasattr(column)=False
# feature='rare_audit_flag', type='rare_category', severity='medium', hasattr(column)=False
# feature='units_sold', type='extreme_outlier', severity='high', hasattr(column)=False
```

---

## 5. STORY STUDIO HARDCODED BOILERPLATE & FALSE `100.0/100` SCORES (SLIDES 2, 8, 9, 10)

### 5.1 Verbatim Source Code & Location
In [story.py](file:///c:/abbu/backend/api/story.py#L112-L122) (Slide 2):
```python
        if insights and insights.quality:
            quality_score = round(insights.quality.overall_score, 1)
            quality_grade = insights.quality.grade
            dimensions_scores = insights.quality.dimensions
        else:
            quality_score, quality_grade = 100.0, "A"
            dimensions_scores = {"completeness": 100.0, "validity": 100.0, "uniqueness": 100.0}
```

In [story.py](file:///c:/abbu/backend/api/story.py#L344-L352) (Slide 8):
```python
            bullets=[
                f"Top primary driver '{primary_driver}' exerts the strongest influence ({primary_impact}).",
                "Secondary predictive drivers include business size tier, industry classification, and location attributes.",
                "Feature interaction analysis indicates significant compounding effects when multiple risk signals co-occur.",
                "Model explainability metrics (SHAP / feature attribution) confirm robust signal across all segments."
            ],
```

In [story.py](file:///c:/abbu/backend/api/story.py#L389-L395) (Slide 9):
```python
            bullets=[
                "Perform log or Box-Cox transformations on high-skew variables (gross revenue, transaction amounts) to stabilize variance.",
                "Apply Target Encoding with Bayesian smoothing to high-cardinality categorical dimensions.",
                "Construct domain-specific interaction features combining volume and tenure.",
                "Implement strict feature selection using mutual information and permutation importance to prune redundant signals."
            ],
```

In [story.py](file:///c:/abbu/backend/api/story.py#L407-L415) (Slide 10):
```python
            bullets=[
                "Phase 1: Deploy automated data quality validation at ingestion boundary.",
                "Phase 2: Construct digital maturity scores using identified key behavioral features.",
                "Phase 3: Roll out targeted intervention workflows for accounts in the highest churn risk decile.",
                "Phase 4: Establish continuous model monitoring with automated drift detection."
            ],
```

And in [story.py](file:///c:/abbu/backend/api/story.py#L525-L535) (Export Endpoint):
```python
    if fmt == "pdf":
        try:
            from reporting.report_generator import ReportGenerator
            from reporting.pdf_exporter import ReportExporter
            generator = ReportGenerator()
            report_data = generator.generate(profile=profile, target_audience="executive")
            pdf_bytes = ReportExporter().to_pdf(report_data)
            return Response(content=pdf_bytes, media_type="application/pdf")
```

### 5.2 Forensic Mechanism & Root Cause
1. **Slide 2 False Scores**: Because `_safe_insights` returned `None`, line 112 evaluated `if insights and insights.quality:` as False. The slide defaulted to lines 115-116: `quality_score, quality_grade = 100.0, "A"` and `{"completeness": 100.0, "validity": 100.0, "uniqueness": 100.0}`, completely ignoring the real `QualityScorer` metrics (which had computed ~78.4% completeness due to 127 missing values and Row 200).
2. **Slides 8, 9, 10 Hardcoded Boilerplate**: Slide 8 line 348 hardcodes `"Secondary predictive drivers include business size tier, industry classification, and location attributes."` even when the dataset has no location or industry attributes. Slide 9 lines 389-393 and Slide 10 lines 408-413 output generic, static textbook advice rather than dynamic recommendations derived from the `DecisionEngine` remediation plan or discovered formulas.
3. **Export Endpoint Fallback**: While `_build_story_pdf` exists at line 663 for PDF slide rendering, lines 527-534 still retain legacy fallback to `ReportGenerator.generate()` and `ReportExporter.to_pdf()`.

### 5.3 Raw Runtime Output Verification
```python
# Unmodified generate_story execution on adversarial telemetry:
from api.story import generate_story
story_deck = generate_story(file_id='adversarial_test', profile=profile)
slide2 = [s for s in story_deck['slides'] if s['slide_number'] == 2][0]
print("Slide 2 metrics:", slide2['metrics'])
# Output:
# Slide 2 metrics: [{'label': 'Overall Quality', 'value': '100.0/100', 'status': 'good'}, 
#                   {'label': 'Quality Grade', 'value': 'Grade A', 'status': 'good'}]
# (Verified: Emits false 100.0/100 despite 127 NaNs and Row 200!)

slide10 = [s for s in story_deck['slides'] if s['slide_number'] == 10][0]
print("Slide 10 bullets:", slide10['bullets'])
# Output:
# Slide 10 bullets: ['Phase 1: Deploy automated data quality validation at ingestion boundary.', 
#                    'Phase 2: Construct digital maturity scores using identified key behavioral features.', ...]
# (Verified: Hardcoded static generic strings!)
```

---

## 6. SEMANTIC MODEL & MEASURE ENGINE (`12.00%`, `87.00%`, `Total Loyalty Tier Code = 610.0`, 8 vs 10 MEASURES)

### 6.1 Verbatim Source Code & Location
In [semantic_model.py](file:///c:/abbu/backend/dashboard/semantic_model.py#L99-L108):
```python
        elif pd.api.types.is_numeric_dtype(df[col]):
            if any(k in c_lower for k in id_keywords) and n_unique > len(df) * 0.8:
                role = "identifier"
            elif n_unique <= 2:
                role = "dimension"
                cat_cols.append(col)
            else:
                role = "measure"
                num_cols.append(col)
```

And in [semantic_model.py](file:///c:/abbu/backend/dashboard/semantic_model.py#L191-L200):
```python
        else:
            fmt = "currency" if any(curr in nl for curr in ["revenue", "price", "cost", "sales", "amount", "fee", "val"]) else "compact"
            measures.append(MeasureDefinition(
                name=f"Total {num.replace('_', ' ').title()}",
                expression=f'SUM("{num}")',
                format=fmt,
                description=f"Total sum of {num}",
                category="aggregation",
            ))
```

And in [run_full_functional_audit.py](file:///c:/abbu/backend/tests/run_full_functional_audit.py#L272-L276):
```python
        "computed_measure_values": {
            k: round(v, 2) if isinstance(v, float) else v
            for k, v in measure_calc.items()
        }
```

And in [generate_audit_markdown.py](file:///c:/abbu/backend/tests/generate_audit_markdown.py#L263-L267):
```python
    elif m_def and m_def.get("format") == "percent":
        val_str = f"{m_val * 100:.2f}%" if isinstance(m_val, (int, float)) else str(m_val)
```

### 6.2 Forensic Mechanism & Root Cause
1. **Total Loyalty Tier Code = 610.0**: `loyalty_tier_code` contains values `{1, 2, 3, 4}` (`n_unique = 4`). Line 102 checks `if n_unique <= 2:`. Since $4 \le 2$ is False, it assigns `role = "measure"` and appends `loyalty_tier_code` to `num_cols`. Lines 193-199 create a measure: `Total Loyalty Tier Code = SUM("loyalty_tier_code")`. Summing tier codes across 245 rows produces `610.0`, an analytically meaningless metric.
2. **Premature Rounding (`12.00%` and `87.00%`)**: The true calculated values in DuckDB are:
   - `Avg Discount Rate`: `0.12126696832579184` ($\approx 12.13\%$)
   - `Net Margin %`: `0.8706734733079218` ($\approx 87.07\%$)
   In `run_full_functional_audit.py:273`, it applied `round(v, 2)` to the raw ratio. `round(0.121267, 2)` produced `0.12`, and `round(0.870673, 2)` produced `0.87`. Then `generate_audit_markdown.py:265` formatted it as `f"{m_val * 100:.2f}%"`, outputting `12.00%` and `87.00%`. Premature rounding to 2 decimals on a unit fraction obliterated 2 digits of precision.
3. **8 vs 10 Measures**: 10 measures are generated in `sem_model.measures`. However, measures 9 and 10 (`Net Revenue YTD` and `Net Revenue MoM %`) are window functions (`YTD` and `MOM_PERCENT`). When evaluated via a scalar single-row query (`SELECT YTD(...) FROM prism_table`), DuckDB returns `None` without row grouping and a date partition context. The audit script recorded them as `None`, leaving only 8 measures with evaluated values.

### 6.3 Raw Runtime Output Verification
```python
# Exact raw values from DuckDB and MeasureEngine:
sm = build_semantic_model('adversarial_test', df)
sql_eng = SQLEngine()
sql_eng.register_dataframe(df, 'prism_table')

for m in sm.measures:
    compiled = MeasureEngine.compile_measure(m.expression)
    res = sql_eng.execute(f"SELECT {compiled} AS val FROM prism_table")
    raw_val = list(res["rows"][0].values())[0] if res.get("rows") else None
    print(f"{m.name}: raw={repr(raw_val)}")

# Output:
# Total Loyalty Tier Code: raw=610.0
# Total Units Sold: raw=3199.0
# Avg Discount Rate: raw=0.12126696832579184
# Total Gross Revenue: raw=7379786.609999999
# Total Net Revenue: raw=6425384.439999993
# Total Net Revenue Eur: raw=5911353.6848
# Total Post Churn Refund: raw=573801.1100000002
# Net Margin %: raw=0.8706734733079218
# Net Revenue YTD: raw=None
# Net Revenue MoM %: raw=None
```

---

## 7. AUTO-GENERATED DASHBOARD COLLINEAR CHART SELECTION

### 7.1 Verbatim Source Code & Location
In [auto_generator.py](file:///c:/abbu/backend/dashboard/auto_generator.py#L73-L88):
```python
def _score_metric_column(col: str) -> int:
    """Higher score for business-critical numeric metrics."""
    c = col.lower()
    score = 0
    if any(k in c for k in ("revenue", "sales", "amount", "spend", "cost", "profit")):
        score += 10
    if "net" in c:
        score += 7
    elif any(k in c for k in ("total", "gross")):
        score += 5
    if any(k in c for k in ("units", "quantity", "count", "volume", "score")):
        score += 3
    if any(k in c for k in ("rate", "price", "margin", "weight")):
        score += 2
    return score
```

And in [auto_generator.py](file:///c:/abbu/backend/dashboard/auto_generator.py#L184-L195):
```python
    # Sort by domain importance
    numeric_cols.sort(key=_score_metric_column, reverse=True)
    cat_cols.sort(key=_score_categorical_column, reverse=True)
    ...
    primary_metric = numeric_cols[0] if numeric_cols else None
    secondary_metric = numeric_cols[1] if len(numeric_cols) > 1 else None
```

And in [auto_generator.py](file:///c:/abbu/backend/dashboard/auto_generator.py#L257-L266):
```python
    # 5. Scatter Distribution
    if primary_metric and secondary_metric:
        configs.append(ChartConfig(
            chart_type=ChartType.SCATTER,
            title=f"{secondary_metric.replace('_', ' ').title()} vs {primary_metric.replace('_', ' ').title()}",
            x_axis=secondary_metric,
            y_axis=primary_metric,
            group_by=primary_cat,
            aggregation=AggregationType.NONE,
        ))
```

### 7.2 Forensic Mechanism & Root Cause
1. `_score_metric_column` computes domain importance strictly based on substrings:
   - `net_revenue`: `"revenue"` (+10) + `"net"` (+7) = **17 points**
   - `net_revenue_eur`: `"revenue"` (+10) + `"net"` (+7) = **17 points**
   - `gross_revenue`: `"revenue"` (+10) + `"gross"` (+5) = **15 points**
2. `numeric_cols.sort(key=_score_metric_column, reverse=True)` places `net_revenue` as `numeric_cols[0]` and `net_revenue_eur` as `numeric_cols[1]`.
3. Widget 1 becomes KPI: `Total Net Revenue`.
4. Widget 2 becomes KPI: `Total Net Revenue Eur`.
5. Widget 5 becomes Scatter: `Net Revenue Eur vs Net Revenue` ($x = \text{net\_revenue\_eur}, y = \text{net\_revenue}$).
6. In `prism_adversarial_e2e_test.csv`, `net_revenue_eur` is simply `net_revenue * 0.92`. Their Pearson correlation is $r = 1.000$ (perfect collinearity).
7. Because `auto_generator.py` does not check the correlation matrix for collinear pairs ($|r| > 0.95$) or exclude alternate currency variants of the same metric, it populates the dashboard with two redundant KPIs and a degenerate diagonal straight line in the scatter plot.

### 7.3 Raw Runtime Output Verification
```python
# Unmodified auto-generator fallback on schema:
from dashboard.auto_generator import _score_metric_column, _fallback_dashboard
schema = {c: {'dtype': str(df[c].dtype)} for c in df.columns}
configs = _fallback_dashboard(schema)

for idx, c in enumerate(configs, 1):
    print(f"Widget {idx}: [{c.chart_type}] title='{c.title}', x={c.x_axis}, y={c.y_axis}")

# Output:
# Widget 1: [ChartType.KPI] title='Total Net Revenue', x=None, y=net_revenue
# Widget 2: [ChartType.KPI] title='Total Net Revenue Eur', x=None, y=net_revenue_eur
# Widget 3: [ChartType.LINE] title='Monthly Net Revenue Trend', x=order_date, y=net_revenue
# Widget 4: [ChartType.BAR] title='Gross vs Net Revenue by Product Category', x=product_category, y=None
# Widget 5: [ChartType.SCATTER] title='Net Revenue Eur vs Net Revenue', x=net_revenue_eur, y=net_revenue
# Widget 6: [ChartType.SMART_NARRATIVE] title='Executive Smart Narrative', x=None, y=None
```

---

## 8. PROFILING HOLDOVERS: `units_sold` CONTINUOUS & `loyalty_tier_code <-> units_sold` FOREIGN KEY

### 8.1 Verbatim Source Code & Location
#### 1. `units_sold` Continuous Classification
In [type_detector.py](file:///c:/abbu/backend/profiling/type_detector.py#L276-L293):
```python
        # ── Continuous vs Discrete ──
        if pd.api.types.is_integer_dtype(non_null):
            n_unique = non_null.nunique()
            if n_unique <= 20 and n > 50:
                return SemanticType.NUMERIC_DISCRETE, 0.85
            return SemanticType.NUMERIC_DISCRETE, 0.75
        else:
            # Float — check if actually integer-valued
            try:
                is_int = (non_null == non_null.astype(int)).all()
                if is_int:
                    n_unique = non_null.nunique()
                    if n_unique <= 20 and n > 50:
                        return SemanticType.NUMERIC_DISCRETE, 0.80
            except (ValueError, OverflowError):
                pass

            return SemanticType.NUMERIC_CONTINUOUS, 0.80
```

#### 2. Spurious Foreign Key Between `loyalty_tier_code` and `units_sold`
In [key_detector.py](file:///c:/abbu/backend/profiling/key_detector.py#L117-L132):
```python
                intersection = len(values_a.intersection(values_b))
                overlap_a = intersection / len(values_a)
                overlap_b = intersection / len(values_b)

                # Check if A is largely a subset of B
                if overlap_a >= 0.95:
                    candidates.append(KeyCandidate(
                        columns=[col_a, col_b],
                        uniqueness=overlap_a,
                        justification=(
                            f"{overlap_a:.1%} of unique values in '{col_a}' exist in "
                            f"'{col_b}' ({len(values_b)} unique values). "
                            f"Potential join key relationship."
                        ),
                    ))
```

### 8.2 Forensic Mechanism & Root Cause
1. **`units_sold` Continuous**: In `prism_adversarial_e2e_test.csv`, `units_sold` contains 1 `NaN` value. In pandas, integer columns containing missing values are represented as `float64`. Line 276 `is_integer_dtype` evaluates to False, entering the `else:` branch. Line 284 confirms `is_int = True` because all non-null values are exact integers (`-5.0, 1.0, 2.0, ..., 24.0`). However, line 287 requires `n_unique <= 20`. Because `units_sold` has 25 distinct values ($25 \le 20 \rightarrow \text{False}$), it fails the check and falls through to line 292: `return SemanticType.NUMERIC_CONTINUOUS, 0.80`. The detector arbitrarily designated integer sales counts as continuous purely because cardinality exceeded 20.
2. **Foreign Key `loyalty_tier_code <-> units_sold`**: `loyalty_tier_code` has unique values `{1, 2, 3, 4}`. `units_sold` has unique values `{-5, 1, 2, 3, 4, 5, ..., 24}`. The set `{1, 2, 3, 4}` is a 100% mathematical subset of `units_sold` (`overlap_a = 1.0 >= 0.95`). `KeyDetector.detect_foreign_keys` performs set-subset comparisons without checking column semantics, naming conventions, or data categories. It flagged a customer loyalty tier as a foreign key of item sales quantity.

### 8.3 Raw Runtime Output Verification
```python
# Type detector on units_sold:
units_s = df['units_sold']
st, conf = SemanticTypeDetector.detect(units_s, 'units_sold')
print(f"units_sold: dtype={units_s.dtype}, nunique={units_s.nunique()}, classified={st}, conf={conf}")
# Output:
# units_sold: dtype=float64, nunique=25, classified=SemanticType.NUMERIC_CONTINUOUS, conf=0.8

# KeyDetector on foreign keys:
fk_candidates = KeyDetector.detect_foreign_keys(df)
for fk in fk_candidates:
    print(f"FK: {fk.columns}, overlap={fk.uniqueness}, justification={fk.justification}")
# Output:
# FK: ['loyalty_tier_code', 'units_sold'], overlap=1.0, 
# justification=100.0% of unique values in 'loyalty_tier_code' exist in 'units_sold' (25 unique values). Potential join key relationship.
```

---

## 9. DUCKDB FLOAT SERIALIZATION PRECISION NOISE

### 9.1 Verbatim Source Code & Location
In [sql_engine.py](file:///c:/abbu/backend/sql/sql_engine.py#L388-L407):
```python
    @staticmethod
    def _serialize_row(row: tuple) -> list:
        """Serialize a DuckDB row to JSON-safe values."""
        return [_safe_val(v) for v in row]


def _safe_val(v: Any) -> Any:
    """Convert a value to JSON-safe type."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, (pd.Timestamp, np.datetime64)):
        return str(v)
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return v
```

And in [formatters.ts](file:///c:/abbu/frontend/src/components/dashboard/utils/formatters.ts#L6-L12):
```typescript
export function cleanNumber(val: any): number | null {
    if (val === null || val === undefined || val === '') return null;
    const num = Number(val);
    if (isNaN(num)) return null;
    // Fix floating-point precision issues
    return Math.round(num * 1e8) / 1e8;
}
```

### 9.2 Forensic Mechanism & Root Cause
1. DuckDB's internal C++ query engine performs double-precision binary floating point operations (`DOUBLE`).
2. When executing sums or averages across hundreds of rows, binary float arithmetic creates standard IEEE-754 precision artifacts (e.g. `2808625.6099999994` instead of `2808625.61`).
3. In `sql_engine.py:400`, `_safe_val` converts `np.floating` to standard Python `float(v)` without applying rounding (e.g., `round(v, 4)`), emitting raw float noise across all REST API payloads.
4. The frontend prevents UI corruption through `cleanNumber(val)` in `formatters.ts:11`, which rounds numbers to 8 decimal places (`Math.round(num * 1e8) / 1e8`), but raw backend API responses remain unquantized.

### 9.3 Raw Runtime Output Verification
```python
# SQL Engine query execution:
eng = SQLEngine()
eng.register_dataframe(df, 'test_table')
res = eng.execute('SELECT "product_category", SUM("net_revenue") AS sum_net, AVG("discount_rate") AS avg_disc FROM "test_table" GROUP BY "product_category"')

for r in res['rows'][:3]:
    print(r)
# Output:
# {'product_category': 'Enterprise Software', 'sum_net': 2808625.6099999994, 'avg_disc': 0.11590909090909095}
# {'product_category': 'Consulting', 'sum_net': 1509204.0500000003, 'avg_disc': 0.12906976744186044}
# {'product_category': 'Hardware', 'sum_net': 115770.59000000003, 'avg_disc': 0.1340909090909091}
```

---

## 10. AUDIT RUNNER SCRIPT CHECK

### 10.1 Verbatim Source Code & Location
In [run_full_functional_audit.py](file:///c:/abbu/backend/tests/run_full_functional_audit.py#L203-L210):
```python
        "vif_multicollinearity": {
            k: round(v, 2)
            for k, v in (
                corr_analysis.get("multicollinearity", {}).get("vif_scores", {})
                if isinstance(corr_analysis, dict)
                else (getattr(corr_analysis.multicollinearity, "vif_scores", {}) if getattr(corr_analysis, "multicollinearity", None) else {})
            ).items()
        },
```

In [run_full_functional_audit.py](file:///c:/abbu/backend/tests/run_full_functional_audit.py#L353-L360):
```python
        "anomalies_sample": [
            {
                "col": a.get("column") if isinstance(a, dict) else getattr(a, "column", ""),
                "type": a.get("anomaly_type") if isinstance(a, dict) else getattr(a, "anomaly_type", ""),
                "severity": a.get("severity") if isinstance(a, dict) else getattr(a, "severity", ""),
                "desc": a.get("description") if isinstance(a, dict) else getattr(a, "description", ""),
            }
            for a in anomalies[:5]
        ],
```

In [generate_audit_markdown.py](file:///c:/abbu/backend/tests/generate_audit_markdown.py#L391-L407):
```python
## 15. COMPREHENSIVE REPERTOIRE VERIFICATION VERDICT

| Functional Discipline | Total Modules Audited | Errors Observed | Regressions | Verification Outcome |
| :--- | :---: | :---: | :---: | :---: |
| **Ingestion & RFC-4180 Parsing** | 4 | 0 | 0 | **100% PERFECT** |
| **Deep Profiling & 27 Taxonomy** | 6 | 0 | 0 | **100% PERFECT** |
| **Correlations & Multicollinearity** | 3 | 0 | 0 | **100% PERFECT** |
| **Mathematical Formula Engine** | 2 | 0 | 0 | **100% PERFECT** |
| **Autonomous Target & Leakage Guard** | 2 | 0 | 0 | **100% PERFECT** |
| **Autonomous Decision Engine** | 12 | 0 | 0 | **100% PERFECT** |
| **Sandboxed DuckDB SQLEngine** | 3 | 0 | 0 | **100% PERFECT** |
| **Semantic Model & DAX Measures** | 4 | 0 | 0 | **100% PERFECT** |
| **AI Dashboard & Prompt-to-SQL** | 5 | 0 | 0 | **100% PERFECT** |
| **AI Super-Visuals & Forecasting** | 5 | 0 | 0 | **100% PERFECT** |
| **Executive Story Studio Deck** | 3 | 0 | 0 | **100% PERFECT** |
| **TOTALS** | **49 Functions / 14 Modules** | **0 Errors** | **0 Regressions** | **ALL TESTS PASSED** |
```

### 10.2 Forensic Mechanism & Root Cause
1. **Empty VIF / MI Dump**: The audit runner script did not fail when VIF and Mutual Information were empty; it serialized `{}` directly into `backend/tests/full_functional_audit_dump.json`.
2. **Anomaly Column Defaulting**: In line 355, it queried `getattr(a, "column", "")` instead of `getattr(a, "feature", "")`. This dumped `""` for `col` into JSON, leading `generate_audit_markdown.py` to label all anomalies as `'dataset_level'`.
3. **Hardcoded Static Verdict**: Lines 391-407 of `generate_audit_markdown.py` do not calculate error or regression metrics from the test execution. The table is a hardcoded static markdown template proclaiming `100% PERFECT` across all 11 disciplines, creating a deceptive false-positive pass report while critical defects remained active in the engine.

---

## CONCLUSION & VERIFICATION PROOF

All 10 defects have been verified via read-only inspection and small non-destructive Python execution snippets. No source code was altered during this investigation. Every finding has been mapped to exact line numbers and concrete mechanisms.
