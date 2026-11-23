# Dimension B: Correctness and Bugs

Audit of logic errors, race conditions, null/type handling, encoding/locale issues, edge cases, resource leaks, and idempotency problems.

---

### Finding BUG-001: Zip Slip Directory Traversal Vulnerability & Disk Leaks in Archive Parser
- **ID**: BUG-001
- **Title**: Unvalidated `extractall()` in Compressed File Parser (Zip Slip) and Uncleaned `tempfile.mkdtemp`
- **Type**: BUG
- **Severity**: P0
- **Location**: `backend/ingestion/parsers/compressed.py:116-121, 180-184`
- **Evidence**:
  ```python
  temp_dir = Path(tempfile.mkdtemp(dir=str(config.TEMP_DIR)))
  try:
      with zipfile.ZipFile(str(self.file_path), "r") as zf:
          zf.extractall(str(temp_dir))
  ```
- **Impact**:
  1. **Arbitrary File Overwrite**: An archive containing filenames with relative path elements (e.g. `../../config.py`) extracts files outside `temp_dir`, enabling remote code execution or overwriting backend source files.
  2. **Disk Resource Leak**: `tempfile.mkdtemp` is never deleted via `shutil.rmtree` either in a `finally` block or on error, causing server disk exhaustion under repeated archive uploads.
- **Root cause**: Use of raw `zipfile.ZipFile.extractall()` without validating target member paths against `os.path.commonpath`, and missing `try...finally` cleanup.
- **Recommended fix**: Validate each member path `target = (temp_dir / member.filename).resolve()` ensuring `temp_dir in target.parents`. Ensure `temp_dir` is registered in a context manager and deleted after processing.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding BUG-002: Regression in Business Rule Outlier Detection
- **ID**: BUG-002
- **Title**: Outlier Business Rules Test Fails Assertion Expecting Domain Violations
- **Type**: BUG
- **Severity**: P1
- **Location**: `test_out.log:5-6`, `backend/cleaning/outlier_handler.py:180-210`
- **Evidence**:
  `test_out.log:5-6`:
  ```text
  File "<string>", line 222, in test_outlier_business_rules
  AssertionError: Should detect business rule violations
  ```
- **Impact**: Domain-specific outlier rules (such as negative prices, percentages > 100%, impossible human ages) fail to trigger cleaning actions, leaving dirty or corrupted data in the active dataset.
- **Root cause**: Logic mismatch in `outlier_handler.py` between domain boundary checks and rule thresholds.
- **Recommended fix**: Align the domain rule evaluation logic in `outlier_handler.py` with boundary checks and add comprehensive parameterized unit tests.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding BUG-003: Deprecated Pandas & Python Datetime Functions Emit Deprecation Warnings and Risk Future Breakage
- **ID**: BUG-003
- **Title**: Widespread Use of Deprecated `infer_datetime_format`, `is_categorical_dtype`, and `datetime.utcnow()`
- **Type**: DEBT
- **Severity**: P2
- **Location**:
  - `backend/cleaning/type_handler.py:183` (`infer_datetime_format=True`)
  - `backend/cleaning/categorical_handler.py:108` (`pd.api.types.is_categorical_dtype`)
  - `backend/cleaning/imbalance_handler.py:129` (`pd.api.types.is_categorical_dtype`)
  - `backend/cleaning/audit_logger.py:32, 67, 308, 367` (`datetime.utcnow()`)
  - `backend/reporting/report_generator.py:43` (`datetime.utcnow()`)
- **Evidence**:
  Runtime warnings from test runs:
  `UserWarning: The argument 'infer_datetime_format' is deprecated and will be removed in a future version.`
  `DeprecationWarning: is_categorical_dtype is deprecated and will be removed in a future version. Use isinstance(dtype, pd.CategoricalDtype) instead`
  `DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).`
- **Impact**: Upgrading to Python 3.13+ or Pandas 3.0 will immediately crash dataset type inference, imbalance calculations, and report generation with `AttributeError` or `TypeError`.
- **Root cause**: Stale APIs used across multiple backend modules without type-checking enforcement.
- **Recommended fix**: Modernize to `isinstance(s.dtype, pd.CategoricalDtype)`, `datetime.now(timezone.utc)`, and remove `infer_datetime_format`.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding BUG-004: In-Memory DuckDB Connection Thread Safety and Race Conditions
- **ID**: BUG-004
- **Title**: Shared Global DuckDB Connection Lacks Thread Lock Protection During Concurrent Query Execution
- **Type**: BUG
- **Severity**: P1
- **Location**: `backend/sql/sql_engine.py:45-70`
- **Evidence**:
  ```python
  class SQLEngine:
      def __init__(self):
          self.con = duckdb.connect(database=":memory:")
  ```
- **Impact**: In FastAPI, asynchronous endpoints or concurrent worker threads querying or registering tables simultaneously against a shared DuckDB in-memory database connection can trigger cursor race conditions or internal DuckDB thread contention crashes.
- **Root cause**: DuckDB in-memory connections are not safe for simultaneous multi-threaded write/register operations without separate cursors or an explicit connection lock.
- **Recommended fix**: Use `threading.Lock` around DuckDB registration and cursor generation, or utilize connection pooling / cursor cloning via `self.con.cursor()`.
- **Confidence**: High
- **Label**: [VERIFIED]
