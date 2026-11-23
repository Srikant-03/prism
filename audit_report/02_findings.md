# 02 — Findings Register

This register details all architectural, security, functional, testing, and documentation defects identified during the hostile code audit of PRISM. Every finding is backed by verified file paths, exact line numbers, and runtime execution evidence.

Findings are sorted strictly by severity: **CRITICAL** > **HIGH** > **MEDIUM** > **LOW** > **INFO**.

---

## Summary of Findings

| ID | Title | Severity | Category | File & Line Range |
|---|---|---|---|---|
| `SEC-01` | Active Hardcoded & Leaked Gemini API Keys in Environment File | **CRITICAL** | Security / Secrets | `backend/.env:4,8` |
| `SEC-02` | Global Shared Brute-Force Lockout Enables Server-Wide Denial of Service (DoS) | **CRITICAL** | Security / Resilience | `backend/api/dependencies.py:39-46` |
| `SEC-03` | Monolithic Shared API Key Embedded into Client-Side Frontend Bundle | **HIGH** | Security / Auth | `frontend/.env:2`, `backend/config.py:133` |
| `SEC-04` | Complete Lack of Multi-Tenant Isolation & Authorization Checks in State Store | **HIGH** | Security / Data Isolation | `backend/state.py:28-55` |
| `BUG-01` | Temporal Aggregation Collapse: SQL Engine Emits Raw Rows Instead of Time Buckets | **HIGH** | Correctness / Analytics | `backend/dashboard/config_to_sql.py:143-147` |
| `BUG-02` | Unawaited Coroutine Object Returned in Synchronous API Status Method | **HIGH** | Reliability / Async Bug | `backend/llm/api_manager.py:148` |
| `TST-01` | Test Suite Execution Broken: Live API Call & Crash at Module Import | **HIGH** | Testing / CI/CD | `backend/tests/test_models.py:8`, `test_keys.py:1-28` |
| `TST-02` | SSRF Security Tests Fail Due to Hardcoded Mismatched Authentication Key | **HIGH** | Testing / Security | `backend/tests/test_monitor_ssrf_security.py:29,45` |
| `DOC-01` | Fictitious Architecture Claim: React Flow Advertised But Missing From Codebase | **MEDIUM** | Documentation / Integrity | `README.md:105`, `frontend/src/components/insights/RelationshipGraph.tsx:6` |
| `DOC-02` | Inaccurate Version Claims: React 18 Advertised While React 19 Is Installed | **MEDIUM** | Documentation / Compatibility | `README.md:11`, `frontend/package.json:26` |
| `TST-03` | Frontend Vitest Suite Broken With 33% Failure Rate and 99% Codebase Untested | **MEDIUM** | Testing / Quality | `frontend/src/test/components.test.tsx:18,27` |
| `PRF-01` | Extreme Computational Latency and Divide-By-Zero Warnings in Formula Profiling | **MEDIUM** | Performance / Numerical Stability | `backend/cleaning/leakage_handler.py:220`, `test_formula_detector.py` |
| `REL-01` | SQLEngine Lacks Execution Timeout Guard Against Unbounded Resource Exhaustion | **MEDIUM** | Reliability / DoS | `backend/sql/sql_engine.py:171-173` |
| `ING-01` | Concurrency Race Condition and File Overwrite in GZIP Extraction Handler | **MEDIUM** | Ingestion / Concurrency | `backend/ingestion/parsers/compressed.py:206-211` |
| `BLD-01` | Missing Root `pytest.ini` / `pyproject.toml` Prevents Standard Test Runner Discovery | **LOW** | Build / DevEx | Project Root |
| `BLD-02` | Circular Chunk Dependencies and 5.3MB Bloated Frontend Bundle | **LOW** | Frontend / Performance | `frontend/vite.config.ts:30-50` |

---

## Detailed Findings

### [SEC-01] Active Hardcoded & Leaked Gemini API Keys in Environment File
- **Severity**: **CRITICAL**
- **Category**: Security / Secrets Management
- **File & Lines**: `backend/.env#L4-L8`
- **Description**: The project's committed/local configuration file `backend/.env` contains an active primary `GEMINI_API_KEY` (`AIzaSyD9Wki2OsZ6QKhINjK4VESE5vOCck4pCqU`) and a comma-separated list of over 30 valid Gemini API keys under `GEMINI_API_KEYS`. While lines 7-8 claim "(Removed the 37 hardcoded public keys...)", line 8 continues to assign 32 active API keys in plaintext.
- **Reproduction**:
  ```bash
  type backend\.env
  ```
- **Evidence**:
  ```text
  Line 4: GEMINI_API_KEY="AIzaSyD9Wki2OsZ6QKhINjK4VESE5vOCck4pCqU"
  Line 8: GEMINI_API_KEYS="AIzaSyCcgkA_iiFJo5BuVZTHfncmZiLzZinu-BA,AIzaSyC_SydONGc47Kb2dkWUzDgn5xbf2xwIvaM..."
  ```
- **Impact**: Anyone with read access to the project directory or repository history gains complete access to the user's Google Cloud/Gemini API quota, allowing unauthorized API calls, potential quota depletion, financial liability if billing is linked, or immediate Google key revocation.
- **Recommended Fix**: Immediately revoke all exposed keys in Google AI Studio / GCP Console. Ensure `backend/.env` is strictly listed in `.gitignore` and only distributed as an empty template in `backend/.env.example`.

---

### [SEC-02] Global Shared Brute-Force Lockout Enables Server-Wide Denial of Service (DoS)
- **Severity**: **CRITICAL**
- **Category**: Security / Resilience / Rate Limiting
- **File & Lines**: `backend/api/dependencies.py#L39-L46`
- **Description**: The authentication middleware implements a brute-force lockout mechanism intended to stop credential stuffing. However, lines 35-46 record all authentication failures under a single static key `"global"` rather than per client IP:
  ```python
  if not received_key or received_key != AppConfig.API_KEY:
      _failed_attempts["global"].append(time.time())
      _check_lockout("global")
      raise HTTPException(status_code=401, detail="Invalid or missing API Key")
  ```
  When `len(_failed_attempts["global"]) >= 10`, `_check_lockout` raises `HTTP 429 Too Many Requests` with a 60-second lockout.
- **Reproduction**: Send 10 invalid requests (`curl -H "X-API-Key: invalid" http://localhost:8000/api/profile/any-id`). On the 11th request, even legitimate requests with the valid master API key fail with HTTP 429.
- **Evidence**: `backend/api/dependencies.py` lines 40-42:
  `_failed_attempts["global"].append(time.time())`
  `_check_lockout("global")`
- **Impact**: Any unauthenticated attacker on the network can send 10 garbage requests to completely shut down access to PRISM for all legitimate users worldwide every 60 seconds.
- **Recommended Fix**: Inject `Request` into `verify_api_key`, extract `request.client.host`, and track failed attempts per unique client IP address rather than globally.

---

### [SEC-03] Monolithic Shared API Key Embedded into Client-Side Frontend Bundle
- **Severity**: **HIGH**
- **Category**: Security / Architecture
- **File & Lines**: `frontend/.env#L2`, `backend/config.py#L133`, `backend/api/dependencies.py#L39`
- **Description**: PRISM claims to be a multi-feature business intelligence platform, yet its entire authentication boundary is guarded by a single static shared secret (`DATA_INTEL_API_KEY`, defaulting to `change-me-to-a-secure-secret`). This exact secret is copied into `frontend/.env` as `VITE_API_KEY`. In Vite/React architectures, any variable prefixed with `VITE_` is compiled into static JavaScript files shipped to every user's web browser.
- **Reproduction**:
  1. Inspect `frontend/.env`: `VITE_API_KEY=change-me-to-a-secure-secret`.
  2. Run `npm run build` in `frontend`.
  3. Inspect `dist/assets/index-*.js`: The string `change-me-to-a-secure-secret` is embedded in plaintext in client assets.
- **Evidence**:
  ```text
  frontend/.env:2 -> VITE_API_KEY=change-me-to-a-secure-secret
  backend/config.py:133 -> API_KEY: str = os.getenv("DATA_INTEL_API_KEY", "")
  ```
- **Impact**: There is zero authentication defense. Any user who loads the frontend UI has the master secret that authorizes administrative access to all 23 backend API endpoints, including deleting datasets, executing arbitrary SQL, and triggering server actions.
- **Recommended Fix**: Implement proper user sessions (JWT / OAuth2 / session cookies) and remove static administrative API keys from client-side bundles.

---

### [SEC-04] Complete Lack of Multi-Tenant Isolation & Authorization Checks in State Store
- **Severity**: **HIGH**
- **Category**: Security / Data Privacy & Isolation
- **File & Lines**: `backend/state.py#L28-L55`
- **Description**: In-memory state stores (`_ingestion_store`, `_profile_store`, `_cleaning_store`, `_target_store`, `_df_store`) are plain Python dictionaries keyed strictly by `file_id` (a UUID). There is no concept of user ownership, tenancy, workspace, or session authorization. Any caller who provides or guesses a `file_id` can read, modify, or drop datasets belonging to any other user.
- **Reproduction**: Call `GET /api/profile/{file_id}` or `POST /api/sql/query` with another user's `file_id`. The server returns the other user's raw dataset schema and analytical data without checking caller identity.
- **Evidence**: `backend/state.py`:
  ```python
  _df_store: dict[str, pd.DataFrame] = {}
  _ingestion_store: dict[str, dict] = {}
  _profile_store: dict[str, dict] = {}
  ```
- **Impact**: Zero multi-user privacy. If hosted on a shared server, User A can inspect, query, and exfiltrate User B's corporate datasets.
- **Recommended Fix**: Add a `user_id` / `org_id` foreign key to all state entries and verify in `verify_api_key` or route dependencies that the requesting user owns the requested `file_id`.

---

### [BUG-01] Temporal Aggregation Collapse: SQL Engine Emits Raw Rows Instead of Time Buckets
- **Severity**: **HIGH**
- **Category**: Correctness / Data Analytics
- **File & Lines**: `backend/dashboard/config_to_sql.py#L143-L147`, `backend/tests/test_temporal_grain_sql.py#L137,L171`
- **Description**: When generating DuckDB SQL for time-series charts with temporal grains (e.g. Monthly Revenue, Quarterly Growth), `config_to_sql` generates:
  ```sql
  SELECT strftime(DATE_TRUNC('month', TRY_CAST("date" AS DATE)), '%Y-%m') AS "date", SUM(revenue) AS "revenue"
  FROM "upload_test"
  GROUP BY "date"
  ORDER BY "date" ASC
  ```
  In DuckDB and SQL standard, `GROUP BY "date"` binds to the underlying table column `"upload_test"."date"` (which holds distinct days) rather than the projected alias `"date"`!
  As a result, DuckDB does NOT aggregate by month. It returns 111 raw daily rows where multiple rows have the identical label `'2023-01'`.
- **Reproduction**: Run `backend/venv/Scripts/python.exe -m pytest backend/tests/test_temporal_grain_sql.py -v`:
  ```text
  AssertionError: Expected 18 months, got 111
  AssertionError: Expected 6 quarters, got 111
  ```
- **Evidence**:
  ```python
  # backend/tests/test_temporal_grain_sql.py line 137
  assert len(result) == 18, f"Expected 18 months, got {len(result)}"
  # Returned: 111 rows instead of 18
  ```
- **Impact**: Charts rendered in the dashboard display erratic, unaggregated scatter points with duplicated month labels on the X-axis rather than accurate monthly totals, producing completely invalid business intelligence metrics.
- **Recommended Fix**: Name the alias distinctly (e.g. `AS "date_bucket"`) and specify `GROUP BY "date_bucket"` or `GROUP BY 1` in the generated SQL statement.

---

### [BUG-02] Unawaited Coroutine Object Returned in Synchronous API Status Method
- **Severity**: **HIGH**
- **Category**: Reliability / Async Bug
- **File & Lines**: `backend/llm/api_manager.py#L148`
- **Description**: In `APIKeyManager.get_status()`, the synchronous method calls `self.all_keys_exhausted()` which is an `async def` coroutine function:
  ```python
  def get_status(self) -> dict:
      return {
          ...
          "all_exhausted": self.all_keys_exhausted(),  # BUG: Missing await / called synchronously!
          ...
      }
  ```
  Calling an async coroutine without `await` does not execute it; it returns a `<coroutine object APIKeyManager.all_keys_exhausted at 0x...>`.
- **Reproduction**:
  ```python
  from llm.api_manager import APIKeyManager
  km = APIKeyManager(['key1'])
  print(km.get_status()['all_exhausted'])
  # Output: <coroutine object APIKeyManager.all_keys_exhausted at 0x...>
  # RuntimeWarning: coroutine 'APIKeyManager.all_keys_exhausted' was never awaited
  ```
- **Evidence**: Direct runtime verification yielded:
  `all_exhausted type: <class 'coroutine'>`
  `RuntimeWarning: coroutine 'APIKeyManager.all_keys_exhausted' was never awaited`
- **Impact**: Any endpoint or component serializing API pool status will crash with `TypeError: Object of type coroutine is not JSON serializable` or evaluate `"all_exhausted"` as truthy (since all Python objects evaluate to True in boolean contexts).
- **Recommended Fix**: Compute exhaustion status synchronously or define `all_keys_exhausted_sync()`:
  `"all_exhausted": len(self.exhausted_keys) >= len(self.keys) if self.keys else True`.

---

### [TST-01] Test Suite Execution Broken: Live API Call & Crash at Module Import
- **Severity**: **HIGH**
- **Category**: Testing / CI/CD
- **File & Lines**: `backend/tests/test_models.py#L8`, `backend/tests/test_keys.py#L1-28`
- **Description**: The file `backend/tests/test_models.py` is named with the pytest prefix `test_*.py` but contains no test functions. Instead, it executes live Google GenAI initialization at module root:
  ```python
  key = os.environ.get("GEMINI_API_KEY", "")
  client = genai.Client(api_key=key) # Crashes if key is empty!
  ```
  When running `pytest` out-of-the-box in an environment without `GEMINI_API_KEY` exported in the shell, pytest imports `test_models.py`, crashes with `ValueError: No API key was provided`, and halts test collection for the entire test suite.
- **Reproduction**:
  ```bash
  backend\venv\Scripts\python.exe -m pytest backend/tests/test_models.py
  # ValueError: No API key was provided. Please pass a valid API key.
  # Interrupted: 1 error during collection
  ```
- **Evidence**: Terminal output from `pytest backend/tests/test_models.py`:
  `ERROR backend/tests/test_models.py - ValueError: No API key was provided`
- **Impact**: Automated CI/CD pipelines and developer test runs fail immediately on fresh checkouts.
- **Recommended Fix**: Move scratch scripts out of `backend/tests/` or rename to `scripts/benchmark_models.py` and wrap live network calls inside `@pytest.mark.skipif(not os.getenv("GEMINI_API_KEY"), reason="API key missing")`.

---

### [TST-02] SSRF Security Tests Fail Due to Hardcoded Mismatched Authentication Key
- **Severity**: **HIGH**
- **Category**: Testing / Security Validation
- **File & Lines**: `backend/tests/test_monitor_ssrf_security.py#L29,L45`
- **Description**: `test_monitor_ssrf_security.py` verifies that malicious URLs (`file:///etc/passwd`, `http://169.254.169.254`, `http://localhost`) are blocked by `/api/monitor/schedule`. However, the test hardcodes:
  ```python
  headers = {"X-API-Key": "dev-secret-key-123"}
  ```
  while `backend/.env` is configured with `DATA_INTEL_API_KEY=change-me-to-a-secure-secret`.
  Both tests fail with `assert 401 == 400`, meaning the SSRF filtering logic is never actually tested or proven functional during automated test runs!
- **Reproduction**:
  ```bash
  backend\venv\Scripts\python.exe -m pytest backend/tests/test_monitor_ssrf_security.py
  # FAILED test_monitor_blocks_file_scheme - assert 401 == 400
  # FAILED test_monitor_blocks_loopback_and_metadata_ip - assert 401 == 400
  ```
- **Evidence**: Exact failure trace:
  `where 401 = <Response [401 Unauthorized]>.status_code`
- **Impact**: Security-critical regression tests give a false failure, leaving the real SSRF protection unverified by automated CI.
- **Recommended Fix**: Use `from config import AppConfig` and set `headers = {"X-API-Key": AppConfig.API_KEY}` in the test fixture.

---

### [DOC-01] Fictitious Architecture Claim: React Flow Advertised But Missing From Codebase
- **Severity**: **MEDIUM**
- **Category**: Documentation / System Integrity
- **File & Lines**: `README.md#L105`, `frontend/package.json#L14-L30`, `frontend/src/components/insights/RelationshipGraph.tsx#L6`
- **Description**: `README.md` line 105 states:
  > *"Interactive Relationship Graphs: Visualize cross-table foreign key links and column correlations through node-link relationship diagrams powered by React Flow."*
  In reality, `react-flow-renderer` / `@xyflow/react` is **not installed** in `frontend/package.json`. The relationship graph is actually rendered using **Apache ECharts** (`echarts-for-react`, line 6).
- **Reproduction**: Search `package.json` for `flow`: zero matches found. Inspect `RelationshipGraph.tsx:6`: `import ReactECharts from 'echarts-for-react'`.
- **Evidence**:
  ```typescript
  // frontend/src/components/insights/RelationshipGraph.tsx line 6
  import ReactECharts from 'echarts-for-react';
  ```
- **Impact**: False architectural claims in public-facing documentation misleading stakeholders and developers.
- **Recommended Fix**: Update `README.md` to accurately state that relationship visualizations are powered by Apache ECharts.

---

### [DOC-02] Inaccurate Version Claims: React 18 Advertised While React 19 Is Installed
- **Severity**: **MEDIUM**
- **Category**: Documentation / Compatibility
- **File & Lines**: `README.md#L11`, `frontend/package.json#L26-L27`
- **Description**: `README.md` badge line 11 advertises `React 18`. In reality, `frontend/package.json` specifies:
  `"react": "^19.2.0"`, `"react-dom": "^19.2.0"`.
  React 19 introduces major architectural changes (e.g. removal of `defaultProps`, changes to ref handling, concurrent rendering changes) which directly caused warnings and timeout failures in Vitest (`@testing-library/react`).
- **Reproduction**: Compare `README.md:11` badge with `frontend/package.json:26`.
- **Evidence**: `frontend/package.json`:
  ```json
  "react": "^19.2.0",
  "react-dom": "^19.2.0"
  ```
- **Impact**: Documentation mismatch and potential runtime incompatibilities with third-party libraries not yet audited for React 19.
- **Recommended Fix**: Update README badge and documentation to declare React 19 compatibility.

---

### [TST-03] Frontend Vitest Suite Broken With 33% Failure Rate and 99% Codebase Untested
- **Severity**: **MEDIUM**
- **Category**: Testing / Quality Assurance
- **File & Lines**: `frontend/src/test/components.test.tsx#L18,L27`
- **Description**: The frontend contains 98 TypeScript/TSX files and 440 components, hooks, and services. Only ONE single test file exists (`src/test/components.test.tsx`), containing exactly 6 test cases. When executed (`npm test`), 2 out of the 6 tests fail (33.3% test failure rate):
  1. `renders without crashing`: Times out after 5000ms.
  2. `cycles through themes on click`: Fails with `TestingLibraryElementError: Found multiple elements with the role "button"`.
- **Reproduction**:
  ```bash
  npm --prefix frontend test
  # Test Files: 1 failed (1)
  # Tests: 2 failed | 4 passed (6)
  ```
- **Evidence**: Vitest test runner output:
  `FAIL src/test/components.test.tsx > ThemeToggle > renders without crashing`
  `FAIL src/test/components.test.tsx > ThemeToggle > cycles through themes on click`
- **Impact**: Zero reliable automated regression testing for the frontend. 97 out of 98 frontend components have literally 0% automated test coverage.
- **Recommended Fix**: Fix the element query in `components.test.tsx` (use `getAllByRole` or test-ids) and add comprehensive component test suites for `FileUploader`, `QueryWorkbench`, `DashboardPage`, and `ProfileDashboard`.

---

### [PRF-01] Extreme Computational Latency and Divide-By-Zero Warnings in Formula Profiling
- **Severity**: **MEDIUM**
- **Category**: Performance / Numerical Stability
- **File & Lines**: `backend/profiling/formula_detector.py`, `backend/cleaning/leakage_handler.py#L220`
- **Description**: Running `test_formula_detector.py` requires 35.7 seconds for only 11 tests. Profiling reveals:
  1. Combinatorial formula search performs unconstrained multi-column regressions.
  2. In `statsmodels/stats/outliers_influence.py:197`, when two columns are perfectly collinear ($R^2 = 1.0$), calculating Variance Inflation Factor ($VIF = 1 / (1 - R^2)$) triggers a `RuntimeWarning: divide by zero encountered in scalar divide`.
  3. `leakage_handler.py:220` calls `pd.to_datetime(sample, errors="coerce")` without specifying format, repeatedly falling back to `dateutil` and emitting 22 warnings.
- **Reproduction**: Run `backend/venv/Scripts/python.exe -m pytest backend/tests/test_formula_detector.py -v`.
- **Evidence**:
  `RuntimeWarning: divide by zero encountered in scalar divide: vif = 1. / (1. - r_squared_i)`
  `UserWarning: Could not infer format, so each element will be parsed individually...`
- **Impact**: Severe CPU bottlenecks on wide datasets (50+ columns) and console warning flooding that degrades production log readability.
- **Recommended Fix**: Clamp $R^2$ to $0.9999$ before VIF division to prevent zero-division infinity, and pass explicit format strings to `pd.to_datetime`.

---

### [REL-01] SQLEngine Lacks Execution Timeout Guard Against Unbounded Resource Exhaustion
- **Severity**: **MEDIUM**
- **Category**: Reliability / Resource Management
- **File & Lines**: `backend/sql/sql_engine.py#L171-L173`
- **Description**: While `SQLEngine` correctly disables DuckDB external file access and filters out DDL/DML keywords, it provides no execution timeout or memory limiter on queries:
  ```python
  result = self.conn.execute(sql)
  ```
  A client submitting a CPU-intensive query (e.g. Cartesian product `SELECT * FROM a, b, c` or recursive CTE) will block the DuckDB thread indefinitely, consuming 100% CPU and exhausting server RAM.
- **Reproduction**: Submit a query with `WITH RECURSIVE` or a 5-way cross-join of registered tables.
- **Evidence**: Code review of `backend/sql/sql_engine.py:137-200` confirms zero statement timeout configuration or `conn.interrupt()` thread timers.
- **Impact**: Single slow query can freeze the entire analytics engine and crash the server.
- **Recommended Fix**: Configure DuckDB statement timeouts (`SET max_memory='2GB'`, `SET statement_timeout='30s'`) and implement an async timeout wrapper using `asyncio.wait_for`.

---

### [ING-01] Concurrency Race Condition and File Overwrite in GZIP Extraction Handler
- **Severity**: **MEDIUM**
- **Category**: Ingestion / Concurrency Safety
- **File & Lines**: `backend/ingestion/parsers/compressed.py#L205-L211`
- **Description**: In `CompressedParser._parse_gzip()`:
  ```python
  inner_name = self.file_path.stem  # "data.csv" from "data.csv.gz"
  temp_file = config.TEMP_DIR / inner_name
  with gzip.open(str(self.file_path), "rb") as gz:
      with open(str(temp_file), "wb") as out:
          shutil.copyfileobj(gz, out)
  ```
  If two users concurrently upload a file named `data.csv.gz`, both threads write to the EXACT SAME path `temp/data.csv`. The files will interleave or overwrite each other mid-read, corrupting data.
- **Reproduction**: Upload two distinct `data.csv.gz` files concurrently.
- **Evidence**: `backend/ingestion/parsers/compressed.py` line 206 uses `self.file_path.stem` without a UUID or unique directory.
- **Impact**: Data corruption and cross-session data contamination during concurrent uploads.
- **Recommended Fix**: Generate a unique UUID subfolder: `temp_file = Path(tempfile.mkdtemp(dir=config.TEMP_DIR)) / inner_name`.

---

### [BLD-01] Missing Root `pytest.ini` / `pyproject.toml` Prevents Standard Test Runner Discovery
- **Severity**: **LOW**
- **Category**: Build Configuration / Developer Experience
- **File & Lines**: Project Root
- **Description**: The repository does not include a `pytest.ini`, `pyproject.toml`, or `setup.cfg`. Consequently, running `pytest` from the root directory sets `PYTHONPATH` to `.`, causing all backend imports (`from api...`, `from profiling...`) to fail with `ModuleNotFoundError`.
- **Reproduction**: Run `pytest` from root without setting `PYTHONPATH=backend`.
- **Evidence**: All 27 test files failed with `ModuleNotFoundError: No module named 'profiling'` / `'api'` / `'state'` until `PYTHONPATH=backend` was manually supplied.
- **Impact**: Onboarding friction; automated tools and new engineers cannot run tests without undocumented environment variable tweaks.
- **Recommended Fix**: Add `pytest.ini` to root with `pythonpath = backend`.

---

### [BLD-02] Circular Chunk Dependencies and 5.3MB Bloated Frontend Bundle
- **Severity**: **LOW**
- **Category**: Frontend / Performance
- **File & Lines**: `frontend/vite.config.ts#L30-L50`
- **Description**: Running `npm run build` succeeds but emits 4 circular chunk warnings:
  - `react-dom -> vendor -> react-dom`
  - `vendor -> runtime -> vendor`
  - `antd-icons -> antd-ui -> antd-icons`
  - `feature-cleaning -> feature-profiling -> feature-cleaning`
  The total uncompressed JavaScript output exceeds 5.3 MB (with ECharts taking 1.34 MB and AntD taking 1.1 MB).
- **Reproduction**: Run `npm --prefix frontend run build`.
- **Evidence**: Vite build log output:
  `Circular chunk: react-dom -> vendor -> react-dom`
  `dist/assets/echarts-BhJh3EWG.js: 1,337.43 kB`
- **Impact**: Slower initial page load times on low-bandwidth connections and potential module evaluation order anomalies due to circular chunking.
- **Recommended Fix**: Refactor `manualChunks` in `vite.config.ts` to cleanly partition vendor dependencies and dynamically import ECharts.
