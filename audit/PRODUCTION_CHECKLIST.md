# Production Readiness Checklist & Master Plan

## Production Readiness Scorecard

| Dimension | Score (0-5) | One-Line Justification | Top Blocker |
|---|---|---|---|
| **A. Functional Completeness** | 3.5 / 5 | Core wrangling works; ZIP archive and multi-file re-ingestion are broken stubs. | Ingestion drops extracted archive files (`FC-001`) |
| **B. Correctness and Bugs** | 3.0 / 5 | Zip Slip arbitrary file overwrite and DuckDB concurrency bugs present. | Zip Slip vulnerability in compressed parser (`BUG-001`) |
| **C. Error Handling & Resilience** | 2.5 / 5 | Unhandled long-running timeouts and global asyncio lock lifecycle flaws. | Unbounded CPU tasks stall entire worker (`ERR-001`) |
| **D. Security** | 1.0 / 5 | Public API secret in SPA bundle, trivial global DoS, SSRF in monitor. | Global DoS in auth lockout (`SEC-002`) & SSRF (`SEC-001`) |
| **E. Data Integrity & Persistence** | 2.0 / 5 | Cleaned data silently discarded due to mutable subscript bug on DiskCache. | `update_stored_dataframe` silent data loss (`DATA-001`) |
| **F. Performance & Scalability** | 2.0 / 5 | Architecture locked to single-process Windows; horizontal scaling impossible. | In-memory DuckDB + local diskcache (`PERF-001`) |
| **G. Testing** | 2.0 / 5 | Pytest fails collection in clean CI; vitest broken; zero CI workflows. | External LLM crashes test collection (`TEST-001`) |
| **H. Observability** | 2.0 / 5 | Free-text unstructured console logs; zero APM or Prometheus metrics. | Missing structured logs and metrics (`OBS-001`) |
| **I. Configuration & Secrets** | 3.0 / 5 | Plain class env vars with no pydantic validation; soft startup warnings. | No fail-fast startup on missing secrets (`CFG-002`) |
| **J. Build, Deploy & Operations** | 1.5 / 5 | No Dockerfile, no CI/CD; Windows-only `.bat` scripts with hardcoded taskkill. | Zero containerization or Linux support (`OPS-001`) |
| **K. Code Quality & Debt** | 3.0 / 5 | Good modularity in analyzers; massive monolithic exporters and scratch files. | Monolithic 1200+ line report generator (`QUAL-001`) |
| **L. Documentation** | 3.0 / 5 | Good user quickstart; README claims multi-user enterprise readiness prematurely. | Inaccurate architectural scale claims (`DOC-001`) |
| **M. UX / API Contract** | 3.5 / 5 | Rich UI components; fragmented error envelopes and unversioned routes. | Inconsistent error schemas across routes (`API-001`) |
| **N. Legal & Compliance** | 2.5 / 5 | Schema and sample data sent to cloud Gemini API without user consent dialog. | Undisclosed outbound LLM telemetry (`LEG-001`) |
| **O. Domain-Specific Risks** | 3.0 / 5 | Indirect prompt injection via malicious data columns; no DuckDB memory caps. | Indirect prompt injection via table headers (`DOM-001`) |

**Overall Readiness Score: 2.5 / 5.0 (Developer Prototype — NOT Production Ready)**

---

## Launch Gate (Must Complete Before Any Real User Touches This)
The minimal set of 8 critical items required before permitting untrusted user interaction:
1. **[SEC-001 / BUG-001]** Fix Zip Slip path traversal and eliminate SSRF / local file reads in `/api/monitor/schedule`.
2. **[SEC-002]** Replace `"global"` lockout key in `verify_api_key` with client-IP based rate tracking to prevent global DoS.
3. **[SEC-003]** Remove symmetric backend API key from public frontend client bundle; introduce proper user sessions or proxy routing.
4. **[DATA-001]** Fix `update_stored_dataframe` so cleaned DataFrames are actually written back to `_ingestion_store`.
5. **[TEST-001]** Mock Gemini API client in test collection so `pytest` passes cleanly in CI without live API credentials.
6. **[DOM-003]** Configure DuckDB `SET max_memory = '2GB'` to prevent client SQL queries from crashing the backend process.
7. **[LEG-001]** Display upfront consent modal before transmitting dataset schemas and samples to Google Gemini.
8. **[OPS-001]** Provide a standardized non-root `Dockerfile` so the system can be deployed and supervised predictably.

---

## Production Readiness Master Checklist

### M0: Blockers (Launch-Blocking Vulnerabilities & Build/Test Failures)
- [x] `CHK-M0-01` | Fix Zip Slip Path Traversal & Temp Directory Leaks | P0 | Effort: S (3h) | Depends on: None | Files: `backend/ingestion/parsers/compressed.py` | Acceptance: Path traversal attempts via zip headers fail with validation error; temp dirs auto-deleted | How to verify: Run exploit test with archive containing `../../evil.txt`
  - **Verified in commit `1799bfa`**: `test_zip_slip_security.py` passes 2/2 (path traversal attempt blocked with security warning and temp directories safely pruned).
- [ ] `CHK-M0-02` | Eliminate SSRF and Arbitrary Local File Read in Monitor | P0 | Effort: S (4h) | Depends on: None | Files: `backend/api/monitor.py` | Acceptance: Disallow `file://` schemes, loopback IPs (127.0.0.1), and cloud metadata IP (169.254.169.254) | How to verify: `curl -X POST http://localhost:8000/api/monitor/schedule` with `file:///etc/passwd` returns 400
- [ ] `CHK-M0-03` | Fix Global Denial of Service in Authentication Lockout | P0 | Effort: S (2h) | Depends on: None | Files: `backend/api/dependencies.py` | Acceptance: Brute-force lockout tracked per client IP using `request.client.host`, never globally | How to verify: Send 11 failed requests from IP A; verify IP B can still authenticate successfully
- [ ] `CHK-M0-04` | Fix Silent Data Loss in `update_stored_dataframe` | P0 | Effort: S (2h) | Depends on: None | Files: `backend/ingestion/orchestrator.py` | Acceptance: Cleaned dataframe modifications are re-persisted to `_ingestion_store[file_id] = stored` | How to verify: Execute cleaning action and assert subsequent `get_df(file_id)` reflects changes
- [ ] `CHK-M0-05` | Mock External Gemini Client in Test Collection | P0 | Effort: S (2h) | Depends on: None | Files: `backend/tests/test_models.py` | Acceptance: `pytest` runs and passes with zero errors on clean environment without `GEMINI_API_KEY` | How to verify: `unset GEMINI_API_KEY && pytest backend/tests/test_models.py`
- [ ] `CHK-M0-06` | Remove Public API Secret from Frontend Client Bundle | P0 | Effort: M (6h) | Depends on: `CHK-M0-03` | Files: `frontend/src/api/ingestion.ts`, `frontend/src/api/client.ts` | Acceptance: No secret authentication tokens embedded in client-side Vite distribution bundles | How to verify: Inspect `frontend/dist/assets/*.js` and verify no API keys present
- [ ] `CHK-M0-07` | Create Production Multi-Stage Dockerfile | P0 | Effort: M (6h) | Depends on: None | Files: `Dockerfile`, `docker-compose.yml`, `.dockerignore` | Acceptance: Container builds with non-root user and serves frontend and backend on Linux | How to verify: `docker build -t prism:latest . && docker run -p 8000:8000 prism:latest`

### M1: Core Completion (Finishing Incomplete Features)
- [ ] `CHK-M1-01` | Implement Multi-File Archive Extraction Pipeline | P1 | Effort: M (8h) | Depends on: `CHK-M0-01` | Files: `backend/ingestion/orchestrator.py` | Acceptance: Uploading a zip of multiple CSVs loads all files into DuckDB and returns dataset manifest | How to verify: Upload multi-file zip via `/api/upload` and assert all tables present
- [ ] `CHK-M1-02` | Fix Business Rules Outlier Detection Regression | P1 | Effort: S (4h) | Depends on: None | Files: `backend/cleaning/outlier_handler.py` | Acceptance: `test_outlier_business_rules` passes without assertion failures | How to verify: Run `pytest backend/tests/test_cleaning.py -k test_outlier_business_rules`
- [ ] `CHK-M1-03` | Fix Async Lock Initialization in `APIKeyManager` | P1 | Effort: S (3h) | Depends on: None | Files: `backend/llm/api_manager.py` | Acceptance: `asyncio.Lock()` created lazily inside running event loop; `get_status()` awaited | How to verify: Call `get_status()` across multiple async workers without runtime errors
- [ ] `CHK-M1-04` | Implement Unsaved Changes Guard in Frontend | P2 | Effort: S (3h) | Depends on: None | Files: `frontend/src/App.tsx` | Acceptance: Browser warns user before navigating away if cleaning recipe or SQL query is dirty | How to verify: Modify dataset cell, attempt to close tab, verify native confirmation prompt

### M2: Correctness, Security Hardening & Concurrency
- [ ] `CHK-M2-01` | Protect DuckDB In-Memory Connection Concurrency | P1 | Effort: S (4h) | Depends on: None | Files: `backend/sql/sql_engine.py` | Acceptance: Thread-safe locks or separate cursors used for concurrent queries | How to verify: Run 20 concurrent SQL queries via `asyncio.gather` without crashing DuckDB
- [ ] `CHK-M2-02` | Enforce Strict Memory & Thread Limits on DuckDB | P1 | Effort: S (2h) | Depends on: None | Files: `backend/sql/sql_engine.py` | Acceptance: DuckDB configured with `SET max_memory = '2GB'` and query limits | How to verify: Execute heavy cross-join and assert DuckDB aborts with memory cap error rather than OOM
- [ ] `CHK-M2-03` | Sanitize Dataset Values in External LLM Prompts | P1 | Effort: S (4h) | Depends on: None | Files: `backend/sql/nl_query.py`, `backend/chat/engine.py` | Acceptance: Column names and data samples wrapped in isolated XML delimiter tags | How to verify: Test prompt injection string in column name; verify LLM treats it as literal data
- [ ] `CHK-M2-04` | Modernize Deprecated Pandas and Datetime APIs | P2 | Effort: S (3h) | Depends on: None | Files: `backend/cleaning/type_handler.py`, `backend/cleaning/categorical_handler.py` | Acceptance: Zero `UserWarning` or `DeprecationWarning` emitted during test runs | How to verify: `pytest backend/tests/test_cleaning.py -W error::DeprecationWarning`

### M3: Reliability, Data Safety & Performance
- [ ] `CHK-M3-01` | Add Execution Timeouts to Long-Running Profiling Tasks | P1 | Effort: M (6h) | Depends on: None | Files: `backend/profiling/engine.py`, `backend/api/upload.py` | Acceptance: Complex dataset profiling halts gracefully after 120s timeout without stalling process | How to verify: Feed pathological dataset and assert clean 408/504 timeout response
- [ ] `CHK-M3-02` | Configure Bounded Eviction Policy on DiskCache Stores | P1 | Effort: S (3h) | Depends on: None | Files: `backend/state.py` | Acceptance: `eviction_policy="least-recently-stored"` configured on all TTLStores | How to verify: Fill cache beyond limit; verify oldest entries evicted without `DiskFullError`
- [ ] `CHK-M3-03` | Offload CPU-Bound Profiling to Background Executor | P1 | Effort: M (8h) | Depends on: `CHK-M3-01` | Files: `backend/api/profiling.py` | Acceptance: Incoming HTTP requests continue serving while dataset profiling executes | How to verify: Hit `/health` while profiling 50MB CSV; latency remains < 50ms

### M4: Testing, Observability & Operations
- [ ] `CHK-M4-01` | Fix Frontend Test Runner Execution on Windows/Node | P1 | Effort: S (4h) | Depends on: None | Files: `frontend/vite.config.ts`, `frontend/src/test/setup.ts` | Acceptance: `npm run test` executes component tests cleanly within 10s | How to verify: Run `npm run test` in `frontend/` and assert all test files pass
- [ ] `CHK-M4-02` | Create GitHub Actions CI Workflow | P1 | Effort: S (4h) | Depends on: `CHK-M0-05`, `CHK-M4-01` | Files: `.github/workflows/ci.yml` | Acceptance: Linting, type checks, and backend/frontend tests run on every commit | How to verify: Push branch and inspect green CI pipeline
- [ ] `CHK-M4-03` | Add Structured JSON Logging and Prometheus Metrics | P2 | Effort: M (6h) | Depends on: None | Files: `backend/main.py`, `backend/config.py` | Acceptance: Logs formatted as JSON with correlation IDs; `/metrics` exposes request counts | How to verify: `curl http://localhost:8000/metrics` returns valid Prometheus metrics text
- [ ] `CHK-M4-04` | Add Readiness Health Probe with Active Canary Tests | P2 | Effort: S (3h) | Depends on: None | Files: `backend/api/health.py` | Acceptance: `/health/ready` verifies DuckDB query execution and cache writeability | How to verify: Intentionally fill cache or break DuckDB; verify probe returns 503

### M5: Documentation, Polish & Compliance
- [ ] `CHK-M5-01` | Implement Outbound AI Data Consent Disclosure Modal | P1 | Effort: S (4h) | Depends on: None | Files: `frontend/src/components/chat/PrivacyDisclosure.tsx` | Acceptance: Users must explicitly acknowledge that schema metadata is sent to Google Gemini | How to verify: First chat or NL query triggers confirmation dialog before HTTP dispatch
- [ ] `CHK-M5-02` | Standardize RFC-7807 Error Response Payloads | P2 | Effort: M (6h) | Depends on: None | Files: `backend/main.py`, `backend/api/*` | Acceptance: All 4xx and 5xx responses conform to unified `{ "type", "title", "status", "detail" }` | How to verify: Send invalid request to upload, sql, and cleaning; assert matching JSON schema
- [ ] `CHK-M5-03` | Refactor Monolithic `report_generator.py` into Format Modules | P2 | Effort: M (8h) | Depends on: None | Files: `backend/reporting/report_generator.py` | Acceptance: Split into `reporting/exporters/{pdf,docx,html,ipynb}.py` | How to verify: Run `pytest backend/tests/test_reporting.py`
- [ ] `CHK-M5-04` | Clean Up Abandoned Root Debug Scripts | P3 | Effort: S (2h) | Depends on: None | Files: `backend/` | Acceptance: Remove `test_gemini*.py`, `patch_*.py` from backend root | How to verify: Assert backend root contains only canonical modules and configs
- [ ] `CHK-M5-05` | Update README and Environment Documentation | P2 | Effort: S (3h) | Depends on: None | Files: `README.md`, `backend/.env.example` | Acceptance: Accurate architecture scope, complete environment variables documented | How to verify: Review README matches actual local single-user architecture

---

## Standard Checks Verified as Already OK
- `[ALREADY OK, verified by backend/sql/sql_engine.py:110-150]` SQL injection protection in SQL Workbench: read-only validation AST parser blocks `DROP`, `DELETE`, `UPDATE`, `ALTER`, and multi-statement queries.
- `[ALREADY OK, verified by backend/ingestion/detector.py:45-120]` Encoding detection with `charset-normalizer` correctly handles non-UTF8 text files and BOM markers.
- `[ALREADY OK, verified by backend/cleaning/audit_logger.py:40-100]` Full undo/redo audit trail tracking in data cleaning operations with step serialization.
- `[ALREADY OK, verified by frontend/package.json:15-30]` Modern React 19 + TypeScript + Ant Design UI stack with zero vulnerabilities reported in npm audit.

---

## Risk Register (Residual Risks & Mitigations)

| Risk ID | Residual Risk Description | Impact | Likelihood | Mitigation Strategy |
|---|---|---|---|---|
| **RISK-01** | Google Gemini API Outages or Quota Depletion | AI-driven features (NL-to-SQL, chat, auto-dashboards) become unavailable | Medium | Maintain the regex/heuristic fallback mode with explicit UI degradation warnings; provide local Ollama LLM provider option. |
| **RISK-02** | Heavy Memory Consumption of Large Datasets (>500MB) | Host server Out-Of-Memory termination on low-spec VMs | Medium | Enforce `MAX_FILE_SIZE` strict limits at reverse proxy (Nginx `client_max_body_size 250M`); migrate DataFrame engine to PyArrow. |
| **RISK-03** | Local DiskCache Data Loss on Container Restart | Sessions and cached cleaning plans lost when ephemeral cloud containers cycle | High | For multi-instance cloud deployments, replace `.prism_cache` with a managed Redis cluster and S3 object storage. |
| **RISK-04** | Statistical Outlier Truncation Altering True Anomalies | Legitimate extreme data points (e.g. fraud, clinical extremes) removed | Medium | Require double-confirmation in UI for destructive outlier removal; keep uncleaned original table immutable in DuckDB. |
