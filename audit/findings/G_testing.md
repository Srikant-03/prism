# Dimension G: Testing

Audit of unit, integration, and e2e test coverage, critical paths with zero tests, brittle tests, missing fixtures, test isolation, and CI enforcement.

---

### Finding TEST-001: Test Collection Fails in Clean Environments Due to Unmocked External LLM Client
- **ID**: TEST-001
- **Title**: Pytest Crashes During Test Collection Because `test_models.py` Instantiates `genai.Client` at Module Level
- **Type**: BUG
- **Severity**: P0
- **Location**: `backend/tests/test_models.py:5-8`
- **Evidence**:
  ```python
  key = os.environ.get("GEMINI_API_KEY", "")
  client = genai.Client(api_key=key)  # Executes on import!
  ```
  Command execution output:
  ```text
  ERROR collecting backend/tests/test_models.py
  ValueError: No API key was provided. Please pass a valid API key.
  !!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
  ```
- **Impact**: Any clean CI runner, new developer machine, or air-gapped test suite without a valid Google Gemini API key exported in the environment immediately fails `pytest` with exit code 1 before executing a single test.
- **Root cause**: Instantiating network-dependent third-party SDK clients at top-level module scope rather than using pytest fixtures or mocks.
- **Recommended fix**: Wrap LLM tests with `@pytest.mark.skipif(not os.getenv("GEMINI_API_KEY"), reason="No API key")` or mock `genai.Client` using `unittest.mock.MagicMock`.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding TEST-002: Complete Failure of Frontend Test Runner (`vitest`)
- **ID**: TEST-002
- **Title**: Vitest Worker Process Times Out on Windows Preventing Frontend Test Execution
- **Type**: BUG
- **Severity**: P1
- **Location**: `frontend/src/test/components.test.tsx`, `frontend/vite.config.ts`
- **Evidence**:
  ```text
  Error: [vitest-pool]: Failed to start forks worker for test files C:/abbu/frontend/src/test/components.test.tsx.
  Caused by: Error: [vitest-pool-runner]: Timeout waiting for worker to respond
  Duration 60.17s
  ```
- **Impact**: Developers and automated systems cannot execute tests against the 77 frontend React components. All frontend changes are deployed completely unverified by unit or component tests.
- **Root cause**: Vitest fork pool configuration conflict with Node/Windows IPC or missing pool options in `vite.config.ts`.
- **Recommended fix**: Configure `test: { pool: 'threads' }` in `vite.config.ts` and set proper worker timeouts for Windows environments.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding TEST-003: Total Absence of Continuous Integration (CI/CD) Automation
- **ID**: TEST-003
- **Title**: No GitHub Actions, GitLab CI, or Automated Validation Workflows Configured
- **Type**: MISSING
- **Severity**: P1
- **Location**: Repository root (`.github/` does not exist)
- **Evidence**:
  [VERIFIED] Directory listing shows no `.github/`, `.gitlab-ci.yml`, or Azure Pipelines files.
- **Impact**: Broken code, syntax errors, and regressions can be pushed directly to `main` without automated linting, type-checking, or test validation.
- **Root cause**: Early-stage prototype workflow relying exclusively on local batch scripts (`start.bat`).
- **Recommended fix**: Create `.github/workflows/ci.yml` running linting (`ruff`, `eslint`), type-checking (`pyright`, `tsc`), and test suites (`pytest`, `vitest`) on every push and pull request.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding TEST-004: Zero Automated End-to-End (E2E) Browser Tests
- **ID**: TEST-004
- **Title**: No Playwright or Cypress Suites to Validate Full User Workflows
- **Type**: MISSING
- **Severity**: P2
- **Location**: Repository root
- **Evidence**:
  No E2E test framework installed in `frontend/package.json` or backend.
- **Impact**: Critical end-to-end user journeys (uploading CSV → automatic profiling → rendering charts → applying cleaning recipe → exporting report) are never validated automatically. Subtile UI bugs or broken API contracts can easily slip into production.
- **Root cause**: E2E testing deferred during rapid feature addition.
- **Recommended fix**: Add Playwright test suite covering top 3 core user journeys.
- **Confidence**: High
- **Label**: [VERIFIED]
