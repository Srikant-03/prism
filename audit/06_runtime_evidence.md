# 06 — Runtime Evidence

## Runtime Execution & Tooling Audit

This document records the exact commands, execution outputs, and failure points observed when executing builds, tests, and static checks in the local development environment.

---

### 1. Test Suite: Backend Test Runner (`pytest`)

#### Command Executed:
```powershell
c:\abbu\backend\venv\Scripts\pytest backend/tests/test_models.py
```

#### Output:
```text
C:\abbu\backend\venv\Lib\site-packages\pytest_asyncio\plugin.py:208: PytestDeprecationWarning: The configuration option "asyncio_default_fixture_loop_scope" is unset.
The event loop scope for asynchronous fixtures will default to the fixture caching scope. Future versions of pytest-asyncio will default the loop scope for asynchronous fixtures to function scope.

=================================== ERRORS ====================================
________________ ERROR collecting backend/tests/test_models.py ________________
backend\tests\test_models.py:8: in <module>
    client = genai.Client(api_key=key)
backend\venv\Lib\site-packages\google\genai\client.py:426: in __init__
    self._api_client = self._get_api_client(...)
backend\venv\Lib\site-packages\google\genai\_api_client.py:707: in __init__
    raise ValueError(
E   ValueError: No API key was provided. Please pass a valid API key. Learn how to create an API key at https://ai.google.dev/gemini-api/docs/api-key.
------------------------------- Captured stdout -------------------------------
Testing Key: ...NONE
============================== warnings summary ===============================
RequestsDependencyWarning: urllib3 (2.6.3) or chardet (7.0.0)/charset_normalizer (3.4.1) doesn't match a supported version!
=========================== short test summary info ===========================
ERROR backend/tests/test_models.py - ValueError: No API key was provided. Ple...
!!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
```

#### Verification & Analysis:
- **[VERIFIED] Critical Finding**: `test_models.py` has no mock or fixture for external LLM client creation. Running tests in a clean CI environment or on an air-gapped machine crashes the entire test collection step because `genai.Client` is invoked at top-level module load time.

---

### 2. Historical Test Run Artifact (`test_out.log`)

#### Evidence from `c:\abbu\test_out.log`:
```text
C:\abbu\backend\cleaning\type_handler.py:183: UserWarning: The argument 'infer_datetime_format' is deprecated and will be removed in a future version.
  dt = pd.to_datetime(val_str, dayfirst=False, infer_datetime_format=True)
Traceback (most recent call last):
  File "<string>", line 222, in test_outlier_business_rules
AssertionError: Should detect business rule violations

Results: 16 passed, 1 failed, 17 total
SOME TESTS FAILED
```

#### Verification & Analysis:
- **[VERIFIED] Regression in Cleaning Rules**: `test_outlier_business_rules` fails an assertion expecting domain business rule violations to be flagged.
- **[VERIFIED] Deprecated Pandas API**: `infer_datetime_format=True` is actively throwing `UserWarning` during dataset parsing.

---

### 3. Frontend Test Runner (`vitest`)

#### Command Executed:
```powershell
npm run test  (in c:\abbu\frontend)
```

#### Output:
```text
> frontend@0.0.0 test
> vitest run

 RUN  v4.0.18 C:/abbu/frontend

⎯⎯⎯⎯⎯⎯ Unhandled Errors ⎯⎯⎯⎯⎯⎯
Error: [vitest-pool]: Failed to start forks worker for test files C:/abbu/frontend/src/test/components.test.tsx.
Caused by: Error: [vitest-pool-runner]: Timeout waiting for worker to respond
 ❯ Timeout.<anonymous> node_modules/vitest/dist/chunks/cli-api.B7PN_QUv.js:7626:58

 Test Files  no tests
      Tests  no tests
     Errors  1 error
   Duration  60.17s
```

#### Verification & Analysis:
- **[VERIFIED] Broken Frontend Test Setup**: Frontend test pool fails to start on Windows; `components.test.tsx` hangs vitest worker until the 60s timeout elapses. Zero automated tests are functioning in CI/local runs for the frontend.

---

### 4. Dependency Version Mismatches

- **[VERIFIED] Incompatible urllib3 & charset-normalizer**:
  `RequestsDependencyWarning: urllib3 (2.6.3) or chardet (7.0.0)/charset_normalizer (3.4.1) doesn't match a supported version!`
  The frozen environment has conflicting HTTP client library versions that emit warnings on every HTTP request.
