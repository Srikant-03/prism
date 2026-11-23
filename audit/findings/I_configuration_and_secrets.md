# Dimension I: Configuration and Secrets

Audit of environment handling, config validation at startup, per-environment config, secrets management, feature flags, and defaults.

---

### Finding CFG-001: Configuration Relies on Raw Class-Level Evaluation Instead of Pydantic Settings
- **ID**: CFG-001
- **Title**: Import-Order Dependent `os.getenv` Calls Freeze Config State Prematurely
- **Type**: RISK
- **Severity**: P1
- **Location**: `backend/config.py:10-140`
- **Evidence**:
  ```python
  class LLMConfig:
      # NOTE: load_dotenv() MUST be called before this module is imported.
      GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
  ```
- **Impact**: Any test module, worker script, or CLI tool that imports `config.py` before `load_dotenv()` initializes all configuration settings to empty strings or defaults. The settings cannot be dynamically reloaded or validated. Furthermore, invalid environment types (e.g. passing `PORT=invalid`) trigger unhandled `ValueError` at module import time.
- **Root cause**: Using plain Python classes with top-level `os.getenv` instead of `pydantic-settings` (`BaseSettings`).
- **Recommended fix**: Migrate `AppConfig`, `LLMConfig`, and `IngestionConfig` to `pydantic_settings.BaseSettings` with typed fields, default validators, and `.env` auto-discovery.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding CFG-002: Incomplete Secret Key Validation at Startup
- **ID**: CFG-002
- **Title**: Application Boots Without Valid API Keys or Production Secret Assertions
- **Type**: RISK
- **Severity**: P1
- **Location**: `backend/main.py:62-67`, `backend/config.py:133`
- **Evidence**:
  ```python
  if AppConfig.API_KEY == "dev-secret-key-123":
      warnings_list.append("DATA_INTEL_API_KEY is using the default dev key...")
  ```
  Startup lifecycle only appends a warning to a logger; it does NOT abort startup in production mode (`ENVIRONMENT=production`).
- **Impact**: In a cloud deployment where a secrets manager fails or an environment variable is omitted, the backend quietly starts up with empty or default secret keys, exposing unprotected endpoints or failing silently on user actions.
- **Root cause**: Soft warning logging instead of strict fail-fast validation on startup.
- **Recommended fix**: In production mode (`ENVIRONMENT == "production"`), fail fast and abort startup if `DATA_INTEL_API_KEY` is missing or equal to default test values.
- **Confidence**: High
- **Label**: [VERIFIED]
