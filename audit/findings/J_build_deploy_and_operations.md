# Dimension J: Build, Deploy, and Operations

Audit of reproducible builds, lockfiles, containerization, CI/CD, environments, IaC, zero-downtime deploys, and disaster recovery.

---

### Finding OPS-001: Complete Absence of Dockerfile and Containerization
- **ID**: OPS-001
- **Title**: No Dockerfile, Container Image, or Process Supervisor Provided
- **Type**: MISSING
- **Severity**: P0
- **Location**: Repository root
- **Evidence**:
  [VERIFIED] Directory listing shows no `Dockerfile`, `docker-compose.yml`, or container spec anywhere in the project.
- **Impact**: The application cannot be deployed to standard container orchestrators (Kubernetes, AWS ECS, Google Cloud Run, Azure Container Apps). Deployment requires manual setup of Python 3.12, Node.js, and virtual environments on a host VM, preventing automated scaling, reproducible deploys, and immutable releases.
- **Root cause**: Project was built and maintained exclusively as a local Windows desktop prototype.
- **Recommended fix**: Create a multi-stage `Dockerfile` (Node.js stage building frontend Vite assets, Python 3.12-slim stage serving FastAPI with unprivileged non-root user).
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding OPS-002: Hardcoded Windows-Only Process Lifecycle Scripts
- **ID**: OPS-002
- **Title**: Lifecycle Controlled Exclusively by Windows Batch Scripts (`start.bat`, `stop.bat`)
- **Type**: RISK
- **Severity**: P1
- **Location**: `start.bat:1-55`, `stop.bat:1-15`
- **Evidence**:
  ```batch
  :: stop.bat
  for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8000 ^| findstr LISTENING') do (
      taskkill /PID %%a /F >nul 2>&1
  )
  ```
- **Impact**: The project cannot be started or managed on Linux or macOS systems. Furthermore, using `taskkill /F` forcibly SIGKILLs running Python processes, immediately terminating DuckDB disk writes and leaving `.prism_cache` SQLite databases in potential journal recovery or lock states.
- **Root cause**: Reliance on hardcoded Windows shell commands without graceful SIGTERM handling or cross-platform process management.
- **Recommended fix**: Provide cross-platform CLI entry points (e.g. `Makefile` or Python script `cli.py`) and adhere to standard SIGTERM signals for graceful FastAPI shutdown.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding OPS-003: Unpinned Dependencies and Missing Backend Lockfile
- **ID**: OPS-003
- **Title**: `backend/requirements.txt` Has Floating Specifiers Without a Deterministic Lockfile
- **Type**: RISK
- **Severity**: P1
- **Location**: `backend/requirements.txt:1-53`
- **Evidence**:
  Key packages (`python-dotenv`, `diskcache`, `nltk`, `google-genai`, `fpdf2`, `slowapi`) have zero version constraints. Other packages use floating `>=` bounds (`fastapi>=0.115.6`, `pandas>=2.2.3`).
- **Impact**: Builds are non-deterministic. A future release of `pandas`, `fastapi`, or `google-genai` introducing breaking API changes will silently break production builds without code changes.
- **Root cause**: No dependency management tool (Poetry, pip-tools, or `uv`) enforcing a locked manifest.
- **Recommended fix**: Standardize on `uv` or `pip-tools` and generate a verified, cryptographically-hashed `requirements.lock` committed to version control.
- **Confidence**: High
- **Label**: [VERIFIED]
