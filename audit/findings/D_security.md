# Dimension D: Security

Audit of authentication, authorization, access control (IDOR), injection (SQL/template/command), XSS/CSRF/SSRF, deserialization, secrets, and supply chain.

---

### Finding SEC-001: Server-Side Request Forgery (SSRF) and Arbitrary Local File Read in Data Freshness Monitor
- **ID**: SEC-001
- **Title**: Unvalidated `origin_url` in `/api/monitor/schedule` Enables SSRF and Arbitrary Local File Read
- **Type**: BUG
- **Severity**: P0
- **Location**: `backend/api/monitor.py:49, 129-133`
- **Evidence**:
  ```python
  class MonitorRequest(BaseModel):
      file_id: str
      origin_url: str
      cron_expr: str = "0 * * * *"

  async def _run_monitor_job(job_id: str):
      ...
      if origin_url.startswith("http") or origin_url.endswith(".csv"):
          new_df = await asyncio.to_thread(pd.read_csv, origin_url)
  ```
- **Impact**: Attackers can submit internal network URLs (e.g., `http://169.254.169.254/latest/meta-data/` on AWS/GCP or `http://localhost:8080/internal/admin`) or local files (`file:///etc/passwd` or `C:/Windows/win.ini.csv`) to read sensitive internal server infrastructure data or exploit unauthenticated internal microservices.
- **Root cause**: Direct passing of user-controlled URL strings to `pandas.read_csv` without host allowlisting, private IP blocking, or URL scheme validation.
- **Recommended fix**: Enforce an allowlist of permitted external hostnames, validate that parsed URLs resolve only to non-private/non-loopback IP addresses (preventing RFC-1918 and loopback access), and disallow `file://` schemes.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding SEC-002: Trivial Global Denial of Service via Shared Lockout Dictionary
- **ID**: SEC-002
- **Title**: Authentication Failure Lockout Keyed to Fixed `"global"` String Shuts Down API for All Users
- **Type**: BUG
- **Severity**: P0
- **Location**: `backend/api/dependencies.py:35-46`
- **Evidence**:
  ```python
  # We don't have the request object here, but we can use a fixed key for the lockout check
  # For a more accurate IP-based lockout, inject Request and use request.client.host
  received_key = x_api_key or api_key

  if not received_key or received_key != AppConfig.API_KEY:
      # Record failure under a generic key (or use IP if Request is injected)
      _failed_attempts["global"].append(time.time())
      _check_lockout("global")
      raise HTTPException(status_code=401, detail="Invalid or missing API Key")
  ```
- **Impact**: Any unauthenticated attacker who sends 10 requests with invalid keys triggers `_check_lockout("global")`, locking out every legitimate user and all frontend traffic worldwide for 60 seconds. Sending 10 requests every minute achieves a permanent, zero-cost Denial of Service.
- **Root cause**: Temporary developer shortcut using `"global"` key instead of injecting `fastapi.Request` and tracking client IPs.
- **Recommended fix**: Inject `request: Request` into `verify_api_key`, extract client IP from `request.client.host` (or validated proxy headers like `X-Forwarded-For`), and key brute-force lockout strictly per IP.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding SEC-003: Public Exposure of Shared Secret API Key in Frontend SPA Bundle
- **ID**: SEC-003
- **Title**: Symmetric API Secret Embedded Directly in Client-Side JavaScript Build
- **Type**: RISK
- **Severity**: P0
- **Location**: `frontend/src/api/ingestion.ts:14`, `frontend/src/api/client.ts:16`
- **Evidence**:
  ```typescript
  export const API_KEY = import.meta.env.VITE_API_KEY || 'dev-secret-key-123';
  ```
- **Impact**: In a web deployment, the single backend authentication token (`DATA_INTEL_API_KEY`) is shipped to every end-user browser within the bundled JS assets. The entire backend API is effectively unauthenticated since the authentication token is public knowledge.
- **Root cause**: Conceptual mismatch: treating a backend secret key as an API key shipped in client-side SPA bundle rather than implementing user-level authentication (JWT, OAuth2, or session cookies).
- **Recommended fix**: Replace single shared static API key with user session cookies or short-lived Bearer tokens issued via authentication flow (e.g. OAuth / Supabase / Auth0).
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding SEC-004: Insecure Direct Object Reference (IDOR) on Dataset & Cache Access
- **ID**: SEC-004
- **Title**: Any User Can Access, Modify, or Delete Datasets Given Any UUID
- **Type**: RISK
- **Severity**: P1
- **Location**: `backend/api/upload.py:180-210`, `backend/state.py:40-70`
- **Evidence**:
  ```python
  @router.get("/{file_id}/profile")
  async def get_profile_data(file_id: str):
      profile = get_profile(file_id)
      if not profile:
          raise HTTPException(status_code=404, detail="Profile not found")
      return profile
  ```
- **Impact**: Because there is no concept of dataset ownership or tenant scoping, any user who knows or guesses a `file_id` (or intercepts one) can download other users' raw datasets, profiles, cleaning plans, or SQL queries.
- **Root cause**: Multi-tenancy and user ownership model are completely absent from `state.py`.
- **Recommended fix**: Add user/tenant context to `state.py` methods and verify that the calling session or user owns `file_id` before serving data.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding SEC-005: Unrestricted File Upload & Missing Executable Content Screening
- **ID**: SEC-005
- **Title**: Upload Endpoint Permits Arbitrary Extensions and Oversized File Flooding
- **Type**: RISK
- **Severity**: P1
- **Location**: `backend/api/upload.py:75-115`
- **Evidence**:
  Uploaded files are saved to `backend/uploads/` on disk using client-provided filenames sanitized only by simple string replacement, without antivirus scanning, MIME magic verification, or quota limits per IP.
- **Impact**: Attackers can flood disk storage with multi-gigabyte files or upload polyglot executables into the server uploads directory.
- **Root cause**: Upload handling accepts arbitrary payloads up to 500 MB without per-IP rate limits or virus screening.
- **Recommended fix**: Validate magic bytes using `python-magic` before writing to disk, enforce strict file size limits based on format, and stream directly to temporary storage with automatic eviction.
- **Confidence**: High
- **Label**: [VERIFIED]
