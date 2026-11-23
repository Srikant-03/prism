# Dimension M: UX / API Contract

Audit of consistent error formats, status codes, API versioning, backward compatibility, accessibility, and loading/error states.

---

### Finding API-001: Fragmented and Inconsistent Error Response Payloads
- **ID**: API-001
- **Title**: Lack of Standard RFC-7807 Error Envelope Across FastAPI Routers
- **Type**: RISK
- **Severity**: P2
- **Location**: `backend/api/upload.py`, `backend/api/cleaning.py`, `backend/api/sql.py`
- **Evidence**:
  - `upload.py` raises `HTTPException(status_code=400, detail="string")` (FastAPI standard `{"detail": "string"}`).
  - `sql.py` returns HTTP 200 with `{"error": str(e), "success": False}` on syntax errors.
  - `cleaning.py` catches exceptions and returns `{"status": "failed", "reason": str(e)}`.
- **Impact**: Frontend error interceptors and third-party API consumers cannot uniformly extract error codes or localized error messages. Some failures render as empty dialogs or unhandled promise rejections in the UI.
- **Root cause**: Lack of a centralized FastAPI exception handler mapping all errors to a single schema (e.g. `{"error": {"code": str, "message": str, "details": list}}`).
- **Recommended fix**: Implement an application-wide FastAPI exception handler for `HTTPException` and `RequestValidationError` enforcing RFC-7807 Problem Details.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding API-002: Hardcoded Un-Versioned `/api/*` Routes Without API Version Prefix
- **ID**: API-002
- **Title**: Missing `/api/v1` URL Versioning Namespace
- **Type**: DEBT
- **Severity**: P3
- **Location**: `backend/main.py:125-155`
- **Evidence**:
  All 26 routers are mounted directly under `/api/upload`, `/api/profile`, `/api/sql` without a `/api/v1` namespace.
- **Impact**: Making breaking changes to request or response schemas breaks existing frontend builds or API clients without any mechanism for phased migration or backward compatibility.
- **Root cause**: Initial routing built without API version prefix.
- **Recommended fix**: Nest all feature routers under an `/api/v1` router prefix while maintaining backward-compatible redirects if necessary.
- **Confidence**: High
- **Label**: [VERIFIED]
