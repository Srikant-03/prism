# Dimension H: Observability

Audit of structured logging, log hygiene, metrics, tracing, health/readiness endpoints, alerting hooks, error tracking, and debuggability.

---

### Finding OBS-001: Lack of Structured JSON Logging and Centralized Log Formatting
- **ID**: OBS-001
- **Title**: Free-Text Unstructured Logging Emitted Directly to Console
- **Type**: DEBT
- **Severity**: P2
- **Location**: `backend/main.py:74-77`, `backend/config.py:45-55`
- **Evidence**:
  ```python
  logger.info(
      "[STARTUP] Data Intelligence Platform v%s | host=%s port=%d debug=%s",
      AppConfig.VERSION, AppConfig.HOST, AppConfig.PORT, AppConfig.DEBUG,
  )
  ```
- **Impact**: In containerized or Kubernetes production environments, log aggregation pipelines (Datadog, AWS CloudWatch, Elastic/Logstash) cannot parse timestamps, request IDs, correlation IDs, or error codes into filterable facets. Debugging production incidents requires painful manual regex grep searches.
- **Root cause**: Standard library `logging.basicConfig()` without structured JSON formatting (`structlog` or `python-json-logger`).
- **Recommended fix**: Adopt `structlog` or configure a JSON log formatter outputting ISO-8601 timestamps, log level, correlation ID, and module name as JSON attributes.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding OBS-002: Zero Application Performance Metrics (APM) and OpenTelemetry Tracing
- **ID**: OBS-002
- **Title**: No Prometheus `/metrics` Endpoint or OpenTelemetry Tracing Instrumentation
- **Type**: MISSING
- **Severity**: P2
- **Location**: `backend/main.py:86-105`
- **Evidence**:
  No APM client (`prometheus_fastapi_instrumentator`, `opentelemetry`) registered in `backend/requirements.txt` or `main.py`.
- **Impact**: Operators cannot observe request latency histograms, p99 percentiles, active DuckDB query execution times, or memory saturation. Performance regressions in production cannot be detected before users experience failures.
- **Root cause**: APM instrumentation omitted during initial prototype build.
- **Recommended fix**: Integrate `prometheus-fastapi-instrumentator` exposing `/metrics` with custom counters for dataset uploads, parsing durations, and LLM token usage.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding OBS-003: Health Endpoint Does Not Verify Subsystem Readiness (DuckDB / Cache / Disk)
- **ID**: OBS-003
- **Title**: Superficial Health Check Returns 200 OK Even If Disk or Cache Is Corrupted
- **Type**: RISK
- **Severity**: P2
- **Location**: `backend/api/health.py:10-35` (or `main.py`)
- **Evidence**:
  Health route returns static `{"status": "ok", "version": "..."}` without performing active canary checks on `.prism_cache/` writeability or DuckDB query execution.
- **Impact**: Load balancers keep routing traffic to dead workers whose disk is full or whose DuckDB engine has crashed, serving 500 errors to users instead of failing health checks for container restart.
- **Root cause**: Liveness and readiness probes not separated; no active dependency health probing.
- **Recommended fix**: Add `/health/ready` probe that executes `SELECT 1` in DuckDB and tests a temporary write to `CACHE_DIR`.
- **Confidence**: High
- **Label**: [VERIFIED]
