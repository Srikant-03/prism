# Dimension C: Error Handling and Resilience

Audit of unhandled exceptions, swallowed errors, missing timeouts, retries, circuit breakers, graceful degradation, and input boundaries.

---

### Finding ERR-001: Missing Timeouts and Cancellation in Long-Running Ingestion & Profiling
- **ID**: ERR-001
- **Title**: Synchronous Long-Running CPU Tasks Lack Timeouts and Task Cancellation
- **Type**: RISK
- **Severity**: P1
- **Location**: `backend/profiling/engine.py:85-120`, `backend/api/upload.py:110-150`
- **Evidence**:
  ```python
  # profiling/engine.py
  def run(self) -> dict:
      for analyzer in self.analyzers:
          analyzer.analyze(self.df)
  ```
- **Impact**: Uploading a multi-gigabyte or pathologically complex dataset (e.g. 1,000 text columns with heavy regex or n-gram analysis) blocks backend worker execution indefinitely. The client experiences a connection drop/timeout, but the backend continues consuming 100% CPU with no cancellation mechanism or execution deadline.
- **Root cause**: No execution deadline (`asyncio.wait_for`, Celery task timeouts, or thread execution limits) applied to intensive statistical algorithms.
- **Recommended fix**: Wrap dataset profiling jobs in bounded asynchronous executor tasks with an explicit timeout (e.g. 120 seconds default), checking for task cancellation flags between analyzers.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding ERR-002: Async Event Loop Binding Bug in APIKeyManager
- **ID**: ERR-002
- **Title**: Singleton `APIKeyManager` Instantiates `asyncio.Lock()` in Global Scope
- **Type**: BUG
- **Severity**: P1
- **Location**: `backend/llm/api_manager.py:46, 153`
- **Evidence**:
  ```python
  class APIKeyManager:
      def __init__(self, keys: List[str]):
          ...
          self._lock = asyncio.Lock()  # Created at module import time!

  key_manager = APIKeyManager(AppConfig.llm.GEMINI_API_KEYS)
  ```
- **Impact**: In Python 3.10+, instantiating `asyncio.Lock()` outside of an active running event loop attaches the lock to an uninitialized loop. If FastAPI creates a fresh event loop during worker initialization or if calls are dispatched from threads, subsequent `async with self._lock:` calls fail with `RuntimeError: <Lock ...> is bound to a different event loop`.
- **Root cause**: Eager creation of asyncio primitives at module import time instead of lazy initialization within the running event loop.
- **Recommended fix**: Initialize `_lock: Optional[asyncio.Lock] = None` and lazily acquire it with `if self._lock is None: self._lock = asyncio.Lock()`.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding ERR-003: Swallowed Coroutine in Status Serialization
- **ID**: ERR-003
- **Title**: Synchronous `get_status()` Calls `async def all_keys_exhausted()` Without Await
- **Type**: BUG
- **Severity**: P2
- **Location**: `backend/llm/api_manager.py:141-150`
- **Evidence**:
  ```python
  def get_status(self) -> dict:
      return {
          "total_keys": len(self.keys),
          "all_exhausted": self.all_keys_exhausted(),  # Coroutine unawaited!
      }
  ```
- **Impact**: Any monitoring endpoint querying LLM pool health receives a Python coroutine object in the dictionary instead of a boolean value, causing JSON serialization errors (`TypeError: Object of type coroutine is not JSON serializable`) or false evaluations where coroutine truthiness evaluates to `True`.
- **Root cause**: Asynchronous method invoked inside synchronous helper without awaiting.
- **Recommended fix**: Change `get_status` to `async def get_status(self) -> dict:` and await `all_keys_exhausted()`.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding ERR-004: Unhandled File Parsing Exceptions Fail With Generic 500
- **ID**: ERR-004
- **Title**: Parser Errors in Ingestion Pipeline Not Converted to User-Actionable 400 Bad Request
- **Type**: RISK
- **Severity**: P2
- **Location**: `backend/api/upload.py:145-175`
- **Evidence**:
  ```python
  except Exception as e:
      logger.exception("Upload failed")
      raise HTTPException(status_code=500, detail=str(e))
  ```
- **Impact**: Malformed user files (e.g. non-UTF8 text, broken Excel headers, corrupted Parquet metadata) trigger HTTP 500 Internal Server Error rather than informative 400 or 422 Bad Request error responses explaining file corruption.
- **Root cause**: Catch-all generic exception handler maps all parsing failures to HTTP 500.
- **Recommended fix**: Catch domain-specific parser exceptions (`MalformedDataError`, `EncodingError`, `UnsupportedFormatError`) and return structured 400 status codes with column/line repair hints.
- **Confidence**: High
- **Label**: [VERIFIED]
