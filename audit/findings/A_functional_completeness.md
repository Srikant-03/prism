# Dimension A: Functional Completeness

Audit of missing features, stubs, unhandled scenarios, dead code, half-wired components, and mocked logic in production paths.

---

### Finding FC-001: Compressed Multi-File Ingestion Drops Extracted Files
- **ID**: FC-001
- **Title**: ZIP Archive Ingestion Returns `None` and Bypasses Parsed File Registration
- **Type**: INCOMPLETE
- **Severity**: P1
- **Location**: `backend/ingestion/orchestrator.py:468-475`
- **Evidence**:
  ```python
  if fmt == "archive":
      # Handled by compressed parser which may extract multiple files
      # Return first parsed dataframe or let caller handle
      # For now, compressed parser extracts to temp dir and returns None
      # to signal orchestrator to re-ingest
      pass
  ```
- **Impact**: When a user uploads a ZIP archive containing datasets, the file is unzipped to a temporary directory, but the orchestrator does not iterate through the extracted files to load them into the dataset session store. The upload silently fails or reports an empty dataset.
- **Root cause**: Implementation stub left unfinished with a comment "For now... returns None to signal orchestrator to re-ingest" without the orchestrator loop implemented.
- **Recommended fix**: Implement multi-file resolution in `IngestionOrchestrator.ingest()` to iterate over extracted files in `temp_dir`, parse the primary table, register child tables in DuckDB, and return a multi-dataset manifest to the client.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding FC-002: Hardcoded Mock Fallback in NLP Query Translator
- **ID**: FC-002
- **Title**: Natural Language to SQL Relies on Hardcoded Mock Heuristics When LLM Fails
- **Type**: MOCK / RISK
- **Severity**: P2
- **Location**: `backend/sql/nl_query.py:112-145`
- **Evidence**:
  ```python
  except Exception as e:
      logger.warning(f"Gemini NL-to-SQL failed ({e}), falling back to regex heuristic")
      return self._heuristic_fallback(prompt, table_name, schema)
  ```
- **Impact**: If Gemini API quota is exhausted or offline, queries revert to rudimentary keyword matching (e.g. searching for words like "count", "average"). Non-trivial analytical questions yield syntactically invalid or semantically misleading DuckDB SQL queries without notifying the user of degradation.
- **Root cause**: Fallback heuristic is brittle regex string matching that produces invalid SQL for complex joins or date groupings.
- **Recommended fix**: Explicitly signal fallback state in the API response model (`is_fallback: True`, `confidence: "low"`) and provide clear UI warnings rather than silently executing guessing queries.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding FC-003: Collaboration Endpoints Have No Persistence or WebSocket Sync
- **ID**: FC-003
- **Title**: Collaboration Feature (`/api/collab`) Uses Volatile In-Memory Dictionary Without Cross-Client Broadcasting
- **Type**: STUBBED
- **Severity**: P2
- **Location**: `backend/api/collab.py:15-38`
- **Evidence**:
  ```python
  # In-memory collaboration state
  _collab_rooms: dict[str, dict] = {}
  ```
- **Impact**: Multi-user collaboration advertised in the frontend cannot sync state between multiple browser windows or machines without WebSockets or server-sent events (SSE). Any worker restart or separate process erases all shared notes and cursors.
- **Root cause**: Ephemeral dictionary placeholder without real-time pub/sub infrastructure.
- **Recommended fix**: Either declare PRISM as strictly single-player and remove mock collab UI, or implement real-time synchronization via WebSockets backed by Redis or SQLite WAL pub/sub.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding FC-004: Missing Unsaved Work Warning & Session Auto-Save in Frontend
- **ID**: FC-004
- **Title**: Refreshing or Navigating Away Immediately Destroys Uncommitted Data Wrangling Changes
- **Type**: MISSING
- **Severity**: P2
- **Location**: `frontend/src/App.tsx:50-85`
- **Evidence**:
  App state holds session ID in local memory (`useState`), but changes in active cell repairs or un-downloaded recipe drafts lack a `beforeunload` event handler.
- **Impact**: Users accidentally closing the browser tab lose all work-in-progress transformation rules.
- **Root cause**: No window `beforeunload` listener or automatic recipe autosaving to `localStorage`.
- **Recommended fix**: Add `useBeforeUnload` hook when uncommitted cleaning steps or SQL scratchpads are dirty.
- **Confidence**: High
- **Label**: [VERIFIED]
