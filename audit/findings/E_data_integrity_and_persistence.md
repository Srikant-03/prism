# Dimension E: Data Integrity and Persistence

Audit of schema design, constraints, transactions, migration safety, backup/restore, data validation, consistency, orphaned data, retention, and PII handling.

---

### Finding DATA-001: Silent Loss of Cleaned Datasets via Mutable Subscript Bug in `TTLStore`
- **ID**: DATA-001
- **Title**: `update_stored_dataframe` Mutates Deserialized In-Memory Copy Without Re-Persisting to DiskCache
- **Type**: BUG
- **Severity**: P0
- **Location**: `backend/ingestion/orchestrator.py:506-511`, `backend/state.py:41-48`
- **Evidence**:
  ```python
  # ingestion/orchestrator.py
  def update_stored_dataframe(file_id: str, df: pd.DataFrame) -> bool:
      """Update the stored DataFrame after cleaning operations."""
      if file_id in _ingestion_store:
          _ingestion_store[file_id]["dataframe"] = df
          return True
      return False
  ```
- **Impact**: When users apply data cleaning actions, cell edits, outlier capping, or duplicate removals, `update_stored_dataframe` is called. It retrieves a deserialized dictionary copy from `diskcache`, mutates the copy's `"dataframe"` key, and drops it without ever invoking `__setitem__` on `_ingestion_store`. As a result, **cleaned data is never persisted**. All subsequent dashboard renders, SQL queries, profiling re-runs, and exports silently revert to the original, dirty dataset.
- **Root cause**: Subscript assignment on an unpickled mapping object (`cache[key][sub] = val`) does not trigger `cache.__setitem__`.
- **Recommended fix**: Explicitly retrieve the envelope, assign the new DataFrame, and write it back:
  ```python
  stored = _ingestion_store.get(file_id)
  if stored is not None:
      stored["dataframe"] = df
      _ingestion_store[file_id] = stored
      return True
  return False
  ```
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding DATA-002: Zero Database Migrations and Complete Absence of Relational Schema Constraints
- **ID**: DATA-002
- **Title**: No Persistent Relational Database, Migrations, or Referential Integrity (Pure File/Cache Persistence)
- **Type**: RISK
- **Severity**: P1
- **Location**: Entire backend architecture (`backend/state.py:25-75`)
- **Evidence**:
  Persistence relies solely on Python `diskcache` SQLite stores with TTLs ranging from 2 hours to 30 days (`.prism_cache/`). There is no PostgreSQL, MySQL, or persistent database layer; no Alembic migrations exist.
- **Impact**: Any cache corruption, eviction, or manual deletion of `.prism_cache` wipes out all user session states, saved recipes, tags, and annotations permanently. There is no automated point-in-time recovery, backup mechanism, or rollback tooling.
- **Root cause**: Prototyped as a local ephemeral tool without production persistence planning.
- **Recommended fix**: Migrate session metadata, user annotations, saved recipes, and audit logs to a managed relational database (e.g. PostgreSQL with Alembic migrations or embedded SQLite with WAL mode and scheduled backups).
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding DATA-003: Unbounded DiskCache Allocation Without Global Eviction Policy
- **ID**: DATA-003
- **Title**: Multiple 5GB `diskcache` Stores Configured with `eviction_policy="none"` Can Exhaust Host Disk
- **Type**: RISK
- **Severity**: P1
- **Location**: `backend/state.py:34-39`
- **Evidence**:
  ```python
  self.cache = diskcache.Cache(
      os.path.join(CACHE_DIR, name),
      size_limit=int(5 * 1024**3), # 5GB limit per cache
      eviction_policy="none"       # We use manual TTL
  )
  ```
- **Impact**: Seven independent caches are initialized with 5GB limits each, allowing up to 35GB of disk consumption in `.prism_cache/`. Because `eviction_policy="none"` is set, once a cache hits its limit before TTL expiration, subsequent writes crash with `DiskFullError` or raise unhandled exceptions on new uploads.
- **Root cause**: Disabling built-in LRU/least-recently-stored eviction in `diskcache.Cache`.
- **Recommended fix**: Set `eviction_policy="least-recently-stored"` or `"least-recently-used"` and enforce global disk usage thresholds.
- **Confidence**: High
- **Label**: [VERIFIED]
