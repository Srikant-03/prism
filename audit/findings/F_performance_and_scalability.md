# Dimension F: Performance and Scalability

Audit of hot paths, algorithmic complexity, blocking I/O, memory use, caching, pagination, connection pooling, concurrency limits, and statefulness blocking horizontal scaling.

---

### Finding PERF-001: Architecture Strictly Precludes Horizontal Scaling (Multi-Worker / Multi-Instance)
- **ID**: PERF-001
- **Title**: Local-Only State (`:memory:` DuckDB + Local `.prism_cache` SQLite) Prevents Multi-Worker Deployment
- **Type**: RISK
- **Severity**: P0
- **Location**: `backend/sql/sql_engine.py:48`, `backend/state.py:25-39`, `backend/main.py:75-80`
- **Evidence**:
  ```python
  # sql/sql_engine.py
  self.con = duckdb.connect(database=":memory:")
  ```
- **Impact**: Running Uvicorn with `--workers 4` or deploying across multiple container replicas causes catastrophic session misses. A user uploading a file to Worker A cannot query SQL on Worker B because Worker B's in-memory DuckDB has no knowledge of the table, and file state stored in local `.prism_cache` is partitioned per container disk.
- **Root cause**: Monolithic local-first state design where analytical database and cache exist solely within a single OS process / local disk.
- **Recommended fix**: For multi-worker deployments, persist uploaded datasets to object storage (S3/GCS/MinIO) and register them into persistent analytical query engines (DuckDB file-backed storage, MotherDuck, or ClickHouse), with distributed cache backed by Redis.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding PERF-002: Heavy CPU-Bound Profiling and Statistical Tests Block Event Loop
- **ID**: PERF-002
- **Title**: Multi-Threaded/CPU-Intensive Pandas & Scipy Computations Run in Worker Process
- **Type**: RISK
- **Severity**: P1
- **Location**: `backend/profiling/engine.py:90-130`, `backend/api/profiling.py:45-80`
- **Evidence**:
  ```python
  @router.post("/{file_id}/profile")
  async def run_profile(file_id: str):
      df = get_stored_dataframe(file_id)
      profiler = DataProfiler(df)
      result = profiler.run()  # Synchronous CPU-bound computation
  ```
- **Impact**: When a user triggers dataset profiling or statistical testing, the single Uvicorn process is saturated calculating skewness, kurtosis, correlation matrices, and entropy across thousands of cells. While this computation runs, all other HTTP requests (heartbeats, chart renders, chat queries) stall or timeout.
- **Root cause**: Synchronous statistical profiling executed directly within the web process without offloading to background worker pools (Celery / ARQ / ProcessPoolExecutor).
- **Recommended fix**: Offload all profiling, cleaning simulations, and statistical test jobs to a background task queue or `ProcessPoolExecutor`, returning 202 Accepted with polling job status.
- **Confidence**: High
- **Label**: [VERIFIED]

---

### Finding PERF-003: Large In-Memory DataFrame Copies Duplicate Memory Footprint
- **ID**: PERF-003
- **Title**: Multiple Unindexed Copies of Full DataFrames Loaded Concurrently During Analysis
- **Type**: RISK
- **Severity**: P1
- **Location**: `backend/cleaning/decision_engine.py:110-140`, `backend/insights/engine.py:80-110`
- **Evidence**:
  DataFrames are repeatedly duplicated using `.copy()` during outlier simulation, imputation tests, and before/after comparisons without chunking or columnar downcasting.
- **Impact**: Uploading a 250 MB CSV can consume 1.5–3.0 GB of RAM in Python due to Pandas object overhead and temporary copies, causing Out-Of-Memory (OOM) killer terminations on standard cloud instances (e.g. 2 GB / 4 GB RAM VMs).
- **Root cause**: Eager in-memory dataframe copying without Apache Arrow zero-copy memory mapping.
- **Recommended fix**: Migrate DataFrame backends to PyArrow dtypes (`pd.read_csv(..., engine="pyarrow", dtype_backend="pyarrow")`) and leverage DuckDB zero-copy views directly.
- **Confidence**: High
- **Label**: [VERIFIED]
