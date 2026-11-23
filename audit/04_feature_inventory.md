# 04 — Feature Inventory

## Feature Status Legend

| Status | Meaning |
|---|---|
| COMPLETE | Feature works end-to-end with backend + frontend |
| PARTIAL | Core logic exists but missing edge cases or UI integration |
| STUBBED | Code skeleton exists but not wired or functional |
| BROKEN | Code exists but fails at runtime or has logic errors |
| PLANNED-ONLY | Mentioned in README but no code exists |

## Features

### 1. File Upload & Ingestion

| Feature | Status | Evidence |
|---|---|---|
| Single-file CSV upload | COMPLETE | [api/upload.py](file:///c:/abbu/backend/api/upload.py), [ingestion/parsers/csv_parser.py](file:///c:/abbu/backend/ingestion/parsers/csv_parser.py) |
| XLSX upload with sheet selection | COMPLETE | [parsers/excel_parser.py](file:///c:/abbu/backend/ingestion/parsers/excel_parser.py), [SheetSelector.tsx](file:///c:/abbu/frontend/src/components/upload/SheetSelector.tsx) |
| JSON upload | COMPLETE | [parsers/json_parser.py](file:///c:/abbu/backend/ingestion/parsers/json_parser.py) |
| Parquet upload | COMPLETE | [parsers/parquet_parser.py](file:///c:/abbu/backend/ingestion/parsers/parquet_parser.py) |
| XML upload | COMPLETE | [parsers/xml_parser.py](file:///c:/abbu/backend/ingestion/parsers/xml_parser.py) |
| SQL dump upload | COMPLETE | [parsers/sql_parser.py](file:///c:/abbu/backend/ingestion/parsers/sql_parser.py) |
| ZIP archive ingestion | PARTIAL | ZIP extraction works but multi-file re-ingestion after extraction has a gap: `_handle_compressed` returns `None` and the orchestrator continues with normal flow which expects a single file ([orchestrator.py:468-472](file:///c:/abbu/backend/ingestion/orchestrator.py#L468-L472)) |
| GZ/GZIP decompression | COMPLETE | [parsers/compressed.py:186-230](file:///c:/abbu/backend/ingestion/parsers/compressed.py#L186-L230) |
| Encoding auto-detection | COMPLETE | [ingestion/detector.py](file:///c:/abbu/backend/ingestion/detector.py) |
| Malformed data detection & repair | COMPLETE | [ingestion/malformed_handler.py](file:///c:/abbu/backend/ingestion/malformed_handler.py) |
| Multi-file schema comparison | COMPLETE | [ingestion/schema_comparator.py](file:///c:/abbu/backend/ingestion/schema_comparator.py) |
| WebSocket progress streaming | PARTIAL | WS endpoint exists but callback uses `asyncio.get_event_loop()` deprecated pattern ([upload.py:169](file:///c:/abbu/backend/api/upload.py#L169)) |
| Mid-session secondary upload | COMPLETE | [App.tsx:103-114](file:///c:/abbu/frontend/src/App.tsx#L103-L114) |
| Chunked large-file reading | COMPLETE | [ingestion/chunked_reader.py](file:///c:/abbu/backend/ingestion/chunked_reader.py) |

### 2. Data Profiling

| Feature | Status | Evidence |
|---|---|---|
| Numeric profiling | COMPLETE | [profiling/numeric_profiler.py](file:///c:/abbu/backend/profiling/numeric_profiler.py) |
| Categorical profiling | COMPLETE | [profiling/categorical_profiler.py](file:///c:/abbu/backend/profiling/categorical_profiler.py) |
| Datetime profiling | COMPLETE | [profiling/datetime_profiler.py](file:///c:/abbu/backend/profiling/datetime_profiler.py) |
| Boolean profiling | COMPLETE | [profiling/boolean_profiler.py](file:///c:/abbu/backend/profiling/boolean_profiler.py) |
| Text profiling | COMPLETE | [profiling/text_profiler.py](file:///c:/abbu/backend/profiling/text_profiler.py) |
| Correlation analysis | COMPLETE | [profiling/correlation_analyzer.py](file:///c:/abbu/backend/profiling/correlation_analyzer.py) |
| Target variable detection | COMPLETE | [profiling/target_detector.py](file:///c:/abbu/backend/profiling/target_detector.py) |
| Domain detection | COMPLETE | [profiling/domain_detector.py](file:///c:/abbu/backend/profiling/domain_detector.py) |
| Key detection | COMPLETE | [profiling/key_detector.py](file:///c:/abbu/backend/profiling/key_detector.py) |
| Formula detection | COMPLETE | [profiling/formula_detector.py](file:///c:/abbu/backend/profiling/formula_detector.py) |
| Functional dependency | COMPLETE | [profiling/functional_dependency.py](file:///c:/abbu/backend/profiling/functional_dependency.py) |
| Temporal analysis | COMPLETE | [profiling/temporal_analyzer.py](file:///c:/abbu/backend/profiling/temporal_analyzer.py) |
| Geo analysis | PARTIAL | File exists but limited functionality (2.4KB) | [profiling/geo_analyzer.py](file:///c:/abbu/backend/profiling/geo_analyzer.py) |

### 3. Data Cleaning

| Feature | Status | Evidence |
|---|---|---|
| Missing value handler | COMPLETE | [cleaning/missing_handler.py](file:///c:/abbu/backend/cleaning/missing_handler.py) (33KB) |
| Outlier handler | COMPLETE | [cleaning/outlier_handler.py](file:///c:/abbu/backend/cleaning/outlier_handler.py) (43KB) |
| Duplicate handler | COMPLETE | [cleaning/duplicate_handler.py](file:///c:/abbu/backend/cleaning/duplicate_handler.py) |
| Type handler | COMPLETE | [cleaning/type_handler.py](file:///c:/abbu/backend/cleaning/type_handler.py) |
| Categorical handler | COMPLETE | [cleaning/categorical_handler.py](file:///c:/abbu/backend/cleaning/categorical_handler.py) |
| Datetime handler | COMPLETE | [cleaning/datetime_handler.py](file:///c:/abbu/backend/cleaning/datetime_handler.py) |
| Text handler | COMPLETE | [cleaning/text_handler.py](file:///c:/abbu/backend/cleaning/text_handler.py) |
| Scaling handler | COMPLETE | [cleaning/scaling_handler.py](file:///c:/abbu/backend/cleaning/scaling_handler.py) |
| Standardization handler | COMPLETE | [cleaning/standardization_handler.py](file:///c:/abbu/backend/cleaning/standardization_handler.py) |
| Feature selection handler | COMPLETE | [cleaning/feature_selection_handler.py](file:///c:/abbu/backend/cleaning/feature_selection_handler.py) |
| Leakage handler | COMPLETE | [cleaning/leakage_handler.py](file:///c:/abbu/backend/cleaning/leakage_handler.py) |
| Imbalance handler | COMPLETE | [cleaning/imbalance_handler.py](file:///c:/abbu/backend/cleaning/imbalance_handler.py) |
| Decision engine | COMPLETE | [cleaning/decision_engine.py](file:///c:/abbu/backend/cleaning/decision_engine.py) (43KB) |
| Audit logger | COMPLETE | [cleaning/audit_logger.py](file:///c:/abbu/backend/cleaning/audit_logger.py) |
| What-If simulation | COMPLETE | [cleaning/simulation_utils.py](file:///c:/abbu/backend/cleaning/simulation_utils.py), [api/simulate.py](file:///c:/abbu/backend/api/simulate.py) |
| Cell-level repair | COMPLETE | [cleaning/cell_repair.py](file:///c:/abbu/backend/cleaning/cell_repair.py) |
| Cleaning recipe browser | COMPLETE | [api/recipe.py](file:///c:/abbu/backend/api/recipe.py), [RecipeBrowser.tsx](file:///c:/abbu/frontend/src/components/cleaning/RecipeBrowser.tsx) |

### 4. SQL & Query Engine

| Feature | Status | Evidence |
|---|---|---|
| DuckDB SQL execution | COMPLETE | [sql/sql_engine.py](file:///c:/abbu/backend/sql/sql_engine.py) |
| Visual query builder | COMPLETE | [sql/query_builder.py](file:///c:/abbu/backend/sql/query_builder.py), [QueryWorkbench.tsx](file:///c:/abbu/frontend/src/components/sql/QueryWorkbench.tsx) |
| Natural language to SQL | COMPLETE | [sql/nl_query.py](file:///c:/abbu/backend/sql/nl_query.py), [NLQueryPanel.tsx](file:///c:/abbu/frontend/src/components/sql/NLQueryPanel.tsx) |
| Template library | COMPLETE | [sql/template_generator.py](file:///c:/abbu/backend/sql/template_generator.py), [TemplateLibrary.tsx](file:///c:/abbu/frontend/src/components/sql/TemplateLibrary.tsx) |
| Query history | COMPLETE | [QueryHistory.tsx](file:///c:/abbu/frontend/src/components/sql/QueryHistory.tsx) |
| Query caching | COMPLETE | [sql_engine.py:277-296](file:///c:/abbu/backend/sql/sql_engine.py#L277-L296) |
| Explain/analyze plan | COMPLETE | [sql_engine.py:213-243](file:///c:/abbu/backend/sql/sql_engine.py#L213-L243) |
| View management | COMPLETE | [sql_engine.py:247-273](file:///c:/abbu/backend/sql/sql_engine.py#L247-L273) |
| Join builder | COMPLETE | [api/joins.py](file:///c:/abbu/backend/api/joins.py), [JoinBuilder.tsx](file:///c:/abbu/frontend/src/components/sql/JoinBuilder.tsx) |

### 5. AI Dashboard

| Feature | Status | Evidence |
|---|---|---|
| NL prompt → chart config | COMPLETE | [dashboard/prompt_interpreter.py](file:///c:/abbu/backend/dashboard/prompt_interpreter.py) |
| Auto-generated initial dashboard | COMPLETE | [dashboard/auto_generator.py](file:///c:/abbu/backend/dashboard/auto_generator.py) |
| Dashboard save/load/delete | COMPLETE | [api/dashboard.py](file:///c:/abbu/backend/api/dashboard.py) |
| Dashboard share (read-only) | PARTIAL | Endpoint exists but no actual sharing mechanism (no unique URLs or access control) |

### 6. Insights & Hypothesis Engine

| Feature | Status | Evidence |
|---|---|---|
| Hypothesis generation | COMPLETE | [insights/hypothesis_engine.py](file:///c:/abbu/backend/insights/hypothesis_engine.py) (54KB) |
| Anomaly detection | COMPLETE | [insights/anomaly_detector.py](file:///c:/abbu/backend/insights/anomaly_detector.py) |
| Relationship classification | COMPLETE | [insights/relationship_classifier.py](file:///c:/abbu/backend/insights/relationship_classifier.py) |
| Statistical tests | COMPLETE | [insights/stat_tests.py](file:///c:/abbu/backend/insights/stat_tests.py) |
| Quality scoring | COMPLETE | [insights/quality_scorer.py](file:///c:/abbu/backend/insights/quality_scorer.py) |
| Feature ranking | COMPLETE | [insights/feature_ranker.py](file:///c:/abbu/backend/insights/feature_ranker.py) |
| Hypothesis testing (stat) | COMPLETE | [api/hypothesis_testing.py](file:///c:/abbu/backend/api/hypothesis_testing.py) |
| Anomaly watchlist | COMPLETE | [api/watchlist.py](file:///c:/abbu/backend/api/watchlist.py), [AnomalyWatchlist.tsx](file:///c:/abbu/frontend/src/components/insights/AnomalyWatchlist.tsx) |

### 7. Reporting & Export

| Feature | Status | Evidence |
|---|---|---|
| PDF report generation | COMPLETE | [reporting/report_generator.py](file:///c:/abbu/backend/reporting/report_generator.py) |
| DOCX report generation | COMPLETE | Same |
| HTML report | COMPLETE | Same |
| PPTX (PowerPoint) | COMPLETE | [api/story.py](file:///c:/abbu/backend/api/story.py) uses python-pptx |
| Data export (CSV/JSON/Parquet) | COMPLETE | [reporting/data_exporter.py](file:///c:/abbu/backend/reporting/data_exporter.py) |
| Code exporter | COMPLETE | [reporting/code_exporter.py](file:///c:/abbu/backend/reporting/code_exporter.py) |
| Executive story builder | COMPLETE | [api/story.py](file:///c:/abbu/backend/api/story.py) (40KB) |

### 8. Collaboration

| Feature | Status | Evidence |
|---|---|---|
| Annotation/comment system | PARTIAL | [api/collab.py](file:///c:/abbu/backend/api/collab.py), [CollabPanel.tsx](file:///c:/abbu/frontend/src/components/collab/CollabPanel.tsx) — annotations stored in diskcache (single-user, no multi-user sync) |
| Workspace sessions | STUBBED | No actual session persistence beyond TTLStore |

### 9. ML Readiness & Drift

| Feature | Status | Evidence |
|---|---|---|
| ML readiness assessment | COMPLETE | [api/ml_readiness.py](file:///c:/abbu/backend/api/ml_readiness.py) |
| Data drift monitoring | COMPLETE | [api/drift.py](file:///c:/abbu/backend/api/drift.py) |
| System resource monitoring | COMPLETE | [api/monitor.py](file:///c:/abbu/backend/api/monitor.py) |

### 10. Chat Assistant

| Feature | Status | Evidence |
|---|---|---|
| Gemini chat with dataset context | COMPLETE | [chat/engine.py](file:///c:/abbu/backend/chat/engine.py), [ChatSidebar.tsx](file:///c:/abbu/frontend/src/components/chat/ChatSidebar.tsx) |
| Privacy disclosure | COMPLETE | [PrivacyDisclosure.tsx](file:///c:/abbu/frontend/src/components/chat/PrivacyDisclosure.tsx) |

### 11. Additional UI Features

| Feature | Status | Evidence |
|---|---|---|
| Dark/light theme toggle | COMPLETE | [useTheme.tsx](file:///c:/abbu/frontend/src/hooks/useTheme.tsx), [ThemeToggle.tsx](file:///c:/abbu/frontend/src/components/common/ThemeToggle.tsx) |
| Dataset diff/comparison | COMPLETE | [DatasetDiff.tsx](file:///c:/abbu/frontend/src/components/comparison/DatasetDiff.tsx) |
| Live data grid (AG Grid) | COMPLETE | [LiveDataGrid.tsx](file:///c:/abbu/frontend/src/components/grid/LiveDataGrid.tsx) |
| Onboarding walkthrough | PARTIAL | [OnboardingWalkthrough.tsx](file:///c:/abbu/frontend/src/components/common/OnboardingWalkthrough.tsx) exists but not wired into App.tsx |
| Present mode | PARTIAL | [PresentMode.tsx](file:///c:/abbu/frontend/src/components/dashboard/PresentMode.tsx) exists but integration unclear |

## API Endpoints (All Routers)

26 router modules registered in [main.py:130-152](file:///c:/abbu/backend/main.py#L130-L152):
upload, profiling, cleaning, sql, reporting, chat, grid, watchlist, graph, story, recipe, metadata, collab, simulate, stats, explain, hypotheses, dashboard, drift, hypothesis_testing, joins, monitor, ml_readiness, models (in api/), dependencies, plus root `/` and `/health`.
