# 00 — Coverage Ledger

## Audit Scope & Ledger Verification
Total project tracked files: 135+ key files across backend and frontend. Every file in the core codebase has been examined and classified according to its architectural role, size, and audit status.

## Coverage Ledger

### Project Root
| File Path | Purpose (1-line) | Language/Type | Size | Audit Status |
|---|---|---|---|---|
| `README.md` | Top-level project pitch, feature claims, and quickstart guide | Markdown | 11.0 KB | [READ FULLY] |
| `start.bat` | Windows bootstrap script to create venv, install deps, launch backend & frontend | Windows Batch | 2.8 KB | [READ FULLY] |
| `stop.bat` | Windows script to kill uvicorn and vite processes | Windows Batch | 251 B | [READ FULLY] |
| `.gitignore` | Root git ignore rules | Config | 355 B | [READ FULLY] |
| `dashboard_test_data.csv` | Sample test data file for dashboards | CSV / Data | 21.7 KB | [INSPECTED SAMPLES] |
| `test_out.log` | Test run log artifact showing failing business rule test | Log | 4.5 KB | [READ FULLY] |
| `assets/prism_architecture.jpg` | Architectural diagram visual asset | Binary Image | 797 KB | [BINARY/ASSET: inspected metadata] |
| `assets/prism_hero.jpg` | UI hero screenshot visual asset | Binary Image | 521 KB | [BINARY/ASSET: inspected metadata] |

---

### Backend Core & Configuration
| File Path | Purpose (1-line) | Language/Type | Size | Audit Status |
|---|---|---|---|---|
| `backend/main.py` | FastAPI application factory, lifespan, CORS, middleware, router mount | Python | 7.8 KB | [READ FULLY] |
| `backend/config.py` | Central configuration using python-dotenv with Pydantic settings | Python | 5.7 KB | [READ FULLY] |
| `backend/state.py` | Unified in-memory/diskcache TTL data store abstraction | Python | 11.9 KB | [READ FULLY] |
| `backend/requirements.txt` | Python dependency specifications | Plaintext | 739 B | [READ FULLY] |
| `backend/requirements_frozen.txt` | Pinned pip freeze output | Plaintext | 3.5 KB | [READ FULLY] |
| `backend/pyrightconfig.json` | Pyright type-checker configuration | JSON | 394 B | [READ FULLY] |
| `backend/.env.example` | Template environment variable definitions | Config | 1.3 KB | [READ FULLY] |
| `backend/models/schemas.py` | Unified Pydantic request/response schemas | Python | 15.2 KB | [READ FULLY] |
| `backend/llm/api_manager.py` | Gemini API key pool manager with sequential failover & rate limit backoff | Python | 6.2 KB | [READ FULLY] |

---

### Backend Ingestion Engine (`backend/ingestion/`)
| File Path | Purpose (1-line) | Language/Type | Size | Audit Status |
|---|---|---|---|---|
| `backend/ingestion/orchestrator.py` | Ingestion pipeline coordinator: format dispatch, validation, registration | Python | 18.5 KB | [READ FULLY] |
| `backend/ingestion/detector.py` | Format, encoding, delimiter, and header detection | Python | 8.9 KB | [READ FULLY] |
| `backend/ingestion/chunked_reader.py` | Large file stream reader with memory limits | Python | 6.4 KB | [READ FULLY] |
| `backend/ingestion/malformed_handler.py` | Malformed row repair and recovery logic | Python | 7.2 KB | [READ FULLY] |
| `backend/ingestion/schema_comparator.py` | Schema drift and cross-dataset comparison | Python | 5.8 KB | [READ FULLY] |
| `backend/ingestion/parsers/base.py` | Base abstract parser class | Python | 3.1 KB | [READ FULLY] |
| `backend/ingestion/parsers/csv_parser.py` | CSV/TSV parser utilizing CleverCSV fallback | Python | 7.4 KB | [READ FULLY] |
| `backend/ingestion/parsers/excel_parser.py` | Excel parser with multi-sheet detection via openpyxl/xlrd | Python | 6.8 KB | [READ FULLY] |
| `backend/ingestion/parsers/json_parser.py` | Streaming JSON/JSONL parser via ijson | Python | 6.1 KB | [READ FULLY] |
| `backend/ingestion/parsers/parquet_parser.py` | Apache Parquet parser using pyarrow | Python | 4.2 KB | [READ FULLY] |
| `backend/ingestion/parsers/xml_parser.py` | XML parser converting structured trees to DataFrames | Python | 5.9 KB | [READ FULLY] |
| `backend/ingestion/parsers/sql_parser.py` | SQL dump schema and insert statement parser | Python | 6.3 KB | [READ FULLY] |
| `backend/ingestion/parsers/compressed.py` | ZIP/GZ decompressor and multi-file extractor | Python | 7.8 KB | [READ FULLY] |

---

### Backend Profiling Engine (`backend/profiling/`)
| File Path | Purpose (1-line) | Language/Type | Size | Audit Status |
|---|---|---|---|---|
| `backend/profiling/engine.py` | Profiling orchestrator running statistical analyzers concurrently | Python | 14.8 KB | [READ FULLY] |
| `backend/profiling/profiling_models.py` | Data structures for column statistics and profile summaries | Python | 8.3 KB | [READ FULLY] |
| `backend/profiling/numeric_profiler.py` | Computes moments, quartiles, skewness, kurtosis, and zero-inflation | Python | 7.2 KB | [READ FULLY] |
| `backend/profiling/categorical_profiler.py` | High cardinality, entropy, frequency, and distribution profiling | Python | 6.5 KB | [READ FULLY] |
| `backend/profiling/datetime_profiler.py` | Temporal min/max, periodicity, gaps, and daylight savings detection | Python | 5.9 KB | [READ FULLY] |
| `backend/profiling/text_profiler.py` | Text length, language, sentiment, and token profiling | Python | 6.1 KB | [READ FULLY] |
| `backend/profiling/boolean_profiler.py` | Boolean flag ratio and truthiness detection | Python | 3.8 KB | [READ FULLY] |
| `backend/profiling/correlation_analyzer.py` | Pearson, Spearman, and Cramér's V correlation matrices | Python | 6.8 KB | [READ FULLY] |
| `backend/profiling/type_detector.py` | Semantic type inference beyond raw pandas dtypes | Python | 7.5 KB | [READ FULLY] |
| `backend/profiling/key_detector.py` | Primary and composite key candidate detection | Python | 5.2 KB | [READ FULLY] |
| `backend/profiling/domain_detector.py` | Regex and dictionary matching for emails, SSNs, IPs, currencies | Python | 8.1 KB | [READ FULLY] |
| `backend/profiling/geo_analyzer.py` | Country/city/zip/coordinate pattern detection | Python | 5.6 KB | [READ FULLY] |
| `backend/profiling/temporal_analyzer.py` | Trend, seasonality, and temporal grain analysis | Python | 6.9 KB | [READ FULLY] |
| `backend/profiling/target_detector.py` | Supervised learning target candidate scoring | Python | 5.4 KB | [READ FULLY] |
| `backend/profiling/cross_column_models.py` | Cross-column relationship definitions | Python | 4.8 KB | [READ FULLY] |

---

### Backend Cleaning Engine (`backend/cleaning/`)
| File Path | Purpose (1-line) | Language/Type | Size | Audit Status |
|---|---|---|---|---|
| `backend/cleaning/decision_engine.py` | Autonomous rule engine proposing cleaning plans | Python | 18.2 KB | [READ FULLY] |
| `backend/cleaning/cleaning_models.py` | Pydantic definitions for actions, recommendations, and plans | Python | 7.1 KB | [READ FULLY] |
| `backend/cleaning/missing_handler.py` | Strategies for drop, mean, median, mode, forward-fill | Python | 7.5 KB | [READ FULLY] |
| `backend/cleaning/outlier_handler.py` | IQR, Z-score, isolation forest, and domain capping | Python | 8.4 KB | [READ FULLY] |
| `backend/cleaning/duplicate_handler.py` | Exact and fuzzy duplicate record resolution | Python | 6.3 KB | [READ FULLY] |
| `backend/cleaning/type_handler.py` | Type coercion and format standardization | Python | 8.9 KB | [READ FULLY] |
| `backend/cleaning/datetime_handler.py` | Date parsing, timezone normalization, component extraction | Python | 7.1 KB | [READ FULLY] |
| `backend/cleaning/text_handler.py` | Stopword removal, case normalization, regex cleaning | Python | 6.8 KB | [READ FULLY] |
| `backend/cleaning/scaling_handler.py` | MinMax, Standard, and Robust scaling transforms | Python | 5.2 KB | [READ FULLY] |
| `backend/cleaning/imbalance_handler.py` | Class imbalance detection and weighting suggestions | Python | 4.9 KB | [READ FULLY] |
| `backend/cleaning/cell_repair.py` | Granular individual cell manual and automated overrides | Python | 5.7 KB | [READ FULLY] |
| `backend/cleaning/simulation_utils.py` | Dry-run transformation and diff generator | Python | 6.1 KB | [READ FULLY] |
| `backend/cleaning/standardization_handler.py` | Categorical normalization and casing rules | Python | 5.4 KB | [READ FULLY] |
| `backend/cleaning/leakage_handler.py` | Data leakage check between features and target | Python | 5.9 KB | [READ FULLY] |
| `backend/cleaning/feature_selection_handler.py` | Collinearity and variance threshold pruning | Python | 5.8 KB | [READ FULLY] |

---

### Backend SQL, Insights, Dashboard & Chat
| File Path | Purpose (1-line) | Language/Type | Size | Audit Status |
|---|---|---|---|---|
| `backend/sql/sql_engine.py` | DuckDB execution wrapper with read-only validation | Python | 9.8 KB | [READ FULLY] |
| `backend/sql/nl_query.py` | Gemini natural language to DuckDB SQL translator | Python | 7.6 KB | [READ FULLY] |
| `backend/sql/query_builder.py` | Programmatic visual query builder (joins, filters, aggregates) | Python | 8.2 KB | [READ FULLY] |
| `backend/sql/template_generator.py` | SQL analytical recipe generator | Python | 7.1 KB | [READ FULLY] |
| `backend/insights/engine.py` | Automated insights synthesis and coordinator | Python | 12.3 KB | [READ FULLY] |
| `backend/insights/hypothesis_engine.py` | Statistical hypothesis testing generator | Python | 8.7 KB | [READ FULLY] |
| `backend/insights/anomaly_detector.py` | Unsupervised anomaly discovery | Python | 7.4 KB | [READ FULLY] |
| `backend/insights/quality_scorer.py` | Multi-dimensional data health score (completeness, validity) | Python | 6.5 KB | [READ FULLY] |
| `backend/insights/feature_ranker.py` | Feature importance relative to detected targets | Python | 6.1 KB | [READ FULLY] |
| `backend/insights/briefing_generator.py` | Markdown executive briefing synthesizer | Python | 6.9 KB | [READ FULLY] |
| `backend/insights/graph_builder.py` | Relationship graph generator for column associations | Python | 5.8 KB | [READ FULLY] |
| `backend/insights/export_service.py` | Serializer for insight exports | Python | 4.3 KB | [READ FULLY] |
| `backend/dashboard/chart_config_models.py` | Pydantic schema for 10 chart widget types | Python | 6.4 KB | [READ FULLY] |
| `backend/dashboard/prompt_interpreter.py` | Gemini prompt to chart configuration compiler | Python | 7.9 KB | [READ FULLY] |
| `backend/dashboard/config_to_sql.py` | Compiles chart configurations into DuckDB SQL queries | Python | 9.2 KB | [READ FULLY] |
| `backend/chat/engine.py` | Multi-turn conversational assistant with dataset context injection | Python | 10.4 KB | [READ FULLY] |
| `backend/reporting/report_generator.py` | Multi-format comprehensive report builder (PDF/DOCX/HTML) | Python | 55.3 KB | [READ FULLY] |
| `backend/reporting/data_exporter.py` | Raw data exporter (CSV, Parquet, JSON, Excel) | Python | 6.2 KB | [READ FULLY] |
| `backend/reporting/code_exporter.py` | Python/Pandas reproducible cleaning pipeline generator | Python | 5.8 KB | [READ FULLY] |

---

### Backend API Endpoints (`backend/api/`)
26 endpoint routers in `backend/api/` covering:
- `upload.py`, `profile.py`, `cleaning.py`, `sql.py`, `insights.py`, `dashboard.py`, `chat.py`, `export.py`, `health.py`, `sessions.py`, `tags.py`, `annotations.py`, `cell_edit.py`, `recipe.py`, `story.py`, `diff.py`, `data_preview.py`, `advanced_cleaning.py`, `collab.py`, `llm_keys.py`, `schema_override.py`, `templates.py`, `walkthrough.py`, `what_if.py`, `watchlist.py`, `target.py`.
- Status: All [READ FULLY].

---

### Frontend Components & Architecture (`frontend/src/`)
- `App.tsx`: Main layout, tab switcher, state coordinator [READ FULLY]
- `api/`: `client.ts`, `ingestion.ts`, `cleaning.ts`, `sql.ts` [READ FULLY]
- `hooks/`: `useUpload.ts`, `useCleaning.ts`, `useSQL.ts`, `useSession.ts`, `useTheme.tsx` [READ FULLY]
- `types/`: `ingestion.ts`, `profiling.ts`, `cleaning.ts`, `insight.ts`, `dashboard.ts`, `sql.ts` [READ FULLY]
- 77 UI components across upload, profiling, cleaning, SQL, insights, dashboard, reporting, chat, and grid [SKIMMED: UI layout verified against API contracts].
