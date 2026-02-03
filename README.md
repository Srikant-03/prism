<div align="center">

<h1>🔷 Prism</h1>
<p><strong>One file in. A full spectrum of intelligence out.</strong></p>

[![FastAPI](https://img.shields.io/badge/Backend-FastAPI-009688?style=flat-square&logo=fastapi)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/Frontend-React%20%2B%20Vite-61DAFB?style=flat-square&logo=react)](https://react.dev/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python)](https://python.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178C6?style=flat-square&logo=typescript)](https://typescriptlang.org)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

</div>

---

## What is Prism?

**Prism** is a full-stack, AI-augmented data intelligence platform. Upload any dataset — structured or messy, large or small — and Prism autonomously transforms it into a complete analytical workspace. It profiles your data, detects anomalies, cleans and transforms columns, powers a live SQL workbench, generates executive reports, and lets you converse with your data through an AI assistant.

No configuration. No code required. One file in — a full spectrum of insight out.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Feature Reference](#feature-reference)
  - [File Ingestion](#1-file-ingestion--format-handling)
  - [Autonomous Profiling](#2-autonomous-data-profiling)
  - [Live Data Grid](#3-live-data-grid)
  - [Data Cleaning Engine](#4-data-cleaning-engine)
  - [SQL Workbench](#5-sql-workbench)
  - [AI Chat Assistant](#6-ai-chat-assistant)
  - [Anomaly Watchlist](#7-anomaly-watchlist)
  - [Statistical Testing Suite](#8-statistical-testing-suite)
  - [Hypothesis Generator](#9-hypothesis-generator)
  - [What-If Simulator](#10-what-if-simulator)
  - [Column Relationship Graph](#11-column-relationship-graph)
  - [Dataset Comparison](#12-dataset-comparison-diff)
  - [Explain This Column](#13-explain-this-column)
  - [Data Story Builder](#14-data-story-builder)
  - [Data Recipe System](#15-data-recipe-system)
  - [Collaborative Annotations](#16-collaborative-annotations)
  - [Reporting & Export](#17-reporting--export)
  - [UI & Workspace](#18-ui--workspace)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Environment Variables](#environment-variables)
- [API Reference](#api-reference)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                     PRISM PLATFORM                      │
│                                                         │
│   ┌─────────────┐          ┌────────────────────────┐   │
│   │   Frontend  │  REST    │       Backend          │   │
│   │  React/Vite │ ◄──────► │     FastAPI + Gemini   │   │
│   │  TypeScript │  API     │     Python 3.10+       │   │
│   └─────────────┘          └────────────────────────┘   │
│         │                            │                  │
│   Ant Design UI               Pandas / NumPy            │
│   AG Grid                     SciPy / NetworkX          │
│   ECharts / Recharts          DuckDB (SQL Engine)       │
│   Monaco Editor               Gemini API (AI)           │
│   IndexedDB (persist)         ReportLab / python-docx   │
└─────────────────────────────────────────────────────────┘
```

---

## Feature Reference

### 1. File Ingestion & Format Handling

Prism handles virtually every tabular format without manual configuration.

| Format | Details |
|--------|---------|
| `.csv` | Auto-detects delimiter: comma, semicolon, pipe, tab, space |
| `.xlsx / .xls / .xlsm` | Multi-sheet support with interactive sheet selector |
| `.parquet / .feather` | Columnar formats read natively |
| `.json` | Flat and nested — auto-flattens with path-based column naming (e.g. `address.city`) |
| `.tsv / .txt` | Tab-separated and fixed-width tabular formats |
| `.xml` | Auto schema detection |
| `.sql` | Parses SQL dump files |
| `.zip / .gz` | Auto-decompresses on the fly |

**Additional ingestion capabilities:**
- **Auto Encoding Detection**: UTF-8, UTF-16, Latin-1, ISO-8859-1, Windows-1252, BOM markers
- **Chunked Reading**: Files over 50MB are read in configurable chunks — never loads the entire file into memory
- **Multi-file Upload**: Same schema → auto-merge; different schema → separate tables; mixed → user decision dialog
- **Real-time Progress**: Live progress bar with estimated time and memory usage during ingestion

---

### 2. Autonomous Data Profiling

Prism generates a complete dataset profile automatically, with no configuration required.

**Dataset-level metrics:**
- Row count, column count, total memory usage
- Duplicate row detection
- Missing value summary (total and per-column)
- Schema classification and type inference

**Column-level metrics (per column):**
- Data type (inferred, with override capability)
- Null count and null percentage
- Cardinality (unique value count)
- Min, Max, Mean, Median, Mode, Standard Deviation, Variance
- Skewness and Kurtosis for numeric columns
- Top-N most frequent values for categorical columns
- Value distribution histograms (inline, interactive)

**Quality Scoring:**
- Gamified 0–100 quality score with trophy tiers (Bronze → Platinum)
- Scored across 5 dimensions: Completeness, Consistency, Uniqueness, Validity, Timeliness
- Before/after improvement tracking during cleaning

---

### 3. Live Data Grid

A full Excel-like interactive data grid powered by AG Grid.

| Capability | Details |
|------------|---------|
| **Sort** | Click column header to cycle through asc / desc / none |
| **Multi-sort** | Hold `Shift` + click a second column for multi-column sort |
| **Filter** | Per-column filter bar with text search, numeric range sliders, date range pickers, and categorical multi-select |
| **Global Search** | Highlights matching cells across all columns simultaneously |
| **Pin Columns** | Freeze columns to the left or right (like Excel's freeze pane) |
| **Resize Columns** | Drag column borders to resize |
| **Reorder Columns** | Drag column headers to reorder |
| **Hide / Show Columns** | Column visibility panel with toggle switches |
| **Row Selection** | Single and multi-row selection with checkbox column |
| **Bulk Operations** | Delete, export, or annotate multiple selected rows |
| **Context Menu** | Right-click any cell for quick actions (copy, filter by, inspect) |
| **Conditional Formatting** | Heatmap background for numeric columns based on value intensity |
| **Auto-fit Columns** | Double-click column border to auto-size to content |

---

### 4. Data Cleaning Engine

A visual, no-code pipeline for data quality transformations.

**Operations available:**
- Drop columns by name or pattern
- Fill null values (mean, median, mode, constant, forward-fill, back-fill)
- Clip outliers (Z-score or IQR based)
- Normalize/standardize numeric columns (Min-Max, Z-score)
- Parse and standardize datetime columns
- Convert data types (string → numeric, etc.)
- Rename columns
- Deduplicate rows (by key columns or entire row)
- Drop rows where any / all values are null

**Schema Override Panel:** Manually correct inferred types per column before proceeding.

**Intelligent Cell Repair:** AI-powered suggestions for replacing corrupt or illogical cell values based on column distribution and domain context.

---

### 5. SQL Workbench

A fully featured, in-browser SQL query environment.

- **Monaco Editor** (same engine as VS Code) with SQL syntax highlighting, autocompletion, and keyword suggestions
- **Visual Query Builder**: Point-and-click interface for SELECT, JOIN, WHERE, GROUP BY without writing SQL
- **Schema Sidebar**: Always-visible column browser with types and sample values
- **Paginated Results**: Sortable and filterable result table with row count, execution time, and memory display
- **Auto-Visualization**: After every query, Prism analyzes the result shape and automatically renders the most appropriate chart:
  - Single numeric → KPI card
  - One dimension + one measure → Bar chart
  - One datetime + one measure → Line chart
  - Two numeric columns → Scatter plot
  - Categorical with low cardinality → Pie / Donut chart
  - Many columns, many rows → Heatmap data table
- **Query History**: Full history with timestamps, re-runnable with one click

---

### 6. AI Chat Assistant

A persistent, context-aware AI conversation panel powered by Gemini.

- Understands your full dataset schema, profiling results, and every cleaning step applied
- Multi-turn conversation with history
- Generates SQL from natural language queries
- Produces clickable **action cards** (run this SQL, navigate to this tab, apply this fix)
- Proactive insight suggestions based on detected patterns
- **Privacy-first**: Only schema and metadata are sent — raw data rows are never transmitted to the AI by default (user must explicitly approve)

---

### 7. Anomaly Watchlist

A persistent monitoring system for tracking data quality issues.

- One-click to "bookmark" a specific anomaly found during profiling
- Watchlist persists across sessions
- Configurable severity thresholds (warning / critical)
- Summary panel showing total active issues, resolved count, and trend

---

### 8. Statistical Testing Suite

Formal hypothesis testing with plain-English interpretations.

| Test | Use Case |
|------|---------|
| **Independent samples T-Test** | Compare means between two groups |
| **One-way ANOVA** | Compare means across 3+ groups |
| **Chi-Square Test** | Test independence between two categorical variables |
| **Pearson / Spearman Correlation** | Measure linear and rank-based relationships |
| **Shapiro-Wilk** | Test for normality |

Every test result is accompanied by a **plain English interpretation** so non-statisticians can act on the findings.

---

### 9. Hypothesis Generator

Automatically surfaces data hypotheses without any prompting.

- Scans column pairs and identifies statistically interesting relationships
- Generates insight cards like: *"Revenue is significantly higher for users in Tier A vs. Tier B (p < 0.01)"*
- Each hypothesis card shows: the finding, supporting statistics, confidence level, and a suggested action
- Bulk accept or dismiss hypotheses

---

### 10. What-If Simulator

Simulate the impact of any cleaning operation before committing it.

- Select a transformation (e.g., "Fill nulls in `salary` with median")
- Prism instantly previews: rows affected, column distribution before/after, memory delta, and quality score change
- Commit or discard — no irreversible actions

---

### 11. Column Relationship Graph

A force-directed, interactive graph of column correlations and relationships.

- Nodes = columns, coloured by data type
- Edges = correlation strength (thickness represents strength)
- Adjustable correlation threshold slider (filter out weak relationships)
- Hover tooltips show exact correlation coefficient
- Zoom, pan, and node-drag interactions powered by ECharts

---

### 12. Dataset Comparison (Diff)

Compare two versions of the same dataset (before vs. after cleaning).

- Side-by-side statistics for every column
- Row count deltas, null count changes, distribution shifts
- Columns added or removed are highlighted
- Value change summaries for key columns

---

### 13. Explain This Column

AI-powered natural language explanation of any column.

- Describes what the column likely represents based on its name, type, and distribution
- Identifies potential data quality risks
- Suggests downstream use cases (feature for ML, filter dimension, segment key, etc.)

---

### 14. Data Story Builder

Transform your analysis into a presentable data story.

- Automatically generates a sequence of "slides":
  - **Title slide**: Dataset name, row/column counts, key quality score
  - **KPI slide**: Top 3–5 most important numbers
  - **Insight slides**: One insight per significant finding
  - **Recommendation slide**: Suggested next steps
- Edit, reorder, or delete slides
- Export as a shareable HTML presentation

---

### 15. Data Recipe System

Save and reuse sequences of cleaning steps as "Recipes."

- After building a cleaning pipeline, save it as a named recipe
- Browse the recipe library
- Apply a saved recipe to any new dataset with one click
- Recipes are fully editable

---

### 16. Collaborative Annotations

Attach notes and findings to specific data points.

- Right-click any cell or column to add a note
- Notes are target-specific (column, row, or cell-level)
- Annotations are persisted per dataset
- Useful for flagging issues for teammates or future review

---

### 17. Reporting & Export

**Full Analysis Report** (Pillar 4.1):
- Executive summary in plain English
- Dataset overview and profiling findings
- Quality assessment with scored dimensions
- Preprocessing decisions log
- Before/after statistics for every transformed column
- Feature importance ranking
- Recommended next steps

Export formats: **PDF**, **DOCX**, **HTML**, **Jupyter Notebook**

> Toggle options: Include Charts, Include Anomaly Deep-dive

---

**Code Export** (Pillar 4.2):

Export the entire preprocessing pipeline as:

| Format | Description |
|--------|-------------|
| `pipeline.py` | Documented Python script using pandas & scikit-learn |
| `pipeline.ipynb` | Jupyter Notebook with markdown explanations |
| `pipeline.json` | Machine-readable pipeline definition |
| `pipeline.sql` | SQL queries for database reproduction |

---

**Data Export** (Pillar 4.3):

Export the cleaned dataset as: **CSV**, **Excel**, **JSON**, **Parquet**, **Feather**, **SQL INSERT statements**

---

### 18. UI & Workspace

| Feature | Details |
|---------|---------|
| **Themes** | Dark, Light, and Cyberpunk modes — persists across sessions |
| **Onboarding** | Step-by-step first-run walkthrough for new users |
| **Responsiveness** | Optimized for desktop and tablet (≥768px) |
| **Session Persistence** | IndexedDB-powered session recovery across page reloads |
| **Privacy Disclosure** | Explicit consent gate before any AI feature is used |
| **Accessibility** | Semantic HTML, ARIA labels, keyboard-navigable interactions |

---

## Project Structure

```
prism/
├── backend/
│   ├── main.py                   # FastAPI app entry point, all routers registered
│   ├── config.py                 # App configuration (CORS, env vars)
│   ├── api/
│   │   ├── upload.py             # File ingestion & format detection
│   │   ├── profiling.py          # Dataset + column profiling
│   │   ├── cleaning.py           # Cleaning transformations
│   │   ├── sql.py                # SQL query engine (DuckDB)
│   │   ├── reporting.py          # Report generation & data export
│   │   ├── chat.py               # AI assistant API
│   │   ├── grid.py               # Live data grid API
│   │   ├── watchlist.py          # Anomaly watchlist
│   │   ├── simulate.py           # What-If simulator
│   │   ├── stats.py              # Statistical testing
│   │   ├── explain.py            # Column explainer (AI)
│   │   ├── graph.py              # Relationship graph API
│   │   ├── story.py              # Data story builder
│   │   ├── recipe.py             # Data recipe system
│   │   ├── metadata.py           # Column tagger / metadata
│   │   └── collab.py             # Collaborative annotations
│   ├── ingestion/
│   │   ├── file_detector.py      # Format detection & routing
│   │   ├── encoding_detector.py  # Encoding auto-detection
│   │   └── multi_file_handler.py # Multi-upload schema comparison
│   ├── profiling/
│   │   ├── profiler.py           # Core profiling engine
│   │   ├── type_inference.py     # Column type inference
│   │   └── quality_scoring.py    # Quality score computation
│   ├── cleaning/
│   │   ├── cleaning_engine.py    # Transformation pipeline
│   │   ├── cell_repair.py        # Intelligent cell repair
│   │   └── simulator.py          # What-If simulation
│   ├── insights/
│   │   ├── anomaly_detector.py   # Z-Score & Isolation Forest
│   │   ├── hypothesis_engine.py  # Automated hypothesis generation
│   │   ├── stat_tests.py         # Statistical test suite
│   │   └── graph_builder.py      # Relationship graph construction
│   ├── comparison/
│   │   └── diff_engine.py        # Dataset diff engine
│   ├── chat/
│   │   └── engine.py             # AI conversation engine (Gemini)
│   └── reporting/
│       └── report_builder.py     # PDF/DOCX/HTML report generator
│
└── frontend/
    └── src/
        ├── App.tsx                # Root application shell
        ├── index.css              # Global design system (dark/light/cyberpunk)
        ├── components/
        │   ├── upload/            # FileUploader, UploadProgress, SheetSelector
        │   ├── profiling/         # ProfileDashboard, ColumnExplainer, ColumnTagger
        │   ├── grid/              # LiveDataGrid, GridToolbar
        │   ├── cleaning/          # CleaningPanel, RecipeBrowser, CellRepairPanel
        │   ├── sql/               # QueryWorkbench, VisualQueryBuilder
        │   ├── insights/          # HypothesisCards, StatTestPanel, RelationshipGraph
        │   ├── comparison/        # DatasetDiff
        │   ├── reporting/         # ReportPanel, StoryBuilder
        │   ├── chat/              # ChatSidebar, PrivacyDisclosure
        │   ├── collab/            # CollabPanel
        │   └── common/            # Layout, ThemeToggle, OnboardingWalkthrough
        ├── hooks/
        │   ├── useUpload.ts       # Upload state machine
        │   ├── useTheme.tsx       # Dark/Light/Cyberpunk theme context
        │   ├── useSession.ts      # Session state management
        │   └── useCleaning.ts     # Cleaning pipeline state
        └── utils/
            └── persistence.ts     # IndexedDB session storage
```

---

## Setup & Installation

### Prerequisites

| Tool | Version |
|------|---------|
| Python | 3.10 or higher |
| Node.js | 18 or higher |
| npm | 9 or higher |

---

### 1. Clone the Repository

```bash
git clone <your-repo-url> prism
cd prism
```

---

### 2. Backend Setup

```powershell
cd backend

# Create and activate a virtual environment (recommended)
python -m venv venv
venv\Scripts\activate       # Windows
# source venv/bin/activate  # macOS / Linux

# Install dependencies
pip install -r requirements.txt

# Create your environment file
copy .env.example .env      # Windows
# cp .env.example .env      # macOS / Linux

# Add your Gemini API key to .env (see Environment Variables section)
```

**Start the backend server:**

```powershell
python main.py
```

> The API is now live at `http://localhost:8000`  
> Interactive API docs: `http://localhost:8000/docs`

---

### 3. Frontend Setup

Open a second terminal:

```powershell
cd frontend

# Install dependencies
npm install

# Start the development server
npm run dev
```

> The app is now live at `http://localhost:5173`

---

### 4. Production Build (Optional)

```powershell
cd frontend
npm run build
# Outputs to frontend/dist/
```

---

## Environment Variables

Create a `.env` file in the `backend/` directory:

```env
# Required — Gemini API key for AI features (Chat, Explain, Hypothesis)
GEMINI_API_KEY=your_gemini_api_key_here

# Optional — Backend server settings (defaults shown)
HOST=0.0.0.0
PORT=8000
DEBUG=true

# Optional — CORS origins (comma-separated)
CORS_ORIGINS=http://localhost:5173,http://localhost:3000
```

> **Get a Gemini API key**: [https://aistudio.google.com/app/apikey](https://aistudio.google.com/app/apikey)  
> AI features (Chat, Column Explainer, Hypothesis Generator) require this key. All other features work without it.

---

## API Reference

All endpoints are prefixed with `/api/`. Full interactive documentation is available at `http://localhost:8000/docs` after starting the backend.

| Router | Base Path | Description |
|--------|-----------|-------------|
| Upload | `/api/upload` | File ingestion, format detection |
| Profiling | `/api/profile` | Dataset and column profiling |
| Cleaning | `/api/clean` | Apply transformations |
| SQL | `/api/sql` | Execute SQL queries |
| Reporting | `/api/reporting` | Generate and export reports |
| Chat | `/api/chat` | AI assistant messages |
| Grid | `/api/grid` | Live data grid data source |
| Watchlist | `/api/watchlist` | Anomaly watchlist management |
| Simulate | `/api/simulate` | What-If impact prediction |
| Statistics | `/api/stats` | Statistical test execution |
| Explain | `/api/explain` | Column AI explanation |
| Graph | `/api/graph` | Column relationship graph data |
| Story | `/api/story` | Data story generation |
| Recipe | `/api/recipe` | Cleaning recipe management |
| Metadata | `/api/metadata` | Column tagging |
| Collab | `/api/collab` | Collaborative annotations |

---

## License

MIT — Built by Srikant.
