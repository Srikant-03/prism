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

**Prism** is a full-stack, AI-augmented data intelligence platform. Upload any dataset — CSV, Excel, JSON, Parquet, XML, SQL dumps, or compressed archives — and Prism autonomously profiles it, detects anomalies, cleans it, lets you query it with SQL or plain English, and generates exportable reports. No configuration, no code, no setup wizards.

The entire platform runs locally on your laptop. Your data never leaves your machine (AI features send only column metadata, never raw rows, unless you explicitly opt in).

---

## Table of Contents

- [How It Works](#how-it-works)
- [Quick Start (Run It in 2 Minutes)](#quick-start)
- [Manual Setup](#manual-setup)
- [Environment Variables](#environment-variables)
- [All Features](#all-features)
- [Project Structure](#project-structure)
- [API Reference](#api-reference)
- [Stopping Prism](#stopping-prism)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## How It Works

Prism has two parts: a **Python backend** (FastAPI) and a **React frontend** (Vite). They communicate via REST API on `localhost`.

Here is the complete user journey, from launch to insight:

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                              PRISM WORKFLOW                                    │
│                                                                                │
│  1. LAUNCH                                                                     │
│     Double-click start.bat (Windows)                                           │
│     → Creates Python venv + installs backend deps (pip)                        │
│     → Installs frontend deps (npm)                                             │
│     → Starts FastAPI server on :8000                                           │
│     → Starts Vite dev server on :5173                                          │
│     → Auto-opens your browser                                                  │
│                                                                                │
│  2. UPLOAD                                                                     │
│     Drag-and-drop or click to upload any supported file                        │
│     → Backend auto-detects format, encoding, delimiter                         │
│     → If Excel: prompts you to pick which sheets to import                     │
│     → If malformed rows found: shows them for review (accept/drop/reject)      │
│     → If multiple files: asks to merge (same schema) or keep separate          │
│     → Displays real-time upload progress bar                                   │
│                                                                                │
│  3. AUTO-PROFILE                                                               │
│     Immediately after upload, the backend generates a full dataset profile:    │
│     → Row/column counts, memory usage, duplicate detection                     │
│     → Per-column: type, nulls, cardinality, stats, distribution histograms    │
│     → Data quality score (0–100) across 5 dimensions                           │
│     → Primary key detection, foreign key candidates, ID column flagging        │
│     → Anomaly detection (Z-score + Isolation Forest)                           │
│     → Feature importance ranking                                               │
│     → Auto-generated analyst briefing                                          │
│                                                                                │
│  4. INTERACT                                                                   │
│     The main dashboard opens with these tabs:                                  │
│                                                                                │
│     📊 Data Preview     — Paginated table of your raw data                     │
│     📋 Data Grid        — Full AG Grid with sort, filter, search, pin, resize  │
│     💻 SQL Engine       — Monaco editor + DuckDB + auto-visualization          │
│     📄 Reporting        — Export reports (PDF/DOCX/HTML/Notebook) + data        │
│     🔗 Relationship     — Force-directed column correlation graph              │
│                                                                                │
│     Plus floating buttons:                                                     │
│     🤖 AI Chat (Gemini) — Ask questions, get SQL, receive action cards         │
│     🎨 Theme Toggle     — Switch between Dark / Light mode                     │
│                                                                                │
│  5. CLEAN                                                                      │
│     Inside the profiling dashboard, a Cleaning tab lets you:                   │
│     → Apply no-code transformations (fill nulls, drop cols, scale, etc.)       │
│     → Preview impact before applying (What-If Simulator)                       │
│     → Use AI Cell Repair for corrupt values                                    │
│     → Override inferred column types (Schema Override panel)                   │
│                                                                                │
│  6. EXPORT                                                                     │
│     → Full analysis report: PDF, DOCX, HTML, Jupyter Notebook                 │
│     → Pipeline code: Python script, Notebook, JSON, SQL                        │
│     → Cleaned data: CSV, Excel, JSON, Parquet, Feather, SQL INSERT             │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

<h2 id="quick-start">🚀 Quick Start (Run It in 2 Minutes)</h2>

### Prerequisites

You need these installed on your machine:

| Tool | Minimum Version | Download |
|------|----------------|----------|
| **Python** | 3.10+ | [python.org/downloads](https://www.python.org/downloads/) |
| **Node.js** | 18+ | [nodejs.org](https://nodejs.org/) |
| **Git** | Any | [git-scm.com](https://git-scm.com/) |

> **Verify installations:** Open a terminal and run `python --version` and `node --version`. Both should print a version number.

### Step 1: Clone

```bash
git clone https://github.com/Srikant-03/prism.git
cd prism
```

### Step 2: Get a Gemini API Key (Free)

1. Go to [Google AI Studio](https://aistudio.google.com/app/apikey)
2. Click **"Create API Key"**
3. Copy the key (starts with `AIza...`)

### Step 3: Configure Environment

```bash
# Windows
copy backend\.env.example backend\.env

# Mac / Linux
cp backend/.env.example backend/.env
```

Open `backend/.env` in any text editor and paste your Gemini key:
```
GEMINI_API_KEY=AIzaSy...your_key_here
```

> **Note:** The `DATA_INTEL_API_KEY` is used for internal API auth. For local use, the default value works fine. Change it if you plan to expose the backend to a network.

### Step 4: Launch

**Windows:**
```
Double-click start.bat
```
That's it. The script will:
1. Create a Python virtual environment in `backend/venv/`
2. Install all Python dependencies from `backend/requirements.txt`
3. Install all Node.js dependencies from `frontend/package.json`
4. Start the backend server (http://localhost:8000)
5. Start the frontend dev server (http://localhost:5173)
6. Open your browser automatically

**Mac / Linux:**
You need two terminal windows.

*Terminal 1 — Backend:*
```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```

*Terminal 2 — Frontend:*
```bash
cd frontend
npm install
npm run dev
```

Then open http://localhost:5173 in your browser.

---

## Manual Setup

If the quick start doesn't work, here's a detailed breakdown:

### Backend

```bash
cd backend

# 1. Create virtual environment
python -m venv venv

# 2. Activate it
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Set up environment variables
# Windows: copy .env.example .env
# Mac/Linux: cp .env.example .env
# Then edit .env and add your GEMINI_API_KEY

# 5. Start the server
python main.py
```

The backend starts at **http://localhost:8000**. You can verify by visiting:
- http://localhost:8000 → JSON status response
- http://localhost:8000/health → `{"status": "healthy"}`
- http://localhost:8000/docs → Interactive Swagger API docs

### Frontend

```bash
cd frontend

# 1. Install dependencies
npm install

# 2. Start dev server
npm run dev
```

The frontend starts at **http://localhost:5173**.

---

## Environment Variables

All configuration is in `backend/.env`. Copy from `backend/.env.example` to get started.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GEMINI_API_KEY` | **Yes** (for AI features) | *(empty)* | Your Google Gemini API key. Get one free at [aistudio.google.com](https://aistudio.google.com/app/apikey) |
| `GEMINI_API_KEYS` | No | *(falls back to single key)* | Comma-separated list of keys for automatic rotation |
| `DATA_INTEL_API_KEY` | No | `dev-secret-key-123` | API key for backend request authentication |
| `HOST` | No | `0.0.0.0` | Backend server bind address |
| `PORT` | No | `8000` | Backend server port |
| `DEBUG` | No | `false` | Enable debug mode (auto-reload, stack traces) |
| `CORS_ORIGINS` | No | `http://localhost:5173,http://localhost:3000` | Allowed frontend origins |
| `LLM_TEMPERATURE` | No | `0.1` | AI response temperature (0 = deterministic, 1 = creative) |
| `LLM_MAX_TOKENS` | No | `4096` | Maximum tokens per AI response |

> **Without a Gemini key**: The AI Chat, Column Explainer, Hypothesis Generator, and Natural Language SQL features will not work. Every other feature (profiling, cleaning, SQL, reporting, grid, etc.) works perfectly without any API key.

---

## All Features

### 📥 File Ingestion & Format Handling

| Format | Details |
|--------|---------|
| `.csv` | Auto-detects delimiter: comma, semicolon, pipe, tab, space |
| `.xlsx / .xls / .xlsm` | Multi-sheet support with interactive sheet selector |
| `.parquet / .feather` | Columnar formats read natively via PyArrow |
| `.json` | Flat and nested — auto-flattens with path-based column naming (`address.city`) |
| `.tsv / .txt` | Tab-separated and fixed-width tabular formats |
| `.xml` | Auto schema detection |
| `.sql` | Parses SQL dump files |
| `.zip / .gz` | Auto-decompresses on the fly |

- **Auto Encoding Detection**: UTF-8, UTF-16, Latin-1, ISO-8859-1, Windows-1252
- **Chunked Reading**: Files over 50MB are processed in 5MB chunks — never loads the entire file into memory
- **Multi-file Upload**: Same schema → auto-merge; different schema → separate tables; mixed → user decision dialog
- **Malformed Row Handler**: Detects and reports malformed rows — accept best-effort, drop bad rows, or reject the file

---

### 📊 Autonomous Data Profiling

Generates a complete dataset profile with zero configuration:

**Dataset-level:**
- Row count, column count, total memory usage, duplicate row detection
- Schema classification and type inference
- Gamified **Data Quality Score** (0–100) with trophy tiers (Bronze → Platinum)
- Scored across 5 dimensions: Completeness, Consistency, Uniqueness, Validity, Timeliness

**Column-level (for every column):**
- Inferred data type (with manual override capability)
- Null count and percentage
- Cardinality (unique value count)
- Statistics: Min, Max, Mean, Median, Mode, Std Dev, Variance, Skewness, Kurtosis
- Top-N most frequent values
- Interactive value distribution histograms
- Primary key candidate detection (with intelligent heuristics — excludes floats, validates against dataset size)
- Foreign key candidate detection
- ID column flagging (for exclusion from analysis)

**AI-Powered Insights (requires Gemini key):**
- Anomaly detection (Z-score + Isolation Forest)
- Feature importance ranking
- Auto-generated analyst briefing

---

### 📋 Live Data Grid (AG Grid)

A full Excel-like interactive grid:

| Capability | Details |
|------------|---------|
| **Sort** | Click column header to cycle asc / desc / none |
| **Multi-sort** | Hold `Shift` + click for multi-column sort |
| **Filter** | Per-column filters: text search, numeric range, date range, categorical multi-select |
| **Global Search** | Highlights matching cells across all columns |
| **Pin Columns** | Freeze columns to left or right |
| **Resize / Reorder** | Drag column borders or headers |
| **Hide / Show** | Column visibility panel with toggles |
| **Row Selection** | Single and multi-row selection with checkboxes |
| **Context Menu** | Right-click for quick actions |
| **Conditional Formatting** | Heatmap backgrounds for numeric columns |

---

### 🧹 Data Cleaning Engine

A visual, no-code pipeline:

- Drop columns by name or pattern
- Fill null values (mean, median, mode, constant, forward-fill, back-fill)
- Clip outliers (Z-score or IQR based)
- Normalize / standardize (Min-Max, Z-score)
- Parse and standardize datetime columns
- Convert data types
- Rename columns
- Deduplicate rows
- Drop rows where any/all values are null

**Schema Override Panel:** Manually correct inferred types per column.

**Intelligent Cell Repair (AI):** Scans for corrupt or illogical cell values and suggests replacements based on column distribution and context.

**What-If Simulator:** Preview the exact impact of any transformation (rows affected, distribution before/after, memory delta, quality score change) before committing.

---

### 💻 SQL Workbench

- **Monaco Editor** — VS Code's editor with SQL syntax highlighting and autocompletion
- **DuckDB Engine** — Lightning-fast, in-memory analytical SQL processing
- **Natural Language → SQL** — Type "show me the top 5 cities by revenue" and the AI writes the query
- **Visual Query Builder** — Point-and-click interface for SELECT, JOIN, WHERE, GROUP BY
- **Schema Sidebar** — Always-visible column browser with types and sample values
- **Paginated Results** — Sortable, filterable result table with execution time and row count
- **Auto-Visualization** — After every query, Prism charts the result automatically:
  - Single numeric → KPI card
  - 1 dimension + 1 measure → Bar chart
  - 1 datetime + 1 measure → Line chart
  - 2 numeric → Scatter plot
  - Low-cardinality categorical → Pie/Donut
  - Many columns → Heatmap table
- **Query History** — Full history with timestamps, re-runnable with one click

---

### 🤖 AI Chat Assistant (Gemini)

- Understands your full dataset schema, profiling results, and every cleaning step applied
- Multi-turn conversation with persistent history
- Generates SQL from natural language
- Produces clickable **action cards** (run SQL, navigate to tab, apply fix)
- **Privacy-first**: Only metadata is sent to the AI. Raw data rows are never transmitted unless you explicitly approve via the **Privacy Disclosure Gate**.

---

### 📈 Statistical Testing Suite

| Test | Use Case |
|------|----------|
| **T-Test** | Compare means between two groups |
| **One-way ANOVA** | Compare means across 3+ groups |
| **Chi-Square** | Test independence between categorical variables |
| **Pearson / Spearman** | Measure linear and rank-based relationships |
| **Shapiro-Wilk** | Test for normality |

Every result includes a **plain English interpretation**.

---

### 🔍 Additional Intelligence Features

- **Hypothesis Generator** — Automatically surfaces interesting statistical relationships across column pairs with insight cards
- **Column Relationship Graph** — Force-directed network visualization of correlations (zoom, pan, drag, threshold slider)
- **Explain This Column (AI)** — Natural language description of any column's likely meaning, quality risks, and downstream use cases
- **Dataset Comparison (Diff)** — Side-by-side before/after statistics for every column
- **Anomaly Watchlist** — Bookmark and track data quality issues across sessions
- **Data Story Builder** — Auto-generates a slide sequence (title, KPIs, insights, recommendations) exportable as HTML
- **Data Recipe System** — Save cleaning pipelines as reusable recipes, apply to new datasets
- **Collaborative Annotations** — Attach notes to cells, columns, or rows; persisted per dataset
- **Column Tagging / Metadata** — Tag columns with custom metadata labels

---

### 📄 Reporting & Export

**Analysis Reports:** PDF, DOCX, HTML, Jupyter Notebook — includes executive summary, profiling findings, quality assessment, preprocessing log, before/after stats, feature importance, and recommendations.

**Code Export:** Export the entire preprocessing pipeline as:
- `pipeline.py` — Documented Python script
- `pipeline.ipynb` — Jupyter Notebook with markdown
- `pipeline.json` — Machine-readable pipeline
- `pipeline.sql` — SQL reproduction

**Data Export:** CSV, Excel, JSON, Parquet, Feather, SQL INSERT statements

---

### 🎨 UI & Workspace

| Feature | Details |
|---------|---------|
| **Themes** | Dark and Light modes, persisted across sessions |
| **Onboarding Tour** | Step-by-step walkthrough for first-time users |
| **Session Persistence** | IndexedDB-powered — reload the browser and never lose your place |
| **Privacy Disclosure** | Explicit consent gate before any AI feature is used |
| **Accessibility** | Semantic HTML, ARIA labels, keyboard navigation |
| **Responsive Design** | Optimized for desktop and tablet (≥768px) |

---

## Project Structure

```
prism/
├── start.bat                     # One-click launcher (Windows)
├── stop.bat                      # Kill both servers (Windows)
├── .gitignore
├── README.md
│
├── backend/
│   ├── main.py                   # FastAPI entry point (16 routers, CORS, rate limiter)
│   ├── config.py                 # All settings (IngestionConfig, CleaningConfig, LLMConfig, AppConfig)
│   ├── requirements.txt          # Python dependencies
│   ├── .env.example              # Environment variable template
│   │
│   ├── api/                      # REST API routers
│   │   ├── upload.py             # POST /api/upload — file ingestion & format detection
│   │   ├── profiling.py          # GET /api/profile/{file_id} — full dataset profiling
│   │   ├── cleaning.py           # POST /api/clean — apply transformations
│   │   ├── sql.py                # POST /api/sql — execute SQL queries (DuckDB)
│   │   ├── reporting.py          # POST /api/reporting — generate & export reports
│   │   ├── chat.py               # POST /api/chat — AI assistant messages
│   │   ├── grid.py               # GET /api/grid — live data grid data source
│   │   ├── watchlist.py          # Anomaly watchlist CRUD
│   │   ├── simulate.py           # What-If impact simulation
│   │   ├── stats.py              # Statistical test execution
│   │   ├── explain.py            # AI column explanation
│   │   ├── graph.py              # Column relationship graph data
│   │   ├── story.py              # Data story generation
│   │   ├── recipe.py             # Cleaning recipe management
│   │   ├── metadata.py           # Column tagging
│   │   ├── collab.py             # Collaborative annotations
│   │   └── dependencies.py       # Shared auth middleware (API key verification)
│   │
│   ├── ingestion/                # File format detection, parsing, chunked reading
│   ├── profiling/                # Statistical profiling, correlation, key detection
│   ├── cleaning/                 # Decision engine, handlers (missing, outlier, type, text...)
│   ├── insights/                 # Quality scoring, anomaly detection, feature ranking
│   ├── sql/                      # DuckDB engine, NL→SQL, query builder, templates
│   ├── llm/                      # Gemini API key rotation & rate limiting
│   ├── reporting/                # Report generation (PDF/DOCX/HTML), code & data export
│   └── tests/                    # pytest test suites
│
└── frontend/
    ├── package.json              # Node.js dependencies
    ├── index.html                # SPA entry point
    └── src/
        ├── App.tsx               # Root component (upload state machine → dashboard tabs)
        ├── index.css             # Global design system (dark mode, glassmorphism, animations)
        ├── components/
        │   ├── upload/           # FileUploader, UploadProgress, SheetSelector, MalformedViewer
        │   ├── profiling/        # ProfileDashboard, ColumnExplainer, SchemaOverride
        │   ├── grid/             # LiveDataGrid (AG Grid)
        │   ├── cleaning/         # CleaningDashboard, CellRepairPanel, RecipeBrowser
        │   ├── sql/              # QueryWorkbench, VisualQueryBuilder, Monaco Editor
        │   ├── insights/         # HypothesisCards, StatTestPanel, RelationshipGraph
        │   ├── comparison/       # DatasetDiff
        │   ├── reporting/        # ReportPanel, StoryBuilder
        │   ├── chat/             # ChatSidebar, PrivacyDisclosure
        │   ├── collab/           # CollabPanel
        │   └── common/           # Layout, ThemeToggle, OnboardingWalkthrough
        ├── hooks/                # React hooks (useUpload, useTheme, useSession, useCleaning, useSQL)
        ├── api/                  # Axios API client functions (ingestion.ts, cleaning.ts, etc.)
        └── types/                # TypeScript type definitions
```

---

## API Reference

All endpoints are prefixed with `/api/`. Full interactive documentation is available at **http://localhost:8000/docs** after starting the backend.

| Router | Base Path | Key Endpoints |
|--------|-----------|---------------|
| Upload | `/api/upload` | `POST /` — upload files, `POST /select-sheets`, `POST /confirm-malformed` |
| Profiling | `/api/profile` | `GET /{file_id}` — full profile with insights |
| Cleaning | `/api/clean` | `POST /analyze`, `POST /apply`, `POST /cell-repair` |
| SQL | `/api/sql` | `POST /execute`, `POST /nl-query`, `GET /schema`, `POST /visual-query` |
| Reporting | `/api/reporting` | `POST /report`, `POST /export/code`, `POST /export/data` |
| Chat | `/api/chat` | `POST /message` |
| Grid | `/api/grid` | `GET /{file_id}` |
| Watchlist | `/api/watchlist` | CRUD operations |
| Simulate | `/api/simulate` | `POST /` — preview impact |
| Statistics | `/api/stats` | `POST /test` — run statistical tests |
| Explain | `/api/explain` | `POST /{column}` — AI column explanation |
| Graph | `/api/graph` | `GET /{file_id}` — correlation graph |
| Story | `/api/story` | `POST /generate` — data story slides |
| Recipe | `/api/recipe` | CRUD operations |
| Metadata | `/api/metadata` | `POST /{file_id}/tag` |
| Collab | `/api/collab` | Annotation CRUD |

---

## Stopping Prism

**Windows:** Double-click `stop.bat` — it kills both server windows automatically.

**Mac / Linux:** Press `Ctrl+C` in each terminal window.

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `ModuleNotFoundError: No module named 'X'` | Re-run `pip install -r requirements.txt` inside the activated `backend/venv` |
| Frontend shows blank page | Check that the backend is running on :8000 (`http://localhost:8000/health`) |
| AI features don't work | Verify `GEMINI_API_KEY` is set in `backend/.env`. Get a key from [aistudio.google.com](https://aistudio.google.com/app/apikey) |
| `python` command not found | Use `python3` instead (common on Mac/Linux), or add Python to your PATH |
| Port 8000 or 5173 already in use | Change `PORT` in `.env` for backend, or edit `vite.config.ts` for frontend |
| `npm install` fails | Delete `frontend/node_modules` and `frontend/package-lock.json`, then re-run `npm install` |

---

## License

MIT — Designed and built by **Srikant**.
