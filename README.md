<div align="center">

<h1>🔷 Prism — Data Intelligence Platform</h1>
<p><strong>One file in. A full spectrum of intelligence out.</strong></p>

[![FastAPI](https://img.shields.io/badge/Backend-FastAPI-009688?style=flat-square&logo=fastapi)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/Frontend-React%20%2B%20Vite-61DAFB?style=flat-square&logo=react)](https://react.dev/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python)](https://python.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178C6?style=flat-square&logo=typescript)](https://typescriptlang.org)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

</div>

---

## 🚀 What is Prism?

**Prism** is a full-stack, AI-augmented data intelligence platform designed to replace hours of manual data wrangling with a seamless, zero-config experience. 

Upload any dataset — structured or messy, large or small — and Prism autonomously transforms it into a complete analytical workspace. Discover insights instantly, clean data visually without code, query via SQL or plain English, and generate executive reports in one click.

Whether you are a data scientist needing rapid EDA, or a business analyst requiring quick insights, **Prism bridges the gap between raw data and actionable intelligence.**

---

## ✨ Features at a Glance

Prism packs an enormous suite of features into an elegant, highly responsive modern UI. **No configuration. No code required.**

### 📥 1. Universal File Ingestion
- **Format Agnostic**: Natively handles `.csv`, `.xlsx`, `.xlsm`, `.parquet`, `.json` (nested & flat), `.xml`, `.tsv`, `.txt`, and `.sql` dumps.
- **Auto-Detection**: Instantly detects encodings (UTF-8, Latin-1, Windows-1252), delimiters, and malformed rows.
- **Massive File Support**: Uses chunked reading for files over 50MB to strictly manage memory.
- **Zip / Gz Extraction**: Auto-decompresses compressed archives on the fly.
- **Multi-File Stitching**: Drag in multiple matching files at once and Prism will intelligently union them.

### 🧠 2. Autonomous AI Data Profiling
- **Immediate Insights**: Generates a rich, interactive profile of your entire dataset the moment it uploads.
- **Metrics on Everything**: Row/column counts, missing values, duplicates, and memory usage.
- **Column-Level Details**: Statistical summaries (Min, Max, Mean, Median, Mode, Variance, Skewness, Kurtosis), histograms, cardinality, and type distributions.
- **Gamified Data Quality Score**: Grades your data out of 100 on Completeness, Consistency, Uniqueness, Validity, and Timeliness.

### 🧹 3. Visual Data Cleaning Engine
- **No-Code Pipeline**: UI-driven operations to drop columns, fill missing values, clip outliers (Z-score/IQR), normalize, scale, and standardize dates.
- **Intelligent Cell Repair**: AI scans the data and flags specific illogical cells (typos, impossible dates), offering 1-click replacement recommendations.
- **Schema Overrides**: Review inferred types and manually override them to dictate how they behave in downstream analysis.
- **What-If Simulator**: Preview the exact memory, row, and distribution impact of any cleaning operation *before* you apply it.

### 🗃️ 4. Advanced SQL Workbench
- **In-Browser IDE**: Monaco Editor powered SQL interface with syntax highlighting and auto-completion.
- **DuckDB Engine**: Lightning-fast, in-memory analytical SQL processing.
- **Natural Language to SQL**: Ask "What were the top 5 regions by sales?" and the AI will write the exact SQL query.
- **Auto-Visualization**: Automatically charts the results of your query (bar, line, scatter, pie, or heatmap) based on the resultant data shape.

### 🤖 5. Context-Aware AI Chat Assistant
- **Continuous Intelligence**: A persistent chat panel powered by Google Gemini that knows your schema, profiling results, and cleaning history.
- **Actionable Responses**: Generates clickable UI actions directly in chat to jump to specific tabs or apply cleaning operations.
- **Strict Privacy Gate**: Raw data is NEVER sent to the AI without an explicit, verifiable opt-in from the user. Only metadata is shared by default.

### 🎯 6. Insight & Reporting Generation
- **Analyst Briefing**: Generates a multi-page, executive-style summary of your dataset.
- **Feature Importance Ranking**: Automatically determines which columns act as the strongest predictors in your data.
- **Automated Anomaly Watchlist**: Flags unusual outliers across all dimensions for your review.
- **Export Everywhere**: Export your insights, data stories, and analysis directly to **PDF, DOCX, HTML, or Jupyter Notebooks**. Generate pipeline code in Python or export cleaned data back to CSV/Excel/Parquet.

### 📊 7. Multi-Dimensional Cross-Analysis
- **Relationship Graph**: Force-directed network diagram showing strong correlations between columns.
- **Statistical Tests**: 1-click Pearson/Spearman correlations, T-Tests, ANOVAs, and Chi-Square tests translated into plain English explanations.
- **Hypothesis Generator**: Evaluates column pairs in the background and surfaces interesting statistical hypotheses without being asked.
- **Dataset Diffing**: Compare the "Before" and "After" state of your dataset to verify your cleaning pipeline results.

### 🎨 8. Premium Technical Aesthetic
- Seamless Dark, Light, and Cyberpunk UI themes utilizing glassmorphism and subtle animations.
- Persistent IndexedDB state management — reload the browser and never lose your place.
- Interactive, responsive AG Grid implementation for massive live-data rendering.

---

## 🛠️ Tech Stack

**Frontend:**
*   **React 18** + **Vite**
*   **TypeScript**
*   **Ant Design (antd)** + Custom CSS Design System
*   **AG Grid** for high-performance data visualization
*   **Monaco Editor** for SQL
*   **Recharts / ECharts**

**Backend:**
*   **FastAPI** + **Uvicorn**
*   **Python 3.10+** (Pandas, NumPy, Scikit-Learn, SciPy)
*   **DuckDB** (Blazing fast in-process SQL OLAP)
*   **Google Gemini AI** (`google-generativeai`)
*   **slowapi** (Rate Limiting) & **python-docx** / **ReportLab** (Exports)

---

## 💻 Step-by-Step Installation

Anyone can run Prism entirely locally on their machine.

### Prerequisites
1. **Python 3.10** or higher installed and added to your systemic PATH.
2. **Node.js 18** or higher installed.
3. Git installed.

### Step 1: Clone the Repository
```bash
git clone https://github.com/your-username/prism.git
cd prism
```

### Step 2: Environment Configuration
Prism needs an API key to run its AI functions (like the internal Chat Assistant).
1. Go to `backend/` and copy the template environment file:
   - On Windows: `copy .env.example .env`
   - On Mac/Linux: `cp .env.example .env`
2. Open the newly created `backend/.env` file.
3. Obtain a free Gemini API key from [Google AI Studio](https://aistudio.google.com/app/apikey).
4. Paste your key into the `GEMINI_API_KEY=` field.
*(Note: Prism secures API interactions locally using `DATA_INTEL_API_KEY`. You can leave this as the default for local use).*

### Step 3: Launching the Application

**🟢 For Windows Users (The Easy Way):**
Simply double-click the `start.bat` file in the root of the project. 
The script will automatically:
- Create a Python Virtual Environment.
- Install all Backend dependencies (`pip install`).
- Install all Frontend dependencies (`npm install`).
- Launch both servers and automatically open the application in your browser.

**🔵 For Mac / Linux Users (Manual Launch):**
You'll need two terminal windows.

*Terminal 1 - Backend:*
```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```
*(Backend runs on http://localhost:8000)*

*Terminal 2 - Frontend:*
```bash
cd frontend
npm install
npm run dev
```
*(Frontend runs on http://localhost:5173)*

Open your browser to the Frontend URL to access Prism!

---

## 🔒 Security & Privacy Architecture

Prism is designed primarily as a local-first workspace.
- **AI Privacy Gate**: Prism comes with a hard user-consent gate. By default, **only column headers, types, and mathematical abstractions (like min/max bounds) are ever shared with the AI endpoint**. Raw cell data is walled off unless the user explicitly checks the "Enable Insight Generation" consent box.
- **Rate Limiting**: Built-in backend rate-limiting (`slowapi`) prevents API abuse.
- **API Key Required**: All backend routes enforce API key checks if you choose to deploy Prism behind a reverse proxy or on a remote VM.

---

## 📜 License

MIT License. Designed and Built by Srikant.
