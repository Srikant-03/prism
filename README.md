<div align="center">
  <img src="https://raw.githubusercontent.com/ant-design/ant-design/master/components/style/color/bezierEasing.less" width="100" height="0" />
</div>

<h1 align="center" style="border-bottom: none">⚡ Data Intelligence Platform (PRISM)</h1>
<h3 align="center">Your Autonomous, Zero-Setup AI Data Analyst</h3>

<p align="center">
  <b>Just drop in your data. It does the rest.</b><br>
  No coding. No struggling with Pandas. No massive cloud bills.
</p>

---

## 🚀 What is Prism?

**Prism** is a local-first, blazing-fast Data Intelligence Platform designed to automate the most tedious parts of data science: ingestion, cleaning, profiling, and analysis. It combines a robust Python backend with a stunning, highly-interactive React frontend to give you a truly autonomous AI Data Analyst living right on your machine.

**Upload a completely messed up dataset**, and Prism will automatically detect the format, guess the encoding, handle missing values, map exactly how the columns relate, generate specific hypotheses, create auto-visualizations, and let you query the data in **plain English**.

---

## 🔥 Unrivaled Features

Prism isn't just a dashboard. It's an entire data engineering pipeline wrapped in a seamless UI.

### 🧠 The AI Brain
* **Natural Language to SQL:** Ask your data questions in plain English (e.g., *"Show me the top 5 products by revenue last quarter"*). The AI translates this into hyper-optimized DuckDB SQL.
* **Autonomous Hypotheses:** The moment you upload data, Prism starts generating testable theories about causality and correlations without you lifting a finger.
* **AI-Powered Data Dashboard:** It instantly calculates exactly which graph tells the best story based solely on column types. No configuration required.

### 🌪️ The "Upload Anything" Engine
* **Universal File Support:** Throw `.csv`, `.xlsx`, `.json`, `.parquet`, `.xml`, or even raw `.sql` dumps at it. 
* **Seamless ZIP Extraction:** Upload a `.zip` archive full of CSVs—Prism automatically unzips it in the background and processes every file simultaneously.
* **Mid-Session Multi-File Append:** Missing a dataset? Just click the **Floating Add Button** inside the workspace to inject new tables directly into the SQL engine *without* losing your current progress.
* **Chunked Large File Ingestion:** Have a 5GB CSV? Prism handles it smoothly without crashing your browser or eating all your RAM. 

### 🧹 No-Code Data Cleaning & Profiling
* **Instant Deep Profiling:** See null distributions, unique counts, inferred semantic types, and out-of-bounds data points the second your file finishes uploading.
* **Auto-Correction Engine:** Prism suggests how to handle duplicates or massive null gaps. Let it drop missing rows, fill with medians, or flag anomalies intelligently.
* **Malformed Data Side-by-Side:** When data is profoundly broken, Prism shows you the exact rows causing issues and lets you configure salvage strategies visually.

### 🛠️ The SQL Workbench
* **Zero-Setup DuckDB:** An in-memory, insanely fast analytical database is spun up automatically for every session.
* **Visual Query Builder:** Don't know SQL? Use the drag-and-drop Visual Builder to join tables, filter data, and apply complex aggregations.
* **Instant Auto-Viz Engine:** Every time you run a query, the results are instantly graphed out.

### 📄 Instant Exporting
* **One-Click Reports:** Generate breathtaking Analyst Briefs detailing all findings directly to **PDF** or **DOCX**.
* **Export Anything:** Download your cleaned data to Parquet, JSON, or CSV instantly.

---

## ⚡ The Ultimate 1-Click Setup Guide

We’ve engineered Prism to be incredibly easy to start, even if you have absolutely **zero programming experience**. 

### Step 1: Install the Bare Essentials
If you don't already have these, install them first:
1. **[Python 3.10+](https://www.python.org/downloads/)**
   - *⚠️ CRITICAL:* During the Python installation, you **MUST** check the box that says **"Add Python to PATH"** at the very bottom of the installer window.
2. **[Node.js (18+)](https://nodejs.org/en/download/)**
   - Download the LTS version and install it with default settings.
3. **[Git](https://git-scm.com/downloads)**
   - So you can clone the repository.
4. *(Optional but Highly Recommended)* **[Visual Studio Code](https://code.visualstudio.com/)** to view the code.

### Step 2: Get Your Free AI Brain (API Key)
Prism uses Google's Gemini AI to power its incredible reasoning engine.
1. Go to [Google AI Studio](https://aistudio.google.com/app/apikey).
2. Sign in with any Google account.
3. Click **"Create API Key"** and copy the long string of letters and numbers it gives you.

### Step 3: Magic 1-Click Startup
You don't need to manually install dependencies, configure virtual environments, or worry about environment variables. **Our `start.bat` script handles *literally everything*.**

1. Open your terminal (Command Prompt or PowerShell) and run:
   ```bash
   git clone https://github.com/YourUsername/prism.git
   cd prism
   ```
2. Double click the **`start.bat`** file in the folder (or run `start.bat` in the terminal).

**What the script does autonomously:**
- ✅ Verifies you have Python and Node.js installed correctly.
- ✅ Creates an isolated Python virtual environment safely.
- ✅ Installs complex backend Data Science libraries (`pandas`, `duckdb`, `fastapi`, etc.).
- ✅ Installs frontend React dependencies seamlessly.
- ✅ Creates your `.env` configuration files completely automatically. 
- ✅ Boots the backend server and frontend client simultaneously.
- ✅ Automatically opens your browser to `http://localhost:5173`.

### Step 4: Paste Your Key
The **first time** the script runs, it will create a `backend/.env` file for you automatically. 
1. Open the `prism/backend/.env` file in Notepad or VS Code.
2. Find the line that says `GEMINI_API_KEY=""`.
3. Paste the key you got from Step 2 inside the quotes (e.g., `GEMINI_API_KEY="AIzaSyYourSecretKey..."`).
4. Save the file. (You never have to do this again).

**That's it. You're completely done.** Your autonomous AI analyst is alive. Drop in a dataset and watch the magic happen.

---

## 📸 Navigating the Interface

### The Workspace
- **Data Grid:** An Excel-style endless-scroll view into your datasets.
- **SQL Query Engine:** Let the AI write queries for you, or build them visually.
- **AI Dashboard:** Look for the dashboard tab—Prism will generate bar, line, and scatter plots comparing the most vital columns automatically.
- **Add Dataset Button:** See that little `+` button floating in the bottom right? At any point during your analysis, click it to upload a completely new table (or ZIP file!), and it will be silently injected into your SQL engine so you can instantly `JOIN` it with your existing data!

---

## 🛠️ Tech Stack Architecture
**Frontend:**
- React 18 / TypeScript
- Vite
- Ant Design (Custom Themed Glassmorphism UI)
- Recharts (Interactive Visualizations)
- React Flow (Relationship Graphs)

**Backend:**
- Python 3.10+
- FastAPI (High-performance async server)
- Pandas & DuckDB (Core analytics and SQL engine)
- Uvicorn (ASGI Server)
- Python-Magic (Magic-byte threat detection)
- Google GenAI SDK (LLM integration)

---

## 🛡️ License & Privacy
Prism is built to be a privacy-first local powerhouse. Your actual dataset rows **never** leave your machine. The only data sent to the AI API are schema structures (column names and types) for reasoning purposes. 

### Built with ❤️ for data people.
