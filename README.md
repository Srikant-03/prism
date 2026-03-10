<div align="center">

<img src="https://img.shields.io/badge/Status-Active-success?style=for-the-badge" alt="Status" />
<img src="https://img.shields.io/badge/AI_Powered-Data_Intelligence-8A2BE2?style=for-the-badge" alt="AI Powered" />

# 🔷 Prism: The Future of Data Intelligence

**One file in. A full spectrum of AI-driven intelligence out.** 
Turn raw, messy data into interactive dashboards, deep statistical insights, and executive reports—instantly, effortlessly, and entirely on your local machine.

[![FastAPI](https://img.shields.io/badge/Backend-FastAPI-009688?style=flat-square&logo=fastapi)](https://fastapi.tiangolo.com/)
[![React](https://img.shields.io/badge/Frontend-React%20%2B%20Vite-61DAFB?style=flat-square&logo=react)](https://react.dev/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python)](https://python.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178C6?style=flat-square&logo=typescript)](https://typescriptlang.org)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

</div>

---

## ⚡ Why You Need Prism Right Now
Are you tired of spending hours writing Pandas scripts just to figure out what's inside a CSV? Have you ever struggled to clean a messy dataset or build a chart for an executive presentation?

**Prism is a complete, cutting-edge AI data analyst that lives on your laptop.**

Upload literally *any* data file—CSV, Excel, JSON, Parquet, or SQL dumps—and Prism instantly takes over. It automatically detects patterns, flags anomalies, scores your data quality, generates SQL queries from plain English, builds force-directed correlation graphs, and even proposes high-level strategic hypotheses that human analysts might miss.

No coding required. No cloud subscription needed. Your data never leaves your laptop.

---

## 🔥 Phenomenal Features

Prism is packed with every tool a data scientist, business analyst, or curious developer could ever want:

### 🧠 The AI Brain (Powered by Gemini)
- **Natural Language SQL:** Type "Show me sales by region" and watch Prism write the complex DuckDB SQL query and instantly graph the result!
- **Deep Hypothesis Engine:** Prism scans your dataset for hidden non-linear relationships, multi-collinear clusters, and data leakage risks, providing you with strategic, testable hypotheses.
- **Explain This Column:** Don't understand what `cust_id_rev_v2` means? Prism's AI will deduce the business meaning and use-cases for any column instantly.
- **AI Data Storyteller:** Generates a 10-slide executive presentation summarizing the most critical insights in your data, ready for export.
- **Intelligent Cell Repair:** Got corrupt data? Prism uses AI to suggest the most statistically probable replacement values.

### 📊 Autonomous Data Profiling
- **Zero-Config Ingestion:** Drop in an Excel file with multiple sheets or a massive nested JSON. Prism auto-detects encodings, delimiters, and schemas.
- **Gamified Data Quality Score:** Get a score from 0-100 indicating how clean your data is, backed by an Anomaly Detector that spots outliers using Z-scores and Isolation Forests.
- **Interactive Histograms & Stats:** Every single column gets a beautiful, interactive breakdown of its distribution, skewness, missing values, and cardinality.
- **ML Readiness Report Card:** Instantly know if your dataset is ready to train a machine learning model, tracking class imbalances and target prediction confidence.

### 🧹 No-Code Data Cleaning & Engineering
- **Visual Pipeline:** Drop rows, fill nulls, scale data, and standardize formats with a few clicks.
- **What-If Simulator:** Before you hit "apply," Prism shows you the exact volumetric and statistical impact of your cleaning operation. 
- **Recipe Library:** Save your cleaning steps and apply them instantly to the next dataset that comes in.

### 💻 The Workings
- **Monaco SQL Editor:** A full VS Code-style SQL editor built right into your browser, powered by an ultra-fast DuckDB in-memory engine.
- **AG Grid Integration:** Sort, filter, group, and search thousands of rows in milliseconds using our enterprise-grade data grid.
- **Multi-Dataset Joins:** Merge multiple files visually with AI-suggested join keys and foreign key detection.

### 📤 Instant Exporting
- Export your cleaned data to Parquet, Feather, JSON, or SQL inserts.
- Export beautiful PDF or DOCX analysis reports.
- Export the exact Python code or Jupyter Notebook required to reproduce your entire cleaning pipeline!

---

## 🚀 The Ultimate Beginner's Setup Guide

Want to run Prism on your own computer? It's incredibly easy! Even if you have never coded before in your life, just follow these simple steps.

### Step 1: Install the Required Tools
Prism needs three basic programs to run. If you don't have them, download and install them now:

1. **Python (The Engine):** 
   - Go to [Python's official website](https://www.python.org/downloads/).
   - Click the big "Download Python 3.x" button.
   - **CRITICAL:** When running the installer, make sure you check the box that says **"Add python.exe to PATH"** before clicking Install!
2. **Node.js (The User Interface):** 
   - Go to [Node.js](https://nodejs.org/).
   - Download the **"LTS" (Long Term Support)** version.
   - Run the installer and click "Next" through all the default options.
3. **VS Code (The Editor) & Git:**
   - Go to [Visual Studio Code](https://code.visualstudio.com/) and install it. This is where you'll view the code.
   - Go to [Git](https://git-scm.com/downloads) and install it. This lets you download Prism to your computer.

*To check if they installed correctly, open a terminal (Search for "Command Prompt" on Windows or "Terminal" on Mac) and type `python --version` and `node --version`.*

### Step 2: Download Prism
1. Open your terminal (Command Prompt or Mac Terminal).
2. Type this exact command and press Enter:
   ```bash
   git clone https://github.com/Srikant-03/prism.git
   ```
3. Once it finishes downloading, move into the new folded by typing:
   ```bash
   cd prism
   ```

### Step 3: Get Your Free AI Key
Prism uses Google's incredible Gemini AI to power its smart features. 
1. Go to [Google AI Studio](https://aistudio.google.com/app/apikey).
2. Sign in with your Google account.
3. Click **"Create API Key"** and copy the long string of text (it starts with `AIza...`). Keep this secret!

### Step 4: Add Your AI Key to Prism
1. Open the `prism` folder in **VS Code**. (You can drag and drop the folder into the VS Code window).
2. On the left side, open the `backend` folder.
3. Find the file named `.env.example`.
4. Right-click that file and select **Rename**. Change its name to exactly: `.env` (Just dot env, nothing before the dot).
5. Open your new `.env` file and find the line that says `GEMINI_API_KEY=`.
6. Paste your key right after the equals sign so it looks like this: 
   ```text
   GEMINI_API_KEY=AIzaSy...your_secret_key_here
   ```
7. Save the file (Ctrl+S or Cmd+S).

### Step 5: Start Prism!

**If you are on Windows:**
Simply go to the `prism` folder on your computer and double-click the file named **`start.bat`**.
- *That's it!* A black window will pop up, install everything automatically, and seamlessly open Prism in your web browser!

**If you are on Mac or Linux:**
You just need to run a few quick commands in your terminal.
1. Open a terminal and start the backend:
   ```bash
   cd backend
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   python main.py
   ```
2. Open a **second, new terminal window** and start the frontend:
   ```bash
   cd frontend
   npm install
   npm run dev
   ```
3. Open your web browser and go to: **http://localhost:5173**

---

## 🛑 How to Stop Prism
When you are done analyzing your data:
- **Windows:** Double-click the `stop.bat` file in the main folder to cleanly shut everything down.
- **Mac/Linux:** Go to your terminal windows and press `Ctrl + C` in both of them.

---

## 🛠️ Troubleshooting

- **The frontend shows a blank page?** Check your backend terminal window. Make sure it says the server is running on `http://localhost:8000`.
- **The AI Chat isn't answering?** You likely forgot to save your `.env` file or you pasted the `GEMINI_API_KEY` incorrectly.
- **"python is not recognized as an internal or external command"** You forgot to check the "Add to PATH" box when installing Python! Re-run the Python installer and choose 'Modify' to fix it.
- **"npm is not recognized"** Restart your computer after installing Node.js so that your terminal recognizes the new commands.

---

## 📜 License
This massively powerful tool is licensed under **MIT**. Built with passion by Srikant.

<div align="center">
  <i>Stop guessing. Start knowing. Drop your data into Prism today.</i>
</div>
