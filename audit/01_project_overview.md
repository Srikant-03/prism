# 01 — Project Overview

## What This Software Is

**PRISM** (Data Intelligence Platform) is a **local-first, AI-powered data analysis platform** that lets users upload datasets (CSV, Excel, JSON, Parquet, XML, SQL dumps, ZIP archives), automatically profile them, clean them, run natural-language queries against them, generate executive reports, and build interactive dashboards — all without writing code.

[VERIFIED] Evidence: [README.md](file:///c:/abbu/README.md), [main.py](file:///c:/abbu/backend/main.py)

## Who Uses It

Individual data analysts, engineers, and researchers who want to explore datasets quickly without setting up complex toolchains. Currently positioned as a **single-user desktop tool** (Windows primary, via `start.bat`).

[INFERRED] There is no multi-user auth, no user management, no tenant isolation.

## Core Problem It Solves

Eliminates the 80% of time spent on data cleaning, schema detection, and query writing. Provides Power BI-like dashboards and AI-generated insights from plain-English prompts, all running locally so data never leaves the machine.

## Intended Production Use Case

**Not yet production-ready.** Currently a **developer-stage prototype** intended as a personal productivity tool. The README positions it as an alternative to Power BI / Tableau / Pandas, but the architecture (in-memory state, single-worker, hardcoded secrets) is not designed for multi-user or hosted deployment.

## Scale Assumptions

| Dimension | Current Assumption | Evidence |
|---|---|---|
| Users | **1 concurrent user** | Single-process uvicorn, in-memory state ([main.py:4-6](file:///c:/abbu/backend/main.py#L4-L6)) |
| Data size | **<500 MB per file** | `MAX_FILE_SIZE` default ([config.py:21](file:///c:/abbu/backend/config.py#L21)) |
| Deployment | **Local Windows machine** | `start.bat`, no Dockerfile, no CI/CD |
| Persistence | **Session-scoped**, disk-cache with TTL | [state.py:28-39](file:///c:/abbu/backend/state.py#L28-L39) |
| AI dependency | **Google Gemini API** (free tier) | [config.py:113-122](file:///c:/abbu/backend/config.py#L113-L122) |

## Tech Stack Summary

| Layer | Technology | Version |
|---|---|---|
| Backend | Python + FastAPI | 3.10+ / ≥0.115 |
| Analytics | DuckDB (in-memory) | ≥1.1.3 |
| Data Processing | Pandas + PyArrow | ≥2.2.3 |
| AI / LLM | Google GenAI SDK (Gemini) | Floating |
| Frontend | React + TypeScript + Vite | 19 / 5.9 / 7.3 |
| UI Framework | Ant Design | 6.3 |
| Charts | Recharts + ECharts | 3.7 / 5.6 |
| State | diskcache (TTLStore) | Floating |
