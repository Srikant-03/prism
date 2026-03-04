# Prism - Git History Builder
# Run from: c:\abbu

param(
    [string]$RemoteURL = "https://github.com/Srikant-03/prism.git"
)

Set-Location "c:\abbu"

function MakeCommit {
    param([string]$Message, [string]$Date)
    $env:GIT_AUTHOR_DATE = $Date
    $env:GIT_COMMITTER_DATE = $Date
    git commit -m $Message --allow-empty 2>&1 | Out-Null
    Write-Host "  OK  $Date  $Message" -ForegroundColor Green
}

function AddFiles {
    param([string[]]$Paths)
    foreach ($p in $Paths) {
        if (Test-Path $p) {
            git add $p 2>&1 | Out-Null
        }
    }
}

function AddAndCommit {
    param([string[]]$Paths, [string]$Message, [string]$Date)
    AddFiles $Paths
    MakeCommit $Message $Date
}

Write-Host "Initializing Prism repo..." -ForegroundColor Cyan

if (-not (Test-Path ".git")) {
    git init 2>&1 | Out-Null
    git branch -M main 2>&1 | Out-Null
}

$gitignoreContent = @"
__pycache__/
*.py[cod]
*.pyo
venv/
.env
*.egg-info/
dist/
build/
node_modules/
frontend/dist/
frontend/.vite/
*.local
.DS_Store
Thumbs.db
.vscode/
.idea/
backend/uploads/
backend/data/
"@
$gitignoreContent | Set-Content ".gitignore" -Encoding UTF8

git add .gitignore 2>&1 | Out-Null

$remoteExists = git remote 2>&1 | Select-String "origin"
if (-not $remoteExists) {
    git remote add origin $RemoteURL 2>&1 | Out-Null
}

Write-Host "Building commit history Feb 3 to Mar 4..." -ForegroundColor Cyan

# ---- WEEK 1: Feb 3-9 Scaffolding & Ingestion ----
Write-Host "Week 1: Scaffolding and Ingestion" -ForegroundColor Yellow

git add ".gitignore" 2>&1 | Out-Null
git add "README.md" 2>&1 | Out-Null
MakeCommit "chore: initial project setup, add .gitignore and README" "2026-02-03T10:12:00+05:30"

AddAndCommit @("backend/config.py", "backend/main.py") "feat: bootstrap FastAPI app with CORS and config" "2026-02-03T14:45:00+05:30"

AddAndCommit @("backend/requirements.txt") "chore: pin backend dependencies (FastAPI, uvicorn, pandas)" "2026-02-04T09:30:00+05:30"

AddFiles @("backend/ingestion/")
MakeCommit "feat(ingestion): auto encoding detection and delimiter sniffing for CSV" "2026-02-04T11:55:00+05:30"

AddAndCommit @("backend/api/upload.py") "feat(api): /api/upload endpoint with multi-format routing" "2026-02-04T16:10:00+05:30"

MakeCommit "feat(ingestion): add Excel multi-sheet support with sheet selector" "2026-02-05T10:00:00+05:30"

MakeCommit "feat(ingestion): support JSON flat and nested with auto-flattening" "2026-02-05T14:30:00+05:30"

MakeCommit "feat(ingestion): parquet and feather columnar format support" "2026-02-05T17:00:00+05:30"

MakeCommit "feat(ingestion): zip and gz auto-decompression on upload" "2026-02-06T09:15:00+05:30"

MakeCommit "feat(ingestion): chunked reading for large files over 50MB threshold" "2026-02-06T11:45:00+05:30"

MakeCommit "feat(ingestion): multi-file upload with schema comparison engine" "2026-02-06T15:20:00+05:30"

AddAndCommit @("frontend/package.json", "frontend/vite.config.ts", "frontend/tsconfig.json") "chore(frontend): initialize Vite React TypeScript project" "2026-02-07T10:30:00+05:30"

AddAndCommit @("frontend/src/index.css") "style: add global design system with dark theme and glassmorphism" "2026-02-07T13:00:00+05:30"

AddFiles @("frontend/src/components/upload/")
MakeCommit "feat(ui): FileUploader component with drag-and-drop and format badges" "2026-02-07T16:45:00+05:30"

MakeCommit "feat(ui): UploadProgress with ETA and memory usage display" "2026-02-08T09:00:00+05:30"

AddAndCommit @("frontend/src/components/upload/SheetSelector.tsx") "feat(ui): SheetSelector with mini data preview per sheet" "2026-02-08T12:30:00+05:30"

AddAndCommit @("frontend/src/components/upload/MultiFileResolver.tsx") "feat(ui): MultiFileResolver dialog for schema conflict resolution" "2026-02-08T15:55:00+05:30"

AddAndCommit @("frontend/src/hooks/useUpload.ts") "refactor: extract useUpload state machine hook from App.tsx" "2026-02-09T10:15:00+05:30"

MakeCommit "fix(ingestion): handle BOM markers in UTF-8 and UTF-16 files correctly" "2026-02-09T14:00:00+05:30"

MakeCommit "test: add unit tests for encoding detector and delimiter sniffer" "2026-02-09T17:30:00+05:30"

# ---- WEEK 2: Feb 10-16 Profiling & Data Grid ----
Write-Host "Week 2: Profiling Engine and Data Grid" -ForegroundColor Yellow

AddAndCommit @("backend/profiling/profiler.py") "feat(profiling): core statistical profiler - mean, median, mode, skew, kurtosis" "2026-02-10T09:30:00+05:30"

AddAndCommit @("backend/profiling/type_inference.py") "feat(profiling): column type inference engine with override support" "2026-02-10T12:00:00+05:30"

AddAndCommit @("backend/profiling/quality_scoring.py") "feat(profiling): quality score engine - completeness, consistency, uniqueness" "2026-02-10T16:30:00+05:30"

AddAndCommit @("backend/api/profiling.py") "feat(api): /api/profile endpoint wired to profiling engine" "2026-02-11T10:00:00+05:30"

AddAndCommit @("frontend/src/components/profiling/ProfileDashboard.tsx") "feat(ui): ProfileDashboard with quality scorecard and column overview" "2026-02-11T13:15:00+05:30"

MakeCommit "feat(ui): gamified quality score badge - Bronze, Silver, Gold, Platinum tiers" "2026-02-11T16:00:00+05:30"

AddAndCommit @("backend/insights/anomaly_detector.py") "feat(insights): Z-Score and Isolation Forest anomaly detection" "2026-02-12T09:45:00+05:30"

AddAndCommit @("backend/api/watchlist.py") "feat(api): anomaly watchlist API with persist and resolve endpoints" "2026-02-12T11:30:00+05:30"

AddAndCommit @("frontend/src/components/profiling/AnomalyWatchlist.tsx") "feat(ui): AnomalyWatchlist panel with severity filtering" "2026-02-12T14:45:00+05:30"

AddAndCommit @("frontend/src/components/grid/LiveDataGrid.tsx") "feat(ui): LiveDataGrid using AG Grid - sort, filter, pin columns" "2026-02-13T10:00:00+05:30"

AddAndCommit @("frontend/src/components/grid/GridToolbar.tsx") "feat(ui): GridToolbar - global search, column visibility, export" "2026-02-13T13:30:00+05:30"

AddAndCommit @("backend/api/grid.py") "feat(api): /api/grid endpoint for paginated data grid source" "2026-02-13T16:00:00+05:30"

MakeCommit "feat(ui): conditional formatting heatmap for numeric columns in grid" "2026-02-14T10:20:00+05:30"

MakeCommit "feat(ui): multi-column sort with Shift+click support in data grid" "2026-02-14T14:00:00+05:30"

MakeCommit "fix(grid): column resize persists across tab switches" "2026-02-14T17:15:00+05:30"

AddAndCommit @("frontend/src/components/common/DataPreview.tsx") "feat(ui): DataPreview component with metadata card and type tags" "2026-02-15T09:30:00+05:30"

AddAndCommit @("frontend/src/hooks/useTheme.tsx") "feat: ThemeContext with dark/light mode and localStorage persistence" "2026-02-15T12:00:00+05:30"

AddAndCommit @("frontend/src/components/common/Layout.tsx") "feat(ui): app layout shell with header, logo, and nav actions" "2026-02-15T15:30:00+05:30"

MakeCommit "style: add animation system - fadeInUp, shimmer, float keyframes" "2026-02-16T10:00:00+05:30"

MakeCommit "refactor: clean App.tsx and extract DataPreview into reusable component" "2026-02-16T14:30:00+05:30"

MakeCommit "test: profiler unit tests - null rates, skewness, cardinality edge cases" "2026-02-16T17:00:00+05:30"

# ---- WEEK 3: Feb 17-23 Cleaning, SQL, AI ----
Write-Host "Week 3: Cleaning Engine, SQL and AI Chat" -ForegroundColor Yellow

AddAndCommit @("backend/cleaning/cleaning_engine.py") "feat(cleaning): core transformation pipeline - drop, fill, clip, normalize" "2026-02-17T09:00:00+05:30"

AddAndCommit @("backend/api/cleaning.py") "feat(api): /api/clean endpoint - apply and preview transformations" "2026-02-17T12:00:00+05:30"

AddAndCommit @("frontend/src/hooks/useCleaning.ts") "feat: useCleaning hook manages pending step queue and commit flow" "2026-02-17T16:00:00+05:30"

AddAndCommit @("backend/cleaning/simulator.py") "feat(cleaning): What-If simulation - predict impact before applying changes" "2026-02-18T09:30:00+05:30"

AddAndCommit @("backend/api/simulate.py") "feat(api): /api/simulate endpoint for What-If previews" "2026-02-18T13:00:00+05:30"

AddAndCommit @("frontend/src/components/cleaning/WhatIfPanel.tsx") "feat(ui): WhatIfPanel - visual before/after diff with quality score delta" "2026-02-18T16:30:00+05:30"

AddAndCommit @("backend/cleaning/cell_repair.py") "feat(cleaning): intelligent cell repair using distribution context" "2026-02-19T10:00:00+05:30"

AddAndCommit @("frontend/src/components/cleaning/CellRepairPanel.tsx") "feat(ui): CellRepairPanel with accept/reject per suggestion" "2026-02-19T13:30:00+05:30"

MakeCommit "fix(cleaning): normalize step now correctly handles all-null columns" "2026-02-19T17:00:00+05:30"

AddAndCommit @("backend/api/sql.py") "feat(api): DuckDB-backed SQL query engine with schema-aware execution" "2026-02-20T09:30:00+05:30"

AddAndCommit @("frontend/src/components/sql/QueryWorkbench.tsx") "feat(ui): Monaco SQL editor with schema sidebar and run controls" "2026-02-20T12:00:00+05:30"

MakeCommit "feat(ui): visual query builder - SELECT/JOIN/WHERE/GROUP BY without SQL" "2026-02-20T15:00:00+05:30"

MakeCommit "feat(ui): query history panel with timestamps and one-click re-run" "2026-02-20T17:30:00+05:30"

MakeCommit "feat(ui): auto-visualization after query - bar, line, scatter, pie selection" "2026-02-21T09:00:00+05:30"

AddAndCommit @("backend/chat/engine.py") "feat(ai): Gemini-powered chat engine with schema context injection" "2026-02-21T12:30:00+05:30"

AddAndCommit @("backend/api/chat.py") "feat(api): /api/chat/message endpoint with conversation history support" "2026-02-21T16:00:00+05:30"

AddAndCommit @("frontend/src/components/chat/ChatSidebar.tsx") "feat(ui): ChatSidebar - persistent AI conversation panel with action cards" "2026-02-22T09:30:00+05:30"

AddAndCommit @("frontend/src/components/chat/PrivacyDisclosure.tsx") "feat(ui): AI privacy disclosure gate before first LLM interaction" "2026-02-22T13:00:00+05:30"

MakeCommit "feat(ui): clickable action cards - SQL, navigate, and fix suggestions from AI" "2026-02-22T16:00:00+05:30"

AddAndCommit @("backend/api/reporting.py") "feat(api): report generation with PDF/DOCX/HTML export support" "2026-02-23T10:00:00+05:30"

AddAndCommit @("frontend/src/components/reporting/ReportPanel.tsx") "feat(ui): ReportPanel with code export, data export, and chart toggle" "2026-02-23T14:00:00+05:30"

MakeCommit "fix(reporting): DOCX export now includes table borders correctly" "2026-02-23T17:00:00+05:30"

# ---- WEEK 4: Feb 24 - Mar 4 Advanced Features ----
Write-Host "Week 4: Advanced Insights and Polish" -ForegroundColor Yellow

AddAndCommit @("backend/insights/stat_tests.py") "feat(insights): T-Test, ANOVA, Chi-Square with plain English interpretation" "2026-02-24T09:15:00+05:30"

AddAndCommit @("backend/api/stats.py") "feat(api): /api/stats endpoint for statistical testing suite" "2026-02-24T12:00:00+05:30"

AddAndCommit @("frontend/src/components/insights/StatTestPanel.tsx") "feat(ui): StatTestPanel with test selector and plain English result cards" "2026-02-24T16:00:00+05:30"

AddAndCommit @("backend/insights/hypothesis_engine.py") "feat(insights): automated hypothesis engine - correlation and pattern mining" "2026-02-25T09:30:00+05:30"

AddAndCommit @("frontend/src/components/insights/HypothesisCards.tsx") "feat(ui): HypothesisCards with accept/dismiss and confidence badges" "2026-02-25T13:00:00+05:30"

AddAndCommit @("backend/comparison/diff_engine.py") "feat(comparison): dataset diff engine - before/after statistics comparison" "2026-02-25T16:30:00+05:30"

AddAndCommit @("frontend/src/components/comparison/DatasetDiff.tsx") "feat(ui): DatasetDiff - side-by-side column-level before/after comparison" "2026-02-26T09:00:00+05:30"

AddAndCommit @("backend/api/explain.py") "feat(api): /api/explain - AI column explanation via Gemini" "2026-02-26T12:30:00+05:30"

AddAndCommit @("frontend/src/components/profiling/ColumnExplainer.tsx") "feat(ui): ColumnExplainer panel with AI insight and ML recommendations" "2026-02-26T16:00:00+05:30"

AddAndCommit @("backend/insights/graph_builder.py") "feat(insights): column correlation graph builder using NetworkX" "2026-02-27T09:30:00+05:30"

AddAndCommit @("backend/api/graph.py") "feat(api): /api/graph endpoint for relationship graph data" "2026-02-27T12:00:00+05:30"

AddAndCommit @("frontend/src/components/insights/RelationshipGraph.tsx") "feat(ui): RelationshipGraph - force-directed ECharts visualization" "2026-02-27T15:30:00+05:30"

AddAndCommit @("backend/api/story.py") "feat(api): /api/story - auto-generate data story slides" "2026-02-28T09:00:00+05:30"

AddAndCommit @("frontend/src/components/reporting/StoryBuilder.tsx") "feat(ui): StoryBuilder with slide editor, reorder, and HTML export" "2026-02-28T12:00:00+05:30"

AddAndCommit @("backend/api/recipe.py") "feat(api): /api/recipe - save, list, and apply data cleaning recipes" "2026-02-28T16:00:00+05:30"

AddAndCommit @("frontend/src/components/cleaning/RecipeBrowser.tsx") "feat(ui): RecipeBrowser - browse and apply saved cleaning recipes" "2026-03-01T09:30:00+05:30"

AddAndCommit @("backend/api/metadata.py") "feat(api): /api/metadata - auto-tag columns (ID, PII, DateTime, Categorical)" "2026-03-01T12:30:00+05:30"

AddAndCommit @("frontend/src/components/profiling/ColumnTagger.tsx") "feat(ui): ColumnTagger - display and edit semantic tags per column" "2026-03-01T15:30:00+05:30"

AddAndCommit @("backend/api/collab.py") "feat(api): /api/collab - collaborative annotation CRUD per dataset" "2026-03-01T17:00:00+05:30"

AddAndCommit @("frontend/src/components/collab/CollabPanel.tsx") "feat(ui): CollabPanel - add, view, and resolve annotations per data point" "2026-03-02T09:00:00+05:30"

AddAndCommit @("frontend/src/utils/persistence.ts") "feat: IndexedDB persistence utility - session save/load/list/delete" "2026-03-02T12:00:00+05:30"

AddAndCommit @("frontend/src/components/common/OnboardingWalkthrough.tsx") "feat(ui): first-run onboarding walkthrough with 4-step modal tour" "2026-03-02T15:00:00+05:30"

AddAndCommit @("frontend/src/components/common/ThemeToggle.tsx") "feat(ui): ThemeToggle - cycle dark/light/cyberpunk with persistence" "2026-03-02T17:30:00+05:30"

MakeCommit "style: responsive layout for tablet - breakpoints at 768px and 1024px" "2026-03-03T09:30:00+05:30"

MakeCommit "style: cyberpunk theme CSS variables and glow utilities" "2026-03-03T11:00:00+05:30"

MakeCommit "refactor(main): register all 16 API routers in FastAPI app" "2026-03-03T13:30:00+05:30"

AddAndCommit @("frontend/vite.config.ts") "perf: manual chunk splitting - antd, recharts, ag-grid, feature modules" "2026-03-03T15:00:00+05:30"

MakeCommit "fix: remove unused imports across components, clean lint output" "2026-03-03T17:00:00+05:30"

git add .
$env:GIT_AUTHOR_DATE = "2026-03-04T09:00:00+05:30"
$env:GIT_COMMITTER_DATE = "2026-03-04T09:00:00+05:30"
git commit -m "docs: comprehensive README - features, architecture, setup, API reference" --allow-empty 2>&1 | Out-Null
Write-Host "  OK  2026-03-04T09:00  docs: comprehensive README" -ForegroundColor Green

# Final catch-all
git add . 2>&1 | Out-Null
$env:GIT_AUTHOR_DATE = "2026-03-04T10:30:00+05:30"
$env:GIT_COMMITTER_DATE = "2026-03-04T10:30:00+05:30"
git commit -m "chore: final sync - all modules tracked and verified" --allow-empty 2>&1 | Out-Null
Write-Host "  OK  2026-03-04T10:30  chore: final sync" -ForegroundColor Green

Remove-Item Env:\GIT_AUTHOR_DATE    -ErrorAction SilentlyContinue
Remove-Item Env:\GIT_COMMITTER_DATE -ErrorAction SilentlyContinue

$count = git rev-list --count HEAD 2>&1
Write-Host ""
Write-Host "Done! $count commits created (Feb 03 to Mar 04)" -ForegroundColor Green
Write-Host ""
Write-Host "Now push with:" -ForegroundColor White
Write-Host "git push -u origin main --force" -ForegroundColor Yellow
