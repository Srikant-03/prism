@echo off
title Prism Launcher
color 0B
echo.
echo  ==============================
echo    PRISM - Starting up...
echo  ==============================
echo.

REM ── Check Python ──
python --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python not found. Please install Python 3.10+
    pause
    exit /b 1
)

REM ── Check Node ──
node --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Node.js not found. Please install Node.js 18+
    pause
    exit /b 1
)

REM ── Install backend deps if venv missing ──
if not exist "backend\venv" (
    echo  [SETUP] Creating Python virtual environment...
    python -m venv backend\venv
)
echo  [SETUP] Checking backend dependencies...
backend\venv\Scripts\python -m pip install -r backend\requirements.txt --quiet

REM ── Install frontend deps if node_modules missing ──
if not exist "frontend\node_modules" (
    echo  [SETUP] Installing frontend dependencies...
    cd frontend
    npm install --silent
    cd ..
)

REM ── Start Backend in new window ──
echo  [START] Launching backend on http://localhost:8000
start "Prism Backend" cmd /k "cd /d %~dp0backend && venv\Scripts\python.exe main.py"

REM ── Wait 2 seconds for backend to boot ──
timeout /t 2 /nobreak >nul

REM ── Start Frontend in new window ──
echo  [START] Launching frontend on http://localhost:5173
start "Prism Frontend" cmd /k "cd /d %~dp0frontend && npm run dev"

REM ── Wait 3 seconds then open browser ──
timeout /t 3 /nobreak >nul
echo  [OPEN]  Opening browser...
start http://localhost:5173

echo.
echo  Both servers are running.
echo  Backend:  http://localhost:8000
echo  Frontend: http://localhost:5173
echo  API Docs: http://localhost:8000/docs
echo.
echo  Close the Backend and Frontend windows to stop Prism.
echo.
pause
