@echo off
title Prism - Stop
echo Stopping Prism servers...
taskkill /FI "WindowTitle eq Prism Backend*" /F >nul 2>&1
taskkill /FI "WindowTitle eq Prism Frontend*" /F >nul 2>&1
echo Done. All Prism processes stopped.
timeout /t 2 /nobreak >nul
