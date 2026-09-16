@echo off
echo ========================================================
echo   Starting Real-Time Stock Sentiment Pipeline Workers
echo ========================================================
cd /d %~dp0
.\.venv\Scripts\python.exe run_pipeline.py
pause
