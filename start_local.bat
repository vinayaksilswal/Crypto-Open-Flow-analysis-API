@echo off
title Crypto Trading - Local Startup
color 0A

echo ============================================================
echo   CryptoFlow API - Local Dev Launcher
echo ============================================================
echo.

REM Step 1: Start Docker infra (PostgreSQL + Redis only)
echo [1/4] Starting Docker infrastructure (PostgreSQL + Redis)...
docker compose up -d timescaledb redis
if %errorlevel% neq 0 (
    echo [ERROR] Docker Compose failed. Is Docker Desktop running?
    pause
    exit /b 1
)

echo.
echo [2/4] Waiting 8 seconds for DB to be ready...
timeout /t 8 /nobreak >nul

REM Step 2: Install deps if needed
echo [3/4] Ensuring Python dependencies are installed...
pip install -r requirements.txt --quiet
echo.

REM Step 3: Launch each service in its own terminal window
echo [4/4] Launching microservices...

echo   - API Gateway       http://localhost:8000
start "API_Gateway" cmd /k "title API_Gateway && color 0B && python -m uvicorn api_gateway:app --host 0.0.0.0 --port 8000 --reload"

timeout /t 2 /nobreak >nul

echo   - Ingestion Worker
start "Ingestion_Worker" cmd /k "title Ingestion_Worker && color 0E && python ingestion_service.py"

timeout /t 2 /nobreak >nul

echo   - Analytics Worker
start "Analytics_Worker" cmd /k "title Analytics_Worker && color 0D && python analytics_worker.py"

echo.
echo ============================================================
echo   All services launched!
echo   API Gateway  : http://localhost:8000
echo   API Docs     : http://localhost:8000/docs
echo   Dashboard    : http://localhost:8000/
echo ============================================================
echo.
echo   Close the individual terminal windows to stop each service.
echo   To stop Docker infra: docker compose down
echo.
pause
