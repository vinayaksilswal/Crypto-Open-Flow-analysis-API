@echo off
title Crypto Trading - Local Startup
color 0A

echo ============================================================
echo   CryptoFlow API - Local Dev Launcher
echo ============================================================
echo.

setlocal EnableExtensions

REM Step 0: Check for Docker
docker --version
if %errorlevel% neq 0 (
    echo [ERROR] Docker is not installed or not in PATH.
    pause
    exit /b 1
)

REM Step 1: Start Docker infra (PostgreSQL + Redis only) - DISABLED (Using Production DB/Redis)
echo [1/4] Skipping Docker infrastructure (Using Production DB)...
REM docker compose up -d timescaledb redis
if %errorlevel% neq 0 (
    echo [ERROR] Docker Compose failed. Is Docker Desktop running?
    pause
    exit /b 1
)

echo.
echo [2/4] Waiting for services...
REM Avoid `timeout` here because some non-interactive shells report "Input redirection is not supported".
REM If you need a delay, run this script in a normal cmd.exe window.

REM Step 2: Install deps if needed
echo [3/4] Ensuring Python dependencies are installed...
REM NOTE: Some environments (CI/remote runners) disallow stdin redirection and cause pip to abort.
REM If you need deps installed, run manually: python -m pip install -r requirements.txt
echo [INFO] Skipping auto pip install (run it manually if needed).
echo.

REM Step 3: Launch each service in its own terminal window
echo [4/4] Launching microservices (CONNECTED TO PRODUCTION)...
echo.
echo   - API Gateway       http://localhost:8000
echo   - Ingestion Worker
echo   - Analytics Worker
echo.

REM Environment:
REM - Python services will auto-load env.local (and .env) via python-dotenv.
REM - Set PORT/HOST here only if you want to override what is in env.local.
if "%PORT%"=="" set "PORT=8000"
if "%HOST%"=="" set "HOST=0.0.0.0"

start "API_Gateway" cmd /k "title API_Gateway && color 0B && python -m uvicorn api_gateway:app --host %HOST% --port %PORT% --reload"

start "Ingestion_Worker" cmd /k "title Ingestion_Worker && color 0E && python ingestion_service.py"

start "Analytics_Worker" cmd /k "title Analytics_Worker && color 0D && python analytics_worker.py"

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
