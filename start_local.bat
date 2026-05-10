@echo off
title Crypto Trading - Local Startup
color 0A

echo ============================================================
echo   CryptoFlow API - Local Dev Launcher
echo ============================================================
echo.

setlocal EnableExtensions

echo [1/3] Waiting for services...

echo [2/3] Ensuring Python dependencies are installed...
echo [INFO] Skipping auto pip install (run it manually if needed).
echo.

echo [3/3] Launching microservices (CONNECTED TO PRODUCTION)...
echo.
echo   - API Gateway       http://localhost:8000
echo   - Ingestion Worker
echo   - Analytics Worker
echo.

if "%PORT%"=="" set "PORT=8000"
if "%HOST%"=="" set "HOST=0.0.0.0"

start "API_Gateway" cmd /k "title API_Gateway && color 0B && python -m uvicorn api_gateway:app --host %HOST% --port %PORT%"

start "Ingestion_Worker" cmd /k "title Ingestion_Worker && color 0E && python ingestion_service.py"

start "Analytics_Worker" cmd /k "title Analytics_Worker && color 0D && python analytics_worker.py"

start "Caddy" cmd /k "title Caddy Reverse Proxy && color 0F && caddy.exe run"

echo ============================================================
echo   All services launched!
echo   API Gateway  : http://localhost:8000
echo   API Docs     : http://localhost:8000/docs
echo   Dashboard    : http://localhost:8000/
echo   Caddy Proxy  : https://crypto-order-flow.quantcai.in
echo ============================================================
echo.
echo   Close the individual terminal windows to stop each service.
echo.
pause
