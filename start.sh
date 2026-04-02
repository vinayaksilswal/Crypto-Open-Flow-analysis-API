#!/bin/bash

# Start the ingestion service in the background
echo "Starting Harvester (Ingestion Service)..."
python ingestion_service.py &

# Start the analytics worker in the background
echo "Starting Cruncher (Analytics Worker)..."
python analytics_worker.py &

# Start the API Gateway (FastAPI) in the foreground
# It binds to $PORT dynamically assigned by Render, defaulting to 8000
echo "Starting API Gateway..."
PORT=${PORT:-8000}
exec gunicorn api_gateway:app \
     --worker-class uvicorn.workers.UvicornWorker \
     --workers 2 \
     --bind 0.0.0.0:$PORT \
     --timeout 120 \
     --access-logfile - \
     --error-logfile -
