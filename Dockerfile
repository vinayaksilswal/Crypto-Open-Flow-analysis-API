FROM python:3.12-slim

# Security: non-root user
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY api_gateway.py .
COPY ingestion_service.py .
COPY analytics_worker.py .
COPY static/ static/

# Own files as non-root user
RUN chown -R appuser:appuser /app
USER appuser

# Expose port (Render sets PORT env var)
EXPOSE 8000

# Default: run all services using the start script
COPY start.sh .
# Switch to root temporarily to fix permissions, then back to appuser
USER root
RUN chmod +x start.sh
RUN chown appuser:appuser start.sh
USER appuser

# Run the single container start script
CMD ["./start.sh"]
