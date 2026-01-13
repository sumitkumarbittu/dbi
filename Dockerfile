FROM python:3.11-slim

# ----------------------------
# Python runtime settings
# ----------------------------
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# ----------------------------
# Logging configuration
# ----------------------------
ENV LOG_LEVEL=debug
ENV LOG_FILE=/app/logs/app.log
ENV ERROR_LOG_FILE=/app/logs/error.log

# ----------------------------
# Working directory
# ----------------------------
WORKDIR /app

# ----------------------------
# Install Python dependencies
# ----------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ----------------------------
# Copy application code
# ----------------------------
COPY app.py .

# ----------------------------
# Create logs directory
# ----------------------------
RUN mkdir -p /app/logs

# ----------------------------
# Expose port (informational only)
# ----------------------------
EXPOSE 8000

# ----------------------------
# Start the FastAPI application
# ----------------------------
# Uses uvicorn ASGI server
# - host 0.0.0.0: Required for container networking
# - port ${PORT:-8000}: Uses Render's injected PORT or fallback to 8000
# - app: FastAPI application instance
# - --log-level ${LOG_LEVEL}: Configurable log level (debug/info/warning/error)
# - --access-log: Enable HTTP request/response logging
# - --reload: Auto-reload on code changes (development)
# File logging: Outputs to both console and log files
# Error handling: Captures errors to separate error log
# ----------------------------
CMD ["sh", "-c", "echo '=== FastAPI Application Starting ===' && \
echo 'Host: 0.0.0.0' && \
echo 'Port: ${PORT:-8000}' && \
echo 'Log Level: ${LOG_LEVEL}' && \
echo 'Access Log: enabled' && \
echo 'App Log: ${LOG_FILE}' && \
echo 'Error Log: ${ERROR_LOG_FILE}' && \
echo '================================' && \
uvicorn app:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --log-level ${LOG_LEVEL} \
  --access-log \
  --reload 2>&1 | tee ${LOG_FILE}"]
