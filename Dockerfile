FROM python:3.11-slim

# ----------------------------
# Python runtime settings
# ----------------------------
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

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
# Expose port (informational only)
# ----------------------------
EXPOSE 8000

# ----------------------------
# Start server
# - Render injects $PORT
# - Local fallback = 8000
# ----------------------------
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"]
