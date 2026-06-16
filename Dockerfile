# =============================================
# citg_v2 — Dockerfile
# Supports two modes via APP_MODE env var:
#   web   → FastAPI dashboard (uvicorn)
#   *     → Telegram parser (parser.py)
# =============================================

FROM python:3.11-slim

LABEL maintainer="citg_v2"
LABEL description="Telegram Parser: parser (cron) + web (FastAPI dashboard)"

# Prevent Python from writing .pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies required for compiling Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies separately for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Cache bust to force fresh code copy on Render
ARG CACHE_BUST=61

# Copy application source code
COPY . .

# Create sessions directory with proper permissions
RUN mkdir -p /app/sessions && chmod +x entrypoint.sh

# Expose port (used in web mode)
EXPOSE 8000

# Healthcheck for web mode (Render will ignore for cron/worker)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD if [ "$APP_MODE" = "web" ]; then \
            curl -f http://localhost:${PORT:-10000}/api/stats || exit 1; \
        else \
            exit 0; \
        fi

# Entrypoint: choose mode based on APP_MODE env variable
CMD ["./entrypoint.sh"]
