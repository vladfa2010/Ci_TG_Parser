FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev libc-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

# Default: run parser. Override with APP_MODE=web for dashboard
CMD ["sh", "-c", "if [ \"$APP_MODE\" = 'web' ]; then uvicorn web:app --host 0.0.0.0 --port ${PORT:-10000}; else python markettwits_parser.py; fi"]
