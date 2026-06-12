#!/bin/sh
# =============================================
# Entrypoint: выбор режима работы на Render
#   APP_MODE=web   → uvicorn (FastAPI dashboard)
#   APP_MODE=*     → python parser.py (Telegram parser)
# =============================================

set -e

echo "=== TG Parser v2 ==="
echo "APP_MODE: ${APP_MODE:-parser}"
echo "PORT: ${PORT:-10000}"
echo "CHANNELS: ${CHANNELS:-markettwits}"
echo "===================="

if [ "$APP_MODE" = "web" ]; then
    echo "Starting web dashboard..."
    exec uvicorn web:app --host 0.0.0.0 --port "${PORT:-10000}"
else
    echo "Starting parser..."
    exec python parser.py
fi
