#!/usr/bin/env python3
"""
CITG Dashboard v2 — Multi-Channel FastAPI Dashboard.
Channel-aware SPA with Charts. ECharts, dark theme, error recovery.

Imports models from models.py (SQLAlchemy 2.0 async) and config from config.py.
"""
from __future__ import annotations

import logging
import traceback
import csv
import io
import json
import re
import hashlib
import secrets
import hmac
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import FastAPI, Query, Depends, Request, Form, status, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from models import Base, Channel, Post, ParseLog, ChannelError, User
from config import settings, settings as cfg

# Lazy import parser to avoid circular deps and heavy init
_parser_module = None

def _get_parser():
    global _parser_module
    if _parser_module is None:
        from parser import MultiChannelParser
        _parser_module = MultiChannelParser()
    return _parser_module

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─── Sentiment Lexicon (Russian) ─────────────────────────────
SENTIMENT_POSITIVE = frozenset({
    "рост", "прибыль", "прибыльный", "прибыльная", "прирост", "повышение", "подъем",
    "подъём", "рали", "ралли", "бык", "бычий", "лонг", "покупка", "покупаем",
    "покупать", "вход", "вошел", "вошёл", "цель", "тейк", "тейк-профит", " профит",
    "доход", "доходность", "доходный", "окупаемость", "плюс", "позитив", "позитивный",
    "оптимизм", "оптимистичный", "сильный", "укрепление", "восстановление", "отскок",
    "прорыв", "breakout", "рванул", "взлетел", "взлет", "растет", "растёт", "расти",
    "зеленый", "зелёный", "зелень", "buy", "long", "bull", "bullish", "profit",
    "growth", "gain", "up", "rise", "rising", "rocket", "moon", " ATH", " ath",
    " rekord", "рекорд", "максимум", "high", "higher", "strong", " outperform",
    "перспектива", "потенциал", "увеличение", "расширение", "дивиденд", "купон",
    "ивестиция", "вклад", "пассивный доход", "капитализация", "рост капитализации",
})

SENTIMENT_NEGATIVE = frozenset({
    "падение", "убыток", "убыточный", "убыточная", "понижение", "снижение", "спад",
    "медведь", "медвежий", "шорт", "продажа", "продаем", "продаж", "продать",
    "выход", "вышел", "стоп", "стоп-лосс", "лосс", "потеря", "потери", "минус",
    "негатив", "негативный", "пессимизм", "пессимистичный", "слабый", "ослабление",
    "обвал", "кризис", "крах", "крах", "пузырь", "коррекция", "просадка", "просел",
    "просел", "обвалился", "рухнул", "падает", "падать", "красный", "красные",
    "sell", "short", "bear", "bearish", "loss", "losses", "down", "drop", "fall",
    "falling", "crash", "dump", "crisis", "correction", "weak", "underperform",
    "банкротство", "дефолт", "санкции", "штраф", "иск", "претензия", "спор",
    "конфликт", "задержка", "отсрочка", "срыв", "невыполнение", "риск", "опасность",
    "угроза", "нестабильность", "волатильность", "биржевой стресс", "ликвидация",
    "маржин-колл", "форс-мажор", "паника", "истерия", "флуд", "fud",
})

# ─── Sector Mapping ──────────────────────────────────────────
TICKER_TO_SECTOR = {
    # Oil & Gas
    "LKOH": "Oil & Gas", "SIBN": "Oil & Gas", "NVTK": "Oil & Gas",
    "TATN": "Oil & Gas", "BANE": "Oil & Gas", "ROSN": "Oil & Gas",
    "GAZP": "Oil & Gas", "SNGS": "Oil & Gas", "TRNF": "Oil & Gas",
    # Banks
    "SBER": "Banks", "VTBR": "Banks", "TCSG": "Banks", "CBOM": "Banks",
    "ALFA": "Banks", "BSPB": "Banks", "QIWI": "Banks", "SFIN": "Banks",
    # Metals & Mining
    "GMKN": "Metals", "MAGN": "Metals", "NLMK": "Metals", "CHMF": "Metals",
    "ALRS": "Metals", "RUAL": "Metals", "MTLR": "Metals", "PLZL": "Metals",
    "POLY": "Metals", "IRKT": "Metals",
    # Telecom
    "MTSS": "Telecom", "RTKM": "Telecom", "VEON": "Telecom", "AFKS": "Telecom",
    # Tech
    "YDEX": "Tech", "OZON": "Tech", "OKEY": "Tech", "BELU": "Tech",
    "MDMG": "Tech", "SGZH": "Tech", "CIAN": "Tech", "POSI": "Tech",
    # Utilities
    "HYDR": "Utilities", "FEES": "Utilities", "TGKA": "Utilities",
    "UPRO": "Utilities", "MSNG": "Utilities", "ENPG": "Utilities",
    # Consumer / Retail
    "MVID": "Consumer", "FIVE": "Consumer", "LENT": "Consumer",
    "FIXP": "Consumer", "X5": "Consumer", "MGNT": "Consumer",
    "APTK": "Consumer", "GCHE": "Consumer",
    # Transport
    "AFLT": "Transport", "FLOT": "Transport", "NKHP": "Transport",
    "GLTR": "Transport",
    # Chemicals
    "PHOR": "Chemicals", "AKRN": "Chemicals", "KZOS": "Chemicals",
    # Defence
    "MOEX": "Finance", "ISIN": "Finance",
}

# ─── Stop words for word cloud ───────────────────────────────
STOP_WORDS = frozenset({
    "и", "в", "на", "с", "по", "к", "для", "не", "что", "это", "от", "за",
    "до", "из", "за", "при", "то", "а", "но", "или", "да", "же", "бы", "так",
    "как", "его", "ее", "её", "их", "мы", "вы", "он", "она", "они", "мне",
    "тебе", "вас", "нас", "ему", "ей", "им", "также", "еще", "ещё", "уже",
    "был", "была", "были", "было", "есть", "нет", "может", "можно", "нужно",
    "только", "даже", "уже", "все", "всё", "этот", "эта", "эти", "тот", "та",
    "те", "тут", "там", "здесь", "где", "когда", "почему", "зачем", "кто",
    "чем", "чтобы", "если", "потому", "поэтому", "однако", "хотя", "ведь",
    "просто", "очень", "более", "менее", "больше", "меньше", "почти", "около",
    "the", "and", "for", "are", "but", "not", "you", "all", "can", "had",
    "her", "was", "one", "our", "out", "day", "get", "has", "him", "his",
    "how", "its", "may", "new", "now", "old", "see", "two", "who", "boy",
    "did", "she", "use", "her", "way", "many", "oil", "gas", "rub", "usd",
    "eur", "cny", "ton", "bbl", "mln", "bln", "тыс", "млн", "млрд", "р",
    "₽", "$", "app", "www", "https", "http", "com", "ru", "ru", "index",
    "imoex", "moex", "ртс", "rts", "shares", "stock", "market", "сектор",
    "акция", "акции", "тикер", "канал", "пост", "новость", "новости",
})

# ─── Database ────────────────────────────────────────────────
DATABASE_URL = cfg.database_url_async
engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# ─── Lazy DB init (called on first API request) ─────────────────────
_db_initialized = False
_sender_name_ok = False  # cached: does posts.sender_name column exist?

async def _ensure_db():
    """Create tables and admin user on first call. Idempotent and lock-safe."""
    global _db_initialized, _sender_name_ok
    if _db_initialized:
        return
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

            # Check if migration is actually needed before ALTER TABLE
            # to avoid AccessExclusiveLock contention between instances.
            result = await conn.execute(text("""
                SELECT is_nullable FROM information_schema.columns
                WHERE table_name = 'parse_logs' AND column_name = 'channel_id'
            """))
            row = result.fetchone()
            if row and row[0] == 'NO':
                await conn.execute(text("SET LOCAL lock_timeout = '3s'"))
                try:
                    await conn.execute(text(
                        "ALTER TABLE parse_logs ALTER COLUMN channel_id DROP NOT NULL"
                    ))
                    logger.info("[migrate] parse_logs.channel_id → nullable")
                except Exception:
                    pass  # Already done by another instance

            # Migration: add sender_name to posts if missing (ALWAYS check)
            try:
                result = await conn.execute(text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'posts' AND column_name = 'sender_name'
                """))
                if result.fetchone():
                    _sender_name_ok = True
                    logger.info("[migrate] posts.sender_name exists")
                else:
                    await conn.execute(text("SET LOCAL lock_timeout = '3s'"))
                    await conn.execute(text(
                        "ALTER TABLE posts ADD COLUMN sender_name VARCHAR(255)"
                    ))
                    _sender_name_ok = True
                    logger.info("[migrate] posts.sender_name column added")
            except Exception:
                _sender_name_ok = False
                logger.warning("[migrate] posts.sender_name check failed, will use NULL")

        async with async_session() as session:
            # Reactivate all channels once on startup (recovers from auto-deactivation bug)
            result = await session.execute(
                text("""
                    UPDATE channels 
                    SET is_active = TRUE, parse_error_count = 0,
                        last_error_message = NULL, last_error_at = NULL
                    WHERE is_active = FALSE OR parse_error_count > 0
                    RETURNING id, username
                """)
            )
            reactivated = result.mappings().all()
            if reactivated:
                logger.info("[migrate] Reactivated %d channels: %s",
                    len(reactivated),
                    [r["username"] or str(r["id"]) for r in reactivated]
                )
                await session.commit()

            result = await session.execute(select(User))
            if result.scalars().first() is None:
                admin = User(username="vlad", is_active=True)
                admin.set_password("!1234567890")
                session.add(admin)
                await session.commit()
                logger.info("[lazy-init] Admin user 'vlad' created")

        _db_initialized = True
    except Exception as e:
        logger.error("[lazy-init] DB init error: %s", e)


app = FastAPI()

import secrets as _secrets

# Derive a stable auth key from DB URL (hashed) or generate random.
# Never use raw DB URL as key — it would allow session forgery.
_db_url = cfg.database_url_async or ""
if _db_url:
    AUTH_SECRET_KEY = hashlib.sha256(
        (_db_url + "citg-auth-v2-salt").encode()
    ).hexdigest()[:32]
else:
    AUTH_SECRET_KEY = _secrets.token_hex(32)
    logger.warning("[security] DB URL not set, using random auth secret. Sessions will invalidate on restart.")

# ─── Rate Limiter (simple in-memory) ─────────────────────────
import time as _time
_login_attempts: dict[str, list[float]] = {}
MAX_LOGIN_ATTEMPTS = 5
LOGIN_WINDOW_SECONDS = 900  # 15 minutes

def _check_rate_limit(key: str) -> bool:
    """Return True if within rate limit, False if exceeded."""
    now = _time.time()
    attempts = _login_attempts.get(key, [])
    attempts = [t for t in attempts if now - t < LOGIN_WINDOW_SECONDS]
    _login_attempts[key] = attempts
    if len(attempts) >= MAX_LOGIN_ATTEMPTS:
        return False
    attempts.append(now)
    return True
SESSION_COOKIE_NAME = "citg_auth_v2"  # CHANGED to invalidate old sessions
SESSION_MAX_AGE = 86400 * 7  # 7 days

# Map of path -> requires auth (for debugging)
_AUTH_DEBUG = False


def _sign_session(username: str) -> str:
    """Create signed session token."""
    timestamp = str(int(datetime.now(timezone.utc).timestamp()))
    payload = f"{username}:{timestamp}"
    sig = hmac.new(AUTH_SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()[:32]
    return f"{payload}:{sig}"


def _verify_session(token: str) -> str | None:
    """Verify session token, return username or None."""
    if not token or ":" not in token:
        return None
    parts = token.rsplit(":", 1)
    if len(parts) != 2:
        return None
    payload, sig = parts
    expected = hmac.new(AUTH_SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()[:32]
    if not secrets.compare_digest(sig, expected):
        return None
    # Check expiry
    try:
        username, timestamp_str = payload.rsplit(":", 1)
        timestamp = int(timestamp_str)
        if int(datetime.now(timezone.utc).timestamp()) - timestamp > SESSION_MAX_AGE:
            return None
        return username
    except (ValueError, IndexError):
        return None


async def _get_current_user(request: Request) -> str | None:
    """Get username from session cookie."""
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None
    return _verify_session(token)


async def _require_auth(request: Request):
    """Dependency: require authenticated user."""
    user = await _get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return user


# Security headers + no-cache for all responses
@app.middleware("http")
async def add_security_headers(request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["X-Frame-Options"] = "DENY"                        # Clickjacking protection
    response.headers["X-Content-Type-Options"] = "nosniff"              # MIME sniffing protection
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"  # HSTS
    return response


# Auth middleware: protect HTML pages with session cookie
@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    
    # Public paths that don't require auth
    public_paths = {"/login", "/favicon.ico"}
    if path in public_paths:
        return await call_next(request)

    # API paths use universal auth (checked by endpoint dependencies)
    if path.startswith("/api/") or path == "/rss":
        return await call_next(request)

    # HTML pages require session cookie
    token = request.cookies.get(SESSION_COOKIE_NAME)
    user = _verify_session(token) if token else None
    
    if _AUTH_DEBUG:
        logger.info(f"[AUTH] path={path} token={'present' if token else 'missing'} user={user}")
    
    if not user:
        response = RedirectResponse(url="/login", status_code=302)
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        # Delete any stale cookies
        response.delete_cookie("citg_session")  # old cookie name
        response.delete_cookie(SESSION_COOKIE_NAME)
        return response

    return await call_next(request)


def json_response(data, status=200):
    return JSONResponse(content=data, status_code=status)


def _channel_filter_clause(channel: Optional[str]) -> tuple[str, dict]:
    """Build SQL channel filter clause and params for JOIN with channels table.
    
    Supports both username (public) and numeric_id (private) lookups.
    """
    if not channel:
        return "", {}
    if channel.isdigit():
        return "AND (c.numeric_id = :ch_num OR c.telegram_id = :ch_tid)", {
            "ch_num": int(channel),
            "ch_tid": int(f"-100{channel}"),
        }
    return "AND c.username = :channel", {"channel": channel}


def _channel_where_clause(channel: Optional[str]) -> tuple[str, dict]:
    """Build SQL channel filter for WHERE clauses with channels table.
    
    Supports both username (public) and numeric_id (private) lookups.
    """
    if not channel:
        return "", {}
    # If channel looks like a number → filter by numeric_id, else by username
    if channel.isdigit():
        return "AND (c.numeric_id = :ch_num OR c.telegram_id = :ch_tid)", {
            "ch_num": int(channel),
            "ch_tid": int(f"-100{channel}"),
        }
    return "AND c.username = :channel", {"channel": channel}


# ─── Universal Authentication ────────────────────────────────
# Tries cookie session first (browser), then Basic Auth (curl/API)

security = HTTPBasic(auto_error=False)

async def _get_auth_user(
    request: Request,
    credentials: HTTPBasicCredentials = Depends(security),
) -> str:
    """Universal auth: cookie session (browser) OR Basic Auth (curl/API)."""
    await _ensure_db()
    # 1. Try cookie session (for logged-in browser users doing AJAX)
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token:
        user = _verify_session(token)
        if user:
            return user

    # 2. Try HTTP Basic Auth (for curl/external clients)
    if credentials:
        async with async_session() as session:
            result = await session.execute(select(User).where(User.username == credentials.username))
            user = result.scalar_one_or_none()
            if user and user.is_active and user.check_password(credentials.password):
                return credentials.username

    # 3. Unauthorized
    raise HTTPException(status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": "Basic"})


async def get_since(session, delta):
    since = datetime.now(timezone.utc) - delta
    max_pub = (await session.execute(text("SELECT MAX(published_at) FROM posts"))).scalar()
    if max_pub and max_pub.year > datetime.now(timezone.utc).year:
        since = max_pub - delta
    return since


# ═══════════════════════════════════════════════════════════════
# HTML TEMPLATES
# ═══════════════════════════════════════════════════════════════

# ─── SPA: Posts + Tags (Updated with channel filter) ─────────
INDEX_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CITG Dashboard</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0a0a1a;color:#e2e8f0;line-height:1.5}
.wrap{max-width:1200px;margin:0 auto;padding:24px}
header{margin-bottom:24px}h1{color:#00d4aa;font-size:28px;font-weight:700}h1 a{color:inherit;text-decoration:none}
.sub{color:#64748b;font-size:14px;margin-top:4px}
nav{display:flex;gap:4px;background:#0f172a;padding:4px;border-radius:10px;border:1px solid #1e293b;width:fit-content;margin-bottom:24px}
nav button{background:none;border:none;color:#64748b;padding:10px 20px;border-radius:8px;font-size:14px;font-weight:500;cursor:pointer;transition:.15s}
nav button:hover{color:#e2e8f0;background:#1e293b}
nav button.on{color:#0a0a1a;background:#00d4aa;font-weight:600}
nav a{color:#64748b;text-decoration:none;padding:10px 20px;border-radius:8px;font-size:14px;font-weight:500;display:flex;align-items:center;gap:6px}
nav a:hover{color:#e2e8f0;background:#1e293b}

/* Channel selector */
.ch-sel{display:flex;align-items:center;gap:8px;margin-bottom:16px;background:#0f172a;padding:8px 16px;border-radius:10px;border:1px solid #1e293b;width:fit-content}
.ch-sel label{color:#64748b;font-size:13px;font-weight:500}
.ch-sel select{background:#0a0a1a;border:1px solid #1e293b;color:#e2e8f0;padding:8px 14px;border-radius:8px;font-size:14px;outline:none;cursor:pointer;min-width:180px}
.ch-sel select:focus{border-color:#00d4aa}

/* Loader */
#loader{position:fixed;inset:0;background:#0a0a1a;z-index:9999;display:flex;flex-direction:column;align-items:center;justify-content:center;transition:opacity .4s}
#loader.done{opacity:0;pointer-events:none}
.loader-ring{width:48px;height:48px;border:3px solid #1e293b;border-top-color:#00d4aa;border-radius:50%;animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.loader-text{margin-top:16px;color:#64748b;font-size:14px}
#loader-sub{margin-top:8px;color:#334155;font-size:12px}

/* Stats */
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:15px;margin-bottom:24px}
.stat{background:#0f172a;border:1px solid #1e293b;border-radius:12px;padding:20px}
.stat-v{font-size:28px;font-weight:700;color:#00d4aa}
.stat-l{font-size:12px;color:#64748b;margin-top:4px}

/* Skeleton */
@keyframes shimmer{0%{background-position:-200%0}100%{background-position:200%0}}
.sk{background:linear-gradient(90deg,#0f172a 25%,#1e293b 50%,#0f172a 75%);background-size:200% 100%;animation:shimmer 1.5s infinite;border-radius:8px}

/* Filters */
.filters{display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap}
.filters input,.filters select{background:#0f172a;border:1px solid #1e293b;color:#e2e8f0;padding:10px 16px;border-radius:8px;font-size:14px;outline:none}
.filters input:focus,.filters select:focus{border-color:#00d4aa}
.btn{background:#00d4aa;color:#0a0a1a;border:none;padding:10px 24px;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer}
.btn:hover{opacity:.85}

/* Posts */
.posts{display:flex;flex-direction:column;gap:12px}
.post{background:#0f172a;border:1px solid #1e293b;border-radius:12px;padding:16px}
.post:hover{border-color:#334155}
.post-head{display:flex;gap:12px;margin-bottom:8px;font-size:13px;color:#64748b;flex-wrap:wrap;align-items:center}
.post-ch{color:#00d4aa;font-weight:600;text-decoration:none;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.post-ch:hover{text-decoration:underline}
.post-sender{color:#94a3b8;font-size:12px}
.post-body{color:#e2e8f0;white-space:pre-wrap;word-break:break-word;line-height:1.6}
.post-tags{display:flex;gap:6px;margin-top:10px;flex-wrap:wrap}
.tag{background:#1e293b;color:#00d4aa;padding:4px 12px;border-radius:20px;font-size:12px;font-weight:500}

/* Period selector */
.period{display:flex;gap:4px;background:#0f172a;padding:4px;border-radius:10px;border:1px solid #1e293b;width:fit-content}
.period button{background:none;border:none;color:#64748b;padding:8px 16px;border-radius:8px;font-size:13px;font-weight:500;cursor:pointer;transition:.15s}
.period button:hover{color:#e2e8f0;background:#1e293b}
.period button.on{color:#0a0a1a;background:#00d4aa;font-weight:600}

/* Pagination */
.page{display:flex;justify-content:center;align-items:center;gap:8px;margin-top:24px}
.page button{background:#0f172a;border:1px solid #1e293b;color:#00d4aa;padding:8px 20px;border-radius:8px;cursor:pointer;font-size:14px}
.page button:hover{background:#1e293b}
.page button:disabled{opacity:.3;cursor:not-allowed;color:#64748b}
.page span{color:#64748b;font-size:14px;padding:0 12px}

/* Error */
.err{background:#0f172a;border:1px solid #7f1d1d;border-radius:12px;padding:24px;text-align:center;margin:20px 0}
.err h3{color:#f87171;font-size:18px;margin-bottom:8px}
.err p{color:#94a3b8;font-size:14px;margin-bottom:16px}
.err button{background:#dc2626;color:#fff;border:none;padding:10px 24px;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer}
.empty{text-align:center;color:#64748b;padding:40px;font-size:14px}
</style>
</head>
<body>
<div id="loader"><div class="loader-ring"></div><div class="loader-text">Loading dashboard...</div><div id="loader-sub">Connecting to database</div></div>

<div class="wrap">
<header><h1><a href="/">CITG Dashboard</a></h1><p class="sub" id="subtitle">Loading...</p></header>
<nav>
<button class="on" data-tab="posts">Posts</button>
<button data-tab="tags">Tags 24h</button>
<a href="/charts">&#128202; Charts</a>
<a href="/analytics">&#128270; Analytics</a>
<a href="/tag-daily">&#128200; Stock</a>
<a href="/sentiment">&#129504; Sentiment</a>
<a href="/viral">&#128293; Viral</a>
<a href="/sectors">&#127775; Sectors</a>
<a href="/wordcloud">&#9729;&#65039; Words</a>
<a href="/crossmarket">&#127758; Macro</a>
<a href="/channels">&#128226; Channels</a>
<a href="/crosschannel">&#128200; Cross-Ch</a>
</nav>

<!-- Channel Filter -->
<div class="ch-sel">
<label>Channel:</label>
<select id="ch-filter" onchange="page=1;loadPosts();loadChannelStats();">
<option value="">All channels</option>
</select>
<span id="ch-info" style="color:#64748b;font-size:12px"></span>
</div>

<section id="tab-posts">
<div class="stats" id="p-stats"><div class="sk" style="height:60px"></div><div class="sk" style="height:60px"></div><div class="sk" style="height:60px"></div><div class="sk" style="height:60px"></div><div class="sk" style="height:60px"></div></div>
<div class="filters">
<input type="text" id="q" placeholder="Search text..." onkeydown="if(event.key==='Enter'){page=1;loadPosts()}">
<select id="sort"><option value="new">Newest</option><option value="views">Most views</option></select>
<button class="btn" onclick="page=1;loadPosts()">Search</button>
</div>
<div id="p-list"></div>
<div class="page" id="p-page"></div>
</section>

<section id="tab-tags" style="display:none">
<div class="period" id="tag-period" style="margin-bottom:16px">
<button class="on" data-h="24">24h</button>
<button data-h="72">3d</button>
<button data-h="168">7d</button>
<button data-h="720">30d</button>
</div>
<div class="stats" id="t-stats"><div class="sk" style="height:60px"></div><div class="sk" style="height:60px"></div><div class="sk" style="height:60px"></div></div>
<div id="t-list"></div>
</section>
</div>

<script>
(function(){
'use strict';
var page=1,loading={posts:false,tags:false};
var $=function(id){return document.getElementById(id)};
var currentChannel='';

function hideLoader(){var el=$('loader');if(el&&!el.classList.contains('done'))el.classList.add('done')}
setTimeout(hideLoader,6000);

function showError(id,msg){$(id).innerHTML='<div class="err"><h3>Failed to load</h3><p>'+esc(msg)+'</p><button onclick="location.reload()">Reload Page</button></div>';hideLoader()}
function esc(t){return String(t||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function fmt(n){return(n||0).toLocaleString('en').replace(/,/g,' ')}
function fmtViews(n){n=n||0;if(n>=1e6)return(n/1e6).toFixed(1)+'M';if(n>=1e3)return(n/1e3).toFixed(1)+'K';return String(n)}

var _postAbort=null;
async function api(path,attempt){attempt=attempt||1;try{var r=await fetch('/api'+path,{cache:'no-store',credentials:'include'});if(!r.ok)throw new Error('HTTP '+r.status);var d=await r.json();if(d.error)throw new Error(d.error);return d}catch(e){if(attempt<3){await new Promise(function(r){setTimeout(r,1000*attempt)});return api(path,attempt+1)}throw e}}

// Load channel list into dropdown
async function loadChannels(){
try{
var data=await api('/channels');
var chs=data.channels||[];
var sel=$('ch-filter');
chs.forEach(function(ch){
var opt=document.createElement('option');
// Use username for public, numeric_id for private channels
opt.value=ch.username||(ch.numeric_id!=null?String(ch.numeric_id):String(ch.telegram_id)||'');
opt.textContent=(ch.title||ch.username||(ch.numeric_id!=null?'c/'+ch.numeric_id:'@channel'))+(ch.is_active?'':' [off]');
sel.appendChild(opt);
});
}catch(e){console.error('channels load error:',e);}
}

// Load stats for selected channel
async function loadChannelStats(){
try{
var ch=$('ch-filter').value;
var q=ch?'?channel='+encodeURIComponent(ch):'';
var stats=await api('/stats'+q);
$('p-stats').innerHTML='<div class="stat"><div class="stat-v">'+fmt(stats.total_posts)+'</div><div class="stat-l">Total</div></div><div class="stat"><div class="stat-v">'+fmt(stats.today_posts)+'</div><div class="stat-l">Today</div></div><div class="stat"><div class="stat-v">'+fmt(stats.week_posts)+'</div><div class="stat-l">Week</div></div><div class="stat"><div class="stat-v">'+fmt(stats.avg_views)+'</div><div class="stat-l">Avg</div></div><div class="stat"><div class="stat-v">'+fmt(stats.total_parses)+'</div><div class="stat-l">Parses</div></div>';
$('ch-info').textContent=stats.channel_title?'('+esc(stats.channel_title)+')':'';
}catch(e){console.error('stats error:',e);}
}

// Nav
document.querySelectorAll('nav button').forEach(function(btn){btn.addEventListener('click',function(){var tab=btn.dataset.tab;document.querySelectorAll('nav button').forEach(function(b){b.classList.remove('on')});btn.classList.add('on');$('tab-posts').style.display=tab==='posts'?'':'none';$('tab-tags').style.display=tab==='tags'?'':'none';if(tab==='tags')loadTags()})});

// Posts
var _postBusy=false;
async function loadPosts(){
window.loadPosts=loadPosts;

  if(_postBusy)return;
  _postBusy=true;
  $('loader-sub').textContent='Loading posts...';
  try{
    var q=$('q').value,ch=$('ch-filter').value,sort=$('sort').value;
    var url='/api/posts?page='+page+'&search='+encodeURIComponent(q)+'&sort='+sort+(ch?'&channel='+encodeURIComponent(ch):'');
    var r=await fetch(url,{cache:'no-store',credentials:'include'});
    if(!r.ok)throw new Error('HTTP '+r.status);
    var data=await r.json();
    if(data.error)throw new Error(data.error);
    var chCount=data.posts?data.posts.length:0;
    $('subtitle').textContent=(ch?'Channel: '+esc(ch)+' | ':'')+chCount+' posts shown';
    if(!data.posts||!data.posts.length){
      $('p-list').innerHTML='<div class="empty">'+(ch?'No posts for channel '+esc(ch):'No posts')+'</div>';
    }else{
      $('p-list').innerHTML=data.posts.map(function(p){
        var tags=(p.hashtags||[]).map(function(t){return'<span class="tag">'+esc(t)+'</span>'}).join('');
        var chUrl=p.numeric_id?'https://t.me/c/'+p.numeric_id+'/'+p.id:p.channel_username?'https://t.me/'+esc(p.channel_username)+'/'+p.id:'#';
        var chLabel=p.channel_username?'@'+esc(p.channel_username):p.numeric_id?'c/'+p.numeric_id:'channel';
        return'<div class="post"><div class="post-head"><span style="color:#64748b">#'+p.id+'</span><a class="post-ch" href="'+chUrl+'" target="_blank" title="'+esc(chLabel)+'">'+esc(p.channel_title||chLabel)+'</a>'+(p.sender_name?'<span class="post-sender">by '+esc(p.sender_name)+'</span>':'')+'<span style="color:#00d4aa;font-weight:600">'+fmtViews(p.views)+'</span><span style="color:#64748b;font-size:12px">'+(p.published?p.published.slice(0,16).replace('T',' '):'')+'</span></div><div class="post-body">'+esc(p.text||'(no text)')+'</div>'+(tags?'<div class="post-tags">'+tags+'</div>':'')+'</div>';
      }).join('');
    }
    $('p-page').innerHTML='<button '+(page>1?'onclick="goPage('+(page-1)+')"':'disabled')+'>&larr; Prev</button><span>Page '+page+'</span><button '+(chCount===20?'onclick="goPage('+(page+1)+')"':'disabled')+'>Next &rarr;</button>';
    hideLoader();
  }catch(e){
    console.error('loadPosts error:',e);
    showError('p-list',e.message);
  }finally{
    _postBusy=false;
  }
}
window.goPage=function(p){page=p;loadPosts()};

// Tags -- period selector state
var tagHours=24;

async function loadTags(hours){hours=hours||tagHours;if(loading.tags)return;loading.tags=true;$('loader-sub').textContent='Loading tags...';tagHours=hours;try{var ch=$('ch-filter').value;var chQ=ch?'&channel='+encodeURIComponent(ch):'';var data=await api('/tags?hours='+hours+chQ);var tags=data.tags||[];var periodLabel=hours>=720?(hours/720)+' month':hours>=24?(hours/24)+' day':'hour';periodLabel=hours===24?'24 hours':hours===72?'3 days':hours===168?'7 days':hours===720?'30 days':periodLabel;$('t-stats').innerHTML='<div class="stat"><div class="stat-v">'+tags.length+'</div><div class="stat-l">Tags</div></div><div class="stat"><div class="stat-v">'+(tags[0]?esc(tags[0].tag):'-')+'</div><div class="stat-l">Top</div></div><div class="stat"><div class="stat-v">'+fmt(tags.reduce(function(a,t){return a+t.count},0))+'</div><div class="stat-l">Tagged</div></div>';if(!tags.length){$('t-list').innerHTML='<div class="empty">No tags for selected period</div>';hideLoader();loading.tags=false;return}var maxC=Math.max.apply(null,tags.map(function(t){return t.count}));var colors=['#00d4aa','#00b894','#0984e3','#6c5ce7','#fd79a8','#e17055','#fdcb6e','#55efc4'];$('t-list').innerHTML='<div style="color:#64748b;font-size:13px;margin-bottom:16px">Last '+periodLabel+' -- tag ranking by frequency</div>'+tags.map(function(t,i){var pct=Math.round((t.count/maxC)*100);return'<div style="display:flex;align-items:center;gap:15px;margin-bottom:10px;padding:14px 16px;background:#0f172a;border:1px solid #1e293b;border-radius:10px"><div style="min-width:160px;font-weight:600;color:#00d4aa;font-size:14px">'+esc(t.tag)+'</div><div style="flex:1;height:28px;background:#0a0a1a;border-radius:6px;overflow:hidden"><div style="height:100%;border-radius:6px;display:flex;align-items:center;padding:0 12px;font-size:12px;font-weight:600;color:#fff;transition:width .8s;width:'+pct+'%;background:'+colors[i%colors.length]+'">'+t.count+' posts</div></div><div style="min-width:90px;text-align:right;color:#64748b;font-size:12px">'+fmt(t.total_views)+' views<br>~'+fmt(t.avg_views)+'</div></div>'}).join('');hideLoader()}catch(e){if(e.name!=='AbortError'){console.error(e);showError('t-list',e.message)}}finally{loading.tags=false}}

// Tag period selector handlers
document.querySelectorAll('#tag-period button').forEach(function(btn){btn.addEventListener('click',function(){document.querySelectorAll('#tag-period button').forEach(function(b){b.classList.remove('on')});btn.classList.add('on');loadTags(parseInt(btn.dataset.h))})});

loadChannels();
loadChannelStats();
loadPosts();
})();
</script>
</body>
</html>'''


# ─── SPA: Stock + Tag Correlation ────────────────────────────
TAG_DAILY_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Stock & Tag — CITG</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0a0a1a;color:#e2e8f0;line-height:1.5}
.wrap{max-width:1200px;margin:0 auto;padding:24px}
header{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:24px}
h1{color:#00d4aa;font-size:28px;font-weight:700}
.back{color:#64748b;text-decoration:none;font-size:14px}
.back:hover{color:#00d4aa}

/* Loader */
#loader{position:fixed;inset:0;background:#0a0a1a;z-index:9999;display:flex;flex-direction:column;align-items:center;justify-content:center;transition:opacity .4s}
#loader.done{opacity:0;pointer-events:none}
.loader-ring{width:48px;height:48px;border:3px solid #1e293b;border-top-color:#00d4aa;border-radius:50%;animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.loader-text{margin-top:16px;color:#64748b;font-size:14px}

/* Search */
.search-box{display:flex;gap:8px;margin-bottom:20px;align-items:center;flex-wrap:wrap}
.search-box input{flex:1;background:#0f172a;border:1px solid #1e293b;color:#e2e8f0;padding:12px 16px;border-radius:10px;font-size:14px;outline:none}
.search-box input:focus{border-color:#00d4aa}
.search-box button{background:#00d4aa;color:#0a0a1a;border:none;padding:12px 28px;border-radius:10px;font-size:14px;font-weight:600;cursor:pointer}
.search-box button:hover{opacity:.85}
.search-box .hint{color:#64748b;font-size:13px;margin-left:12px}

/* Charts */
.chart-box{background:#0f172a;border:1px solid #1e293b;border-radius:16px;padding:20px;margin-bottom:20px}
.chart-title{font-size:16px;font-weight:600;margin-bottom:12px;color:#00d4aa}
.chart{min-height:360px}

/* Stats */
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:20px}
.stat{background:#0f172a;border:1px solid #1e293b;border-radius:12px;padding:16px;text-align:center}
.stat-v{font-size:22px;font-weight:700;color:#00d4aa}
.stat-l{font-size:11px;color:#64748b;margin-top:4px}
.stat-v.red{color:#f87171}
.stat-v.green{color:#00d4aa}

/* Error */
.err{background:#0f172a;border:1px solid #7f1d1d;border-radius:12px;padding:24px;text-align:center}
.err h3{color:#f87171;margin-bottom:8px}
.empty{text-align:center;color:#64748b;padding:60px;font-size:14px}
</style>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
</head>
<body>
<div id="loader"><div class="loader-ring"></div><div class="loader-text">Loading...</div></div>

<div class="wrap">
<header><h1>Stock Price & News Activity</h1><a href="/" class="back">&larr; Back</a></header>

<div class="stats" id="top-stats">
<div class="stat"><div class="stat-v" id="s-price">-</div><div class="stat-l">Stock Price</div></div>
<div class="stat"><div class="stat-v" id="s-change">-</div><div class="stat-l">90d Change</div></div>
<div class="stat"><div class="stat-v" id="s-total">-</div><div class="stat-l">News Posts</div></div>
<div class="stat"><div class="stat-v" id="s-days">-</div><div class="stat-l">Days with News</div></div>
</div>

<div class="search-box">
<input type="text" id="ticker-input" value="LKOH" placeholder="Enter ticker: LKOH, SBER, GAZP, YDEX..." onkeydown="if(event.key==='Enter')loadCharts()">
<button onclick="loadCharts()">Show</button>
<span class="hint">90 days | MOEX + Telegram news</span>
</div>

<div class="chart-box">
<div class="chart-title">&#128200; OHLC Candlestick (MOEX)</div>
<div class="chart" id="stock-chart"></div>
</div>

<div class="chart-box">
<div class="chart-title">&#128172; Telegram News with #<span id="tag-label">LKOH</span> <span style="color:#64748b;font-size:13px">-- click a bar to see posts</span></div>
<div class="chart" id="tag-chart"></div>
</div>

<div id="posts-box" class="chart-box" style="display:none;margin-top:20px">
<div class="chart-title">&#128220; Posts for <span id="posts-date">-</span></div>
<div id="posts-list" style="max-height:400px;overflow-y:auto"></div>
</div>

<div id="intraday-box" class="chart-box" style="display:none;margin-top:20px">
<div class="chart-title">&#9200; 5-Min Intraday OHLC + News Markers</div>
<div class="chart" id="intraday-chart" style="min-height:300px"></div>
</div>
</div>

<script>
(function(){
'use strict';
var $=function(id){return document.getElementById(id)};
var stockChart=null, tagChart=null, intradayChart=null;
var currentTicker='', currentTag='', currentFullDates=[], currentDayLabels=[], currentCounts=[];

function hideLoader(){var el=$('loader');if(el&&!el.classList.contains('done'))el.classList.add('done')}
setTimeout(hideLoader,6000);

function esc(t){return String(t||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function fmt(n){return(n||0).toLocaleString('en').replace(/,/g,' ')}

async function api(path){
  var r=await fetch('/api'+path,{cache:'no-store',credentials:'include'});
  if(!r.ok) throw new Error('HTTP '+r.status);
  var d=await r.json();
  if(d.error) throw new Error(d.error);
  return d;
}

function getStockChart(){if(!stockChart)stockChart=echarts.init($('stock-chart'),null,{renderer:'canvas'});return stockChart;}
function getTagChart(){if(!tagChart)tagChart=echarts.init($('tag-chart'),null,{renderer:'canvas'});return tagChart;}

async function showPostsForDay(idx){
  if(idx<0||!currentFullDates[idx]||currentCounts[idx]===0)return;
  var date=currentFullDates[idx];
  var label=currentDayLabels[idx];
  $('posts-box').style.display='';
  $('intraday-box').style.display='';
  $('posts-list').innerHTML='<div style="text-align:center;color:#64748b;padding:20px">Loading...</div>';
  if(!intradayChart)intradayChart=echarts.init($('intraday-chart'),null,{renderer:'canvas'});
  intradayChart.showLoading({text:'Loading 5-min candles...',color:'#00d4aa',textColor:'#64748b',maskColor:'rgba(10,10,26,0.8)'});
  $('posts-date').textContent=label+' ('+date+')';
  try{
    var data=await api('/analytics/tag-posts-by-day?tag='+encodeURIComponent(currentTag)+'&date='+encodeURIComponent(date));
    var posts=data.posts||[];
    var intra=await api('/stock/intraday?ticker='+encodeURIComponent(currentTicker)+'&date='+encodeURIComponent(date));
    var times=intra.times||[];
    var ohlc=intra.ohlc||[];
    var timeIndex={};
    for(var i=0;i<times.length;i++)timeIndex[times[i]]=i;
    var overlayData=times.map(function(){return null;});
    var newsMap={};
    posts.filter(function(p){return p.published;}).forEach(function(p,pi){
      var h=parseInt(p.published.slice(11,13));
      var m=parseInt(p.published.slice(14,16));
      h=(h+3)%24;
      m=Math.round(m/10)*10;
      if(m===60){m=0;h=(h+1)%24;}
      var t=(h<10?'0':'')+h+':'+(m<10?'0':'')+m;
      var idx=timeIndex[t]!==undefined?timeIndex[t]:-1;
      console.log('POST',pi,'UTC='+p.published.slice(11,16),'MSK='+t,'idx='+idx,'text='+p.text.slice(0,30));
      if(idx>=0&&idx<ohlc.length){
        overlayData[idx]=ohlc[idx][3];
        newsMap[idx]=p.text?p.text:'News';
        console.log('  -> placed at idx',idx);
      }
    });
    console.log('overlayData non-null:',overlayData.filter(function(x){return x!==null;}).length);
    var scatterData=[];
    for(var i=0;i<overlayData.length;i++){
      if(overlayData[i]!==null)scatterData.push([i,overlayData[i],newsMap[i]]);
    }
    if(times.length&&ohlc.length){
      intradayChart.hideLoading();
      intradayChart.setOption({
        backgroundColor:'transparent',
        tooltip:{trigger:'axis',axisPointer:{type:'cross'},formatter:function(p){
          var d=p[0]; var o=d.data[1],cl=d.data[2],lo=d.data[3],hi=d.data[4];
          var color=cl>=o?'#00d4aa':'#f87171';
          return d.name+'<br><span style="color:'+color+'">O:'+fmt(o)+' C:'+fmt(cl)+' L:'+fmt(lo)+' H:'+fmt(hi)+'</span>';
        }},
        grid:{left:50,right:20,top:30,bottom:50},
        xAxis:{type:'category',data:times,axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b',fontSize:9,interval:11}},
        yAxis:{type:'value',name:'RUB',scale:true,splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b'}},
        series:[
          {type:'candlestick',data:ohlc,itemStyle:{color:'#00d4aa',color0:'#f87171',borderColor:'#00d4aa',borderColor0:'#f87171'}},
          {type:'scatter',data:scatterData,symbol:'circle',symbolSize:14,
           itemStyle:{color:'#fdcb6e',borderColor:'#fff',borderWidth:2},
           label:{show:true,formatter:'!',color:'#0a0a1a',fontSize:10,fontWeight:'bold'},
           emphasis:{scale:1.5,itemStyle:{color:'#fdcb6e',borderColor:'#00d4aa',borderWidth:3}},
           tooltip:{trigger:'item',show:true,confine:true,textStyle:{width:400},formatter:function(p){var d=p.data,t=d[2]||'';var txt=t.length>300?t.substring(0,300)+'...':t;return'<div style=\"max-width:380px;word-break:break-word;white-space:normal;line-height:1.4\"><b style=\"color:#fdcb6e\">News at '+esc(times[d[0]])+'</b><br>'+esc(txt)+'</div>';}}}
        ]
      },true);
    }else{intradayChart.hideLoading();$('intraday-chart').innerHTML='<div class="empty">No intraday data for '+date+'</div>';}
    if(!posts.length){$('posts-list').innerHTML='<div style="text-align:center;color:#64748b;padding:20px">No posts for this day</div>';return;}
    $('posts-list').innerHTML=posts.map(function(p){
      return'<div style="background:#0a0a1a;border-radius:8px;padding:12px;margin-bottom:8px;font-size:13px"><div style="color:#64748b;font-size:11px;margin-bottom:4px">ID:'+p.id+' | views:'+fmt(p.views)+' | '+esc((p.published||'').slice(0,16).replace('T',' '))+'</div><div style="color:#e2e8f0;white-space:pre-wrap;word-break:break-word">'+esc(p.text||'(no text)')+'</div></div>';
    }).join('');
  }catch(e){$('posts-list').innerHTML='<div style="text-align:center;color:#f87171;padding:20px">'+esc(e.message)+'</div>';}
}

async function loadCharts(){
  var ticker=$('ticker-input').value.trim().toUpperCase();
  if(!ticker){$('ticker-input').focus();return;}
  $('tag-label').textContent=ticker;
  $('s-price').textContent='...';
  $('s-change').textContent='...';
  $('s-total').textContent='...';

  if(stockChart){try{stockChart.dispose();}catch(e){}stockChart=null;}
  if(tagChart){try{tagChart.dispose();}catch(e){}tagChart=null;}
  if(intradayChart){try{intradayChart.dispose();}catch(e){}intradayChart=null;}
  $('stock-chart').innerHTML='';
  $('tag-chart').innerHTML='';
  $('intraday-chart').innerHTML='';
  $('posts-box').style.display='none';
  $('intraday-box').style.display='none';

  try{
    var s=await api('/stock/price?ticker='+encodeURIComponent(ticker)+'&days=90');
    var t=await api('/analytics/tag-daily?tag=%23'+encodeURIComponent(ticker)+'&days=90');

    var ohlc=s.ohlc||[];
    var latest=ohlc.length?ohlc[ohlc.length-1][1]:0;
    var first=ohlc.length?ohlc[0][0]:0;
    var pct=first?(((latest-first)/first)*100):0;
    $('s-price').textContent=latest?fmt(latest)+' RUB':'N/A';
    var chEl=$('s-change');
    chEl.textContent=pct?(pct>=0?'+':'')+pct.toFixed(1)+'%':'N/A';
    chEl.className='stat-v '+(pct>=0?'green':'red');

    var counts=t.counts||[];
    currentFullDates=t.full_dates||t.days||[];
    currentDayLabels=t.days||[];
    currentCounts=counts;
    currentTicker=ticker;
    currentTag='#'+ticker;
    var total=counts.reduce(function(a,b){return a+b},0);
    var nonzero=counts.filter(function(c){return c>0}).length;
    $('s-total').textContent=fmt(total);
    $('s-days').textContent=nonzero;

    if(s.days&&s.days.length&&ohlc.length){
      getStockChart().setOption({
        backgroundColor:'transparent',
        tooltip:{trigger:'axis',axisPointer:{type:'cross'},formatter:function(p){
          var d=p[0];
          var o=d.data[1],cl=d.data[2],lo=d.data[3],hi=d.data[4];
          var color=cl>=o?'#00d4aa':'#f87171';
          return d.name+'<br><span style="color:'+color+'">O:'+fmt(o)+' C:'+fmt(cl)+'<br>L:'+fmt(lo)+' H:'+fmt(hi)+'</span>';
        }},
        grid:{left:50,right:20,top:20,bottom:70},
        xAxis:{type:'category',data:s.days,axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b',rotate:45,fontSize:10}},
        yAxis:{type:'value',name:'RUB',scale:true,splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b',formatter:function(v){return v>=1000?(v/1000).toFixed(0)+'k':v;}}},
        series:[{
          type:'candlestick',data:ohlc,
          itemStyle:{color:'#00d4aa',color0:'#f87171',borderColor:'#00d4aa',borderColor0:'#f87171'},
          markLine:{silent:true,data:[{type:'average',name:'Avg'}],lineStyle:{color:'#64748b',type:'dashed',width:1},label:{color:'#64748b',formatter:function(p){return fmt(p.value);}}}
        }]
      },true);
    }else{$('stock-chart').innerHTML='<div class="empty">No stock data for '+esc(ticker)+'</div>';}

    if(t.days&&t.days.length){
      getTagChart().off('click');
      getTagChart().on('click',function(params){if(params.componentType==='series')showPostsForDay(params.dataIndex);});
      getTagChart().setOption({
        backgroundColor:'transparent',
        tooltip:{trigger:'axis',formatter:function(p){return p[0].name+': '+p[0].value+' posts';}},
        grid:{left:50,right:20,top:20,bottom:70},
        xAxis:{type:'category',data:t.days,axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b',rotate:45,fontSize:10}},
        yAxis:{type:'value',name:'Posts',splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b'}},
        series:[{
          type:'bar',data:counts,itemStyle:{color:function(p){return p.value>0?'#6c5ce7':'#1e293b'},borderRadius:[3,3,0,0]},
          animationDuration:600
        }]
      },true);
    }else{$('tag-chart').innerHTML='<div class="empty">No news data for #'+esc(ticker)+'</div>';}

    hideLoader();
  }catch(e){
    console.error(e);
    getStockChart().setOption({
      backgroundColor:'transparent',
      title:{text:'Error: '+esc(e.message),left:'center',top:'center',
             textStyle:{color:'#f87171',fontSize:14}}
    },true);
    getTagChart().setOption({
      backgroundColor:'transparent',
      title:{text:'Error: '+esc(e.message),left:'center',top:'center',
             textStyle:{color:'#f87171',fontSize:14}}
    },true);
    hideLoader();
  }
}

window.loadCharts=loadCharts;
window.addEventListener('resize',function(){try{if(stockChart)stockChart.resize();}catch(e){}try{if(tagChart)tagChart.resize();}catch(e){}try{if(intradayChart)intradayChart.resize();}catch(e){}});
loadCharts();
})();
</script>
</body>
</html>'''


# ─── SPA: Analytics (Tag Deep Dive) ──────────────────────────
ANALYTICS_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Tag Analytics — CITG</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0a0a1a;color:#e2e8f0;line-height:1.5}
.wrap{max-width:1200px;margin:0 auto;padding:24px}
header{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:24px}
h1{color:#00d4aa;font-size:28px;font-weight:700}
.back{color:#64748b;text-decoration:none;font-size:14px}
.back:hover{color:#00d4aa}

/* Loader */
#loader{position:fixed;inset:0;background:#0a0a1a;z-index:9999;display:flex;flex-direction:column;align-items:center;justify-content:center;transition:opacity .4s}
#loader.done{opacity:0;pointer-events:none}
.loader-ring{width:48px;height:48px;border:3px solid #1e293b;border-top-color:#00d4aa;border-radius:50%;animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.loader-text{margin-top:16px;color:#64748b;font-size:14px}

/* Search */
.search-box{display:flex;gap:8px;margin-bottom:24px}
.search-box input{flex:1;background:#0f172a;border:1px solid #1e293b;color:#e2e8f0;padding:12px 16px;border-radius:10px;font-size:14px;outline:none}
.search-box input:focus{border-color:#00d4aa}
.search-box button{background:#00d4aa;color:#0a0a1a;border:none;padding:12px 24px;border-radius:10px;font-size:14px;font-weight:600;cursor:pointer}
.search-box button:hover{opacity:.85}

/* Sections */
.section{background:#0f172a;border:1px solid #1e293b;border-radius:16px;padding:20px;margin-bottom:20px}
.section h2{font-size:18px;color:#00d4aa;margin-bottom:16px}
.section h2 span{color:#64748b;font-size:13px;font-weight:400;margin-left:8px}

/* Tag list */
.tag-list{display:flex;flex-direction:column;gap:8px}
.tag-row{display:flex;align-items:center;gap:12px;padding:10px 14px;background:#0a0a1a;border-radius:8px;cursor:pointer;transition:.15s}
.tag-row:hover{background:#1e293b}
.tag-name{min-width:140px;font-weight:600;color:#00d4aa;font-size:14px}
.tag-bar{flex:1;height:24px;background:#0f172a;border-radius:6px;overflow:hidden}
.tag-bar-fill{height:100%;border-radius:6px;display:flex;align-items:center;padding:0 10px;font-size:11px;font-weight:600;color:#fff;transition:width .6s}
.tag-count{min-width:60px;text-align:right;color:#64748b;font-size:12px}
.tag-views{color:#94a3b8;font-size:11px;min-width:80px;text-align:right}

/* Trends */
.trend-up{color:#00d4aa}
.trend-down{color:#f87171}
.trend-same{color:#64748b}
.trend-pct{font-size:12px;font-weight:600;margin-left:6px}

/* Word cloud */
.cloud{display:flex;flex-wrap:wrap;gap:8px;align-items:center;justify-content:center;min-height:200px;padding:20px}
.cloud-tag{padding:8px 16px;border-radius:20px;font-weight:600;cursor:pointer;transition:transform .2s,opacity .2s;opacity:.8}
.cloud-tag:hover{transform:scale(1.1);opacity:1}

/* Posts by tag */
.posts-by-tag{margin-top:12px}
.post-mini{background:#0a0a1a;border-radius:8px;padding:12px;margin-bottom:8px;font-size:13px}
.post-mini-head{color:#64748b;font-size:11px;margin-bottom:4px}
.post-mini-body{color:#e2e8f0;white-space:pre-wrap;word-break:break-word;max-height:80px;overflow:hidden}

/* Export btn */
.export-btn{background:#0f172a;border:1px solid #00d4aa;color:#00d4aa;padding:10px 20px;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;text-decoration:none;display:inline-block}
.export-btn:hover{background:#00d4aa;color:#0a0a1a}

/* Error */
.err{background:#0f172a;border:1px solid #7f1d1d;border-radius:12px;padding:24px;text-align:center}
.err h3{color:#f87171;margin-bottom:8px}
.err p{color:#94a3b8;margin-bottom:16px}
.empty{text-align:center;color:#64748b;padding:40px;font-size:14px}
</style>
</head>
<body>
<div id="loader"><div class="loader-ring"></div><div class="loader-text">Loading analytics...</div></div>

<div class="wrap">
<header><h1>Tag Analytics</h1><a href="/" class="back">&larr; Back to Dashboard</a></header>

<div class="search-box">
<input type="text" id="tag-search" placeholder="Search tag (e.g. #россия)..." onkeydown="if(event.key==='Enter')searchTag()">
<button onclick="searchTag()">Search Posts</button>
<a href="/api/analytics/export-csv" class="export-btn" target="_blank">Export CSV</a>
</div>

<div class="section">
<h2>All-Time Top Tags <span>by frequency</span></h2>
<div id="alltime-list"><div class="empty">Loading...</div></div>
</div>

<div class="section">
<h2>Trending <span>this week vs last week</span></h2>
<div id="trends-list"><div class="empty">Loading...</div></div>
</div>

<div class="section">
<h2>Tag Cloud</h2>
<div id="cloud" class="cloud"><div class="empty">Loading...</div></div>
</div>

<div id="posts-section" class="section" style="display:none">
<h2 id="posts-title">Posts</h2>
<div id="posts-list" class="posts-by-tag"></div>
</div>
</div>

<script>
(function(){
'use strict';
var $=function(id){return document.getElementById(id)};
function hideLoader(){var el=$('loader');if(el&&!el.classList.contains('done'))el.classList.add('done')}
setTimeout(hideLoader,6000);
function esc(t){return String(t||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function fmt(n){return(n||0).toLocaleString('en').replace(/,/g,' ')}

async function api(path){
  var r=await fetch('/api'+path,{cache:'no-store',credentials:'include'});
  if(!r.ok) throw new Error('HTTP '+r.status);
  var d=await r.json();
  if(d.error) throw new Error(d.error);
  return d;
}

async function loadAll(){
  try{
    var t=await api('/analytics/alltime-tags');
    renderAllTime(t.tags||[]);
    var tr=await api('/analytics/trends');
    renderTrends(tr.trends||[]);
    hideLoader();
  }catch(e){
    console.error(e);
    $('alltime-list').innerHTML='<div class="err"><h3>Error</h3><p>'+esc(e.message)+'</p></div>';
    $('trends-list').innerHTML='<div class="err"><h3>Error</h3><p>'+esc(e.message)+'</p></div>';
    hideLoader();
  }
}

function renderAllTime(tags){
  if(!tags.length){$('alltime-list').innerHTML='<div class="empty">No tagged posts</div>';return;}
  var maxC=Math.max.apply(null,tags.map(function(t){return t.count||0}))||1;
  var colors=['#00d4aa','#00b894','#0984e3','#6c5ce7','#fd79a8','#e17055','#fdcb6e','#55efc4'];
  $('alltime-list').innerHTML=tags.slice(0,50).map(function(t,i){
    var pct=Math.round(((t.count||0)/maxC)*100);
    return'<div class="tag-row" onclick="searchTag(&quot;'+esc(t.tag)+'&quot;)"><div class="tag-name">'+esc(t.tag)+'</div><div class="tag-bar"><div class="tag-bar-fill" style="width:'+pct+'%;background:'+colors[i%colors.length]+'">'+fmt(t.count)+'</div></div><div class="tag-views">'+fmt(t.total_views||0)+' views</div></div>';
  }).join('');
  renderCloud(tags);
}

function renderTrends(trends){
  if(!trends.length){$('trends-list').innerHTML='<div class="empty">No trend data</div>';return;}
  $('trends-list').innerHTML=trends.map(function(t){
    var cls=t.pct>0?'trend-up':t.pct<0?'trend-down':'trend-same';
    var arrow=t.pct>0?'&#9650;':t.pct<0?'&#9660;':'&#9644;';
    return'<div class="tag-row"><div class="tag-name">'+esc(t.tag)+'</div><div style="flex:1"></div><div class="tag-count">'+fmt(t.this_week)+' this week</div><div class="tag-count">'+fmt(t.last_week)+' last</div><div class="trend-pct '+cls+'">'+arrow+' '+Math.abs(t.pct)+'%</div></div>';
  }).join('');
}

function renderCloud(tags){
  if(!tags.length){$('cloud').innerHTML='<div class="empty">No data</div>';return;}
  var maxC=Math.max.apply(null,tags.map(function(t){return t.count||0}))||1;
  var colors=['#00d4aa','#00b894','#0984e3','#6c5ce7','#fd79a8','#e17055','#fdcb6e','#55efc4','#00cec9','#81ecec'];
  $('cloud').innerHTML=tags.slice(0,40).map(function(t,i){
    var size=10+Math.round(((t.count||0)/maxC)*26);
    return'<span class="cloud-tag" style="font-size:'+size+'px;background:'+colors[i%colors.length]+'20;color:'+colors[i%colors.length]+';border:1px solid '+colors[i%colors.length]+'40" onclick="searchTag(&quot;'+esc(t.tag)+'&quot;)">'+esc(t.tag)+'</span>';
  }).join('');
}

async function searchTag(tag){
  var q=tag||$('tag-search').value.trim();
  if(!q)return;
  $('tag-search').value=q;
  $('posts-section').style.display='';
  $('posts-list').innerHTML='<div class="empty">Loading posts...</div>';
  $('posts-title').innerHTML='Posts with '+esc(q);
  try{
    var data=await api('/analytics/posts-by-tag?tag='+encodeURIComponent(q));
    var posts=data.posts||[];
    if(!posts.length){$('posts-list').innerHTML='<div class="empty">No posts found</div>';return;}
    $('posts-list').innerHTML=posts.map(function(p){
      return'<div class="post-mini"><div class="post-mini-head">ID:'+p.id+' | views:'+fmt(p.views)+' | '+esc((p.published||'').slice(0,16))+'</div><div class="post-mini-body">'+esc(p.text||'(no text)')+'</div></div>';
    }).join('');
  }catch(e){
    $('posts-list').innerHTML='<div class="err"><h3>Error</h3><p>'+esc(e.message)+'</p></div>';
  }
}

window.searchTag=searchTag;
loadAll();
})();
</script>
</body>
</html>'''


# ─── SPA: Charts (separate page) ─────────────────────────────
CHARTS_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Charts — CITG</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0a0a1a;color:#e2e8f0;line-height:1.5}
.wrap{max-width:1400px;margin:0 auto;padding:24px}
header{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:24px}
h1{color:#00d4aa;font-size:28px;font-weight:700}
.sub{color:#64748b;font-size:14px}
.back{color:#64748b;text-decoration:none;font-size:14px}
.back:hover{color:#00d4aa}
.period{display:flex;gap:4px;background:#0f172a;padding:4px;border-radius:10px;border:1px solid #1e293b}
.period button{background:none;border:none;color:#64748b;padding:8px 16px;border-radius:8px;font-size:13px;font-weight:500;cursor:pointer}
.period button:hover{color:#e2e8f0;background:#1e293b}
.period button.on{color:#0a0a1a;background:#00d4aa;font-weight:600}

/* Loader */
#loader{position:fixed;inset:0;background:#0a0a1a;z-index:9999;display:flex;flex-direction:column;align-items:center;justify-content:center;transition:opacity .4s}
#loader.done{opacity:0;pointer-events:none}
.loader-ring{width:48px;height:48px;border:3px solid #1e293b;border-top-color:#00d4aa;border-radius:50%;animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.loader-text{margin-top:16px;color:#64748b;font-size:14px}
#loader-sub{margin-top:8px;color:#334155;font-size:12px}

.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(400px,1fr));gap:20px;margin-bottom:20px}
.chart-box{background:#0f172a;border:1px solid #1e293b;border-radius:16px;padding:20px}
.chart-box:hover{border-color:#334155}
.chart-title{font-size:16px;font-weight:600;margin-bottom:4px}
.chart-sub{font-size:13px;color:#64748b;margin-bottom:16px}
.chart{min-height:360px}
.full{grid-column:1/-1}

.stats-bar{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:12px;margin-bottom:20px}
.stat{background:#0f172a;border:1px solid #1e293b;border-radius:12px;padding:16px;text-align:center}
.stat-v{font-size:24px;font-weight:700;color:#00d4aa}
.stat-l{font-size:11px;color:#64748b;margin-top:4px}

.loading{display:flex;align-items:center;justify-content:center;height:360px;color:#64748b;font-size:14px}
.spinner{width:32px;height:32px;border:2px solid #1e293b;border-top-color:#00d4aa;border-radius:50%;animation:spin 1s linear infinite;margin-right:12px}

.err-box{background:#0f172a;border:1px solid #7f1d1d;border-radius:12px;padding:24px;text-align:center}
.err-box h3{color:#f87171;margin-bottom:8px}
.err-box p{color:#94a3b8;margin-bottom:16px}
.err-box button{background:#dc2626;color:#fff;border:none;padding:10px 24px;border-radius:8px;font-weight:600;cursor:pointer}
</style>
</head>
<body>
<div id="loader"><div class="loader-ring"></div><div class="loader-text">Loading charts...</div><div id="loader-sub">Fetching data</div></div>

<div class="wrap">
<header>
<h1>Analytics Dashboard</h1>
<div class="period">
<button class="on" data-d="1">24h</button>
<button data-d="3">3d</button>
<button data-d="7">7d</button>
<button data-d="30">30d</button>
</div>
<a href="/" class="back">&larr; Back to Posts</a>
</header>

<div class="stats-bar" id="top-stats"></div>

<div class="grid">
<div class="chart-box full">
<div class="chart-title">Tag Bubble Chart</div>
<div class="chart-sub">Size = frequency, Y = avg reach</div>
<div class="chart" id="c-bubble"><div class="loading"><div class="spinner"></div>Loading...</div></div>
</div>

<div class="chart-box">
<div class="chart-title">Activity Heatmap</div>
<div class="chart-sub">Posts by day &amp; hour</div>
<div class="chart" id="c-heat"><div class="loading"><div class="spinner"></div></div></div>
</div>

<div class="chart-box">
<div class="chart-title">Views Distribution</div>
<div class="chart-sub">Posts by view ranges</div>
<div class="chart" id="c-hist"><div class="loading"><div class="spinner"></div></div></div>
</div>

<div class="chart-box full">
<div class="chart-title">Tag Timeline</div>
<div class="chart-sub">Daily activity per tag</div>
<div class="chart" id="c-time"><div class="loading"><div class="spinner"></div></div></div>
</div>

<div class="chart-box">
<div class="chart-title">Top Tag Pairs</div>
<div class="chart-sub">Tags that appear together</div>
<div class="chart" id="c-pair"><div class="loading"><div class="spinner"></div></div></div>
</div>
</div>
</div>

<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<script>
(function(){
'use strict';
var days=1, charts={};
var $=function(id){return document.getElementById(id)};

function hideLoader(){var el=$('loader');if(el&&!el.classList.contains('done'))el.classList.add('done')}
setTimeout(hideLoader,5000);

window.onerror=function(msg,url,line){console.error('JS ERROR:',msg,'line',line);hideLoader();try{getChart('c-bubble').setOption({backgroundColor:'transparent',title:{text:'JS Error: '+msg,left:'center',top:'center',textStyle:{color:'#f87171',fontSize:14}}},true);}catch(e){}return true};

function fmt(n){return(n||0).toLocaleString('en').replace(/,/g,' ')}
function esc(t){return String(t||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}

async function api(path,attempt){
  attempt=attempt||1;
  try{
    console.log('API fetch:',path);
    var r=await fetch('/api'+path,{cache:'no-store',credentials:'include'});
    if(!r.ok) throw new Error('HTTP '+r.status);
    var d=await r.json();
    if(d.error) throw new Error(d.error);
    console.log('API OK:',path);
    return d;
  }catch(e){
    console.error('API fail',path,'attempt',attempt,e.message);
    if(attempt<3){await new Promise(function(r){setTimeout(r,1000*attempt)});return api(path,attempt+1)}
    throw e;
  }
}

// Period selector
document.querySelectorAll('.period button').forEach(function(btn){
  btn.addEventListener('click',function(){
    document.querySelectorAll('.period button').forEach(function(b){b.classList.remove('on')});
    btn.classList.add('on');
    days=parseInt(btn.dataset.d);
    loadAll();
  });
});

function resetCharts(){
  Object.keys(charts).forEach(function(id){try{charts[id].dispose();}catch(e){}});
  charts={};
}

function showChartsLoader(msg){
  var sub=$('loader-sub');if(sub)sub.textContent=msg||'Fetching data...';
  var loader=$('loader');if(loader){loader.classList.remove('done');loader.style.opacity='';loader.style.pointerEvents='';}
}

async function loadAll(){
  console.log('loadAll() start, days='+days);
  resetCharts();
  ['c-bubble','c-heat','c-hist','c-time','c-pair'].forEach(function(id){var el=$(id);if(el)el.innerHTML='';});
  showChartsLoader('Fetching data for '+days+'d...');
  try{
    if(typeof echarts==='undefined'){
      throw new Error('ECharts not loaded. Check CDN connection.');
    }
    console.log('ECharts OK');
    var t,a,v,tl,p;
    try{
      var results=await Promise.all([
        api('/charts/tags?days='+days),
        api('/charts/activity?days='+days),
        api('/charts/views?days='+days),
        api('/charts/timeline?days='+days),
        api('/charts/pairs?days='+days)
      ]);
      t=results[0];a=results[1];v=results[2];tl=results[3];p=results[4];
    }catch(pe){throw new Error('API: '+pe.message)}
    console.log('All API loaded, tags:',t.tags.length);
    $('top-stats').innerHTML=[['Posts',fmt(t.total_posts)],['Tags',t.tags.length],['Top',t.tags[0]?t.tags[0].tag:'-'],['Avg',fmt(t.avg_reach)],['Peak',a.peak_hour+'h']].map(function(s){return'<div class="stat"><div class="stat-v">'+esc(s[1])+'</div><div class="stat-l">'+s[0]+'</div></div>'}).join('');
    try{renderBubble(t.tags);console.log('bubble OK')}catch(e){console.error('bubble:',e);try{getChart('c-bubble').setOption({backgroundColor:'transparent',title:{text:'Error: '+e.message,left:'center',top:'center',textStyle:{color:'#f87171',fontSize:14}}},true);}catch(e2){}}
    try{renderHeat(a.hours,a.peak_hour);console.log('heat OK')}catch(e){console.error('heat:',e);try{getChart('c-heat').setOption({backgroundColor:'transparent',title:{text:'Error: '+e.message,left:'center',top:'center',textStyle:{color:'#f87171',fontSize:14}}},true);}catch(e2){}}
    try{renderHist(v.bins);console.log('hist OK')}catch(e){console.error('hist:',e);try{getChart('c-hist').setOption({backgroundColor:'transparent',title:{text:'Error: '+e.message,left:'center',top:'center',textStyle:{color:'#f87171',fontSize:14}}},true);}catch(e2){}}
    try{renderTime(tl.tags);console.log('time OK')}catch(e){console.error('time:',e);try{getChart('c-time').setOption({backgroundColor:'transparent',title:{text:'Error: '+e.message,left:'center',top:'center',textStyle:{color:'#f87171',fontSize:14}}},true);}catch(e2){}}
    try{renderPairs(p.pairs);console.log('pairs OK')}catch(e){console.error('pairs:',e);try{getChart('c-pair').setOption({backgroundColor:'transparent',title:{text:'Error: '+e.message,left:'center',top:'center',textStyle:{color:'#f87171',fontSize:14}}},true);}catch(e2){}}
    hideLoader();
    console.log('all done');
  }catch(e){
    console.error('loadAll ERROR:',e);
    resetCharts();
    ['c-bubble','c-heat','c-hist','c-time','c-pair'].forEach(function(id){var el=$(id);if(el)el.innerHTML='';});
    try{echarts.init($('c-bubble'),null,{renderer:'canvas'}).setOption({backgroundColor:'transparent',title:{text:'Error: '+esc(e.message),left:'center',top:'center',textStyle:{color:'#f87171',fontSize:14}}});}catch(e2){}
    hideLoader();
  }
}

function getChart(id){
  if(!charts[id]){
    var el=$(id);
    if(!el) throw new Error('Element #'+id+' not found');
    if(typeof echarts==='undefined') throw new Error('ECharts not loaded');
    charts[id]=echarts.init(el,null,{renderer:'canvas'});
  }
  return charts[id];
}

function renderBubble(tags){
  var c=getChart('c-bubble');
  var data=tags.slice(0,30).map(function(t,i){return[i+1,t.avg_views,t.count,t.tag,t.total_views]});
  c.setOption({
    backgroundColor:'transparent',
    tooltip:{formatter:function(p){return'<b>'+p.data[3]+'</b><br>Posts: '+p.data[2]+'<br>Avg: '+fmt(p.data[1])+'<br>Total: '+fmt(p.data[4])}},
    grid:{left:60,right:30,top:30,bottom:60},
    xAxis:{type:'value',name:'Rank',splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b'}},
    yAxis:{type:'value',name:'Avg Views',splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b',formatter:function(v){return v>=1000?(v/1000)+'K':v}}},
    series:[{type:'scatter',data:data,symbolSize:function(v){return Math.max(15,Math.min(80,v[2]*3))},itemStyle:{color:function(p){var cl=['#00d4aa','#00b894','#0984e3','#6c5ce7','#fd79a8','#e17055','#fdcb6e','#55efc4'];return cl[p.dataIndex%cl.length]}},label:{show:true,formatter:function(p){return p.data[3]},position:'top',color:'#94a3b8',fontSize:11}}]
  });
}

function renderHeat(hours,peak){
  var c=getChart('c-heat');
  var dayNames=['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
  var data=[];
  for(var d=0;d<7;d++)for(var h=0;h<24;h++)data.push([h,d,hours[d*24+h]||0]);
  var mx=Math.max.apply(null,data.map(function(x){return x[2]}))||1;
  c.setOption({
    backgroundColor:'transparent',
    tooltip:{formatter:function(p){return dayNames[p.data[1]]+' '+p.data[0]+':00 \u2014 '+p.data[2]+' posts'}},
    grid:{left:60,right:20,top:10,bottom:30},
    xAxis:{type:'category',data:Array.from({length:24},function(_,i){return i}),splitArea:{show:false},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b',interval:2}},
    yAxis:{type:'category',data:dayNames,splitArea:{show:false},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b'}},
    visualMap:{min:0,max:mx,orient:'horizontal',left:'center',bottom:0,inRange:{color:['#0f172a','#1e293b','#00d4aa','#00b894','#fdcb6e']},textStyle:{color:'#64748b'}},
    series:[{type:'heatmap',data:data,label:{show:false}}]
  });
}

function renderHist(bins){
  var c=getChart('c-hist');
  c.setOption({
    backgroundColor:'transparent',
    tooltip:{formatter:function(p){return p.name+': '+p.value+' posts'}},
    grid:{left:50,right:30,top:20,bottom:50},
    xAxis:{type:'category',data:bins.map(function(b){return b.label}),axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b',rotate:30}},
    yAxis:{type:'value',splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b'}},
    series:[{type:'bar',data:bins.map(function(b){return b.count}),itemStyle:{color:function(p){var cl=['#00d4aa','#00b894','#0984e3','#6c5ce7','#fd79a8','#e17055'];return cl[p.dataIndex%cl.length]},borderRadius:[4,4,0,0]}}]
  });
}

function renderTime(tags){
  var c=getChart('c-time');
  if(!tags||!tags.length){
    c.setOption({title:{text:'No data',left:'center',top:'center',textStyle:{color:'#64748b'}}},true);
    return;
  }
  var series=tags.slice(0,8).map(function(t,i){
    var cl=['#00d4aa','#00b894','#0984e3','#6c5ce7','#fd79a8','#e17055','#fdcb6e','#55efc4'];
    return{name:t.tag,type:'line',smooth:true,symbol:'none',lineStyle:{width:2,color:cl[i%8]},areaStyle:{color:cl[i%8],opacity:.1},data:t.series};
  });
  c.setOption({
    backgroundColor:'transparent',
    tooltip:{trigger:'axis'},
    legend:{data:tags.slice(0,8).map(function(t){return t.tag}),textStyle:{color:'#94a3b8'},top:0},
    grid:{left:60,right:30,top:50,bottom:40},
    xAxis:{type:'category',data:tags[0]?tags[0].labels:[],axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b'}},
    yAxis:{type:'value',name:'Posts',splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b'}},
    series:series
  });
}

function renderPairs(pairs){
  var c=getChart('c-pair');
  if(!pairs||!pairs.length){
    c.setOption({title:{text:'No pairs data',left:'center',top:'center',textStyle:{color:'#64748b'}}},true);
    return;
  }
  var data=pairs.slice(0,10);
  c.setOption({
    backgroundColor:'transparent',
    tooltip:{trigger:'axis'},
    grid:{left:140,right:30,top:20,bottom:30},
    xAxis:{type:'value',splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b'}},
    yAxis:{type:'category',data:data.map(function(p){return p.pair}).reverse(),axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#94a3b8',fontSize:11}},
    series:[{type:'bar',data:data.map(function(p){return p.count}).reverse(),itemStyle:{color:'#00d4aa',borderRadius:[0,4,4,0]}}]
  });
}

window.addEventListener('resize',function(){Object.values(charts).forEach(function(c){try{if(c)c.resize();}catch(e){}})});
window.loadAll=loadAll;
loadAll();
})();
</script>
</body>
</html>'''


# ─── SPA: Sentiment & Intelligence ───────────────────────────
SENTIMENT_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sentiment & Intelligence — CITG</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0a0a1a;color:#e2e8f0;line-height:1.5}
.wrap{max-width:1400px;margin:0 auto;padding:24px}
header{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:24px}
h1{color:#00d4aa;font-size:28px;font-weight:700}
.back{color:#64748b;text-decoration:none;font-size:14px}
.back:hover{color:#00d4aa}

/* Loader */
#loader{position:fixed;inset:0;background:#0a0a1a;z-index:9999;display:flex;flex-direction:column;align-items:center;justify-content:center;transition:opacity .4s}
#loader.done{opacity:0;pointer-events:none}
.loader-ring{width:48px;height:48px;border:3px solid #1e293b;border-top-color:#00d4aa;border-radius:50%;animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.loader-text{margin-top:16px;color:#64748b;font-size:14px}

/* Period */
.period{display:flex;gap:4px;background:#0f172a;padding:4px;border-radius:10px;border:1px solid #1e293b}
.period button{background:none;border:none;color:#64748b;padding:8px 16px;border-radius:8px;font-size:13px;font-weight:500;cursor:pointer}
.period button:hover{color:#e2e8f0;background:#1e293b}
.period button.on{color:#0a0a1a;background:#00d4aa;font-weight:600}

/* Grid */
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(400px,1fr));gap:20px;margin-bottom:20px}
.grid-2{grid-template-columns:repeat(auto-fit,minmax(500px,1fr))}
.chart-box{background:#0f172a;border:1px solid #1e293b;border-radius:16px;padding:20px}
.chart-box:hover{border-color:#334155}
.chart-title{font-size:16px;font-weight:600;margin-bottom:4px;color:#00d4aa}
.chart-sub{font-size:13px;color:#64748b;margin-bottom:16px}
.chart{min-height:360px}
.full{grid-column:1/-1}

/* Alerts */
.alert-row{display:flex;align-items:center;gap:12px;padding:10px 14px;background:#0a0a1a;border-radius:8px;margin-bottom:8px;cursor:pointer;transition:.15s}
.alert-row:hover{background:#1e293b}
.alert-tag{min-width:100px;font-weight:600;color:#00d4aa;font-size:14px}
.alert-bar{flex:1;height:24px;background:#0f172a;border-radius:6px;overflow:hidden}
.alert-bar-fill{height:100%;border-radius:6px;display:flex;align-items:center;padding:0 10px;font-size:11px;font-weight:600;color:#fff;transition:width .8s}
.alert-pct{min-width:60px;text-align:right;font-size:13px;font-weight:600}
.alert-pct.up{color:#00d4aa}
.alert-pct.down{color:#f87171}

/* Correlation */
.corr-legend{display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap;font-size:12px;color:#64748b}
.corr-legend span{display:inline-block;width:16px;height:16px;border-radius:3px;margin-right:4px;vertical-align:middle}

/* Stats */
.stats-bar{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:12px;margin-bottom:20px}
.stat{background:#0f172a;border:1px solid #1e293b;border-radius:12px;padding:16px;text-align:center}
.stat-v{font-size:24px;font-weight:700;color:#00d4aa}
.stat-l{font-size:11px;color:#64748b;margin-top:4px}

/* Word list */
.word-row{display:flex;align-items:center;gap:12px;padding:8px 12px;background:#0a0a1a;border-radius:6px;margin-bottom:6px}
.word-text{min-width:120px;font-weight:600;color:#00d4aa;font-size:13px}
.word-bar{flex:1;height:20px;background:#0f172a;border-radius:4px;overflow:hidden}
.word-bar-fill{height:100%;border-radius:4px;display:flex;align-items:center;padding:0 8px;font-size:11px;font-weight:600;color:#fff}
.word-count{min-width:50px;text-align:right;color:#64748b;font-size:12px}

/* Error */
.err-box{background:#0f172a;border:1px solid #7f1d1d;border-radius:12px;padding:24px;text-align:center}
.err-box h3{color:#f87171;margin-bottom:8px}
.empty{text-align:center;color:#64748b;padding:60px;font-size:14px}
</style>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
</head>
<body>
<div id="loader"><div class="loader-ring"></div><div class="loader-text">Loading intelligence...</div></div>

<div class="wrap">
<header>
<h1>Sentiment & Intelligence</h1>
<div class="period">
<button class="on" data-d="1">1d</button>
<button data-d="3">3d</button>
<button data-d="7">7d</button>
<button data-d="30">30d</button>
</div>
<a href="/" class="back">&larr; Back</a>
</header>

<div class="stats-bar" id="top-stats"></div>

<div class="grid grid-2">
<div class="chart-box full">
<div class="chart-title">Sentiment Timeline</div>
<div class="chart-sub">Positive vs Negative vs Neutral posts per day</div>
<div class="chart" id="s-timeline"></div>
</div>

<div class="chart-box">
<div class="chart-title">News Velocity Alerts</div>
<div class="chart-sub">Tickers with anomalous mention growth</div>
<div class="chart" id="s-alerts"></div>
</div>

<div class="chart-box">
<div class="chart-title">Correlation Matrix</div>
<div class="chart-sub">Tags that appear together in posts</div>
<div class="chart" id="s-corr"></div>
</div>

<div class="chart-box full">
<div class="chart-title">Pre-Market Intelligence</div>
<div class="chart-sub">MOEX: pre-market (before 10:00 MSK) / market hours / after-hours</div>
<div class="chart" id="s-premarket"></div>
</div>
</div>
</div>

<script>
(function(){
'use strict';
var days=7;
var $=function(id){return document.getElementById(id)};

function hideLoader(){var el=$('loader');if(el&&!el.classList.contains('done'))el.classList.add('done')}
setTimeout(hideLoader,6000);

function fmt(n){return(n||0).toLocaleString('en').replace(/,/g,' ')}
function esc(t){return String(t||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}

async function api(path){
  var r=await fetch('/api'+path,{cache:'no-store',credentials:'include'});
  if(!r.ok) throw new Error('HTTP '+r.status);
  var d=await r.json();
  if(d.error) throw new Error(d.error);
  return d;
}

var charts={};
function getChart(id){
  if(!charts[id]){var el=$(id);if(!el)throw new Error('No #'+id);charts[id]=echarts.init(el,null,{renderer:'canvas'});}
  return charts[id];
}
function resetCharts(){Object.keys(charts).forEach(function(id){try{charts[id].dispose();}catch(e){}});charts={};}

// Period selector
document.querySelectorAll('.period button').forEach(function(btn){
  btn.addEventListener('click',function(){
    document.querySelectorAll('.period button').forEach(function(b){b.classList.remove('on')});
    btn.classList.add('on');
    days=parseInt(btn.dataset.d);
    loadAll();
  });
});

async function loadAll(){
  resetCharts();
  ['s-timeline','s-alerts','s-corr','s-premarket'].forEach(function(id){var el=$(id);if(el)el.innerHTML='';});
  try{
    var st=await api('/sentiment/timeline?days='+days);
    var vel=await api('/velocity/alerts?days='+days);
    var corr=await api('/correlation/matrix?days='+days);
    var pre=await api('/premarket/intel?days='+days);

    var totalPos=st.positive.reduce(function(a,b){return a+b},0);
    var totalNeg=st.negative.reduce(function(a,b){return a+b},0);
    $('top-stats').innerHTML=[
      ['Positive',fmt(totalPos)],['Negative',fmt(totalNeg)],
      ['Alerts',fmt(vel.alerts.length)],['Correlations',fmt(corr.tags.length)]
    ].map(function(s){return'<div class="stat"><div class="stat-v">'+esc(s[1])+'</div><div class="stat-l">'+s[0]+'</div></div>'}).join('');

    renderSentiment(st);
    renderAlerts(vel.alerts);
    renderCorrelation(corr);
    renderPremarket(pre);
    hideLoader();
  }catch(e){
    console.error(e);
    hideLoader();
  }
}

function renderSentiment(data){
  var c=getChart('s-timeline');
  c.setOption({
    backgroundColor:'transparent',
    tooltip:{trigger:'axis'},
    legend:{data:['Positive','Negative','Neutral'],textStyle:{color:'#94a3b8'},top:0},
    grid:{left:50,right:30,top:50,bottom:40},
    xAxis:{type:'category',data:data.days,axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b',rotate:45}},
    yAxis:{type:'value',name:'Posts',splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b'}},
    series:[
      {name:'Positive',type:'line',smooth:true,data:data.positive,itemStyle:{color:'#00d4aa'},areaStyle:{color:'#00d4aa',opacity:.1}},
      {name:'Negative',type:'line',smooth:true,data:data.negative,itemStyle:{color:'#f87171'},areaStyle:{color:'#f87171',opacity:.1}},
      {name:'Neutral',type:'line',smooth:true,data:data.neutral,itemStyle:{color:'#64748b'},areaStyle:{color:'#64748b',opacity:.05}}
    ]
  },true);
}

function renderAlerts(alerts){
  if(!alerts||!alerts.length){$('s-alerts').innerHTML='<div class="empty">No velocity alerts</div>';return;}
  var maxC=Math.max.apply(null,alerts.map(function(a){return a.pct}))||1;
  var colors=['#00d4aa','#00b894','#0984e3','#6c5ce7','#fd79a8','#e17055','#fdcb6e'];
  $('s-alerts').innerHTML=alerts.slice(0,15).map(function(a,i){
    var pct=Math.round((a.pct/maxC)*100);
    return'<div class="alert-row"><div class="alert-tag">'+esc(a.tag)+'</div><div class="alert-bar"><div class="alert-bar-fill" style="width:'+pct+'%;background:'+colors[i%colors.length]+'">'+a.current+' vs '+a.previous+'</div></div><div class="alert-pct up">+'+a.pct+'%</div></div>';
  }).join('');
}

function renderCorrelation(data){
  var c=getChart('s-corr');
  if(!data.tags||!data.tags.length){$('s-corr').innerHTML='<div class="empty">No correlation data</div>';return;}
  var n=data.tags.length;
  var heatData=[];
  for(var i=0;i<n;i++)for(var j=0;j<n;j++)if(data.matrix[i]&&data.matrix[i][j]>0)heatData.push([j,i,data.matrix[i][j]]);
  var maxVal=Math.max.apply(null,heatData.map(function(d){return d[2]}))||1;
  c.setOption({
    backgroundColor:'transparent',
    tooltip:{formatter:function(p){return data.tags[p.data[1]]+' + '+data.tags[p.data[0]]+': '+p.data[2]+' posts';}},
    grid:{left:80,right:20,top:20,bottom:80},
    xAxis:{type:'category',data:data.tags,axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b',rotate:45,fontSize:9}},
    yAxis:{type:'category',data:data.tags,axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b',fontSize:9}},
    visualMap:{min:0,max:maxVal,orient:'horizontal',left:'center',bottom:0,inRange:{color:['#0f172a','#1e293b','#00d4aa','#00b894','#fdcb6e']},textStyle:{color:'#64748b'}},
    series:[{type:'heatmap',data:heatData,label:{show:false}}]
  },true);
}

function renderPremarket(data){
  var c=getChart('s-premarket');
  c.setOption({
    backgroundColor:'transparent',
    tooltip:{trigger:'axis'},
    legend:{data:['Pre-market','Market hours','After-hours'],textStyle:{color:'#94a3b8'},top:0},
    grid:{left:50,right:30,top:50,bottom:40},
    xAxis:{type:'category',data:data.days,axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b',rotate:45}},
    yAxis:{type:'value',name:'Posts',splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b'}},
    series:[
      {name:'Pre-market',type:'bar',stack:'total',data:data.premarket,itemStyle:{color:'#fdcb6e'}},
      {name:'Market hours',type:'bar',stack:'total',data:data.market,itemStyle:{color:'#00d4aa'}},
      {name:'After-hours',type:'bar',stack:'total',data:data.afterhours,itemStyle:{color:'#6c5ce7'}}
    ]
  },true);
}

window.addEventListener('resize',function(){Object.values(charts).forEach(function(c){try{if(c)c.resize();}catch(e){}})});
loadAll();
})();
</script>
</body>
</html>'''


# ─── Shared styles for simple pages ──────────────────────────
VIRAL_SHARED_CSS = '''
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0a0a1a;color:#e2e8f0;line-height:1.5}
.wrap{max-width:1400px;margin:0 auto;padding:24px}
header{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:24px}
h1{color:#00d4aa;font-size:28px;font-weight:700}
.back{color:#64748b;text-decoration:none;font-size:14px}
.back:hover{color:#00d4aa}
.period{display:flex;gap:4px;background:#0f172a;padding:4px;border-radius:10px;border:1px solid #1e293b}
.period button{background:none;border:none;color:#64748b;padding:8px 16px;border-radius:8px;font-size:13px;font-weight:500;cursor:pointer}
.period button:hover{color:#e2e8f0;background:#1e293b}
.period button.on{color:#0a0a1a;background:#00d4aa;font-weight:600}
#loader{position:fixed;inset:0;background:#0a0a1a;z-index:9999;display:flex;flex-direction:column;align-items:center;justify-content:center;transition:opacity .4s}
#loader.done{opacity:0;pointer-events:none}
.loader-ring{width:48px;height:48px;border:3px solid #1e293b;border-top-color:#00d4aa;border-radius:50%;animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.loader-text{margin-top:16px;color:#64748b;font-size:14px}
.empty{text-align:center;color:#64748b;padding:60px;font-size:14px}
'''

# ─── Page 1: Viral Posts (Updated with channel name) ─────────
VIRAL_POSTS_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Viral Posts — CITG</title>
<style>''' + VIRAL_SHARED_CSS + '''
.vpost{background:#0f172a;border:1px solid #1e293b;border-radius:12px;padding:16px;margin-bottom:12px;cursor:pointer;transition:.15s}
.vpost:hover{border-color:#334155}
.vpost-head{display:flex;justify-content:space-between;margin-bottom:8px;font-size:12px;color:#64748b}
.vpost-ch{color:#00d4aa;font-weight:600;font-size:12px;margin-right:8px}
.vpost-body{color:#e2e8f0;white-space:pre-wrap;word-break:break-word;max-height:80px;overflow:hidden;line-height:1.5;font-size:14px}
.vpost-stats{display:flex;gap:20px;margin-top:10px;font-size:13px;color:#64748b}
.vpost-stats span{color:#00d4aa;font-weight:600}
</style>
</head>
<body>
<div id="loader"><div class="loader-ring"></div><div class="loader-text">Loading...</div></div>
<div class="wrap">
<header><h1>Viral Posts</h1>
<div class="period">
<button class="on" data-d="1">1d</button>
<button data-d="3">3d</button>
<button data-d="7">7d</button>
<button data-d="30">30d</button>
</div>
<a href="/" class="back">&larr; Back</a>
</header>
<div id="content"></div>
</div>
<script>
(function(){
var days=7,$=function(id){return document.getElementById(id)};
function hideLoader(){var el=$('loader');if(el&&!el.classList.contains('done'))el.classList.add('done')}
setTimeout(hideLoader,5000);
function esc(t){return String(t||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function fmt(n){return(n||0).toLocaleString('en').replace(/,/g,' ')}

async function load(){
  $('content').innerHTML='<div style="text-align:center;color:#64748b;padding:40px">Loading...</div>';
  try{
    var r=await fetch('/api/viral/posts?days='+days+'&limit=10',{cache:'no-store'});
    if(!r.ok) throw new Error('HTTP '+r.status);
    var d=await r.json();
    if(d.error) throw new Error(d.error);
    var posts=d.posts||[];
    if(!posts.length){$('content').innerHTML='<div class="empty">No viral posts</div>';hideLoader();return;}
    $('content').innerHTML=posts.map(function(p,i){
      var ch=p.channel_username||'unknown';
      var link=(p.channel_type==='private'&&p.numeric_id)?'https://t.me/c/'+p.numeric_id+'/'+p.id:'https://t.me/'+ch+'/'+p.id;
      return'<div class="vpost" onclick="window.open(\''+link+'\')">'+
        '<div class="vpost-head"><span>#'+(i+1)+'</span><span class="vpost-ch">@'+esc(ch)+'</span><span>'+(p.published?p.published.slice(0,16).replace('T',' '):'')+'</span></div>'+
        '<div class="vpost-body">'+esc((p.text||'(no text)').slice(0,250))+'</div>'+
        '<div class="vpost-stats"><span>Views '+fmt(p.views)+'</span><span>Forwards '+fmt(p.forwards)+'</span></div></div>';
    }).join('');
  }catch(e){$('content').innerHTML='<div style="text-align:center;color:#f87171;padding:40px">Error: '+esc(e.message)+'</div>';}
  hideLoader();
}

document.querySelectorAll('.period button').forEach(function(btn){
  btn.addEventListener('click',function(){
    document.querySelectorAll('.period button').forEach(function(b){b.classList.remove('on')});
    btn.classList.add('on');days=parseInt(btn.dataset.d);load();
  });
});
load();
})();
</script>
</body>
</html>'''

# ─── Page 2: Sector Rotation ─────────────────────────────────
SECTORS_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sector Rotation — CITG</title>
<style>''' + VIRAL_SHARED_CSS + '''
.sector-row{display:flex;align-items:center;gap:12px;padding:14px 16px;background:#0f172a;border:1px solid #1e293b;border-radius:12px;margin-bottom:10px}
.sector-row:hover{border-color:#334155}
.sector-name{min-width:160px;font-weight:600;color:#00d4aa;font-size:15px}
.sector-bar{flex:1;height:32px;background:#0a0a1a;border-radius:8px;overflow:hidden}
.sector-bar-fill{height:100%;border-radius:8px;display:flex;align-items:center;padding:0 12px;font-size:12px;font-weight:600;color:#fff;transition:width .8s}
.sector-count{min-width:70px;text-align:right;color:#64748b;font-size:13px}
</style>
</head>
<body>
<div id="loader"><div class="loader-ring"></div><div class="loader-text">Loading...</div></div>
<div class="wrap">
<header><h1>Sector Rotation</h1>
<div class="period">
<button class="on" data-d="1">1d</button>
<button data-d="3">3d</button>
<button data-d="7">7d</button>
<button data-d="30">30d</button>
</div>
<a href="/" class="back">&larr; Back</a>
</header>
<div id="content"></div>
</div>
<script>
(function(){
var days=7,$=function(id){return document.getElementById(id)};
function hideLoader(){var el=$('loader');if(el&&!el.classList.contains('done'))el.classList.add('done')}
setTimeout(hideLoader,5000);
function esc(t){return String(t||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function fmt(n){return(n||0).toLocaleString('en').replace(/,/g,' ')}

async function load(){
  $('content').innerHTML='<div style="text-align:center;color:#64748b;padding:40px">Loading...</div>';
  try{
    var r=await fetch('/api/sector/rotation?days='+days,{cache:'no-store'});
    if(!r.ok) throw new Error('HTTP '+r.status);
    var d=await r.json();
    if(d.error) throw new Error(d.error);
    var sectors=d.sectors||[],total=d.total||1;
    if(!sectors.length){$('content').innerHTML='<div class="empty">No sector data</div>';hideLoader();return;}
    var maxC=Math.max.apply(null,sectors.map(function(s){return s.count}))||1;
    var colors=['#00d4aa','#00b894','#0984e3','#6c5ce7','#fd79a8','#e17055','#fdcb6e','#55efc4','#00cec9','#81ecec'];
    $('content').innerHTML=sectors.map(function(s,i){
      var pct=Math.round((s.count/maxC)*100);
      return'<div class="sector-row"><div class="sector-name">'+esc(s.name)+'</div><div class="sector-bar"><div class="sector-bar-fill" style="width:'+pct+'%;background:'+colors[i%colors.length]+'">'+fmt(s.count)+'</div></div><div class="sector-count">'+Math.round((s.count/total)*100)+'%</div></div>';
    }).join('');
  }catch(e){$('content').innerHTML='<div style="text-align:center;color:#f87171;padding:40px">Error: '+esc(e.message)+'</div>';}
  hideLoader();
}

document.querySelectorAll('.period button').forEach(function(btn){
  btn.addEventListener('click',function(){
    document.querySelectorAll('.period button').forEach(function(b){b.classList.remove('on')});
    btn.classList.add('on');days=parseInt(btn.dataset.d);load();
  });
});
load();
})();
</script>
</body>
</html>'''

# ─── Page 3: Word Cloud ──────────────────────────────────────
WORDCLOUD_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Word Cloud — CITG</title>
<style>''' + VIRAL_SHARED_CSS + '''
.cloud{display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:center;min-height:400px;padding:30px}
.cloud-tag{padding:10px 20px;border-radius:24px;font-weight:600;cursor:pointer;transition:transform .2s,opacity .2s;opacity:.85}
.cloud-tag:hover{transform:scale(1.12);opacity:1}
</style>
</head>
<body>
<div id="loader"><div class="loader-ring"></div><div class="loader-text">Loading...</div></div>
<div class="wrap">
<header><h1>Word Cloud</h1>
<div class="period">
<button class="on" data-d="1">1d</button>
<button data-d="3">3d</button>
<button data-d="7">7d</button>
<button data-d="30">30d</button>
</div>
<a href="/" class="back">&larr; Back</a>
</header>
<div id="content" class="cloud"></div>
</div>
<script>
(function(){
var days=7,$=function(id){return document.getElementById(id)};
function hideLoader(){var el=$('loader');if(el&&!el.classList.contains('done'))el.classList.add('done')}
setTimeout(hideLoader,5000);
function esc(t){return String(t||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}

async function load(){
  $('content').innerHTML='<div style="text-align:center;color:#64748b;padding:40px">Loading...</div>';
  try{
    var r=await fetch('/api/wordcloud?days='+days+'&limit=80',{cache:'no-store'});
    if(!r.ok) throw new Error('HTTP '+r.status);
    var d=await r.json();
    if(d.error) throw new Error(d.error);
    var words=d.words||[];
    if(!words.length){$('content').innerHTML='<div class="empty">No word data</div>';hideLoader();return;}
    var maxC=words[0].count||1;
    var colors=['#00d4aa','#00b894','#0984e3','#6c5ce7','#fd79a8','#e17055','#fdcb6e','#55efc4','#00cec9','#81ecec'];
    $('content').innerHTML=words.map(function(w,i){
      var size=12+Math.round((w.count/maxC)*30);
      return'<span class="cloud-tag" style="font-size:'+size+'px;background:'+colors[i%colors.length]+'20;color:'+colors[i%colors.length]+';border:1px solid '+colors[i%colors.length]+'40">'+esc(w.text)+'</span>';
    }).join('');
  }catch(e){$('content').innerHTML='<div style="text-align:center;color:#f87171;padding:40px">Error: '+esc(e.message)+'</div>';}
  hideLoader();
}

document.querySelectorAll('.period button').forEach(function(btn){
  btn.addEventListener('click',function(){
    document.querySelectorAll('.period button').forEach(function(b){b.classList.remove('on')});
    btn.classList.add('on');days=parseInt(btn.dataset.d);load();
  });
});
load();
})();
</script>
</body>
</html>'''

# ─── Page 4: Cross-Market ────────────────────────────────────
CROSSMARKET_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cross-Market — CITG</title>
<style>''' + VIRAL_SHARED_CSS + '''
.xpost{background:#0f172a;border:1px solid #1e293b;border-radius:12px;padding:14px;margin-bottom:10px;cursor:pointer;transition:.15s}
.xpost:hover{border-color:#334155}
.xpost-head{display:flex;justify-content:space-between;margin-bottom:6px;font-size:12px;color:#64748b}
.xpost-body{color:#e2e8f0;white-space:pre-wrap;word-break:break-word;max-height:60px;overflow:hidden;font-size:14px;line-height:1.5}
.xticker{display:inline-block;background:#1e293b;color:#00d4aa;padding:4px 12px;border-radius:16px;font-size:13px;font-weight:600;margin:4px}
</style>
</head>
<body>
<div id="loader"><div class="loader-ring"></div><div class="loader-text">Loading...</div></div>
<div class="wrap">
<header><h1>Cross-Market</h1>
<div class="period">
<button class="on" data-d="1">1d</button>
<button data-d="3">3d</button>
<button data-d="7">7d</button>
<button data-d="30">30d</button>
</div>
<a href="/" class="back">&larr; Back</a>
</header>
<div id="content"></div>
<div id="tickers" style="margin-top:16px"></div>
</div>
<script>
(function(){
var days=7,$=function(id){return document.getElementById(id)};
function hideLoader(){var el=$('loader');if(el&&!el.classList.contains('done'))el.classList.add('done')}
setTimeout(hideLoader,5000);
function esc(t){return String(t||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function fmt(n){return(n||0).toLocaleString('en').replace(/,/g,' ')}

async function load(){
  $('content').innerHTML='<div style="text-align:center;color:#64748b;padding:40px">Loading...</div>';
  $('tickers').innerHTML='';
  try{
    var r=await fetch('/api/crossmarket/links?days='+days,{cache:'no-store'});
    if(!r.ok) throw new Error('HTTP '+r.status);
    var d=await r.json();
    if(d.error) throw new Error(d.error);
    var posts=d.posts||[],tickers=d.tickers||[];
    if(!posts.length){$('content').innerHTML='<div class="empty">No cross-market posts</div>';hideLoader();return;}
    $('content').innerHTML=posts.slice(0,10).map(function(p){
      var ch=p.channel_username||'unknown';
      var link=(p.channel_type==='private'&&p.numeric_id)?'https://t.me/c/'+p.numeric_id+'/'+p.id:'https://t.me/'+ch+'/'+p.id;
      return'<div class="xpost" onclick="window.open(\''+link+'\')">'+
        '<div class="xpost-head"><span>Views '+fmt(p.views)+' | '+(p.published?p.published.slice(0,16).replace('T',' '):'')+' | @'+esc(ch)+'</span></div>'+
        '<div class="xpost-body">'+esc((p.text||'(no text)').slice(0,250))+'</div></div>';
    }).join('');
    if(tickers.length){
      $('tickers').innerHTML='<div style="color:#64748b;font-size:13px;margin-bottom:10px">Co-mentioned tickers:</div>'+
        tickers.map(function(t){return'<span class="xticker">'+esc(t.tag)+' ('+t.count+')</span>';}).join('');
    }
  }catch(e){$('content').innerHTML='<div style="text-align:center;color:#f87171;padding:40px">Error: '+esc(e.message)+'</div>';}
  hideLoader();
}

document.querySelectorAll('.period button').forEach(function(btn){
  btn.addEventListener('click',function(){
    document.querySelectorAll('.period button').forEach(function(b){b.classList.remove('on')});
    btn.classList.add('on');days=parseInt(btn.dataset.d);load();
  });
});
load();
})();
</script>
</body>
</html>'''


# ═══════════════════════════════════════════════════════════════
# NEW HTML TEMPLATES (v2 — channel-aware)
# ═══════════════════════════════════════════════════════════════

# ─── Page: Channels List ─────────────────────────────────────
CHANNELS_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Channels — CITG</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0a0a1a;color:#e2e8f0;line-height:1.5}
.wrap{max-width:1400px;margin:0 auto;padding:24px}
header{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:24px}
h1{color:#00d4aa;font-size:28px;font-weight:700}
.back{color:#64748b;text-decoration:none;font-size:14px}
.back:hover{color:#00d4aa}

#loader{position:fixed;inset:0;background:#0a0a1a;z-index:9999;display:flex;flex-direction:column;align-items:center;justify-content:center;transition:opacity .4s}
#loader.done{opacity:0;pointer-events:none}
.loader-ring{width:48px;height:48px;border:3px solid #1e293b;border-top-color:#00d4aa;border-radius:50%;animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.loader-text{margin-top:16px;color:#64748b;font-size:14px}

.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:15px;margin-bottom:24px}
.stat{background:#0f172a;border:1px solid #1e293b;border-radius:12px;padding:20px;text-align:center}
.stat-v{font-size:28px;font-weight:700;color:#00d4aa}
.stat-l{font-size:12px;color:#64748b;margin-top:4px}

.ch-row{display:flex;align-items:center;gap:16px;padding:16px;background:#0f172a;border:1px solid #1e293b;border-radius:12px;margin-bottom:10px;transition:.15s}
.ch-row:hover{border-color:#334155}
.ch-info{flex:1}
.ch-title{font-size:15px;font-weight:600;color:#e2e8f0}
.ch-user{font-size:12px;color:#64748b;margin-top:2px}
.ch-meta{display:flex;gap:16px;font-size:12px;color:#64748b}
.ch-meta span{color:#00d4aa;font-weight:600}
.badge{display:inline-block;padding:4px 10px;border-radius:12px;font-size:11px;font-weight:600}
.badge-active{background:#00d4aa20;color:#00d4aa;border:1px solid #00d4aa40}
.badge-inactive{background:#f8717120;color:#f87171;border:1px solid #f8717140}
.badge-error{background:#fdcb6e20;color:#fdcb6e;border:1px solid #fdcb6e40}
.empty{text-align:center;color:#64748b;padding:60px;font-size:14px}
.err{background:#0f172a;border:1px solid #7f1d1d;border-radius:12px;padding:24px;text-align:center}
.err h3{color:#f87171;margin-bottom:8px}

/* Add channel form */
.add-form{display:flex;gap:10px;margin-bottom:24px;flex-wrap:wrap}
.add-form input{flex:1;min-width:200px;background:#0f172a;border:1px solid #1e293b;color:#e2e8f0;padding:12px 16px;border-radius:10px;font-size:14px;outline:none}
.add-form input:focus{border-color:#00d4aa}
.add-form button{background:#00d4aa;color:#0a0a1a;border:none;padding:12px 28px;border-radius:10px;font-size:14px;font-weight:600;cursor:pointer}
.add-form button:hover{opacity:.85}
.add-form button:disabled{opacity:.5;cursor:not-allowed}
.add-msg{font-size:13px;margin-top:8px;min-height:20px}
.add-msg.ok{color:#00d4aa}
.add-msg.err{color:#f87171}

/* Channel actions */
.ch-actions{display:flex;gap:8px}
.ch-actions button{background:#1e293b;border:1px solid #334155;color:#e2e8f0;padding:6px 14px;border-radius:6px;font-size:12px;cursor:pointer;transition:.15s}
.ch-actions button:hover{background:#334155}
.ch-actions .btn-del{color:#f87171;border-color:#7f1d1d}
.ch-actions .btn-del:hover{background:#7f1d1d}
</style>
</head>
<body>
<div id="loader"><div class="loader-ring"></div><div class="loader-text">Loading channels...</div></div>
<div class="wrap">
<header><h1>Channels</h1><a href="/" class="back">&larr; Back</a></header>

<!-- Add channel form + Parse trigger -->
<div class="add-form">
<input type="text" id="ch-input" placeholder="Username или numeric ID канала..." onkeydown="if(event.key==='Enter')addChannel()">
<button id="ch-add-btn" onclick="addChannel()">+ Добавить канал</button>
<button id="parse-btn" onclick="triggerParse()" style="background:#0984e3;color:#fff;margin-left:8px">🔄 Запустить парсинг</button>
<button id="clear-btn" onclick="clearAll()" style="background:#7f1d1d;color:#fff;margin-left:8px">🗑 Очистить все</button>
</div>
<div class="add-msg" id="add-msg"></div>
<div class="add-msg" id="parse-status" style="color:#64748b;font-size:12px;margin-top:4px"></div>

<div class="stats" id="top-stats">
<div class="stat"><div class="stat-v" id="s-total">-</div><div class="stat-l">Total</div></div>
<div class="stat"><div class="stat-v" id="s-active">-</div><div class="stat-l">Active</div></div>
<div class="stat"><div class="stat-v" id="s-posts">-</div><div class="stat-l">Total Posts</div></div>
<div class="stat"><div class="stat-v" id="s-errors">-</div><div class="stat-l">With Errors</div></div>
</div>

<div id="content"></div>
</div>

<script>
(function(){
var $=function(id){return document.getElementById(id)};
function hideLoader(){var el=$('loader');if(el&&!el.classList.contains('done'))el.classList.add('done')}
setTimeout(hideLoader,6000);
function esc(t){return String(t||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function fmt(n){return(n||0).toLocaleString('en').replace(/,/g,' ')}

async function api(path){
  var r=await fetch('/api'+path,{cache:'no-store',credentials:'include'});
  if(!r.ok) throw new Error('HTTP '+r.status);
  var d=await r.json();
  if(d.error) throw new Error(d.error);
  return d;
}

async function loadAll(){
  try{
    var data=await api('/channels');
    var chs=data.channels||[];
    if(!chs.length){$('content').innerHTML='<div class="empty">No channels configured</div>';hideLoader();return;}

    var total=chs.length;
    var active=chs.filter(function(c){return c.is_active}).length;
    var posts=chs.reduce(function(a,c){return a+(c.posts_count||0)},0);
    var errors=chs.filter(function(c){return c.parse_error_count>0}).length;

    $('s-total').textContent=fmt(total);
    $('s-active').textContent=fmt(active);
    $('s-posts').textContent=fmt(posts);
    $('s-errors').textContent=fmt(errors);

    $('content').innerHTML=chs.map(function(c){
      var badgeClass=c.is_active?'badge-active':'badge-inactive';
      var badgeText=c.is_active?'active':'inactive';
      if(c.parse_error_count>0){badgeClass='badge-error';badgeText='error('+c.parse_error_count+')';}
      var lastParsed=c.last_parsed_at?c.last_parsed_at.slice(0,16).replace('T',' '):'never';
      var chLink=(c.channel_type==='private'&&c.numeric_id)?'https://t.me/c/'+c.numeric_id:c.username?'https://t.me/'+c.username:'#';
      var toggleLabel=c.is_active?'Stop':'Start';
      return'<div class="ch-row" id="ch-'+c.id+'">'+
        '<div class="ch-info">'+
          '<div class="ch-title"><a href="'+chLink+'" target="_blank" style="color:inherit;text-decoration:none">'+esc(c.title||c.username||'Channel #'+c.id)+'</a></div>'+
          '<div class="ch-user">'+(c.channel_type==='private'?'c/'+c.numeric_id:'@'+esc(c.username||'-'))+' | tid:'+c.telegram_id+'</div>'+
          '<div class="ch-meta">'+
            '<span>'+fmt(c.posts_count||0)+' posts</span>'+
            '<span>'+fmt(c.subscriber_count||0)+' subs</span>'+
            '<span>parsed: '+lastParsed+'</span>'+
            (c.last_error_message?'<span style="color:#f87171" title="'+esc(c.last_error_message)+'">error</span>':'')+
          '</div>'+
        '</div>'+
        '<span class="badge '+badgeClass+'">'+badgeText+'</span>'+
        '<div class="ch-actions">'+
          '<button onclick="toggleChannel('+c.id+')">'+toggleLabel+'</button>'+
          '<button class="btn-del" onclick="deleteChannel('+c.id+')">Delete</button>'+
        '</div>'+
      '</div>';
    }).join('');
    hideLoader();
  }catch(e){
    console.error(e);
    $('content').innerHTML='<div class="err"><h3>Error</h3><p>'+esc(e.message)+'</p></div>';
    hideLoader();
  }
}

// Add channel
function _extractChannelId(raw){
  raw=raw.trim();
  // Full URL: https://t.me/c/3147415698/1997 or https://t.me/markettwits
  var m=raw.match(/t\.me\/(?:c\/)?([^\/]+)/);
  if(m)return m[1];
  // web.telegram.org: https://web.telegram.org/a/#-1003147415698 or #-740684703
  m=raw.match(/web\.telegram\.org\/a\/#(-?\d+)/);
  if(m)return m[1];
  // Bare number (positive user ID, negative group ID, or -100... channel ID)
  if(/^-?\d+$/.test(raw))return raw;
  // @username
  return raw.replace(/^@/,'');
}

async function addChannel(){
  var input=$('ch-input');
  var btn=$('ch-add-btn');
  var msg=$('add-msg');
  var id=_extractChannelId(input.value);
  if(!id){msg.textContent='Введите username, numeric ID или ссылку на канал';msg.className='add-msg err';return;}
  btn.disabled=true;
  msg.textContent='Добавление...';msg.className='add-msg';
  try{
    var data=await api('/channel/add?identifier='+encodeURIComponent(id));
    if(data.success){
      msg.textContent='Канал добавлен: '+(data.channel.title||data.channel.username||data.channel.numeric_id);msg.className='add-msg ok';
      input.value='';
      loadAll();
    }else{
      msg.textContent='Ошибка: '+(data.error||'unknown');msg.className='add-msg err';
    }
  }catch(e){
    msg.textContent='Ошибка: '+(e.message||e);msg.className='add-msg err';
  }finally{
    btn.disabled=false;
  }
}

// Toggle channel active
// ─── Live Parse Progress ────────────────────────────────────
var _parsePollInterval=null;

function _renderProgress(state){
  var el=$('parse-status');
  if(!state.running){
    if(state.finished_at){
      var done=state.channels_done||0;
      var posts=state.posts_new_total||0;
      el.innerHTML='<span style="color:#00d4aa">Complete: '+done+' channels, +'+posts+' posts</span>';
    }else{
      el.innerHTML='';
    }
    return;
  }
  var total=state.channels_total||0;
  var done=state.channels_done||0;
  var posts=state.posts_new_total||0;
  var current=state.current_channel||'...';
  var op=state.current_operation||'Working...';
  var pct=total>0?Math.round(done/total*100):0;
  
  el.innerHTML=
    '<div style="margin-top:10px;padding:12px;background:#0f172a;border:1px solid #1e293b;border-radius:10px">'+
      '<div style="display:flex;justify-content:space-between;margin-bottom:6px;font-size:13px">'+
        '<span style="color:#94a3b8">'+esc(op)+'</span>'+
        '<span style="color:#00d4aa;font-weight:600">'+done+'/'+total+' ('+pct+'%)</span>'+
      '</div>'+
      '<div style="background:#1e293b;height:6px;border-radius:3px;overflow:hidden">'+
        '<div style="background:#00d4aa;height:100%;width:'+pct+'%;transition:width .3s"></div>'+
      '</div>'+
      '<div style="margin-top:6px;font-size:12px;color:#64748b">'+
        'Posts: +'+posts+' new'+(current?' | Current: '+esc(current):'')+
      '</div>'+
    '</div>';
}

async function _pollParseStatus(){
  try{
    var data=await api('/parse/status');
    var state=data.state||{};
    _renderProgress(state);
    if(!state.running&&state.finished_at){
      clearInterval(_parsePollInterval);
      _parsePollInterval=null;
      $('parse-btn').disabled=false;
      loadAll();
      return;
    }
  }catch(e){console.error('poll error',e);}
}

async function triggerParse(){
  var btn=$('parse-btn');
  btn.disabled=true;
  _renderProgress({running:true,channels_total:0,channels_done:0,current_operation:'Starting...'});
  try{
    var data=await api('/parse/trigger');
    if(data.success){
      if(_parsePollInterval) clearInterval(_parsePollInterval);
      _parsePollInterval=setInterval(_pollParseStatus,3000);
      _pollParseStatus();
    }else{
      _renderProgress({running:false,error:data.error});
      btn.disabled=false;
    }
  }catch(e){
    $('parse-status').innerHTML='<span style="color:#f87171">Error: '+esc(e.message||'')+'</span>';
    btn.disabled=false;
  }
}

async function toggleChannel(id){
  try{
    var data=await api('/channel/toggle/'+id);
    if(data.success) loadAll();
  }catch(e){console.error(e);}
}

// Delete channel
async function deleteChannel(id){
  if(!confirm('Удалить канал и все его посты?')) return;
  try{
    var r=await fetch('/api/channel/delete/'+id,{method:'DELETE',cache:'no-store',credentials:'include'});
    var data=await r.json();
    if(data.success){var el=$('ch-'+id);if(el)el.remove();}
  }catch(e){console.error(e);}
}

async function clearAll(){
  if(!confirm('ВНИМАНИЕ: Это удалит ВСЕ каналы, посты, логи и ошибки из базы. Продолжить?')) return;
  // Блокируем кнопку на время запроса (фикс 1: двойное нажатие)
  var btn=$('clear-btn');
  if(btn.disabled) return;
  btn.disabled=true;
  btn.textContent='⏳ Очищаем...';
  $('parse-status').textContent='Проверка...';
  try{
    // Проверяем — не идёт ли парсинг (фикс 2: race condition)
    var statusR=await fetch('/api/parse/status',{cache:'no-store',credentials:'include'});
    var statusData=await statusR.json();
    if(statusData.state && statusData.state.running){
      $('parse-status').textContent='❌ Нельзя очистить — идёт парсинг. Дождитесь завершения.';
      btn.disabled=false;
      btn.textContent='🗑 Очистить все';
      return;
    }
    $('parse-status').textContent='Очистка базы...';
    var r=await fetch('/api/channel/clear-all',{method:'POST',cache:'no-store',credentials:'include'});
    var data=await r.json();
    if(!data.success){$('parse-status').textContent='❌ Ошибка: '+(data.error||'unknown'); btn.disabled=false; btn.textContent='🗑 Очистить все'; return;}
    // Poll status every 2 seconds
    var poll=setInterval(async function(){
      try{var s=await fetch('/api/clear-all/status',{cache:'no-store',credentials:'include'});
        var st=await s.json();
        if(!st.state.running){clearInterval(poll); $('parse-status').textContent='✅ База очищена. Перезагрузка...'; setTimeout(function(){location.reload();}, 1500);}
        else{$('parse-status').textContent='⏳ Очищаем...';}
      }catch(e){clearInterval(poll); $('parse-status').textContent='❌ Ошибка проверки статуса'; btn.disabled=false; btn.textContent='🗑 Очистить все';}
    }, 2000);
  }catch(e){$('parse-status').textContent='❌ Ошибка сети: '+e; console.error(e); btn.disabled=false; btn.textContent='🗑 Очистить все';}
}

// Export functions for onclick handlers
window.addChannel=addChannel;
window.triggerParse=triggerParse;
window.toggleChannel=toggleChannel;
window.deleteChannel=deleteChannel;
window.clearAll=clearAll;

loadAll();
})();
</script>
</body>
</html>'''

# ─── Page: Cross-Channel Comparison ──────────────────────────
CROSSCHANNEL_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cross-Channel — CITG</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0a0a1a;color:#e2e8f0;line-height:1.5}
.wrap{max-width:1400px;margin:0 auto;padding:24px}
header{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:24px}
h1{color:#00d4aa;font-size:28px;font-weight:700}
.back{color:#64748b;text-decoration:none;font-size:14px}
.back:hover{color:#00d4aa}
.period{display:flex;gap:4px;background:#0f172a;padding:4px;border-radius:10px;border:1px solid #1e293b}
.period button{background:none;border:none;color:#64748b;padding:8px 16px;border-radius:8px;font-size:13px;font-weight:500;cursor:pointer}
.period button:hover{color:#e2e8f0;background:#1e293b}
.period button.on{color:#0a0a1a;background:#00d4aa;font-weight:600}

#loader{position:fixed;inset:0;background:#0a0a1a;z-index:9999;display:flex;flex-direction:column;align-items:center;justify-content:center;transition:opacity .4s}
#loader.done{opacity:0;pointer-events:none}
.loader-ring{width:48px;height:48px;border:3px solid #1e293b;border-top-color:#00d4aa;border-radius:50%;animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.loader-text{margin-top:16px;color:#64748b;font-size:14px}

.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(500px,1fr));gap:20px;margin-bottom:20px}
.chart-box{background:#0f172a;border:1px solid #1e293b;border-radius:16px;padding:20px}
.chart-title{font-size:16px;font-weight:600;margin-bottom:4px;color:#00d4aa}
.chart-sub{font-size:13px;color:#64748b;margin-bottom:16px}
.chart{min-height:360px}
.full{grid-column:1/-1}

.stats-bar{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:12px;margin-bottom:20px}
.stat{background:#0f172a;border:1px solid #1e293b;border-radius:12px;padding:16px;text-align:center}
.stat-v{font-size:24px;font-weight:700;color:#00d4aa}
.stat-l{font-size:11px;color:#64748b;margin-top:4px}
.empty{text-align:center;color:#64748b;padding:60px;font-size:14px}
.err-box{background:#0f172a;border:1px solid #7f1d1d;border-radius:12px;padding:24px;text-align:center}
.err-box h3{color:#f87171;margin-bottom:8px}
</style>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
</head>
<body>
<div id="loader"><div class="loader-ring"></div><div class="loader-text">Loading cross-channel data...</div></div>
<div class="wrap">
<header>
<h1>Cross-Channel Comparison</h1>
<div class="period">
<button class="on" data-d="7">7d</button>
<button data-d="30">30d</button>
<button data-d="90">90d</button>
</div>
<a href="/" class="back">&larr; Back</a>
</header>

<div class="stats-bar" id="top-stats"></div>

<div class="grid">
<div class="chart-box full">
<div class="chart-title">Posts per Channel</div>
<div class="chart-sub">Number of posts per channel</div>
<div class="chart" id="ch-posts-chart"></div>
</div>

<div class="chart-box full">
<div class="chart-title">Total Views per Channel</div>
<div class="chart-sub">Cumulative views per channel</div>
<div class="chart" id="ch-views-chart"></div>
</div>

<div class="chart-box full">
<div class="chart-title">Avg Views per Post</div>
<div class="chart-sub">Average reach per channel</div>
<div class="chart" id="ch-avg-chart"></div>
</div>

<div class="chart-box full">
<div class="chart-title">Posts Timeline by Channel</div>
<div class="chart-sub">Daily post count per channel</div>
<div class="chart" id="ch-timeline-chart"></div>
</div>
</div>
</div>

<script>
(function(){
'use strict';
var days=7,charts={};
var $=function(id){return document.getElementById(id)};

function hideLoader(){var el=$('loader');if(el&&!el.classList.contains('done'))el.classList.add('done')}
setTimeout(hideLoader,6000);

function fmt(n){return(n||0).toLocaleString('en').replace(/,/g,' ')}
function esc(t){return String(t||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}

async function api(path){
  var r=await fetch('/api'+path,{cache:'no-store',credentials:'include'});
  if(!r.ok) throw new Error('HTTP '+r.status);
  var d=await r.json();
  if(d.error) throw new Error(d.error);
  return d;
}

function getChart(id){
  if(!charts[id]){
    var el=$(id);
    if(!el) throw new Error('No #'+id);
    charts[id]=echarts.init(el,null,{renderer:'canvas'});
  }
  return charts[id];
}
function resetCharts(){
  Object.keys(charts).forEach(function(id){try{charts[id].dispose();}catch(e){}});
  charts={};
}

document.querySelectorAll('.period button').forEach(function(btn){
  btn.addEventListener('click',function(){
    document.querySelectorAll('.period button').forEach(function(b){b.classList.remove('on')});
    btn.classList.add('on');
    days=parseInt(btn.dataset.d);
    loadAll();
  });
});

async function loadAll(){
  resetCharts();
  ['ch-posts-chart','ch-views-chart','ch-avg-chart','ch-timeline-chart'].forEach(function(id){var el=$(id);if(el)el.innerHTML='';});
  try{
    var data=await api('/channels/comparison?days='+days);
    var chs=data.channels||[];
    if(!chs.length){$('top-stats').innerHTML='<div class="empty">No channel data</div>';hideLoader();return;}

    var totalPosts=chs.reduce(function(a,c){return a+(c.posts_count||0)},0);
    var totalViews=chs.reduce(function(a,c){return a+(c.total_views||0)},0);
    $('top-stats').innerHTML=[
      ['Channels',chs.length],['Total Posts',fmt(totalPosts)],['Total Views',fmt(totalViews)]
    ].map(function(s){return'<div class="stat"><div class="stat-v">'+esc(s[1])+'</div><div class="stat-l">'+s[0]+'</div></div>'}).join('');

    var colors=['#00d4aa','#00b894','#0984e3','#6c5ce7','#fd79a8','#e17055','#fdcb6e','#55efc4','#00cec9','#81ecec','#a29bfe','#fab1a0'];

    // Posts per channel
    getChart('ch-posts-chart').setOption({
      backgroundColor:'transparent',
      tooltip:{trigger:'axis',formatter:function(p){return p[0].name+': '+fmt(p[0].value)+' posts';}},
      grid:{left:120,right:30,top:20,bottom:30},
      xAxis:{type:'value',splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b'}},
      yAxis:{type:'category',data:chs.map(function(c){return c.username||'ch-'+c.id}).reverse(),axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#94a3b8',fontSize:12}},
      series:[{type:'bar',data:chs.map(function(c){return c.posts_count||0}).reverse(),itemStyle:{color:function(p){return colors[p.dataIndex%colors.length]},borderRadius:[0,4,4,0]}}]
    },true);

    // Views per channel
    getChart('ch-views-chart').setOption({
      backgroundColor:'transparent',
      tooltip:{trigger:'axis',formatter:function(p){return p[0].name+': '+fmt(p[0].value)+' views';}},
      grid:{left:120,right:30,top:20,bottom:30},
      xAxis:{type:'value',splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b',formatter:function(v){return v>=1000?(v/1000).toFixed(0)+'k':v;}}},
      yAxis:{type:'category',data:chs.map(function(c){return c.username||'ch-'+c.id}).reverse(),axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#94a3b8',fontSize:12}},
      series:[{type:'bar',data:chs.map(function(c){return c.total_views||0}).reverse(),itemStyle:{color:function(p){return colors[p.dataIndex%colors.length]},borderRadius:[0,4,4,0]}}]
    },true);

    // Avg views per channel
    getChart('ch-avg-chart').setOption({
      backgroundColor:'transparent',
      tooltip:{trigger:'axis',formatter:function(p){return p[0].name+': '+fmt(p[0].value)+' avg views';}},
      grid:{left:120,right:30,top:20,bottom:30},
      xAxis:{type:'value',splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b'}},
      yAxis:{type:'category',data:chs.map(function(c){return c.username||'ch-'+c.id}).reverse(),axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#94a3b8',fontSize:12}},
      series:[{type:'bar',data:chs.map(function(c){return c.avg_views||0}).reverse(),itemStyle:{color:function(p){return colors[p.dataIndex%colors.length]},borderRadius:[0,4,4,0]}}]
    },true);

    // Timeline
    if(data.timeline && data.timeline.days && data.timeline.days.length){
      var series=data.timeline.channels.map(function(ch,idx){
        return{name:ch.username||'ch-'+ch.id,type:'line',smooth:true,symbol:'none',lineStyle:{width:2,color:colors[idx%colors.length]},areaStyle:{color:colors[idx%colors.length],opacity:.1},data:ch.series};
      });
      getChart('ch-timeline-chart').setOption({
        backgroundColor:'transparent',
        tooltip:{trigger:'axis'},
        legend:{data:data.timeline.channels.map(function(c){return c.username||'ch-'+c.id}),textStyle:{color:'#94a3b8'},top:0},
        grid:{left:60,right:30,top:50,bottom:40},
        xAxis:{type:'category',data:data.timeline.days,axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b',rotate:45}},
        yAxis:{type:'value',name:'Posts',splitLine:{lineStyle:{color:'#1e293b'}},axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#64748b'}},
        series:series
      },true);
    }else{
      $('ch-timeline-chart').innerHTML='<div class="empty">No timeline data</div>';
    }

    hideLoader();
  }catch(e){
    console.error(e);
    $('top-stats').innerHTML='<div class="err-box"><h3>Error</h3><p>'+esc(e.message)+'</p></div>';
    hideLoader();
  }
}

window.addEventListener('resize',function(){Object.values(charts).forEach(function(c){try{if(c)c.resize();}catch(e){}})});
loadAll();
})();
</script>
</body>
</html>'''


# ═══════════════════════════════════════════════════════════════
# LOGIN PAGE
# ═══════════════════════════════════════════════════════════════
LOGIN_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CITG — Login</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{
  background:#0a0a1a;
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  display:flex;align-items:center;justify-content:center;
  min-height:100vh;color:#e2e8f0;
}
.login-box{
  background:#0f172a;
  border:1px solid #1e293b;
  border-radius:12px;
  padding:40px 32px;
  width:100%;max-width:380px;
  box-shadow:0 20px 60px rgba(0,0,0,.4);
}
.login-box h2{
  text-align:center;
  margin-bottom:24px;
  font-size:22px;
  font-weight:600;
  color:#f8fafc;
  letter-spacing:.5px;
}
.login-box h2 span{color:#00d4aa}
.field{margin-bottom:16px}
.field label{
  display:block;
  margin-bottom:6px;
  font-size:13px;
  color:#64748b;
  font-weight:500;
}
.field input{
  width:100%;
  padding:10px 14px;
  border:1px solid #1e293b;
  border-radius:8px;
  background:#1e293b;
  color:#f1f5f9;
  font-size:14px;
  outline:none;
  transition:border-color .2s;
}
.field input:focus{border-color:#00d4aa}
.btn{
  width:100%;
  padding:11px;
  border:none;
  border-radius:8px;
  background:#00d4aa;
  color:#0a0a1a;
  font-size:14px;
  font-weight:600;
  cursor:pointer;
  transition:opacity .2s;
  margin-top:8px;
}
.btn:hover{opacity:.85}
#msg{display:none}
</style>
</head>
<body>
<div class="login-box">
  <h2><span>CI</span>TG</h2>
  <form method="POST" action="/login">
    <div class="field">
      <label>Username</label>
      <input type="text" name="username" required autofocus autocomplete="username">
    </div>
    <div class="field">
      <label>Password</label>
      <input type="password" name="password" required autocomplete="current-password">
    </div>
    <button class="btn" type="submit">Login</button>
    <div id="msg"></div>
  </form>
</div>
</body>
</html>'''


# ═══════════════════════════════════════════════════════════════
# PAGE ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/login", response_class=HTMLResponse)
async def login_page():
    return HTMLResponse(content=LOGIN_HTML)


@app.post("/login")
async def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    # Rate limiting by IP
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        error_html = LOGIN_HTML.replace(
            '<div id="msg"></div>',
            '<div id="msg" style="display:block;color:#f87171;text-align:center;margin-top:12px;font-size:13px">Too many attempts. Try again in 15 minutes.</div>'
        )
        return HTMLResponse(content=error_html, status_code=429)

    async with async_session() as session:
        result = await session.execute(select(User).where(User.username == username))
        user = result.scalar_one_or_none()
        if not user or not user.is_active or not user.check_password(password):
            # Return login page with error
            error_html = LOGIN_HTML.replace(
                '<div id="msg"></div>',
                '<div id="msg" style="display:block;color:#f87171;text-align:center;margin-top:12px;font-size:13px">Invalid credentials</div>'
            )
            return HTMLResponse(content=error_html, status_code=401)

        # Create session
        token = _sign_session(user.username)
        response = RedirectResponse(url="/", status_code=302)
        response.set_cookie(
            SESSION_COOKIE_NAME, token,
            httponly=True,
            secure=True,          # HTTPS only (Render uses HTTPS)
            path="/",             # All paths
            max_age=SESSION_MAX_AGE,
            samesite="lax",
        )
        return response


@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(SESSION_COOKIE_NAME)
    response.delete_cookie("citg_session")  # old cookie name
    return response


@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(content=INDEX_HTML)


@app.get("/charts", response_class=HTMLResponse)
async def charts_page():
    return HTMLResponse(content=CHARTS_HTML)


@app.get("/analytics", response_class=HTMLResponse)
async def analytics_page():
    return HTMLResponse(content=ANALYTICS_HTML)


@app.get("/tag-daily", response_class=HTMLResponse)
async def tag_daily_page():
    return HTMLResponse(content=TAG_DAILY_HTML)


@app.get("/sentiment", response_class=HTMLResponse)
async def sentiment_page():
    return HTMLResponse(content=SENTIMENT_HTML)


@app.get("/viral", response_class=HTMLResponse)
async def viral_page():
    return HTMLResponse(content=VIRAL_POSTS_HTML)


@app.get("/sectors", response_class=HTMLResponse)
async def sectors_page():
    return HTMLResponse(content=SECTORS_HTML)


@app.get("/wordcloud", response_class=HTMLResponse)
async def wordcloud_page():
    return HTMLResponse(content=WORDCLOUD_HTML)


@app.get("/crossmarket", response_class=HTMLResponse)
async def crossmarket_page():
    return HTMLResponse(content=CROSSMARKET_HTML)


# ─── NEW: Channels page (v2) ─────────────────────────────────
@app.get("/channels", response_class=HTMLResponse)
async def channels_page():
    return HTMLResponse(content=CHANNELS_HTML)


# ─── NEW: Cross-Channel comparison page (v2) ─────────────────
@app.get("/crosschannel", response_class=HTMLResponse)
async def crosschannel_page():
    return HTMLResponse(content=CROSSCHANNEL_HTML)


# ═══════════════════════════════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════════════════════════════

# ─── API: Channels list (v2) ─────────────────────────────────
@app.get("/api/channels", dependencies=[Depends(_get_auth_user)])
async def api_channels():
    """Return all channels with computed metrics."""
    try:
        async with async_session() as session:
            result = await session.execute(text("""
                SELECT
                    c.id, c.telegram_id, c.numeric_id, c.channel_type, c.username, c.title,
                    c.subscriber_count, c.is_active, c.last_parsed_at,
                    c.total_posts_parsed, c.parse_error_count, c.last_error_message,
                    (SELECT COUNT(*) FROM posts WHERE channel_id = c.id) as posts_count,
                    (SELECT COALESCE(SUM(views_count), 0) FROM posts WHERE channel_id = c.id) as total_views,
                    (SELECT COALESCE(AVG(views_count), 0)::int FROM posts WHERE channel_id = c.id) as avg_views,
                    CASE WHEN c.parse_error_count > 0 THEN 'error'
                         WHEN c.last_parsed_at IS NOT NULL THEN 'success'
                         ELSE 'pending'
                    END as last_parse_status
                FROM channels c
                ORDER BY c.is_active DESC, c.last_parsed_at DESC NULLS LAST
            """))
            rows = result.mappings().all()
            channels = []
            for r in rows:
                channels.append({
                    "id": r["id"],
                    "telegram_id": r["telegram_id"],
                    "numeric_id": r["numeric_id"],
                    "channel_type": r["channel_type"],
                    "username": r["username"],
                    "title": r["title"],
                    "subscriber_count": r["subscriber_count"] or 0,
                    "is_active": r["is_active"],
                    "last_parsed_at": r["last_parsed_at"].isoformat() if r["last_parsed_at"] else None,
                    "total_posts_parsed": r["total_posts_parsed"] or 0,
                    "parse_error_count": r["parse_error_count"] or 0,
                    "last_error_message": r["last_error_message"],
                    "posts_count": r["posts_count"] or 0,
                    "total_views": r["total_views"] or 0,
                    "avg_views": r["avg_views"] or 0,
                    "last_parse_status": r["last_parse_status"],
                })
            return {"channels": channels}
    except Exception as e:
        logger.error(f"/channels error: {e}"); traceback.print_exc()
        return json_response({"channels": [], "error": str(e)}, 500)


# ─── API: Debug routes (diagnostic) ──────────────────────────
@app.get("/api/debug/routes", dependencies=[Depends(_get_auth_user)])
async def api_debug_routes():
    """Return all registered API routes for debugging."""
    routes = []
    for r in app.routes:
        if hasattr(r, "methods"):
            routes.append({"path": r.path, "methods": list(r.methods)})
    return {"routes": routes}


# ─── API: Test endpoint ──────────────────────────────────────
@app.get("/api/test", dependencies=[Depends(_get_auth_user)])
async def api_test():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


# ─── API: Parse trigger ──────────────────────────────────────
# Rich parse state for live progress tracking
_parse_state = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "current_channel": None,
    "current_operation": None,
    "channels_total": 0,
    "channels_done": 0,
    "posts_new_total": 0,
    "posts_parsed_total": 0,
    "error": None,
}

def _update_parse_state(**kwargs):
    """Callback: parser calls this to update live status."""
    global _parse_state
    for k, v in kwargs.items():
        if k == "increment_channels_done" and v:
            _parse_state["channels_done"] = _parse_state.get("channels_done", 0) + v
        elif k == "increment_posts_new" and v:
            _parse_state["posts_new_total"] = _parse_state.get("posts_new_total", 0) + v
        elif k == "increment_posts_parsed" and v:
            _parse_state["posts_parsed_total"] = _parse_state.get("posts_parsed_total", 0) + v
        elif k in _parse_state:
            _parse_state[k] = v

@app.get("/api/parse/trigger", dependencies=[Depends(_get_auth_user)])
async def api_parse_trigger(background_tasks: BackgroundTasks):
    """Trigger parsing of all active channels via BackgroundTasks.
    
    Returns immediately — parsing runs in background without blocking web.
    """
    global _parse_state
    if _parse_state["running"]:
        return {"success": False, "error": "Parsing already running", "state": _parse_state}
    
    # Ensure DB is initialized
    await _ensure_db()
    
    # Reset state
    _parse_state = {
        "running": True,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "current_channel": None,
        "current_operation": "Starting...",
        "channels_total": 0,
        "channels_done": 0,
        "posts_new_total": 0,
        "posts_parsed_total": 0,
        "error": None,
    }
    
    # Add parsing task to background — doesn't block HTTP response
    background_tasks.add_task(_run_parser_background)
    
    return {"success": True, "message": "Parsing started", "state": _parse_state}

async def _run_parser_background():
    """Background parsing task — runs in event loop without blocking web."""
    global _parse_state
    logger.info("[bg-parse] Starting background parse...")
    try:
        parser = _get_parser()
        await parser.init_db()
        # Inject callback so parser reports live progress
        parser._progress_callback = _update_parse_state
        results = await parser.parse_all(history=False)
        _parse_state["running"] = False
        _parse_state["finished_at"] = datetime.now(timezone.utc).isoformat()
        _parse_state["current_operation"] = "Complete"
        logger.info("[bg-parse] Completed: %d channels", len(results))
    except Exception as e:
        logger.error("[bg-parse] Error: %s", e)
        _parse_state["error"] = str(e)
        _parse_state["running"] = False
        _parse_state["finished_at"] = datetime.now(timezone.utc).isoformat()


@app.get("/api/parse/status", dependencies=[Depends(_get_auth_user)])
async def api_parse_status():
    """Get current parsing status (for live progress polling)."""
    return {"state": _parse_state}


# ─── API: Reactivate all channels (admin only) ───────────────
@app.post("/api/admin/reactivate-all", dependencies=[Depends(_get_auth_user)])
async def api_admin_reactivate_all():
    """Reactivate ALL channels and reset error counts."""
    try:
        await _ensure_db()
        async with async_session() as session:
            result = await session.execute(
                text("""
                    UPDATE channels 
                    SET is_active = TRUE, parse_error_count = 0, 
                        last_error_message = NULL, last_error_at = NULL
                    WHERE is_active = FALSE OR parse_error_count > 0
                    RETURNING id, username, telegram_id
                """)
            )
            updated = result.mappings().all()
            await session.commit()
            return {
                "success": True,
                "reactivated": len(updated),
                "channels": [{"id": r["id"], "username": r["username"], "tid": r["telegram_id"]} for r in updated]
            }
    except Exception as e:
        logger.error("[reactivate-all] Error: %s", e)
        return json_response({"error": str(e)}, 500)


# ─── API: Add channel ────────────────────────────────────────
@app.get("/api/channel/add", dependencies=[Depends(_get_auth_user)])
async def api_channel_add(identifier: str = Query(..., description="Username, numeric ID или ссылка на канал")):
    """Добавляет новый канал в БД и синхронизирует его метаданные.

    Supports:
      - https://t.me/c/3147415698/1997
      - https://t.me/markettwits
      - https://web.telegram.org/a/#-1003147415698
      - 3147415698 (bare numeric ID)
      - @markettwits
    """
    # ── 1. Parse/extract clean identifier ─────────────────────
    raw = identifier.strip()
    url_match = re.search(r't\.me/(?:c/)?([^/]+)', raw)
    if url_match:
        clean_id = url_match.group(1)
    elif re.search(r'-100(\d+)', raw):
        clean_id = re.search(r'-100(\d+)', raw).group(1)
    else:
        clean_id = raw.lstrip('@')

    if not clean_id:
        return json_response({"success": False, "error": "Empty identifier"}, 400)

    logger.info(f"[channel/add] raw='{identifier}' → clean='{clean_id}'")

    # ── 2. Validate Telegram credentials ──────────────────────
    if cfg.TG_API_ID <= 0 or not cfg.TG_API_HASH:
        logger.error(f"[channel/add] TG_API_ID={cfg.TG_API_ID} TG_API_HASH={'set' if cfg.TG_API_HASH else 'empty'}")
        return json_response({"success": False, "error": "Telegram API credentials not configured on server"}, 500)

    if not cfg.TG_STRING_SESSION and not cfg.TG_SESSION:
        return json_response({"success": False, "error": "Telegram session not configured"}, 500)

    # ── 3. Check duplicates in DB ─────────────────────────────
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        from telethon.tl.types import PeerChannel
        from telethon.errors import FloodWaitError

        async with async_session() as session:
            # Build duplicate check
            dup_where = [Channel.username == clean_id]
            if clean_id.isdigit():
                # Positive ID like 3147415698 → telegram_id = -1003147415698
                num_id = int(clean_id)
                dup_where.append(Channel.numeric_id == num_id)
                dup_where.append(Channel.telegram_id == int(f"-100{clean_id}"))
            elif clean_id.startswith('-') and clean_id[1:].isdigit():
                # Negative ID like -740684703 → telegram_id = -740684703
                int_id = int(clean_id)
                dup_where.append(Channel.telegram_id == int_id)
                dup_where.append(Channel.numeric_id == abs(int_id))

            from sqlalchemy import or_
            existing = await session.execute(
                select(Channel).where(or_(*dup_where))
            )
            if existing.scalar_one_or_none():
                return json_response({"success": False, "error": "Канал уже существует"}, 409)

            # ── 4. Connect to Telegram ──────────────────────────
            session_str = cfg.TG_STRING_SESSION
            session_file = cfg.TG_SESSION or "/app/sessions/citg_session"
            client = TelegramClient(
                StringSession(session_str) if session_str else session_file,
                cfg.TG_API_ID,
                cfg.TG_API_HASH,
            )
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    return json_response({"success": False, "error": "Telegram session unauthorized. Regenerate TG_STRING_SESSION."}, 500)

                # ── 5. Resolve entity ─────────────────────────
                entity = None

                # Case A: bare negative ID like -740684703 (basic group/chat)
                if clean_id.startswith('-') and clean_id[1:].isdigit():
                    try:
                        int_id = int(clean_id)
                        logger.info(f"[channel/add] Trying bare negative ID: {int_id}")
                        entity = await client.get_entity(int_id)
                    except Exception as e_neg:
                        logger.info(f"[channel/add] Bare negative ID failed: {e_neg}")

                # Case B: positive digit like 3147415698 (channel without -100 prefix)
                elif clean_id.isdigit():
                    try:
                        # Full telegram ID with -100 prefix
                        full_id = int(f"-100{clean_id}")
                        logger.info(f"[channel/add] Trying -100 prefixed ID: {full_id}")
                        entity = await client.get_entity(full_id)
                    except Exception as e1:
                        logger.info(f"[channel/add] -100 ID failed: {e1}, trying PeerChannel")
                        try:
                            entity = await client.get_entity(PeerChannel(int(clean_id)))
                        except Exception as e2:
                            logger.info(f"[channel/add] PeerChannel failed: {e2}")

                # Case C: Fallback to username/string
                if entity is None:
                    logger.info(f"[channel/add] Trying username/peer: {clean_id}")
                    entity = await client.get_entity(clean_id)

                # ── 6. Extract metadata ───────────────────────
                has_username = bool(getattr(entity, "username", None))
                channel_type = "public" if has_username else "private"
                telegram_id = entity.id

                # Compute numeric_id (without -100 prefix)
                if telegram_id < 0:
                    numeric_id = abs(telegram_id) % 1_000_000_000_000
                else:
                    numeric_id = telegram_id

                logger.info(f"[channel/add] Resolved: tid={telegram_id} numeric={numeric_id} type={channel_type} title='{entity.title}'")

                # ── 7. Save to DB ─────────────────────────────
                channel = Channel(
                    telegram_id=telegram_id,
                    numeric_id=numeric_id,
                    channel_type=channel_type,
                    username=entity.username if has_username else None,
                    title=entity.title or "Unknown",
                    description=getattr(entity, "about", None),
                    subscriber_count=getattr(entity, "participants_count", 0),
                    is_active=True,
                    parse_error_count=0,
                    total_posts_parsed=0,
                )
                session.add(channel)
                await session.commit()

                return {
                    "success": True,
                    "channel": {
                        "id": channel.id,
                        "telegram_id": channel.telegram_id,
                        "numeric_id": channel.numeric_id,
                        "channel_type": channel.channel_type,
                        "username": channel.username,
                        "title": channel.title,
                    },
                }
            finally:
                await client.disconnect()

    except FloodWaitError as e:
        logger.warning(f"[channel/add] FloodWait: {e.seconds}s")
        return json_response({"success": False, "error": f"Telegram rate limit. Wait {e.seconds}s."}, 429)
    except ValueError as e:
        logger.error(f"[channel/add] ValueError: {e}")
        return json_response({"success": False, "error": f"Cannot find channel. Make sure you're a member and the ID is correct. Raw: '{identifier}' → Clean: '{clean_id}'"}, 404)
    except Exception as e:
        logger.error(f"[channel/add] Exception: {type(e).__name__}: {e}")
        traceback.print_exc()
        return json_response({"success": False, "error": f"{type(e).__name__}: {e}"}, 500)


# ─── API: Toggle channel active ──────────────────────────────
@app.get("/api/channel/toggle/{channel_id}", dependencies=[Depends(_get_auth_user)])
async def api_channels_toggle(channel_id: int):
    """Включает/выключает канал (is_active)."""
    try:
        async with async_session() as session:
            result = await session.execute(select(Channel).where(Channel.id == channel_id))
            channel = result.scalar_one_or_none()
            if not channel:
                return json_response({"success": False, "error": "Канал не найден"}, 404)

            channel.is_active = not channel.is_active
            # Сбрасываем error_count при активации
            if channel.is_active:
                channel.parse_error_count = 0
                channel.last_error_message = None
            await session.commit()

            return {"success": True, "is_active": channel.is_active, "channel_id": channel_id}
    except Exception as e:
        logger.error(f"/channels/toggle error: {e}"); traceback.print_exc()
        return json_response({"success": False, "error": str(e)}, 500)


# ─── API: Delete channel ─────────────────────────────────────
@app.delete("/api/channel/delete/{channel_id}", dependencies=[Depends(_get_auth_user)])
async def api_channels_delete(channel_id: int):
    """Удаляет канал и все его посты из БД."""
    try:
        async with async_session() as session:
            result = await session.execute(select(Channel).where(Channel.id == channel_id))
            channel = result.scalar_one_or_none()
            if not channel:
                return json_response({"success": False, "error": "Канал не найден"}, 404)

            # Удаляем канал (посты удалятся CASCADE если настроено)
            await session.delete(channel)
            await session.commit()

            return {"success": True, "deleted_id": channel_id}
    except Exception as e:
        logger.error(f"/channels/delete error: {e}"); traceback.print_exc()
        return json_response({"success": False, "error": str(e)}, 500)



# ─── API: Clear all data (admin only) ────────────────────────
# ─── Clear-all state ─────────────────────────────────────────
_clear_all_state = {"running": False, "started_at": None, "error": None}

async def _run_clear_all():
    """Background task: clear all data."""
    global _clear_all_state, _parse_state
    _clear_all_state = {"running": True, "started_at": datetime.now(timezone.utc).isoformat(), "error": None}
    # Блокируем крон-парсер
    _parse_state["clearing"] = True
    logger.info("[clear-all] Starting background clear (cron locked)...")
    try:
        # Устанавливаем флаг в БД для надежности
        try:
            async with async_session() as session:
                result = await session.execute(text("UPDATE parse_state SET global_lock = TRUE"))
                await session.commit()
        except Exception:
            pass
        async with engine.begin() as conn:
            for table in ["channel_group_members", "channel_errors", "parse_logs", "parse_state", "posts", "channels", "channel_groups"]:
                result = await conn.execute(text(f"DELETE FROM {table}"))
                logger.info("[clear-all] Deleted from %s", table)
        logger.info("[clear-all] Complete! Unlocking cron...")
        _clear_all_state["running"] = False
        _parse_state["clearing"] = False
        # Снимаем блокировку в БД
        try:
            async with async_session() as session:
                await session.execute(text("UPDATE parse_state SET global_lock = FALSE"))
                await session.commit()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"[clear-all] Error: {e}")
        _clear_all_state["running"] = False
        _parse_state["clearing"] = False
        _clear_all_state["error"] = str(e)
        # Снимаем блокировку даже при ошибке
        try:
            async with async_session() as session:
                await session.execute(text("UPDATE parse_state SET global_lock = FALSE"))
                await session.commit()
        except Exception:
            pass

@app.post("/api/channel/clear-all", dependencies=[Depends(_get_auth_user)])
async def api_channel_clear_all(background_tasks: BackgroundTasks):
    """Запустить очистку всех данных в фоне. Возвращает сразу."""
    if _parse_state.get("running"):
        return json_response({"success": False, "error": "Parsing in progress — cannot clear"}, 409)
    if _clear_all_state.get("running"):
        return json_response({"success": False, "error": "Clear already running"}, 409)
    background_tasks.add_task(_run_clear_all)
    return {"success": True, "message": "Очистка запущена...", "check_status": "/api/clear-all/status"}

@app.get("/api/clear-all/status")
async def api_clear_all_status():
    """Статус фоновой очистки."""
    return {"state": _clear_all_state}


# ─── API: Cross-Channel comparison (v2) ──────────────────────
@app.get("/api/channels/comparison", dependencies=[Depends(_get_auth_user)])
async def api_channels_comparison(days: int = Query(7, ge=1, le=90)):
    """Compare channels by activity: posts, views, timeline."""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            # Per-channel metrics
            result = await session.execute(text("""
                SELECT
                    c.id, c.username, c.title,
                    COUNT(p.id) as posts_count,
                    COALESCE(SUM(p.views_count), 0) as total_views,
                    COALESCE(AVG(p.views_count), 0)::int as avg_views
                FROM channels c
                LEFT JOIN posts p ON p.channel_id = c.id AND p.published_at > :since
                GROUP BY c.id, c.username, c.title
                ORDER BY posts_count DESC
            """), {"since": since})
            ch_rows = result.mappings().all()

            # Timeline per channel per day
            days_list = []
            for i in range(days + 1):
                dt = since + timedelta(days=i)
                days_list.append(dt.strftime("%m-%d"))

            timeline_channels = []
            for ch in ch_rows:
                if ch["posts_count"] == 0:
                    continue
                daily = await session.execute(text("""
                    SELECT ((published_at AT TIME ZONE 'UTC')::date)::text as d, COUNT(*) as cnt
                    FROM posts
                    WHERE channel_id = :ch_id AND published_at > :since
                    GROUP BY d ORDER BY d
                """), {"ch_id": ch["id"], "since": since})
                day_map = {r["d"]: r["cnt"] for r in daily.mappings().all()}
                series = []
                for i in range(days + 1):
                    dt = since + timedelta(days=i)
                    series.append(day_map.get(dt.strftime("%Y-%m-%d"), 0))
                timeline_channels.append({
                    "username": ch["username"],
                    "title": ch["title"],
                    "series": series,
                })

            return {
                "channels": [
                    {
                        "id": r["id"],
                        "username": r["username"],
                        "title": r["title"],
                        "posts_count": r["posts_count"] or 0,
                        "total_views": r["total_views"] or 0,
                        "avg_views": r["avg_views"] or 0,
                    } for r in ch_rows
                ],
                "timeline": {
                    "days": days_list,
                    "channels": timeline_channels,
                } if timeline_channels else None,
            }
    except Exception as e:
        logger.error(f"/channels/comparison error: {e}"); traceback.print_exc()
        return json_response({"channels": [], "error": str(e)}, 500)


# ─── API: Stats (with optional channel filter) ───────────────
@app.get("/api/stats", dependencies=[Depends(_get_auth_user)])
async def api_stats(channel: Optional[str] = Query(None)):
    try:
        async with async_session() as session:
            ch_filter, ch_params = _channel_where_clause(channel)
            total = (await session.execute(text(f"""
                SELECT COUNT(*) FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE 1=1 {ch_filter}
            """), ch_params)).scalar()
            today = (await session.execute(text(f"""
                SELECT COUNT(*) FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since {ch_filter}
            """), {"since": datetime.now(timezone.utc) - timedelta(days=1), **ch_params})).scalar()
            week = (await session.execute(text(f"""
                SELECT COUNT(*) FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since {ch_filter}
            """), {"since": datetime.now(timezone.utc) - timedelta(days=7), **ch_params})).scalar()
            avg_views = int((await session.execute(text(f"""
                SELECT COALESCE(AVG(p.views_count), 0) FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE 1=1 {ch_filter}
            """), ch_params)).scalar() or 0)
            parses = (await session.execute(text(f"""
                SELECT COUNT(*) FROM parse_logs pl
                JOIN channels c ON pl.channel_id = c.id
                WHERE 1=1 {ch_filter}
            """), ch_params)).scalar()
            last = (await session.execute(text(f"""
                SELECT pl.started_at FROM parse_logs pl
                JOIN channels c ON pl.channel_id = c.id
                WHERE 1=1 {ch_filter}
                ORDER BY pl.started_at DESC LIMIT 1
            """), ch_params)).scalar()

            channel_title = None
            if channel:
                if channel.isdigit():
                    ch_title = (await session.execute(
                        text("SELECT title FROM channels WHERE numeric_id = :ch"),
                        {"ch": int(channel)}
                    )).scalar()
                else:
                    ch_title = (await session.execute(
                        text("SELECT title FROM channels WHERE username = :ch"),
                        {"ch": channel}
                    )).scalar()
                channel_title = ch_title

            return {
                "total_posts": total, "today_posts": today, "week_posts": week,
                "avg_views": avg_views, "total_parses": parses,
                "last_parsed": last.strftime("%Y-%m-%d %H:%M") if last else None,
                "channel_title": channel_title,
            }
    except Exception as e:
        logger.error(f"/stats error: {e}"); traceback.print_exc()
        return json_response({"error": str(e), "total_posts": 0, "today_posts": 0, "week_posts": 0, "avg_views": 0, "total_parses": 0}, 500)


# ─── API: Posts (with optional channel filter) ───────────────
@app.get("/api/posts", dependencies=[Depends(_get_auth_user)])
async def api_posts(
    page: int = 1, limit: int = 20,
    search: str = "", sort: str = "new",
    channel: Optional[str] = Query(None)
):
    logger.info("[api/posts] channel=%r ch_filter=%r ch_params=%r", channel, ch_filter if 'ch_filter' in dir() else '-', '-')
    try:
        async with async_session() as session:
            ch_filter, ch_params = _channel_where_clause(channel)
            logger.info("[api/posts] channel=%r ch_filter=%r ch_params=%r", channel, ch_filter, ch_params)
            search_filter = ""
            search_params = {}
            if search:
                search_filter = "AND p.text ILIKE :search"
                search_params = {"search": f"%{search}%"}

            order_col = "p.views_count DESC" if sort == "views" else "p.published_at DESC"

            # Check if sender_name exists (cached in module-level flag)
            sql_sender = "NULLIF(p.sender_name, '') as sender_name" if _sender_name_ok else "NULL as sender_name"
            
            result = await session.execute(text(f"""
                SELECT p.telegram_message_id, p.text, p.views_count,
                       p.hashtags, p.published_at,
                       {sql_sender},
                       c.username as channel_username, c.title as channel_title,
                       c.channel_type, c.numeric_id
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE 1=1 {ch_filter} {search_filter}
                ORDER BY {order_col}
                LIMIT :limit OFFSET :offset
            """), {**ch_params, **search_params, "limit": limit, "offset": (page - 1) * limit})
            rows = result.mappings().all()
            return {"posts": [
                {
                    "id": r["telegram_message_id"],
                    "text": r["text"],
                    "views": r["views_count"],
                    "hashtags": r["hashtags"] or [],
                    "published": r["published_at"].isoformat() if r["published_at"] else None,
                    "sender_name": r["sender_name"],
                    "channel_username": r["channel_username"],
                    "channel_title": r["channel_title"],
                    "channel_type": r["channel_type"],
                    "numeric_id": r["numeric_id"],
                } for r in rows
            ]}
    except Exception as e:
        logger.error(f"/posts error: {e}"); traceback.print_exc()
        return json_response({"error": str(e), "posts": []}, 500)


# ─── API: Tags (parametric, with channel filter) ─────────────
@app.get("/api/tags", dependencies=[Depends(_get_auth_user)])
async def api_tags(
    hours: int = Query(24, ge=1, le=720),
    channel: Optional[str] = Query(None)
):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(hours=hours))
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                WITH tagged AS (
                    SELECT p.*, c.username as ch_name FROM posts p
                    JOIN channels c ON p.channel_id = c.id
                    WHERE p.published_at > :since
                      AND p.hashtags IS NOT NULL
                      AND json_typeof(p.hashtags) = 'array'
                      AND json_array_length(p.hashtags) > 0
                      {ch_filter}
                )
                SELECT json_array_elements_text(hashtags) as hashtag, COUNT(*) as cnt,
                       SUM(views_count) as total_views, AVG(views_count)::int as avg_views
                FROM tagged
                GROUP BY json_array_elements_text(hashtags)
                ORDER BY cnt DESC LIMIT 50
            """), {"since": since, **ch_params})
            rows = result.mappings().all()
            if not rows:
                result = await session.execute(text(f"""
                    WITH tagged AS (
                        SELECT p.*, c.username as ch_name FROM posts p
                        JOIN channels c ON p.channel_id = c.id
                        WHERE p.hashtags IS NOT NULL
                          AND json_typeof(p.hashtags) = 'array'
                          AND json_array_length(p.hashtags) > 0
                          {ch_filter}
                    )
                    SELECT json_array_elements_text(hashtags) as hashtag, COUNT(*) as cnt,
                           SUM(views_count) as total_views, AVG(views_count)::int as avg_views
                    FROM tagged
                    GROUP BY json_array_elements_text(hashtags)
                    ORDER BY cnt DESC LIMIT 50
                """), ch_params)
                rows = result.mappings().all()
            return {"tags": [{"tag": r["hashtag"], "count": r["cnt"], "total_views": r["total_views"] or 0, "avg_views": r["avg_views"] or 0} for r in rows]}
    except Exception as e:
        logger.error(f"/tags error: {e}"); traceback.print_exc()
        return json_response({"tags": [], "error": str(e)}, 500)


# Backward compatibility: /api/tags/24h -> /api/tags?hours=24
@app.get("/api/tags/24h", dependencies=[Depends(_get_auth_user)])
async def api_tags_24h_compat():
    return await api_tags(hours=24)


# ─── Charts API (all with channel filter) ────────────────────
@app.get("/api/charts/tags", dependencies=[Depends(_get_auth_user)])
async def chart_tags(days: int = Query(7, ge=1, le=90), channel: Optional[str] = Query(None)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                WITH tagged AS (
                    SELECT p.* FROM posts p
                    JOIN channels c ON p.channel_id = c.id
                    WHERE p.published_at > :since
                      AND p.hashtags IS NOT NULL
                      AND json_typeof(p.hashtags) = 'array'
                      AND json_array_length(p.hashtags) > 0
                      {ch_filter}
                )
                SELECT json_array_elements_text(hashtags) as hashtag, COUNT(*) as cnt,
                       SUM(views_count) as total_views, AVG(views_count)::int as avg_views
                FROM tagged
                GROUP BY json_array_elements_text(hashtags)
                ORDER BY cnt DESC LIMIT 50
            """), {"since": since, **ch_params})
            rows = result.mappings().all()
            total = (await session.execute(text(f"""
                SELECT COUNT(*) FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since {ch_filter}
            """), {"since": since, **ch_params})).scalar()
            avg_reach = int((await session.execute(text(f"""
                SELECT COALESCE(AVG(p.views_count), 0) FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since {ch_filter}
            """), {"since": since, **ch_params})).scalar() or 0)
            return {"tags": [{"tag": r["hashtag"], "count": r["cnt"], "total_views": r["total_views"] or 0, "avg_views": r["avg_views"] or 0} for r in rows], "total_posts": total, "avg_reach": avg_reach}
    except Exception as e:
        logger.error(f"/charts/tags error: {e}"); traceback.print_exc()
        return json_response({"tags": [], "total_posts": 0, "avg_reach": 0, "error": str(e)}, 500)


@app.get("/api/charts/activity", dependencies=[Depends(_get_auth_user)])
async def chart_activity(days: int = Query(7, ge=1, le=90), channel: Optional[str] = Query(None)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                SELECT EXTRACT(DOW FROM p.published_at)::int as dow,
                       EXTRACT(HOUR FROM p.published_at)::int as hr,
                       COUNT(*) as cnt
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since {ch_filter}
                GROUP BY 1, 2 ORDER BY 1, 2
            """), {"since": since, **ch_params})
            hours = [0] * 168
            peak_hour, peak_val = 0, 0
            for r in result.mappings().all():
                idx = r["dow"] * 24 + r["hr"]
                if 0 <= idx < 168:
                    hours[idx] = r["cnt"]
                    if r["cnt"] > peak_val:
                        peak_val = r["cnt"]; peak_hour = r["hr"]
            return {"hours": hours, "peak_hour": peak_hour}
    except Exception as e:
        logger.error(f"/charts/activity error: {e}"); traceback.print_exc()
        return json_response({"hours": [0]*168, "peak_hour": 0, "error": str(e)}, 500)


@app.get("/api/charts/views", dependencies=[Depends(_get_auth_user)])
async def chart_views(days: int = Query(7, ge=1, le=90), channel: Optional[str] = Query(None)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                SELECT CASE
                    WHEN p.views_count < 1000 THEN '0-1K'
                    WHEN p.views_count < 5000 THEN '1-5K'
                    WHEN p.views_count < 10000 THEN '5-10K'
                    WHEN p.views_count < 50000 THEN '10-50K'
                    WHEN p.views_count < 100000 THEN '50-100K'
                    ELSE '100K+'
                END as bucket, COUNT(*) as cnt
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since {ch_filter}
                GROUP BY bucket ORDER BY MIN(p.views_count)
            """), {"since": since, **ch_params})
            rows = result.mappings().all()
            order = ['0-1K', '1-5K', '5-10K', '10-50K', '50-100K', '100K+']
            by_label = {r["bucket"]: r["cnt"] for r in rows}
            return {"bins": [{"label": b, "count": by_label.get(b, 0)} for b in order]}
    except Exception as e:
        logger.error(f"/charts/views error: {e}"); traceback.print_exc()
        return json_response({"bins": [], "error": str(e)}, 500)


@app.get("/api/charts/timeline", dependencies=[Depends(_get_auth_user)])
async def chart_timeline(days: int = Query(7, ge=1, le=90), channel: Optional[str] = Query(None)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            top = await session.execute(text(f"""
                WITH tagged AS (
                    SELECT p.* FROM posts p
                    JOIN channels c ON p.channel_id = c.id
                    WHERE p.published_at > :since
                      AND p.hashtags IS NOT NULL
                      AND json_typeof(p.hashtags) = 'array'
                      AND json_array_length(p.hashtags) > 0
                      {ch_filter}
                )
                SELECT json_array_elements_text(hashtags) as hashtag, COUNT(*) as cnt
                FROM tagged GROUP BY json_array_elements_text(hashtags)
                ORDER BY cnt DESC LIMIT 8
            """), {"since": since, **ch_params})
            top_tags = [r["hashtag"] for r in top.mappings().all()]

            days_list = [(since + timedelta(days=i)).strftime("%m-%d") for i in range(days+1)]
            tag_series = []
            for tag in top_tags:
                daily = await session.execute(text(f"""
                    SELECT ((p.published_at AT TIME ZONE 'UTC')::date)::text as d, COUNT(*) as cnt
                    FROM posts p
                    JOIN channels c ON p.channel_id = c.id
                    WHERE p.published_at > :since
                      AND (p.hashtags)::jsonb @> (:tag_json)::jsonb
                      {ch_filter}
                    GROUP BY d ORDER BY d
                """), {"since": since, "tag_json": f'["{tag}"]', **ch_params})
                day_map = {r["d"]: r["cnt"] for r in daily.mappings().all()}
                series = [day_map.get((since + timedelta(days=i)).strftime("%Y-%m-%d"), 0) for i in range(days+1)]
                tag_series.append({"tag": tag, "series": series, "labels": days_list})

            return {"tags": tag_series}
    except Exception as e:
        logger.error(f"/charts/timeline error: {e}"); traceback.print_exc()
        return json_response({"tags": [], "error": str(e)}, 500)


@app.get("/api/charts/pairs", dependencies=[Depends(_get_auth_user)])
async def chart_pairs(days: int = Query(7, ge=1, le=90), channel: Optional[str] = Query(None)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                WITH post_tags AS (
                    SELECT p.id, json_array_elements_text(p.hashtags) as tag
                    FROM posts p
                    JOIN channels c ON p.channel_id = c.id
                    WHERE p.published_at > :since
                      AND p.hashtags IS NOT NULL
                      AND json_typeof(p.hashtags) = 'array'
                      AND json_array_length(p.hashtags) > 1
                      {ch_filter}
                )
                SELECT pt1.tag || ' + ' || pt2.tag as pair, COUNT(*) as cnt
                FROM post_tags pt1
                JOIN post_tags pt2 ON pt1.id = pt2.id AND pt1.tag < pt2.tag
                GROUP BY pair ORDER BY cnt DESC LIMIT 20
            """), {"since": since, **ch_params})
            rows = result.mappings().all()
            return {"pairs": [{"pair": r["pair"], "count": r["cnt"]} for r in rows]}
    except Exception as e:
        logger.error(f"/charts/pairs error: {e}"); traceback.print_exc()
        return json_response({"pairs": [], "error": str(e)}, 500)


# ─── Analytics API (all with channel filter) ─────────────────
@app.get("/api/analytics/alltime-tags", dependencies=[Depends(_get_auth_user)])
async def analytics_alltime_tags(limit: int = Query(100, ge=1, le=500), channel: Optional[str] = Query(None)):
    try:
        async with async_session() as session:
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                WITH tagged AS (
                    SELECT p.* FROM posts p
                    JOIN channels c ON p.channel_id = c.id
                    WHERE p.hashtags IS NOT NULL
                      AND json_typeof(p.hashtags) = 'array'
                      AND json_array_length(p.hashtags) > 0
                      {ch_filter}
                )
                SELECT json_array_elements_text(hashtags) as hashtag,
                       COUNT(*) as cnt,
                       SUM(views_count) as total_views,
                       AVG(views_count)::int as avg_views
                FROM tagged
                GROUP BY json_array_elements_text(hashtags)
                ORDER BY cnt DESC LIMIT :limit
            """), {"limit": limit, **ch_params})
            rows = result.mappings().all()
            return {"tags": [{"tag": r["hashtag"], "count": r["cnt"], "total_views": r["total_views"] or 0, "avg_views": r["avg_views"] or 0} for r in rows]}
    except Exception as e:
        logger.error(f"/analytics/alltime-tags error: {e}"); traceback.print_exc()
        return json_response({"tags": [], "error": str(e)}, 500)


@app.get("/api/analytics/trends", dependencies=[Depends(_get_auth_user)])
async def analytics_trends(channel: Optional[str] = Query(None)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=1))
            ch_filter, ch_params = _channel_where_clause(channel)
            # This week
            this_week = await session.execute(text(f"""
                WITH tagged AS (
                    SELECT p.* FROM posts p
                    JOIN channels c ON p.channel_id = c.id
                    WHERE p.published_at > :since
                      AND p.hashtags IS NOT NULL
                      AND json_typeof(p.hashtags) = 'array'
                      AND json_array_length(p.hashtags) > 0
                      {ch_filter}
                )
                SELECT json_array_elements_text(hashtags) as hashtag, COUNT(*) as cnt
                FROM tagged GROUP BY json_array_elements_text(hashtags)
                ORDER BY cnt DESC LIMIT 30
            """), {"since": since, **ch_params})
            this_map = {r["hashtag"]: r["cnt"] for r in this_week.mappings().all()}

            # Last week (7-14 days ago)
            last_since = since - timedelta(days=7)
            last_week = await session.execute(text(f"""
                WITH tagged AS (
                    SELECT p.* FROM posts p
                    JOIN channels c ON p.channel_id = c.id
                    WHERE p.published_at > :last_since
                      AND p.published_at <= :since
                      AND p.hashtags IS NOT NULL
                      AND json_typeof(p.hashtags) = 'array'
                      AND json_array_length(p.hashtags) > 0
                      {ch_filter}
                )
                SELECT json_array_elements_text(hashtags) as hashtag, COUNT(*) as cnt
                FROM tagged GROUP BY json_array_elements_text(hashtags)
            """), {"last_since": last_since, "since": since, **ch_params})
            last_map = {r["hashtag"]: r["cnt"] for r in last_week.mappings().all()}

            trends = []
            all_tags = set(list(this_map.keys()) + list(last_map.keys()))
            for tag in all_tags:
                this_c = this_map.get(tag, 0)
                last_c = last_map.get(tag, 0)
                if this_c + last_c < 3:
                    continue
                if last_c == 0:
                    pct = 100
                else:
                    pct = int(((this_c - last_c) / last_c) * 100)
                trends.append({"tag": tag, "this_week": this_c, "last_week": last_c, "pct": pct})
            trends.sort(key=lambda x: abs(x["pct"]), reverse=True)
            return {"trends": trends[:30]}
    except Exception as e:
        logger.error(f"/analytics/trends error: {e}"); traceback.print_exc()
        return json_response({"trends": [], "error": str(e)}, 500)


@app.get("/api/analytics/posts-by-tag", dependencies=[Depends(_get_auth_user)])
async def analytics_posts_by_tag(
    tag: str = Query(...), page: int = Query(1, ge=1), limit: int = Query(20, ge=1, le=50),
    channel: Optional[str] = Query(None)
):
    try:
        async with async_session() as session:
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                SELECT p.telegram_message_id, p.text, p.views_count, p.published_at, c.username as channel_username
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE (p.hashtags)::jsonb @> (:tag_json)::jsonb
                {ch_filter}
                ORDER BY p.published_at DESC
                LIMIT :limit OFFSET :offset
            """), {"tag_json": f'["{tag}"]', "limit": limit, "offset": (page - 1) * limit, **ch_params})
            rows = result.mappings().all()
            return {"posts": [{"id": r["telegram_message_id"], "text": r["text"], "views": r["views_count"] or 0, "published": r["published_at"].isoformat() if r["published_at"] else None, "channel_username": r["channel_username"]} for r in rows]}
    except Exception as e:
        logger.error(f"/analytics/posts-by-tag error: {e}"); traceback.print_exc()
        return json_response({"posts": [], "error": str(e)}, 500)


@app.get("/api/analytics/tag-daily", dependencies=[Depends(_get_auth_user)])
async def analytics_tag_daily(tag: str = Query(...), days: int = Query(90, ge=1, le=365), channel: Optional[str] = Query(None)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                SELECT ((p.published_at AT TIME ZONE 'UTC')::date)::text as d, COUNT(*) as cnt
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since
                  AND (p.hashtags)::jsonb @> (:tag_json)::jsonb
                  {ch_filter}
                GROUP BY d ORDER BY d
            """), {"since": since, "tag_json": f'["{tag}"]', **ch_params})
            day_map = {r["d"]: r["cnt"] for r in result.mappings().all()}

            labels = []
            full_dates = []
            counts = []
            for i in range(days + 1):
                dt = since + timedelta(days=i)
                labels.append(dt.strftime("%m-%d"))
                full_dates.append(dt.strftime("%Y-%m-%d"))
                counts.append(day_map.get(dt.strftime("%Y-%m-%d"), 0))

            return {"tag": tag, "days": labels, "full_dates": full_dates, "counts": counts}
    except Exception as e:
        logger.error(f"/analytics/tag-daily error: {e}"); traceback.print_exc()
        return json_response({"tag": tag, "days": [], "counts": [], "error": str(e)}, 500)


@app.get("/api/analytics/tag-posts-by-day", dependencies=[Depends(_get_auth_user)])
async def analytics_tag_posts_by_day(tag: str = Query(...), date: str = Query(...), channel: Optional[str] = Query(None)):
    try:
        async with async_session() as session:
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                SELECT p.telegram_message_id, p.text, p.views_count, p.published_at, c.username as channel_username
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE (p.hashtags)::jsonb @> (:tag_json)::jsonb
                  AND ((p.published_at AT TIME ZONE 'UTC')::date)::text = :date
                  {ch_filter}
                ORDER BY p.published_at DESC
                LIMIT 50
            """), {"tag_json": f'["{tag}"]', "date": date, **ch_params})
            rows = result.mappings().all()
            return {"posts": [{"id": r["telegram_message_id"], "text": r["text"], "views": r["views_count"] or 0, "published": r["published_at"].isoformat() if r["published_at"] else None, "channel_username": r["channel_username"]} for r in rows]}
    except Exception as e:
        logger.error(f"/analytics/tag-posts-by-day error: {e}"); traceback.print_exc()
        return json_response({"posts": [], "error": str(e)}, 500)


@app.get("/api/analytics/export-csv", dependencies=[Depends(_get_auth_user)])
async def analytics_export_csv(channel: Optional[str] = Query(None)):
    try:
        async with async_session() as session:
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                WITH tagged AS (
                    SELECT p.* FROM posts p
                    JOIN channels c ON p.channel_id = c.id
                    WHERE p.hashtags IS NOT NULL
                      AND json_typeof(p.hashtags) = 'array'
                      AND json_array_length(p.hashtags) > 0
                      {ch_filter}
                )
                SELECT json_array_elements_text(hashtags) as hashtag,
                       COUNT(*) as cnt,
                       SUM(views_count) as total_views,
                       AVG(views_count)::int as avg_views
                FROM tagged
                GROUP BY json_array_elements_text(hashtags)
                ORDER BY cnt DESC
            """), ch_params)
            rows = result.mappings().all()

            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(["hashtag", "count", "total_views", "avg_views"])
            for r in rows:
                writer.writerow([r["hashtag"], r["cnt"], r["total_views"] or 0, r["avg_views"] or 0])

            return JSONResponse(
                content={"csv": output.getvalue(), "rows": len(rows)},
                headers={"Content-Type": "text/csv"}
            )
    except Exception as e:
        logger.error(f"/analytics/export-csv error: {e}"); traceback.print_exc()
        return json_response({"error": str(e)}, 500)


# ─── Stock Price API (MOEX proxy) ────────────────────────────
@app.get("/api/stock/price", dependencies=[Depends(_get_auth_user)])
async def stock_price(ticker: str = Query(...), days: int = Query(90, ge=1, le=365)):
    try:
        import urllib.request
        from datetime import datetime as _dt, timedelta as _td
        ticker = ticker.upper()
        till = _dt.now().strftime("%Y-%m-%d")
        since = (_dt.now() - _td(days=days)).strftime("%Y-%m-%d")
        moex_url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json?from={since}&till={till}&interval=24"
        req = urllib.request.Request(moex_url, headers={"User-Agent": "citg/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())

        candles = data.get("candles", {}).get("data", [])
        if not candles:
            return json_response({"ticker": ticker, "days": [], "closes": [], "error": "No data from MOEX"})

        days_list = []
        ohlc = []
        for row in candles:
            d = _dt.strptime(row[6], "%Y-%m-%d %H:%M:%S").strftime("%m-%d")
            days_list.append(d)
            ohlc.append([row[0], row[1], row[3], row[2]])

        return {"ticker": ticker, "days": days_list, "ohlc": ohlc}
    except Exception as e:
        logger.error(f"/stock/price error: {e}"); traceback.print_exc()
        return json_response({"ticker": ticker, "days": [], "ohlc": [], "error": str(e)}, 500)


@app.get("/api/stock/intraday", dependencies=[Depends(_get_auth_user)])
async def stock_intraday(ticker: str = Query(...), date: str = Query(...)):
    try:
        import urllib.request
        from datetime import datetime as _dt
        ticker = ticker.upper()
        moex_url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json?from={date}&till={date}&interval=10"
        req = urllib.request.Request(moex_url, headers={"User-Agent": "citg/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())

        candles = data.get("candles", {}).get("data", [])
        times = []
        ohlc = []
        for row in candles:
            t = _dt.strptime(row[6], "%Y-%m-%d %H:%M:%S").strftime("%H:%M")
            times.append(t)
            ohlc.append([row[0], row[1], row[3], row[2]])

        return {"ticker": ticker, "date": date, "times": times, "ohlc": ohlc}
    except Exception as e:
        logger.error(f"/stock/intraday error: {e}"); traceback.print_exc()
        return json_response({"ticker": ticker, "date": date, "times": [], "ohlc": [], "error": str(e)}, 500)


# ═══════════════════════════════════════════════════════════
# ═══ Sentiment & Intelligence API (with channel filter) ══
# ═══════════════════════════════════════════════════════════

@app.get("/api/sentiment/timeline", dependencies=[Depends(_get_auth_user)])
async def sentiment_timeline(days: int = Query(7, ge=1, le=30), channel: Optional[str] = Query(None)):
    """Daily sentiment scores: positive / negative / neutral / total"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                SELECT
                    ((p.published_at AT TIME ZONE 'UTC')::date)::text as d,
                    p.text
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since AND p.text IS NOT NULL AND p.text != ''
                {ch_filter}
                ORDER BY d
            """), {"since": since, **ch_params})
            rows = result.mappings().all()

            daily = defaultdict(lambda: {"pos": 0, "neg": 0, "neu": 0, "total": 0})

            for r in rows:
                txt = (r["text"] or "").lower()
                pos_count = sum(1 for w in SENTIMENT_POSITIVE if w in txt)
                neg_count = sum(1 for w in SENTIMENT_NEGATIVE if w in txt)
                day = r["d"]
                daily[day]["total"] += 1
                if pos_count > neg_count:
                    daily[day]["pos"] += 1
                elif neg_count > pos_count:
                    daily[day]["neg"] += 1
                else:
                    daily[day]["neu"] += 1

            labels = []
            pos_series = []
            neg_series = []
            neu_series = []
            for i in range(days + 1):
                dt = since + timedelta(days=i)
                d_str = dt.strftime("%Y-%m-%d")
                labels.append(dt.strftime("%m-%d"))
                pos_series.append(daily[d_str]["pos"])
                neg_series.append(daily[d_str]["neg"])
                neu_series.append(daily[d_str]["neu"])

            return {"days": labels, "positive": pos_series, "negative": neg_series, "neutral": neu_series}
    except Exception as e:
        logger.error(f"/sentiment/timeline error: {e}"); traceback.print_exc()
        return json_response({"days": [], "positive": [], "negative": [], "neutral": [], "error": str(e)}, 500)


@app.get("/api/sentiment/top-words", dependencies=[Depends(_get_auth_user)])
async def sentiment_top_words(days: int = Query(7, ge=1, le=30), sentiment: str = Query("positive"), channel: Optional[str] = Query(None)):
    """Most frequent words from posts with given sentiment"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                SELECT p.text FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since AND p.text IS NOT NULL AND p.text != ''
                {ch_filter}
            """), {"since": since, **ch_params})
            rows = result.mappings().all()

            lexicon = SENTIMENT_POSITIVE if sentiment == "positive" else SENTIMENT_NEGATIVE
            word_counts = {}
            for r in rows:
                txt = (r["text"] or "").lower()
                pos = sum(1 for w in SENTIMENT_POSITIVE if w in txt)
                neg = sum(1 for w in SENTIMENT_NEGATIVE if w in txt)
                if sentiment == "positive" and pos > neg:
                    for w in SENTIMENT_POSITIVE:
                        if w in txt:
                            word_counts[w] = word_counts.get(w, 0) + 1
                elif sentiment == "negative" and neg > pos:
                    for w in SENTIMENT_NEGATIVE:
                        if w in txt:
                            word_counts[w] = word_counts.get(w, 0) + 1

            top = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:30]
            return {"words": [{"text": w, "count": c} for w, c in top]}
    except Exception as e:
        logger.error(f"/sentiment/top-words error: {e}"); traceback.print_exc()
        return json_response({"words": [], "error": str(e)}, 500)


@app.get("/api/velocity/alerts", dependencies=[Depends(_get_auth_user)])
async def velocity_alerts(days: int = Query(7, ge=1, le=30), channel: Optional[str] = Query(None)):
    """Tickers with anomalous mention growth vs previous period"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            prev_since = since - timedelta(days=days)
            ch_filter, ch_params = _channel_where_clause(channel)

            curr = await session.execute(text(f"""
                WITH tagged AS (
                    SELECT p.* FROM posts p
                    JOIN channels c ON p.channel_id = c.id
                    WHERE p.published_at > :since
                      AND p.hashtags IS NOT NULL
                      AND json_typeof(p.hashtags) = 'array'
                      AND json_array_length(p.hashtags) > 0
                      {ch_filter}
                )
                SELECT json_array_elements_text(hashtags) as tag, COUNT(*) as cnt
                FROM tagged GROUP BY tag
            """), {"since": since, **ch_params})
            curr_map = {r["tag"]: r["cnt"] for r in curr.mappings().all()}

            prev = await session.execute(text(f"""
                WITH tagged AS (
                    SELECT p.* FROM posts p
                    JOIN channels c ON p.channel_id = c.id
                    WHERE p.published_at > :prev_since AND p.published_at <= :since
                      AND p.hashtags IS NOT NULL
                      AND json_typeof(p.hashtags) = 'array'
                      AND json_array_length(p.hashtags) > 0
                      {ch_filter}
                )
                SELECT json_array_elements_text(hashtags) as tag, COUNT(*) as cnt
                FROM tagged GROUP BY tag
            """), {"prev_since": prev_since, "since": since, **ch_params})
            prev_map = {r["tag"]: r["cnt"] for r in prev.mappings().all()}

            alerts = []
            all_tags = set(curr_map.keys()) | set(prev_map.keys())
            for tag in all_tags:
                c = curr_map.get(tag, 0)
                p = prev_map.get(tag, 0)
                if c + p < 3:
                    continue
                if p == 0:
                    pct = 100
                else:
                    pct = int(((c - p) / p) * 100)
                if c > p and pct >= 50:
                    alerts.append({"tag": tag, "current": c, "previous": p, "pct": pct})

            alerts.sort(key=lambda x: x["pct"], reverse=True)
            return {"alerts": alerts[:20]}
    except Exception as e:
        logger.error(f"/velocity/alerts error: {e}"); traceback.print_exc()
        return json_response({"alerts": [], "error": str(e)}, 500)


@app.get("/api/correlation/matrix", dependencies=[Depends(_get_auth_user)])
async def correlation_matrix(days: int = Query(7, ge=1, le=30), channel: Optional[str] = Query(None)):
    """Correlation matrix: which tags appear together in same posts"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                WITH post_tags AS (
                    SELECT p.id, json_array_elements_text(p.hashtags) as tag
                    FROM posts p
                    JOIN channels c ON p.channel_id = c.id
                    WHERE p.published_at > :since
                      AND p.hashtags IS NOT NULL
                      AND json_typeof(p.hashtags) = 'array'
                      AND json_array_length(p.hashtags) > 1
                      {ch_filter}
                )
                SELECT pt1.tag as tag1, pt2.tag as tag2, COUNT(*) as cnt
                FROM post_tags pt1
                JOIN post_tags pt2 ON pt1.id = pt2.id AND pt1.tag < pt2.tag
                GROUP BY pt1.tag, pt2.tag
                HAVING COUNT(*) >= 2
                ORDER BY cnt DESC
                LIMIT 200
            """), {"since": since, **ch_params})
            rows = result.mappings().all()

            all_tags = set()
            pairs = []
            for r in rows:
                all_tags.add(r["tag1"])
                all_tags.add(r["tag2"])
                pairs.append({"t1": r["tag1"], "t2": r["tag2"], "count": r["cnt"]})

            tags = sorted(all_tags)[:30]
            tag_idx = {t: i for i, t in enumerate(tags)}
            n = len(tags)
            matrix = [[0] * n for _ in range(n)]

            for p in pairs:
                if p["t1"] in tag_idx and p["t2"] in tag_idx:
                    i, j = tag_idx[p["t1"]], tag_idx[p["t2"]]
                    matrix[i][j] = p["count"]
                    matrix[j][i] = p["count"]

            return {"tags": tags, "matrix": matrix}
    except Exception as e:
        logger.error(f"/correlation/matrix error: {e}"); traceback.print_exc()
        return json_response({"tags": [], "matrix": [], "error": str(e)}, 500)


@app.get("/api/premarket/intel", dependencies=[Depends(_get_auth_user)])
async def premarket_intel(days: int = Query(7, ge=1, le=30), channel: Optional[str] = Query(None)):
    """Posts segmented by time: pre-market / market hours / after-hours (MOEX: 10:00-18:45 MSK = 07:00-15:45 UTC)"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                SELECT
                    ((p.published_at AT TIME ZONE 'UTC')::date)::text as d,
                    CASE
                        WHEN EXTRACT(HOUR FROM p.published_at)::int BETWEEN 7 AND 14
                             THEN 'market'
                        WHEN EXTRACT(HOUR FROM p.published_at)::int < 7
                             THEN 'premarket'
                        ELSE 'afterhours'
                    END as segment,
                    COUNT(*) as cnt,
                    SUM(p.views_count) as total_views
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since
                {ch_filter}
                GROUP BY d, segment
                ORDER BY d, segment
            """), {"since": since, **ch_params})
            rows = result.mappings().all()

            labels = []
            pre_series = []
            market_series = []
            after_series = []

            for i in range(days + 1):
                dt = since + timedelta(days=i)
                d_str = dt.strftime("%Y-%m-%d")
                labels.append(dt.strftime("%m-%d"))
                day_data = {r["segment"]: r["cnt"] for r in rows if r["d"] == d_str}
                pre_series.append(day_data.get("premarket", 0))
                market_series.append(day_data.get("market", 0))
                after_series.append(day_data.get("afterhours", 0))

            return {"days": labels, "premarket": pre_series, "market": market_series, "afterhours": after_series}
    except Exception as e:
        logger.error(f"/premarket/intel error: {e}"); traceback.print_exc()
        return json_response({"days": [], "premarket": [], "market": [], "afterhours": [], "error": str(e)}, 500)


# ═══════════════════════════════════════════════════════════
# ═══ Viral & Cross-Market API (with channel filter) ══════
# ═══════════════════════════════════════════════════════════

@app.get("/api/viral/posts", dependencies=[Depends(_get_auth_user)])
async def viral_posts(days: int = Query(7, ge=1, le=30), limit: int = Query(10, ge=1, le=20), channel: Optional[str] = Query(None)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                SELECT p.telegram_message_id, p.text, p.views_count, p.forwards_count, p.published_at,
                       c.username as channel_username, c.channel_type, c.numeric_id
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since
                {ch_filter}
                LIMIT 500
            """), {"since": since, **ch_params})
            rows = result.mappings().all()
            posts = sorted(rows, key=lambda r: r["views_count"] or 0, reverse=True)[:limit]
            return {"posts": [{
                "id": r["telegram_message_id"],
                "text": r["text"],
                "views": r["views_count"] or 0,
                "forwards": r["forwards_count"] or 0,
                "published": r["published_at"].isoformat() if r["published_at"] else None,
                "channel_username": r["channel_username"],
                "channel_type": r["channel_type"],
                "numeric_id": r["numeric_id"],
            } for r in posts]}
    except Exception as e:
        logger.error(f"/viral/posts error: {e}")
        return json_response({"posts": [], "error": str(e)}, 500)


@app.get("/api/sector/rotation", dependencies=[Depends(_get_auth_user)])
async def sector_rotation(days: int = Query(7, ge=1, le=30), channel: Optional[str] = Query(None)):
    """NO json_array_elements -- fetch hashtags json, parse in Python"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                SELECT p.hashtags FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since
                  AND p.hashtags IS NOT NULL
                  AND json_typeof(p.hashtags) = 'array'
                  AND json_array_length(p.hashtags) > 0
                {ch_filter}
                LIMIT 3000
            """), {"since": since, **ch_params})
            rows = result.mappings().all()

            tag_counter = Counter()
            for r in rows:
                for tag in (r["hashtags"] or []):
                    tag_counter[tag] += 1

            sector_counts = defaultdict(int)
            for tag, cnt in tag_counter.most_common(100):
                clean = tag.replace("#", "").upper()
                sector = TICKER_TO_SECTOR.get(clean, "Other")
                sector_counts[sector] += cnt

            sectors = sorted(sector_counts.items(), key=lambda x: x[1], reverse=True)
            return {
                "sectors": [{"name": s, "count": c} for s, c in sectors],
                "total": sum(c for _, c in sectors),
            }
    except Exception as e:
        logger.error(f"/sector/rotation error: {e}")
        return json_response({"sectors": [], "total": 0, "error": str(e)}, 500)


@app.get("/api/wordcloud", dependencies=[Depends(_get_auth_user)])
async def wordcloud_data(days: int = Query(7, ge=1, le=30), limit: int = Query(50, ge=1, le=100), channel: Optional[str] = Query(None)):
    """NO json_array_elements -- fetch hashtags json, count in Python"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            result = await session.execute(text(f"""
                SELECT p.hashtags FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since
                  AND p.hashtags IS NOT NULL
                  AND json_typeof(p.hashtags) = 'array'
                  AND json_array_length(p.hashtags) > 0
                {ch_filter}
                LIMIT 3000
            """), {"since": since, **ch_params})
            rows = result.mappings().all()

            counter = Counter()
            for r in rows:
                for tag in (r["hashtags"] or []):
                    counter[tag] += 1

            return {"words": [{"text": w, "count": c} for w, c in counter.most_common(limit)]}
    except Exception as e:
        logger.error(f"/wordcloud error: {e}")
        return json_response({"words": [], "error": str(e)}, 500)


@app.get("/api/crossmarket/links", dependencies=[Depends(_get_auth_user)])
async def crossmarket_links(days: int = Query(7, ge=1, le=30), channel: Optional[str] = Query(None)):
    """Fetch recent posts, filter in Python -- no ILIKE on text columns"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            ch_filter, ch_params = _channel_where_clause(channel)
            macro_keywords = ["нефть", "brent", "usd", "доллар", "eur", "рубль", "ставка", "цб", "moex"]

            result = await session.execute(text(f"""
                SELECT p.telegram_message_id, p.text, p.views_count, p.published_at, p.hashtags,
                       c.username as channel_username, c.channel_type, c.numeric_id
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE p.published_at > :since
                {ch_filter}
                LIMIT 500
            """), {"since": since, **ch_params})
            rows = result.mappings().all()

            ticker_counter = Counter()
            posts_out = []
            for r in rows:
                text_lower = (r["text"] or "").lower()
                hashtags = r["hashtags"] or []
                match = False
                for kw in macro_keywords:
                    if kw in text_lower or any(kw in (t or "").lower() for t in hashtags):
                        match = True
                        break
                if not match:
                    continue
                posts_out.append({
                    "id": r["telegram_message_id"],
                    "text": r["text"],
                    "views": r["views_count"] or 0,
                    "published": r["published_at"].isoformat() if r["published_at"] else None,
                    "channel_username": r["channel_username"],
                    "channel_type": r["channel_type"],
                    "numeric_id": r["numeric_id"],
                })
                for tag in hashtags:
                    ticker_counter[tag] += 1

            posts_out.sort(key=lambda p: p["views"], reverse=True)
            tickers = [{"tag": t, "count": c} for t, c in ticker_counter.most_common(15)]
            return {"posts": posts_out[:30], "tickers": tickers}
    except Exception as e:
        logger.error(f"/crossmarket/links error: {e}")
        return json_response({"posts": [], "tickers": [], "error": str(e)}, 500)


# ─── RSS Feed ────────────────────────────────────────────────
_RSS_CHANNELS = []  # Channels managed via web UI only


@app.get("/rss", dependencies=[Depends(_get_auth_user)])
async def rss_feed(
    limit: int = Query(50, ge=1, le=200),
    channel: str = Query("", description="Filter by channel username(s), comma-separated"),
):
    try:
        from email.utils import format_datetime

        requested_channels = None
        if channel:
            requested_channels = [c.strip() for c in channel.split(",") if c.strip()]
        else:
            requested_channels = _RSS_CHANNELS

        async with async_session() as session:
            if requested_channels:
                placeholders = ", ".join([f":ch{i}" for i in range(len(requested_channels))])
                channel_filter = f"AND c.username IN ({placeholders})"
                params = {f"ch{i}": ch for i, ch in enumerate(requested_channels)}
            else:
                channel_filter = ""
                params = {}
            params["limit"] = limit

            result = await session.execute(text(f"""
                SELECT
                    p.telegram_message_id,
                    p.text,
                    p.published_at,
                    c.username as channel_username,
                    c.title as channel_title,
                    c.channel_type,
                    c.numeric_id
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE 1=1 {channel_filter}
                ORDER BY p.published_at DESC
                LIMIT :limit
            """), params)
            rows = result.fetchall()

            items = []
            for row in rows:
                msg_id = row[0]
                body = row[1] or ""
                pub = row[2]
                ch_username = row[3]
                ch_title = row[4] or (ch_username or "channel")
                ch_type = row[5]
                numeric_id = row[6]

                # Generate correct Telegram link
                if ch_type == "private" and numeric_id:
                    link = f"https://t.me/c/{numeric_id}/{msg_id}"
                    source_url = f"https://t.me/c/{numeric_id}"
                elif ch_username:
                    link = f"https://t.me/{ch_username}/{msg_id}"
                    source_url = f"https://t.me/{ch_username}"
                else:
                    link = f"https://t.me/c/{numeric_id}/{msg_id}" if numeric_id else "#"
                    source_url = f"https://t.me/c/{numeric_id}" if numeric_id else "#"

                title = body[:100].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                desc = body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                pub_date = format_datetime(pub) if pub else ""
                items.append(f"""<item>
<title>{title}</title>
<link>{link}</link>
<description>{desc}</description>
<pubDate>{pub_date}</pubDate>
<guid>{link}</guid>
<source url="{source_url}">{ch_title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")}</source>
</item>""")

            if len(requested_channels) == 1:
                ch_name = requested_channels[0]
                rss_title = ch_name
                # Check if it's a numeric ID (private channel)
                rss_link = f"https://t.me/c/{ch_name}" if ch_name.isdigit() else f"https://t.me/{ch_name}"
                rss_desc = f"Лента канала {ch_name}"
            else:
                rss_title = "CITG"
                rss_link = "https://t.me"
                rss_desc = "Агрегатор Telegram-каналов"

            rss = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
<channel>
<title>{rss_title}</title>
<link>{rss_link}</link>
<description>{rss_desc}</description>
<language>ru</language>
<lastBuildDate>{format_datetime(datetime.now())}</lastBuildDate>
{chr(10).join(items)}
</channel>
</rss>"""
            return HTMLResponse(
                content=rss,
                media_type="application/rss+xml; charset=utf-8",
                headers={"Cache-Control": "max-age=300, public"}
            )
    except Exception as e:
        logger.error(f"/rss error: {e}"); traceback.print_exc()
        return json_response({"error": str(e)}, 500)


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("web:app", host="0.0.0.0", port=cfg.PORT, reload=False)