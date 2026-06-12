# TG Parser — Telegram Channel Parser

Parser for @markettwits Telegram channel. Saves posts to PostgreSQL with full hashtag analytics, daily histograms, trend detection, and CSV export.

## Stack

- **Telethon** — MTProto client for reading Telegram channels
- **PostgreSQL** — data storage (334K+ posts)
- **SQLAlchemy 2.0** — async ORM
- **FastAPI** — web dashboard with SPA
- **ECharts 5.5** — interactive charts (bubble, heatmap, histogram, timeline)
- **Docker** — containerization
- **Render** — hosting (cron job + web service + managed PostgreSQL)

## Known Issues

### Tags 24h tab — infinite loading
**Status:** Fixed in commit `TBD`  
**Root cause:** `showTab()` used `event.target` which is undefined in some browsers. `tagsLoaded` flag prevented retry on error.  
**Fix:** Pass button element explicitly (`this`), replace boolean flag with `tagsLoading` guard, add error handling + retry button.

### Cold start on Render Starter
**Status:** Mitigated  
**Root cause:** Render Starter plan puts service to sleep after 15 min of inactivity. First request takes 10-30s to wake up.  
**Workaround:** SPA architecture — static HTML renders instantly, data loads via async API calls. Welcome overlay with loader animation shown during wake-up.

## Quick Start (Docker Compose locally)

```bash
# 1. Clone
git clone https://github.com/vladfa2010/tgparser.git
cd tgparser

# 2. Env
cp .env.example .env
# Edit .env: TG_API_ID, TG_API_HASH (from https://my.telegram.org/apps)

# 3. First run — generate session
docker compose run --rm parser python generate_session.py
# Copy the output string

# 4. Add TG_STRING_SESSION to .env

# 5. Run
export HISTORY=1  # First run: full history
docker compose up --build
```

## Deploy to Render

### Step 1: Create PostgreSQL

Render Dashboard → **New** → **PostgreSQL**:
- Name: `tgparser-db`
- Plan: **Free** (1 GB)
- Region: closest to you

Wait for it to be ready. Copy the **Internal Database URL**.

### Step 2: Generate String Session (locally)

```bash
git clone https://github.com/vladfa2010/tgparser.git
cd tgparser
pip install telethon

export TG_API_ID=your_api_id
export TG_API_HASH=your_api_hash
python generate_session.py
# Enter phone → code from Telegram
# Copy the long string output
```

### Step 3: Create Cron Job

Render Dashboard → **New** → **Cron Job**:

| Field | Value |
|-------|-------|
| Name | `tgparser-cron` |
| Runtime | **Docker** |
| Root Directory | *(empty)* |
| Schedule | `*/5 * * * *` (every 5 min) |

**Build Command**: *(Docker uses Dockerfile, leave empty)*

**Env Vars**:

| Key | Value | Secret? |
|-----|-------|---------|
| `DATABASE_URL` | From Step 1 (Internal Database URL) | No |
| `TG_API_ID` | Your API ID | **Yes** |
| `TG_API_HASH` | Your API hash | **Yes** |
| `TG_STRING_SESSION` | From Step 2 | **Yes** |
| `CHANNEL_USERNAME` | `markettwits` | No |

### Step 4: First history run (optional)

To backfill history, run manually once:

Render Dashboard → tgparser-cron → **Manual Deploy** with env var `HISTORY=1`.

Or create a **Background Worker** (Starter plan) for initial import, then delete it.

### Step 5: Done

Cron job will run every 5 minutes, parse new posts, save to PostgreSQL.

Check logs in Render dashboard.

## Database Schema

```
channels     — tracked channel info
posts        — posts (text, hashtags, views, media, forwards)
parse_logs   — parsing history (posts count, duration, errors)
```

## Dashboard Pages

| Page | Path | Features |
|------|------|----------|
| **Posts** | `/` | Browse posts, search, pagination, tags 24h tab |
| **Charts** | `/charts` | 5 ECharts visualizations: bubble, heatmap, histogram, timeline, pairs |
| **Analytics** | `/analytics` | All-time top tags, trending, tag cloud, search by tag, CSV export |
| **Stock & Tag** | `/tag-daily` | MOEX OHLC candlestick + news histogram + 5-min intraday with news markers |

## Files

| File | Purpose |
|------|---------|
| `markettwits_parser.py` | Main parser |
| `web.py` | FastAPI dashboard (SPA + 15 API endpoints) |
| `generate_session.py` | Generate TG_STRING_SESSION locally |
| `fix_hashtags.py` | Retroactive hashtag extraction for old posts |
| `query.py` | CLI database query tool |
| `Dockerfile` | Container image |
| `docker-compose.yml` | Local dev stack |
| `render.yaml` | Render blueprint |
| `requirements.txt` | Python deps |
| `.env.example` | Env template |
