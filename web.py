#!/usr/bin/env python3
"""
TG Parser Dashboard — Production SPA with Charts.
Unified: posts, tags, analytics. ECharts, dark theme, error recovery.
"""
import os
import logging
import traceback
import csv
import io
import json
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import JSON as JSONCol, BigInteger, Boolean, Column, DateTime, Integer, String, Text, UniqueConstraint

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
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

Base = declarative_base()
engine = create_async_engine(DATABASE_URL)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Post(Base):
    __tablename__ = "posts"
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, index=True)
    telegram_message_id = Column(BigInteger)
    text = Column(Text)
    views_count = Column(Integer, default=0)
    forwards_count = Column(Integer, default=0)
    replies_count = Column(Integer, default=0)
    hashtags = Column(JSONCol, default=list)
    forward_from = Column(String(255))
    published_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class ParseLog(Base):
    __tablename__ = "parse_logs"
    id = Column(Integer, primary_key=True)
    posts_parsed = Column(Integer, default=0)
    posts_new = Column(Integer, default=0)
    duration_ms = Column(Integer)
    error_message = Column(Text)
    started_at = Column(DateTime(timezone=True))


app = FastAPI()


def json_response(data, status=200):
    return JSONResponse(content=data, status_code=status)


async def get_since(session, delta):
    since = datetime.now(timezone.utc) - delta
    max_pub = (await session.execute(text("SELECT MAX(published_at) FROM posts"))).scalar()
    if max_pub and max_pub.year > datetime.now(timezone.utc).year:
        since = max_pub - delta
    return since


# ─── SPA: Posts + Tags ───────────────────────────────────────
INDEX_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TG Parser Dashboard</title>
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
.post-head{display:flex;gap:16px;margin-bottom:8px;font-size:13px;color:#64748b;flex-wrap:wrap}
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
<header><h1>TG Parser Dashboard</h1><p class="sub" id="subtitle">Loading...</p></header>
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
</nav>

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

function hideLoader(){var el=$('loader');if(el&&!el.classList.contains('done'))el.classList.add('done')}
setTimeout(hideLoader,6000);

function showError(id,msg){$(id).innerHTML='<div class="err"><h3>Failed to load</h3><p>'+esc(msg)+'</p><button onclick="location.reload()">Reload Page</button></div>';hideLoader()}
function esc(t){return String(t||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function fmt(n){return(n||0).toLocaleString('en').replace(/,/g,' ')}

async function api(path,attempt){attempt=attempt||1;try{var r=await fetch('/api'+path,{cache:'no-store'});if(!r.ok)throw new Error('HTTP '+r.status);var d=await r.json();if(d.error)throw new Error(d.error);return d}catch(e){if(attempt<3){await new Promise(function(r){setTimeout(r,1000*attempt)});return api(path,attempt+1)}throw e}}

// Nav
document.querySelectorAll('nav button').forEach(function(btn){btn.addEventListener('click',function(){var tab=btn.dataset.tab;document.querySelectorAll('nav button').forEach(function(b){b.classList.remove('on')});btn.classList.add('on');$('tab-posts').style.display=tab==='posts'?'':'none';$('tab-tags').style.display=tab==='tags'?'':'none';if(tab==='tags')loadTags()})});

// Posts
async function loadPosts(){if(loading.posts)return;loading.posts=true;$('loader-sub').textContent='Loading posts...';var q=$('q').value,sort=$('sort').value;try{var data=await api('/posts?page='+page+'&search='+encodeURIComponent(q)+'&sort='+sort),stats=await api('/stats');$('subtitle').textContent=fmt(stats.total_posts)+' posts | Last: '+(stats.last_parsed||'-');$('p-stats').innerHTML='<div class="stat"><div class="stat-v">'+fmt(stats.total_posts)+'</div><div class="stat-l">Total</div></div><div class="stat"><div class="stat-v">'+fmt(stats.today_posts)+'</div><div class="stat-l">Today</div></div><div class="stat"><div class="stat-v">'+fmt(stats.week_posts)+'</div><div class="stat-l">Week</div></div><div class="stat"><div class="stat-v">'+fmt(stats.avg_views)+'</div><div class="stat-l">Avg</div></div><div class="stat"><div class="stat-v">'+fmt(stats.total_parses)+'</div><div class="stat-l">Parses</div></div>';if(!data.posts||!data.posts.length){$('p-list').innerHTML='<div class="empty">No posts</div>'}else{$('p-list').innerHTML=data.posts.map(function(p){var tags=(p.hashtags||[]).map(function(t){return'<span class="tag">'+esc(t)+'</span>'}).join('');return'<div class="post"><div class="post-head"><span>ID:'+p.id+'</span><span>views:'+fmt(p.views)+'</span><span>'+(p.published?p.published.slice(0,16).replace('T',' '):'')+'</span></div><div class="post-body">'+esc(p.text||'(no text)')+'</div>'+(tags?'<div class="post-tags">'+tags+'</div>':'')+'</div>'}).join('')}$('p-page').innerHTML='<button '+(page>1?'onclick="goPage('+(page-1)+')"':'disabled')+'>&larr; Prev</button><span>Page '+page+'</span><button '+((data.posts||[]).length===20?'onclick="goPage('+(page+1)+')"':'disabled')+'>Next &rarr;</button>';hideLoader()}catch(e){console.error(e);showError('p-list',e.message)}finally{loading.posts=false}}
window.goPage=function(p){page=p;loadPosts()};

// Tags — period selector state
var tagHours=24;

async function loadTags(hours){hours=hours||tagHours;if(loading.tags)return;loading.tags=true;$('loader-sub').textContent='Loading tags...';tagHours=hours;try{var data=await api('/tags?hours='+hours);var tags=data.tags||[];var periodLabel=hours>=720?(hours/720)+' month':hours>=24?(hours/24)+' day':'hour';periodLabel=hours===24?'24 hours':hours===72?'3 days':hours===168?'7 days':hours===720?'30 days':periodLabel;$('t-stats').innerHTML='<div class="stat"><div class="stat-v">'+tags.length+'</div><div class="stat-l">Tags</div></div><div class="stat"><div class="stat-v">'+(tags[0]?esc(tags[0].tag):'-')+'</div><div class="stat-l">Top</div></div><div class="stat"><div class="stat-v">'+fmt(tags.reduce(function(a,t){return a+t.count},0))+'</div><div class="stat-l">Tagged</div></div>';if(!tags.length){$('t-list').innerHTML='<div class="empty">No tags for selected period</div>';hideLoader();loading.tags=false;return}var maxC=Math.max.apply(null,tags.map(function(t){return t.count}));var colors=['#00d4aa','#00b894','#0984e3','#6c5ce7','#fd79a8','#e17055','#fdcb6e','#55efc4'];$('t-list').innerHTML='<div style="color:#64748b;font-size:13px;margin-bottom:16px">Last '+periodLabel+' — tag ranking by frequency</div>'+tags.map(function(t,i){var pct=Math.round((t.count/maxC)*100);return'<div style="display:flex;align-items:center;gap:15px;margin-bottom:10px;padding:14px 16px;background:#0f172a;border:1px solid #1e293b;border-radius:10px"><div style="min-width:160px;font-weight:600;color:#00d4aa;font-size:14px">'+esc(t.tag)+'</div><div style="flex:1;height:28px;background:#0a0a1a;border-radius:6px;overflow:hidden"><div style="height:100%;border-radius:6px;display:flex;align-items:center;padding:0 12px;font-size:12px;font-weight:600;color:#fff;transition:width .8s;width:'+pct+'%;background:'+colors[i%colors.length]+'">'+t.count+' posts</div></div><div style="min-width:90px;text-align:right;color:#64748b;font-size:12px">'+fmt(t.total_views)+' views<br>~'+fmt(t.avg_views)+'</div></div>'}).join('');hideLoader()}catch(e){console.error(e);showError('t-list',e.message)}finally{loading.tags=false}}

// Tag period selector handlers
document.querySelectorAll('#tag-period button').forEach(function(btn){btn.addEventListener('click',function(){document.querySelectorAll('#tag-period button').forEach(function(b){b.classList.remove('on')});btn.classList.add('on');loadTags(parseInt(btn.dataset.h))})});

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
<title>Stock & Tag — TG Parser</title>
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
<div class="chart-title">&#128172; Telegram News with #<span id="tag-label">LKOH</span> <span style="color:#64748b;font-size:13px">— click a bar to see posts</span></div>
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
  var r=await fetch('/api'+path,{cache:'no-store'});
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
    // Load intraday in parallel
    var intra=await api('/stock/intraday?ticker='+encodeURIComponent(currentTicker)+'&date='+encodeURIComponent(date));
    var times=intra.times||[];
    var ohlc=intra.ohlc||[];
    // News markers: find time index in times array for category X-axis
    var timeIndex={};
    for(var i=0;i<times.length;i++)timeIndex[times[i]]=i;
    // Build overlay data: null everywhere except news timestamp = high price
    // Round post time to nearest 10-min candle (MOEX interval=10 = 10-min step)
    var overlayData=times.map(function(){return null;});
    var newsMap={};
    posts.filter(function(p){return p.published;}).forEach(function(p,pi){
      var h=parseInt(p.published.slice(11,13));
      var m=parseInt(p.published.slice(14,16));
      h=(h+3)%24; // UTC→MSK
      m=Math.round(m/10)*10; // round to nearest 10 min
      if(m===60){m=0;h=(h+1)%24;}
      var t=(h<10?'0':'')+h+':'+(m<10?'0':'')+m;
      var idx=timeIndex[t]!==undefined?timeIndex[t]:-1;
      console.log('POST',pi,'UTC='+p.published.slice(11,16),'MSK='+t,'idx='+idx,'text='+p.text.slice(0,30));
      if(idx>=0&&idx<ohlc.length){
        overlayData[idx]=ohlc[idx][3]; // high price
        newsMap[idx]=p.text?p.text:'News';
        console.log('  -> placed at idx',idx);
      }
    });
    console.log('overlayData non-null:',overlayData.filter(function(x){return x!==null;}).length);
    // Build scatter data: [index, high, text] for each news
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

  // Reset charts fresh — previous error may have destroyed DOM
  if(stockChart){try{stockChart.dispose();}catch(e){}stockChart=null;}
  if(tagChart){try{tagChart.dispose();}catch(e){}tagChart=null;}
  if(intradayChart){try{intradayChart.dispose();}catch(e){}intradayChart=null;}
  $('stock-chart').innerHTML='';
  $('tag-chart').innerHTML='';
  $('intraday-chart').innerHTML='';
  $('posts-box').style.display='none';
  $('intraday-box').style.display='none';

  try{
    // Fetch stock price + tag activity in parallel
    var s=await api('/stock/price?ticker='+encodeURIComponent(ticker)+'&days=90');
    var t=await api('/analytics/tag-daily?tag=%23'+encodeURIComponent(ticker)+'&days=90');

    // Stock stats
    var ohlc=s.ohlc||[];
    var latest=ohlc.length?ohlc[ohlc.length-1][1]:0;  // close
    var first=ohlc.length?ohlc[0][0]:0;  // open of first day
    var pct=first?(((latest-first)/first)*100):0;
    $('s-price').textContent=latest?fmt(latest)+' RUB':'N/A';
    var chEl=$('s-change');
    chEl.textContent=pct?(pct>=0?'+':'')+pct.toFixed(1)+'%':'N/A';
    chEl.className='stat-v '+(pct>=0?'green':'red');

    // Tag stats
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

    // Render stock chart (OHLC candlestick)
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

    // Render tag chart
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
    // Render error inside charts without destroying DOM
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
<title>Tag Analytics — TG Parser</title>
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
<input type="text" id="tag-search" placeholder="Search tag (e.g. #россия)...
" onkeydown="if(event.key==='Enter')searchTag()">
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
  var r=await fetch('/api'+path,{cache:'no-store'});
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
<title>Charts — TG Parser</title>
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
// FORCE hide after 5s no matter what
setTimeout(hideLoader,5000);

// Global error handler
window.onerror=function(msg,url,line){console.error('JS ERROR:',msg,'line',line);hideLoader();try{getChart('c-bubble').setOption({backgroundColor:'transparent',title:{text:'JS Error: '+msg,left:'center',top:'center',textStyle:{color:'#f87171',fontSize:14}}},true);}catch(e){}return true};

function fmt(n){return(n||0).toLocaleString('en').replace(/,/g,' ')}
function esc(t){return String(t||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}

async function api(path,attempt){
  attempt=attempt||1;
  try{
    console.log('API fetch:',path);
    var r=await fetch('/api'+path,{cache:'no-store'});
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
  // Reset charts fresh — previous error may have destroyed DOM
  resetCharts();
  ['c-bubble','c-heat','c-hist','c-time','c-pair'].forEach(function(id){var el=$(id);if(el)el.innerHTML='';});
  showChartsLoader('Fetching data for '+days+'d...');
  try{
    if(typeof echarts==='undefined'){
      throw new Error('ECharts not loaded. Check CDN connection.');
    }
    console.log('ECharts OK');
    // Parallel fetch all APIs
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
    // Stats
    $('top-stats').innerHTML=[['Posts',fmt(t.total_posts)],['Tags',t.tags.length],['Top',t.tags[0]?t.tags[0].tag:'-'],['Avg',fmt(t.avg_reach)],['Peak',a.peak_hour+'h']].map(function(s){return'<div class="stat"><div class="stat-v">'+esc(s[1])+'</div><div class="stat-l">'+s[0]+'</div></div>'}).join('');
    // Render each chart individually so one failure doesn't kill all
    try{renderBubble(t.tags);console.log('bubble OK')}catch(e){console.error('bubble:',e);try{getChart('c-bubble').setOption({backgroundColor:'transparent',title:{text:'Error: '+e.message,left:'center',top:'center',textStyle:{color:'#f87171',fontSize:14}}},true);}catch(e2){}}
    try{renderHeat(a.hours,a.peak_hour);console.log('heat OK')}catch(e){console.error('heat:',e);try{getChart('c-heat').setOption({backgroundColor:'transparent',title:{text:'Error: '+e.message,left:'center',top:'center',textStyle:{color:'#f87171',fontSize:14}}},true);}catch(e2){}}
    try{renderHist(v.bins);console.log('hist OK')}catch(e){console.error('hist:',e);try{getChart('c-hist').setOption({backgroundColor:'transparent',title:{text:'Error: '+e.message,left:'center',top:'center',textStyle:{color:'#f87171',fontSize:14}}},true);}catch(e2){}}
    try{renderTime(tl.tags);console.log('time OK')}catch(e){console.error('time:',e);try{getChart('c-time').setOption({backgroundColor:'transparent',title:{text:'Error: '+e.message,left:'center',top:'center',textStyle:{color:'#f87171',fontSize:14}}},true);}catch(e2){}}
    try{renderPairs(p.pairs);console.log('pairs OK')}catch(e){console.error('pairs:',e);try{getChart('c-pair').setOption({backgroundColor:'transparent',title:{text:'Error: '+e.message,left:'center',top:'center',textStyle:{color:'#f87171',fontSize:14}}},true);}catch(e2){}}
    hideLoader();
    console.log('all done');
  }catch(e){
    console.error('loadAll ERROR:',e);
    // Re-init containers and show error via setOption (don't destroy DOM with innerHTML)
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
<title>Sentiment & Intelligence — TG Parser</title>
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
<div class="chart-title">📊 Sentiment Timeline</div>
<div class="chart-sub">Positive vs Negative vs Neutral posts per day</div>
<div class="chart" id="s-timeline"></div>
</div>

<div class="chart-box">
<div class="chart-title">🚀 News Velocity Alerts</div>
<div class="chart-sub">Tickers with anomalous mention growth</div>
<div class="chart" id="s-alerts"></div>
</div>

<div class="chart-box">
<div class="chart-title">🔗 Correlation Matrix</div>
<div class="chart-sub">Tags that appear together in posts</div>
<div class="chart" id="s-corr"></div>
</div>

<div class="chart-box full">
<div class="chart-title">⏰ Pre-Market Intelligence</div>
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
  var r=await fetch('/api'+path,{cache:'no-store'});
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

    // Stats
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

# ─── Page 1: Viral Posts ─────────────────────────────────────
VIRAL_POSTS_HTML = '''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Viral Posts — TG Parser</title>
<style>''' + VIRAL_SHARED_CSS + '''
.vpost{background:#0f172a;border:1px solid #1e293b;border-radius:12px;padding:16px;margin-bottom:12px;cursor:pointer;transition:.15s}
.vpost:hover{border-color:#334155}
.vpost-head{display:flex;justify-content:space-between;margin-bottom:8px;font-size:12px;color:#64748b}
.vpost-body{color:#e2e8f0;white-space:pre-wrap;word-break:break-word;max-height:80px;overflow:hidden;line-height:1.5;font-size:14px}
.vpost-stats{display:flex;gap:20px;margin-top:10px;font-size:13px;color:#64748b}
.vpost-stats span{color:#00d4aa;font-weight:600}
</style>
</head>
<body>
<div id="loader"><div class="loader-ring"></div><div class="loader-text">Loading...</div></div>
<div class="wrap">
<header><h1>🔥 Viral Posts</h1>
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
      var link='https://t.me/markettwits/'+p.id;
      return'<div class="vpost" onclick="window.open(\''+link+'\')">'+
        '<div class="vpost-head"><span>#'+(i+1)+'</span><span>'+(p.published?p.published.slice(0,16).replace('T',' '):'')+'</span></div>'+
        '<div class="vpost-body">'+esc((p.text||'(no text)').slice(0,250))+'</div>'+
        '<div class="vpost-stats"><span>👁 '+fmt(p.views)+'</span><span>↗️ '+fmt(p.forwards)+'</span></div></div>';
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
<title>Sector Rotation — TG Parser</title>
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
<header><h1>🏭 Sector Rotation</h1>
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
<title>Word Cloud — TG Parser</title>
<style>''' + VIRAL_SHARED_CSS + '''
.cloud{display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:center;min-height:400px;padding:30px}
.cloud-tag{padding:10px 20px;border-radius:24px;font-weight:600;cursor:pointer;transition:transform .2s,opacity .2s;opacity:.85}
.cloud-tag:hover{transform:scale(1.12);opacity:1}
</style>
</head>
<body>
<div id="loader"><div class="loader-ring"></div><div class="loader-text">Loading...</div></div>
<div class="wrap">
<header><h1>☁️ Word Cloud</h1>
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
<title>Cross-Market — TG Parser</title>
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
<header><h1>🌍 Cross-Market</h1>
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
      var link='https://t.me/markettwits/'+p.id;
      return'<div class="xpost" onclick="window.open(\''+link+'\')">'+
        '<div class="xpost-head"><span>👁 '+fmt(p.views)+' | '+(p.published?p.published.slice(0,16).replace('T',' '):'')+'</span></div>'+
        '<div class="xpost-body">'+esc((p.text||'(no text)').slice(0,250))+'</div></div>';
    }).join('');
    if(tickers.length){
      $('tickers').innerHTML='<div style="color:#64748b;font-size:13px;margin-bottom:10px">📌 Co-mentioned tickers:</div>'+
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


# ─── API: Stats ──────────────────────────────────────────────
@app.get("/api/stats")
async def api_stats():
    try:
        async with async_session() as session:
            total = (await session.execute(select(func.count()).select_from(Post))).scalar()
            today = (await session.execute(select(func.count()).select_from(Post).where(Post.published_at > datetime.now(timezone.utc) - timedelta(days=1)))).scalar()
            week = (await session.execute(select(func.count()).select_from(Post).where(Post.published_at > datetime.now(timezone.utc) - timedelta(days=7)))).scalar()
            avg_views = int((await session.execute(select(func.avg(Post.views_count)).select_from(Post))).scalar() or 0)
            parses = (await session.execute(select(func.count()).select_from(ParseLog))).scalar()
            last = (await session.execute(select(ParseLog.started_at).order_by(ParseLog.started_at.desc()).limit(1))).scalar()
            return {"total_posts": total, "today_posts": today, "week_posts": week, "avg_views": avg_views, "total_parses": parses, "last_parsed": last.strftime("%Y-%m-%d %H:%M") if last else None}
    except Exception as e:
        logger.error(f"/stats error: {e}"); traceback.print_exc()
        return json_response({"error": str(e), "total_posts": 0, "today_posts": 0, "week_posts": 0, "avg_views": 0, "total_parses": 0}, 500)


# ─── API: Posts ──────────────────────────────────────────────
@app.get("/api/posts")
async def api_posts(page: int = 1, limit: int = 20, search: str = "", sort: str = "new"):
    try:
        async with async_session() as session:
            query = select(Post)
            if search: query = query.where(Post.text.ilike(f"%{search}%"))
            query = query.order_by(Post.views_count.desc() if sort == "views" else Post.published_at.desc())
            query = query.offset((page - 1) * limit).limit(limit)
            result = await session.execute(query)
            posts = result.scalars().all()
            return {"posts": [{"id": p.telegram_message_id, "text": p.text, "views": p.views_count, "hashtags": p.hashtags or [], "published": p.published_at.isoformat() if p.published_at else None} for p in posts]}
    except Exception as e:
        logger.error(f"/posts error: {e}"); traceback.print_exc()
        return json_response({"error": str(e), "posts": []}, 500)


# ─── API: Tags (parametric) ──────────────────────────────────
@app.get("/api/tags")
async def api_tags(hours: int = Query(24, ge=1, le=720)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(hours=hours))
            result = await session.execute(text("""
                WITH tagged AS (
                    SELECT * FROM posts WHERE published_at > :since
                      AND hashtags IS NOT NULL
                      AND json_typeof(hashtags) = 'array'
                      AND json_array_length(hashtags) > 0
                )
                SELECT json_array_elements_text(hashtags) as hashtag, COUNT(*) as cnt,
                       SUM(views_count) as total_views, AVG(views_count)::int as avg_views
                FROM tagged
                GROUP BY json_array_elements_text(hashtags)
                ORDER BY cnt DESC LIMIT 50
            """), {"since": since})
            rows = result.mappings().all()
            if not rows:
                result = await session.execute(text("""
                    WITH tagged AS (
                        SELECT * FROM posts WHERE hashtags IS NOT NULL
                          AND json_typeof(hashtags) = 'array'
                          AND json_array_length(hashtags) > 0
                    )
                    SELECT json_array_elements_text(hashtags) as hashtag, COUNT(*) as cnt,
                           SUM(views_count) as total_views, AVG(views_count)::int as avg_views
                    FROM tagged
                    GROUP BY json_array_elements_text(hashtags)
                    ORDER BY cnt DESC LIMIT 50
                """))
                rows = result.mappings().all()
            return {"tags": [{"tag": r["hashtag"], "count": r["cnt"], "total_views": r["total_views"] or 0, "avg_views": r["avg_views"] or 0} for r in rows]}
    except Exception as e:
        logger.error(f"/tags error: {e}"); traceback.print_exc()
        return json_response({"tags": [], "error": str(e)}, 500)


# Backward compatibility: /api/tags/24h → /api/tags?hours=24
@app.get("/api/tags/24h")
async def api_tags_24h_compat():
    return await api_tags(hours=24)


# ─── Charts API ──────────────────────────────────────────────
@app.get("/api/charts/tags")
async def chart_tags(days: int = Query(7, ge=1, le=90)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            result = await session.execute(text("""
                WITH tagged AS (
                    SELECT * FROM posts WHERE published_at > :since
                      AND hashtags IS NOT NULL
                      AND json_typeof(hashtags) = 'array'
                      AND json_array_length(hashtags) > 0
                )
                SELECT json_array_elements_text(hashtags) as hashtag, COUNT(*) as cnt,
                       SUM(views_count) as total_views, AVG(views_count)::int as avg_views
                FROM tagged
                GROUP BY json_array_elements_text(hashtags)
                ORDER BY cnt DESC LIMIT 50
            """), {"since": since})
            rows = result.mappings().all()
            total = (await session.execute(select(func.count()).select_from(Post).where(Post.published_at > since))).scalar()
            avg_reach = int((await session.execute(select(func.avg(Post.views_count)).select_from(Post).where(Post.published_at > since))).scalar() or 0)
            return {"tags": [{"tag": r["hashtag"], "count": r["cnt"], "total_views": r["total_views"] or 0, "avg_views": r["avg_views"] or 0} for r in rows], "total_posts": total, "avg_reach": avg_reach}
    except Exception as e:
        logger.error(f"/charts/tags error: {e}"); traceback.print_exc()
        return json_response({"tags": [], "total_posts": 0, "avg_reach": 0, "error": str(e)}, 500)


@app.get("/api/charts/activity")
async def chart_activity(days: int = Query(7, ge=1, le=90)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            result = await session.execute(text("""
                SELECT EXTRACT(DOW FROM published_at)::int as dow,
                       EXTRACT(HOUR FROM published_at)::int as hr,
                       COUNT(*) as cnt
                FROM posts WHERE published_at > :since
                GROUP BY 1, 2 ORDER BY 1, 2
            """), {"since": since})
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


@app.get("/api/charts/views")
async def chart_views(days: int = Query(7, ge=1, le=90)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            result = await session.execute(text("""
                SELECT CASE
                    WHEN views_count < 1000 THEN '0-1K'
                    WHEN views_count < 5000 THEN '1-5K'
                    WHEN views_count < 10000 THEN '5-10K'
                    WHEN views_count < 50000 THEN '10-50K'
                    WHEN views_count < 100000 THEN '50-100K'
                    ELSE '100K+'
                END as bucket, COUNT(*) as cnt
                FROM posts WHERE published_at > :since
                GROUP BY bucket ORDER BY MIN(views_count)
            """), {"since": since})
            rows = result.mappings().all()
            order = ['0-1K', '1-5K', '5-10K', '10-50K', '50-100K', '100K+']
            by_label = {r["bucket"]: r["cnt"] for r in rows}
            return {"bins": [{"label": b, "count": by_label.get(b, 0)} for b in order]}
    except Exception as e:
        logger.error(f"/charts/views error: {e}"); traceback.print_exc()
        return json_response({"bins": [], "error": str(e)}, 500)


@app.get("/api/charts/timeline")
async def chart_timeline(days: int = Query(7, ge=1, le=90)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            # Get top 8 tags
            top = await session.execute(text("""
                WITH tagged AS (
                    SELECT * FROM posts WHERE published_at > :since
                      AND hashtags IS NOT NULL
                      AND json_typeof(hashtags) = 'array'
                      AND json_array_length(hashtags) > 0
                )
                SELECT json_array_elements_text(hashtags) as hashtag, COUNT(*) as cnt
                FROM tagged GROUP BY json_array_elements_text(hashtags)
                ORDER BY cnt DESC LIMIT 8
            """), {"since": since})
            top_tags = [r["hashtag"] for r in top.mappings().all()]

            days_list = [(since + timedelta(days=i)).strftime("%m-%d") for i in range(days+1)]
            tag_series = []
            for tag in top_tags:
                daily = await session.execute(text("""
                    SELECT ((published_at AT TIME ZONE 'UTC')::date)::text as d, COUNT(*) as cnt
                    FROM posts WHERE published_at > :since
                      AND (hashtags)::jsonb @> (:tag_json)::jsonb
                    GROUP BY d ORDER BY d
                """), {"since": since, "tag_json": f'["{tag}"]'})
                day_map = {r["d"]: r["cnt"] for r in daily.mappings().all()}
                series = [day_map.get((since + timedelta(days=i)).strftime("%Y-%m-%d"), 0) for i in range(days+1)]
                tag_series.append({"tag": tag, "series": series, "labels": days_list})

            return {"tags": tag_series}
    except Exception as e:
        logger.error(f"/charts/timeline error: {e}"); traceback.print_exc()
        return json_response({"tags": [], "error": str(e)}, 500)


@app.get("/api/charts/pairs")
async def chart_pairs(days: int = Query(7, ge=1, le=90)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            result = await session.execute(text("""
                WITH post_tags AS (
                    SELECT id, json_array_elements_text(hashtags) as tag
                    FROM posts WHERE published_at > :since
                      AND hashtags IS NOT NULL
                      AND json_typeof(hashtags) = 'array'
                      AND json_array_length(hashtags) > 1
                )
                SELECT pt1.tag || ' + ' || pt2.tag as pair, COUNT(*) as cnt
                FROM post_tags pt1
                JOIN post_tags pt2 ON pt1.id = pt2.id AND pt1.tag < pt2.tag
                GROUP BY pair ORDER BY cnt DESC LIMIT 20
            """), {"since": since})
            rows = result.mappings().all()
            return {"pairs": [{"pair": r["pair"], "count": r["cnt"]} for r in rows]}
    except Exception as e:
        logger.error(f"/charts/pairs error: {e}"); traceback.print_exc()
        return json_response({"pairs": [], "error": str(e)}, 500)


# ─── RSS Feed ────────────────────────────────────────────────
# Optional comma-separated list of channel usernames to include.
# Falls back to single "markettwits" if env not set.
_RSS_CHANNELS = [c.strip() for c in os.getenv("RSS_CHANNELS", os.getenv("CHANNELS", "markettwits")).split(",") if c.strip()]


@app.get("/rss")
async def rss_feed(
    limit: int = Query(50, ge=1, le=200),
    channel: str = Query("", description="Filter by channel username(s), comma-separated"),
):
    try:
        from email.utils import format_datetime

        # Build channel filter
        requested_channels = None
        if channel:
            requested_channels = [c.strip() for c in channel.split(",") if c.strip()]
        else:
            requested_channels = _RSS_CHANNELS

        async with async_session() as session:
            # Build query with optional channel filter
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
                    c.title as channel_title
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
                ch_username = row[3] or "markettwits"
                ch_title = row[4] or ch_username

                # Escape XML
                title = body[:100].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                desc = body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                link = f"https://t.me/{ch_username}/{msg_id}"
                pub_date = format_datetime(pub) if pub else ""
                items.append(f"""<item>
<title>{title}</title>
<link>{link}</link>
<description>{desc}</description>
<pubDate>{pub_date}</pubDate>
<guid>{link}</guid>
<source url="https://t.me/{ch_username}">{ch_title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")}</source>
</item>""")

            # Build channel info for RSS header
            if len(requested_channels) == 1:
                rss_title = requested_channels[0]
                rss_link = f"https://t.me/{requested_channels[0]}"
                rss_desc = f"Лента канала @{requested_channels[0]}"
            else:
                rss_title = "MarketTwits"
                rss_link = "https://t.me/markettwits"
                rss_desc = "Финансы, рынки и экономика"

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


# ─── Stock Price API (MOEX proxy) ────────────────────────────
@app.get("/api/stock/price")
async def stock_price(ticker: str = Query(...), days: int = Query(90, ge=1, le=365)):
    try:
        import urllib.request
        from datetime import datetime, timedelta
        ticker = ticker.upper()
        till = datetime.now().strftime("%Y-%m-%d")
        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        moex_url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json?from={since}&till={till}&interval=24"
        req = urllib.request.Request(moex_url, headers={"User-Agent": "tgparser/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())

        candles = data.get("candles", {}).get("data", [])
        if not candles:
            return json_response({"ticker": ticker, "days": [], "closes": [], "error": "No data from MOEX"})

        days_list = []
        ohlc = []  # [open, close, low, high] for ECharts candlestick
        for row in candles:
            d = datetime.strptime(row[6], "%Y-%m-%d %H:%M:%S").strftime("%m-%d")
            days_list.append(d)
            ohlc.append([row[0], row[1], row[3], row[2]])  # [open, close, low, high]

        return {"ticker": ticker, "days": days_list, "ohlc": ohlc}
    except Exception as e:
        logger.error(f"/stock/price error: {e}"); traceback.print_exc()
        return json_response({"ticker": ticker, "days": [], "ohlc": [], "error": str(e)}, 500)


@app.get("/api/stock/intraday")
async def stock_intraday(ticker: str = Query(...), date: str = Query(...)):
    try:
        import urllib.request
        from datetime import datetime
        ticker = ticker.upper()
        # MOEX: interval=10 = 5-minute candles (MOEX numbering convention)
        moex_url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json?from={date}&till={date}&interval=10"
        req = urllib.request.Request(moex_url, headers={"User-Agent": "tgparser/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())

        candles = data.get("candles", {}).get("data", [])
        times = []
        ohlc = []
        for row in candles:
            t = datetime.strptime(row[6], "%Y-%m-%d %H:%M:%S").strftime("%H:%M")
            times.append(t)
            ohlc.append([row[0], row[1], row[3], row[2]])  # [open, close, low, high]

        return {"ticker": ticker, "date": date, "times": times, "ohlc": ohlc}
    except Exception as e:
        logger.error(f"/stock/intraday error: {e}"); traceback.print_exc()
        return json_response({"ticker": ticker, "date": date, "times": [], "ohlc": [], "error": str(e)}, 500)


# ─── Analytics API ───────────────────────────────────────────
@app.get("/api/analytics/alltime-tags")
async def analytics_alltime_tags(limit: int = Query(100, ge=1, le=500)):
    try:
        async with async_session() as session:
            result = await session.execute(text("""
                WITH tagged AS (
                    SELECT * FROM posts
                    WHERE hashtags IS NOT NULL
                      AND json_typeof(hashtags) = 'array'
                      AND json_array_length(hashtags) > 0
                )
                SELECT json_array_elements_text(hashtags) as hashtag,
                       COUNT(*) as cnt,
                       SUM(views_count) as total_views,
                       AVG(views_count)::int as avg_views
                FROM tagged
                GROUP BY json_array_elements_text(hashtags)
                ORDER BY cnt DESC LIMIT :limit
            """), {"limit": limit})
            rows = result.mappings().all()
            return {"tags": [{"tag": r["hashtag"], "count": r["cnt"], "total_views": r["total_views"] or 0, "avg_views": r["avg_views"] or 0} for r in rows]}
    except Exception as e:
        logger.error(f"/analytics/alltime-tags error: {e}"); traceback.print_exc()
        return json_response({"tags": [], "error": str(e)}, 500)


@app.get("/api/analytics/trends")
async def analytics_trends():
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=1))
            # This week
            this_week = await session.execute(text("""
                WITH tagged AS (
                    SELECT * FROM posts WHERE published_at > :since
                      AND hashtags IS NOT NULL
                      AND json_typeof(hashtags) = 'array'
                      AND json_array_length(hashtags) > 0
                )
                SELECT json_array_elements_text(hashtags) as hashtag, COUNT(*) as cnt
                FROM tagged GROUP BY json_array_elements_text(hashtags)
                ORDER BY cnt DESC LIMIT 30
            """), {"since": since})
            this_map = {r["hashtag"]: r["cnt"] for r in this_week.mappings().all()}

            # Last week (7-14 days ago)
            last_since = since - timedelta(days=7)
            last_week = await session.execute(text("""
                WITH tagged AS (
                    SELECT * FROM posts WHERE published_at > :last_since
                      AND published_at <= :since
                      AND hashtags IS NOT NULL
                      AND json_typeof(hashtags) = 'array'
                      AND json_array_length(hashtags) > 0
                )
                SELECT json_array_elements_text(hashtags) as hashtag, COUNT(*) as cnt
                FROM tagged GROUP BY json_array_elements_text(hashtags)
            """), {"last_since": last_since, "since": since})
            last_map = {r["hashtag"]: r["cnt"] for r in last_week.mappings().all()}

            # Calculate trends
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


@app.get("/api/analytics/posts-by-tag")
async def analytics_posts_by_tag(tag: str = Query(...), page: int = Query(1, ge=1), limit: int = Query(20, ge=1, le=50)):
    try:
        async with async_session() as session:
            result = await session.execute(text("""
                SELECT telegram_message_id, text, views_count, published_at
                FROM posts
                WHERE (hashtags)::jsonb @> (:tag_json)::jsonb
                ORDER BY published_at DESC
                LIMIT :limit OFFSET :offset
            """), {"tag_json": f'["{tag}"]', "limit": limit, "offset": (page - 1) * limit})
            rows = result.mappings().all()
            return {"posts": [{"id": r["telegram_message_id"], "text": r["text"], "views": r["views_count"] or 0, "published": r["published_at"].isoformat() if r["published_at"] else None} for r in rows]}
    except Exception as e:
        logger.error(f"/analytics/posts-by-tag error: {e}"); traceback.print_exc()
        return json_response({"posts": [], "error": str(e)}, 500)


@app.get("/api/analytics/tag-daily")
async def analytics_tag_daily(tag: str = Query(...), days: int = Query(90, ge=1, le=365)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            result = await session.execute(text("""
                SELECT ((published_at AT TIME ZONE 'UTC')::date)::text as d, COUNT(*) as cnt
                FROM posts WHERE published_at > :since
                  AND (hashtags)::jsonb @> (:tag_json)::jsonb
                GROUP BY d ORDER BY d
            """), {"since": since, "tag_json": f'["{tag}"]'})
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


@app.get("/api/analytics/tag-posts-by-day")
async def analytics_tag_posts_by_day(tag: str = Query(...), date: str = Query(...)):
    try:
        async with async_session() as session:
            result = await session.execute(text("""
                SELECT telegram_message_id, text, views_count, published_at
                FROM posts
                WHERE (hashtags)::jsonb @> (:tag_json)::jsonb
                  AND ((published_at AT TIME ZONE 'UTC')::date)::text = :date
                ORDER BY published_at DESC
                LIMIT 50
            """), {"tag_json": f'["{tag}"]', "date": date})
            rows = result.mappings().all()
            return {"posts": [{"id": r["telegram_message_id"], "text": r["text"], "views": r["views_count"] or 0, "published": r["published_at"].isoformat() if r["published_at"] else None} for r in rows]}
    except Exception as e:
        logger.error(f"/analytics/tag-posts-by-day error: {e}"); traceback.print_exc()
        return json_response({"posts": [], "error": str(e)}, 500)


@app.get("/api/analytics/export-csv")
async def analytics_export_csv():
    try:
        async with async_session() as session:
            result = await session.execute(text("""
                WITH tagged AS (
                    SELECT * FROM posts
                    WHERE hashtags IS NOT NULL
                      AND json_typeof(hashtags) = 'array'
                      AND json_array_length(hashtags) > 0
                )
                SELECT json_array_elements_text(hashtags) as hashtag,
                       COUNT(*) as cnt,
                       SUM(views_count) as total_views,
                       AVG(views_count)::int as avg_views
                FROM tagged
                GROUP BY json_array_elements_text(hashtags)
                ORDER BY cnt DESC
            """))
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

# ═══════════════════════════════════════════════════════════
# ═══ NEW: Sentiment & Intelligence API ═══════════════════
# ═══════════════════════════════════════════════════════════

@app.get("/api/sentiment/timeline")
async def sentiment_timeline(days: int = Query(7, ge=1, le=30)):
    """Daily sentiment scores: positive / negative / neutral / total"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            result = await session.execute(text("""
                SELECT
                    ((published_at AT TIME ZONE 'UTC')::date)::text as d,
                    text
                FROM posts
                WHERE published_at > :since AND text IS NOT NULL AND text != ''
                ORDER BY d
            """), {"since": since})
            rows = result.mappings().all()

            from collections import defaultdict
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

            return {
                "days": labels,
                "positive": pos_series,
                "negative": neg_series,
                "neutral": neu_series,
            }
    except Exception as e:
        logger.error(f"/sentiment/timeline error: {e}"); traceback.print_exc()
        return json_response({"days": [], "positive": [], "negative": [], "neutral": [], "error": str(e)}, 500)


@app.get("/api/sentiment/top-words")
async def sentiment_top_words(days: int = Query(7, ge=1, le=30), sentiment: str = Query("positive")):
    """Most frequent words from posts with given sentiment"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            result = await session.execute(text("""
                SELECT text FROM posts
                WHERE published_at > :since AND text IS NOT NULL AND text != ''
            """), {"since": since})
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


@app.get("/api/velocity/alerts")
async def velocity_alerts(days: int = Query(7, ge=1, le=30)):
    """Tickers with anomalous mention growth vs previous period"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            prev_since = since - timedelta(days=days)

            # Current period mentions
            curr = await session.execute(text("""
                WITH tagged AS (
                    SELECT * FROM posts WHERE published_at > :since
                      AND hashtags IS NOT NULL
                      AND json_typeof(hashtags) = 'array'
                      AND json_array_length(hashtags) > 0
                )
                SELECT json_array_elements_text(hashtags) as tag, COUNT(*) as cnt
                FROM tagged GROUP BY tag
            """), {"since": since})
            curr_map = {r["tag"]: r["cnt"] for r in curr.mappings().all()}

            # Previous period mentions
            prev = await session.execute(text("""
                WITH tagged AS (
                    SELECT * FROM posts
                    WHERE published_at > :prev_since AND published_at <= :since
                      AND hashtags IS NOT NULL
                      AND json_typeof(hashtags) = 'array'
                      AND json_array_length(hashtags) > 0
                )
                SELECT json_array_elements_text(hashtags) as tag, COUNT(*) as cnt
                FROM tagged GROUP BY tag
            """), {"prev_since": prev_since, "since": since})
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
                if c > p and pct >= 50:  # Only positive anomalies
                    alerts.append({"tag": tag, "current": c, "previous": p, "pct": pct})

            alerts.sort(key=lambda x: x["pct"], reverse=True)
            return {"alerts": alerts[:20]}
    except Exception as e:
        logger.error(f"/velocity/alerts error: {e}"); traceback.print_exc()
        return json_response({"alerts": [], "error": str(e)}, 500)


@app.get("/api/correlation/matrix")
async def correlation_matrix(days: int = Query(7, ge=1, le=30)):
    """Correlation matrix: which tags appear together in same posts"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            result = await session.execute(text("""
                WITH post_tags AS (
                    SELECT id, json_array_elements_text(hashtags) as tag
                    FROM posts WHERE published_at > :since
                      AND hashtags IS NOT NULL
                      AND json_typeof(hashtags) = 'array'
                      AND json_array_length(hashtags) > 1
                )
                SELECT pt1.tag as tag1, pt2.tag as tag2, COUNT(*) as cnt
                FROM post_tags pt1
                JOIN post_tags pt2 ON pt1.id = pt2.id AND pt1.tag < pt2.tag
                GROUP BY pt1.tag, pt2.tag
                HAVING COUNT(*) >= 2
                ORDER BY cnt DESC
                LIMIT 200
            """), {"since": since})
            rows = result.mappings().all()

            # Collect unique tags
            all_tags = set()
            pairs = []
            for r in rows:
                all_tags.add(r["tag1"])
                all_tags.add(r["tag2"])
                pairs.append({"t1": r["tag1"], "t2": r["tag2"], "count": r["cnt"]})

            tags = sorted(all_tags)[:30]  # Limit for display
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


@app.get("/api/premarket/intel")
async def premarket_intel(days: int = Query(7, ge=1, le=30)):
    """Posts segmented by time: pre-market / market hours / after-hours (MOEX: 10:00-18:45 MSK = 07:00-15:45 UTC)"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            result = await session.execute(text("""
                SELECT
                    ((published_at AT TIME ZONE 'UTC')::date)::text as d,
                    CASE
                        WHEN EXTRACT(HOUR FROM published_at)::int BETWEEN 7 AND 14
                             THEN 'market'
                        WHEN EXTRACT(HOUR FROM published_at)::int < 7
                             THEN 'premarket'
                        ELSE 'afterhours'
                    END as segment,
                    COUNT(*) as cnt,
                    SUM(views_count) as total_views
                FROM posts
                WHERE published_at > :since
                GROUP BY d, segment
                ORDER BY d, segment
            """), {"since": since})
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

            return {
                "days": labels,
                "premarket": pre_series,
                "market": market_series,
                "afterhours": after_series,
            }
    except Exception as e:
        logger.error(f"/premarket/intel error: {e}"); traceback.print_exc()
        return json_response({"days": [], "premarket": [], "market": [], "afterhours": [], "error": str(e)}, 500)


# ═══════════════════════════════════════════════════════════
# ═══ Viral & Cross-Market API (NO json_array_elements!) ══
# ═══════════════════════════════════════════════════════════

@app.get("/api/viral/posts")
async def viral_posts(days: int = Query(7, ge=1, le=30), limit: int = Query(10, ge=1, le=20)):
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            # Use indexed published_at filter, skip ORDER BY views_count (no index)
            # Instead: fetch recent posts, sort in Python
            result = await session.execute(text("""
                SELECT telegram_message_id, text, views_count, forwards_count, published_at
                FROM posts
                WHERE published_at > :since
                LIMIT 500
            """), {"since": since})
            rows = result.mappings().all()
            # Sort in Python
            posts = sorted(rows, key=lambda r: r["views_count"] or 0, reverse=True)[:limit]
            return {"posts": [{
                "id": r["telegram_message_id"],
                "text": r["text"],
                "views": r["views_count"] or 0,
                "forwards": r["forwards_count"] or 0,
                "published": r["published_at"].isoformat() if r["published_at"] else None,
                "channel": "markettwits",
            } for r in posts]}
    except Exception as e:
        logger.error(f"/viral/posts error: {e}")
        return json_response({"posts": [], "error": str(e)}, 500)


@app.get("/api/sector/rotation")
async def sector_rotation(days: int = Query(7, ge=1, le=30)):
    """NO json_array_elements — fetch hashtags json, parse in Python"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            result = await session.execute(text("""
                SELECT hashtags FROM posts
                WHERE published_at > :since
                  AND hashtags IS NOT NULL
                  AND json_typeof(hashtags) = 'array'
                  AND json_array_length(hashtags) > 0
                LIMIT 3000
            """), {"since": since})
            rows = result.mappings().all()

            from collections import defaultdict, Counter
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


@app.get("/api/wordcloud")
async def wordcloud_data(days: int = Query(7, ge=1, le=30), limit: int = Query(50, ge=1, le=100)):
    """NO json_array_elements — fetch hashtags json, count in Python"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            result = await session.execute(text("""
                SELECT hashtags FROM posts
                WHERE published_at > :since
                  AND hashtags IS NOT NULL
                  AND json_typeof(hashtags) = 'array'
                  AND json_array_length(hashtags) > 0
                LIMIT 3000
            """), {"since": since})
            rows = result.mappings().all()

            from collections import Counter
            counter = Counter()
            for r in rows:
                for tag in (r["hashtags"] or []):
                    counter[tag] += 1

            return {"words": [{"text": w, "count": c} for w, c in counter.most_common(limit)]}
    except Exception as e:
        logger.error(f"/wordcloud error: {e}")
        return json_response({"words": [], "error": str(e)}, 500)


@app.get("/api/crossmarket/links")
async def crossmarket_links(days: int = Query(7, ge=1, le=30)):
    """Fetch recent posts, filter in Python — no ILIKE on text columns"""
    try:
        async with async_session() as session:
            since = await get_since(session, timedelta(days=days))
            macro_keywords = ["нефть", "brent", "usd", "доллар", "eur", "рубль", "ставка", "цб", "moex"]

            # Simple fetch by date — Python filtering
            result = await session.execute(text("""
                SELECT telegram_message_id, text, views_count, published_at, hashtags
                FROM posts
                WHERE published_at > :since
                LIMIT 500
            """), {"since": since})
            rows = result.mappings().all()

            from collections import Counter
            ticker_counter = Counter()
            posts_out = []
            for r in rows:
                text_lower = (r["text"] or "").lower()
                hashtags = r["hashtags"] or []
                # Check if post mentions macro keywords
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
                    "channel": "markettwits",
                })
                for tag in hashtags:
                    ticker_counter[tag] += 1

            # Sort by views in Python
            posts_out.sort(key=lambda p: p["views"], reverse=True)
            tickers = [{"tag": t, "count": c} for t, c in ticker_counter.most_common(15)]
            return {"posts": posts_out[:30], "tickers": tickers}
    except Exception as e:
        logger.error(f"/crossmarket/links error: {e}")
        return json_response({"posts": [], "tickers": [], "error": str(e)}, 500)

