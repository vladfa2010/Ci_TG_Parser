# CITG v3 — Telegram Parser для 20+ приватных каналов

> Сбор сообщений из закрытых Telegram-каналов в единую ленту. Безопасный парсинг с защитой от FloodWait.

---

## Что нового в v3

| Функция | Описание |
|---------|----------|
| **20 приватных каналов** | Парсинг закрытых каналов через `access_hash` кэш в БД |
| **Zero `get_entity()`** | `InputPeerChannel` строится из БД — нет лишних API-вызовов |
| **Adaptive Rate Limiter** | Динамическая задержка: ускоряется при успехе, замедляется при ошибках |
| **Circuit Breaker** | 3 ошибки → канал пропускается 30 минут. FloodWait > 60сек → глобальная пауза |
| **Auto-migration** | Парсер сам добавляет недостающие колонки в БД — не нужен ручной SQL |
| **🗑 Очистить все** | Кнопка в вебе — мгновенная очистка базы (с защитой от двойного нажатия) |
| **Web ↔ Cron lock** | Крон не парсит во время очистки из веба |
| **Jitter** | Случайные задержки 0-10 сек — имитация human-like поведения |

## Архитектура

```
                    ┌──────────────────┐
                    │   PostgreSQL 16  │
                    │   (Render)       │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
     ┌────────▼──────┐ ┌────▼──────┐ ┌────▼──────┐
     │   citg-web    │ │ citg-cron │ │  cleanup  │
     │   FastAPI     │ │  parser   │ │  (ручная) │
     │   :10000      │ │ */20 min  │ │  🗑 btn   │
     │               │ │           │ │           │
     │  Dashboard    │ │ 20 ch     │ │ TRUNCATE  │
     │  Auth         │ │ sequential│ │  all data │
     └───────────────┘ └───────────┘ └───────────┘
```

## Безопасность парсинга (почему не банит)

| Защита | Как работает |
|--------|-------------|
| `get_dialogs()` → кэш | Один bulk call на старте, потом 0 `get_entity()` |
| `InputPeerChannel` из БД | `access_hash` кэшируется между запусками |
| `message.post_author` | Имя автора без API-вызова (не `message.sender`) |
| Cron `*/20 * * * *` | Не чаще раза в 20 минут |
| Sequential (1 канал) | Не параллельно — последовательно |
| Jitter 0-10 сек | Случайные задержки между каналами |
| Adaptive delay | 0.5-30 сек адаптивно по реакции Telegram |
| Circuit breaker | 3 ошибки → 30 мин паузы для канала |
| Global cooldown | FloodWait > 60сек → остановка 30 мин |

## Deploy на Render

### 1. PostgreSQL

**New → PostgreSQL**
- Name: `citg-db`
- Plan: **Starter**
- Скопируй Internal Database URL

### 2. Web Service (дашборд)

**New → Web Service**
- Source: GitHub → `Ci_TG_Parser`
- Branch: **`v2-citg-rebrand`**
- Runtime: **Docker**
- Name: `citg-web`
- Plan: **Starter**

**Env vars:**
```
APP_MODE=web
PORT=10000
DATABASE_URL=<из citg-db>
```

**Secrets:**
```
TG_API_ID=<твой API ID>
TG_API_HASH=<твой API hash>
TG_STRING_SESSION=<строка сессии>
```

### 3. Cron Job (парсер)

**New → Cron Job**
- Source: GitHub → `Ci_TG_Parser`
- Branch: `v2-citg-rebrand`
- Runtime: **Docker**
- Name: `citg-cron`
- Schedule: `*/20 * * * *`

**Env vars:**
```
DATABASE_URL=<из citg-db>
SCHEDULE_MODE=1
CHANNEL_DELAY_SEC=10
API_CALL_DELAY_MS=1000
BATCH_COMMIT_SIZE=50
JITTER_SEC=10
```

**Secrets:**
```
TG_API_ID=<твой API ID>
TG_API_HASH=<твой API hash>
TG_STRING_SESSION=<строка сессии>
```

### 4. Генерация TG_STRING_SESSION

```bash
pip install telethon
python generate_session.py
# Введи номер → код из Telegram → скопируй строку
```

### 5. Миграция БД (автоматическая)

Парсер сам добавляет недостающие колонки при старте:
- `channels.access_hash` — кэш для `InputPeerChannel`
- `channels.entity_resolved_at` — когда обновлён кэш
- `parse_state` — таблица состояния парсера

Не нужен ручной SQL!

### 6. Добавление каналов

1. Открой `/channels` в вебе
2. Введи username или numeric ID канала
3. Нажми **"+ Добавить канал"**
4. Нажми **"🔄 Запустить парсинг"**

## Управление каналами

### Через веб (/channels)

| Кнопка | Действие |
|--------|----------|
| **+ Добавить канал** | Добавить по username/numeric ID |
| **🔄 Запустить парсинг** | Запустить парсер вручную |
| **🗑 Очистить все** | Удалить ВСЕ каналы, посты, логи (конфирм) |

### Очистка базы

**🗑 Очистить все:**
1. `confirm()` диалог — подтверждение
2. Проверка — не идёт ли парсинг (крон заблокирован)
3. Фоновая очистка через `DELETE FROM`
4. Крон пропускает прогон пока идёт очистка
5. Страница перезагружается автоматически

## API Endpoints

```bash
# Добавить канал
curl -u "vlad:!1234567890" \
  "https://your-app.onrender.com/api/channel/add?identifier=3147415698"

# Список каналов
curl -u "vlad:!1234567890" \
  "https://your-app.onrender.com/api/channels"

# Запустить парсинг
curl -u "vlad:!1234567890" \
  "https://your-app.onrender.com/api/parse/trigger"

# Статус парсинга
curl -u "vlad:!1234567890" \
  "https://your-app.onrender.com/api/parse/status"

# Очистить все данные
curl -u "vlad:!1234567890" -X POST \
  "https://your-app.onrender.com/api/channel/clear-all"
```

## Мониторинг

### Веб-статус
- `/api/parse/status` — текущий прогресс парсинга
- `/api/clear-all/status` — статус очистки
- `/api/stats` — общая статистика

### Логи Render
```
[citg-cron] Каналов к парсингу: 20
[citg-cron] Обработано=150, Новых=12, Ошибок=0
[citg-cron] RateLimiter: delay=1.2s, flood_waits=0
[citg-cron] CircuitBreaker: global_cooldown=False
```

### SQL (если нужно)
```sql
-- Сколько каналов
SELECT COUNT(*) FROM channels WHERE is_active = TRUE;

-- Сколько постов
SELECT COUNT(*) FROM posts;

-- Когда последний прогон
SELECT * FROM parse_state;
```

## Стек

Python 3.11, FastAPI, SQLAlchemy 2.0 async, Telethon (MTProto), PostgreSQL 16, ECharts 5.5, Docker
