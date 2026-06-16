# CITG v3 — Telegram Parser для каналов и чатов

> Сбор сообщений из закрытых Telegram-каналов и чатов в единую ленту. Безопасный парсинг с защитой от FloodWait.

---

## Что нового в v3

| Функция | Описание |
|---------|----------|
| **Каналы + чаты** | Парсинг и каналов (broadcast), и чатов/групп (megagroup) |
| **access_hash кэш** | `InputPeerChannel` / `InputPeerChat` из БД — минимум API-вызовов |
| **Fallback get_entity** | Если access_hash нет в БД — автоматический resolve через `get_entity(PeerChannel())` |
| **Adaptive Rate Limiter** | Динамическая задержка: 0.5–30 сек по реакции Telegram |
| **Circuit Breaker** | 3 ошибки → канал пропускается 30 минут |
| **🗑 Очистить все** | Кнопка в вебе — очистка базы с защитой от двойного нажатия и крона |
| **Auto-migration** | Парсер сам добавляет колонки в БД при старте |

---

## Как добавить канал (важно!)

### Поддерживаемые форматы

| Формат | Пример | Результат |
|--------|--------|-----------|
| Telegram Web | `https://web.telegram.org/a/#-740684703` | ✅ Канал `-100740684703` |
| t.me ссылка | `https://t.me/c/3147415698` | ✅ Канал `-1003147415698` |
| Username | `@markettwits` или `markettwits` | ✅ Публичный канал |
| Numeric ID | `3147415698` | ✅ Канал `-1003147415698` |

### Пошагово

1. Открой **/channels** в вебе
2. Вставь ссылку или ID в поле
3. Нажми **"+ Добавить канал"**
4. Нажми **"🔄 Запустить парсинг"**

### ⚠️ Если парсинг не работает

**Симптом:** `Канал не имеет access_hash` или `NOT found in get_dialogs()`

**Решение:**
```
1. Удалите канал (🗑 напротив канала в списке)
2. Добавьте заново по ссылке из Telegram Web
3. Запустите парсинг 🔄
```

**Почему:** Telegram ID каналов должен быть в формате `-100XXXXXXX`. 
Если ID без `-100` (например просто `-740684703`), `get_dialogs()` 
не находит канал — нужен правильный формат.

---

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
     │   FastAPI     │ │  parser   │ │  🗑 btn   │
     │   :10000      │ │ */20 min  │ │           │
     │               │ │           │ │           │
     │  Dashboard    │ │ 20 ch     │ │ DELETE    │
     │  Auth         │ │ seqential │ │  per tbl  │
     └───────────────┘ └───────────┘ └───────────┘
```

**Важно:** веб и крон — **разные процессы**. Оба подключаются к одной БД.

---

## Безопасность парсинга

| Защита | Как работает |
|--------|-------------|
| `get_dialogs()` → кэш | Один bulk call, потом 0 `get_entity()` для известных каналов |
| `access_hash` в БД | Кэшируется между запусками, не теряется при рестарте |
| `InputPeerChannel` из БД | Без `get_entity()` — напрямую из кэша |
| `message.post_author` | Имя автора поста без API-вызова |
| `message.sender_id` | Для чатов — ID без resolve |
| Cron `*/20 * * * *` | Не чаще раза в 20 минут |
| Sequential | 1 канал за раз, не параллельно |
| Jitter 0–10 сек | Случайные задержки между каналами |
| Adaptive delay | 0.5–30 сек по реакции Telegram |
| FloodWait → cooldown | При >60 сек — глобальная пауза 30 мин |

---

## Deploy на Render

### 1. PostgreSQL

**New → PostgreSQL**
- Name: `citg-db`
- Plan: **Starter**
- Скопируй Internal Database URL

### 2. Web Service

**New → Web Service**
- GitHub → `Ci_TG_Parser`
- Branch: **`v2-citg-rebrand`**
- Runtime: **Docker**
- Name: `citg-web`

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

### 3. Cron Job

**New → Cron Job**
- GitHub → `Ci_TG_Parser`
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

**Secrets:** (те же что и для веба)

### 4. Генерация TG_STRING_SESSION

```bash
pip install telethon
python generate_session.py
# Введи номер → код из Telegram → скопируй строку
```

### 5. Миграция БД

Парсер сам добавляет колонки при старте. Ручной SQL не нужен.

---

## FAQ

### Парсинг долго ждёт на `get_dialogs()`

**Нормально.** Telegram возвращает `FloodWait` на `get_dialogs()` при частых запросах. Парсер ждёт автоматически (видно в логах: `Sleeping for 23s on GetDialogsRequest flood wait`).

### `Entity is not a Channel` или `Could not find the input entity`

Канал добавлен с неправильным ID. Удалите и добавьте заново по ссылке из Telegram Web.

### Кнопка 🗑 не очищает базу

Проверьте логи. Очистка идёт по одной таблице за раз с паузой 1 секунда. Если PostgreSQL ушёл в recovery — подождите 30 секунд и попробуйте снова.

### Крон и 🗑 одновременно

Кнопка 🗑 блокирует крон через `parse_state.global_lock`. Крон пропустит прогон если идёт очистка.

---

## API

```bash
# Добавить канал
curl -u "vlad:pass" "https://.../api/channel/add?identifier=-740684703"

# Список каналов
curl -u "vlad:pass" "https://.../api/channels"

# Запустить парсинг
curl -u "vlad:pass" "https://.../api/parse/trigger"

# Статус парсинга
curl -u "vlad:pass" "https://.../api/parse/status"

# Очистить все данные
curl -u "vlad:pass" -X POST "https://.../api/channel/clear-all"
```

---

## Стек

Python 3.11, FastAPI, SQLAlchemy 2.0 async, Telethon (MTProto), PostgreSQL 16, Docker
