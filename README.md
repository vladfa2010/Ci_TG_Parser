# CITG v3 — Telegram Parser для каналов и чатов

> Сбор сообщений из закрытых Telegram-каналов и чатов в единую ленту. Безопасный парсинг с защитой от FloodWait.

---

## Что нового в v3

| Функция | Описание |
|---------|----------|
| **Каналы + Чаты** | Парсинг и каналов (`Channel`), и базовых групп (`Chat`) |
| **Автоопределение типа** | При добавлении автоматически определяет Chat vs Channel |
| **access_hash кэш** | Только для каналов — `InputPeerChannel` из БД |
| **InputPeerChat** | Для базовых групп — не требует access_hash |
| **Fallback get_entity** | Если access_hash нет — автоматический resolve |
| **Adaptive Rate Limiter** | Динамическая задержка: 0.5–30 сек |
| **Circuit Breaker** | 3 ошибки → канал пропускается 30 минут |
| **🗑 Очистить все** | Кнопка в вебе — очистка базы |
| **Auto-migration** | Парсер сам добавляет колонки в БД |

---

## Chat vs Channel — важно понимать

В Telegram API есть два разных типа групп:

| Тип | Telegram API | InputPeer | access_hash | Как узнать |
|-----|--------------|-----------|-------------|------------|
| **Chat** (basic group) | `Chat` | `InputPeerChat(chat_id)` | **Не нужен** | До 200 чел, нельзя сделать публичным |
| **Channel** (broadcast) | `Channel` | `InputPeerChannel(id, hash)` | **Нужен** | Канал (только админы пишут) |
| **Channel** (megagroup) | `Channel` | `InputPeerChannel(id, hash)` | **Нужен** | Супергруппа (все пишут) |

**Парсер автоматически определяет тип** при добавлении и использует правильный `InputPeer`.

---

## Как добавить канал/чат

### Поддерживаемые форматы

| Формат | Пример | Что произойдёт |
|--------|--------|---------------|
| Telegram Web (канал) | `https://web.telegram.org/a/#-100740684703` | ✅ Channel `-100740684703` |
| Telegram Web (чат) | `https://web.telegram.org/a/#-740684703` | ✅ Chat (basic group) |
| t.me ссылка | `https://t.me/c/3147415698` | ✅ Channel `-1003147415698` |
| Username | `@markettwits` | ✅ Public channel |
| Numeric ID | `3147415698` | ✅ Channel `-1003147415698` |

### Пошагово

1. Открой **/channels** в вебе
2. Вставь ссылку или ID
3. Нажми **"+ Добавить канал"**
4. Убедись что тип определился правильно (видно в списке)
5. Нажми **"🔄 Запустить парсинг"**

### ⚠️ Если парсинг не работает

**Симптом:** `NOT found in get_dialogs()` или `access_hash` ошибка

**Для Channel (супергруппы/каналы):**
```
1. Удалите канал
2. Добавьте заново по ПОЛНОЙ ссылке из Telegram Web
   (убедитесь что в ссылке есть -100)
3. Запустите парсинг 🔄
```

**Для Chat (базовые группы):**
```
1. Удалите чат
2. Добавьте заново — парсер автоматически определит тип "chat"
3. Для чатов не нужен access_hash — должно работать сразу
```

**Почему:** Для `Channel` нужен `access_hash` из `get_dialogs()`. 
Если канал недавно создан или вы не открывали его в Telegram — 
его может не быть в диалогах. Переоткройте канал в Telegram и попробуйте снова.

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
     └───────────────┘ └───────────┘ └───────────┘
```

**Важно:** веб и крон — **разные процессы**. Оба подключаются к одной БД.

---

## Безопасность парсинга

| Защита | Как работает |
|--------|-------------|
| `get_dialogs()` → кэш | Один bulk call на старте |
| `access_hash` в БД | Кэшируется между запусками (только для Channel) |
| `InputPeerChannel` из БД | Без `get_entity()` для известных каналов |
| `InputPeerChat` | Для чатов — access_hash не нужен |
| `message.post_author` | Имя автора поста без API-вызова |
| `message.sender_id` | Для чатов — ID без resolve |
| Cron `*/20 * * * *` | Не чаще раза в 20 минут |
| Sequential | 1 канал за раз |
| Jitter 0–10 сек | Случайные задержки |
| Adaptive delay | 0.5–30 сек по реакции Telegram |
| Circuit breaker | 3 ошибки → 30 мин паузы |

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

### Как узнать тип — Chat или Channel?

После добавления в списке каналов будет написан тип:
- `chat` = базовая группа (`InputPeerChat`)
- `private` = закрытый канал (`InputPeerChannel`)
- `public` = публичный канал (`InputPeerChannel`)

### Для чего нужен access_hash?

Только для **Channel** (broadcast и megagroup). Это внутренний хеш Telegram для доступа к каналу. Сохраняется в БД при первом успешном `get_dialogs()`.

**Chat (basic group) не требует access_hash** — использует `InputPeerChat(chat_id)`.

### Чат не парсится — `NOT found in get_dialogs()`

Чат должен быть в ваших диалогах. Откройте чат в Telegram (клиент или веб) и попробуйте снова. Для чатов `get_dialogs()` возвращает `PeerChat(chat_id)`.

### Канал не парсится — `no access_hash`

Канал должен быть в ваших диалогах и вы должны быть участником. Переоткройте канал в Telegram клиенте и запустите парсинг снова.

### 🗑 Очистить все не работает

Очистка идёт по одной таблице за раз с паузой 1 секунда. Если PostgreSQL ушёл в recovery — подождите 30 секунд.

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
