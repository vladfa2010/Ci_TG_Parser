# CITG v3 — Telegram Parser для каналов и чатов

[![GitHub](https://img.shields.io/badge/GitHub-vladfa2010%2FCi__TG__Parser-181717?logo=github)](https://github.com/vladfa2010/Ci_TG_Parser) [![Branch](https://img.shields.io/badge/branch-v2--citg--rebrand-blue)](https://github.com/vladfa2010/Ci_TG_Parser/tree/v2-citg-rebrand)

> Сбор сообщений из Telegram-каналов и чатов в единую ленту. Безопасный парсинг с защитой от FloodWait.

---

## Возможности v3

| Функция | Описание |
|---------|----------|
| **Каналы + Чаты** | Парсинг broadcast-каналов, супергрупп и базовых чатов |
| **Автоопределение типа** | `isinstance(entity, Chat/Channel)` при добавлении |
| **access_hash кэш** | Для каналов — кэшируется в БД через `get_dialogs()` |
| **InputPeerChat** | Для базовых чатов — не требует `access_hash` |
| **Fallback get_entity** | Если `access_hash` отсутствует — автоматический resolve |
| **Adaptive Rate Limiter** | Задержка 0.5–30 сек по реакции Telegram |
| **Circuit Breaker** | 3 ошибки → пропуск 30 мин |
| **🗑 Очистить все** | Очистка базы с защитой от двойного нажатия и крона |
| **Только существующие каналы** | `sync_dialogs()` обновляет, но **не создаёт** каналы |
| **SenderResolver** | 4-уровневый кэш имён отправителей: post_author → memory → PostgreSQL → get_entity |
| **Views для чатов** | Отображается `—` вместо `0` (у чатов нет счётчика просмотров) |

---

## Chat vs Channel — критично понимать

| | **Chat (basic group)** | **Channel (broadcast)** | **Channel (supergroup)** |
|--|------------------------|-------------------------|--------------------------|
| **Telegram API** | `Chat` | `Channel` | `Channel` |
| **InputPeer** | `InputPeerChat(id)` | `InputPeerChannel(id, hash)` | `InputPeerChannel(id, hash)` |
| **access_hash** | **Не нужен** | **Нужен** | **Нужен** |
| **telegram_id** | Положительный `740684703` | Отрицательный `-100740684703` | Отрицательный `-100740684703` |
| **Как узнать** | До 200 чел, нельзя сделать публичным | Только админы пишут | Все пишут |

**Парсер автоматически определяет тип** по `telegram_id`:
- `tid > 0` → `InputPeerChat` (чат)
- `tid < 0` → `InputPeerChannel` (канал)

---

## Как мы решали проблему Chat vs Channel (подробно)

### Проблема

`CIFRA WORLD` — закрытый чат из Telegram Web (`https://web.telegram.org/a/#-740684703`). Парсер v2 работал только с каналами (`Channel`), а чаты (`Chat`) — нет. После добавления чата парсинг падал с ошибкой `no access_hash`.

### Почему падало

**Ошибка 1:** `sync_dialogs()` фильтровал только `isinstance(entity, Channel)` — чаты пропускались:
```python
# Было — Chat игнорировались
if not isinstance(entity, Channel):
    continue
```

**Ошибка 2:** `build_input_peer()` пытался создать `InputPeerChannel` для чатов — требовал `access_hash`, которого у `Chat` нет:
```python
# Было — Chat падали здесь
if not channel.access_hash:
    raise ValueError("no access_hash")  # Chat не имеет access_hash!
return InputPeerChannel(id, access_hash)
```

**Ошибка 3:** Проверка `channel_type == 'chat'` через SQLAlchemy `getattr()` не работала — атрибут не загружался в новой сессии.

**Ошибка 4:** `_upsert_channel()` создавал ВСЕ 2446 каналов из `get_dialogs()` в БД пользователя.

**Ошибка 5:** `channel_add` для отрицательных ID пробовал `-100` префикс сначала, что для `Chat` вызывало `Invalid object ID for a chat`.

### Решение

| Компонент | Что изменили | Почему |
|-----------|-------------|--------|
| `sync_dialogs()` | Принимает и `Chat`, и `Channel` | Чаты тоже нужно обновлять |
| `sync_dialogs()` | Не создаёт новые каналы (`return` вместо `session.add`) | Предотвращает 2446 лишних каналов |
| `build_input_peer()` | `if tid > 0: return InputPeerChat(tid)` | Chat — положительный ID, не нужен access_hash |
| `build_input_peer()` | `if tid < 0: return InputPeerChannel(...)` | Только для Channel |
| `channel_add` | `isinstance(entity, Chat)` → `channel_type='chat'` | Определяем тип при добавлении |
| `channel_add` | `get_entity(-740684703)` для Chat (без `-100`) | `-100` только для Channel |
| `web.py` | `import asyncio` (был потерян) | `asyncio.create_task()` требует импорт |

### Ключевой инсайт

```python
# Определение типа — только по знаку telegram_id:
if telegram_id > 0:
    # Chat: 740684703 → InputPeerChat(740684703) — access_hash не нужен
    return InputPeerChat(telegram_id)
else:
    # Channel: -100740684703 → InputPeerChannel(id, access_hash)
    return InputPeerChannel(channel_id, access_hash)
```

---

## Как добавить канал/чат

### Поддерживаемые форматы

| Формат | Пример | Результат |
|--------|--------|-----------|
| Telegram Web URL | `https://web.telegram.org/a/#-740684703` | Chat или Channel |
| t.me ссылка | `https://t.me/c/3147415698` | Channel `-1003147415698` |
| Username | `@markettwits` | Public channel |
| Numeric ID | `3147415698` | Channel `-1003147415698` |

### Пошагово

1. Открой **/channels** в вебе
2. Вставь ссылку или ID
3. Нажми **"+ Добавить канал"**
4. Убедись что тип определился правильно (видно в списке)
5. Нажми **"🔄 Запустить парсинг"**

### ⚠️ Если парсинг не работает

**Симптом:** `no access_hash` или `NOT found in get_dialogs()`

**Для Channel:**
```
1. Удалите канал
2. Добавьте заново по ПОЛНОЙ ссылке из Telegram Web
3. Запустите парсинг 🔄
```

**Для Chat:**
```
1. Удалите чат
2. Добавьте заново — тип определится автоматически
3. Для чатов не нужен access_hash — должно работать сразу
```

**Почему:** Для `Channel` нужен `access_hash` из `get_dialogs()`. Если канал недавно создан — его может не быть в диалогах. Переоткройте в Telegram и попробуйте снова.

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

**Важно:** веб и крон — **разные процесса**. Оба подключаются к одной БД.

## Управление списком каналов

> **Парсер работает строго со списком каналов, который вы контролируете через Web UI.**
> Ни cron, ни кнопка «🔄 Запустить парсинг» не добавляют новые каналы автоматически.

### Жизненный цикл канала

```
Добавление через Web UI  →  Канал в БД (is_active=true)  →  Cron/кнопка парсит
        ↑___________________________|                             |
        └_______________________ отключение/удаление в Web UI ←___┘
```

### Где задаётся список для парсинга

Только таблица `channels` в PostgreSQL, поле `is_active`:
- `is_active = TRUE` — канал участвует в парсинге.
- `is_active = FALSE` — канал в БД, но не парсится.
- Удаление — полностью стирает канал и его посты.

### Что делает `sync_dialogs()` при старте парсера

Каждый запуск cron начинается с `sync_dialogs()`:

1. **Получает все диалоги** аккаунта через `get_dialogs()`.
   - Это ~1 bulk-запрос к Telegram.
   - Нужно, чтобы обновить `access_hash` для private-каналов.
2. **Обновляет `access_hash`** для каналов, которые **уже есть в вашей БД**.
   - Для `Channel` и `Chat`.
   - Без `access_hash` private-каналы не распарсятся.
3. **Пропускает** каналы, которых нет в БД.
   - **Не создаёт новые записи.**
   - Раньше создавались все broadcast-каналы из диалогов — это исправлено.

```python
# parser.py, sync_dialogs()
for dialog in dialogs:
    entity = dialog.entity
    if not isinstance(entity, TlChannel):
        continue

    # Обновляем access_hash, но НЕ создаём новый канал
    await self._upsert_channel(
        session, telegram_id, channel_id,
        entity.title, entity.username, access_hash,
        allow_create=False,  # ← ключевое изменение
    )
```

### Почему это важно

Telegram-аккаунт может состоять в тысячах диалогов. Если парсер создавал каналы для всех диалогов, в БД появлялись сотни нежелательных активных каналов, и cron пытался парсить всё подряд.

Теперь:
- **Вы сами решаете**, что парсить.
- **Добавление только через `/channels`**.
- **Cron не создаёт, не активирует и не удаляет** каналы — только читает `is_active`.

### Если в БД остались лишние каналы от старой версии

Если ранее `sync_dialogs()` автоматически создал много активных каналов, их нужно деактивировать или удалить вручную.

**Способ 1 — через Web UI (рекомендуется):**
1. Открой `/channels`.
2. Найди лишние каналы.
3. Выключи переключатель `is_active` или нажми **Удалить**.

**Способ 2 — массово через SQL (Render PostgreSQL Shell):**
1. Render Dashboard → `citg-db` → вкладка **Shell**.
2. Выполни SQL из файла [`scripts/deactivate_auto_created_channels.sql`](scripts/deactivate_auto_created_channels.sql).

**Способ 3 — массово через скрипт локально:**
```bash
export DATABASE_URL="postgresql://citg_db_user:..."
python3 scripts/deactivate_auto_created_channels.py
```

> **Внимание:** перед массовой деактивацией сделай бэкап или хотя бы посмотри список через `SELECT id, title, is_active, created_at FROM channels WHERE is_active = TRUE ORDER BY created_at DESC;`.

### Что будет, если БД пустая

Если в `channels` нет активных записей, cron завершится успешно, но напишет:

```
Каналов к парсингу: 0
Нет активных каналов для парсинга
```

Это нормально — добавь каналы через Web UI.

### Периодичность cron

- **Cron**: настраивается в Render Dashboard (`citg-web` → Settings → Cron Job). В текущем деплое — `*/5 * * * *`.
- **Web-кнопка**: запускает парсинг по требованию.
- Оба работают **только с активными каналами из БД**.

> Слишком частый парсинг многих каналов может вызвать `FloodWait`. Если каналов много — увеличьте интервал в Render Dashboard.

---

## Безопасность парсинга

| Защита | Как работает |
|--------|-------------|
| `get_dialogs()` → кэш | Один bulk call, потом 0 вызовов для известных |
| `access_hash` в БД | Кэшируется между запусками (только Channel) |
| `InputPeerChat` | Для чатов — `access_hash` не нужен |
| `InputPeerChannel` из БД | Без `get_entity()` для известных каналов |
| `SenderResolver` | Кэш имён отправителей в PostgreSQL (чаты) |
| Cron `*/20 * * * *` | Не чаще раза в 20 минут |
| Sequential | 1 канал за раз |
| Jitter 0–10 сек | Случайные задержки |
| Adaptive delay | 0.5–30 сек |
| Circuit breaker | 3 ошибки → 30 мин |

## Sender Name — кто отправил сообщение ✅ РАБОТАЕТ

| Источник | Каналы (Channel) | Чаты (Chat) |
|----------|-----------------|-------------|
| **Механизм** | `message.post_author` | `SenderResolver` (4-уровневый кэш) |
| **API calls** | 0 | 1 на нового отправителя, 0 на известных |
| **Где хранится** | `Post.sender_name` | `Post.sender_name` + `senders` таблица |

### 4-уровневый кэш (SenderResolver)

```
1. msg.post_author ? ──да──→ return (каналы)
        │
        нет (чаты)
        ▼
2. Memory cache[sender_id] ? ──да──→ return
        │
        нет
        ▼
3. БД: SELECT senders WHERE telegram_user_id=? ──да──→ кэшируем в memory, return
        │
        нет
        ▼
4. rate_limiter.before_call()
   get_entity(sender_id) ──→ INSERT senders ──→ кэшируем, return
```

### Таблица `senders`

| Колонка | Тип | Описание |
|---------|-----|----------|
| `telegram_user_id` | `BIGINT PK` | Telegram ID пользователя |
| `first_name` | `VARCHAR(255)` | Имя |
| `last_name` | `VARCHAR(255)` | Фамилия |
| `username` | `VARCHAR(100)` | @username |
| `resolved_at` | `TIMESTAMPTZ` | Когда резолвили |

### Почему это важно

В чатах (`Chat`) `message.post_author` всегда `None` — Telegram не передаёт имя автора. Раньше использовался `message.sender` (API call на каждое сообщение) — это вызывал `FloodWait`.

Новый `SenderResolver`:
- **Первый прогон**: резолвит новых отправителей через `get_entity()` (с rate limiter)
- **Второй прогон**: **0 API calls** — все имена из memory cache + PostgreSQL
- **Кэш переживает рестарт** контейнера на Render

### Views (просмотры)

| Тип | Views |
|-----|-------|
| Каналы (Channel) | Реальное число из Telegram |
| Чаты (Chat) | `0` → в вебе отображается как `—` |


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

### 4. Генерация TG_STRING_SESSION

```bash
pip install telethon
python generate_session.py
# Введи номер → код из Telegram → скопируй строку
```

---

## FAQ

### Парсер добавляет все каналы из аккаунта?

**Нет.** Начиная с последних версий, `sync_dialogs()` **только обновляет** `access_hash` для каналов уже в вашей БД. Новые каналы **не создаются**.

### Как узнать тип — Chat или Channel?

В списке каналов (/channels) видно:
- `chat` = базовая группа (`InputPeerChat`)
- `private` = закрытый канал (`InputPeerChannel`)
- `public` = публичный канал (`InputPeerChannel`)

### Для чего нужен access_hash?

Только для **Channel** (broadcast и supergroup). Внутренний хеш Telegram. Сохраняется при первом `get_dialogs()`.

**Chat не требует access_hash** — использует `InputPeerChat(id)`.

### Старые посты без имени отправителя?

**Нормально.** Старые посты (до внедрения SenderResolver) имеют `sender_name = NULL`.

| Что видишь | Почему | Когда исправится |
|------------|--------|-----------------|
| Новые сообщения → `by Имя` | ✅ SenderResolver работает | Сразу |
| Старые сообщения → пусто | sender_name = NULL (не парсили раньше) | Никогда (инкрементальный парсинг) |

**Исправить старые:** Очистить все (🗑) + перепарсить — имена появятся для всех.
**Или:** Просто ждать — новые сообщения приходят с именами, старые уходят вниз ленты.

### 🗑 Очистить все — безопасно?

Да. Очищает все таблицы кроме `users` (ваш логин). Кнопка блокируется на время очистки, крон пропускает прогон. Каждая таблица очищается отдельно с паузой 1 сек.

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
