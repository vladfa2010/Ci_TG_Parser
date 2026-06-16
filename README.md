# CITG v2 — Multi-Channel Telegram Parser

> Параллельный сбор и аналитика данных из приватных и публичных Telegram-каналов. Веб-дашборд с авторизацией, управлением каналами и cross-channel аналитикой.

---

## Что нового в v2

| Функция | Описание |
|---------|----------|
| **Многоканальный парсинг** | До 10 каналов параллельно (asyncio.gather + Semaphore), до 50 каналов всего |
| **Приватные каналы** | Поддержка `https://t.me/c/{id}` — каналы без @username |
| **Управление каналами через веб** | Добавление / включение / выключение / удаление через UI на `/channels` |
| **Авторизация** | Cookie-сессии для браузера + Basic Auth для API. Предустановленный юзер `vlad` |
| **Фильтрация по каналу** | Вкладка Posts: dropdown фильтр по каналу (публичные + приватные) |
| **Парсер** | Последовательный парсинг, PeerChannel для приватных каналов, изоляция ошибок, FloodWait защита |
| **Карточки постов** | Название канала, отправитель, компактные просмотры (K/M), хэштеги |
| **Глобальная дедупликация** | Посты дедуплицируются по хэшу текста между всеми каналами |
| **Cross-channel аналитика** | Сравнение каналов по активности, просмотрам, хэштегам |

---

## Архитектура

```
                    +------------------+
                    |   PostgreSQL 16  |
                    |   citg-db        |
                    +--------+---------+
                             |
              +--------------+--------------+
              |                             |
     +--------v--------+         +----------v---------+
     |   citg-web      |         |   citg-cron        |
     |   FastAPI       |         |   Telegram Parser  |
     |   :10000        |         |   */5 min          |
     |                 |         |                    |
     |  Dashboard SPA  |         |  10 parallel       |
     |  Auth (cookie)  |         |  channels          |
     +-----------------+         +--------------------+
```

### Сервисы Render

| Сервис | Тип | Назначение |
|--------|-----|------------|
| `citg-db` | PostgreSQL | База данных |
| `citg-web` | Web Service | Дашборд + API |
| `citg-cron` | Cron Job | Парсинг каждые 5 мин |

---

## Стек

Python 3.11, FastAPI, SQLAlchemy 2.0 async, Telethon, PostgreSQL 16, ECharts 5.5, Docker

---

## Deploy на Render (платный)

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
DATABASE_URL=<из citg-db → Internal Database URL>
```

**Secrets** (Add Secret):
```
TG_API_ID=<твой API ID>
TG_API_HASH=<твой API hash>
TG_STRING_SESSION=<строка сессии>
```

### 3. Cron Job (парсер)

**New → Cron Job**
- Source: GitHub → `Ci_TG_Parser`
- Branch: `v2-citg-rebrand`
- Runtime: Docker
- Name: `citg-cron`
- Schedule: `*/5 * * * *`

**Env vars:**
```
DATABASE_URL=<из citg-db>
MAX_CONCURRENT_CHANNELS=10
HISTORY=0
```

**Secrets** (Add Secret):
```
TG_API_ID=<твой API ID>
TG_API_HASH=<твой API hash>
TG_STRING_SESSION=<строка сессии>
```

> Каналы добавляются только через веб-интерфейс. `CHANNELS` env var больше не используется.

### 4. Генерация TG_STRING_SESSION (локально)

```bash
git clone https://github.com/vladfa2010/Ci_TG_Parser.git
cd Ci_TG_Parser
git checkout v2-citg-rebrand
pip install -r requirements.txt
python generate_session.py
# Введи номер телефона → код из Telegram → скопируй строку сессии
```

Вставь строку в **Secrets** обоих сервисов (web + cron).

### 5. Первый запуск

После деплоя `citg-web` автоматически:
- Создаст таблицы в БД
- Создаст юзера `vlad` с паролем `!1234567890`
- Реактивирует все ранее деактивированные каналы (one-time recovery)

Открой `https://your-app.onrender.com/login` и войди.

### 6. Добавление каналов

> Важно: каналы добавляются **только через веб-интерфейс** (`/channels`).
> Env var `CHANNELS` больше не используется как fallback.

### 7. Запуск парсера

- Нажми **"🔄 Запустить парсинг"** на главной
- Парсер работает **последовательно** (1 канал за раз)
- Между каналами: 5 секунд паузы (FloodWait защита)
- Прогресс отображается в реальном времени
- Подробнее: см. раздел **Парсер** в этом документе

---

## Управление каналами

### Через веб-интерфейс

1. Открой `/channels` (через меню)
2. В поле ввода вставь любой формат:
   - `https://t.me/c/3147415698/1997` (приватный канал)
   - `https://t.me/channelname` (публичный канал)
   - `https://web.telegram.org/a/#-1003147415698`
   - `3147415698` (bare numeric ID)
   - `@channelname`
3. Нажми **"+ Добавить канал"**
4. Канал появится в списке → нажми **🔄 Запустить парсинг**

### Через API (curl)

```bash
# Добавить канал
curl -u "vlad:!1234567890" \
  "https://your-app.onrender.com/api/channel/add?identifier=3147415698"

# Список каналов
curl -u "vlad:!1234567890" \
  "https://your-app.onrender.com/api/channels"

# Включить/выключить канал
curl -u "vlad:!1234567890" \
  "https://your-app.onrender.com/api/channel/toggle/42"

# Удалить канал
curl -u "vlad:!1234567890" -X DELETE \
  "https://your-app.onrender.com/api/channel/delete/42"
```

### Формат каналов

| Тип | Пример ссылки | База данных |
|-----|---------------|-------------|
| Публичный | `https://t.me/channelname` | `username = 'channelname'` |
| Приватный | `https://t.me/c/3147415698/1997` | `numeric_id = 3147415698` |

---

## Страницы дашборда

| Страница | Описание |
|----------|----------|
| `/` | Главная — посты с фильтрацией по каналу, статистика, поиск |
| `/charts` | Графики: теги, активность, просмотры, таймлайн |
| `/analytics` | Аналитика: тренды, посты по тегам, экспорт CSV |
| `/tag-daily` | Ежедневная статистика по тегам |
| `/sentiment` | Сентимент-анализ |
| `/viral` | Вирусные посты |
| `/sectors` | Ротация секторов |
| `/wordcloud` | Облако слов |
| `/crossmarket` | Cross-channel аналитика |
| `/channels` | Управление каналами (добавление, on/off, удаление) |
| `/login` | Форма входа |

---

## Фильтрация постов по каналу

На вкладке **Posts** работает фильтр по каналам:

```
[Dropdown: All channels ▼]  [Search]  [Sort: Newest ▼]  [Search button]
```

### Как работает

1. **"All channels"** (по умолчанию) — показывает посты всех каналов
2. **Выбор конкретного канала** — показывает только посты этого канала
3. **Поддерживаются оба типа каналов:**
   - Публичные (`@channelname`) — фильтр по `username`
   - Приватные (`c/3147415698`) — фильтр по `numeric_id`

### Пользовательский сценарий

```
1. Открыть вкладку "Posts"
   → Видны все посты
   → Dropdown: "All channels"

2. Выбрать канал из dropdown
   → Посты перефильтровываются мгновенно
   → В заголовке: "Channel: c/3147415698 | 47 posts shown"

3. Выбрать "All channels" снова
   → Возврат ко всем постам
```

### Техническая реализация

- Frontend: dropdown `onchange` → `loadPosts()` с `?channel=` параметром
- Backend: `_channel_where_clause()` — определяет тип канала:
  - `channel.isdigit()` → фильтр по `numeric_id` или `telegram_id`
  - текст → фильтр по `username`
- SQL: `JOIN channels c ON p.channel_id = c.id WHERE ...`

---

## Карточки постов

Каждый пост отображается как карточка с богатой информацией:

```
┌─────────────────────────────────────────────────────────────┐
│ #12345  Market Twits    by Иван Петров    12.3K   2024-06-15 │
├─────────────────────────────────────────────────────────────┤
│ Текст сообщения...                                          │
│ #тег1 #тег2 #тег3                                          │
└─────────────────────────────────────────────────────────────┘
```

### Поля карточки

| Поле | Описание | Пример |
|------|----------|--------|
| `ID` | Telegram message ID в канале | `#12345` |
| **Название канала** | `channel.title` — человекочитаемое имя | **Market Twits** |
| **Отправитель** | `sender_name` — кто написал сообщение | `by Иван Петров` |
| **Просмотры** | Компактный формат: K/M | `12.3K` / `1.5M` |
| **Дата** | Дата публикации | `2024-06-15 14:30` |
| **Текст** | Полный текст поста | — |
| **Хэштеги** | Цветные теги | `#тег1` `#тег2` |

### Отправитель (`sender_name`)

- Для **каналов**: имя пользователя, который опубликовал пост
- Для **ботов**: имя бота
- Заполняется автоматически при парсинге (колонка `posts.sender_name`)
- Если неизвестен — поле скрыто
- Подробнее: см. раздел **Парсер** → `sender_name` в pipeline

### Компактные просмотры (`fmtViews`)

| Диапазон | Отображение |
|----------|-------------|
| 0–999 | `523` |
| 1 000–999 999 | `12.3K` |
| 1 000 000+ | `1.5M` |

---

## Парсер (Parser)

### Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                        MultiChannelParser                        │
│                                                                  │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │  init_db()  │───→│  parse_all() │───→│ parse_single_    │  │
│  │             │    │              │    │ channel()        │  │
│  └─────────────┘    └──────┬───────┘    └──────────────────┘  │
│                            │                                     │
│                     ┌──────┴───────┐                            │
│                     │ for ch in    │                            │
│                     │   channels:  │                            │
│                     │   new DB     │                            │
│                     │   session    │                            │
│                     │   parse(ch)  │                            │
│                     │   commit/    │                            │
│                     │   rollback   │                            │
│                     └──────────────┘                            │
└─────────────────────────────────────────────────────────────────┘
```

### Логика работы (pipeline)

#### 1. Запуск
```
[Web кнопка "🔄 Запустить парсинг"]
         │
         ▼
[MultiChannelParser.parse_all()]
         │
         ├── 1. Получает список активных каналов из БД
         │      SELECT * FROM channels WHERE is_active = TRUE
         │
         ├── 2. Для каждого канала (ПОСЛЕДОВАТЕЛЬНО):
         │      a. Свежая DB сессия (изоляция ошибок)
         │      b. _lookup_channel() — ищем в БД
         │      c. parse_single_channel() — парсим сообщения
         │      d. Сохраняем / rollback при ошибке
         │      e. Пауза 5 секунд (FloodWait защита)
         │
         └── 3. Все каналы обработаны
```

#### 2. Парсинг одного канала
```
parse_single_channel(channel):
    │
    ├── 1. Определяет entity_id (КРИТИЧНО):
    │      - Приватный канал (нет username):
    │         * telegram_id > 0 (старые данные):
    │           entity_id = PeerChannel(-100{telegram_id})
    │         * telegram_id < 0 (новые данные):
    │           entity_id = PeerChannel(telegram_id)
    │      - Публичный канал (есть username):
    │         entity_id = telegram_id напрямую
    │
    ├── 2. Итерирует сообщения:
    │      client.iter_messages(entity_id, min_id=...)
    │
    ├── 3. Для каждого сообщения:
    │      - Извлекает текст, хэштеги, упоминания, URL
    │      - Определяет тип медиа (фото/видео/документ)
    │      - Получает счётчики (просмотры, репосты, ответы)
    │      - Определяет отправителя (sender_name)
    │      - Создаёт SHA-256 хэш текста (дедупликация)
    │      - Каждые 50 сообщений: пауза 0.5 сек
    │
    └── 4. Сохраняет посты в БД (INSERT ... ON CONFLICT DO NOTHING)
```

### Защита от FloodWait

| Механизм | Значение | Описание |
|----------|----------|----------|
| **Пауза между каналами** | 5 секунд | После каждого канала — sleep |
| **Пауза между сообщениями** | 0.5 сек / 50 msg | Rate limiting |
| **Нет get_entity()** | — | Используем telegram_id из БД |

### Entity ID — приватные vs публичные каналы

Telegram API требует правильный entity type для каждого типа чата:

```
Приватный канал (t.me/c/3147415698):
  Без username → PeerChannel
  
  DB telegram_id=3147415698 (positive, old data):
    → entity_id = PeerChannel(-1003147415698)
    
  DB telegram_id=-1003147415698 (negative, new data):
    → entity_id = PeerChannel(-1003147415698)

Публичный канал (@channel):
  С username → username или telegram_id напрямую
  → entity_id = channel.telegram_id
```

Код автоматически определяет тип и создаёт правильный PeerChannel для всех приватных каналов.

### Изоляция ошибок (fresh DB session)

Каждый канал парсится в **отдельной DB сессии**:

```
for ch in channels:
    async with new_db_session() as session:
        try:
            parse(channel)      ← парсим
            session.commit()    ← сохраняем
        except Exception:
            session.rollback()  ← откатываем
            log_error()         ← логируем
    # ← сессия закрывается
    # ← ошибка НЕ влияет на следующий канал
```

Это гарантирует что ошибка в одном канале (например, ChatIdInvalid) **не ломает парсинг остальных каналов**.

### SIGTERM

При получении SIGTERM текущий канал **заканчивается полностью**, потом парсер останавливается. Никакого сложного состояния — просто выход из цикла.

### Обработка ошибок

| Ошибка | Действие | Повтор |
|--------|----------|--------|
| `ChatIdInvalidError` | Лог ошибки, следующий канал | Нет |
| `PeerUser/PeerChannel` | Лог ошибки, следующий канал | Нет |
| `FloodWaitError` | Пропускаем канал (не ждём) | Следующий cron |
| `Channel не в БД` | Пропускаем | Добавить через веб |
| `SIGTERM` | Завершаем текущий, останавливаемся | — |

### Инкрементальный парсинг

```
Первый запуск:    min_id = 0          → парсит ВСЮ историю
Последующие:      min_id = MAX(id)    → только НОВЫЕ сообщения
```

### Настройка через Environment Variables

| Переменная | Значение | Описание |
|------------|----------|----------|
| `MAX_CONCURRENT_CHANNELS` | 10 | Макс. параллельных каналов |
| `BATCH_SIZE` | 50 | Сообщений в одном batch |
| `MAX_RETRIES` | 3 | Попыток при ошибке |
| `RETRY_DELAY_BASE` | 2 | Базовая задержка retry (сек) |
| `HISTORY` | 0 | 0=инкрементально, 1=вся история |

---

## API Endpoints

Все endpoints требуют авторизации (Basic Auth или cookie-сессия после логина).

### Каналы
- `GET /api/channels` — список каналов с метриками
- `GET /api/channel/add?identifier=` — добавить канал
- `GET /api/channel/toggle/{id}` — включить/выключить
- `DELETE /api/channel/delete/{id}` — удалить (каскадно удаляет посты и логи)
- `GET /api/channels/comparison` — cross-channel сравнение

### Посты
- `GET /api/stats?channel=` — статистика (опционально по каналу)
- `GET /api/posts?page=&search=&sort=&channel=` — список постов с фильтром по каналу
  - `channel` — `username` для публичных или `numeric_id` для приватных
- `GET /api/tags?hours=&channel=` — хэштеги (опционально по каналу)

### Парсинг
- `GET /api/parse/trigger` — запустить парсинг всех активных каналов
- `GET /api/parse/status` — статус текущего парсинга (polling)

### Прочее
- `GET /api/test` — проверка работоспособности
- `GET /api/debug/routes` — список всех routes
- `GET /rss` — RSS-лента

---

## Переменные окружения

| Переменная | Обязательная | Значение | Описание |
|------------|-------------|----------|----------|
| `DATABASE_URL` | да | — | PostgreSQL connection string |
| `APP_MODE` | да (web) | `web` / `parser` | Режим работы |
| `TG_API_ID` | да | — | Telegram API ID |
| `TG_API_HASH` | да | — | Telegram API hash |
| `TG_STRING_SESSION` | да | — | Строковая сессия |
| `PORT` | нет | 10000 | Порт для веб-сервера |
| `MAX_CONCURRENT_CHANNELS` | нет | 10 | Параллельных каналов |
| `HISTORY` | нет | 0 | Парсить всю историю (1) |

---

## Аутентификация

### Предустановленный юзер
- **Username:** `vlad`
- **Password:** `!1234567890`

Создаётся автоматически при первом старте `citg-web`.

### Добавить юзера (через код)

```python
from models import User

user = User(username="admin", is_active=True)
user.set_password("password")
session.add(user)
session.commit()
```

### Режимы авторизации

| Клиент | Механизм |
|--------|----------|
| Браузер (после логина) | Cookie-сессия (7 дней) |
| curl / API | HTTP Basic Auth |

---

## Модели базы данных

| Таблица | Назначение |
|---------|-----------|
| `users` | Юзеры дашборда (PBKDF2 password hashing) |
| `channels` | Telegram-каналы (публичные и приватные) |
| `posts` | Посты из каналов |
| `parse_logs` | Логи парсинга |
| `channel_errors` | Ошибки по каналам |
| `channel_groups` | Группировка каналов |
| `channel_group_members` | Связь каналов и групп |

---

## Лицензия

MIT
