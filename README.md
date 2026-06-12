# CITG v2 — Multi-Channel Telegram Parser

> Параллельный сбор и аналитика данных из нескольких Telegram-каналов с веб-дашбордом.

## Содержание

- [Что нового в v2](#что-нового-в-v2)
- [Архитектура](#архитектура)
- [Стек технологий](#стек-технологий)
- [Быстрый старт](#быстрый-старт)
- [Переменные окружения](#переменные-окружения)
- [Управление каналами](#управление-каналами)
- [API Endpoints](#api-endpoints)
- [Страницы дашборда](#страницы-дашборда)
- [Миграция с v1](#миграция-с-v1)
- [Deploy на Render](#deploy-на-render)
- [Лицензия](#лицензия)

---

## Что нового в v2

| Функция | Описание |
|---------|----------|
| **Многоканальный параллельный парсинг** | Одновременный сбор из до 5 Telegram-каналов, каждый в отдельном async-задаче |
| **Retry logic** | Экспоненциальный backoff при ошибках сети: 1s, 2s, 4s, 8s, 16s |
| **Health tracking каналов** | Автоматическое отключение канала при 5+ ошибках подряд, восстановление через интервал |
| **Глобальная дедупликация** | Уникальный хэш каждого поста `(channel_id, message_id)` предотвращает дубли |
| **Cross-channel аналитика** | Сравнительные графики активности, пересечения тем и трендов между каналами |
| **Channel-aware фильтры** | Все страницы дашборда поддерживают фильтрацию по выбранным каналам |
| **Группировка каналов** | Объединение каналов в логические группы (таблица `channel_groups`) |

---

## Архитектура

```
+---------------------------------------------------+
|                    Docker Compose                   |
|                                                   |
|  +------------------+    +----------------------+ |
|  |   Parser         |    |   Web Dashboard      | |
|  |   (parser.py)    |    |   (web.py)           | |
|  |                  |    |                      | |
|  |  - Telethon      |    |  - FastAPI           | |
|  |  - 5 параллельных|    |  - Jinja2 templates  | |
|  |    парсеров      |    |  - ECharts           | |
|  |  - Retry logic   |    |  - REST API          | |
|  |  - Health check  |    |                      | |
|  +--------+---------+    +----------+-----------+ |
|           |                         |              |
|           +-----------+-------------+              |
|                       |                            |
|              +--------v---------+                  |
|              |   PostgreSQL 16  |                  |
|              |                  |                  |
|              |  - channels      |                  |
|              |  - posts         |                  |
|              |  - parse_logs    |                  |
|              |  - channel_errors|                  |
|              |  - channel_groups|                  |
|              +------------------+                  |
|                                                   |
+---------------------------------------------------+
```

### Поток данных

1. **Parser** подключается к Telegram API через Telethon
2. Для каждого канала создается отдельная async-задача
3. Посты сохраняются в PostgreSQL с дедупликацией по хэшу
4. **Web** читает данные из БД и отдает дашборд + API
5. Health-tracker мониторит ошибки и отключает проблемные каналы

---

## Стек технологий

| Компонент | Технология | Версия |
|-----------|-----------|--------|
| Язык | Python | 3.11+ |
| Telegram API | Telethon | 1.35+ |
| Web-фреймворк | FastAPI | 0.110+ |
| ORM | SQLAlchemy | 2.0 async |
| База данных | PostgreSQL | 16 |
| Charts | ECharts | 5.4+ |
| Контейнеризация | Docker + Compose | - |
| Шаблонизатор | Jinja2 | 3.1+ |

---

## Быстрый старт

### 1. Клонирование и подготовка

```bash
git clone https://github.com/username/citg_v2.git
cd citg_v2
cp .env.example .env
```

### 2. Настройка переменных окружения

Отредактируйте `.env`:

```bash
# Telegram API credentials (получить на https://my.telegram.org)
TG_API_ID=12345678
TG_API_HASH=your_api_hash_here

# Каналы для парсинга (через запятую, без @)
CHANNELS=markettwits,ru2ch,etc

# PostgreSQL
DATABASE_URL=postgresql+asyncpg://postgres:postgres@db:5432/citg

# Парсер
PARSER_INTERVAL=300
MAX_CHANNELS=5
```

### 3. Инициализация базы данных

```bash
docker compose --profile init run --rm init
```

Создает все таблицы: `channels`, `posts`, `parse_logs`, `channel_errors`, `channel_groups`, `channel_group_members`.

### 4. Генерация Telegram-сессии

```bash
docker compose run --rm parser python generate_session.py
```

Введите номер телефона и код подтверждения из Telegram. Файл сессии сохранится в `data/session.session`.

### 5. Запуск парсера

```bash
# Первый запуск — парсер начнет собирать посты
docker compose up parser

# Фоновый режим
docker compose up -d parser
```

### 6. Запуск дашборда

```bash
# Дашборд доступен на http://localhost:8000
docker compose up web

# Или все сервисы сразу
docker compose up -d
```

### 7. Просмотр логов

```bash
# Логи парсера
docker compose logs -f parser

# Логи веб-сервера
docker compose logs -f web

# Логи базы данных
docker compose logs -f db
```

---

## Переменные окружения

| Переменная | Обязательная | Значение по умолчанию | Описание |
|------------|:------------:|----------------------|----------|
| `TG_API_ID` | Да | — | App ID из https://my.telegram.org |
| `TG_API_HASH` | Да | — | App Hash из https://my.telegram.org |
| `CHANNELS` | Да | — | Список каналов через запятую (без `@`) |
| `DATABASE_URL` | Да | `postgresql+asyncpg://postgres:postgres@db:5432/citg` | URL подключения к PostgreSQL |
| `PARSER_INTERVAL` | Нет | `300` | Интервал между циклами парсинга, секунды |
| `MAX_CHANNELS` | Нет | `5` | Максимальное количество параллельных каналов |
| `RETRY_BASE_DELAY` | Нет | `1` | Базовая задержка retry, секунды |
| `RETRY_MAX_DELAY` | Нет | `60` | Максимальная задержка retry, секунды |
| `HEALTH_ERROR_THRESHOLD` | Нет | `5` | Количество ошибок для отключения канала |
| `HEALTH_RECOVERY_INTERVAL` | Нет | `3600` | Интервал проверки восстановления канала, секунды |
| `WEB_PORT` | Нет | `8000` | Порт веб-сервера |
| `LOG_LEVEL` | Нет | `INFO` | Уровень логирования (DEBUG, INFO, WARNING, ERROR) |
| `TIMEZONE` | Нет | `Europe/Moscow` | Часовой пояс для отображения данных |
| `ENABLE_WORDCLOUD` | Нет | `true` | Включить генерацию облака слов |
| `MAX_POSTS_PER_CHANNEL` | Нет | `1000` | Лимит постов на канал за один цикл |

---

## Управление каналами

### Через переменные окружения

```bash
# В .env файле
CHANNELS=markettwits,ru2ch,etc,another_channel
```

### Через базу данных

Таблица `channels` содержит все настройки каналов:

```sql
-- Список всех каналов
SELECT id, username, display_name, is_active, health_status, last_parsed_at
FROM channels;

-- Включить / отключить канал
UPDATE channels SET is_active = false WHERE username = 'ru2ch';

-- Изменить отображаемое имя
UPDATE channels SET display_name = 'Market Twits' WHERE username = 'markettwits';
```

### Поля таблицы `channels`

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | SERIAL | Первичный ключ |
| `username` | VARCHAR | Username канала (без `@`) |
| `display_name` | VARCHAR | Отображаемое имя в дашборде |
| `is_active` | BOOLEAN | Активен ли канал для парсинга |
| `health_status` | VARCHAR | `healthy`, `degraded`, `disabled` |
| `error_count` | INTEGER | Счетчик ошибок подряд |
| `last_error_at` | TIMESTAMP | Время последней ошибки |
| `last_parsed_at` | TIMESTAMP | Время последнего успешного парсинга |
| `created_at` | TIMESTAMP | Время добавления канала |

### Группировка каналов

```sql
-- Создать группу
INSERT INTO channel_groups (name, description) VALUES ('Финансы', 'Финансовые каналы');

-- Добавить каналы в группу
INSERT INTO channel_group_members (group_id, channel_id) VALUES (1, 1);
```

---

## API Endpoints

### Основные endpoints

| Метод | Путь | Описание |
|-------|------|----------|
| `GET` | `/` | Главная страница дашборда (список постов) |
| `GET` | `/charts` | Страница с графиками активности |
| `GET` | `/analytics` | Аналитическая сводка |
| `GET` | `/api/posts` | JSON: список постов с пагинацией и фильтрами |
| `GET` | `/api/posts/stats` | JSON: статистика по постам (количество, активность) |
| `GET` | `/api/channels` | JSON: список всех каналов со статусом |
| `GET` | `/api/channels/{id}/stats` | JSON: статистика по конкретному каналу |
| `GET` | `/api/analytics/tag-daily` | JSON: ежедневная статистика по тегам |
| `GET` | `/api/analytics/sentiment` | JSON: анализ тональности по каналам |
| `GET` | `/api/analytics/viral` | JSON: вирусные посты (топ по просмотрам/репостам) |
| `GET` | `/api/analytics/sectors` | JSON: распределение по секторам/темам |
| `GET` | `/api/analytics/crosschannel` | JSON: cross-channel сравнительная аналитика |
| `GET` | `/api/wordcloud` | JSON: данные для облака слов |
| `GET` | `/health` | Health check (статус парсера и БД) |

### Параметры фильтрации для `/api/posts`

| Параметр | Тип | Описание |
|----------|-----|----------|
| `channel` | string | Фильтр по username канала |
| `channels` | string | Фильтр по нескольким каналам (через запятую) |
| `search` | string | Поиск по тексту поста |
| `date_from` | date | Начальная дата (YYYY-MM-DD) |
| `date_to` | date | Конечная дата (YYYY-MM-DD) |
| `limit` | int | Лимит записей (default: 50, max: 500) |
| `offset` | int | Смещение для пагинации |

### Примеры запросов

```bash
# Посты из всех каналов за сегодня
curl "http://localhost:8000/api/posts?date_from=2024-01-01&limit=10"

# Посты конкретного канала
curl "http://localhost:8000/api/posts?channel=markettwits"

# Посты из нескольких каналов
curl "http://localhost:8000/api/posts?channels=markettwits,ru2ch"

# Поиск по тексту
curl "http://localhost:8000/api/posts?search=bitcoin"

# Статистика по каналам
curl "http://localhost:8000/api/channels"

# Cross-channel аналитика
curl "http://localhost:8000/api/analytics/crosschannel?days=7"
```

---

## Страницы дашборда

### `/` — Главная (Лента постов)
- Таблица всех собранных постов
- Фильтры: по каналам, по дате, по тексту
- Пагинация, сортировка по времени
- Отображение: автор, текст, дата, просмотры, репосты, реакции

### `/charts` — Графики активности
- Линейный график: посты по времени (группировка по часам/дням)
- Столбчатая диаграмма: активность по каналам
- График просмотров и реакций
- Фильтрация по каналам и датам

### `/analytics` — Аналитическая сводка
- KPI: всего постов, активных каналов, постов за 24ч
- Топ-10 постов по просмотрам
- Распределение постов по каналам (pie chart)
- Тренды: сравнение текущего периода с предыдущим

### `/tag-daily` — Ежедневные теги
- Календарь с популярными тегами по дням
- Heatmap активности тегов
- Тренды: рост/падение популярности тегов

### `/sentiment` — Анализ тональности
- Распределение sentiment: positive / neutral / negative
- Динамика тональности по времени
- Сравнение тональности между каналами

### `/viral` — Вирусные посты
- Топ постов по просмотрам, репостам, реакциям
- Фильтры: период, канал
- Метрики вирусности: velocity score

### `/sectors` — Секторный анализ
- Распределение постов по тематическим секторам
- Активность секторов по времени
- Сравнение секторов между каналами

### `/wordcloud` — Облако слов
- Визуализация наиболее частых слов
- Фильтрация по каналам и периоду
- Настройка количества слов и исключений

### `/crossmarket` — Cross-market анализ
- Пересечение тем между каналами
- Корреляция активности
- Общие тренды

### `/channels` — Управление каналами
- Список всех каналов со статусом health
- История ошибок по каналам
- Возможность включить/отключить канал

### `/crosschannel` — Cross-channel аналитика
- Сравнительные графики по выбранным каналам
- Наложение трендов нескольких каналов
- Матрица корреляций

---

## Миграция с v1

### Автоматическая миграция

Новые таблицы создаются автоматически при первом запуске:

```python
# В parser.py инициализация:
async with engine.begin() as conn:
    await conn.run_sync(Base.metadata.create_all)
```

### Новые таблицы

| Таблица | Назначение |
|---------|-----------|
| `channel_errors` | Лог ошибок парсинга по каналам |
| `channel_groups` | Группы каналов |
| `channel_group_members` | Связь групп и каналов |

### Что изменилось

- **Таблица `channels`**: добавлены поля `health_status`, `error_count`, `last_error_at`, `display_name`
- **Таблица `posts`**: добавлено поле `channel_id` ( foreign key на `channels` )
- **Таблица `parse_logs`**: добавлено поле `channel_id`
- **Данные сохраняются**: существующие посты и логи остаются без изменений

### Ручная миграция (если нужно)

```sql
-- Добавить колонки в существующую таблицу channels
ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS health_status VARCHAR(20) DEFAULT 'healthy',
    ADD COLUMN IF NOT EXISTS error_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS display_name VARCHAR(255);

-- Создать таблицу ошибок
CREATE TABLE IF NOT EXISTS channel_errors (
    id SERIAL PRIMARY KEY,
    channel_id INTEGER REFERENCES channels(id),
    error_type VARCHAR(100),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Создать таблицу групп
CREATE TABLE IF NOT EXISTS channel_groups (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS channel_group_members (
    group_id INTEGER REFERENCES channel_groups(id),
    channel_id INTEGER REFERENCES channels(id),
    PRIMARY KEY (group_id, channel_id)
);
```

---

## Deploy на Render

Проект включает `render.yaml` для деплоя на [Render](https://render.com):

```yaml
# render.yaml
services:
  - type: web
    name: citg-web
    runtime: docker
    plan: standard
    envVars:
      - key: DATABASE_URL
        fromDatabase:
          name: citg-db
          property: connectionString
      - key: TG_API_ID
        sync: false
      - key: TG_API_HASH
        sync: false
      - key: CHANNELS
        sync: false
    healthCheckPath: /health

  - type: worker
    name: citg-worker
    runtime: docker
    plan: standard
    dockerfilePath: ./Dockerfile.parser
    envVars:
      - key: DATABASE_URL
        fromDatabase:
          name: citg-db
          property: connectionString
      - key: TG_API_ID
        sync: false
      - key: TG_API_HASH
        sync: false
      - key: CHANNELS
        sync: false

databases:
  - name: citg-db
    plan: standard
    postgresMajorVersion: 16
```

### Шаги деплоя

1. Форкните репозиторий на GitHub
2. Создайте новый Blueprint на Render, указав репозиторий
3. Настройте Environment Variables в Render Dashboard:
   - `TG_API_ID` — ваш App ID
   - `TG_API_HASH` — ваш App Hash
   - `CHANNELS` — список каналов
4. Render автоматически создаст БД и запустит сервисы

---

## Структура проекта

```
citg_v2/
├── docker-compose.yml        # Docker Compose конфигурация
├── Dockerfile                # Dockerfile для web
├── Dockerfile.parser         # Dockerfile для parser
├── render.yaml               # Render Blueprint
├── .env.example              # Пример переменных окружения
├── requirements.txt          # Python-зависимости
│
├── parser.py                 # Основной модуль парсера
├── web.py                    # FastAPI веб-приложение
├── models.py                 # SQLAlchemy модели
├── database.py               # Подключение к БД
├── generate_session.py       # Генерация Telegram-сессии
│
├── templates/                # Jinja2-шаблоны
│   ├── base.html
│   ├── index.html
│   ├── charts.html
│   ├── analytics.html
│   ├── tag_daily.html
│   ├── sentiment.html
│   ├── viral.html
│   ├── sectors.html
│   ├── wordcloud.html
│   ├── crossmarket.html
│   ├── channels.html
│   └── crosschannel.html
│
├── static/                   # CSS, JS, assets
│   ├── style.css
│   └── script.js
│
└── data/                     # Данные (volume)
    └── session.session       # Telegram-сессия
```

---

## Лицензия

MIT License

Copyright (c) 2024

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
