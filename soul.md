# Soul — CITG Parser: контекст работы

> Файл для сохранения контекста между сессиями.
> Обновляется по мере работы.

---

## Проект

**CITG** — парсер Telegram-каналов в единую ленту.
- 20 закрытых (private) каналов, пользователь — участник всех
- Единая лента постов с веб-дашбордом (FastAPI)
- Аналитика: теги, тикеры, сентимент, графики (ECharts)
- Платформа: **Render (paid tier)**
- БД: **PostgreSQL** (asyncpg)

## Репозиторий

```
https://github.com/vladfa2010/Ci_TG_Parser
ветка: v2-citg-rebrand
```

## Роль

Опытный **CTO в команде**. Пишу качественный production-ready код.
- Архитектурные решения с обоснованием
- Code review перед пушем
- Обсуждаю план перед реализацией

## Настройки работы

| Параметр | Значение |
|----------|----------|
| Язык кода | Python (async) |
| Язык комментариев/логов | **Русский** |
| Стиль | Production-ready, type hints, докстринги |
| SQLAlchemy | 2.0 async |
| Telegram API | Telethon (MTProto) |
| Push в git | **Force push в `v2-citg-rebrand`** (разрешено) |
| Токены | Пользователь предоставляет по запросу |

## Архитектура парсера (v3)

```
parser.py (v3)
├── ChannelResolver      # get_dialogs() + кэш access_hash в БД
├── ChannelParser        # iter_messages без get_entity()
├── AdaptiveRateLimiter  # Адаптивный rate limiting
├── CircuitBreaker       # Per-channel + global защита
├── MultiChannelParser   # Оркестратор + parse_all() для web.py
└── _progress_callback   # Live прогресс в веб
```

**Ключевые решения:**
- **Список каналов контролируется строго через Web UI.** `sync_dialogs()` обновляет `access_hash` только для каналов, уже добавленных в БД; новые каналы не создаёт.
- `access_hash` кэшируется в БД (обход get_entity())
- `message.post_author` вместо `message.sender` (zero API calls)
- Сессия БД изолирована от чтения Telegram
- Последовательный парсинг с jitter (безопасно для 20 каналов)
- Cron: `*/5 * * * *`

## Сценарии запуска парсера

| Сценарий | Как работает |
|----------|-------------|
| **Cron (auto)** | Render cron каждые 5 минут |
| **Кнопка 🔄 в вебе** | `/api/parse/trigger` → BackgroundTasks → parse_all() |
| **Live прогресс** | `_progress_callback` → `/api/parse/status` |

## Важные файлы

| Файл | Назначение |
|------|-----------|
| `parser.py` | Основной парсер (v3) |
| `models.py` | SQLAlchemy модели (Channel, Post, ParseLog, ParseState) |
| `config.py` | Pydantic Settings + env vars |
| `web.py` | FastAPI дашборд (207KB, монолит) |
| `render.yaml` | Деплой на Render |
| `entrypoint.sh` | Запуск веб + парсера |
| `migration_v3.sql` | Миграция БД |

## Предпочтения пользователя

- Не торопиться. Обсуждать план до реализации.
- Сначала анализ, потом действие.
- Сохранять совместимость с существующим веб-дашбордом.
- Кнопка 🔄 и cron должны оба работать.
- Веб должен показывать статус парсинга (live).
- Git push сразу в `v2-citg-rebrand`, force push ок.

## Правила текущего диалога

- **Один диалог = один проект.** В этом окне работаем только с `Ci_TG_Parser`.
- Другие проекты (`tgparser-*`, `pulse-*`) и их сервисы игнорируются, если пользователь явно не попросит.
- Репозиторий клонирован локально в `Ci_TG_Parser_v2/`, ветка `v2-citg-rebrand`.
- Render API Key хранится в `.env` в корне рабочей директории (не в репозитории).
- Любые `git commit`, `git push`, `git reset`, `git rebase` и прочие git-мутации требуют явного подтверждения пользователя.

## История изменений

- **2025-10-18**: Первая версия парсера
- **2025-12-19**: V2, web dashboard
- **2026-03-14**: Генерация сессии, докеризация
- **2026-06-05**: V2 rebrand, sentiment, stock correlation
- **2026-06-17**: **Parser v3** — rewrite для 20 private channels
  - access_hash cache, circuit breaker, adaptive rate limiter
  - parse_all() + _progress_callback для web.py
