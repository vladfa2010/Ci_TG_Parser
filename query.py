"""
CLI-инструмент для запросов к БД Telegram Parser.

Команды:
    count              -- общее число постов, постов за сегодня, число каналов
    latest [N]         -- последние N постов с именем канала
    top [N]            -- топ N по просмотрам
    tags               -- частота хэштегов
    logs               -- логи парсинга с именем канала
    search <keyword>   -- поиск по тексту
    channels           -- список каналов с метриками
    errors             -- ошибки каналов

Использование:
    python query.py <command> [args]
"""

from __future__ import annotations

import sys
from collections import Counter

from sqlalchemy import create_engine, text

from config import settings


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

engine = create_engine(settings.database_url_sync)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_number(n: int) -> str:
    return f"{n:,}".replace(",", " ")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_count() -> None:
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM posts")).scalar()
        today = conn.execute(
            text(
                "SELECT COUNT(*) FROM posts "
                "WHERE DATE(created_at) = CURRENT_DATE"
            )
        ).scalar()
        channels = conn.execute(text("SELECT COUNT(*) FROM channels")).scalar()
    print(f"  Всего постов  : {_fmt_number(total or 0)}")
    print(f"  Постов сегодня: {_fmt_number(today or 0)}")
    print(f"  Каналов       : {_fmt_number(channels or 0)}")


def cmd_latest(n: int = 10) -> None:
    stmt = text("""
        SELECT p.id, p.text, p.views_count, p.published_at, c.username, c.title
        FROM posts p
        JOIN channels c ON p.channel_id = c.id
        ORDER BY p.published_at DESC NULLS LAST
        LIMIT :limit
    """)
    with engine.connect() as conn:
        rows = conn.execute(stmt, {"limit": n}).mappings().all()
    print(f"{'ID':>6} | {'Канал':<20} | {'Просмотры':>10} | {'Дата':<19} | Текст")
    print("-" * 120)
    for row in rows:
        name = row["username"] or row["title"] or "?"
        txt = (row["text"] or "")[:50].replace("\n", " ")
        views = _fmt_number(row["views_count"] or 0)
        date = str(row["published_at"])[:19] if row["published_at"] else "—"
        print(f"{row['id']:>6} | {name:<20} | {views:>10} | {date:<19} | {txt}")


def cmd_top(n: int = 10) -> None:
    stmt = text("""
        SELECT p.id, p.text, p.views_count, c.username, c.title
        FROM posts p
        JOIN channels c ON p.channel_id = c.id
        ORDER BY p.views_count DESC NULLS LAST
        LIMIT :limit
    """)
    with engine.connect() as conn:
        rows = conn.execute(stmt, {"limit": n}).mappings().all()
    print(f"{'#':>3} | {'Просмотры':>10} | {'Канал':<20} | Текст")
    print("-" * 100)
    for i, row in enumerate(rows, 1):
        name = row["username"] or row["title"] or "?"
        txt = (row["text"] or "")[:55].replace("\n", " ")
        views = _fmt_number(row["views_count"] or 0)
        print(f"{i:>3} | {views:>10} | {name:<20} | {txt}")


def cmd_tags() -> None:
    stmt = text("SELECT hashtags FROM posts WHERE hashtags IS NOT NULL")
    with engine.connect() as conn:
        rows = conn.execute(stmt).mappings().all()
    counter: Counter[str] = Counter()
    for row in rows:
        tags = row["hashtags"]
        if isinstance(tags, list):
            counter.update(t.lower() for t in tags if isinstance(t, str))
    if not counter:
        print("  Хэштегов не найдено.")
        return
    print(f"{'Хэштег':<30} | {'Кол-во':>6}")
    print("-" * 40)
    for tag, cnt in counter.most_common(30):
        print(f"{tag:<30} | {cnt:>6}")


def cmd_logs(n: int = 20) -> None:
    stmt = text("""
        SELECT pl.id, pl.posts_parsed, pl.posts_new, pl.duration_ms,
               pl.error_message, pl.started_at, c.username, c.title
        FROM parse_logs pl
        JOIN channels c ON pl.channel_id = c.id
        ORDER BY pl.started_at DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        rows = conn.execute(stmt, {"limit": n}).mappings().all()
    if not rows:
        print("  Логов пока нет.")
        return
    print(f"{'ID':>5} | {'Канал':<18} | {'Обработано':>5} | {'Новых':>5} | {'ms':>7} | {'Начато':<19} | Ошибка")
    print("-" * 120)
    for row in rows:
        name = row["username"] or row["title"] or "?"
        err = (row["error_message"] or "—")[:30]
        print(
            f"{row['id']:>5} | {name:<18} | {row['posts_parsed']:>5} | "
            f"{row['posts_new']:>5} | {row['duration_ms']:>7} | "
            f"{str(row['started_at'])[:19]:<19} | {err}"
        )


def cmd_search(keyword: str) -> None:
    stmt = text("""
        SELECT p.id, p.text, p.views_count, p.published_at, c.username, c.title
        FROM posts p
        JOIN channels c ON p.channel_id = c.id
        WHERE p.text ILIKE :pattern
        ORDER BY p.published_at DESC NULLS LAST
        LIMIT 50
    """)
    with engine.connect() as conn:
        rows = conn.execute(stmt, {"pattern": f"%{keyword}%"}).mappings().all()
    print(f"Найдено: {len(rows)} постов по запросу '{keyword}'")
    print("-" * 100)
    for row in rows:
        name = row["username"] or row["title"] or "?"
        txt = (row["text"] or "")[:70].replace("\n", " ")
        views = _fmt_number(row["views_count"] or 0)
        print(f"[{name}] ({views} просм.) {txt}")


def cmd_channels() -> None:
    stmt = text("""
        SELECT c.id, c.username, c.title, c.subscriber_count, c.is_active,
               c.parse_error_count, c.last_parsed_at, c.created_at,
               COUNT(p.id) AS post_count
        FROM channels c
        LEFT JOIN posts p ON p.channel_id = c.id
        GROUP BY c.id
        ORDER BY post_count DESC
    """)
    with engine.connect() as conn:
        rows = conn.execute(stmt).mappings().all()
    print(
        f"{'ID':>4} | {'Канал':<18} | {'Подписчики':>10} | "
        f"{'Постов':>6} | {'Активен':>7} | {'Ошибок':>6} | {'Последний парсинг':<19}"
    )
    print("-" * 110)
    for row in rows:
        name = row["username"] or row["title"] or "?"
        subs = _fmt_number(row["subscriber_count"] or 0)
        posts = _fmt_number(row["post_count"] or 0)
        active = "да" if row["is_active"] else "нет"
        errors = row["parse_error_count"] or 0
        last = str(row["last_parsed_at"])[:19] if row["last_parsed_at"] else "—"
        print(
            f"{row['id']:>4} | {name:<18} | {subs:>10} | {posts:>6} | "
            f"{active:>7} | {errors:>6} | {last}"
        )


def cmd_errors() -> None:
    stmt = text("""
        SELECT ce.id, ce.error_message, ce.error_type, ce.created_at,
               c.username, c.title
        FROM channel_errors ce
        JOIN channels c ON ce.channel_id = c.id
        ORDER BY ce.created_at DESC
        LIMIT 30
    """)
    with engine.connect() as conn:
        rows = conn.execute(stmt).mappings().all()
    if not rows:
        print("  Ошибок нет.")
        return
    print(f"{'ID':>5} | {'Канал':<18} | {'Тип':<12} | {'Дата':<19} | Сообщение")
    print("-" * 120)
    for row in rows:
        name = row["username"] or row["title"] or "?"
        err_type = row["error_type"] or "—"
        err_msg = (row["error_message"] or "—")[:50]
        date = str(row["created_at"])[:19]
        print(f"{row['id']:>5} | {name:<18} | {err_type:<12} | {date:<19} | {err_msg}")


# ---------------------------------------------------------------------------
# CLI router
# ---------------------------------------------------------------------------

COMMANDS: dict[str, tuple[callable, str]] = {
    "count": (cmd_count, "count — общая статистика"),
    "latest": (cmd_latest, "latest [N=10] — последние посты"),
    "top": (cmd_top, "top [N=10] — топ по просмотрам"),
    "tags": (cmd_tags, "tags — частота хэштегов"),
    "logs": (cmd_logs, "logs [N=20] — логи парсинга"),
    "search": (cmd_search, "search <keyword> — поиск по тексту"),
    "channels": (cmd_channels, "channels — список каналов"),
    "errors": (cmd_errors, "errors — ошибки каналов"),
}


def usage() -> None:
    print("Использование: python query.py <command> [args]")
    print("\nДоступные команды:")
    for name, (_, desc) in COMMANDS.items():
        print(f"  {desc}")
    print()


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        usage()
        sys.exit(0)
    cmd = sys.argv[1]
    if cmd not in COMMANDS:
        print(f"Неизвестная команда: {cmd}")
        usage()
        sys.exit(1)
    fn, _ = COMMANDS[cmd]
    try:
        if cmd == "latest":
            fn(int(sys.argv[2]) if len(sys.argv) > 2 else 10)
        elif cmd == "top":
            fn(int(sys.argv[2]) if len(sys.argv) > 2 else 10)
        elif cmd == "logs":
            fn(int(sys.argv[2]) if len(sys.argv) > 2 else 20)
        elif cmd == "search":
            if len(sys.argv) < 3:
                print("Укажите keyword: python query.py search <keyword>")
                sys.exit(1)
            fn(sys.argv[2])
        else:
            fn()
    except KeyboardInterrupt:
        print("\nПрервано.")
        sys.exit(0)


if __name__ == "__main__":
    main()
