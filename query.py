#!/usr/bin/env python3
"""
Query tool for tgparser database.
Usage:
    python query.py count              # total posts
    python query.py latest [N]         # last N posts (default 5)
    python query.py top [N]            # top N by views (default 5)
    python query.py tags               # hashtag frequency
    python query.py logs               # recent parse logs
    python query.py search <keyword>   # search in text
"""
import os
import sys

from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    print("ERROR: Set DATABASE_URL env var")
    sys.exit(1)

engine = create_engine(DATABASE_URL.replace("+asyncpg", ""))


def count():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM posts"))
        print(f"\nВсего постов: {result.scalar()}")

        result = conn.execute(text("SELECT COUNT(*) FROM posts WHERE published_at > NOW() - INTERVAL '1 day'"))
        print(f"За сегодня: {result.scalar()}")


def latest(n=5):
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT telegram_message_id, views_count, published_at, LEFT(text, 80) as preview
            FROM posts ORDER BY published_at DESC LIMIT {n}
        """))
        print(f"\nПоследние {n} постов:")
        for row in result.mappings():
            print(f"  ID:{row['telegram_message_id']} | 👁 {row['views_count']} | {row['published_at']} | {row['preview']}")


def top(n=5):
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT telegram_message_id, views_count, LEFT(text, 80) as preview
            FROM posts ORDER BY views_count DESC LIMIT {n}
        """))
        print(f"\nТоп {n} по просмотрам:")
        for row in result.mappings():
            print(f"  ID:{row['telegram_message_id']} | 👁 {row['views_count']} | {row['preview']}")


def tags():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT DISTINCT unnest(hashtags) as tag, COUNT(*) as cnt
            FROM posts GROUP BY tag ORDER BY cnt DESC LIMIT 20
        """))
        print("\nПопулярные хэштеги:")
        for row in result.mappings():
            print(f"  #{row['tag']} — {row['cnt']} раз")


def logs():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT started_at, posts_parsed, posts_new, duration_ms, error_message
            FROM parse_logs ORDER BY started_at DESC LIMIT 5
        """))
        print("\nИстория парсинга:")
        for row in result.mappings():
            err = f" ❌ {row['error_message']}" if row['error_message'] else " ✅"
            print(f"  {row['started_at']} | парс: {row['posts_parsed']} нов: {row['posts_new']} | {row['duration_ms']}ms{err}")


def search(keyword):
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT telegram_message_id, views_count, LEFT(text, 100) as preview
            FROM posts WHERE text ILIKE '%{keyword}%' LIMIT 10
        """))
        print(f"\nРезультаты поиска '{keyword}':")
        for row in result.mappings():
            print(f"  ID:{row['telegram_message_id']} | 👁 {row['views_count']} | {row['preview']}")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "count"

    if cmd == "count":
        count()
    elif cmd == "latest":
        latest(int(sys.argv[2]) if len(sys.argv) > 2 else 5)
    elif cmd == "top":
        top(int(sys.argv[2]) if len(sys.argv) > 2 else 5)
    elif cmd == "tags":
        tags()
    elif cmd == "logs":
        logs()
    elif cmd == "search":
        search(sys.argv[2] if len(sys.argv) > 2 else "")
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
