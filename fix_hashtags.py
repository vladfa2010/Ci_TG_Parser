"""
Ретроактивное извлечение хэштегов из существующих постов.

Сканирует таблицу posts, находит записи с пустыми или невалидными hashtags,
извлекает хэштеги вида #\S+ из текста поста и обновляет поле hashtags.

Использование:
    python fix_hashtags.py           # с подтверждением
    python fix_hashtags.py --yes     # без подтверждения
"""

from __future__ import annotations

import re
import sys
from collections.abc import Sequence

from sqlalchemy import create_engine, text

from config import settings

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HASHTAG_RE = re.compile(r"#\S+")
BATCH_SIZE = 500

engine = create_engine(settings.database_url_sync)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_hashtags(text: str | None) -> list[str]:
    """Извлекает хэштеги из текста поста."""
    if not text:
        return []
    return HASHTAG_RE.findall(text)


def find_posts_needing_fix() -> Sequence:
    """Находит посты с пустыми или невалидными hashtags."""
    stmt = text("""
        SELECT id, text, hashtags
        FROM posts
        WHERE text IS NOT NULL
          AND (
              hashtags IS NULL
              OR hashtags = 'null'
              OR hashtags = '[]'
              OR hashtags::text = '[]'
          )
        ORDER BY id
    """)
    with engine.connect() as conn:
        return conn.execute(stmt).mappings().all()


def find_mismatched_hashtags() -> Sequence:
    """Находит посты, у которых hashtags не соответствует тексту (есть # в тексте, но нет в hashtags)."""
    stmt = text("""
        SELECT id, text, hashtags
        FROM posts
        WHERE text IS NOT NULL
          AND text LIKE '%#%'
          AND (
              hashtags IS NULL
              OR hashtags = '[]'
              OR jsonb_array_length(hashtags::jsonb) = 0
          )
        ORDER BY id
    """)
    with engine.connect() as conn:
        return conn.execute(stmt).mappings().all()


def update_post_hashtags(post_id: int, hashtags: list[str]) -> None:
    """Обновляет поле hashtags для конкретного поста."""
    stmt = text("""
        UPDATE posts
        SET hashtags = :hashtags
        WHERE id = :post_id
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {"hashtags": hashtags, "post_id": post_id})


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def scan_and_fix(*, auto_confirm: bool = False) -> tuple[int, int]:
    """Сканирует посты и извлекает хэштеги.

    Returns:
        (processed_count, updated_count)
    """
    posts = find_mismatched_hashtags()
    total = len(posts)
    print(f"Найдено постов для обработки: {total}")

    if total == 0:
        print("Все хэштеги на месте. Исправлений не требуется.")
        return 0, 0

    # Preview first 5
    preview = posts[:5]
    print("\n--- Примера ---")
    for row in preview:
        tags = extract_hashtags(row["text"])
        txt = (row["text"] or "")[:80].replace("\n", " ")
        print(f"  [id={row['id']}] теги: {tags}")
        print(f"    текст: {txt}...")
    if total > 5:
        print(f"  ... и ещё {total - 5} постов")

    # Confirmation
    if not auto_confirm:
        print()
        answer = input(f"Обновить хэштеги для {total} постов? [y/N]: ").strip().lower()
        if answer not in ("y", "yes", "д", "да"):
            print("Отменено.")
            return 0, 0

    # Process in batches
    processed = 0
    updated = 0
    batch_count = 0

    for row in posts:
        tags = extract_hashtags(row["text"])
        processed += 1
        if tags:
            update_post_hashtags(row["id"], tags)
            updated += 1

        if processed % BATCH_SIZE == 0:
            batch_count += 1
            print(f"  ... обработано {processed}/{total} (обновлено {updated})")

    print(f"\nГотово: обработано {processed}, обновлено {updated} постов.")
    return processed, updated


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def usage() -> None:
    print("Использование: python fix_hashtags.py [--yes]")
    print("  --yes    пропустить подтверждение")


def main() -> None:
    auto_confirm = "--yes" in sys.argv[1:]
    if "-h" in sys.argv[1:] or "--help" in sys.argv[1:]:
        usage()
        sys.exit(0)

    print("=" * 50)
    print("Ретроактивное извлечение хэштегов")
    print("=" * 50)

    scan_and_fix(auto_confirm=auto_confirm)


if __name__ == "__main__":
    main()
