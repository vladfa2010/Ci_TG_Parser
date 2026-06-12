#!/usr/bin/env python3
"""Retroactively extract hashtags. SQL fixed for JSON column."""
import json, os, re, sys
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
SYNC_URL = DATABASE_URL.replace("+asyncpg", "")

HASHTAG_RE = re.compile(r"#\S+")
BATCH_SIZE = 500

def extract_hashtags(text):
    return HASHTAG_RE.findall(text) if text else []

def get_stats(conn):
    total = conn.execute(text("SELECT COUNT(*) FROM posts")).scalar()
    # FIXED: use ::text cast for JSON comparison
    empty = conn.execute(text("""
        SELECT COUNT(*) FROM posts
        WHERE hashtags IS NULL
           OR hashtags::text = '[]'
           OR json_typeof(hashtags) != 'array'
    """)).scalar()
    tagged = conn.execute(text("""
        SELECT COUNT(*) FROM posts
        WHERE json_typeof(hashtags) = 'array'
          AND json_array_length(hashtags) > 0
    """)).scalar()
    sample = conn.execute(text("""
        SELECT telegram_message_id, LEFT(text, 60), hashtags::text
        FROM posts
        WHERE hashtags::text = '[]'
          AND text LIKE '%#%'
        LIMIT 5
    """)).fetchall()
    return {"total": total, "empty": empty, "tagged": tagged, "sample": sample}

def process_batch(conn, rows):
    updated = 0
    skipped = 0
    for row in rows:
        post_id, text_content = row[0], row[1] or ""
        new_tags = extract_hashtags(text_content)
        if not new_tags:
            skipped += 1
            continue
        conn.execute(
            text("UPDATE posts SET hashtags = :tags WHERE id = :id"),
            {"tags": json.dumps(new_tags), "id": post_id},
        )
        updated += 1
    return updated, skipped

def main():
    if not SYNC_URL:
        print("ERROR: DATABASE_URL not set"); sys.exit(1)

    print("=" * 60)
    print("HASHTAG RETROACTIVE FIX")
    print("=" * 60)
    print(f"DB: {SYNC_URL.split('@')[-1] if '@' in SYNC_URL else 'local'}")
    print()

    engine = create_engine(SYNC_URL)
    with engine.connect() as conn:
        print("--- BEFORE ---")
        before = get_stats(conn)
        print(f"  Total posts:     {before['total']}")
        print(f"  Empty hashtags:  {before['empty']}")
        print(f"  Tagged posts:    {before['tagged']}")
        print()

        if before["sample"]:
            print("  Sample posts with #text but [] hashtags:")
            for row in before["sample"]:
                print(f"    ID {row[0]}: {row[1]}... | tags={row[2]}")
            print()

        if before["empty"] == 0:
            print("No posts to fix. Exiting."); return

        confirm = input(f"Fix {before['empty']} posts? [y/N]: ").strip().lower()
        if confirm != "y":
            print("Aborted."); return

        print("\nProcessing...")
        total_updated = 0
        total_skipped = 0
        offset = 0

        while True:
            rows = conn.execute(text("""
                SELECT id, text
                FROM posts
                WHERE hashtags IS NULL
                   OR hashtags::text = '[]'
                   OR json_typeof(hashtags) != 'array'
                ORDER BY id
                LIMIT :limit OFFSET :offset
            """), {"limit": BATCH_SIZE, "offset": offset}).fetchall()

            if not rows:
                break

            updated, skipped = process_batch(conn, rows)
            conn.commit()
            total_updated += updated
            total_skipped += skipped
            offset += len(rows)
            print(f"  Batch: +{updated} fixed, {skipped} no-tags, total: {offset}")

        print("\n--- AFTER ---")
        after = get_stats(conn)
        print(f"  Total posts:     {after['total']}")
        print(f"  Empty hashtags:  {after['empty']}")
        print(f"  Tagged posts:    {after['tagged']}")
        print(f"\nPosts fixed: {total_updated}")
        print(f"Posts still empty (no # in text): {total_skipped}")
        print("\nDone!")

if __name__ == "__main__":
    start = datetime.now(timezone.utc)
    main()
    print(f"Time: {(datetime.now(timezone.utc) - start).total_seconds():.1f}s")
