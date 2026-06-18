#!/usr/bin/env python3
"""One-time cleanup: deactivate channels auto-created by sync_dialogs().

Usage:
    export DATABASE_URL="postgresql+asyncpg://..."
    python3 scripts/deactivate_auto_created_channels.py

Or with the sync DSN:
    export DATABASE_URL="postgresql://citg_db_user:..."
    python3 scripts/deactivate_auto_created_channels.py
"""
import asyncio
import os
import sys
from datetime import datetime, timezone

import asyncpg

# Deactivate channels created on or after this timestamp.
# Default: 2026-06-18 UTC, when v3 started auto-creating broadcast channels.
CUTOFF_UTC = datetime(2026, 6, 18, 0, 0, 0, tzinfo=timezone.utc)


async def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("ERROR: DATABASE_URL is not set", file=sys.stderr)
        return 1

    # asyncpg accepts postgresql:// or postgresql+asyncpg://
    dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")

    conn = await asyncpg.connect(dsn=dsn)
    try:
        preview = await conn.fetch(
            """
            SELECT id, telegram_id, numeric_id, title, username, created_at
            FROM channels
            WHERE created_at >= $1
              AND is_active = true
            ORDER BY created_at DESC
            """,
            CUTOFF_UTC,
        )
        print(f"Channels to deactivate (created >= {CUTOFF_UTC.isoformat()}): {len(preview)}")
        for row in preview[:20]:
            print(
                f"  id={row['id']} tid={row['telegram_id']} "
                f"title='{row['title']}' created_at={row['created_at']}"
            )
        if len(preview) > 20:
            print(f"  ... and {len(preview) - 20} more")

        if not preview:
            print("Nothing to deactivate.")
            return 0

        confirm = input("Deactivate these channels? [y/N]: ")
        if confirm.lower() not in ("y", "yes"):
            print("Aborted.")
            return 0

        result = await conn.execute(
            """
            UPDATE channels
            SET is_active = false
            WHERE created_at >= $1
              AND is_active = true
            """,
            CUTOFF_UTC,
        )
        print("Done:", result)
        return 0
    finally:
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
