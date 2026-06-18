#!/usr/bin/env python3
"""Backfill AI sentiment for posts that don't have it yet."""
from __future__ import annotations

import asyncio
import os
import sys

# Allow imports from parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from config import settings
from models import Post

BATCH_SIZE = 50


async def backfill() -> None:
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    import sentiment_ai

    async with async_session() as session:
        result = await session.execute(
            select(Post.id, Post.text)
            .where(Post.sentiment_label.is_(None))
            .where(Post.text.isnot(None))
            .where(Post.text != "")
            .order_by(Post.id)
        )
        rows = result.all()

    if not rows:
        print("No posts without sentiment.")
        return

    print(f"Found {len(rows)} posts to analyze")

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        ids = [r.id for r in batch]
        texts = [r.text for r in batch]

        results = await sentiment_ai.analyze_batch(texts)

        async with async_session() as session:
            for post_id, res in zip(ids, results):
                await session.execute(
                    text("""
                        UPDATE posts
                        SET sentiment_label = :label,
                            sentiment_score = :score,
                            sentiment_source = :source
                        WHERE id = :id
                    """),
                    {
                        "id": post_id,
                        "label": res["label"],
                        "score": res["score"],
                        "source": res["source"],
                    },
                )
            await session.commit()

        print(f"  processed {min(i + BATCH_SIZE, len(rows))}/{len(rows)}")

    print("Done.")


if __name__ == "__main__":
    asyncio.run(backfill())
