"""Helpers for replaying the registry feed through handle_message."""

import asyncio
import hashlib
from collections import Counter
from pathlib import Path

from app.ingest import handle_message

FEED_PATH = Path(__file__).resolve().parents[1] / "data" / "feed.jsonl"
FEED_LINES = FEED_PATH.read_text(encoding="utf-8").splitlines()


async def replay(pool, embedder, lines: list[str]) -> Counter:
    """Handle every line as one message, a pool's worth at a time, and count the outcomes."""
    limit = asyncio.Semaphore(pool.max_size)

    async def handle(line: str) -> str:
        async with limit:
            message_id = hashlib.sha256(line.encode()).hexdigest()
            return await handle_message(pool, embedder, line.encode(), message_id)

    return Counter(await asyncio.gather(*(handle(line) for line in lines)))


async def snapshot(conn) -> list[tuple]:
    """Every companies column (embedding as text), ordered by reg_no."""
    cur = await conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = 'companies' ORDER BY ordinal_position"
    )
    columns = [
        "embedding::text" if name == "embedding" else name for (name,) in await cur.fetchall()
    ]
    cur = await conn.execute(f"SELECT {', '.join(columns)} FROM companies ORDER BY reg_no")
    return await cur.fetchall()
