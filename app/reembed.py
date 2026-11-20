"""Re-embed every company with the configured model: `just reembed` after changing EMBED_MODEL.

refresh_embedding already recomputes a row whose stored embedding_model differs from the
embedder's, and leaves an up-to-date row alone, so this is safe to run more than once.
"""

import asyncio

from psycopg_pool import AsyncConnectionPool

from app.config import Settings
from app.db import make_pool
from app.embedding import Embedder, make_embedder
from app.store import refresh_embedding


async def reembed(pool: AsyncConnectionPool, embedder: Embedder) -> int:
    """Refresh every company's embedding, one transaction per company. Returns the count."""
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT reg_no FROM companies ORDER BY reg_no")
        reg_nos = [row[0] for row in await cur.fetchall()]
    for reg_no in reg_nos:
        async with pool.connection() as conn, conn.transaction():
            # Lock the row like the consumer does, so a concurrent update cannot interleave.
            await conn.execute("SELECT 1 FROM companies WHERE reg_no = %s FOR UPDATE", (reg_no,))
            await refresh_embedding(conn, reg_no, embedder)
    return len(reg_nos)


async def main() -> None:
    settings = Settings.from_env()
    embedder = make_embedder(settings.embed_model)
    pool = await make_pool(settings.database_url)
    try:
        count = await reembed(pool, embedder)
    finally:
        await pool.close()
    print(f"refreshed {count} companies with {embedder.model_name}")


if __name__ == "__main__":
    asyncio.run(main())
