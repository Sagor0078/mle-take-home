"""Postgres connection pool."""

from pgvector.psycopg import register_vector_async
from psycopg_pool import AsyncConnectionPool


async def _configure(conn) -> None:
    await register_vector_async(conn)


async def make_pool(url: str) -> AsyncConnectionPool:
    """Open a pool whose connections can send and receive pgvector values."""
    pool = AsyncConnectionPool(url, configure=_configure, open=False)
    await pool.open(wait=True)
    return pool
