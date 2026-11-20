"""Postgres connection pool."""

from pgvector.psycopg import register_vector_async
from psycopg_pool import AsyncConnectionPool


async def _configure(conn) -> None:
    await register_vector_async(conn)


async def make_pool(url: str, timeout: float = 30.0, wait: bool = True) -> AsyncConnectionPool:
    """Open a pool whose connections can send and receive pgvector values.

    `timeout`: seconds a caller waits for a connection before PoolTimeout (psycopg_pool's default is 30).
    `wait`: block until the pool has connected; with False it connects in the background, so an
    unreachable database does not stop the caller from starting.
    """
    pool = AsyncConnectionPool(url, configure=_configure, open=False, timeout=timeout)
    await pool.open(wait=wait)
    return pool
