import asyncio
import uuid

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from psycopg.conninfo import make_conninfo

from app.config import Settings
from app.db import make_pool
from app.embedding import FakeEmbedder
from app.migrate import upgrade_to_head


def _url_for(base_url: str, dbname: str) -> str:
    return make_conninfo(base_url, dbname=dbname)


@pytest_asyncio.fixture(scope="session")
async def database_url():
    """A throwaway, migrated database on the configured server, dropped at the end."""
    base = Settings.from_env().database_url
    admin_url = _url_for(base, "postgres")
    name = f"companies_test_{uuid.uuid4().hex[:8]}"
    async with await psycopg.AsyncConnection.connect(admin_url, autocommit=True) as admin:
        await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    url = _url_for(base, name)
    try:
        await asyncio.to_thread(upgrade_to_head, url)
        yield url
    finally:
        async with await psycopg.AsyncConnection.connect(admin_url, autocommit=True) as admin:
            await admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
            )


@pytest.fixture
def fresh_db_url():
    """An empty database for one test (the session database is already migrated)."""
    base = Settings.from_env().database_url
    admin_url = make_conninfo(base, dbname="postgres")
    name = f"companies_mig_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        yield make_conninfo(base, dbname=name)
    finally:
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


@pytest_asyncio.fixture(scope="session")
async def pool(database_url):
    p = await make_pool(database_url)
    yield p
    await p.close()


async def truncate_all(pool) -> None:
    """Empty every table except alembic_version."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname = 'public' AND tablename <> 'alembic_version'"
        )
        tables = [row[0] for row in await cur.fetchall()]
        if tables:
            names = sql.SQL(", ").join(sql.Identifier(t) for t in tables)
            await conn.execute(sql.SQL("TRUNCATE {} RESTART IDENTITY").format(names))


@pytest_asyncio.fixture
async def clean_db(pool):
    """Start the test with empty tables."""
    await truncate_all(pool)


@pytest.fixture
def embedder():
    """Deterministic offline embedder; tests never download a model."""
    return FakeEmbedder()
