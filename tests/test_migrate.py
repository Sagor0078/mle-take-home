import psycopg

from app.migrate import migrate


async def test_migrate_is_idempotent(database_url):
    async with await psycopg.AsyncConnection.connect(database_url, autocommit=True) as conn:
        assert await migrate(conn) == []  # the fixture already applied it
        cur = await conn.execute("SELECT name FROM schema_migrations")
        assert [r[0] for r in await cur.fetchall()] == ["001_init.sql", "002_embedding_inline.sql"]


async def test_tables_exist(database_url):
    expected = {"companies", "company_events", "processed_messages", "rejected_messages", "embedding_cache"}
    async with await psycopg.AsyncConnection.connect(database_url, autocommit=True) as conn:
        cur = await conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        assert expected <= {r[0] for r in await cur.fetchall()}


async def test_embeddings_are_stored_in_the_row(database_url):
    # EXTERNAL (pgvector's default) moves vectors to TOAST and made the similarity scan ~5x slower.
    async with await psycopg.AsyncConnection.connect(database_url, autocommit=True) as conn:
        cur = await conn.execute(
            "SELECT attstorage FROM pg_attribute"
            " WHERE attrelid = 'companies'::regclass AND attname = 'embedding'"
        )
        assert (await cur.fetchone())[0] == "m"  # MAIN
