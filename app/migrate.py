"""Apply the SQL files in migrations/ in name order, once each."""

import asyncio
from pathlib import Path

import psycopg

from app.config import Settings

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


async def migrate(conn: psycopg.AsyncConnection) -> list[str]:
    """Apply unapplied migrations (autocommit connection) and return their names."""
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
    )
    cur = await conn.execute("SELECT name FROM schema_migrations")
    done = {row[0] for row in await cur.fetchall()}
    applied = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if path.name in done:
            continue
        async with conn.transaction():
            await conn.execute(path.read_text())
            await conn.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (path.name,))
        applied.append(path.name)
    return applied


async def main() -> None:
    async with await psycopg.AsyncConnection.connect(
        Settings.from_env().database_url, autocommit=True
    ) as conn:
        applied = await migrate(conn)
    print(f"applied: {applied}" if applied else "up to date")


if __name__ == "__main__":
    asyncio.run(main())
