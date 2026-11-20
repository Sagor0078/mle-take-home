import asyncio
import logging
import os
import subprocess
import sys

import psycopg
import pytest
from alembic import command
from sqlalchemy.engine import make_url

from app.migrate import ROOT, alembic_config, head_revision, is_at_head, sqlalchemy_url, upgrade_to_head

APP_TABLES = {"companies", "company_events", "processed_messages", "rejected_messages", "embedding_cache"}


def tables(url: str) -> set[str]:
    with psycopg.connect(url) as conn:
        rows = conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'").fetchall()
    return {row[0] for row in rows}


def version(url: str) -> str | None:
    with psycopg.connect(url) as conn:
        row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
    return row[0] if row else None


def assert_full_schema(url: str) -> None:
    assert APP_TABLES | {"alembic_version"} <= tables(url)
    assert version(url) == "0002_embedding_inline"
    with psycopg.connect(url) as conn:
        generated = conn.execute(
            "SELECT is_generated FROM information_schema.columns"
            " WHERE table_name = 'companies' AND column_name = 'active'"
        ).fetchone()[0]
        storage = conn.execute(
            "SELECT attstorage FROM pg_attribute"
            " WHERE attrelid = 'companies'::regclass AND attname = 'embedding'"
        ).fetchone()[0]
    assert generated == "ALWAYS"
    assert storage == "m"  # MAIN: vectors kept in the row (see revision 0002)


@pytest.mark.parametrize(
    "raw", ["postgres://u:p@h:1/d", "postgresql://u:p@h:1/d", "postgresql+psycopg://u:p@h:1/d"]
)
def test_sqlalchemy_url_variants(raw):
    assert sqlalchemy_url(raw) == "postgresql+psycopg://u:p@h:1/d"


@pytest.mark.parametrize(
    "raw", ["host=/var/run/postgresql dbname=d user=u", "postgresql://u@/d?host=/var/run/postgresql"]
)
def test_sqlalchemy_url_unix_socket(raw):
    url = make_url(sqlalchemy_url(raw))
    assert (url.host, url.database, url.query.get("host")) == (None, "d", "/var/run/postgresql")


def test_sqlalchemy_url_from_keyword_conninfo():
    # psycopg's make_conninfo (used by the test fixtures) returns "key=value" strings, not URLs.
    assert sqlalchemy_url("host=h port=1 dbname=d user=u password=p") == "postgresql+psycopg://u:p@h:1/d"


def test_head_revision():
    assert head_revision() == "0002_embedding_inline"


async def test_upgrade_creates_schema(fresh_db_url):
    await asyncio.to_thread(upgrade_to_head, fresh_db_url)
    assert_full_schema(fresh_db_url)
    async with await psycopg.AsyncConnection.connect(fresh_db_url) as conn:
        assert await is_at_head(conn)


def test_upgrade_is_idempotent(fresh_db_url):
    upgrade_to_head(fresh_db_url)
    upgrade_to_head(fresh_db_url)
    assert_full_schema(fresh_db_url)


def test_downgrade_round_trip(fresh_db_url):
    upgrade_to_head(fresh_db_url)
    command.downgrade(alembic_config(fresh_db_url), "base")
    assert not (APP_TABLES & tables(fresh_db_url))
    assert version(fresh_db_url) is None
    upgrade_to_head(fresh_db_url)
    assert_full_schema(fresh_db_url)


def test_concurrent_upgrades_serialise(fresh_db_url):
    # The consumer and `just migrate` may start together. They are separate processes (Alembic's
    # context is process-global), and the advisory lock in env.py makes one wait for the other.
    env = {**os.environ, "DATABASE_URL": fresh_db_url}
    procs = [
        subprocess.Popen([sys.executable, "-m", "app.migrate"], env=env, cwd=ROOT,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for _ in range(2)
    ]
    results = [p.communicate(timeout=60) for p in procs]
    assert [p.returncode for p in procs] == [0, 0], results
    assert_full_schema(fresh_db_url)


async def test_is_at_head_false_without_alembic_version(fresh_db_url):
    async with await psycopg.AsyncConnection.connect(fresh_db_url) as conn:
        assert not await is_at_head(conn)


def test_upgrade_keeps_app_logging(fresh_db_url):
    # The consumer configures INFO logging, then migrates. Alembic's ini logging must not touch it:
    # fileConfig() would reset the root logger to WARNING and silence the consumer's running totals.
    root = logging.getLogger()
    before = (root.level, list(root.handlers))
    root.setLevel(logging.INFO)
    try:
        upgrade_to_head(fresh_db_url)
        assert root.level == logging.INFO
        assert logging.getLogger("consumer").isEnabledFor(logging.INFO)
    finally:
        root.setLevel(before[0])
        root.handlers[:] = before[1]
