"""Schema migrations through Alembic (revisions live in migrations/versions/)."""

from pathlib import Path

import psycopg
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from psycopg.conninfo import conninfo_to_dict
from sqlalchemy.engine import URL

from app.config import Settings

ROOT = Path(__file__).resolve().parent.parent


def sqlalchemy_url(database_url: str) -> str:
    """The same database as a SQLAlchemy URL with the psycopg 3 driver, as Alembic needs it.

    Accepts anything libpq does: postgres:// and postgresql:// URLs or "key=value" strings.
    """
    if database_url.startswith("postgresql+psycopg://"):
        return database_url
    params = conninfo_to_dict(database_url)
    host = params.pop("host", None)
    if host and str(host).startswith("/"):
        params["host"] = host  # a unix-socket directory goes in the query string, not the URL's host
        host = None
    url = URL.create(
        "postgresql+psycopg",
        username=params.pop("user", None),
        password=params.pop("password", None),
        host=host,
        port=int(params["port"]) if "port" in params else None,
        database=params.pop("dbname", None),
        query={k: str(v) for k, v in params.items() if k != "port"},
    )
    return url.render_as_string(hide_password=False)


def alembic_config(database_url: str) -> Config:
    """Alembic settings from alembic.ini, pointed at this database."""
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    # ConfigParser treats % as interpolation; escape it in case the password contains one.
    config.set_main_option("sqlalchemy.url", sqlalchemy_url(database_url).replace("%", "%%"))
    # Called from inside the app: leave the app's logging alone (env.py skips alembic.ini's).
    config.attributes["configure_logger"] = False
    return config


def upgrade_to_head(database_url: str) -> None:
    """Apply every pending revision. Blocking: async callers use asyncio.to_thread."""
    command.upgrade(alembic_config(database_url), "head")


def head_revision() -> str:
    """The newest revision id in migrations/versions/."""
    return ScriptDirectory.from_config(alembic_config("postgresql://unused")).get_current_head()


async def is_at_head(conn: psycopg.AsyncConnection) -> bool:
    """True when the database has been migrated to the newest revision."""
    cur = await conn.execute("SELECT to_regclass('alembic_version') IS NOT NULL")
    if not (await cur.fetchone())[0]:
        return False
    cur = await conn.execute("SELECT version_num FROM alembic_version")
    row = await cur.fetchone()
    return row is not None and row[0] == head_revision()


def main() -> None:
    url = Settings.from_env().database_url
    upgrade_to_head(url)
    print(f"at {head_revision()}")


if __name__ == "__main__":
    main()
