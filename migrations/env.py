"""Alembic environment: plain-SQL revisions, run online under an advisory lock.

There are no SQLAlchemy models (the app has no ORM), so autogenerate is not used: each revision in
versions/ is hand-written SQL. SQLAlchemy is only the connection Alembic needs while migrating.
"""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool, text

from app.config import Settings
from app.migrate import sqlalchemy_url

# Any constant shared by every migrator. Held for the whole run so the consumer and `just migrate`
# starting together take turns instead of both trying to create the same tables.
MIGRATION_LOCK_ID = 7_241_983

config = context.config

# Only when run from the CLI with alembic.ini; never disable loggers the app already configured.
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)


def database_url() -> str:
    return config.get_main_option("sqlalchemy.url") or sqlalchemy_url(Settings.from_env().database_url)


def run_migrations_online() -> None:
    engine = create_engine(database_url(), poolclass=pool.NullPool)
    with engine.connect() as connection:
        connection.execute(text("SELECT pg_advisory_lock(:id)"), {"id": MIGRATION_LOCK_ID})
        connection.commit()
        try:
            context.configure(connection=connection)
            with context.begin_transaction():
                context.run_migrations()
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(:id)"), {"id": MIGRATION_LOCK_ID})
            connection.commit()


if context.is_offline_mode():
    raise SystemExit("Offline (--sql) mode is not supported: run migrations against a database.")
run_migrations_online()
