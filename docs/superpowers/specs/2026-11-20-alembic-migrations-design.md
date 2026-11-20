# Alembic migrations — short design

Date: 2026-11-20 · Status: approved by the candidate

## Intent
Replace the home-made migration runner (`app/migrate.py` + `schema_migrations`) with Alembic, the standard tool a
reviewer expects, without changing the schema or the app's "plain SQL, no ORM" stance.

## Goals / non-goals
- Goals: same schema as today; real downgrades; one migration path used by the consumer, `just migrate`, tests and
  `eval/scale_bench.py`; `/readyz` reports "not migrated" correctly; safe if two processes migrate at once.
- Non-goals: SQLAlchemy models, autogenerate, Celery (dropped by the candidate).
- Anti-goals: any schema drift from `001_init.sql` + `002_embedding_inline.sql`; the app gaining an ORM.

## Approaches
- **Chosen — hand-written revisions running plain SQL (`op.execute`).** Alembic uses a sync SQLAlchemy engine with
  the psycopg driver only for migrations; the app keeps async psycopg.
- Rejected — SQLAlchemy `Table` metadata for autogenerate: duplicates the schema in Python, needs a pgvector type
  adapter, contradicts "all SQL in one place".

## Design (decided)
- `alembic.ini` at the repo root; script location `migrations/` (`env.py`, `script.py.mako`, `versions/`).
- Revisions `0001_init` (today's `001_init.sql`) and `0002_embedding_inline` (today's `002`), each with a downgrade
  that exactly reverses it. The old `.sql` files are removed (their SQL moves into the revisions).
- `env.py` takes the URL from `Settings.from_env().database_url` (or `-x url=` / config override for tests),
  rewrites `postgresql://` → `postgresql+psycopg://`, and holds `pg_advisory_lock` for the run.
- `app/migrate.py` becomes a thin wrapper: `upgrade_to_head(database_url)` (calls `alembic.command.upgrade`),
  `head_revision()`, and `async is_at_head(conn) -> bool` for `/readyz`. `python -m app.migrate` stays for `just migrate`
  (or the recipe calls `alembic upgrade head`; builder's call, documented).
- Consumer calls `upgrade_to_head` via `asyncio.to_thread` at start-up; test fixture and `scale_bench` call it too.
- `/readyz`: 200 only when `alembic_version.version_num == head`.
- Dev DB: `alembic stamp head`, then drop `schema_migrations`. No automatic adoption of old databases (graders start
  empty).
- Tests: upgrade→downgrade base→upgrade round trip; schema after upgrade matches what the app needs (tables, generated
  `active` column, `STORAGE MAIN`); `/readyz` 503 on a database stamped at an older revision; existing suite green.
- Docs: README (commands, layout) and DESIGN (module table, migrations, readyz, "autogenerate deliberately unused").

## Left to the builder
File/function naming inside `migrations/`, whether `just migrate` calls Alembic directly, test structure.
