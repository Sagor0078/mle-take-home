# Alembic Migrations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the home-made SQL migration runner with Alembic (hand-written plain-SQL revisions), used by every caller, with `/readyz` checking the Alembic head.

**Architecture:** Alembic owns `migrations/` (env.py + versions/). It connects through a sync SQLAlchemy engine with the psycopg driver only while migrating; the app stays async psycopg with no ORM. `app/migrate.py` becomes a thin wrapper the consumer, tests, bench and API call.

**Tech Stack:** alembic 1.20.x (pulls SQLAlchemy 2.1.x), psycopg 3, existing app.

**Spec:** `docs/superpowers/specs/2026-11-20-alembic-migrations-design.md` (and, for the schema itself, `docs/superpowers/specs/2026-11-20-company-recommendations-design.md` §6.4).

## Global Constraints

- No ORM, no SQLAlchemy models, no autogenerate. Revisions run plain SQL via `op.execute`.
- Schema after `upgrade head` must be byte-for-byte the SQL in today's `migrations/001_init.sql` then `002_embedding_inline.sql`.
- Revision ids are exactly `0001_init` and `0002_embedding_inline` (`down_revision` `None` and `0001_init`).
- Migrations hold `pg_advisory_lock(7_241_983)` for the whole run (constant named `MIGRATION_LOCK_ID` in `env.py`).
- `env.py` must not disable the app's loggers (`fileConfig(..., disable_existing_loggers=False)`, and only when Alembic is invoked from the CLI with a config file).
- Environment: dev Postgres on 5435 / RabbitMQ on 5673 via the git-ignored `.env`; run tools as `set -a; . ./.env; set +a; uv run ...`. Never `just down`.
- Commit trailer: `Co-Authored-By: <your model> <noreply@anthropic.com>`.

## Review Focus

1. **Consumer and `just migrate` start at the same moment on an empty DB** → one waits on the advisory lock, both end at head, no "relation already exists". Test in Task 1 (`test_concurrent_upgrades_serialise`).
2. **`DATABASE_URL` given as `postgres://` or already `postgresql+psycopg://`** → both work. Test in Task 1 (`test_sqlalchemy_url_variants`).
3. **Running the consumer after `upgrade_to_head`** → its `app.*` loggers still emit (Alembic's `fileConfig` must not disable them). Test in Task 1 (`test_upgrade_keeps_app_loggers_enabled`).
4. **`/readyz` on a database with no `alembic_version` table at all** → 503 JSON, not 500. Test in Task 2 (`test_readyz_unmigrated_database`).
5. **`/readyz` on a database stamped at an older revision** → 503. Test in Task 2 (`test_readyz_behind_head`).

---

## File Structure

```
alembic.ini                                  create — script_location = migrations; logging sections
migrations/env.py                            create — URL from config or Settings; advisory lock; online only
migrations/script.py.mako                    create — standard template
migrations/versions/0001_init.py             create — SQL of old 001_init.sql; downgrade drops tables
migrations/versions/0002_embedding_inline.py create — SQL of old 002; downgrade SET STORAGE EXTENDED
migrations/001_init.sql, 002_embedding_inline.sql   delete
app/migrate.py                               rewrite — Alembic wrapper
app/consumer.py, tests/conftest.py, eval/scale_bench.py, app/api.py   modify — callers
tests/test_migrate.py                        rewrite
Justfile, README.md, DESIGN.md               modify
```

---

### Task 1: Alembic scaffold, revisions, wrapper, callers

**Files:**
- Create: `alembic.ini`, `migrations/env.py`, `migrations/script.py.mako`, `migrations/versions/0001_init.py`, `migrations/versions/0002_embedding_inline.py`
- Delete: `migrations/001_init.sql`, `migrations/002_embedding_inline.sql`
- Rewrite: `app/migrate.py`, `tests/test_migrate.py`
- Modify: `app/consumer.py` (start-up migrate), `tests/conftest.py` (fixture + `truncate_all` excludes `alembic_version` instead of `schema_migrations`), `eval/scale_bench.py`, `Justfile` (`migrate: uv run alembic upgrade head`), `pyproject.toml` (`uv add alembic`)

**Interfaces:**
- Produces (`app/migrate.py`):
```python
def sqlalchemy_url(database_url: str) -> str        # postgres:// | postgresql:// | postgresql+psycopg:// -> postgresql+psycopg://
def alembic_config(database_url: str) -> alembic.config.Config   # script_location=<repo>/migrations, sqlalchemy.url set
def upgrade_to_head(database_url: str) -> None      # alembic.command.upgrade(cfg, "head"); sync — async callers use asyncio.to_thread
def head_revision() -> str                          # ScriptDirectory head, e.g. "0002_embedding_inline"
async def is_at_head(conn: psycopg.AsyncConnection) -> bool   # False if alembic_version missing or != head
def main() -> None                                  # `python -m app.migrate`: upgrade, print "at <rev>"
```
- `env.py`: URL = `config.get_main_option("sqlalchemy.url")` if set, else `sqlalchemy_url(Settings.from_env().database_url)`. Offline mode not supported (raise a clear error). Run inside `connection.begin()`, take `pg_advisory_lock(MIGRATION_LOCK_ID)` first (session lock on the same connection; unlock in `finally`).

- [ ] **Step 1:** `uv add alembic`. Create `alembic.ini` (script_location `%(here)s/migrations`, no `sqlalchemy.url` line, standard `[loggers]` with `root` at WARN and `alembic` at INFO) and `migrations/script.py.mako` (Alembic's default template).
- [ ] **Step 2: Write failing tests** in `tests/test_migrate.py` (use a throwaway DB per test via a helper fixture that creates/drops `companies_mig_<hex>`; the session `database_url` fixture is for the app tests):

```python
def test_sqlalchemy_url_variants():
    for raw in ("postgres://u:p@h:1/d", "postgresql://u:p@h:1/d", "postgresql+psycopg://u:p@h:1/d"):
        assert sqlalchemy_url(raw) == "postgresql+psycopg://u:p@h:1/d"

def test_head_revision(): assert head_revision() == "0002_embedding_inline"

async def test_upgrade_creates_schema(fresh_db_url):
    await asyncio.to_thread(upgrade_to_head, fresh_db_url)
    # tables {companies, company_events, processed_messages, rejected_messages, embedding_cache, alembic_version} exist
    # companies.active is a generated column; companies.embedding attstorage == 'm'
    # alembic_version.version_num == "0002_embedding_inline"; is_at_head(conn) is True

async def test_upgrade_is_idempotent(fresh_db_url): two upgrades -> still head, no error

async def test_downgrade_round_trip(fresh_db_url):
    upgrade head -> command.downgrade(cfg, "base") -> none of the app tables exist, alembic_version empty
    -> upgrade head again -> schema as above

async def test_concurrent_upgrades_serialise(fresh_db_url):          # Review Focus 1
    await asyncio.gather(asyncio.to_thread(upgrade_to_head, fresh_db_url), asyncio.to_thread(upgrade_to_head, fresh_db_url))
    # no exception; at head

def test_upgrade_keeps_app_loggers_enabled(fresh_db_url_sync):     # Review Focus 3
    log = logging.getLogger("app.consumer"); upgrade_to_head(url); assert not log.disabled
    # and also via the CLI path: alembic.command with alembic_config(url) loaded from alembic.ini
```

- [ ] **Step 3:** Run `set -a; . ./.env; set +a; uv run pytest tests/test_migrate.py -v` → FAIL (import errors).
- [ ] **Step 4: Implement.** Move the SQL from `migrations/001_init.sql` into `0001_init.upgrade()` as one `op.execute(...)` per statement (keep comments), downgrade = `DROP TABLE` of the five tables in FK-safe order (leave the `vector` extension — other databases' objects may depend on it; say so in a comment). Move `002`'s SQL into `0002_embedding_inline.upgrade()`; downgrade = `ALTER TABLE companies ALTER COLUMN embedding SET STORAGE EXTENDED` (no row rewrite needed). Delete the two `.sql` files. Write `env.py` and `app/migrate.py` per Interfaces. In `env.py` only call `fileConfig(config.config_file_name, disable_existing_loggers=False)` when `config.config_file_name` is set.
- [ ] **Step 5: Update callers:** consumer start-up → `await asyncio.to_thread(upgrade_to_head, settings.database_url)` (drop the autocommit dance); `tests/conftest.py` → same in the `database_url` fixture, and `truncate_all` excludes `alembic_version`; `eval/scale_bench.py` → same; `Justfile` `migrate: uv run alembic upgrade head`.
- [ ] **Step 6:** Run `uv run pytest tests/test_migrate.py -v` → PASS, then the full suite `uv run pytest -W error` → PASS (161 + new tests; RabbitMQ tests need no `just consume` running).
- [ ] **Step 7: Move the dev DB onto Alembic** (no data loss): `uv run alembic stamp head`, then `psql`/psycopg `DROP TABLE schema_migrations`. Verify: `uv run alembic current` prints `0002_embedding_inline (head)`; `just migrate` is a no-op; companies count still 2000.
- [ ] **Step 8: Commit** `feat: Alembic migrations replace the home-made runner`

---

### Task 2: `/readyz` checks the Alembic head

**Files:**
- Modify: `app/api.py` (readyz), `tests/test_api.py`

**Interfaces:**
- Consumes: `is_at_head(conn)` (Task 1).

- [ ] **Step 1: Failing tests** in `tests/test_api.py`:

```python
async def test_readyz_at_head(client): 200 {"status": "ready"}
async def test_readyz_behind_head(client, pool):                    # Review Focus 5
    # UPDATE alembic_version SET version_num = '0001_init'; GET /readyz -> 503 {"status": "not_ready"}; restore in finally
async def test_readyz_unmigrated_database(...):                    # Review Focus 4
    # app pointed at a fresh empty DB (no alembic_version) -> 503 JSON, not 500
```

- [ ] **Step 2:** Run → FAIL. **Step 3:** Replace the `schema_migrations` query in `/readyz` with `await is_at_head(conn)`; keep the existing `psycopg.Error` / `PoolTimeout` → 503 handling. **Step 4:** Run → PASS; full suite PASS.
- [ ] **Step 5: Commit** `feat: /readyz reports ready only at the Alembic head`

---

### Task 3: Docs

**Files:**
- Modify: `README.md`, `DESIGN.md`

- [ ] **Step 1:** README: `just migrate` now runs `alembic upgrade head`; add `uv run alembic current` / `uv run alembic downgrade -1` to a short "Migrations" note; layout block (`migrations/` = Alembic env + `versions/0001_init.py`, `0002_embedding_inline.py`); `/readyz` meaning ("database reachable and at the latest Alembic revision"); update the test count to the actual collected number.
- [ ] **Step 2:** DESIGN: module table row for `app/migrate.py` and `migrations/`; replace every `schema_migrations` / `001_init.sql` / `002_embedding_inline.sql` mention (grep for them); one decision paragraph: why Alembic, why plain-SQL revisions and no autogenerate (no ORM; schema stays readable SQL), advisory lock for concurrent migrators, downgrade policy (extension left in place); the `STORAGE MAIN` note now points at revision `0002_embedding_inline`.
- [ ] **Step 3:** Verify: `grep -rn "schema_migrations\|001_init.sql\|002_embedding_inline.sql" README.md DESIGN.md app tests eval Justfile` → no hits; `just --list`; `just migrate` prints Alembic output and is a no-op on the dev DB.
- [ ] **Step 4: Commit** `docs: describe Alembic migrations`
