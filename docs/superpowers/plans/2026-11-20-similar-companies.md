# Similar Companies Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consume the `company-events` RabbitMQ feed into Postgres/pgvector with order-independent per-field last-writer-wins, and serve a "similar companies" API from it.

**Architecture:** Pure parsing (`events.py`) → one transaction per message (`ingest.py`: dedupe, reject-or-apply, refresh embedding) over plain SQL (`store.py`). The embedding is *derived from the stored row* after each apply, so it is order-independent too. FastAPI reads the same tables. aio-pika consumer is a thin ack/nack shell around `handle_message`.

**Tech Stack:** Python 3.14 (uv), aio-pika, psycopg 3 (async) + psycopg_pool, pgvector, numpy, fastembed 0.9.x, FastAPI + uvicorn, pytest + pytest-asyncio + httpx.

**Spec:** `docs/superpowers/specs/2026-11-20-company-recommendations-design.md`

## Global Constraints

- Python `>=3.14,<3.15`; dependencies added with `uv add` (dev deps with `uv add --dev`).
- Never declare `company-events` with anything but `durable=True` and no arguments.
- No ORM; SQL lives in `app/store.py` and `migrations/*.sql`.
- Vectors are `vector(384)`, L2-normalised, cosine distance (`<=>`).
- Default model `BAAI/bge-small-en-v1.5`; candidate `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` (both 384-dim, both in fastembed 0.9.1). Tests never download a model: they use `FakeEmbedder`.
- Naive timestamps are `+08:00`. Epoch numbers ≥ 10^11 are milliseconds, otherwise seconds.
- `null`, `""`, missing, and `companyName == "Unknown"` mean *absent*: never overwrite.
- Field tie-break: `(event_at, message_id)` compared lexicographically; missing `event_at` = `-infinity`.
- Integration tests need `just up` (they use the compose Postgres/RabbitMQ, creating and dropping a throwaway database `companies_test_<hex>`).
- API JSON uses camelCase (`registrationNumber`), matching the feed.
- Build order = spec §11 priority. If the 6-hour stop hits, stop at a task boundary and record the rest in `DESIGN.md`.

## Review Focus

1. **Same message delivered twice concurrently** (prefetch > 1, redelivery during processing) → exactly one apply; second is `duplicate`. Test in Task 4 (`test_concurrent_duplicate_applies_once`).
2. **Struck-off company requested by a stale/odd reg-no form** (`" syn-001748 "` in the URL) → normalised, 200 card with `status: struck_off`. Test in Task 6 (`test_struck_off_card_with_messy_reg_no`).
3. **Description changes from text A → B → back to A** → embedding follows the *final* text, cache reused, no stale vector. Test in Task 4 (`test_embedding_follows_final_description`).
4. **Database unavailable while consuming** → message nacked with requeue, not recorded as rejected, consumer keeps running. Test in Task 7 (`test_transient_error_requeues`).
5. **Company with no embedding asked for similar** (4 in feed: no description and no industry) → 200, card, `similar: []`, `reason: "no_description"`. Test in Task 6 (`test_no_embedding_returns_empty_with_reason`).

---

## File Structure

```
app/
  __init__.py
  config.py      Settings.from_env()
  migrate.py     apply migrations/*.sql in order; `python -m app.migrate`
  db.py          make_pool(url) with pgvector registered
  events.py      parse_event(bytes) -> CompanyEvent | raises InvalidEvent   (pure)
  text.py        clean_description, embedding_text                          (pure)
  embedding.py   Embedder protocol, FakeEmbedder, FastEmbedder, text_hash
  store.py       all SQL: record/dedupe, reject, apply LWW, refresh embedding, reads
  ingest.py      handle_message(pool, embedder, body, message_id) -> Outcome
  consumer.py    aio-pika loop; `python -m app.consumer`
  api.py         FastAPI app; `uvicorn app.api:app`
migrations/001_init.sql
eval/
  evaluate.py    model comparison; `python -m eval.evaluate`
  pairs.csv      hand-labelled Malay<->English pairs
  bench.py       endpoint latency (Task 11)
tests/
  conftest.py
  test_migrate.py test_events.py test_text.py test_embedding.py test_store.py
  test_feed_replay.py test_api.py test_consumer.py test_eval_metrics.py test_consumer_rabbitmq.py
Justfile (modify)  README.md (append section)  DESIGN.md (create)
```

---

### Task 1: Project setup, config, migrations, test DB fixture

**Files:**
- Modify: `pyproject.toml` (via `uv add`), `Justfile`
- Create: `app/__init__.py`, `app/config.py`, `app/db.py`, `app/migrate.py`, `migrations/001_init.sql`, `tests/conftest.py`, `tests/test_migrate.py`

**Interfaces:**
- Produces:
  - `Settings` frozen dataclass: `database_url: str`, `rabbitmq_url: str`, `embed_model: str`, `prefetch: int`; `Settings.from_env() -> Settings`. Env: `DATABASE_URL` (default `postgresql://postgres:postgres@localhost:{POSTGRES_PORT or 5432}/companies`), `RABBITMQ_URL` (default `amqp://guest:guest@localhost:{RABBITMQ_PORT or 5672}/`), `EMBED_MODEL` (default `BAAI/bge-small-en-v1.5`), `PREFETCH` (default 32).
  - `async def migrate(conn: psycopg.AsyncConnection) -> list[str]` — applies unapplied `migrations/*.sql` in name order, records them in `schema_migrations(name text primary key, applied_at timestamptz)`, returns names applied.
  - `async def make_pool(url: str) -> AsyncConnectionPool` — opened pool whose `configure` callback calls `pgvector.psycopg.register_vector_async`.
  - Fixtures in `tests/conftest.py`: `database_url` (session; creates `companies_test_<hex>` via the admin URL, runs `migrate`, drops it at end), `pool` (session-scoped `make_pool(database_url)`), `clean_db` (function; `TRUNCATE` every table except `schema_migrations`), `embedder` (a `FakeEmbedder()` — fixture added in Task 3).

- [ ] **Step 1:** `uv add psycopg[binary] psycopg-pool pgvector numpy fastembed fastapi uvicorn` and `uv add --dev pytest pytest-asyncio httpx`. In `pyproject.toml` set `[tool.pytest.ini_options] asyncio_mode = "auto"`, `asyncio_default_fixture_loop_scope = "session"`, `asyncio_default_test_loop_scope = "session"`, `testpaths = ["tests"]`.

- [ ] **Step 2: Write `migrations/001_init.sql`** exactly to the spec §6.4 schema with these changes (all decided):
  - per field `f ∈ {name, nature, industry, state}`: `f text, f_updated_at timestamptz, f_message_id text`.
  - `companies` also: `last_upsert_at timestamptz, struck_off_at timestamptz, embedding vector(384), embedding_source text, embedding_model text, embedding_text_hash text, active boolean GENERATED ALWAYS AS (struck_off_at IS NULL OR COALESCE(last_upsert_at, '-infinity') > struck_off_at) STORED`.
  - `processed_messages` is the single "seen this message_id?" table; `company_events.message_id` references it (keep both).
  - No HNSW index yet (Task 11 decides). Add `CREATE INDEX ON companies (active) WHERE embedding IS NOT NULL`.

- [ ] **Step 3: Write the failing test** `tests/test_migrate.py`

```python
async def test_migrate_is_idempotent(database_url):
    async with await psycopg.AsyncConnection.connect(database_url, autocommit=True) as conn:
        assert await migrate(conn) == []          # fixture already applied it
        cur = await conn.execute("select name from schema_migrations")
        assert [r[0] for r in await cur.fetchall()] == ["001_init.sql"]

async def test_tables_exist(database_url):
    ...  # assert {"companies","company_events","processed_messages","rejected_messages","embedding_cache"} ⊆ tables in public
```

- [ ] **Step 4:** Run `uv run pytest tests/test_migrate.py -v` → FAIL (import error). Implement `config.py`, `db.py`, `migrate.py`, `conftest.py`. Run again → PASS.

- [ ] **Step 5:** Justfile recipes: `migrate: uv run python -m app.migrate`, `consume: uv run python -m app.consumer`, `serve: uv run uvicorn app.api:app --port ${API_PORT:-8000}`, `test: uv run pytest`, `eval: uv run python -m eval.evaluate`. Keep existing recipes unchanged (`setup`'s `uv sync --locked` now installs the new dependencies too).

- [ ] **Step 6: Commit** `git add -A && git commit -m "feat: project setup, schema migrations, test database fixture"`

---

### Task 2: Event parsing and normalisation (pure)

**Files:**
- Create: `app/events.py`, `tests/test_events.py`

**Interfaces:**
- Produces:
```python
EventType = Literal["company-upserted", "company-struck-off"]

@dataclass(frozen=True)
class CompanyFields:
    name: str | None = None
    nature: str | None = None
    industry: str | None = None
    state: str | None = None

@dataclass(frozen=True)
class CompanyEvent:
    type: EventType
    reg_no: str                    # "SYN-000123"
    event_at: datetime | None      # tz-aware; None = missing updatedAt
    fields: CompanyFields          # all None for struck-off or data null/missing
    payload: dict                  # the parsed JSON object, for company_events.payload

class InvalidEvent(Exception):
    reason: str   # one of REASONS
REASONS = ("invalid_json", "not_an_object", "unknown_type", "missing_registration_number",
           "invalid_registration_number", "invalid_updated_at", "invalid_data")

def parse_event(body: bytes) -> CompanyEvent          # raises InvalidEvent
def normalise_reg_no(value: object) -> str            # raises InvalidEvent
def parse_timestamp(value: object) -> datetime | None # raises InvalidEvent("invalid_updated_at")
def normalise_state(value: str | None) -> str | None
CANONICAL_STATES: tuple[str, ...]  # the 16 in spec §6.2
```

- [ ] **Step 1: Write the failing tests** — one per trap row in spec §3. Use literal lines (copy real lines from `data/feed.jsonl` where possible). Required cases and assertions:

```python
@pytest.mark.parametrize("raw,expected", [
    ("SYN-000123", "SYN-000123"), (" SYN-000123 ", "SYN-000123"), ("syn-000123", "SYN-000123"), (119, "SYN-000119")])
def test_normalise_reg_no(raw, expected): assert normalise_reg_no(raw) == expected

@pytest.mark.parametrize("raw", [None, "", "   "])
def test_missing_reg_no(raw): ... reason == "missing_registration_number"
@pytest.mark.parametrize("raw", ["ABC-1", "SYN-12", True, 1.5])
def test_invalid_reg_no(raw): ... reason == "invalid_registration_number"

MYT = timezone(timedelta(hours=8))
@pytest.mark.parametrize("raw,expected", [
    ("2026-08-14T09:00:00+08:00", datetime(2026,8,14,9,0,tzinfo=MYT)),
    ("2026-08-14T01:00:00Z",      datetime(2026,8,14,9,0,tzinfo=MYT)),
    ("2026-08-14T09:00:00",       datetime(2026,8,14,9,0,tzinfo=MYT)),
    (1786671038000,               datetime.fromtimestamp(1786671038, UTC)),
    (1786671038,                  datetime.fromtimestamp(1786671038, UTC)),
    ("1786671038000",             datetime.fromtimestamp(1786671038, UTC)),
    (None, None)])
def test_parse_timestamp(raw, expected): assert parse_timestamp(raw) == expected
def test_garbage_timestamp_rejected(): parse_event(...updatedAt "yesterday"...) -> reason "invalid_updated_at"

def test_truncated_json(): reason "invalid_json"     # use line 98 of the feed (index 97)
def test_control_char_inside_string_is_accepted(): body with a raw \t inside natureOfBusiness parses
def test_null_body(): parse_event(b"null") -> reason "not_an_object"
def test_unknown_type(): reason "unknown_type"
def test_data_string_rejected(): data "companyName=Foo; state=Perak" -> reason "invalid_data"
def test_data_null_is_empty_fields(): upsert with no data -> fields == CompanyFields()
def test_unknown_name_is_absent(): {"companyName": "Unknown"} -> fields.name is None
def test_blank_and_null_fields_are_absent(): "" / "  " / None -> None
def test_state_canonicalised(): "KUALA LUMPUR","kuala lumpur" -> "Kuala Lumpur"; "pulau pinang" -> "Pulau Pinang"
def test_unknown_state_kept_title_cased(): "atlantis" -> "Atlantis"
def test_extra_keys_ignored_but_kept_in_payload(): "source": "ssm-batch" -> payload["source"] == "ssm-batch"
def test_struck_off_has_empty_fields(): type "company-struck-off" -> fields == CompanyFields()
def test_non_string_field_is_absent(): natureOfBusiness 42 -> None
```

- [ ] **Step 2:** `uv run pytest tests/test_events.py -v` → FAIL.
- [ ] **Step 3:** Implement `app/events.py`. `json.loads(body.decode("utf-8"), strict=False)`; decode errors → `invalid_json`. Check order: JSON → object → type → reg no → updatedAt → data. Text fields are `str.strip()`ed and internal whitespace runs collapsed to one space. Strings that are all digits are epoch numbers.
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5: Commit** `feat: parse and normalise registry events`

---

### Task 3: Description cleaning and embedders

**Files:**
- Create: `app/text.py`, `app/embedding.py`, `tests/test_text.py`, `tests/test_embedding.py`; add `embedder` fixture to `tests/conftest.py`

**Interfaces:**
- Produces:
```python
# text.py
EmbeddingSource = Literal["nature", "fallback"]
def clean_description(text: str) -> str
def embedding_text(nature: str | None, industry: str | None) -> tuple[str, EmbeddingSource] | None
#   nature present -> (clean_description(nature), "nature"); else industry present -> (f"{industry} business", "fallback"); else None

# embedding.py
class Embedder(Protocol):
    model_name: str
    def embed(self, texts: list[str]) -> list[np.ndarray]   # each shape (384,), float32, L2-normalised
def text_hash(text: str) -> str                              # sha256 hex of utf-8
class FakeEmbedder:  model_name = "fake-hashed-bow"          # hashed bag of lowercased words, 384 buckets, normalised
class FastEmbedder:  def __init__(self, model_name: str)      # wraps fastembed.TextEmbedding; asserts dim == 384
def make_embedder(model_name: str) -> Embedder               # "fake-hashed-bow" -> FakeEmbedder else FastEmbedder
```

- [ ] **Step 1: Write failing tests**

```python
def test_collapses_repeated_phrase():
    assert clean_description("Perunding pengurusan untuk PKS " * 40) == "Perunding pengurusan untuk PKS"
def test_leaves_normal_text(): assert clean_description("Operating cafes and coffee shops") == "Operating cafes and coffee shops"
def test_does_not_collapse_partial_repeat(): assert clean_description("a b a") == "a b a"
def test_collapses_whitespace(): assert clean_description("  a \n b  ") == "a b"
def test_embedding_text_prefers_nature(): assert embedding_text("Cafes", "Food & Beverage") == ("Cafes", "nature")
def test_embedding_text_fallback(): assert embedding_text(None, "Logistics") == ("Logistics business", "fallback")
def test_embedding_text_none(): assert embedding_text(None, None) is None

def test_fake_embedder_deterministic_and_normalised():
    a, b = FakeEmbedder().embed(["coffee shop", "coffee shop"])
    assert a.shape == (384,) and np.allclose(a, b) and np.isclose(np.linalg.norm(a), 1.0)
def test_fake_embedder_similar_texts_closer():
    q, near, far = FakeEmbedder().embed(["coffee shop", "coffee cafe", "steel fabrication"])
    assert q @ near > q @ far
def test_fake_embedder_empty_text_is_finite(): ...  # no NaN for ""
```

- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. Repeat collapse = find smallest period `p` (in tokens) such that `len(tokens) % p == 0`, `len(tokens)//p >= 2`, and `tokens == tokens[:p] * (len//p)`; FakeEmbedder buckets with `int(sha256(word).hexdigest(), 16) % 384` (Python `hash()` is salted — don't use it); empty text → unit vector on bucket 0. `FastEmbedder` is not unit-tested (would download a model); it's exercised in Task 9.
- [ ] **Step 4:** Run → PASS. **Step 5: Commit** `feat: description cleaning and embedders`

---

### Task 4: Store and ingest — dedupe, reject, per-field LWW, embedding refresh

**Files:**
- Create: `app/store.py`, `app/ingest.py`, `tests/test_store.py`

**Interfaces:**
- Consumes: `CompanyEvent`, `InvalidEvent`, `parse_event` (Task 2); `Embedder`, `text_hash`, `embedding_text` (Task 3); `pool`, `clean_db`, `embedder` fixtures.
- Produces:
```python
# store.py  (every function takes an open connection inside the caller's transaction)
async def claim_message(conn, message_id: str) -> bool                 # INSERT processed_messages ON CONFLICT DO NOTHING; True if new
async def record_rejection(conn, message_id: str, body: bytes, reason: str) -> None
async def apply_event(conn, event: CompanyEvent, message_id: str) -> None   # company_events insert + LWW upsert
async def refresh_embedding(conn, reg_no: str, embedder: Embedder) -> None
@dataclass(frozen=True)
class CompanyRow: reg_no: str; name: str | None; industry: str | None; state: str | None; active: bool; has_embedding: bool
async def get_company(conn, reg_no: str) -> CompanyRow | None

# ingest.py
Outcome = Literal["applied", "duplicate", "rejected"]
async def handle_message(pool, embedder: Embedder, body: bytes, message_id: str | None) -> Outcome
#   message_id None/"" -> sha256(body). One transaction: claim -> (dup? return) -> parse -> reject or apply+refresh.
#   Any non-InvalidEvent exception propagates (transaction rolls back; caller requeues).
```

**LWW SQL (decided):** single `INSERT INTO companies (...) VALUES (...) ON CONFLICT (reg_no) DO UPDATE SET` where, for each field `f`, with incoming key `k = (COALESCE(t,'-infinity'), message_id)` and stored key `s = (COALESCE(companies.f_updated_at,'-infinity'), COALESCE(companies.f_message_id,''))`:
```sql
f            = CASE WHEN EXCLUDED.f IS NOT NULL AND (companies.f IS NULL OR k > s) THEN EXCLUDED.f ELSE companies.f END,
f_updated_at = CASE WHEN <same condition> THEN EXCLUDED.f_updated_at ELSE companies.f_updated_at END,
f_message_id = CASE WHEN <same condition> THEN EXCLUDED.f_message_id ELSE companies.f_message_id END
```
`last_upsert_at = GREATEST(companies.last_upsert_at, EXCLUDED.last_upsert_at)` (upserts only; PG `GREATEST` ignores NULL), `struck_off_at = GREATEST(...)` (strike-offs only; missing time → `'-infinity'`). Build the per-field SQL from a Python list of field names to avoid four hand-copied blocks.

**`refresh_embedding`:** read `nature, industry, embedding_text_hash, embedding_model`; `embedding_text(...)`; if `None` → set embedding columns NULL; if `(hash, model)` unchanged → no-op; else look up `embedding_cache(model, hash)`, else `await asyncio.to_thread(embedder.embed, [text])` and insert into cache (`ON CONFLICT DO NOTHING`); update row's `embedding, embedding_source, embedding_model, embedding_text_hash`.

- [ ] **Step 1: Write failing tests** (`tests/test_store.py`, all use `clean_db`). Helper `msg(reg, at, **data)` builds JSON bytes; `mid` is explicit per call.

```python
async def test_first_upsert_creates_company(): name/industry/state stored, active True, has_embedding True
async def test_duplicate_message_id_is_noop(): second handle_message(...same id) == "duplicate"; company_events count == 1
async def test_invalid_message_recorded_and_acked(): handle_message(b"null", ...) == "rejected"; rejected_messages reason "not_an_object"
async def test_rejected_duplicate_is_duplicate(): same bad body twice -> "rejected", then "duplicate"; one rejected row
async def test_older_event_does_not_overwrite(): 10:00 nature "B" then 09:00 nature "A" -> nature "B"
async def test_older_full_event_fills_fields_newer_partial_left_empty():
    # 10:00 {companyName:"New"} then 09:00 {companyName:"Old", natureOfBusiness:"Cafes", industry:"Food & Beverage"}
    # -> name "New", nature "Cafes", industry "Food & Beverage"
async def test_missing_timestamp_never_overwrites(): 09:00 name "Real" then updatedAt missing name "Other" -> "Real"
async def test_missing_timestamp_fills_empty_field(): missing-time {industry:"Retail"} on fresh company -> industry "Retail"
async def test_tie_broken_by_message_id(): same 09:00, ids "a"/"b", names "A"/"B", both arrival orders -> "B"
async def test_strike_off_hides_company(): upsert 09:00, struck 10:00 -> active False
async def test_stale_upsert_does_not_resurrect(): upsert 09:00, struck 10:00, upsert 09:30 -> active False
async def test_newer_upsert_revives(): upsert 09:00, struck 10:00, upsert 11:00 -> active True
async def test_strike_off_before_any_upsert(): struck 10:00 then upsert 09:00 -> row exists, active False
async def test_fallback_embedding_when_no_description(): industry only -> has_embedding, embedding_source "fallback"
async def test_real_description_replaces_fallback(): then nature arrives (older timestamp) -> source "nature"
async def test_no_embedding_without_description_or_industry(): name only -> has_embedding False
async def test_embedding_follows_final_description():   # Review Focus 3
    # nature A @09, B @10, A @11 -> embedding equals embedder.embed([A])[0]; embedding_cache has 2 rows
async def test_embedding_cache_shared(): two companies same nature -> embedding_cache count 1
async def test_concurrent_duplicate_applies_once():      # Review Focus 1
    outcomes = await asyncio.gather(*(handle_message(pool, embedder, body, "same") for _ in range(5)))
    assert sorted(outcomes) == ["applied"] + ["duplicate"] * 4
```

- [ ] **Step 2:** `uv run pytest tests/test_store.py -v` → FAIL.
- [ ] **Step 3:** Implement `store.py` and `ingest.py` per the interfaces and SQL above. `handle_message` uses `async with pool.connection() as conn, conn.transaction():`.
- [ ] **Step 4:** Run → PASS. Run whole suite `uv run pytest` → PASS.
- [ ] **Step 5: Commit** `feat: idempotent per-field last-writer-wins ingest with derived embeddings`

---

### Task 5: Full-feed replay and order independence

**Files:**
- Create: `tests/test_feed_replay.py`

**Interfaces:**
- Consumes: `handle_message` (Task 4). Message id for each line = `sha256(line.encode()).hexdigest()` (same as `producer/publish.py`).
- Produces: helper `async def replay(pool, embedder, lines: list[str]) -> Counter[Outcome]` and `async def snapshot(conn) -> list[tuple]` (all `companies` columns except `embedding` cast to text, ordered by `reg_no`) — reused by Task 10.

- [ ] **Step 1: Write the tests** (values measured from the feed on 2026-11-20):

```python
async def test_full_feed_counts(pool, embedder, clean_db):
    outcomes = await replay(pool, embedder, FEED_LINES)
    assert outcomes == Counter(applied=2433, duplicate=68, rejected=17)
    # rejected reasons: missing_registration_number 7, invalid_json 5, invalid_data 4, not_an_object 1
    # companies: 2000 total, 8 inactive, 4 without embedding, 35 with embedding_source 'fallback'
    #   (39 without a description, 4 of which also lack industry)
    # 0 companies with name NULL

async def test_any_order_same_state(pool, embedder, clean_db):
    await replay(pool, embedder, FEED_LINES); baseline = await snapshot(...)
    for seed in (1, 2, 3):
        truncate; shuffled = FEED_LINES[:]; random.Random(seed).shuffle(shuffled)
        await replay(pool, embedder, shuffled + shuffled[:500])  # reorder + redeliver
        assert await snapshot(...) == baseline

async def test_replay_twice_is_noop(...): second full replay -> Counter(duplicate=2518); snapshot unchanged
```

- [ ] **Step 2:** Run `uv run pytest tests/test_feed_replay.py -v`. Expected: PASS if Task 4 is right. If a count differs, investigate the specific messages (don't edit the expected numbers to match) — a mismatch means a parsing rule diverges from spec §6.2. `snapshot` includes the `*_message_id` columns: tie-breaks make them deterministic too.
- [ ] **Step 3: Commit** `test: full-feed replay is order- and duplicate-independent`

---

### Task 6: Similarity query and HTTP API

**Files:**
- Modify: `app/store.py`
- Create: `app/api.py`, `tests/test_api.py`

**Interfaces:**
- Consumes: `get_company`, `CompanyRow`, `normalise_reg_no`, `InvalidEvent`, `make_pool`, `Settings`.
- Produces:
```python
# store.py
@dataclass(frozen=True)
class SimilarRow: reg_no: str; name: str | None; industry: str | None; state: str | None; score: float
async def similar_companies(conn, reg_no: str, limit: int) -> list[SimilarRow]
# api.py
def create_app(settings: Settings | None = None) -> FastAPI   # lifespan opens make_pool(settings.database_url), stores on app.state.pool
app = create_app()
```
Routes (camelCase via pydantic `alias_generator=to_camel`, `populate_by_name=True`, serialise by alias):
- `GET /companies/{reg_no}` → `CompanyCard {registrationNumber, name, industry, state, status: "active"|"struck_off"}`; 404 `{"error":"company_not_found"}`; 422 `{"error":"invalid_registration_number"}`.
- `GET /companies/{reg_no}/similar?limit=4` (`1 ≤ limit ≤ 20`, else 422) → `{"company": CompanyCard, "similar": [CompanyCard + score], "reason": null | "struck_off" | "no_description", "model": settings.embed_model}`.
- `GET /healthz` → `{"status":"ok"}`; `GET /readyz` → 200 `{"status":"ready"}` if `select 1` works and `schema_migrations` has `001_init.sql`, else 503.
- `psycopg.OperationalError` / `psycopg_pool.PoolTimeout` → 503 `{"error":"database_unavailable"}`.

**Query (decided — exact scan, deterministic tie-breaks):**
```sql
SELECT c.reg_no, c.name, c.industry, c.state, 1 - (c.embedding <=> q.embedding) AS score
FROM companies c, (SELECT embedding, industry, state FROM companies WHERE reg_no = %(reg)s) q
WHERE c.active AND c.embedding IS NOT NULL AND c.reg_no <> %(reg)s
ORDER BY c.embedding <=> q.embedding,
         c.industry IS DISTINCT FROM q.industry, c.state IS DISTINCT FROM q.state, c.reg_no
LIMIT %(limit)s
```

- [ ] **Step 1: Write failing tests** with `httpx.AsyncClient(transport=ASGITransport(app))`, app built by `create_app(Settings(... database_url=test url, embed_model="fake-hashed-bow" ...))`, data seeded via `handle_message`.

```python
async def test_similar_most_similar_first(): seed cafe/coffee/steel companies -> similar[0] is the coffee one; scores non-increasing
async def test_self_excluded(): reg not in returned list
async def test_struck_off_excluded_from_results(): struck company never appears
async def test_limit_respected_and_validated(): limit=2 -> 2 items; limit=0 / 21 -> 422
async def test_ties_are_deterministic(): 3 companies with identical nature, one same industry+state -> it comes first; rest by reg_no
async def test_reg_no_normalised_in_path(): "/companies/syn-000001/similar" -> 200
async def test_unknown_company_404(); async def test_invalid_reg_no_422(): "/companies/hello"
async def test_struck_off_card_with_messy_reg_no():      # Review Focus 2
    r = await client.get("/companies/%20syn-001748%20"); r.json()["status"] == "struck_off"
    s = await client.get(".../similar") -> 200, similar == [], reason == "struck_off"
async def test_no_embedding_returns_empty_with_reason(): # Review Focus 5
async def test_db_down_returns_503(): monkeypatch store.similar_companies to raise psycopg.OperationalError -> 503 JSON
async def test_readyz(): 200
```

- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Run → PASS; full suite PASS.
- [ ] **Step 5:** Manual smoke: `just up && just migrate`, then (after Task 7) `just consume`/`just publish`/`just serve`, `curl localhost:8000/companies/SYN-001550/similar` returns 4 cards. Before Task 7 exists, skip.
- [ ] **Step 6: Commit** `feat: similar-companies API`

---

### Task 7: RabbitMQ consumer

**Files:**
- Create: `app/consumer.py`, `tests/test_consumer.py`

**Interfaces:**
- Consumes: `handle_message`, `make_pool`, `make_embedder`, `Settings`, `migrate`.
- Produces:
```python
QUEUE = "company-events"
async def on_message(message: AbstractIncomingMessage, pool, embedder) -> Outcome | None
#   handle_message(pool, embedder, message.body, message.message_id)
#   success -> await message.ack(); return outcome
#   any exception -> log with message_id, await message.nack(requeue=True), await asyncio.sleep(1.0) backoff; return None
async def run(settings: Settings, stop: asyncio.Event) -> None
#   make_pool; migrate; embedder = make_embedder(settings.embed_model) (warm it with one embed call);
#   connect_robust; channel.set_qos(prefetch_count=settings.prefetch);
#   queue = declare_queue(QUEUE, durable=True)  # no arguments, ever
#   consume with on_message; await stop.wait(); cancel consumer; wait for in-flight; close
def main() -> None   # SIGINT/SIGTERM set stop; log counts of outcomes every 500 messages and at shutdown
```

- [ ] **Step 1: Write failing tests** with a `FakeMessage` (attrs `body`, `message_id`; async `ack()`/`nack(requeue)` that record calls):

```python
async def test_valid_message_acked(): outcome "applied"; acked
async def test_poison_message_acked(): b"null" -> "rejected"; acked, not nacked
async def test_missing_message_id_uses_body_hash(): message_id None -> processed_messages has sha256(body)
async def test_transient_error_requeues(monkeypatch):     # Review Focus 4
    # patch app.consumer.handle_message to raise psycopg.OperationalError
    # -> nack(requeue=True) called, ack not called, rejected_messages empty, returns None (patch asyncio.sleep to no-op)
```

- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Run → PASS.
- [ ] **Step 5: Manual end-to-end:** `just down && just up && just consume` (separate terminal) → `just publish` → consumer logs `applied=2433 duplicate=68 rejected=17`; `just serve`; `curl -s localhost:8000/companies/SYN-001550/similar | python -m json.tool`. Then `just publish` again → all `duplicate`. Record what you saw in the commit message body.
- [ ] **Step 6: Commit** `feat: RabbitMQ consumer with ack-after-commit`

---

### Task 8: README section and DESIGN.md (first full version)

**Files:**
- Modify: `README.md` (append "## Running the solution" at top, under the title, linking `DESIGN.md`)
- Create: `DESIGN.md`

- [ ] **Step 1: README:** prerequisites, `just setup`, `just up`, `just consume`, `just publish`, `just serve`, example curl + example response, `just test` (needs `just up`), env vars table (`DATABASE_URL`, `RABBITMQ_URL`, `EMBED_MODEL`, `PREFETCH`, `API_PORT`).
- [ ] **Step 2: DESIGN.md** sections: Overview diagram; Decisions (approach A vs B vs C, from spec §4; queue declaration constraint; ack policy; embedding derived from row; exact scan); Assumptions — one bullet per row of spec §3 with the count; API contract and error table; Testing (what each test file proves, the order-independence property); 10x (spec §10 list, plus: embedding inside the transaction holds a row lock — move to outbox worker; batch embeds; HNSW once exact scan > budget; partition by reg_no hash across consumers with single-active-consumer per partition is *not* needed because LWW is order-free); What's left; AI tools used (Claude Code: data profiling, design/plan drafting, code — candidate fills in specifics honestly). Leave a `## Recommendation quality` heading with "see Task 9" placeholder text to be replaced.
- [ ] **Step 3:** Follow your own README from a clean `just down && just up` — every command works.
- [ ] **Step 4: Commit** `docs: README run instructions and DESIGN.md`

---

### Task 9: Embedding-model evaluation (deep-dive hour)

**Files:**
- Create: `eval/__init__.py`, `eval/evaluate.py`, `eval/pairs.csv`, `tests/test_eval_metrics.py`
- Modify: `DESIGN.md` (fill "Recommendation quality"), possibly `app/config.py` default model, `Justfile` (`reembed`)

**Interfaces:**
- Consumes: `make_embedder`, `clean_description`, `embedding_text`; data from the `companies` table of a consumed DB (`DATABASE_URL`).
- Produces:
```python
def industry_precision_at_k(vectors: np.ndarray, industries: list[str | None], k: int) -> float
#   mean over rows with industry not None of (# of top-k cosine neighbours, excluding self, sharing its industry) / k
def pair_recall_at_k(query_vecs: np.ndarray, corpus_vecs: np.ndarray, target_idx: list[int], k: int) -> float
def mean_reciprocal_rank(query_vecs, corpus_vecs, target_idx) -> float
```

- [ ] **Step 1: Failing tests** on tiny hand-built arrays: perfect clustering → precision 1.0; known ranks → recall/MRR exact values (e.g. ranks 1 and 3 → MRR 0.6667, recall@1 0.5).
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement metrics. **Step 4:** Run → PASS.
- [ ] **Step 5: Build `eval/pairs.csv`** (`malay,english` columns): list distinct descriptions (`select distinct nature from companies`), hand-pick 20–30 pairs describing the same business (e.g. "Mengusahakan kedai kopi dan kafe di …" ↔ "Operating cafes and coffee shops in …"). Note in DESIGN.md that these were labelled by the author and are small.
- [ ] **Step 6: `eval/evaluate.py`**: for models `fake-hashed-bow`, `BAAI/bge-small-en-v1.5`, `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`: embed active companies' `embedding_text`, report industry P@4 over all companies **and** over companies whose top-4 excludes identical-text neighbours (identical text trivially shares industry — report both, explain); pair recall@1/@5 and MRR in both directions against the corpus of distinct descriptions; embed time per 100 texts. Print a markdown table; also print top-4 for SYN-001550 per model.
- [ ] **Step 7:** `just eval` → paste table into DESIGN.md with a short interpretation and the chosen default. If the default changes: update `Settings` default, add `reembed` command (`python -m app.reembed`: loop `refresh_embedding` over all reg_nos — it already re-embeds when `embedding_model` differs) + Justfile recipe, add a test that changing model re-embeds (`FakeEmbedder` subclass with a different `model_name`).
- [ ] **Step 8: Commit** `feat: embedding model evaluation` (+ `feat: switch default model` if changed)

---

### Task 10: Consumer end-to-end test against real RabbitMQ

**Files:**
- Create: `tests/test_consumer_rabbitmq.py`

**Interfaces:**
- Consumes: `run`, `QUEUE` (Task 7); `producer.publish.publish(lines, url)`; `replay`/`snapshot` (Task 5).

- [ ] **Step 1: Write tests** — the queue name is fixed (registry-owned), so purge `company-events` at start (`channel.declare_queue(QUEUE, durable=True)` then `.purge()`), mark the module `pytest.mark.rabbitmq` and skip if the broker is unreachable.

```python
async def test_feed_through_rabbitmq_matches_direct_replay():
    # publish FEED_LINES; start run(settings, stop) as a task; poll until queue message_count == 0 and
    # processed_messages count == 2450 (unique lines); stop.set(); await task
    # assert snapshot == snapshot from a direct replay into a second clean DB (or recompute: same counts as Task 5)
async def test_republish_is_all_duplicates(): publish again -> processed_messages count unchanged, companies snapshot unchanged
```

- [ ] **Step 2:** Run `uv run pytest tests/test_consumer_rabbitmq.py -v` → PASS (no new production code expected; if it fails, fix `consumer.py`).
- [ ] **Step 3: Commit** `test: end-to-end through RabbitMQ`

---

### Task 11: Latency measurement and index decision

**Files:**
- Create: `eval/bench.py`
- Modify: `DESIGN.md` (latency section, index decision), possibly `migrations/002_hnsw.sql` and `store.similar_companies`

- [ ] **Step 1:** `eval/bench.py`: against a running `just serve` with the consumed feed, call `/companies/{reg}/similar` for 1,000 random active reg_nos with `httpx.AsyncClient` at concurrency 10; print p50/p95/p99 and `EXPLAIN ANALYZE` of the query for one reg_no.
- [ ] **Step 2:** If p95 < 50 ms with exact scan → keep exact scan; record numbers and the rationale (exact = correct + deterministic tie-breaks; HNSW ordering only uses distance, would need `hnsw.iterative_scan = relaxed_order` + over-fetch + re-sort in Python). Also synthesise 10x/100x rows (duplicate companies with jittered vectors into a scratch DB) and report where exact scan crosses 50 ms — that's the 10x evidence.
- [ ] **Step 3:** If HNSW is adopted: `migrations/002_hnsw.sql` (`CREATE INDEX ... USING hnsw (embedding vector_cosine_ops)`), query over-fetches `limit*4` by distance only with `SET LOCAL hnsw.iterative_scan = relaxed_order`, then re-sorts with the tie-break keys in Python; Task 6 tests must still pass.
- [ ] **Step 4: Commit** `perf: latency benchmark and index decision`

---

## Done criteria
- `just test` green with `just up` running.
- Fresh clone: README commands produce a working `/companies/SYN-001550/similar`.
- `DESIGN.md` covers decisions, every assumption with counts, 10x, what's left, AI tools, evaluation results.
