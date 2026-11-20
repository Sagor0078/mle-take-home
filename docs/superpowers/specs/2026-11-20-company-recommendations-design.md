# Similar Companies Service — Design

Date: 2026-11-20 · Status: draft for approval

## 1. Intent

The OneCredit MLE take-home (`README.md`, version `mle-v2026.11`) asks for:

1. A **consumer** that reads the RabbitMQ `company-events` queue and stores each company in Postgres with a pgvector
   embedding of its `natureOfBusiness`.
2. A **service** the portal calls: given a company, return its most similar companies (most similar first) with what
   a card needs (name, industry, state, registration number). Similarity search runs in Postgres with pgvector.
3. A **`DESIGN.md`** (decisions, assumptions, 10x plan, what's left, AI-tool use).
4. An optional deep-dive hour — chosen: **recommendation quality / embedding-model evaluation**.

The candidate is strong in ML/embeddings and backend and wants a rigorous solution. The graders look at: does it work
and keep working, testing, reasoning in `DESIGN.md`, readability. The candidate must be able to walk through all code
in an interview, so the code should be plain and explicit over clever.

Time box: 4–6 hours total, hard stop at 6. Anything not done goes in "what's left".

## 2. Goals, non-goals, anti-goals

**Goals**
- Final state reflects the *latest information per company* regardless of delivery order, duplicates or redelivery.
- No lost companies: every valid company in the feed is stored, visible on its own endpoint, and eligible to appear
  in other companies' recommendations.
- Fast enough to render during a page load (target p95 < 50 ms on the service for the 2k dataset on a laptop).
- Every feed trap (below) is handled deliberately, tested, and documented.

**Non-goals**
- Authentication, multi-tenancy, a UI, deployment beyond docker-compose.
- Ranking signals beyond nature of business (plus deterministic tie-breaks). README says that's enough for v1.
- Paid APIs or GPU models.

**Anti-goals (failure even if it "works")**
- An older event overwriting newer data, or a stale upsert resurrecting a struck-off company.
- A poison message blocking the queue or crash-looping the consumer.
- Silently dropping a message: everything rejected is recorded with a reason.
- Modifying the registry team's queue declaration (they own it: durable, no arguments).

## 3. The feed (measured, `data/feed.jsonl`, 2,518 lines)

| Trap | Count | Handling (section 6) |
|---|---|---|
| Exact duplicate lines (same `message_id`) | 68 | dedupe on `message_id` |
| Unparseable JSON (truncated) | 5 | reject → `rejected_messages` |
| Body is JSON `null` | 6 | reject |
| `data` is a string (`"companyName=Foo; state=Perak"`) | 4 | reject (test junk; not parsed) |
| Missing `registrationNumber` | 7 | reject (cannot key it) |
| Reg no. lowercase / padded whitespace | many | `strip().upper()` |
| Reg no. as integer (`119`) | 3 | → `SYN-000119` (matches existing companies) |
| `updatedAt` ISO with offset | 2,428 | parse |
| `updatedAt` ISO `Z` | 28 | parse as UTC |
| `updatedAt` naive ISO | 14 | assume `+08:00` (Malaysia time; all other events are +08:00) |
| `updatedAt` epoch milliseconds (int) | 30 | parse as ms UTC |
| `updatedAt` missing | 7 | all are `{"companyName": "Unknown"}` → treat as lowest priority (see 6.3) |
| Out-of-order events for a company | 66 companies | per-field last-writer-wins on `updatedAt` |
| Partial `data` (only `companyName`) | 14 | absent/null fields = "no information", never clear |
| `companyName: "Unknown"` placeholder | 7 | treated as absent |
| Extra keys `source` / `ingestedBy` | ~75 | ignored (kept in raw event) |
| `company-struck-off` events | 8 | tombstone with timestamp |
| State in mixed case (`KUALA LUMPUR`, `selangor`) | many | map to canonical 16 states/FTs |
| `natureOfBusiness` null/empty | 58 | absent (see 6.5 fallback) |
| Description = one phrase repeated dozens of times | several | collapse repeats before embedding |
| Malay descriptions | ~600 | drives the model evaluation |
| Only 178 distinct descriptions for 2,000 companies | — | embedding cache; many exact ties |
| Companies whose *only* event has no description | 35 | fallback embedding text |

These numbers go into `DESIGN.md` and the tests should assert against the same fixtures.

## 4. Approaches considered

- **A. Per-field last-writer-wins + event log (chosen).** Each valid event is stored once in `company_events`; the
  `companies` row keeps a value *and* a timestamp per field; a field only changes if the incoming event is newer.
  Strike-offs are tombstones with their own timestamp. Correct for every out-of-order / partial / resurrection case
  in the feed; the event log gives replay and audit. Cost: more schema and SQL.
- **B. Row-level last-writer-wins** (`ON CONFLICT ... WHERE excluded.updated_at > companies.updated_at`). Simple,
  but a newer partial event (name only) advances the row timestamp and causes an older *full* event to be discarded,
  losing the description. Rejected; described in `DESIGN.md` as the simpler alternative.
- **C. A + async embedding worker (outbox).** Consumer writes state; a separate worker embeds. Right at 10x+, overkill
  for 2k. Goes in the 10x section.

The candidate chose **A**.

## 5. Architecture

```
producer ──► RabbitMQ company-events ──► consumer ──► Postgres (+pgvector) ◄── FastAPI service ◄── portal
                                            │
                                            └─► fastembed (CPU, in-process, cached by text hash)
```

**Stack (decided):** Python 3.14 (uv), `aio-pika` (consumer; already a dependency), `psycopg` 3 async + `psycopg_pool`,
`pgvector` Python adapter, `fastembed`, `FastAPI` + `uvicorn`, `pydantic` for event and response models, `pytest`
+ `pytest-asyncio` + `testcontainers` (or the compose services) for integration tests. Plain SQL migrations applied
at startup by a small `migrate` step (no ORM). `just` recipes for everything.

Suggested layout (builder may adjust):

```
app/
  config.py        # env settings (DB url, AMQP url, model name, prefetch)
  events.py        # parse + normalise raw bytes -> CompanyEvent | Rejection   (pure, no I/O)
  text.py          # description cleaning (collapse repeats, whitespace, truncation)
  embedding.py     # Embedder protocol, fastembed impl, hashed-BoW fake for tests
  store.py         # SQL: record event, apply event (LWW upsert), similarity query
  consumer.py      # aio-pika loop, ack/nack policy
  api.py           # FastAPI app
migrations/001_init.sql
eval/              # model evaluation script + labelled pairs
tests/
```

## 6. Consumer

### 6.1 Queue and acknowledgement
- Declare `company-events` with `durable=True` and **no arguments** — anything else (e.g. a dead-letter exchange)
  fails with `PRECONDITION_FAILED` because the registry team owns it. Hence rejections go to our own table, not a DLQ.
- Manual ack, `prefetch_count` configurable (default 32). Ack **only after the DB transaction commits**.
- Outcomes per message:
  - *Valid* → apply in one transaction → ack.
  - *Duplicate* (`message_id` already seen) → ack, no-op.
  - *Permanently invalid* (parse/validation failure) → insert into `rejected_messages` (raw body, `message_id`,
    reason, received_at) → ack. Never requeue a poison message.
  - *Transient failure* (DB down, connection lost) → nack with requeue; `connect_robust` reconnects; consumer exits
    non-zero only on repeated startup failure.
- Missing AMQP `message_id`: fall back to sha256 of the body (same thing the producer does).
- Graceful shutdown on SIGTERM: stop consuming, finish in-flight messages, close.
- Ordering is *not* relied upon: correctness comes from 6.3, so concurrent processing within the prefetch window is
  safe. The embedding call (CPU-bound) runs in a thread executor so it doesn't block the event loop.

### 6.2 Parsing and normalisation (`events.py`, pure functions, heavily unit-tested)
- Decode UTF-8; `json.loads(..., strict=False)` (tolerate raw control chars inside strings).
- Body must be an object with `type` in {`company-upserted`, `company-struck-off`}; unknown types → reject.
- Registration number: str → `strip().upper()`; int → `f"SYN-{n:06d}"`; must match `^SYN-\d{6}$` after normalising.
- `updatedAt`: ISO with offset; `Z` → UTC; naive → `+08:00`; int/float or all-digit string → epoch ms (values
  < 10^11 treated as seconds); missing → `None` (see 6.3). Unparseable → reject.
- `data` for upserts: must be an object (string → reject). Fields `companyName`, `natureOfBusiness`, `industry`,
  `state`: strings stripped; empty string, `null`, missing, and the placeholder `"Unknown"` (for name) all mean
  **absent**. An upsert with no usable fields at all is recorded as an event but changes nothing.
- `state`: case-insensitive map to the canonical list (Johor, Kedah, Kelantan, Melaka, Negeri Sembilan, Pahang,
  Perak, Perlis, Pulau Pinang, Sabah, Sarawak, Selangor, Terengganu, Kuala Lumpur, Labuan, Putrajaya). Unknown
  value → keep as given (title-cased) and log; do not reject the event for it.
- `industry`: kept as given (all observed values are clean).

### 6.3 Applying an event (per-field last-writer-wins)
Each field `f ∈ {name, nature, industry, state}` has `f` and `f_updated_at`. Incoming event time `t`
(missing `updatedAt` → `t = '-infinity'`, i.e. "only fills fields nobody has set").

- Upsert: for each field present in the event, set it iff `t > f_updated_at` (or the field is unset).
  Ties (`t = f_updated_at`): order-independent tie-break on the larger `message_id` — store `f_message_id` per field
  and compare `(t, message_id)` lexicographically. *[Designer's call]* — "keep existing" would make the result depend on
  arrival order and break the permutation guarantee. None observed in the feed, but the guarantee should be real.
- Strike-off: `struck_off_at = GREATEST(struck_off_at, t)`.
- Visibility: a company is **active** iff `struck_off_at IS NULL OR last_upsert_at > struck_off_at`
  (`last_upsert_at` = max upsert time seen). So a stale upsert can't resurrect it, but a genuinely newer upsert
  (re-registration) does.
- Done as a single `INSERT ... ON CONFLICT (reg_no) DO UPDATE SET f = CASE WHEN ... END, ...` statement — atomic and
  race-free under concurrent consumers without explicit locks. Same transaction also inserts into `company_events`
  and `processed_messages`.
- Embedding is updated under exactly the same condition as `nature` (see 6.5).

**Key property (tested):** final `companies` state is identical for any permutation and any duplication of the feed.

### 6.4 Schema (sketch; builder finalises types/indexes)

```sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE processed_messages (message_id text PRIMARY KEY, processed_at timestamptz NOT NULL DEFAULT now());

CREATE TABLE company_events (           -- every valid event, for audit/replay
  id bigserial PRIMARY KEY, message_id text UNIQUE NOT NULL, reg_no text NOT NULL, type text NOT NULL,
  event_at timestamptz, payload jsonb NOT NULL, received_at timestamptz NOT NULL DEFAULT now());

CREATE TABLE rejected_messages (
  message_id text PRIMARY KEY, body bytea NOT NULL, reason text NOT NULL, received_at timestamptz NOT NULL DEFAULT now());

CREATE TABLE embedding_cache (          -- keyed by model so a model swap doesn't mix vectors
  model text NOT NULL, text_hash text NOT NULL, embedding vector(384) NOT NULL, PRIMARY KEY (model, text_hash));

CREATE TABLE companies (
  reg_no text PRIMARY KEY,
  name text, name_updated_at timestamptz,
  nature text, nature_updated_at timestamptz,
  industry text, industry_updated_at timestamptz,
  state text, state_updated_at timestamptz,
  last_upsert_at timestamptz, struck_off_at timestamptz,
  embedding vector(384), embedding_source text,   -- 'nature' | 'fallback'
  embedding_model text);

CREATE INDEX ON companies USING hnsw (embedding vector_cosine_ops);
```
`processed_messages` and `company_events.message_id` overlap; builder may merge them (rejected messages also need
dedupe, so keep one place that answers "seen this message_id?").

### 6.5 Text and embeddings
- Clean before embedding: collapse whitespace; collapse a phrase repeated back-to-back into one occurrence; truncate
  to the model's token limit. Store the original text, embed the cleaned text.
- Cache by `(model, sha256(cleaned_text))`: 178 distinct descriptions → ~178 model calls for the whole feed.
- **Fallback for companies with no description (35):** embed `"{industry}"` (or `"{industry} company in {state}"` —
  builder picks; evaluate quickly) and mark `embedding_source='fallback'` so they still appear in others'
  recommendations. If neither description nor industry exists, `embedding` stays null and the company is served
  but cannot be a neighbour (counted and reported in `DESIGN.md`).
  When a real description later arrives, it replaces the fallback regardless of timestamps (fallback has no timestamp
  of its own; it's derived).
- Model: configurable via env; vector dimension fixed at 384. Default decided by the evaluation (section 9).
  Candidates: `BAAI/bge-small-en-v1.5` (README's suggestion, English-only) and a 384-dim multilingual model that
  fastembed supports (e.g. `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`; builder confirms against
  fastembed's supported list). Normalise vectors; cosine distance.
- Tests use a deterministic hashed bag-of-words `FakeEmbedder` (same interface) so they don't download models.

## 7. Service (FastAPI)

**`GET /companies/{reg_no}/similar?limit=4`** (limit 1–20, default 4 to match the mock)
- `reg_no` normalised the same way as the consumer (so `syn-001550` works); invalid format → **422**.
- Unknown company → **404** `{"error": "company_not_found"}`.
- Struck-off company → **200** with the card (`"status": "struck_off"`), `similar: []` and
  `"reason": "struck_off"`. Struck-off companies never appear in anyone's recommendations. *(Decided by candidate.)*
- Company with no embedding → **200** with `similar: []` and `"reason": "no_description"` (page still renders).
- Otherwise **200**:
  ```json
  {"company": {"registrationNumber": "SYN-001550", "name": "...", "industry": "...", "state": "..."},
   "similar": [{"registrationNumber": "...", "name": "...", "industry": "...", "state": "...", "score": 0.91}],
   "model": "..."}
  ```
- Query: active companies only, exclude self, `ORDER BY embedding <=> :q`, then deterministic tie-breaks
  (same industry first, same state first, `reg_no`). Ties matter: 178 distinct texts across 2k companies.
- HNSW + a `WHERE active` filter can return fewer than `limit` rows (post-filtering). Use pgvector 0.8's
  `SET LOCAL hnsw.iterative_scan = relaxed_order` and over-fetch then re-sort, or note that at 2k rows an exact scan
  is cheap — builder measures both and records the choice.
- DB/unexpected error → **503** with a JSON body, never an HTML stack trace.

**`GET /companies/{reg_no}`** — the card for the page itself: 200 for every stored company (including struck-off,
with `"status": "active" | "struck_off"`), 404 unknown, 422 invalid format. Cards in `similar` carry `status` too
(always `active`).

**`GET /healthz`** (process up) and **`GET /readyz`** (DB reachable, migrations applied).

## 8. Testing
- **Unit (pure):** one test per trap in section 3 for `events.py`; text cleaning; state mapping; the LWW decision.
- **Integration (real Postgres + pgvector, FakeEmbedder):**
  - Apply the full feed → expected company count (~2,000 minus none lost), rejected count and reasons match section 3.
  - **Order independence:** apply the feed in N random permutations, and twice over → identical `companies` snapshot.
  - Specific scenarios: older full event after newer partial keeps the description; stale upsert after strike-off
    doesn't resurrect; newer upsert after strike-off does; missing-`updatedAt` "Unknown" never overwrites a name.
- **Consumer (real RabbitMQ):** publish the feed, run consumer to quiescence, assert DB state and queue empty;
  poison message is acked and recorded; redelivery is a no-op.
- **API:** status codes and shapes for 200 / 404 / 422, struck-off card + empty list, no-description; self excluded; struck-off excluded;
  deterministic order.

## 9. Deep-dive hour: recommendation quality
- Build `eval/` with: (a) industry agreement — precision@4 of same-industry neighbours (proxy label, documented as
  imperfect); (b) ~20–30 hand-labelled Malay↔English pairs drawn from the 178 distinct descriptions that describe the
  same business, scored by whether the counterpart ranks in the top-k; (c) a few qualitative examples (the README
  mock company SYN-001550).
- Compare bge-small-en vs the multilingual model (and the hashed-BoW placeholder as a floor). Pick the default from
  results; table goes in `DESIGN.md`. Optional: latency micro-benchmark of the endpoint (p50/p95).

## 10. Deliverables
- Code as above, `README.md` section for the candidate's solution (`just migrate`, `just consume`, `just serve`,
  `just test`, `just eval`), `DESIGN.md` at repo root covering decisions, assumptions (every row of section 3),
  10x plan (outbox embedding worker, batching, partitioned consumers, HNSW tuning, read replica/cache), what's left,
  and AI-tool usage.

## 11. Priority if the 6-hour stop is hit (decided by candidate)
Build in this order; cut from the bottom:
1. Parsing/normalisation, LWW apply, order-independence tests.
2. Similar-companies endpoint + API tests.
3. `DESIGN.md` with an honest "what's left".
4. Model evaluation: industry precision@4 first, then Malay↔English pairs.
5. Consumer tests against real RabbitMQ.
6. HNSW vs exact-scan latency measurement.
The `company_events` audit log stays (cheap).

## 12. Left to the builder
- Exact module boundaries, file names, pydantic models, logging format.
- Whether to merge `processed_messages` into `company_events`.
- HNSW vs exact scan and iterative-scan settings (measure).
- Fallback embedding text wording.
- Prefetch value and executor size.
- testcontainers vs reusing the compose services in tests.
