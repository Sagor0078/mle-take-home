# DESIGN

A consumer turns the registry's `company-events` feed into one row per company in Postgres, with a pgvector embedding
of the company's nature of business. A FastAPI service answers "which companies are most like this one?" from that
table. How to run it is in the [README](README.md#running-the-solution).

The short version of the main decision: **every field of a company is its own last-writer-wins register, keyed on the
event's `updatedAt`**, and everything the feed throws at us (duplicates, redelivery, out-of-order, partial updates,
strike-offs, junk) is handled so that the final table does not depend on the order messages arrive in. That property
is tested directly: the full feed in three random orders, each with 500 redeliveries on top, gives a byte-identical
`companies` table, embeddings included.

## 1. Overview

```
data/feed.jsonl ─► producer ─► RabbitMQ: company-events ─► consumer ──────► Postgres 16 + pgvector 0.8
                                (durable, no args;          │  one            companies          (current state)
                                 registry team owns it)     │  transaction    company_events     (audit log)
                                                            │  per message    processed_messages (dedupe)
                                                            │                 rejected_messages  (poison, with reason)
                                                            │                 embedding_cache    (model, text hash)
                                                            └─► fastembed (CPU, in-process, in a worker thread)

portal ─► FastAPI  GET /companies/{reg}/similar ─► one SQL query: cosine distance over active companies
```

| Module | What it does |
|---|---|
| `app/events.py` | Pure parsing: bytes → `CompanyEvent` or `InvalidEvent(reason)`. No I/O. |
| `app/text.py` | Picks the text to embed and collapses descriptions that repeat one phrase end to end. |
| `app/embedding.py` | `Embedder` protocol, `FastEmbedder` (fastembed), `FakeEmbedder` (hashed bag of words, tests only). |
| `app/store.py` | All SQL: claim a message id, record a rejection, apply an event, refresh the embedding, similarity query. |
| `app/ingest.py` | `handle_message`: one message → one transaction → `applied` / `duplicate` / `rejected`. |
| `app/consumer.py` | aio-pika loop: prefetch, ack after commit, back-off and requeue on transient errors, graceful shutdown. |
| `app/api.py` | FastAPI app: cards, similar companies, health and readiness, JSON errors. |
| `app/migrate.py` | Applies `migrations/*.sql` once each, tracked in `schema_migrations`. |
| `app/reembed.py` | `just reembed`: re-embeds every company with the configured model after a model change. |
| `eval/` | `just eval`: the model comparison in section 6 (`metrics.py` pure functions, `evaluate.py`, `pairs.csv`). |

No ORM: the SQL is in `app/store.py` and `migrations/001_init.sql`, so what runs against the database is visible.

## 2. Decisions

### 2.1 Per-field last-writer-wins (chosen) vs row-level vs async worker

| | Approach | Verdict |
|---|---|---|
| **A** | Each field (`name`, `nature`, `industry`, `state`) stores its value, the `updatedAt` of the event that set it, and that event's `message_id`. An incoming field replaces the stored one only if it has a value and its `(updatedAt, message_id)` is larger. | **Chosen.** Correct for every case in the feed. |
| B | Row-level: `ON CONFLICT ... WHERE excluded.updated_at > companies.updated_at`. | Simpler, but wrong here. 14 events carry only `companyName`. If such a partial event is the newest, it advances the row timestamp and the older *full* event that arrives later is discarded, so the company loses its description and drops out of recommendations. |
| C | A, plus a separate outbox worker that computes embeddings. | Right at larger scale (see section 7), more moving parts than 2,000 companies need. |

The merge is a single `INSERT ... ON CONFLICT (reg_no) DO UPDATE SET f = CASE WHEN <incoming wins> ...` statement
(`UPSERT_SQL` in `app/store.py`). It is atomic, so concurrent messages for the same company need no explicit locks:
the second one waits on the row lock and then compares against the committed values.

"Absent" means `null`, `""`, a missing key, whitespace only, a non-string value, or `companyName == "Unknown"`. An
absent field never overwrites anything; it is "no information", not "clear this field".

### 2.2 Ties and missing timestamps

- **Ties** on `updatedAt` are broken by `message_id`, compared byte-wise (`COLLATE "C"`). "Keep what is there" would
  make the winner depend on arrival order, and comparing in the database's default collation would make it depend on
  the server locale. There are no exact ties in this feed, but the order-independence guarantee should not rely on
  that.
- **Missing `updatedAt`** (7 events, all `{"companyName": "Unknown"}`) counts as `-infinity`: such an event can fill a
  field nobody has set, but never overwrites one. With "Unknown" treated as absent, these 7 change nothing today.

### 2.3 Strike-offs

`struck_off_at = GREATEST(struck_off_at, t)` and `last_upsert_at = GREATEST(last_upsert_at, t)`. A generated column
decides visibility:

```sql
active boolean GENERATED ALWAYS AS (
  struck_off_at IS NULL OR COALESCE(last_upsert_at, '-infinity') > struck_off_at) STORED
```

So an upsert that is older than the strike-off cannot resurrect the company, whatever order they arrive in, while a
genuinely newer upsert (a re-registration) does. A strike-off for a company we have never seen creates the row, so a
later stale upsert stays hidden. Struck-off companies are kept and can still be looked up (the page renders with
`"status": "struck_off"`), but they never appear in anyone's recommendations and get none themselves. That behaviour
was the candidate's call; the brief does not say.

### 2.4 Exactly-once effect on an at-least-once queue

Every message, valid or not, is first claimed with `INSERT INTO processed_messages ... ON CONFLICT DO NOTHING` in the
same transaction that applies it. If the row already exists the message is a `duplicate` and nothing else happens. A
concurrent copy of the same message blocks on the primary key until the first transaction commits, then sees the
duplicate. If the first transaction rolls back, the claim disappears with it and the retry is processed normally.

A message without an AMQP `message_id` falls back to sha256 of the body, which is what the producer uses anyway.

Each valid event is also stored in `company_events` (payload as `jsonb`, with `event_at` and `received_at`) for audit
and replay. It is cheap and makes "why does this company look like this?" answerable from the database.

### 2.5 The queue and the ack policy

The registry team declares `company-events` as durable with no arguments, and RabbitMQ refuses a re-declaration with
different arguments (`PRECONDITION_FAILED`). So the consumer declares it identically and cannot attach a dead-letter
exchange; poison messages go to our own `rejected_messages` table instead.

| Outcome | What the consumer does |
|---|---|
| Valid event | apply in one transaction, commit, **then** ack |
| Duplicate `message_id` | ack (the transaction only checked the claim) |
| Permanently invalid | store body + reason in `rejected_messages`, commit, ack. Never requeued. |
| Any other exception (DB down, deadlock, ...) | transaction rolls back; sleep 1 s, **then** nack with requeue |

Sleeping before the nack is deliberate: the delivery keeps its prefetch slot while we wait, so during a database outage
the broker cannot hand the same message straight back and the consumer does not spin at full speed. The connection is
`connect_robust`, so it reconnects after a broker restart. On SIGINT/SIGTERM the consumer cancels its subscription,
waits for in-flight messages to finish and logs the totals. Correctness does not depend on ordering, so up to
`PREFETCH` (32) messages are processed concurrently.

### 2.6 A hostile body is a rejection, never a crash

If parsing raised anything other than `InvalidEvent`, the consumer would treat it as transient and requeue it forever.
So `events.py` turns every way a body can be bad into `InvalidEvent(reason)`: invalid UTF-8, truncated JSON, JSON
nested deep enough to hit Python's recursion limit, a 5,000-digit timestamp (`int()` refuses it), timestamps out of
`datetime`'s range (`10**400`, `1e308`), a 400-digit integer registration number. A parametrised test feeds these
bodies to `parse_event` and checks that nothing but `InvalidEvent` escapes. Reasons: `invalid_json`, `not_an_object`, `unknown_type`, `missing_registration_number`,
`invalid_registration_number`, `invalid_updated_at`, `invalid_data`.

### 2.7 Embeddings are derived from the stored row

After every applied event, `refresh_embedding` reads the company's *current* `nature` and `industry` and makes the
embedding match them. It is not computed from the incoming event. That keeps the embedding as order-independent as
the fields: whatever order events arrive in, the final description is the same, so the final vector is the same.

- **What is embedded:** the description, whitespace-collapsed, and with a phrase repeated end to end (some
  descriptions repeat one sentence dozens of times) reduced to one copy. The original text is stored unchanged.
- **Fallback:** a company with no description but an industry embeds `"{industry} business"` and is marked
  `embedding_source = 'fallback'`. That covers 35 companies that would otherwise be invisible to recommendations
  (the brief's "no lost companies"). When a description arrives it replaces the fallback, whatever its timestamp,
  because the fallback is derived and has no timestamp of its own. 4 companies have neither; they have
  `embedding = NULL`, their own page still works (`"reason": "no_description"`), and they cannot be a neighbour.
- **Skip check:** the row stores `embedding_text_hash`, `embedding_model` and `embedding_source`. If all three match,
  nothing is recomputed. Including the source matters: without it, the same text reached as a fallback and as a real
  description could leave a different `embedding_source` depending on order.
- **Cache:** `embedding_cache (model, sha256(text)) → vector`. The feed has only 178 distinct descriptions for 2,000
  companies, so the model runs a couple of hundred times for the whole feed, not 2,433. Keying on the model means a
  model change never mixes vectors from two models.
- **Model:** `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` through fastembed (CPU, 384 dimensions,
  ~220 MB), configurable with `EMBED_MODEL`. It replaced the brief's `BAAI/bge-small-en-v1.5` after the comparison in
  section 6. Vectors are L2-normalised; distance is cosine (`<=>`). The model call runs in a worker thread
  (`asyncio.to_thread`) so it does not block the event loop.
- **Tests** use `FakeEmbedder`, a deterministic hashed bag of words, so they never download a model.

### 2.8 Similarity search: exact scan

```sql
SELECT c.reg_no, c.name, c.industry, c.state, 1 - (c.embedding <=> q.embedding) AS score
FROM companies c, (SELECT embedding, industry, state FROM companies WHERE reg_no = %(reg)s) q
WHERE c.active AND c.embedding IS NOT NULL AND c.reg_no <> %(reg)s
ORDER BY c.embedding <=> q.embedding,
         c.industry IS DISTINCT FROM q.industry, c.state IS DISTINCT FROM q.state, c.reg_no
LIMIT %(limit)s
```

- **Exact, no HNSW index (yet).** At 2,000 rows a sequential scan is cheap, and it is exact, which matters because of
  the next point. An HNSW index with a `WHERE active` filter can also return fewer than `limit` rows (the filter is
  applied after the index scan). Whether and when to add HNSW is a measurement that is not done yet (section 8).
- **Deterministic ties.** 178 distinct texts across 2,000 companies means many identical scores. Ties go to the same
  industry, then the same state, then `reg_no`, so the panel never shuffles between page loads.
- **Score** is cosine similarity (1 = identical text).

For the README's mock company SYN-001550 ("Operating cafes and coffee shops in the Klang Valley"), the top four are
SYN-000539 (identical description, score 1.0), SYN-000263 and SYN-001650 (the Malay translation, 0.950 each), and
SYN-000344, a coffee shop in Kuantan (0.862). The first three are the mock's three food companies. The mock's fourth
is a Logistics company; with `bge-small-en-v1.5` we reproduced that (an exact tie at 0.830 between SYN-000700 and the
mock's SYN-000831, broken by "same state first"), and the multilingual model replaces it with another café
(section 6).

### 2.9 API errors are JSON with an `error` key

The portal should never get an HTML stack trace. Every non-200 response is JSON with an `error` code (table in
section 4). Validation errors are 422 (FastAPI's default status, with its body rewritten to the same shape), a
database that cannot be reached is 503. Registration numbers in the path are normalised exactly like the feed's, so
`syn-001550` works.

## 3. Assumptions

Counts are from `data/feed.jsonl` (2,518 lines, 2,450 distinct messages). Where the brief was silent, this is the call
that was made.

| Situation in the feed | Count | Assumption / handling |
|---|---|---|
| Exact duplicate lines (same `message_id`) | 68 | At-least-once delivery. Deduplicated on `message_id`; acked as no-ops. |
| Truncated, unparseable JSON | 5 | Reject (`invalid_json`). |
| Body is JSON `null` | 6 lines, 1 distinct | Reject (`not_an_object`); the other 5 are duplicates of it. |
| `data` is a string (`"companyName=Foo; state=Perak"`) | 4 | Reject (`invalid_data`). It looks like test junk; parsing an ad-hoc format would be guessing. |
| Missing `registrationNumber` | 7 | Reject (`missing_registration_number`): there is nothing to key the company on. |
| Reg. no. lowercase or padded with whitespace | 49 | `strip().upper()`. |
| Reg. no. as an integer (`119`) | 3 | `SYN-000119`; these match companies that also appear in string form. |
| `updatedAt` ISO with offset | 2,365 | Parsed as given. |
| `updatedAt` ISO with `Z` | 28 | UTC. |
| `updatedAt` naive ISO | 14 | Assumed `+08:00` (Malaysia time; every event with an explicit offset uses +08:00). |
| `updatedAt` epoch integer | 30 | Milliseconds (all are ≥ 10^11; smaller numbers would be read as seconds). |
| `updatedAt` missing | 7 | Lowest priority (`-infinity`): fills empty fields only. All 7 are `{"companyName": "Unknown"}`. |
| Events for a company arriving out of order | 66 companies | Per-field last-writer-wins on `updatedAt` (section 2.1). The source time decides, not arrival. |
| Partial `data` (only `companyName`) | 14 | Missing fields mean "no information", never "clear". |
| `companyName: "Unknown"` | 7 | A placeholder, treated as absent. |
| Extra keys (`source`, `ingestedBy`) | 72 | Ignored; kept in `company_events.payload`. |
| `company-struck-off` events | 8 | Tombstone with its own timestamp (section 2.3). All 8 companies end up struck off. |
| State in mixed case (`KUALA LUMPUR`, `selangor`) | 30 spellings | Case-insensitive map to the 16 canonical states and federal territories. An unknown state would be kept, title-cased, and logged; none occur. |
| `natureOfBusiness` null or empty | 58 | Absent. |
| Description repeating one phrase many times | several | Collapsed to one copy before embedding (stored text unchanged). |
| Distinct descriptions | 178 for 2,000 companies | Embedding cache; many exact score ties, broken deterministically. |
| Descriptions in Malay | roughly 600 events (73 of 180 distinct texts) | Embedded with a multilingual model, chosen by the comparison in section 6. |
| Companies whose only description is missing | 35 | Industry fallback embedding (section 2.7). |
| Companies with no description and no industry | 4 | No embedding; page renders, `"reason": "no_description"`. |

End result of consuming the whole feed: 2,518 messages → **2,433 applied, 68 duplicates, 17 rejected**
(`missing_registration_number` 7, `invalid_json` 5, `invalid_data` 4, `not_an_object` 1); **2,000 companies**, 8 struck
off, 35 with a fallback embedding, 4 without an embedding, none without a name. Publishing the feed a second time gives
2,518 duplicates and no change. These numbers are asserted in `tests/test_feed_replay.py`, and the same numbers came
out of a real run through RabbitMQ with the real model.

## 4. API

| Endpoint | Success | Notes |
|---|---|---|
| `GET /companies/{reg_no}/similar?limit=4` | 200 `{company, similar[], reason, model}` | `limit` 1–20, default 4 (the mock shows 4). `similar` is most similar first. |
| `GET /companies/{reg_no}` | 200 card | Every stored company, including struck-off ones. |
| `GET /healthz` | 200 `{"status": "ok"}` | Process is up. |
| `GET /readyz` | 200 `{"status": "ready"}` | Database reachable and `001_init.sql` applied; otherwise 503 `{"status": "not_ready"}`. |

A card is `{registrationNumber, name, industry, state, status}` with `status` `"active"` or `"struck_off"`; cards in
`similar` add `score`. JSON is camelCase, like the feed.

| Case | Status | Body |
|---|---|---|
| Normal | 200 | `similar` filled, `"reason": null` |
| Company is struck off | 200 | card with `"status": "struck_off"`, `"similar": []`, `"reason": "struck_off"` |
| Company has no embedding | 200 | `"similar": []`, `"reason": "no_description"` |
| Unknown registration number | 404 | `{"error": "company_not_found"}` |
| Malformed registration number | 422 | `{"error": "invalid_registration_number"}` |
| Bad query parameter (`limit=50`) | 422 | `{"error": "invalid_request", "detail": [...]}` |
| Database unreachable / pool timeout | 503 | `{"error": "database_unavailable"}` |

Struck-off and no-description return 200 with an empty list rather than an error, so the portal can always render
the page and simply hide the panel.

## 5. Testing

`just test` runs 142 tests in about two minutes. Integration tests use the docker-compose Postgres (with pgvector) and a
throwaway database per test session; the RabbitMQ end-to-end tests skip when the broker is unreachable. No test downloads a model.

| File | What it shows |
|---|---|
| `tests/test_events.py` | One test per feed trap in section 3: reg. no. forms, every timestamp form, truncated JSON, invalid UTF-8, raw control characters, non-object bodies, unknown/missing type, string `data`, "Unknown" names, blank fields, state mapping, extra keys. Hostile bodies (huge digit strings, deep nesting) raise only `InvalidEvent`. |
| `tests/test_text.py` | Repeated-phrase collapse (and no collapse of a partial repeat); description vs fallback vs nothing. |
| `tests/test_embedding.py` | The fake embedder is deterministic, normalised and finite on empty text; `text_hash`. |
| `tests/test_migrate.py` | Migrations are idempotent; tables exist. |
| `tests/test_store.py` | The merge rules one scenario at a time: older event does not overwrite; older *full* event after a newer partial fills the missing fields; untimed events only fill empty fields; ties broken by `message_id` in byte order (both arrival orders); strike-off hides; stale upsert does not resurrect; newer upsert revives; strike-off before any upsert; fallback embedding, its replacement, and none at all; the embedding follows the final description; the cache is shared; two concurrent copies of one message apply once; `embedding_source` is the same in both arrival orders. |
| `tests/test_feed_replay.py` | The whole feed through `handle_message`, with up to a pool's worth of messages in flight like the consumer: exact counts and rejection reasons; **order independence**: three random shuffles, each plus 500 redelivered messages, produce a `companies` table identical to the in-order run in every column, including the embedding vectors; replaying the feed twice gives 2,518 duplicates and no change. |
| `tests/test_api.py` | Most similar first; self excluded; struck-off companies excluded; `limit` honoured and validated; deterministic order on ties; `syn-…` normalised; 404, 422; card for an active and a struck-off company; empty list with `no_description`; 503 when the database is down; `/healthz` and `/readyz`. |
| `tests/test_eval_metrics.py` | The evaluation metrics on hand-built vectors: perfect clusters give precision 1.0; known ranks give exact recall and MRR; identical texts and the query itself are skipped; ties count against the target. |
| `tests/test_reembed.py` | `reembed` moves every embedding to a new model (a `FakeEmbedder` subclass with another `model_name`) and is a no-op with the same model. |
| `tests/test_consumer.py` | `on_message`: valid → ack; poison → recorded and acked; no `message_id` → body hash; transient error → sleep, then nack with requeue, nothing recorded. |
| `tests/test_consumer_rabbitmq.py` | Real broker: the feed published to `company-events` and drained by `run` gives the same `companies` table (every column) and rejection counts as a direct replay; publishing it again changes nothing. |

The order-independence test is the one that matters most: it checks the property the design is built around, on the
real feed, rather than the cases I thought of.

The full path through RabbitMQ (`just consume` + `just publish` twice, Ctrl-C) was run by hand with the real model:
`applied=2433 duplicate=68 rejected=17`, then all 2,518 duplicates, then a clean shutdown. There is no automated test
for `run()` against a real broker yet (section 8).

## 6. Recommendation quality

**Question.** 73 of the 180 distinct texts are Malay. Does the brief's English model, `BAAI/bge-small-en-v1.5`, put a
Malay company next to English companies in the same business, or only next to other Malay text? `just eval`
(`eval/evaluate.py`, metrics in `eval/metrics.py`) compares it with `paraphrase-multilingual-MiniLM-L12-v2` (also
384 dimensions, both through fastembed) and the hashed bag of words as a floor.

**Method.** The 1,988 active companies that have something to embed, using exactly the text the service embeds
(`embedding_text`). Each distinct text is embedded once per model; similarity is cosine.

- **Industry P@4.** For each company with an industry, the share of its 4 nearest neighbours in the same industry; a
  neighbour without an industry counts as a miss. Industry is a proxy: it says "same sector", not "good
  recommendation". Reported twice. *All neighbours* is what the panel shows, but most descriptions are shared by 20–40
  companies, so the top 4 are mostly exact copies, which always share an industry in this data: it measures the
  duplicates, not the model. *Other texts* skips neighbours whose text is identical to the company's, so it asks
  whether the nearest *different* description is in the same sector. I kept every company (rather than collapsing to
  180 texts) so a description weighs as often as it is shown; each text maps to one industry here, so collapsing
  would only reweight. "Random neighbours" is the chance level.
- **Malay↔English pairs** (`eval/pairs.csv`, 30 pairs: 24 without a town, 6 with one). For each Malay text, the rank
  of its English counterpart among all 180 distinct texts (the query itself excluded), and the reverse. Recall@1,
  recall@5, MRR. A tie with the counterpart counts against it.
- **Speed:** best of three, embedding the 180 texts on this laptop's CPU, model load excluded.

| Model | Industry P@4 (all) | Industry P@4 (other texts) | ms→en R@1 | ms→en R@5 | ms→en MRR | en→ms R@1 | en→ms R@5 | en→ms MRR | ms / 100 texts |
|---|---|---|---|---|---|---|---|---|---|
| random neighbours | 0.086 | 0.076 | | | | | | | |
| fake-hashed-bow | 0.939 | 0.314 | 0.03 | 0.03 | 0.061 | 0.03 | 0.07 | 0.062 | 2 |
| bge-small-en-v1.5 | 0.925 | 0.496 | 0.03 | 0.07 | 0.071 | 0.17 | 0.23 | 0.221 | 716–962 |
| paraphrase-multilingual-MiniLM-L12-v2 | **0.941** | **0.821** | **0.83** | **0.97** | **0.886** | **0.83** | **0.97** | **0.884** | 493–526 |

(Speed is the range over two runs. The full output, including every pair's rank, is printed by `just eval`.)

**What the numbers say.**

- **bge-small-en does not cross languages.** A Malay description finds its English counterpart first in 1 of 30
  cases (median rank 84 of 179), barely better than the bag of words. A one-off check (Malay detected by the
  descriptions' leading word; not part of `just eval`) shows why: 97% of the 4 nearest neighbours of a Malay text are
  other Malay texts, whatever the business. For the multilingual model it is 60%.
- **The multilingual model does.** 25 of 30 counterparts rank first and 29 of 30 in the top 5, in both directions.
  The misses are the town pairs (it ranks "parcel delivery in another town" above the exact town, worst case 12th)
  and one loose translation ("Kedai runcit barangan keperluan harian" vs "Mini market selling groceries...").
  For recommendations, the same business in another town is an acceptable neighbour.
- **Sector agreement** beyond exact copies rises from 0.50 to 0.82 (chance 0.08). Partly the same effect: the
  nearest different text to a Malay description is often its translation. P@4 over all neighbours is 0.93–0.94 for
  every model, including the bag of words, which confirms it measures duplicates.
- **SYN-001550:** both models give the same first three (the exact copy, then two Malay translations at 0.834 with
  bge, 0.950 multilingual). bge's fourth is a Logistics parcel-delivery company that shares only "the Klang Valley"
  (as in the mock); the multilingual model's is a café in Kuantan.
- **Cost:** both embed in under 10 ms per text, and thanks to the cache only ~180 texts are ever embedded, so speed
  does not decide. The multilingual model is ~220 MB on disk instead of ~67 MB, and truncates at 128 tokens; the
  longest cleaned description is 11 words.

**Limitations.**

- **The pairs were picked and labelled by the author of this code (Claude, during implementation), not by a Malay
  speaker. The candidate should review `eval/pairs.csv`.** 30 pairs is small, and choosing descriptions with a clear
  translation favours a translation-trained model. With n = 30 a recall of 0.83 has a 95% interval of roughly
  0.66–0.93; the gap to 0.03 is far outside that, but a difference of a few pairs would not be.
- Industry is a weak label (a pharmacy and a home-nursing service are both Healthcare and may not be "similar").
- The feed is synthetic and templated (178 descriptions, town variants). Free-text descriptions from a real registry
  could rank the models differently; the eval should be rerun on a real sample before trusting the margin.
- Only these two models were compared; `multilingual-e5-small` would be the next candidate. Both come as fastembed's
  quantised ONNX builds, so the numbers are for those builds. Tie order in P@4 is by registration number, not the
  API's same-industry-first rule; with distinct texts real models almost never tie.

**Decision.** The default is now `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` (`Settings`, README).
It wins every quality metric, decisively on the Malay↔English one that motivated the comparison, at no practical
speed cost. `just reembed` (`app/reembed.py`) re-embedded the dev database in 15 s (it calls `refresh_embedding`, which
already recomputes rows whose `embedding_model` differs), and `GET /companies/SYN-001550/similar` now returns the
result in section 2.8 with `"model": "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"`.

## 7. At 10x (and beyond)

10x is about 20,000 companies and 25,000 events per replay; the current design handles that without structural
change. These are the changes in the order I would make them as volume grows.

1. **Measure, then add HNSW.** The exact scan is linear in the number of active companies. Once it exceeds the latency
   budget, add `CREATE INDEX ... USING hnsw (embedding vector_cosine_ops)` and use pgvector 0.8's
   `SET LOCAL hnsw.iterative_scan = relaxed_order` with an over-fetch and re-sort, so the `active` filter cannot
   return fewer than `limit` rows. The exact ordering keys (industry, state, reg. no.) are then applied to the
   candidates, and HNSW recall should be checked against the exact scan.
2. **Move embedding out of the transaction (approach C).** Today the model runs while the transaction holds the row
   lock on that company. That is fine at this size and thanks to the cache, but with many new descriptions it slows
   every writer. Instead: the consumer marks the row `embedding_stale` (an outbox), commits and acks; a separate
   worker embeds stale rows in batches, compare-and-sets on `embedding_text_hash`, and can run on its own machines.
3. **Batch embeddings.** fastembed is much faster per text when given batches. The worker in (2) embeds a batch of
   distinct uncached texts at once.
4. **More consumers, no partitioning.** Because the merge is order-free and the dedupe is transactional, more
   consumer processes can read the same queue with no partitioning by company and no single-active-consumer. The
   limits become database write throughput and row contention for very hot companies.
5. **Cache and read replicas for the API.** Recommendations only change when the feed changes. Cache responses per
   `(reg_no, limit, model)` and invalidate on apply, or precompute the top 20 per company in the worker. Serve reads
   from a replica.
6. **Housekeeping.** `processed_messages` grows forever; keep it for a window longer than any plausible redelivery
   (or keep only hashes). Partition `company_events` by month.
7. **Model swaps without downtime.** The cache is keyed by model and each row records `embedding_model`.
   `just reembed` exists but works in place: while it runs (15 s for 2,000 companies) the table holds two models'
   vectors and similarity across them is meaningless. At scale, write the new vectors to a second column, backfill,
   then switch the API's query; or filter on `embedding_model` during the switch.

## 8. What's left

Being honest about what is not done or not proven:

- **Recommendation quality is measured only offline, on proxies** (section 6): industry agreement and 30
  Malay↔English pairs labelled by the author, which the candidate should review. No user signal.
- **No latency measurement.** A single request for SYN-001550 took about 30 ms through curl on a laptop, but there is
  no p50/p95 benchmark, and no HNSW vs exact-scan comparison. The spec target is p95 < 50 ms.
- **No automated end-to-end test through RabbitMQ.** `on_message` is tested with a fake message; `run()` (connection,
  prefetch, shutdown) was verified by hand only.
- **Shutdown race window.** `run()` counts a message as in flight when its handler starts. A delivery handed over
  between cancelling the subscription and that first line could be cut off at shutdown. The broker redelivers it
  (it was never acked) and the dedupe makes that safe, so the cost is one redelivery, not data loss.
- **The rejection log line lacks the reason.** The consumer logs `rejected message <id>`; the reason is only in
  `rejected_messages`. `handle_message` should return it.
- **Untested error paths:** `/readyz` returning 503, and the `PoolTimeout` → 503 handler (the `OperationalError` →
  503 path is tested).
- **No retry limit for transient errors.** A message that fails for a non-validation reason on every attempt (a bug,
  say) is requeued every second forever. A retry counter (a classic queue does not count redeliveries, so a table keyed on `message_id`)
  that parks it in `rejected_messages` after N attempts would fix that.
- **Unknown states are logged once per event**, which would be noisy if a source started sending a new spelling.
- **No metrics.** Counts are in logs only; a production version wants applied/duplicate/rejected/requeued counters,
  queue depth, and the age of the newest applied event.
- **`just serve` does not migrate.** It relies on the consumer or `just migrate` having run; `/readyz` reports it.

## 9. AI tools used

<!-- candidate: verify and edit this section -->

I used Claude Code (Anthropic) throughout:

- **Feed profiling:** scripts that counted the traps in `data/feed.jsonl` (section 3).
- **Design and plan:** drafting the design spec and the task-by-task implementation plan from my decisions.
- **Implementation:** writing the code and tests task by task, test first, with a separate code-review pass on each
  task. Several real bugs came out of those reviews (for example the hostile-timestamp crash loop, the
  locale-dependent tie-break, and the nack-before-sleep hot loop).
- **This document and the README section:** first drafts.

The design decisions are mine: per-field last-writer-wins with an event log (approach A), how struck-off companies
behave (kept, visible on their page, never recommended), and the build priorities when time ran short. I reviewed the
code and can walk through any of it.
