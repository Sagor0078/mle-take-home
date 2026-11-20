# MLE Take-Home: Company Recommendation System

## Running the solution

This repo now contains a consumer that reads `company-events` into Postgres/pgvector and a FastAPI service that
returns similar companies. Why it is built this way, what was assumed, how it is tested and what is left are in
[`DESIGN.md`](DESIGN.md). The original assignment text is kept unchanged [below](#the-assignment-original-brief).

### Prerequisites

- Docker, [uv](https://docs.astral.sh/uv/) and [just](https://github.com/casey/just), as in the original brief.
- A **final** Python 3.14 release. Older uv versions install 3.14.0rc2, on which pydantic (and so FastAPI) fails to
  import. If `uv run python --version` prints an `rc`, run `uv self update && uv python install 3.14`, then
  `rm -rf .venv && just setup`. Tested with uv 0.13.9 and Python 3.14.10.
- About 70 MB of free disk and network access the first time the consumer starts: it downloads the embedding model
  `BAAI/bge-small-en-v1.5` (~67 MB) through fastembed.

### Run it

```bash
just setup      # uv sync --locked: installs all dependencies
just up         # Postgres (pgvector) + RabbitMQ, waits until healthy
just migrate    # optional: the consumer also applies migrations when it starts
just consume    # terminal 1: runs until Ctrl-C
just publish    # terminal 2: puts the 2,518-message feed on the queue
just serve      # terminal 3: API on http://localhost:8000 (API_PORT to change)
```

`just consume` applies the migrations, loads the model, declares `company-events` exactly as the registry team does
(durable, no arguments) and processes messages until you press Ctrl-C. It logs a running total every 500 messages, and
on Ctrl-C it waits for in-flight messages and prints the totals. For one publish of the feed that is:

```
final counts: applied=2433 duplicate=68 rejected=17 requeued=0
```

Running `just publish` again is safe: every message is then a `duplicate` and the database does not change.
Rejected messages are kept, with the reason, in the `rejected_messages` table.

`just serve` does **not** apply migrations. Start the consumer (or run `just migrate`) at least once before it;
`GET /readyz` returns 503 until the schema exists.

### Try it

```bash
curl -s localhost:8000/companies/SYN-001550/similar | python -m json.tool
```

Response from a database filled by the steps above (scores shortened here to 3 decimals; the API returns full floats):

```json
{
  "company": {"registrationNumber": "SYN-001550", "name": "Sinar Teguh Bakery Sdn. Bhd.",
              "industry": "Food & Beverage", "state": "Selangor", "status": "active"},
  "similar": [
    {"registrationNumber": "SYN-000539", "name": "Seri Sejahtera Food Sdn. Bhd.",
     "industry": "Food & Beverage", "state": "Johor", "status": "active", "score": 1.0},
    {"registrationNumber": "SYN-000263", "name": "Gemilang Delima Food Sdn. Bhd.",
     "industry": "Food & Beverage", "state": "Johor", "status": "active", "score": 0.834},
    {"registrationNumber": "SYN-001650", "name": "Everbright Pacific Food Sdn. Bhd.",
     "industry": "Food & Beverage", "state": "Kuala Lumpur", "status": "active", "score": 0.834},
    {"registrationNumber": "SYN-000700", "name": "Teguh Mega Logistik Trading",
     "industry": "Logistics", "state": "Selangor", "status": "active", "score": 0.830}
  ],
  "reason": null,
  "model": "BAAI/bge-small-en-v1.5"
}
```

Other endpoints and cases:

| Request | Status | Body |
|---|---|---|
| `GET /companies/SYN-001550/similar?limit=8` | 200 | as above, up to 8 results (`limit` 1–20, default 4) |
| `GET /companies/syn-001550` | 200 | the card for the page itself (reg. no. is normalised) |
| `GET /companies/SYN-000186/similar` | 200 | struck-off company: `"status": "struck_off"`, `"similar": []`, `"reason": "struck_off"` |
| `GET /companies/SYN-000048/similar` | 200 | no description and no industry: `"similar": []`, `"reason": "no_description"` |
| `GET /companies/SYN-999999/similar` | 404 | `{"error": "company_not_found"}` |
| `GET /companies/hello/similar` | 422 | `{"error": "invalid_registration_number"}` |
| `GET /healthz`, `GET /readyz` | 200 / 503 | process up; database reachable and migrated |

Interactive docs are at http://localhost:8000/docs.

### Tests

```bash
just up         # the tests need the Postgres container (not RabbitMQ)
just test       # 127 tests, about a minute
```

The tests create a throwaway database `companies_test_<hex>` on the same Postgres server and drop it at the end, so
they do not touch the `companies` database. They never download a model: they use a deterministic hashed
bag-of-words embedder. What each test file covers is in [`DESIGN.md`](DESIGN.md#5-testing).

### Configuration

Everything has a default that matches `docker-compose.yml`. Set variables in your shell or in a local `.env` file
(git-ignored); the Justfile loads `.env` (`set dotenv-load`), so `docker compose` and the app see the same values.

| Variable | Default | Used by |
|---|---|---|
| `POSTGRES_PORT` | `5432` | compose, and the default `DATABASE_URL` |
| `RABBITMQ_PORT` | `5672` | compose, `just publish`, and the default `RABBITMQ_URL` |
| `RABBITMQ_UI_PORT` | `15672` | compose (management UI) |
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:$POSTGRES_PORT/companies` | consumer, API, migrate, tests |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:$RABBITMQ_PORT/` | consumer, producer |
| `EMBED_MODEL` | `BAAI/bge-small-en-v1.5` | consumer (any 384-dim fastembed model); API reports it as `model` |
| `PREFETCH` | `32` | consumer: unacked messages in flight |
| `API_PORT` | `8000` | `just serve` |

If the default ports are taken on your machine, for example:

```bash
printf 'POSTGRES_PORT=5435\nRABBITMQ_PORT=5673\nRABBITMQ_UI_PORT=15673\n' > .env
```

### Layout

```
app/          config, db (pool), migrate, events (parse/normalise), text (description cleaning),
              embedding, store (all SQL), ingest (one message = one transaction), consumer, api
migrations/   001_init.sql
tests/        unit tests for parsing/text/embedding; integration tests against Postgres
DESIGN.md     decisions, assumptions, testing, 10x, what's left, AI-tool use
```

---

## The assignment (original brief)


Welcome to the **Machine Learning Engineer** take-home from the OneCredit Data & AI team. It's a simplified version of
a system we run in production, where it powers company recommendations and lead generation.

**Who it's for:** people early in their career, with 0 to 2 years of experience. Fresh graduates are welcome.

If you applied, thanks for your time. If you didn't, we hope this helps you learn something new, such as RabbitMQ or
pgvector.

Assessment version: **2026.11** (`mle-v2026.11`). What changed between versions is in [`CHANGELOG.md`](CHANGELOG.md).

## The scenario

When someone opens a company's page on the OneCredit portal, we want to show a **"Similar companies"** panel next to
it. Someone looking at a coffee shop chain in Selangor should see other companies that do similar things.

The registry team already publishes every company change to a RabbitMQ queue. Your job is to build the service that
powers that panel from the feed.

### What the user sees

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  OneCredit Portal                                         Search companies…  │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Sinar Teguh Bakery Sdn. Bhd.                                   SYN-001550   │
│  Food & Beverage · Selangor                                                  │
│  Operating cafes and coffee shops in the Klang Valley                        │
│                                                                              │
│  Similar companies                                                           │
│  ┌────────────────────────────────────┐  ┌────────────────────────────────┐  │
│  │ Seri Sejahtera Food Sdn. Bhd.      │  │ Everbright Pacific Food        │  │
│  │ Food & Beverage · Johor            │  │ Sdn. Bhd.                      │  │
│  │                                    │  │ Food & Beverage · Kuala Lumpur │  │
│  └────────────────────────────────────┘  └────────────────────────────────┘  │
│  ┌────────────────────────────────────┐  ┌────────────────────────────────┐  │
│  │ Gemilang Delima Food Sdn. Bhd.     │  │ Pacific Everbright Logistik    │  │
│  │ Food & Beverage · Johor            │  │ PLT                            │  │
│  │                                    │  │ Logistics · Kuala Lumpur       │  │
│  └────────────────────────────────────┘  └────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

Clicking a card opens that company's page, with its own "Similar companies" panel.

### How it works for the user

```mermaid
sequenceDiagram
    actor User as Portal user
    participant Portal as OneCredit portal
    participant Service as Your service
    participant DB as Postgres + pgvector
    User->>Portal: Opens the page for SYN-001550
    Portal->>Service: Similar companies for SYN-001550
    Service->>DB: Nearest neighbours of SYN-001550
    DB-->>Service: Closest companies
    Service-->>Portal: Similar companies, most similar first
    Portal-->>User: Page with the "Similar companies" panel
    User->>Portal: Clicks Seri Sejahtera Food Sdn. Bhd.
    Portal->>Service: Similar companies for SYN-000539
```

### How the data gets there

```mermaid
flowchart LR
    P[Registry feed<br/>producer] -->|company-events| Q[(RabbitMQ)]
    Q --> C[Your consumer]
    C -->|embed descriptions| E[Embedding model]
    C --> D[(Postgres + pgvector<br/>similarity search)]
    A[Your service] --> D
    W[OneCredit portal] --> A
```

### Notes from the registry team

> The feed merges a few upstream sources and we don't validate what they send. Delivery is at-least-once, and every
> message has an AMQP `message_id`. Each event carries the time the change happened at the source in `updatedAt`. We own
> the `company-events` queue and declare it as durable with no extra arguments. We can't change the format before your
> launch, so please work with what's there.

### What the portal needs

- Recommendations that reflect the latest information we've received about each company.
- No lost companies. If a company is in the feed, users expect to see it on the portal and in other companies'
  recommendations.
- Fast enough to render while the company page loads.

## The task

1. Build a **consumer** that reads the `company-events` queue and stores each company in Postgres, with an embedding
   of its business description in pgvector.
2. Build the **service** the portal calls: given a company, return its most similar companies, with what the portal
   needs to draw each card in the mock above. Run the similarity search in Postgres with pgvector. Matching companies
   on their nature of business is enough for a first version.
3. Write a short `DESIGN.md` covering the decisions you made, the assumptions you made where the scenario was unclear,
   what you would change to handle 10x the volume, and what's left.
4. Optional, up to one hour of your six: go deeper in one area you're strong in. For example: how good the
   recommendations are, latency at a realistic scale, how you'd monitor this in production, or a design for explaining
   a recommendation with an LLM (a design only; no paid APIs). Write it up in `DESIGN.md`.

The rest is your call: the schema, the embedding model, your tests and tooling, and the API, including what the portal
gets back when something goes wrong. Tell us in your own README how to start your consumer and service, and we'll
follow it.

Where the scenario leaves something open, make a reasonable call and write it down. There isn't one right answer.

## Embeddings

Use an open-source embedding model that runs on a laptop CPU. Don't call a paid API. Some options:

- `BAAI/bge-small-en-v1.5`, a small CPU model (384 dimensions), through
  [fastembed](https://qdrant.github.io/fastembed/) or sentence-transformers. The panel in the mock above comes from it.
- A placeholder while you build, such as a hashed bag-of-words vector, as long as you say so in `DESIGN.md`.

## Getting started

You need Docker, [uv](https://docs.astral.sh/uv/) (it installs Python 3.14 for you) and
[just](https://github.com/casey/just). Setup takes about 5 minutes and doesn't count towards your time.

```bash
just setup      # install the producer's dependencies
just up         # start Postgres (with pgvector) and RabbitMQ
just publish    # publish the registry feed to the queue
just down       # stop everything and delete the data
```

Postgres is at `postgresql://postgres:postgres@localhost:5432/companies`, and the RabbitMQ management UI is at
http://localhost:15672 (user `guest`, password `guest`). If those ports are taken on your machine, export
`POSTGRES_PORT`, `RABBITMQ_PORT` or `RABBITMQ_UI_PORT` in your shell before `just up`; `just publish` uses the same
`RABBITMQ_PORT`.

`just publish` sends the whole feed every time you run it. To start again from an empty queue and database, run
`just down` and then `just up`.

## What's already here

```
.
├── data/feed.jsonl        # the registry feed: 2,518 messages about 2,000 synthetic Malaysian companies, duplicates included
├── producer/publish.py    # replays the feed onto the queue
├── docker-compose.yml     # Postgres with pgvector, and RabbitMQ
└── Justfile               # setup, up, publish, down
```

Add whatever else you need (`uv add` for libraries).

## How to submit

1. Click **Use this template** → **Create a new repository**, and make it **public**. Any name is fine.
2. Build your solution in your copy, with `DESIGN.md` at the top level.
3. When you're done, fill in the [submission form](https://docs.google.com/forms/d/e/1FAIpQLSdjCsXP5jFrKrIi4krLVS8Sq0rhM4mLlhycpsAo34sSj4h3FA/viewform) with a link to your repo and your résumé.

If the setup itself is broken (Docker, the services or the data won't start), open a
[Setup problem](https://github.com/OneCreditMY/mle-take-home/issues/new?template=setup-problem.yml) issue. Issues are public, so don't put your
solution or personal details in one.

## Time

Plan for **4 to 6 hours**, within 7 days of creating your copy, and stop at 6 hours. If you run out of time,
write down what you'd do next. A clear, honest "here's what's left" counts in your favour.

## What we look at

- Does it work, and does it keep working?
- How you tested it.
- How you reasoned about your choices and assumptions in `DESIGN.md`.
- How readable your code is.

We don't expect a perfect or complete solution.

## Using AI tools

Go ahead. Tell us in `DESIGN.md` which tools you used and for what. In the interview we'll ask you
to walk us through your code, so make sure you understand everything you submit.

## After you submit

- We review your work as soon as we can.
- If we move forward, the next interview is a walkthrough of your code with us.
- Either way, you'll get written feedback by email.
- Your repo stays yours. Keep it public as a portfolio piece if you like.

## About the data

Everything in this repo is synthetic. We generated it for this assessment.

- The companies, names, registration numbers, descriptions and events are made up. Any resemblance to a real company
  or person is a coincidence.
- No OneCredit customer data, credit data or production data is in this repo, and none was used to create it.
- Registration numbers use a made-up `SYN-` format, so they can't be mistaken for real Companies Commission of
  Malaysia (SSM) numbers.
- Your copy is public, so keep secrets, credentials and personal data out of it.
