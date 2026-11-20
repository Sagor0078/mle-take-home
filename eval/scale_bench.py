"""Exact scan vs HNSW as the table grows. Run with `just scale-bench`.

Creates a throwaway database `companies_bench_<hex>` on the configured server, migrates
it and copies in the companies from DATABASE_URL (read only). It then adds jittered
copies (new reg_nos, each vector plus small Gaussian noise, re-normalised) to reach each
size, and times the similarity query directly with psycopg for random query companies:

- exact: the app's SIMILAR_SQL (sequential scan, exact order with tie-breaks);
- hnsw: an HNSW index (vector_cosine_ops, default m/ef_construction), the query ordered by
  distance only with `hnsw.iterative_scan = relaxed_order`, over-fetching 4x, then re-sorted
  with the same tie-break keys in Python. Recall is measured against the exact top 4.

The database is dropped at the end, also on failure.
"""

import argparse
import asyncio
import random
import statistics
import time
import uuid

import numpy as np
import psycopg
from pgvector.psycopg import register_vector
from psycopg import sql
from psycopg.conninfo import make_conninfo

from app.config import Settings
from app.migrate import migrate
from app.store import SIMILAR_SQL

LIMIT = 4
OVERFETCH = 4
NOISE = 0.01  # per-dimension std; a copy keeps cosine ~0.98 to its source
COLUMNS = ["reg_no", "name", "nature", "industry", "state", "last_upsert_at", "struck_off_at",
           "embedding", "embedding_source", "embedding_model"]
TYPES = ["text", "text", "text", "text", "text", "timestamptz", "timestamptz",
         "vector", "text", "text"]

# Distance-only order so the HNSW index can serve it; the tie-breaks are applied in Python.
# The ORDER BY needs the query vector as a constant (a scalar subquery) for the index to apply;
# the joined `q` row supplies the values for the SELECT list.
HNSW_SQL = """
    SELECT c.reg_no, c.name, c.industry, c.state, c.embedding <=> q.embedding AS distance,
           c.industry IS DISTINCT FROM q.industry AS other_industry,
           c.state IS DISTINCT FROM q.state AS other_state
    FROM companies c, (SELECT embedding, industry, state FROM companies WHERE reg_no = %(reg)s) q
    WHERE c.active AND c.embedding IS NOT NULL AND c.reg_no <> %(reg)s
    ORDER BY c.embedding <=> (SELECT embedding FROM companies WHERE reg_no = %(reg)s)
    LIMIT %(fetch)s
"""


async def apply_migrations(url: str) -> None:
    """The app's migrations, so the bench table has the app's schema and storage settings."""
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as conn:
        await migrate(conn)


def load_source(source_url: str) -> list[tuple]:
    """Every company row from the source database, in COLUMNS order."""
    with psycopg.connect(source_url) as conn:
        register_vector(conn)
        query = sql.SQL("SELECT {} FROM companies ORDER BY reg_no").format(
            sql.SQL(", ").join(map(sql.Identifier, COLUMNS))
        )
        return conn.execute(query).fetchall()


def copy_rows(conn: psycopg.Connection, rows) -> None:
    """Bulk-insert rows (COLUMNS order) with binary COPY."""
    copy_sql = sql.SQL("COPY companies ({}) FROM STDIN WITH (FORMAT BINARY)").format(
        sql.SQL(", ").join(map(sql.Identifier, COLUMNS))
    )
    with conn.cursor().copy(copy_sql) as copy:
        copy.set_types(TYPES)
        for row in rows:
            copy.write_row(row)


def jittered_copies(source: list[tuple], start: int, count: int, rng: np.random.Generator):
    """`count` copies of random embedded source rows, each with a new reg_no and a noisy vector."""
    embedded = [row for row in source if row[7] is not None]
    for n in range(start, start + count):
        row = list(embedded[rng.integers(len(embedded))])
        vector = row[7].to_numpy() + rng.normal(0, NOISE, 384).astype(np.float32)
        row[0] = f"BENCH-{n:07d}"
        row[7] = vector / np.linalg.norm(vector)
        yield row


def percentiles(samples_ms: list[float]) -> str:
    cuts = statistics.quantiles(samples_ms, n=100, method="inclusive")
    return f"p50 {cuts[49]:6.1f} ms  p95 {cuts[94]:6.1f} ms"


def time_exact(conn, reg_nos: list[str]) -> tuple[list[float], dict[str, list]]:
    """Latency (ms) and top-LIMIT (reg_no, distance) of the app's exact query, per company."""
    latencies, tops = [], {}
    for reg in reg_nos:
        start = time.perf_counter()
        rows = conn.execute(SIMILAR_SQL, {"reg": reg, "limit": LIMIT}).fetchall()
        latencies.append((time.perf_counter() - start) * 1000)
        tops[reg] = [(row[0], 1 - row[4]) for row in rows]
    return latencies, tops


def hnsw_similar(conn, reg: str) -> list[tuple[str, float]]:
    """Over-fetch by distance through the index, then apply the exact order's tie-breaks."""
    with conn.transaction():
        conn.execute("SET LOCAL hnsw.iterative_scan = relaxed_order")
        rows = conn.execute(HNSW_SQL, {"reg": reg, "fetch": LIMIT * OVERFETCH}).fetchall()
    rows.sort(key=lambda r: (r[4], r[5], r[6], r[0]))
    return [(row[0], row[4]) for row in rows[:LIMIT]]


def time_hnsw(conn, reg_nos: list[str]) -> tuple[list[float], dict[str, list]]:
    latencies, tops = [], {}
    for reg in reg_nos:
        start = time.perf_counter()
        tops[reg] = hnsw_similar(conn, reg)
        latencies.append((time.perf_counter() - start) * 1000)
    return latencies, tops


def recall(exact: dict[str, list], approx: dict[str, list]) -> tuple[float, float]:
    """(share of exact top-4 reg_nos found, share of queries whose 4 distances all match).

    The second is lenient on ties: a different company at the same distance counts.
    """
    found = same_scores = 0
    for reg, top in exact.items():
        found += len({r for r, _ in top} & {r for r, _ in approx[reg]})
        exact_d, approx_d = [d for _, d in top], [d for _, d in approx[reg]]
        same_scores += len(exact_d) == len(approx_d) and np.allclose(exact_d, approx_d, atol=1e-6)
    return found / (LIMIT * len(exact)), same_scores / len(exact)


def measure(conn, queries: int, seed: int) -> None:
    conn.execute("VACUUM ANALYZE companies")
    total, active = conn.execute(
        "SELECT count(*), count(*) FILTER (WHERE active AND embedding IS NOT NULL) FROM companies"
    ).fetchone()
    candidates = [r[0] for r in conn.execute(
        "SELECT reg_no FROM companies WHERE active AND embedding IS NOT NULL ORDER BY reg_no"
    )]
    reg_nos = random.Random(seed).sample(candidates, min(queries, len(candidates)))
    print(f"\n## {total:,} companies ({active:,} active with an embedding), {len(reg_nos)} queries")

    time_exact(conn, reg_nos[:10])  # warm the cache
    exact_ms, exact_top = time_exact(conn, reg_nos)
    print(f"exact scan          {percentiles(exact_ms)}")

    start = time.perf_counter()
    conn.execute("CREATE INDEX companies_embedding_hnsw ON companies USING hnsw (embedding vector_cosine_ops)")
    print(f"hnsw build          {time.perf_counter() - start:6.1f} s")
    plan = "\n".join(r[0] for r in conn.execute(
        "EXPLAIN " + HNSW_SQL, {"reg": reg_nos[0], "fetch": LIMIT * OVERFETCH}
    ))
    assert "companies_embedding_hnsw" in plan, plan
    time_hnsw(conn, reg_nos[:10])
    hnsw_ms, hnsw_top = time_hnsw(conn, reg_nos)
    by_reg_no, by_score = recall(exact_top, hnsw_top)
    print(f"hnsw + re-sort      {percentiles(hnsw_ms)}")
    print(f"hnsw recall@{LIMIT}       {by_reg_no:.3f} by reg_no, {by_score:.3f} of queries with identical top-{LIMIT} scores")
    conn.execute("DROP INDEX companies_embedding_hnsw")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sizes", type=int, nargs="+", default=[2_000, 20_000, 200_000])
    parser.add_argument("--queries", type=int, default=200)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    source_url = Settings.from_env().database_url
    admin_url = make_conninfo(source_url, dbname="postgres")
    name = f"companies_bench_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        url = make_conninfo(source_url, dbname=name)
        asyncio.run(apply_migrations(url))
        source = load_source(source_url)
        rng = np.random.default_rng(args.seed)
        with psycopg.connect(url, autocommit=True) as conn:
            register_vector(conn)
            # Parallel workers share memory through /dev/shm, which is 64 MB in a default
            # docker container: too small for a 200k-row HNSW build or a parallel VACUUM.
            conn.execute("SET max_parallel_maintenance_workers = 0")
            copy_rows(conn, source)
            size = len(source)
            for target in sorted(args.sizes):
                if target > size:
                    copy_rows(conn, jittered_copies(source, size, target - size, rng))
                    size = target
                measure(conn, args.queries, args.seed)
    finally:
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
        print(f"\ndropped {name}")


if __name__ == "__main__":
    main()
