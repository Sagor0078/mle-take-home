"""HTTP latency of the similar-companies endpoint. Run with `just bench` against `just serve`.

Calls /companies/{reg_no}/similar for random active companies at a fixed concurrency,
prints p50/p95/p99, then EXPLAIN ANALYZE of the similarity query for one company.
"""

import argparse
import asyncio
import random
import statistics
import time

import httpx
import psycopg

from app.config import Settings
from app.store import SIMILAR_SQL


def percentiles(samples_ms: list[float]) -> dict[str, float]:
    """p50/p95/p99 of a list of milliseconds."""
    cuts = statistics.quantiles(samples_ms, n=100, method="inclusive")
    return {"p50": cuts[49], "p95": cuts[94], "p99": cuts[98]}


async def call_all(base_url: str, reg_nos: list[str], concurrency: int) -> list[float]:
    """GET each reg_no's similar list, `concurrency` at a time; return each latency in ms."""
    queue: asyncio.Queue[str] = asyncio.Queue()
    for reg_no in reg_nos:
        queue.put_nowait(reg_no)
    latencies: list[float] = []

    async def worker(client: httpx.AsyncClient) -> None:
        while not queue.empty():
            reg_no = queue.get_nowait()
            start = time.perf_counter()
            response = await client.get(f"/companies/{reg_no}/similar")
            latencies.append((time.perf_counter() - start) * 1000)
            response.raise_for_status()

    limits = httpx.Limits(max_connections=concurrency)
    async with httpx.AsyncClient(base_url=base_url, limits=limits, timeout=10) as client:
        await asyncio.gather(*(worker(client) for _ in range(concurrency)))
    return latencies


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    database_url = Settings.from_env().database_url
    with psycopg.connect(database_url) as conn:
        active = [r[0] for r in conn.execute("SELECT reg_no FROM companies WHERE active ORDER BY reg_no")]
    rng = random.Random(args.seed)
    reg_nos = [rng.choice(active) for _ in range(args.requests)]

    asyncio.run(call_all(args.url, reg_nos[:50], args.concurrency))  # warm up the pool and caches
    start = time.perf_counter()
    latencies = asyncio.run(call_all(args.url, reg_nos, args.concurrency))
    elapsed = time.perf_counter() - start
    stats = percentiles(latencies)
    print(f"{len(latencies)} requests, concurrency {args.concurrency}, {len(latencies) / elapsed:.0f} req/s")
    print("  ".join(f"{name} {value:.1f} ms" for name, value in stats.items()))

    with psycopg.connect(database_url) as conn:
        plan = conn.execute(
            "EXPLAIN (ANALYZE, BUFFERS) " + SIMILAR_SQL, {"reg": "SYN-001550", "limit": 4}
        ).fetchall()
    print("\nEXPLAIN ANALYZE for SYN-001550:")
    print("\n".join(row[0] for row in plan))


if __name__ == "__main__":
    main()
