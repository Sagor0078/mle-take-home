import random
from collections import Counter

from tests.conftest import truncate_all
from tests.feed_helpers import FEED_LINES, replay, snapshot


async def _snapshot(pool) -> list[tuple]:
    async with pool.connection() as conn:
        return await snapshot(conn)


async def _scalar(pool, query: str):
    async with pool.connection() as conn:
        cur = await conn.execute(query)
        return (await cur.fetchone())[0]


async def test_full_feed_counts(pool, embedder, clean_db):
    outcomes = await replay(pool, embedder, FEED_LINES)

    assert outcomes == Counter(applied=2433, duplicate=68, rejected=17)
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT reason, count(*) FROM rejected_messages GROUP BY reason")
        reasons = dict(await cur.fetchall())
    assert reasons == {
        "missing_registration_number": 7,
        "invalid_json": 5,
        "invalid_data": 4,
        "not_an_object": 1,
    }
    assert await _scalar(pool, "SELECT count(*) FROM companies") == 2000
    assert await _scalar(pool, "SELECT count(*) FROM companies WHERE NOT active") == 8
    assert await _scalar(pool, "SELECT count(*) FROM companies WHERE embedding IS NULL") == 4
    assert (
        await _scalar(pool, "SELECT count(*) FROM companies WHERE embedding_source = 'fallback'")
        == 35
    )
    assert await _scalar(pool, "SELECT count(*) FROM companies WHERE name IS NULL") == 0


async def test_any_order_same_state(pool, embedder, clean_db):
    await replay(pool, embedder, FEED_LINES)
    baseline = await _snapshot(pool)

    for seed in (1, 2, 3):
        await truncate_all(pool)
        shuffled = FEED_LINES[:]
        random.Random(seed).shuffle(shuffled)
        # Reorder, and redeliver the first 500 messages.
        await replay(pool, embedder, shuffled + shuffled[:500])
        assert await _snapshot(pool) == baseline, f"seed {seed}"


async def test_replay_twice_is_noop(pool, embedder, clean_db):
    await replay(pool, embedder, FEED_LINES)
    baseline = await _snapshot(pool)

    outcomes = await replay(pool, embedder, FEED_LINES)

    assert outcomes == Counter(duplicate=2518)
    assert await _snapshot(pool) == baseline
