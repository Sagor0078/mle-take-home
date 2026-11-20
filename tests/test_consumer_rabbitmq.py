"""The consumer end to end: publish the feed to a real RabbitMQ and let `run` drain it.

Needs the docker-compose RabbitMQ (skipped when unreachable). The queue name is owned by the
registry team, so the tests purge `company-events` at the start but never delete or redeclare it.
"""

import asyncio
import dataclasses
import time

import aio_pika
import pytest
import pytest_asyncio

from app.config import Settings
from app.consumer import QUEUE, run
from producer.publish import publish
from tests.feed_helpers import FEED_LINES, replay, snapshot
from tests.conftest import truncate_all

pytestmark = pytest.mark.rabbitmq

UNIQUE_MESSAGES = 2450
TIMEOUT_SECONDS = 120
REJECTED_REASONS = {
    "missing_registration_number": 7,
    "invalid_json": 5,
    "invalid_data": 4,
    "not_an_object": 1,
}


@pytest_asyncio.fixture(scope="module")
async def rabbitmq_url():
    url = Settings.from_env().rabbitmq_url
    try:
        connection = await aio_pika.connect_robust(url, timeout=3)
    except Exception as exc:
        pytest.skip(f"RabbitMQ unreachable at {url}: {exc!r}")
    await connection.close()
    return url


@pytest.fixture
def settings(database_url, rabbitmq_url):
    return dataclasses.replace(
        Settings.from_env(),
        database_url=database_url,
        rabbitmq_url=rabbitmq_url,
        embed_model="fake-hashed-bow",
        prefetch=32,
    )


async def _queue_depth(url: str, purge: bool = False) -> int:
    connection = await aio_pika.connect_robust(url)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE, durable=True)  # same declaration as the app
        if purge:
            await queue.purge()
            return 0
        return queue.declaration_result.message_count


async def _scalar(pool, query: str):
    async with pool.connection() as conn:
        cur = await conn.execute(query)
        return (await cur.fetchone())[0]


async def _publish_and_consume(settings, pool) -> None:
    """Publish the whole feed, run the consumer until the queue is drained, then stop it."""
    await publish(FEED_LINES, settings.rabbitmq_url)
    stop = asyncio.Event()
    task = asyncio.create_task(run(settings, stop))
    try:
        deadline = time.monotonic() + TIMEOUT_SECONDS
        while True:
            if task.done():
                task.result()  # surfaces a crash in run()
                pytest.fail("consumer stopped before the queue drained")
            processed = await _scalar(pool, "SELECT count(*) FROM processed_messages")
            depth = await _queue_depth(settings.rabbitmq_url)
            if processed == UNIQUE_MESSAGES and depth == 0:
                break
            if time.monotonic() > deadline:
                pytest.fail(
                    f"not drained after {TIMEOUT_SECONDS}s: processed={processed}/"
                    f"{UNIQUE_MESSAGES}, queue depth={depth}"
                )
            await asyncio.sleep(0.2)
        stop.set()
        await asyncio.wait_for(task, timeout=30)
    finally:
        stop.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def _snapshot(pool) -> list[tuple]:
    async with pool.connection() as conn:
        return await snapshot(conn)


async def _reasons(pool) -> dict:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT reason, count(*) FROM rejected_messages GROUP BY reason")
        return dict(await cur.fetchall())


async def test_feed_through_rabbitmq_matches_direct_replay(settings, pool, embedder, clean_db):
    await _queue_depth(settings.rabbitmq_url, purge=True)

    await _publish_and_consume(settings, pool)
    via_broker = await _snapshot(pool)
    assert await _reasons(pool) == REJECTED_REASONS
    assert len(via_broker) == 2000

    await truncate_all(pool)
    await replay(pool, embedder, FEED_LINES)
    direct = await _snapshot(pool)

    assert via_broker == direct
    assert await _reasons(pool) == REJECTED_REASONS


async def test_republish_is_all_duplicates(settings, pool, clean_db):
    # Own first pass (rather than relying on the previous test's leftovers), then the repeat.
    await _queue_depth(settings.rabbitmq_url, purge=True)
    await _publish_and_consume(settings, pool)
    first = await _snapshot(pool)

    await _publish_and_consume(settings, pool)

    assert await _scalar(pool, "SELECT count(*) FROM processed_messages") == UNIQUE_MESSAGES
    assert await _snapshot(pool) == first
    assert await _reasons(pool) == REJECTED_REASONS
