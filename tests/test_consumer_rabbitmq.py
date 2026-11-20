"""The consumer end to end: publish the feed to a real RabbitMQ and let `run` drain it.

Needs the docker-compose RabbitMQ (skipped when unreachable). The queue name is owned by the
registry team, so the tests purge `company-events` at the start but never delete or redeclare it.
"""

import asyncio
import dataclasses
import os
import time

import aio_pika
import httpx
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
STATS_REFRESH_SECONDS = 6
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


@pytest_asyncio.fixture(scope="module")
async def management_url(rabbitmq_url):
    port = os.environ.get("RABBITMQ_UI_PORT") or "15672"
    url = f"http://localhost:{port}/api/queues/%2F/{QUEUE}"
    try:
        async with httpx.AsyncClient(auth=("guest", "guest"), timeout=3) as client:
            await client.get(url)
    except httpx.HTTPError as exc:
        pytest.skip(f"RabbitMQ management API unreachable at {url}: {exc!r}")
    return url


@pytest_asyncio.fixture(autouse=True)
async def purged_queue(rabbitmq_url):
    """Start and finish each test with an empty queue."""
    await _purge_queue(rabbitmq_url)
    yield
    await _purge_queue(rabbitmq_url)


@pytest.fixture
def settings(database_url, rabbitmq_url):
    return dataclasses.replace(
        Settings.from_env(),
        database_url=database_url,
        rabbitmq_url=rabbitmq_url,
        embed_model="fake-hashed-bow",
        prefetch=32,
    )


async def _purge_queue(url: str) -> None:
    connection = await aio_pika.connect_robust(url)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE, durable=True)  # same declaration as the app
        await queue.purge()


async def _queue_stats(management_url: str) -> tuple[int, int]:
    """(ready + unacked messages, lifetime ack count) from the management API.

    The API refreshes its statistics only every few seconds, so callers must not trust a single
    zero: `_publish_and_consume` also requires the ack counter to rise by the whole feed.
    """
    async with httpx.AsyncClient(auth=("guest", "guest"), timeout=5) as client:
        response = await client.get(management_url)
    response.raise_for_status()
    body = response.json()
    return body.get("messages", 0), body.get("message_stats", {}).get("ack", 0)


async def _scalar(pool, query: str):
    async with pool.connection() as conn:
        cur = await conn.execute(query)
        return (await cur.fetchone())[0]


async def _publish_and_consume(settings, pool, management_url) -> None:
    """Publish the whole feed, run the consumer until the queue is drained, then stop it."""
    await asyncio.sleep(STATS_REFRESH_SECONDS)  # let the baseline stats settle
    _, acked_before = await _queue_stats(management_url)
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
            depth, acked = await _queue_stats(management_url)
            if (
                processed == UNIQUE_MESSAGES
                and depth == 0
                and acked - acked_before >= len(FEED_LINES)
            ):
                break
            if time.monotonic() > deadline:
                pytest.fail(
                    f"not drained after {TIMEOUT_SECONDS}s: processed={processed}/"
                    f"{UNIQUE_MESSAGES}, queue messages={depth}, acked={acked - acked_before}/{len(FEED_LINES)}"
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


async def test_feed_through_rabbitmq_matches_direct_replay(settings, management_url, pool, embedder, clean_db):
    await _publish_and_consume(settings, pool, management_url)
    via_broker = await _snapshot(pool)
    assert await _reasons(pool) == REJECTED_REASONS
    assert len(via_broker) == 2000

    await truncate_all(pool)
    await replay(pool, embedder, FEED_LINES)
    direct = await _snapshot(pool)

    assert via_broker == direct
    assert await _reasons(pool) == REJECTED_REASONS


async def test_republish_is_all_duplicates(settings, management_url, pool, clean_db):
    # Own first pass (rather than relying on the previous test's leftovers), then the repeat.
    await _publish_and_consume(settings, pool, management_url)
    first = await _snapshot(pool)

    await _publish_and_consume(settings, pool, management_url)

    assert await _scalar(pool, "SELECT count(*) FROM processed_messages") == UNIQUE_MESSAGES
    assert await _snapshot(pool) == first
    assert await _reasons(pool) == REJECTED_REASONS
