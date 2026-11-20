"""Consume `company-events` and apply each message to Postgres.

A message is acked only after its transaction commits. Poison messages are
recorded by handle_message and then acked; transient errors are nacked back
onto the queue.
"""

import asyncio
import logging
import signal
from collections import Counter
from typing import Final

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from app.config import Settings
from app.db import make_pool
from app.embedding import Embedder, make_embedder
from app.ingest import Outcome, handle_message
from app.migrate import upgrade_to_head

QUEUE: Final = "company-events"
SUMMARY_EVERY: Final = 500
RETRY_DELAY_SECONDS: Final = 1.0

log = logging.getLogger("consumer")


async def on_message(message: AbstractIncomingMessage, pool, embedder) -> Outcome | None:
    """Handle one delivery. Returns the outcome, or None if it was requeued."""
    try:
        outcome = await handle_message(pool, embedder, message.body, message.message_id)
    except Exception:
        # Transient (database down, deadlock, ...): the transaction rolled back, so retry later.
        log.exception("transient error on message %s; requeueing", message.message_id)
        # Sleep before nack: the delivery holds its prefetch slot, so the broker
        # cannot hand the message straight back while the dependency is down.
        await asyncio.sleep(RETRY_DELAY_SECONDS)
        await message.nack(requeue=True)
        return None
    await message.ack()  # only after the transaction has committed
    return outcome


def _summary(counts: Counter) -> str:
    return " ".join(f"{name}={counts[name]}" for name in ("applied", "duplicate", "rejected", "requeued"))


async def run(settings: Settings, stop: asyncio.Event, embedder: Embedder) -> None:
    """Consume until `stop` is set, then finish in-flight messages and close."""
    pool = await make_pool(settings.database_url)
    try:
        await asyncio.to_thread(upgrade_to_head, settings.database_url)

        counts: Counter = Counter()
        in_flight = 0
        idle = asyncio.Event()
        idle.set()

        async def handler(message: AbstractIncomingMessage) -> None:
            nonlocal in_flight
            in_flight += 1
            idle.clear()
            try:
                outcome = await on_message(message, pool, embedder)
            finally:
                in_flight -= 1
                if in_flight == 0:
                    idle.set()
            counts[outcome or "requeued"] += 1
            if outcome == "rejected":
                log.info("rejected message %s (reason recorded in rejected_messages)", message.message_id)
            if sum(counts.values()) % SUMMARY_EVERY == 0:
                log.info("processed so far: %s", _summary(counts))

        connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=settings.prefetch)
            # The registry team owns this queue: durable, no extra arguments.
            queue = await channel.declare_queue(QUEUE, durable=True)
            consumer_tag = await queue.consume(handler)
            log.info("consuming %s (prefetch=%d, model=%s)", QUEUE, settings.prefetch, embedder.model_name)
            await stop.wait()
            log.info("shutting down: waiting for in-flight messages")
            await queue.cancel(consumer_tag)
            await idle.wait()
        log.info("final counts: %s", _summary(counts))
    finally:
        await pool.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # model download chatter

    settings = Settings.from_env()
    # Load the model (a download on first run) before the signal handlers below take over
    # Ctrl-C, so Ctrl-C here interrupts at once instead of waiting for the download.
    try:
        embedder = make_embedder(settings.embed_model)
        embedder.embed(["warm up"])
    except KeyboardInterrupt:
        log.info("interrupted while loading %s", settings.embed_model)
        return

    async def _main() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await run(settings, stop, embedder)

    asyncio.run(_main())


if __name__ == "__main__":
    main()
