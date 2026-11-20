"""Turn one queue message into one database transaction."""

import hashlib
import logging
from typing import Literal

import psycopg
from psycopg_pool import AsyncConnectionPool

from app import store
from app.embedding import Embedder
from app.events import InvalidEvent, parse_event

Outcome = Literal["applied", "duplicate", "rejected"]

log = logging.getLogger(__name__)


async def handle_message(
    pool: AsyncConnectionPool, embedder: Embedder, body: bytes, message_id: str | None
) -> Outcome:
    """Apply, reject, or skip a message, all in one transaction.

    A missing message_id falls back to sha256 of the body. InvalidEvent, and a
    psycopg.DataError (the data itself is bad, so a retry would fail the same way),
    are recorded as rejections; any other error rolls back and propagates so the
    caller requeues.
    """
    message_id = message_id or hashlib.sha256(body).hexdigest()
    try:
        async with pool.connection() as conn, conn.transaction():
            # Claim first: a concurrent copy of this message waits here until we commit.
            if not await store.claim_message(conn, message_id):
                return "duplicate"
            try:
                event = parse_event(body)
            except InvalidEvent as exc:
                await store.record_rejection(conn, message_id, body, exc.reason)
                return "rejected"
            await store.apply_event(conn, event, message_id)
            await store.refresh_embedding(conn, event.reg_no, embedder)
            return "applied"
    except psycopg.DataError:
        # The transaction above rolled back. Record the rejection in a new one.
        log.exception("message %s could not be stored; rejecting it", message_id)
        async with pool.connection() as conn, conn.transaction():
            if not await store.claim_message(conn, message_id):
                return "duplicate"
            await store.record_rejection(conn, message_id, body, "unstorable")
            return "rejected"
