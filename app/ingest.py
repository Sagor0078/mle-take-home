"""Turn one queue message into one database transaction."""

import hashlib
from typing import Literal

from psycopg_pool import AsyncConnectionPool

from app import store
from app.embedding import Embedder
from app.events import InvalidEvent, parse_event

Outcome = Literal["applied", "duplicate", "rejected"]


async def handle_message(
    pool: AsyncConnectionPool, embedder: Embedder, body: bytes, message_id: str | None
) -> Outcome:
    """Apply, reject, or skip a message, all in one transaction.

    A missing message_id falls back to sha256 of the body. Only InvalidEvent is
    handled here; any other error rolls back and propagates so the caller requeues.
    """
    message_id = message_id or hashlib.sha256(body).hexdigest()
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
