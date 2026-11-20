"""on_message: ack after commit, ack poison, requeue transient errors."""

import hashlib
import json

import psycopg
import pytest

from app import consumer
from app.consumer import on_message


class FakeMessage:
    """Stands in for aio_pika's incoming message; records ack/nack calls."""

    def __init__(self, body: bytes, message_id: str | None = "m-1"):
        self.body = body
        self.message_id = message_id
        self.acked = 0
        self.nacks: list[bool] = []

    async def ack(self) -> None:
        self.acked += 1

    async def nack(self, requeue: bool = True) -> None:
        self.nacks.append(requeue)


async def _count(pool, table: str) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute(f"SELECT count(*) FROM {table}")
        return (await cur.fetchone())[0]


def _valid_body() -> bytes:
    event = {
        "type": "company-upserted",
        "registrationNumber": "SYN-000001",
        "updatedAt": "2026-08-14T09:00:00+08:00",
        "data": {"companyName": "Acme Ltd", "natureOfBusiness": "Trading", "state": "Johor", "industry": "Retail"},
    }
    return json.dumps(event).encode()


async def test_valid_message_acked(pool, clean_db, embedder):
    message = FakeMessage(_valid_body())
    assert await on_message(message, pool, embedder) == "applied"
    assert message.acked == 1 and message.nacks == []
    assert await _count(pool, "processed_messages") == 1


async def test_poison_message_acked(pool, clean_db, embedder):
    message = FakeMessage(b"null")
    assert await on_message(message, pool, embedder) == "rejected"
    assert message.acked == 1 and message.nacks == []
    assert await _count(pool, "rejected_messages") == 1


async def test_missing_message_id_uses_body_hash(pool, clean_db, embedder):
    body = _valid_body()
    message = FakeMessage(body, message_id=None)
    assert await on_message(message, pool, embedder) == "applied"
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT message_id FROM processed_messages")
        assert [r[0] for r in await cur.fetchall()] == [hashlib.sha256(body).hexdigest()]


async def test_transient_error_requeues(pool, clean_db, embedder, monkeypatch):
    async def boom(*args, **kwargs):
        raise psycopg.OperationalError("database went away")

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(consumer, "handle_message", boom)
    monkeypatch.setattr(consumer.asyncio, "sleep", no_sleep)
    message = FakeMessage(_valid_body())
    assert await on_message(message, pool, embedder) is None
    assert message.nacks == [True] and message.acked == 0
    assert await _count(pool, "rejected_messages") == 0
