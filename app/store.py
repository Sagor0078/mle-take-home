"""All SQL for ingesting events. Every function runs inside the caller's transaction."""

import asyncio
from dataclasses import dataclass

from psycopg import AsyncConnection, sql
from psycopg.types.json import Jsonb

from app.embedding import Embedder, text_hash
from app.events import CompanyEvent
from app.text import embedding_text

FIELDS = ("name", "nature", "industry", "state")


@dataclass(frozen=True)
class CompanyRow:
    reg_no: str
    name: str | None
    industry: str | None
    state: str | None
    active: bool
    has_embedding: bool


async def claim_message(conn: AsyncConnection, message_id: str) -> bool:
    """Record the message_id as seen; False if it was already there.

    A concurrent claimant of the same id blocks on the primary key until the first
    transaction commits (then gets False) or rolls back (then gets True).
    """
    cur = await conn.execute(
        "INSERT INTO processed_messages (message_id) VALUES (%s) ON CONFLICT DO NOTHING",
        (message_id,),
    )
    return cur.rowcount == 1


async def record_rejection(conn: AsyncConnection, message_id: str, body: bytes, reason: str) -> None:
    """Keep a message we could not parse, with why, so it can be inspected later."""
    await conn.execute(
        "INSERT INTO rejected_messages (message_id, body, reason) VALUES (%s, %s, %s)",
        (message_id, body, reason),
    )


def _field_assignment(field: str) -> sql.Composable:
    """SET clauses for one field: take the incoming value iff it wins last-writer-wins."""
    f = sql.Identifier(field)
    f_at = sql.Identifier(f"{field}_updated_at")
    f_mid = sql.Identifier(f"{field}_message_id")
    wins = sql.SQL(
        "EXCLUDED.{f} IS NOT NULL AND (companies.{f} IS NULL OR"
        " (COALESCE(EXCLUDED.{f_at}, '-infinity'), EXCLUDED.{f_mid} COLLATE \"C\")"
        " > (COALESCE(companies.{f_at}, '-infinity'), COALESCE(companies.{f_mid}, '') COLLATE \"C\"))"
    ).format(f=f, f_at=f_at, f_mid=f_mid)
    return sql.SQL(", ").join(
        sql.SQL("{col} = CASE WHEN {wins} THEN EXCLUDED.{col} ELSE companies.{col} END").format(
            col=col, wins=wins
        )
        for col in (f, f_at, f_mid)
    )


# Per-field last-writer-wins: an incoming field replaces the stored one iff it has a value
# and the stored one is empty or its (event_at, message_id) is smaller. Missing event_at
# counts as -infinity, so an untimed event only fills empty fields. message_ids compare
# in byte order (COLLATE "C") so the tie-break does not depend on the server's locale.
_COLUMNS = ["reg_no", "last_upsert_at"] + [
    f"{field}{suffix}" for field in FIELDS for suffix in ("", "_updated_at", "_message_id")
]
UPSERT_SQL = sql.SQL(
    "INSERT INTO companies ({columns}) VALUES ({values}) ON CONFLICT (reg_no) DO UPDATE SET "
    "last_upsert_at = GREATEST(companies.last_upsert_at, EXCLUDED.last_upsert_at), {fields}"
).format(
    columns=sql.SQL(", ").join(map(sql.Identifier, _COLUMNS)),
    values=sql.SQL(", ").join(map(sql.Placeholder, _COLUMNS)),
    fields=sql.SQL(", ").join(map(_field_assignment, FIELDS)),
)

STRIKE_OFF_SQL = """
    INSERT INTO companies (reg_no, struck_off_at)
    VALUES (%(reg_no)s, COALESCE(%(event_at)s, '-infinity'::timestamptz))
    ON CONFLICT (reg_no) DO UPDATE
    SET struck_off_at = GREATEST(companies.struck_off_at, EXCLUDED.struck_off_at)
"""


async def apply_event(conn: AsyncConnection, event: CompanyEvent, message_id: str) -> None:
    """Store the event for audit and merge it into the company row."""
    await conn.execute(
        "INSERT INTO company_events (message_id, reg_no, type, event_at, payload)"
        " VALUES (%s, %s, %s, %s, %s)",
        (message_id, event.reg_no, event.type, event.event_at, Jsonb(event.payload)),
    )
    if event.type == "company-struck-off":
        await conn.execute(STRIKE_OFF_SQL, {"reg_no": event.reg_no, "event_at": event.event_at})
        return
    params = {"reg_no": event.reg_no, "last_upsert_at": event.event_at}
    for field in FIELDS:
        value = getattr(event.fields, field)
        present = value is not None
        params[field] = value
        params[f"{field}_updated_at"] = event.event_at if present else None
        params[f"{field}_message_id"] = message_id if present else None
    await conn.execute(UPSERT_SQL, params)


async def refresh_embedding(conn: AsyncConnection, reg_no: str, embedder: Embedder) -> None:
    """Make the row's embedding match its current description (or industry fallback)."""
    cur = await conn.execute(
        "SELECT nature, industry, embedding_text_hash, embedding_model, embedding_source"
        " FROM companies WHERE reg_no = %s",
        (reg_no,),
    )
    nature, industry, stored_hash, stored_model, stored_source = await cur.fetchone()
    chosen = embedding_text(nature, industry)
    if chosen is None:
        await conn.execute(
            "UPDATE companies SET embedding = NULL, embedding_source = NULL,"
            " embedding_model = NULL, embedding_text_hash = NULL WHERE reg_no = %s",
            (reg_no,),
        )
        return
    text, source = chosen
    digest = text_hash(text)
    model = embedder.model_name
    if (digest, model, source) == (stored_hash, stored_model, stored_source):
        return

    cur = await conn.execute(
        "SELECT embedding FROM embedding_cache WHERE model = %s AND text_hash = %s", (model, digest)
    )
    cached = await cur.fetchone()
    if cached:
        vector = cached[0]
    else:
        [vector] = await asyncio.to_thread(embedder.embed, [text])
        await conn.execute(
            "INSERT INTO embedding_cache (model, text_hash, embedding) VALUES (%s, %s, %s)"
            " ON CONFLICT DO NOTHING",
            (model, digest, vector),
        )
    await conn.execute(
        "UPDATE companies SET embedding = %s, embedding_source = %s,"
        " embedding_model = %s, embedding_text_hash = %s WHERE reg_no = %s",
        (vector, source, model, digest, reg_no),
    )


async def get_company(conn: AsyncConnection, reg_no: str) -> CompanyRow | None:
    """The company's current state, or None if we have never heard of it."""
    cur = await conn.execute(
        "SELECT reg_no, name, industry, state, active, embedding IS NOT NULL"
        " FROM companies WHERE reg_no = %s",
        (reg_no,),
    )
    row = await cur.fetchone()
    return CompanyRow(*row) if row else None
