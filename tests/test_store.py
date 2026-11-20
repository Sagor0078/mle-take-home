import asyncio
import json

import numpy as np
import pytest

from app.ingest import handle_message
from app.store import get_company

pytestmark = pytest.mark.usefixtures("clean_db")

REG = "SYN-000001"


def msg(reg: str, at: str | None, type_: str = "company-upserted", **data) -> bytes:
    """A feed message; `at=None` leaves out updatedAt. Keyword args become `data`."""
    body: dict = {"type": type_, "registrationNumber": reg}
    if at is not None:
        body["updatedAt"] = at
    if type_ == "company-upserted":
        body["data"] = data
    return json.dumps(body).encode()


def struck(reg: str, at: str | None) -> bytes:
    return msg(reg, at, type_="company-struck-off")


def at(hhmm: str) -> str:
    return f"2026-01-01T{hhmm}:00+08:00"


async def scalar(pool, query: str, *params):
    async with pool.connection() as conn:
        cur = await conn.execute(query, params)
        row = await cur.fetchone()
        return row[0]


async def company(pool, reg: str = REG):
    async with pool.connection() as conn:
        return await get_company(conn, reg)


async def test_first_upsert_creates_company(pool, embedder):
    body = msg(REG, at("09:00"), companyName="Acme", natureOfBusiness="Cafes",
               industry="Food & Beverage", state="selangor")
    assert await handle_message(pool, embedder, body, "m1") == "applied"
    row = await company(pool)
    assert row.name == "Acme"
    assert row.industry == "Food & Beverage"
    assert row.state == "Selangor"
    assert row.active is True
    assert row.has_embedding is True


async def test_duplicate_message_id_is_noop(pool, embedder):
    body = msg(REG, at("09:00"), companyName="Acme")
    assert await handle_message(pool, embedder, body, "m1") == "applied"
    assert await handle_message(pool, embedder, body, "m1") == "duplicate"
    assert await scalar(pool, "SELECT count(*) FROM company_events") == 1


async def test_missing_message_id_falls_back_to_body_hash(pool, embedder):
    body = msg(REG, at("09:00"), companyName="Acme")
    assert await handle_message(pool, embedder, body, None) == "applied"
    assert await handle_message(pool, embedder, body, "") == "duplicate"


async def test_invalid_message_recorded_and_acked(pool, embedder):
    assert await handle_message(pool, embedder, b"null", "bad") == "rejected"
    assert await scalar(pool, "SELECT reason FROM rejected_messages WHERE message_id = 'bad'") == "not_an_object"


async def test_rejected_duplicate_is_duplicate(pool, embedder):
    assert await handle_message(pool, embedder, b"{not json", "bad") == "rejected"
    assert await handle_message(pool, embedder, b"{not json", "bad") == "duplicate"
    assert await scalar(pool, "SELECT count(*) FROM rejected_messages") == 1


async def nature_of(pool, reg: str = REG):
    return await scalar(pool, "SELECT nature FROM companies WHERE reg_no = %s", reg)


async def test_older_event_does_not_overwrite(pool, embedder):
    await handle_message(pool, embedder, msg(REG, at("10:00"), natureOfBusiness="B"), "m1")
    await handle_message(pool, embedder, msg(REG, at("09:00"), natureOfBusiness="A"), "m2")
    assert await nature_of(pool) == "B"


async def test_older_full_event_fills_fields_newer_partial_left_empty(pool, embedder):
    await handle_message(pool, embedder, msg(REG, at("10:00"), companyName="New"), "m1")
    await handle_message(pool, embedder, msg(REG, at("09:00"), companyName="Old",
                                              natureOfBusiness="Cafes", industry="Food & Beverage"), "m2")
    row = await company(pool)
    assert row.name == "New"
    assert await nature_of(pool) == "Cafes"
    assert row.industry == "Food & Beverage"


async def test_missing_timestamp_never_overwrites(pool, embedder):
    await handle_message(pool, embedder, msg(REG, at("09:00"), companyName="Real"), "m1")
    await handle_message(pool, embedder, msg(REG, None, companyName="Other"), "m2")
    assert (await company(pool)).name == "Real"


async def test_missing_timestamp_fills_empty_field(pool, embedder):
    await handle_message(pool, embedder, msg(REG, None, industry="Retail"), "m1")
    assert (await company(pool)).industry == "Retail"


@pytest.mark.parametrize("order", [("a", "b"), ("b", "a")])
async def test_tie_broken_by_message_id(pool, embedder, order):
    names = {"a": "A", "b": "B"}
    for mid in order:
        await handle_message(pool, embedder, msg(REG, at("09:00"), companyName=names[mid]), mid)
    assert (await company(pool)).name == "B"


async def test_strike_off_hides_company(pool, embedder):
    await handle_message(pool, embedder, msg(REG, at("09:00"), companyName="Acme"), "m1")
    await handle_message(pool, embedder, struck(REG, at("10:00")), "m2")
    assert (await company(pool)).active is False


async def test_stale_upsert_does_not_resurrect(pool, embedder):
    await handle_message(pool, embedder, msg(REG, at("09:00"), companyName="Acme"), "m1")
    await handle_message(pool, embedder, struck(REG, at("10:00")), "m2")
    await handle_message(pool, embedder, msg(REG, at("09:30"), state="Johor"), "m3")
    assert (await company(pool)).active is False


async def test_newer_upsert_revives(pool, embedder):
    await handle_message(pool, embedder, msg(REG, at("09:00"), companyName="Acme"), "m1")
    await handle_message(pool, embedder, struck(REG, at("10:00")), "m2")
    await handle_message(pool, embedder, msg(REG, at("11:00"), state="Johor"), "m3")
    assert (await company(pool)).active is True


async def test_strike_off_before_any_upsert(pool, embedder):
    await handle_message(pool, embedder, struck(REG, at("10:00")), "m1")
    await handle_message(pool, embedder, msg(REG, at("09:00"), companyName="Acme"), "m2")
    row = await company(pool)
    assert row is not None
    assert row.active is False


async def embedding_source(pool, reg: str = REG):
    return await scalar(pool, "SELECT embedding_source FROM companies WHERE reg_no = %s", reg)


async def test_fallback_embedding_when_no_description(pool, embedder):
    await handle_message(pool, embedder, msg(REG, at("09:00"), industry="Retail"), "m1")
    assert (await company(pool)).has_embedding is True
    assert await embedding_source(pool) == "fallback"


async def test_real_description_replaces_fallback(pool, embedder):
    await handle_message(pool, embedder, msg(REG, at("10:00"), industry="Retail"), "m1")
    await handle_message(pool, embedder, msg(REG, at("09:00"), natureOfBusiness="Shoe shop"), "m2")
    assert await embedding_source(pool) == "nature"


async def test_no_embedding_without_description_or_industry(pool, embedder):
    await handle_message(pool, embedder, msg(REG, at("09:00"), companyName="Acme"), "m1")
    assert (await company(pool)).has_embedding is False


async def test_embedding_follows_final_description(pool, embedder):
    await handle_message(pool, embedder, msg(REG, at("09:00"), natureOfBusiness="Cafes"), "m1")
    await handle_message(pool, embedder, msg(REG, at("10:00"), natureOfBusiness="Bakeries"), "m2")
    await handle_message(pool, embedder, msg(REG, at("11:00"), natureOfBusiness="Cafes"), "m3")
    stored = await scalar(pool, "SELECT embedding FROM companies WHERE reg_no = %s", REG)
    np.testing.assert_allclose(stored.to_numpy(), embedder.embed(["Cafes"])[0], rtol=1e-6)
    assert await scalar(pool, "SELECT count(*) FROM embedding_cache") == 2


async def test_embedding_cache_shared(pool, embedder):
    await handle_message(pool, embedder, msg("SYN-000001", at("09:00"), natureOfBusiness="Cafes"), "m1")
    await handle_message(pool, embedder, msg("SYN-000002", at("09:00"), natureOfBusiness="Cafes"), "m2")
    assert await scalar(pool, "SELECT count(*) FROM embedding_cache") == 1


async def test_concurrent_duplicate_applies_once(pool, embedder):
    body = msg(REG, at("09:00"), companyName="Acme", natureOfBusiness="Cafes")
    outcomes = await asyncio.gather(*(handle_message(pool, embedder, body, "same") for _ in range(5)))
    assert sorted(outcomes) == ["applied"] + ["duplicate"] * 4
    assert await scalar(pool, "SELECT count(*) FROM company_events") == 1


@pytest.mark.parametrize("order", [("industry", "nature"), ("nature", "industry")])
async def test_embedding_source_order_independent(pool, embedder, order):
    # The description cleans to the same text as the industry fallback ("Retail business").
    bodies = {
        "industry": msg(REG, at("09:00"), industry="Retail"),
        "nature": msg(REG, at("10:00"), natureOfBusiness="Retail business"),
    }
    for mid in order:
        await handle_message(pool, embedder, bodies[mid], mid)
    assert await embedding_source(pool) == "nature"


async def test_struck_off_without_timestamp(pool, embedder):
    await handle_message(pool, embedder, struck(REG, None), "m1")
    assert await scalar(pool, "SELECT struck_off_at = '-infinity' FROM companies WHERE reg_no = %s", REG)
    assert (await company(pool)).active is False
    await handle_message(pool, embedder, msg(REG, None, companyName="Acme"), "m2")
    assert (await company(pool)).active is False
    await handle_message(pool, embedder, msg(REG, at("09:00"), state="Johor"), "m3")
    assert (await company(pool)).active is True


async def test_upsert_without_fields_creates_row(pool, embedder):
    body = json.dumps({"type": "company-upserted", "registrationNumber": REG, "data": None}).encode()
    assert await handle_message(pool, embedder, body, "m1") == "applied"
    assert await company(pool) is not None
    assert await scalar(pool, "SELECT last_upsert_at FROM companies WHERE reg_no = %s", REG) is None


@pytest.mark.parametrize("order", [("a", "B"), ("B", "a")])
async def test_tie_break_uses_byte_order(pool, embedder, order):
    # Byte order ("C" collation) puts "a" after "B"; a locale collation would not.
    for mid in order:
        await handle_message(pool, embedder, msg(REG, at("09:00"), companyName=f"from {mid}"), mid)
    assert (await company(pool)).name == "from a"
