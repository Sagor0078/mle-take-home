import json

import httpx
import psycopg
import pytest
import pytest_asyncio
from httpx import ASGITransport

from app import api, store
from app.api import create_app
from app.config import Settings
from app.ingest import handle_message

pytestmark = pytest.mark.usefixtures("clean_db")


def upsert(reg: str, at: str = "2026-01-01T09:00:00+08:00", **data) -> bytes:
    return json.dumps(
        {"type": "company-upserted", "registrationNumber": reg, "updatedAt": at, "data": data}
    ).encode()


def strike(reg: str, at: str) -> bytes:
    return json.dumps(
        {"type": "company-struck-off", "registrationNumber": reg, "updatedAt": at}
    ).encode()


def _settings(database_url: str) -> Settings:
    return Settings(
        database_url=database_url,
        rabbitmq_url="amqp://unused",
        embed_model="fake-hashed-bow",
        prefetch=1,
    )


@pytest_asyncio.fixture
async def client(database_url):
    app = create_app(_settings(database_url))
    # ASGITransport does not run the lifespan, so enter it here.
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=ASGITransport(app), base_url="http://test") as c:
            yield c


async def seed(pool, embedder, reg: str, **data) -> None:
    await handle_message(pool, embedder, upsert(reg, **data), f"seed-{reg}")


async def test_similar_most_similar_first(client, pool, embedder):
    await seed(pool, embedder, "SYN-000001", companyName="Cafe One", natureOfBusiness="coffee shop and cafe")
    await seed(pool, embedder, "SYN-000002", companyName="Steel", natureOfBusiness="steel fabrication works")
    await seed(pool, embedder, "SYN-000003", companyName="Coffee", natureOfBusiness="coffee cafe")
    r = await client.get("/companies/SYN-000001/similar")
    assert r.status_code == 200
    body = r.json()
    assert body["company"]["registrationNumber"] == "SYN-000001"
    assert body["reason"] is None
    assert body["model"] == "fake-hashed-bow"
    assert [c["registrationNumber"] for c in body["similar"]] == ["SYN-000003", "SYN-000002"]
    assert body["similar"][0]["status"] == "active"
    scores = [c["score"] for c in body["similar"]]
    assert scores == sorted(scores, reverse=True)


async def test_self_excluded(client, pool, embedder):
    await seed(pool, embedder, "SYN-000001", natureOfBusiness="coffee cafe")
    await seed(pool, embedder, "SYN-000002", natureOfBusiness="coffee cafe")
    body = (await client.get("/companies/SYN-000001/similar")).json()
    assert "SYN-000001" not in [c["registrationNumber"] for c in body["similar"]]


async def test_struck_off_excluded_from_results(client, pool, embedder):
    await seed(pool, embedder, "SYN-000001", natureOfBusiness="coffee cafe")
    await seed(pool, embedder, "SYN-000002", natureOfBusiness="coffee cafe")
    await seed(pool, embedder, "SYN-000003", natureOfBusiness="coffee shop")
    await handle_message(pool, embedder, strike("SYN-000002", "2026-01-01T10:00:00+08:00"), "s1")
    body = (await client.get("/companies/SYN-000001/similar")).json()
    assert [c["registrationNumber"] for c in body["similar"]] == ["SYN-000003"]


async def test_limit_respected_and_validated(client, pool, embedder):
    for i in range(1, 6):
        await seed(pool, embedder, f"SYN-00000{i}", natureOfBusiness="coffee cafe")
    r = await client.get("/companies/SYN-000001/similar?limit=2")
    assert len(r.json()["similar"]) == 2
    assert len((await client.get("/companies/SYN-000001/similar")).json()["similar"]) == 4
    for bad in (0, 21):
        r = await client.get(f"/companies/SYN-000001/similar?limit={bad}")
        assert r.status_code == 422
        assert r.json()["error"] == "invalid_request"
        assert r.json()["detail"]


async def test_ties_are_deterministic(client, pool, embedder):
    nature = "coffee cafe"
    await seed(pool, embedder, "SYN-000001", natureOfBusiness=nature, industry="Food", state="selangor")
    await seed(pool, embedder, "SYN-000005", natureOfBusiness=nature, industry="Other", state="johor")
    await seed(pool, embedder, "SYN-000003", natureOfBusiness=nature, industry="Other", state="johor")
    await seed(pool, embedder, "SYN-000009", natureOfBusiness=nature, industry="Food", state="selangor")
    body = (await client.get("/companies/SYN-000001/similar")).json()
    assert [c["registrationNumber"] for c in body["similar"]] == ["SYN-000009", "SYN-000003", "SYN-000005"]


async def test_reg_no_normalised_in_path(client, pool, embedder):
    await seed(pool, embedder, "SYN-000001", natureOfBusiness="coffee cafe")
    r = await client.get("/companies/syn-000001/similar")
    assert r.status_code == 200
    assert r.json()["company"]["registrationNumber"] == "SYN-000001"


async def test_unknown_company_404(client):
    for path in ("/companies/SYN-000099", "/companies/SYN-000099/similar"):
        r = await client.get(path)
        assert r.status_code == 404
        assert r.json() == {"error": "company_not_found"}


async def test_invalid_reg_no_422(client):
    for path in ("/companies/hello", "/companies/hello/similar"):
        r = await client.get(path)
        assert r.status_code == 422
        assert r.json() == {"error": "invalid_registration_number"}


async def test_company_card(client, pool, embedder):
    await seed(pool, embedder, "SYN-000001", companyName="Acme", industry="Food", state="selangor")
    r = await client.get("/companies/SYN-000001")
    assert r.json() == {
        "registrationNumber": "SYN-000001",
        "name": "Acme",
        "industry": "Food",
        "state": "Selangor",
        "status": "active",
    }


async def test_struck_off_card_with_messy_reg_no(client, pool, embedder):
    await seed(pool, embedder, "SYN-001748", companyName="Gone", natureOfBusiness="coffee cafe")
    await seed(pool, embedder, "SYN-001749", natureOfBusiness="coffee cafe")
    await handle_message(pool, embedder, strike("SYN-001748", "2026-01-01T10:00:00+08:00"), "s1")
    r = await client.get("/companies/%20syn-001748%20")
    assert r.status_code == 200
    assert r.json()["status"] == "struck_off"
    s = await client.get("/companies/%20syn-001748%20/similar")
    assert s.status_code == 200
    body = s.json()
    assert body["company"]["status"] == "struck_off"
    assert body["similar"] == []
    assert body["reason"] == "struck_off"


async def test_no_embedding_returns_empty_with_reason(client, pool, embedder):
    await seed(pool, embedder, "SYN-000001", companyName="Bare")
    await seed(pool, embedder, "SYN-000002", natureOfBusiness="coffee cafe")
    r = await client.get("/companies/SYN-000001/similar")
    assert r.status_code == 200
    assert r.json()["similar"] == []
    assert r.json()["reason"] == "no_description"


async def test_db_down_returns_503(client, pool, embedder, monkeypatch):
    await seed(pool, embedder, "SYN-000001", natureOfBusiness="coffee cafe")

    async def boom(*args, **kwargs):
        raise psycopg.OperationalError("down")

    monkeypatch.setattr(store, "similar_companies", boom)
    r = await client.get("/companies/SYN-000001/similar")
    assert r.status_code == 503
    assert r.json() == {"error": "database_unavailable"}


async def test_unmigrated_database_returns_503_json(client, pool, embedder, monkeypatch):
    await seed(pool, embedder, "SYN-000001", natureOfBusiness="coffee cafe")

    async def no_table(*args, **kwargs):
        raise psycopg.errors.UndefinedTable('relation "companies" does not exist')

    monkeypatch.setattr(store, "similar_companies", no_table)
    r = await client.get("/companies/SYN-000001/similar")
    assert r.status_code == 503
    assert r.json() == {"error": "database_unavailable"}


async def test_unexpected_error_returns_500_json(database_url, pool, embedder, monkeypatch):
    await seed(pool, embedder, "SYN-000001", natureOfBusiness="coffee cafe")

    async def bug(*args, **kwargs):
        raise RuntimeError("a bug")

    monkeypatch.setattr(store, "similar_companies", bug)
    app = create_app(_settings(database_url))
    # The server re-raises after sending the response (so it gets logged); don't fail the test on that.
    transport = ASGITransport(app, raise_app_exceptions=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            r = await c.get("/companies/SYN-000001/similar")
    assert r.status_code == 500
    assert r.json() == {"error": "internal_error"}


async def test_unknown_route_returns_json_error(client):
    r = await client.get("/no/such/route")
    assert r.status_code == 404
    assert r.json() == {"error": "not_found"}
    r = await client.post("/healthz")
    assert r.status_code == 405
    assert r.json() == {"error": "method_not_allowed"}


async def test_database_down_starts_and_answers_503_quickly(monkeypatch):
    monkeypatch.setattr(api, "API_POOL_TIMEOUT_SECONDS", 0.5)
    app = create_app(_settings("postgresql://postgres:postgres@127.0.0.1:1/nothing"))
    async with app.router.lifespan_context(app):  # start-up must not wait for or fail on the database
        async with httpx.AsyncClient(transport=ASGITransport(app), base_url="http://test") as c:
            assert (await c.get("/healthz")).status_code == 200
            r = await c.get("/readyz")
            assert r.status_code == 503
            assert r.json() == {"status": "not_ready"}
            r = await c.get("/companies/SYN-000001/similar")
            assert r.status_code == 503
            assert r.json() == {"error": "database_unavailable"}


async def test_health_and_readyz(client):
    assert (await client.get("/healthz")).json() == {"status": "ok"}
    r = await client.get("/readyz")
    assert r.status_code == 200
    assert r.json() == {"status": "ready"}
