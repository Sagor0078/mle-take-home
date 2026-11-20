"""HTTP API: company cards and similar-company recommendations."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Annotated, Literal

import psycopg
from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg import AsyncConnection
from psycopg_pool import PoolTimeout
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel
from starlette.exceptions import HTTPException as StarletteHTTPException

from app import store
from app.config import Settings
from app.db import make_pool
from app.events import InvalidEvent, normalise_reg_no
from app.store import CompanyRow, SimilarRow

# A request waits at most this long for a database connection, then gets a 503.
API_POOL_TIMEOUT_SECONDS = 5.0


class ApiModel(BaseModel):
    """Base for response bodies: camelCase JSON, like the feed."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class CompanyCard(ApiModel):
    registration_number: str
    name: str | None
    industry: str | None
    state: str | None
    status: Literal["active", "struck_off"]


class SimilarCard(CompanyCard):
    score: float


class SimilarResponse(ApiModel):
    company: CompanyCard
    similar: list[SimilarCard]
    reason: Literal["struck_off", "no_description"] | None
    model: str


class ApiError(Exception):
    """An expected failure that becomes a JSON body {"error": <code>}."""

    def __init__(self, status_code: int, error: str):
        self.status_code = status_code
        self.error = error


def card(row: CompanyRow | SimilarRow) -> dict:
    """The card fields shared by CompanyRow and SimilarRow (similar rows are always active)."""
    active = getattr(row, "active", True)
    return {
        "registration_number": row.reg_no,
        "name": row.name,
        "industry": row.industry,
        "state": row.state,
        "status": "active" if active else "struck_off",
    }


async def get_conn(request: Request) -> AsyncIterator[AsyncConnection]:
    async with request.app.state.pool.connection() as conn:
        yield conn


Conn = Annotated[AsyncConnection, Depends(get_conn)]


async def get_known_company(reg_no: str, conn: Conn) -> CompanyRow:
    """Normalise the path's reg_no and load the company; 422 if malformed, 404 if unknown."""
    try:
        reg_no = normalise_reg_no(reg_no)
    except InvalidEvent:
        raise ApiError(422, "invalid_registration_number")
    company = await store.get_company(conn, reg_no)
    if company is None:
        raise ApiError(404, "company_not_found")
    return company


KnownCompany = Annotated[CompanyRow, Depends(get_known_company)]


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # wait=False: start (and answer /healthz, /readyz) even while the database is down.
        app.state.pool = await make_pool(
            settings.database_url, timeout=API_POOL_TIMEOUT_SECONDS, wait=False
        )
        try:
            yield
        finally:
            await app.state.pool.close()

    app = FastAPI(title="Similar companies", lifespan=lifespan)

    @app.exception_handler(ApiError)
    async def api_error(request: Request, exc: ApiError):
        return JSONResponse({"error": exc.error}, status_code=exc.status_code)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, exc: RequestValidationError):
        detail = [{k: v for k, v in e.items() if k in ("loc", "msg", "type")} for e in exc.errors()]
        return JSONResponse({"error": "invalid_request", "detail": detail}, status_code=422)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, exc: StarletteHTTPException):
        # Unknown route (404), wrong method (405), ...: {"error": "not_found"} etc.
        error = HTTPStatus(exc.status_code).phrase.lower().replace(" ", "_")
        return JSONResponse({"error": error}, status_code=exc.status_code, headers=exc.headers)

    # Any database error: down, pool timeout, or not migrated yet (UndefinedTable).
    @app.exception_handler(psycopg.Error)
    @app.exception_handler(PoolTimeout)
    async def database_unavailable(request: Request, exc: Exception):
        return JSONResponse({"error": "database_unavailable"}, status_code=503)

    @app.exception_handler(Exception)
    async def internal_error(request: Request, exc: Exception):
        # Starlette re-raises the exception after sending this response, so uvicorn logs the traceback.
        return JSONResponse({"error": "internal_error"}, status_code=500)

    @app.get("/companies/{reg_no}", response_model=CompanyCard)
    async def company_card(company: KnownCompany):
        return card(company)

    @app.get("/companies/{reg_no}/similar", response_model=SimilarResponse)
    async def similar(
        company: KnownCompany, conn: Conn, limit: Annotated[int, Query(ge=1, le=20)] = 4
    ):
        reason = None
        rows: list[SimilarRow] = []
        if not company.active:
            reason = "struck_off"
        elif not company.has_embedding:
            reason = "no_description"
        else:
            rows = await store.similar_companies(conn, company.reg_no, limit)
        return {
            "company": card(company),
            "similar": [card(row) | {"score": row.score} for row in rows],
            "reason": reason,
            "model": settings.embed_model,
        }

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz(request: Request):
        try:
            async with request.app.state.pool.connection() as conn:
                cur = await conn.execute(
                    "SELECT 1 FROM schema_migrations WHERE name = '001_init.sql'"
                )
                ready = await cur.fetchone() is not None
        except (psycopg.Error, PoolTimeout):
            ready = False
        if not ready:
            return JSONResponse({"status": "not_ready"}, status_code=503)
        return {"status": "ready"}

    return app


app = create_app()
