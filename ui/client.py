"""A thin HTTP client for the similar-companies API, used by the Streamlit test UI.

Every call returns a Result instead of raising, so the UI can show errors (404, 422, 503,
connection refused) exactly as the portal would receive them.
"""

import time
from dataclasses import dataclass
from urllib.parse import quote

import httpx


@dataclass(frozen=True)
class Result:
    status: int | None  # None when the API could not be reached
    body: dict
    elapsed_ms: float

    @property
    def ok(self) -> bool:
        return self.status == 200


class ApiClient:
    def __init__(self, base_url: str, timeout: float = 10.0, transport: httpx.BaseTransport | None = None):
        self._http = httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout, transport=transport)

    def get(self, path: str, params: dict | None = None) -> Result:
        """GET any path and return status, JSON body (or {"text": ...}) and latency."""
        start = time.perf_counter()
        try:
            response = self._http.get(path, params=params)
        except httpx.HTTPError as exc:
            return Result(None, {"error": f"{type(exc).__name__}: {exc}"}, _ms(start))
        try:
            body = response.json()
        except ValueError:
            body = {"text": response.text}
        return Result(response.status_code, body, _ms(start))

    def card(self, reg_no: str) -> Result:
        return self.get(f"/companies/{_segment(reg_no)}")

    def similar(self, reg_no: str, limit: int = 4) -> Result:
        return self.get(f"/companies/{_segment(reg_no)}/similar", params={"limit": limit})

    def health(self) -> Result:
        return self.get("/healthz")

    def ready(self) -> Result:
        return self.get("/readyz")


def _segment(reg_no: str) -> str:
    """Encode a registration number as one path segment (spaces and slashes included)."""
    return quote(reg_no, safe="")


def _ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000
