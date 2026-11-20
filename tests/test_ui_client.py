import httpx

from ui.client import ApiClient


def client_with(handler) -> ApiClient:
    return ApiClient("http://api.test", transport=httpx.MockTransport(handler))


def test_similar_calls_the_endpoint_with_limit():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"company": {}, "similar": [], "reason": None, "model": "m"})

    result = client_with(handler).similar("SYN-001550", limit=6)
    assert seen["url"] == "http://api.test/companies/SYN-001550/similar?limit=6"
    assert result.ok and result.status == 200
    assert result.body["model"] == "m"
    assert result.elapsed_ms >= 0


def test_error_body_is_kept():
    result = client_with(lambda r: httpx.Response(404, json={"error": "company_not_found"})).card("SYN-999999")
    assert not result.ok
    assert (result.status, result.body) == (404, {"error": "company_not_found"})


def test_reg_no_is_url_encoded():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.raw_path
        return httpx.Response(200, json={})

    client_with(handler).card(" syn-1/2 ")
    assert seen["path"] == b"/companies/%20syn-1%2F2%20"


def test_unreachable_api_is_a_result_not_an_exception():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    result = client_with(handler).health()
    assert not result.ok
    assert result.status is None
    assert "refused" in result.body["error"]


def test_non_json_body_is_returned_as_text():
    result = client_with(lambda r: httpx.Response(500, text="Internal Server Error")).ready()
    assert result.body == {"text": "Internal Server Error"}


def test_raw_get_for_edge_cases():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(404, json={"error": "not_found"})

    result = client_with(handler).get("/nope")
    assert seen["url"] == "http://api.test/nope"
    assert result.status == 404
