"""Streamlit UI for trying the similar-companies API by hand.

Run with `just ui` (the API must be running: `just serve`). It renders the portal's
"Similar companies" panel from the API's JSON, lets you click through to a neighbour, and has an
edge-case tab that fires the error paths (404, 422, struck off, no description, unknown route).
"""

import os

import streamlit as st

from ui.client import ApiClient, Result

EXAMPLES = {
    "SYN-001550": "coffee shop chain in Selangor (the mock in the brief)",
    "SYN-000861": "catering company with a Malay description",
    "SYN-001748": "struck off: card shown, no recommendations",
    "SYN-000048": "no description and no industry: no embedding",
    " syn-001550 ": "messy input: lowercase with spaces, normalised by the API",
}

# (name, path, query params, expected status, expected "error" or "reason" in the body)
EDGE_CASES = [
    ("Unknown company", "/companies/SYN-999999/similar", None, 404, "company_not_found"),
    ("Malformed registration number", "/companies/hello/similar", None, 422, "invalid_registration_number"),
    ("limit = 0", "/companies/SYN-001550/similar", {"limit": 0}, 422, "invalid_request"),
    ("limit = 21", "/companies/SYN-001550/similar", {"limit": 21}, 422, "invalid_request"),
    ("Struck-off company", "/companies/SYN-001748/similar", None, 200, "struck_off"),
    ("No description", "/companies/SYN-000048/similar", None, 200, "no_description"),
    ("Unknown route", "/nope", None, 404, "not_found"),
]


def default_api_url() -> str:
    return os.environ.get("API_URL") or f"http://localhost:{os.environ.get('API_PORT', '8000')}"


def status_badge(result: Result) -> str:
    if result.status is None:
        return ":red[unreachable]"
    colour = "green" if result.ok else "red"
    return f":{colour}[{result.status}] · {result.elapsed_ms:.0f} ms"


def show_card(company: dict, score: float | None = None, key: str | None = None) -> None:
    """One company card, like the portal's: name, industry · state, reg no, optional score."""
    with st.container(border=True):
        st.markdown(f"**{company.get('name') or company['registrationNumber']}**")
        where = " · ".join(part for part in (company.get("industry"), company.get("state")) if part)
        st.caption(f"{where or 'no industry or state'} · `{company['registrationNumber']}`")
        if company.get("status") == "struck_off":
            st.warning("Struck off")
        if score is not None:
            st.progress(max(0.0, min(1.0, score)), text=f"similarity {score:.3f}")
        if key:
            st.button("Open", key=key, on_click=open_company, args=(company["registrationNumber"],))


def open_company(reg_no: str) -> None:
    """Navigate to a company, as clicking a card does on the portal (a button callback: it runs
    before the page re-renders, which is when Streamlit allows changing the search box)."""
    st.session_state.reg_no = reg_no


def similar_tab(api: ApiClient, limit: int) -> None:
    st.session_state.setdefault("reg_no", "SYN-001550")

    cols = st.columns(len(EXAMPLES))
    for col, (reg_no, description) in zip(cols, EXAMPLES.items()):
        col.button(reg_no, help=description, key=f"ex-{reg_no}", use_container_width=True,
                   on_click=open_company, args=(reg_no,))

    reg_no = st.text_input("Registration number", key="reg_no")
    if not reg_no:
        return

    result = api.similar(reg_no, limit=limit)
    st.markdown(f"`GET /companies/{reg_no}/similar?limit={limit}` → {status_badge(result)}")

    if not result.ok:
        st.error(result.body)
        return

    body = result.body
    show_card(body["company"])
    st.subheader("Similar companies")
    if body.get("reason"):
        st.info(f"No recommendations: `{body['reason']}`")
    elif not body["similar"]:
        st.info("No similar companies returned.")
    else:
        per_row = 2
        rows = [body["similar"][i : i + per_row] for i in range(0, len(body["similar"]), per_row)]
        for r, row in enumerate(rows):
            for c, (col, item) in enumerate(zip(st.columns(per_row), row)):
                with col:
                    show_card(item, score=item.get("score"), key=f"open-{r}-{c}-{item['registrationNumber']}")
    st.caption(f"Embedding model: `{body.get('model')}`")
    with st.expander("Raw JSON"):
        st.json(body)


def edge_cases_tab(api: ApiClient) -> None:
    st.write("Each row calls the API and shows what came back next to what the API contract says.")
    if st.button("Run all"):
        for name, path, params, status, code in EDGE_CASES:
            result = api.get(path, params=params)
            got = result.body.get("error") or result.body.get("reason")
            passed = result.status == status and got == code
            query = "?" + "&".join(f"{k}={v}" for k, v in params.items()) if params else ""
            with st.container(border=True):
                st.markdown(f"{'✅' if passed else '❌'} **{name}** · `GET {path}{query}`")
                st.markdown(f"expected: {status} `{code}` · got: {result.status} `{got}` · {result.elapsed_ms:.0f} ms")
                st.json(result.body, expanded=False)


def main() -> None:
    st.set_page_config(page_title="Similar companies", layout="wide")
    st.title("Similar companies: API test UI")

    with st.sidebar:
        api_url = st.text_input("API URL", value=default_api_url())
        limit = st.slider("limit", min_value=1, max_value=20, value=4)
        api = ApiClient(api_url)
        st.markdown(f"**/healthz** {status_badge(api.health())}")
        ready = api.ready()
        st.markdown(f"**/readyz** {status_badge(ready)}")
        if not ready.ok:
            st.caption("Not ready: start the API (`just serve`) and migrate (`just migrate` or `just consume`).")

    similar, edges = st.tabs(["Similar companies", "Edge cases"])
    with similar:
        similar_tab(api, limit)
    with edges:
        edge_cases_tab(api)


main()
