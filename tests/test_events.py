import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.events import (
    CANONICAL_STATES,
    CompanyFields,
    InvalidEvent,
    normalise_reg_no,
    normalise_state,
    parse_event,
    parse_timestamp,
)

MYT = timezone(timedelta(hours=8))
FEED = Path(__file__).parent.parent / "data" / "feed.jsonl"


def event(**overrides) -> bytes:
    body = {
        "type": "company-upserted",
        "registrationNumber": "SYN-000123",
        "updatedAt": "2026-08-14T09:00:00+08:00",
        "data": {"companyName": "Foo Sdn. Bhd."},
    }
    body.update(overrides)
    return json.dumps(body).encode()


def reason_of(body: bytes) -> str:
    with pytest.raises(InvalidEvent) as exc:
        parse_event(body)
    return exc.value.reason


@pytest.mark.parametrize(
    "raw,expected",
    [("SYN-000123", "SYN-000123"), (" SYN-000123 ", "SYN-000123"), ("syn-000123", "SYN-000123"), (119, "SYN-000119")],
)
def test_normalise_reg_no(raw, expected):
    assert normalise_reg_no(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_missing_reg_no(raw):
    with pytest.raises(InvalidEvent) as exc:
        normalise_reg_no(raw)
    assert exc.value.reason == "missing_registration_number"


@pytest.mark.parametrize("raw", ["ABC-1", "SYN-12", True, 1.5, 1234567])
def test_invalid_reg_no(raw):
    with pytest.raises(InvalidEvent) as exc:
        normalise_reg_no(raw)
    assert exc.value.reason == "invalid_registration_number"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("2026-08-14T09:00:00+08:00", datetime(2026, 8, 14, 9, 0, tzinfo=MYT)),
        ("2026-08-14T01:00:00Z", datetime(2026, 8, 14, 9, 0, tzinfo=MYT)),
        ("2026-08-14T09:00:00", datetime(2026, 8, 14, 9, 0, tzinfo=MYT)),
        (1786671038000, datetime.fromtimestamp(1786671038, UTC)),
        (1786671038, datetime.fromtimestamp(1786671038, UTC)),
        ("1786671038000", datetime.fromtimestamp(1786671038, UTC)),
        (None, None),
    ],
)
def test_parse_timestamp(raw, expected):
    result = parse_timestamp(raw)
    assert result == expected
    if expected is not None:
        assert result.tzinfo is not None


@pytest.mark.parametrize("raw", ["yesterday", True, "", [1]])
def test_garbage_timestamp_rejected(raw):
    with pytest.raises(InvalidEvent) as exc:
        parse_timestamp(raw)
    assert exc.value.reason == "invalid_updated_at"


def test_garbage_timestamp_in_event():
    assert reason_of(event(updatedAt="yesterday")) == "invalid_updated_at"


def test_truncated_json():
    line = FEED.read_text().splitlines()[97]
    assert reason_of(line.encode()) == "invalid_json"


def test_invalid_utf8():
    assert reason_of(b"\xff\xfe{}") == "invalid_json"


def test_control_char_inside_string_is_accepted():
    body = b'{"type": "company-upserted", "registrationNumber": "SYN-000001", "data": {"natureOfBusiness": "Rubber\tgloves"}}'
    assert parse_event(body).fields.nature == "Rubber gloves"


@pytest.mark.parametrize("body", [b"null", b"[]", b'"x"', b"42"])
def test_not_an_object(body):
    assert reason_of(body) == "not_an_object"


@pytest.mark.parametrize("value", ["company-deleted", None, 5])
def test_unknown_type(value):
    assert reason_of(event(type=value)) == "unknown_type"


def test_missing_type():
    assert reason_of(b'{"registrationNumber": "SYN-000001"}') == "unknown_type"


def test_missing_reg_no_in_event():
    assert reason_of(event(registrationNumber=None)) == "missing_registration_number"


def test_data_string_rejected():
    assert reason_of(event(data="companyName=Foo; state=Perak")) == "invalid_data"


@pytest.mark.parametrize("data", [[1], 7])
def test_data_non_object_rejected(data):
    assert reason_of(event(data=data)) == "invalid_data"


def test_data_null_is_empty_fields():
    assert parse_event(event(data=None)).fields == CompanyFields()


def test_data_missing_is_empty_fields():
    body = json.loads(event())
    del body["data"]
    assert parse_event(json.dumps(body).encode()).fields == CompanyFields()


def test_missing_updated_at_is_none():
    body = json.loads(event())
    del body["updatedAt"]
    assert parse_event(json.dumps(body).encode()).event_at is None


def test_unknown_name_is_absent():
    assert parse_event(event(data={"companyName": "Unknown"})).fields.name is None


@pytest.mark.parametrize("value", ["", "  ", None])
def test_blank_and_null_fields_are_absent(value):
    data = {"companyName": value, "natureOfBusiness": value, "industry": value, "state": value}
    assert parse_event(event(data=data)).fields == CompanyFields()


def test_whitespace_is_stripped_and_collapsed():
    fields = parse_event(event(data={"companyName": "  Foo   Bar \n Sdn. Bhd. "})).fields
    assert fields.name == "Foo Bar Sdn. Bhd."


def test_state_canonicalised():
    assert normalise_state("KUALA LUMPUR") == "Kuala Lumpur"
    assert normalise_state("kuala lumpur") == "Kuala Lumpur"
    assert normalise_state("pulau pinang") == "Pulau Pinang"
    assert parse_event(event(data={"state": "NEGERI SEMBILAN"})).fields.state == "Negeri Sembilan"


def test_unknown_state_kept_title_cased():
    assert normalise_state("atlantis") == "Atlantis"


def test_state_none():
    assert normalise_state(None) is None


def test_sixteen_canonical_states():
    assert len(CANONICAL_STATES) == 16


def test_extra_keys_ignored_but_kept_in_payload():
    body = event(source="ssm-batch")
    parsed = parse_event(body)
    assert parsed.payload["source"] == "ssm-batch"
    assert parsed.payload == json.loads(body)


def test_full_upsert():
    parsed = parse_event(event(registrationNumber=119, data={
        "companyName": "Foo", "natureOfBusiness": "Gloves", "industry": "Manufacturing", "state": "perak"}))
    assert parsed.type == "company-upserted"
    assert parsed.reg_no == "SYN-000119"
    assert parsed.event_at == datetime(2026, 8, 14, 9, 0, tzinfo=MYT)
    assert parsed.fields == CompanyFields("Foo", "Gloves", "Manufacturing", "Perak")


def test_struck_off_has_empty_fields():
    parsed = parse_event(event(type="company-struck-off", data={"companyName": "Foo"}))
    assert parsed.type == "company-struck-off"
    assert parsed.fields == CompanyFields()


def test_non_string_field_is_absent():
    assert parse_event(event(data={"natureOfBusiness": 42})).fields.nature is None
