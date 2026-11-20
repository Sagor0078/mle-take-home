"""Parse and normalise registry events. Pure functions: no I/O."""

import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import Literal

log = logging.getLogger(__name__)

EventType = Literal["company-upserted", "company-struck-off"]
EVENT_TYPES = ("company-upserted", "company-struck-off")

REASONS = (
    "invalid_json",
    "not_an_object",
    "unknown_type",
    "missing_registration_number",
    "invalid_registration_number",
    "invalid_updated_at",
    "invalid_data",
)

CANONICAL_STATES = (
    "Johor", "Kedah", "Kelantan", "Melaka", "Negeri Sembilan", "Pahang", "Perak", "Perlis",
    "Pulau Pinang", "Sabah", "Sarawak", "Selangor", "Terengganu", "Kuala Lumpur", "Labuan", "Putrajaya",
)
_STATES_BY_LOWER = {state.lower(): state for state in CANONICAL_STATES}

MALAYSIA_TZ = timezone(timedelta(hours=8))
MILLISECOND_THRESHOLD = 10**11  # epoch numbers at or above this are milliseconds
REG_NO_PATTERN = re.compile(r"SYN-[0-9]{6}")


class InvalidEvent(Exception):
    """The message cannot be turned into an event; `reason` is one of REASONS."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class CompanyFields:
    name: str | None = None
    nature: str | None = None
    industry: str | None = None
    state: str | None = None


@dataclass(frozen=True)
class CompanyEvent:
    type: EventType
    reg_no: str
    event_at: datetime | None  # tz-aware; None means the event had no updatedAt
    fields: CompanyFields  # all None for struck-off events or empty data
    payload: dict  # the parsed JSON object, stored in company_events.payload


def normalise_reg_no(value: object) -> str:
    """Return the registration number as 'SYN-000123' or raise InvalidEvent."""
    if value is None or (isinstance(value, str) and not value.strip()):
        raise InvalidEvent("missing_registration_number")
    if isinstance(value, bool):
        raise InvalidEvent("invalid_registration_number")
    if isinstance(value, int):
        text = f"SYN-{value:06d}"
    elif isinstance(value, str):
        text = value.strip().upper()
    else:
        raise InvalidEvent("invalid_registration_number")
    if not REG_NO_PATTERN.fullmatch(text):
        raise InvalidEvent("invalid_registration_number")
    return text


def parse_timestamp(value: object) -> datetime | None:
    """Return a tz-aware datetime, None if value is None, or raise InvalidEvent."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise InvalidEvent("invalid_updated_at")
    try:
        if isinstance(value, str) and value.isascii() and value.isdigit():
            value = int(value)  # ValueError for absurdly long digit strings
        if isinstance(value, (int, float)):
            seconds = value / 1000 if abs(value) >= MILLISECOND_THRESHOLD else value
            return datetime.fromtimestamp(seconds, UTC)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.strip())
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=MALAYSIA_TZ)
    except (ValueError, OverflowError, OSError):
        pass
    raise InvalidEvent("invalid_updated_at")


def normalise_state(value: str | None) -> str | None:
    """Map a state to its canonical spelling; unknown states are kept, title-cased."""
    if value is None:
        return None
    canonical = _STATES_BY_LOWER.get(value.lower())
    if canonical:
        return canonical
    log.warning("unknown state %r kept as given", value)
    return value.title()


def _clean_text(value: object) -> str | None:
    """Strip and collapse whitespace; non-strings and blanks are absent (None)."""
    if not isinstance(value, str):
        return None
    return " ".join(value.split()) or None


def _parse_fields(data: object) -> CompanyFields:
    if data is None:
        return CompanyFields()
    if not isinstance(data, dict):
        raise InvalidEvent("invalid_data")
    name = _clean_text(data.get("companyName"))
    if name == "Unknown":
        name = None
    return CompanyFields(
        name=name,
        nature=_clean_text(data.get("natureOfBusiness")),
        industry=_clean_text(data.get("industry")),
        state=normalise_state(_clean_text(data.get("state"))),
    )


def parse_event(body: bytes) -> CompanyEvent:
    """Parse a raw message body into a CompanyEvent or raise InvalidEvent."""
    try:
        payload = json.loads(body.decode("utf-8"), strict=False)
    except (ValueError, RecursionError):  # includes UnicodeDecodeError, JSONDecodeError, too-deep nesting
        raise InvalidEvent("invalid_json") from None
    if not isinstance(payload, dict):
        raise InvalidEvent("not_an_object")
    event_type = payload.get("type")
    if event_type not in EVENT_TYPES:
        raise InvalidEvent("unknown_type")
    reg_no = normalise_reg_no(payload.get("registrationNumber"))
    event_at = parse_timestamp(payload.get("updatedAt"))
    if event_type == "company-upserted":
        fields = _parse_fields(payload.get("data"))
    else:
        fields = CompanyFields()
    return CompanyEvent(event_type, reg_no, event_at, fields, payload)
