from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo


LOCAL_TIMEZONE = ZoneInfo("America/Argentina/Buenos_Aires")


def to_local(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(LOCAL_TIMEZONE)


def format_local_datetime(value: datetime | None) -> str:
    local = to_local(value)
    if local is None:
        return ""
    return local.strftime("%b %d %H:%M")


def format_local_date(value: datetime | None) -> str:
    local = to_local(value)
    if local is None:
        return ""
    return local.strftime("%b %d")


def format_local_date_time(value: datetime | None) -> str:
    local = to_local(value)
    if local is None:
        return ""
    return local.strftime("%b %d %H:%M")
