from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from icalendar import Calendar


def parse_ics_events(ics_text: str) -> list[dict[str, Any]]:
    calendar = Calendar.from_ical(ics_text)
    events: list[dict[str, Any]] = []
    for component in calendar.walk():
        if component.name != "VEVENT":
            continue
        start = component.get("DTSTART")
        end = component.get("DTEND")
        last_modified = component.get("LAST-MODIFIED")
        events.append(
            {
                "uid": str(component.get("UID")),
                "summary": str(component.get("SUMMARY") or ""),
                "description": str(component.get("DESCRIPTION") or ""),
                "categories": [str(v) for v in component.get("CATEGORIES", [])] if component.get("CATEGORIES") else [],
                "starts_at": _ical_datetime(start.dt) if start else None,
                "ends_at": _ical_datetime(end.dt) if end else None,
                "last_modified": _ical_datetime(last_modified.dt) if last_modified else None,
            }
        )
    return events


def _ical_datetime(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    return datetime.combine(value, datetime.min.time(), tzinfo=UTC).isoformat()
