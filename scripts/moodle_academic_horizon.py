#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

API_BASE = os.environ.get("MOODLE_TRACKER_API", "http://localhost:8000")
TZ = ZoneInfo("America/Argentina/Buenos_Aires")
HORIZON_DAYS = int(os.environ.get("MOODLE_HORIZON_DAYS", "14"))
EXAM_WORDS = ("parcial", "recuperatorio", "examen")
SCHEDULE_TITLE_WORDS = ("cronograma", "calendario", "horario", "fechas")
DATE_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?(?:\s*(?:de\s*)?(\d{1,2})[:.](\d{2}))?", re.I)


def fetch(path: str):
    with urllib.request.urlopen(f"{API_BASE}{path}", timeout=20) as r:
        return json.load(r)


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def fmt(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%a %d/%m %H:%M")


def course_name(course_map: dict[int, str], course_id) -> str:
    return course_map.get(course_id, f"Course {course_id}")


def looks_like_schedule_doc(item: dict) -> bool:
    title = (item.get("title") or "").lower()
    return item.get("review_reason") == "high_risk_schedule_document" or any(word in title for word in SCHEDULE_TITLE_WORDS)


def schedule_text(item: dict) -> str:
    text = item.get("body_text") or ""
    if text.strip():
        return text
    item_id = item.get("id")
    if item_id is None:
        return ""
    try:
        content = fetch(f"/items/{item_id}/content")
    except Exception:
        return ""
    best = ""
    for artifact in content.get("artifacts") or []:
        candidate = artifact.get("extracted_text") or ""
        if artifact.get("extraction_status") == "completed" and len(candidate) > len(best):
            best = candidate
    return best


def extract_schedule_events(risks: list[dict], now: datetime, horizon_end: datetime) -> list[dict]:
    events: list[dict] = []
    current_year = now.astimezone(TZ).year
    for item in risks:
        if not looks_like_schedule_doc(item):
            continue
        text = schedule_text(item)
        if not any(word in text.lower() for word in EXAM_WORDS):
            continue
        for match in DATE_RE.finditer(text):
            start = max(match.start() - 45, 0)
            end = min(match.end() + 55, len(text))
            context = text[start:end]
            lowered_context = context.lower()
            if not any(word in lowered_context for word in EXAM_WORDS):
                continue
            # Avoid matching neighboring class/consultation dates just because an exam
            # appears later in the same dense schedule row.
            before = text[max(match.start() - 35, 0):match.start()].lower()
            after = text[match.end():min(match.end() + 45, len(text))].lower()
            if not any(word in after for word in EXAM_WORDS):
                continue
            after_lower = after.lower()
            exam_positions = [after_lower.find(word) for word in EXAM_WORDS if word in after_lower]
            if exam_positions:
                before_exam_word = after[:min(exam_positions)]
                if DATE_RE.search(before_exam_word):
                    continue
            day = int(match.group(1))
            month = int(match.group(2))
            year = int(match.group(3)) if match.group(3) else current_year
            if year < 100:
                year += 2000
            hour = int(match.group(4) or 0)
            minute = int(match.group(5) or 0)
            try:
                local_dt = datetime(year, month, day, hour, minute, tzinfo=TZ)
            except ValueError:
                continue
            dt = local_dt.astimezone(timezone.utc)
            if now <= dt <= horizon_end:
                label = " ".join(context.split())
                events.append({
                    "at": dt.isoformat().replace("+00:00", "Z"),
                    "course_id": item.get("course_id"),
                    "title": item.get("title"),
                    "context": label[:220],
                    "url": item.get("primary_url"),
                })
    deduped = {}
    for event in events:
        key = (event["course_id"], event["at"], event["context"][:80])
        deduped.setdefault(key, event)
    return sorted(deduped.values(), key=lambda e: (e["at"], e.get("course_id") or 0))


def main() -> int:
    now = datetime.now(timezone.utc)
    horizon_end = now + timedelta(days=HORIZON_DAYS)
    health = fetch("/health/details")
    courses = fetch("/courses")
    course_map = {c["id"]: c["display_name"] for c in courses}
    deadlines = fetch("/deadlines/upcoming")
    risks = fetch("/risks")

    upcoming_deadlines = []
    for item in deadlines:
        if item.get("completion_state") == "completed":
            continue
        due = parse_dt(item.get("due_at"))
        if due and now <= due <= horizon_end:
            upcoming_deadlines.append(item)
    upcoming_deadlines.sort(key=lambda i: (i.get("due_at") or "", i.get("id") or 0))

    schedule_events = extract_schedule_events(risks, now, horizon_end)

    lines = []
    if health.get("details", {}).get("stale_collectors") or health.get("details", {}).get("source_auth_health") != "healthy":
        lines.append("Tracker health needs attention: " + json.dumps(health.get("details", {}), ensure_ascii=False))

    if upcoming_deadlines:
        lines.append(f"Deadlines in next {HORIZON_DAYS} days:")
        for item in upcoming_deadlines[:8]:
            due = parse_dt(item.get("due_at"))
            lines.append(f"• {fmt(due)} — {course_name(course_map, item.get('course_id'))} — {item.get('title')} ({item.get('completion_state')})")
    else:
        lines.append(f"No incomplete Moodle deadlines in next {HORIZON_DAYS} days.")

    if schedule_events:
        lines.append("Schedule/exam watch:")
        for event in schedule_events[:8]:
            at = parse_dt(event["at"])
            lines.append(f"• {fmt(at)} — {course_name(course_map, event.get('course_id'))} — {event['context']}")

    payload = {
        "send_update": bool(upcoming_deadlines or schedule_events or health.get("details", {}).get("stale_collectors")),
        "horizon_days": HORIZON_DAYS,
        "deadline_count": len(upcoming_deadlines),
        "schedule_event_count": len(schedule_events),
        "lines": lines,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
