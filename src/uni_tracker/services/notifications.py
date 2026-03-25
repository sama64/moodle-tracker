from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from uni_tracker.config import get_settings
from uni_tracker.models import Acknowledgement, ItemVersion, NormalizedItem, Notification
from uni_tracker.services.parsing import strip_html
from uni_tracker.services.persistence import ItemChange
from uni_tracker.services.timeutils import format_local_date, format_local_date_time, format_local_datetime
from uni_tracker.services.tools import get_item_course_name


URGENT_KEYWORDS = (
    "parcial",
    "recuperatorio",
    "examen",
    "entrega",
    "vence",
    "deadline",
    "suspendida",
    "cambio de aula",
)
KEYWORD_URGENT_ITEM_TYPES = {"announcement", "forum_discussion", "assignment", "quiz", "calendar_event"}
DIGEST_WINDOW_HOURS = 24
ACTION_WINDOW_DAYS = 14
MAX_SECTION_ITEMS = 5


@dataclass
class DigestLine:
    title: str
    body: str
    url: str | None
    course: str
    sort_key: tuple[int, datetime, str]

    def render(self) -> str:
        parts = [f"- {self.title}"]
        if self.body:
            parts.append(self.body)
        if self.url:
            parts.append(self.url)
        return " ".join(parts)


def schedule_notifications_for_item(
    session: Session,
    item: NormalizedItem,
    change: ItemChange | None = None,
) -> None:
    if item.status != "active":
        return

    now = datetime.now(UTC)
    text = f"{item.title} {item.body_text or ''}".lower()
    urgent = False
    severity = "info"
    reason = "new_or_updated_item"
    payload: dict[str, Any] = {}

    if change is not None and change.change_type in {"deadline_changed", "deadline_removed", "deadline_added"}:
        urgent = True
        severity = "high"
        reason = change.change_type
        payload["changed_fields"] = change.changed_fields
        payload["previous_values"] = {
            "due_at": change.previous_values.get("due_at") if change.previous_values else None,
            "starts_at": change.previous_values.get("starts_at") if change.previous_values else None,
        }
        payload["new_values"] = {
            "due_at": change.new_values.get("due_at") if change.new_values else None,
            "starts_at": change.new_values.get("starts_at") if change.new_values else None,
        }
    elif change is not None and change.change_type in {"schedule_changed", "exam_changed"}:
        urgent = True
        severity = "high" if change.change_type == "exam_changed" else "medium"
        reason = change.change_type
        payload["changed_fields"] = change.changed_fields
    elif change is not None and change.change_type == "date_mentions_changed":
        severity = "info"
        reason = change.change_type

    due_at = _normalize_datetime(item.due_at)
    starts_at = _normalize_datetime(item.starts_at)

    if (not urgent) and due_at and 0 <= (due_at - now).total_seconds() <= 72 * 3600:
        urgent = True
        severity = "high"
        reason = "due_within_72h"
    elif (not urgent) and starts_at and 0 <= (starts_at - now).total_seconds() <= 14 * 24 * 3600:
        if change is not None and change.change_type == "schedule_changed":
            urgent = True
            severity = "high"
            reason = "class_session_changed_within_14d"
    elif item.item_type in KEYWORD_URGENT_ITEM_TYPES and any(keyword in text for keyword in URGENT_KEYWORDS):
        urgent = True
        severity = "medium"
        reason = "high_signal_keywords"
    elif item.review_status == "needs_review":
        severity = "medium"
        reason = item.review_reason or "needs_review"

    if urgent:
        dedup_key = f"urgent:{item.id}:{reason}:{item.field_hash}"
        notification = session.scalar(select(Notification).where(Notification.dedup_key == dedup_key))
        if notification is None:
            payload.update(
                {
                    "reason": reason,
                    "change_type": change.change_type if change else None,
                    "reminder_number": 0,
                    "base_dedup": dedup_key,
                }
            )
            session.add(
                Notification(
                    normalized_item_id=item.id,
                    channel="telegram",
                    severity=severity,
                    kind="urgent",
                    payload=payload,
                    dedup_key=dedup_key,
                    ack_required=True,
                    scheduled_for=now,
                )
            )


def schedule_daily_digest(session: Session) -> None:
    settings = get_settings()
    now = datetime.now(UTC)
    run_hour = settings.daily_digest_hour
    scheduled_for = now.replace(hour=run_hour, minute=0, second=0, microsecond=0)
    dedup_key = f"digest:{scheduled_for.date().isoformat()}"
    existing = session.scalar(select(Notification).where(Notification.dedup_key == dedup_key))
    if existing is None:
        anchor_item_id = session.scalar(select(NormalizedItem.id).limit(1))
        if anchor_item_id is None:
            return
        if scheduled_for < now:
            scheduled_for = now
        session.add(
            Notification(
                normalized_item_id=anchor_item_id,
                channel="telegram",
                severity="info",
                kind="digest",
                payload={},
                dedup_key=dedup_key,
                ack_required=False,
                scheduled_for=scheduled_for,
            )
        )


def dispatch_due_notifications(session: Session) -> dict[str, int]:
    settings = get_settings()
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        return {"sent": 0, "skipped": 0}

    now = datetime.now(UTC)
    pending = session.scalars(
        select(Notification)
        .where(
            Notification.channel == "telegram",
            Notification.sent_at.is_(None),
            Notification.scheduled_for <= now,
        )
        .order_by(Notification.scheduled_for)
    ).all()

    sent = 0
    skipped = 0
    with httpx.Client(timeout=20.0) as client:
        for notification in pending:
            if notification.ack_required and _is_acknowledged(session, notification.normalized_item_id):
                notification.sent_at = now
                notification.delivery_error = "acknowledged_before_send"
                skipped += 1
                continue

            if notification.kind == "digest":
                text = build_digest_message(session)
                if not text:
                    notification.sent_at = now
                    notification.delivery_error = "empty_digest"
                    skipped += 1
                    continue
            else:
                item = session.get(NormalizedItem, notification.normalized_item_id)
                if item is None:
                    notification.sent_at = now
                    notification.delivery_error = "missing_item"
                    skipped += 1
                    continue
                text = build_urgent_message(item, notification)

            response = client.post(
                f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage",
                json={
                    "chat_id": settings.telegram_chat_id,
                    "text": text,
                    "disable_web_page_preview": True,
                },
            )
            notification.attempt_count += 1
            if response.is_success:
                notification.sent_at = now
                notification.delivery_error = None
                _schedule_reminder_if_needed(session, notification, now)
                sent += 1
            else:
                notification.delivery_error = response.text
    return {"sent": sent, "skipped": skipped}


def build_urgent_message(item: NormalizedItem, notification: Notification) -> str:
    lines = [f"[{notification.severity.upper()}] {item.title}"]
    if item.primary_url:
        lines.append(item.primary_url)
    if item.starts_at:
        lines.append(f"Starts: {format_local_datetime(item.starts_at)}")
    if item.due_at:
        lines.append(f"Due: {format_local_datetime(item.due_at)}")
    if notification.payload and notification.payload.get("reason"):
        lines.append(f"Reason: {notification.payload['reason']}")
    if notification.payload and notification.payload.get("previous_values"):
        previous_due = notification.payload["previous_values"].get("due_at")
        new_due = notification.payload.get("new_values", {}).get("due_at")
        if previous_due or new_due:
            lines.append(
                "Deadline change: "
                f"{_format_version_datetime(previous_due, date_only=True)} -> "
                f"{_format_version_datetime(new_due, date_only=True)}"
            )
    lines.append(f"Item ID: {item.id}")
    return "\n".join(lines)


def build_digest_message(session: Session, window_hours: int = DIGEST_WINDOW_HOURS) -> str:
    since = datetime.now(UTC) - timedelta(hours=window_hours)
    items = session.scalars(
        select(NormalizedItem)
        .where(NormalizedItem.updated_at >= since, NormalizedItem.review_status != "archived")
        .order_by(NormalizedItem.updated_at.desc())
    ).all()
    if not items:
        return ""
    latest_versions = _latest_versions_by_item(session, since)
    duplicate_titles = {_digest_title(item) for item in items if item.item_type != "calendar_event"}
    urgent_item_ids = set(
        session.scalars(
            select(Notification.normalized_item_id).where(
                Notification.kind == "urgent",
                Notification.scheduled_for >= since,
            )
        ).all()
    )

    grouped: dict[tuple[int | None, str], list[NormalizedItem]] = defaultdict(list)
    urgent_changes: dict[str, list[DigestLine]] = defaultdict(list)
    action_needed: dict[str, list[DigestLine]] = defaultdict(list)
    important_announcements: dict[str, list[DigestLine]] = defaultdict(list)

    for item in items:
        if item.id in urgent_item_ids:
            continue
        if item.item_type == "calendar_event" and _digest_title(item) in duplicate_titles:
            continue
        grouped[(item.course_id, _digest_title(item))].append(item)

    representatives = [
        _select_digest_representative(group, latest_versions)
        for group in grouped.values()
    ]
    representatives.sort(key=lambda item: _normalize_datetime(item.updated_at) or datetime.min.replace(tzinfo=UTC), reverse=True)

    for item in representatives:
        course_name = get_item_course_name(session, item)
        change = latest_versions.get(item.id)
        line = _digest_line(item, course_name, change)
        bucket = _digest_bucket(item, change, since)
        if bucket == "urgent_changes":
            urgent_changes[course_name].append(line)
        elif bucket == "action_needed":
            action_needed[course_name].append(line)
        elif bucket == "important_announcements":
            important_announcements[course_name].append(line)

    sections = [
        ("Urgent changes", urgent_changes),
        ("Action needed soon", action_needed),
        ("Important announcements", important_announcements),
    ]

    rendered = [f"Daily Moodle digest ({format_local_date(datetime.now(UTC))})"]
    for title, courses in sections:
        if not courses:
            continue
        rendered.append(title)
        ordered_courses = sorted(
            courses.items(),
            key=lambda entry: min(line.sort_key for line in entry[1]),
        )
        for course_name, course_lines in ordered_courses:
            lines = sorted(course_lines, key=lambda line: line.sort_key)
            rendered.append(course_name)
            for line in lines[:MAX_SECTION_ITEMS]:
                rendered.append(line.render())

    if len(rendered) == 1:
        return ""
    return "\n".join(rendered)


def acknowledge_item(session: Session, item_id: int, actor: str = "api") -> bool:
    item = session.get(NormalizedItem, item_id)
    if item is None:
        return False
    existing = session.scalar(
        select(Acknowledgement).where(
            Acknowledgement.normalized_item_id == item_id,
            Acknowledgement.actor == actor,
        )
    )
    if existing is None:
        session.add(Acknowledgement(normalized_item_id=item_id, actor=actor))
    return True


def _is_acknowledged(session: Session, item_id: int) -> bool:
    return (
        session.scalar(
            select(Acknowledgement.id).where(Acknowledgement.normalized_item_id == item_id).limit(1)
        )
        is not None
    )


def _schedule_reminder_if_needed(session: Session, notification: Notification, now: datetime) -> None:
    if not notification.ack_required:
        return
    reminder_number = int((notification.payload or {}).get("reminder_number", 0))
    if reminder_number >= 2:
        return
    base_dedup = (notification.payload or {}).get("base_dedup") or notification.dedup_key
    reminder_dedup = f"{base_dedup}:reminder:{reminder_number + 1}"
    existing = session.scalar(select(Notification).where(Notification.dedup_key == reminder_dedup))
    if existing is not None:
        return
    payload = dict(notification.payload or {})
    payload["reminder_number"] = reminder_number + 1
    session.add(
        Notification(
            normalized_item_id=notification.normalized_item_id,
            channel=notification.channel,
            severity=notification.severity,
            kind=notification.kind,
            dedup_key=reminder_dedup,
            payload=payload,
            ack_required=True,
            scheduled_for=now + timedelta(hours=12),
        )
    )


def _latest_versions_by_item(session: Session, since: datetime) -> dict[int, ItemVersion]:
    versions = session.scalars(
        select(ItemVersion).where(ItemVersion.changed_at >= since).order_by(ItemVersion.changed_at.desc())
    ).all()
    latest: dict[int, ItemVersion] = {}
    for version in versions:
        if version.normalized_item_id not in latest:
            latest[version.normalized_item_id] = version
    return latest


def _digest_bucket(item: NormalizedItem, version: ItemVersion | None, since: datetime) -> str:
    if version and _change_type_from_version(version) in {"deadline_changed", "deadline_removed", "deadline_added", "schedule_changed", "exam_changed"}:
        return "urgent_changes"

    if item.status != "active":
        return "materials"

    now = datetime.now(UTC)
    due_at = _normalize_datetime(item.due_at)
    starts_at = _normalize_datetime(item.starts_at)
    if item.item_type == "calendar_event":
        if due_at and 0 <= (due_at - now).total_seconds() <= ACTION_WINDOW_DAYS * 24 * 3600:
            return "action_needed"
        if starts_at and 0 <= (starts_at - now).total_seconds() <= ACTION_WINDOW_DAYS * 24 * 3600:
            return "action_needed"
        return "important_announcements"
    if due_at and 0 <= (due_at - now).total_seconds() <= ACTION_WINDOW_DAYS * 24 * 3600:
        return "action_needed"
    if item.review_status in {"needs_review", "watch"}:
        return "action_needed"

    if item.item_type in {"announcement", "forum_discussion"}:
        return "important_announcements"
    if item.item_type in {"assignment", "quiz"} and item.updated_at >= since:
        return "important_announcements"

    if item.item_type in {"material", "material_file"}:
        if item.review_status == "watch":
            return "urgent_changes"
        if item.review_status == "needs_review":
            return "action_needed"
        if version and version.changed_fields:
            return "materials"

    return "materials"


def _digest_line(item: NormalizedItem, course_name: str, version: ItemVersion | None) -> DigestLine:
    body = _digest_body(item, version)
    title = _digest_title(item)
    sort_key = _digest_sort_key(item, version)
    if item.item_type in {"assignment", "quiz"} and item.due_at:
        title = f"{format_local_date_time(item.due_at)} {title}"
    elif item.item_type == "calendar_event":
        event_date = item.due_at or item.starts_at
        if event_date:
            title = f"{format_local_date_time(event_date)} {title}"
    elif item.item_type in {"announcement", "forum_discussion"} and item.starts_at:
        title = f"{format_local_date_time(item.starts_at)} {title}"
    elif item.item_type in {"material", "material_file"} and (item.starts_at or item.updated_at):
        title = f"{format_local_date_time(item.starts_at or item.updated_at)} {title}"
    return DigestLine(
        title=title,
        body=body,
        url=item.primary_url,
        course=course_name,
        sort_key=sort_key,
    )


def _digest_body(item: NormalizedItem, version: ItemVersion | None) -> str:
    if version:
        change_type = _change_type_from_version(version)
        if change_type == "deadline_changed":
            previous_due = (version.previous_values or {}).get("due_at")
            new_due = (version.new_values or {}).get("due_at")
            return (
                "deadline changed from "
                f"{_format_version_datetime(previous_due, date_only=True)} to "
                f"{_format_version_datetime(new_due, date_only=True)}"
            )
        if change_type == "deadline_removed":
            previous_due = (version.previous_values or {}).get("due_at")
            return f"deadline removed after {_format_version_datetime(previous_due, date_only=True)}"
        if change_type == "deadline_added":
            new_due = (version.new_values or {}).get("due_at")
            return f"new deadline: {_format_version_datetime(new_due, date_only=True)}"
        if change_type == "schedule_changed":
            previous_starts = (version.previous_values or {}).get("starts_at")
            new_starts = (version.new_values or {}).get("starts_at")
            return (
                "schedule changed from "
                f"{_format_version_datetime(previous_starts)} to "
                f"{_format_version_datetime(new_starts)}"
            )
        if change_type == "exam_changed":
            return "exam date updated"

    if item.item_type in {"announcement", "forum_discussion"}:
        snippet = strip_html(item.body_text) or item.title
        return _shorten(snippet, 140)

    if item.item_type in {"assignment", "quiz"} and item.due_at:
        return ""

    if item.item_type == "calendar_event":
        if item.starts_at:
            return ""
        if item.due_at:
            return ""

    if item.review_status in {"watch", "needs_review"} and item.review_reason:
        return item.review_reason.replace("_", " ")

    if item.body_text:
        if item.item_type in {"material", "material_file"}:
            return ""
        return _shorten(strip_html(item.body_text) or item.body_text, 100)

    return ""


def _change_type_from_version(version: ItemVersion) -> str:
    changed_fields = set(version.changed_fields or [])
    previous_values = version.previous_values or {}
    new_values = version.new_values or {}
    if "due_at" in changed_fields:
        if previous_values.get("due_at") and new_values.get("due_at"):
            return "deadline_changed"
        if previous_values.get("due_at") and not new_values.get("due_at"):
            return "deadline_removed"
        if not previous_values.get("due_at") and new_values.get("due_at"):
            return "deadline_added"
    if "starts_at" in changed_fields:
        return "schedule_changed"
    if "facts_payload" in changed_fields:
        previous_facts = {fact.get("fact_type") for fact in previous_values.get("facts_payload") or []}
        new_facts = {fact.get("fact_type") for fact in new_values.get("facts_payload") or []}
        if {"class_session_at", "starts_at"} & (previous_facts | new_facts):
            return "schedule_changed"
        if {"due_at"} & (previous_facts | new_facts):
            return "deadline_changed"
        if {"exam_at"} & (previous_facts | new_facts):
            return "exam_changed"
    return "content_changed"


def _shorten(value: str, limit: int) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _digest_signature(item: NormalizedItem) -> tuple[int | None, str]:
    return item.course_id, _digest_title(item)


def _select_digest_representative(
    items: list[NormalizedItem],
    latest_versions: dict[int, ItemVersion],
) -> NormalizedItem:
    def rank(item: NormalizedItem) -> tuple[int, int, int, datetime]:
        version = latest_versions.get(item.id)
        change_type = _change_type_from_version(version) if version else "content_changed"
        change_priority = {
            "deadline_changed": 5,
            "deadline_removed": 4,
            "deadline_added": 4,
            "schedule_changed": 3,
            "exam_changed": 3,
            "content_changed": 2,
        }.get(change_type, 2)
        due_priority = 1 if item.due_at is not None else 0
        starts_priority = 1 if item.starts_at is not None else 0
        updated_at = _normalize_datetime(item.updated_at) or datetime.min.replace(tzinfo=UTC)
        return change_priority, due_priority, starts_priority, updated_at

    return max(items, key=rank)


def _digest_title(item: NormalizedItem) -> str:
    title = item.title.strip().lower()
    if item.item_type == "calendar_event":
        for suffix in (
            " está en fecha de entrega",
            " esta en fecha de entrega",
            " abre",
            " cierra",
        ):
            if title.endswith(suffix):
                title = title[: -len(suffix)].strip()
                break
    return title


def _digest_sort_key(item: NormalizedItem, version: ItemVersion | None) -> tuple[int, datetime, str]:
    change_type = _change_type_from_version(version) if version else ""
    priority = {
        "deadline_changed": 0,
        "deadline_removed": 0,
        "deadline_added": 0,
        "schedule_changed": 1,
        "exam_changed": 1,
    }.get(change_type, 2)
    due_at = _normalize_datetime(item.due_at)
    starts_at = _normalize_datetime(item.starts_at)
    when = due_at or starts_at or _normalize_datetime(item.updated_at) or datetime.max.replace(tzinfo=UTC)
    title = _digest_title(item)
    return priority, when, title


def _format_version_datetime(value: str | None, *, date_only: bool = False) -> str:
    if not value:
        return ""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    if date_only:
        return format_local_date(parsed)
    return format_local_datetime(parsed)
