from __future__ import annotations

from datetime import UTC, datetime, timedelta

import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from uni_tracker.models import Course, ItemFact, NormalizedItem, Notification


def get_recent_changes(session: Session, window_hours: int = 48) -> list[NormalizedItem]:
    since = datetime.now(UTC) - timedelta(hours=window_hours)
    return get_changes_since(session, since)


def get_changes_since(session: Session, since: datetime) -> list[NormalizedItem]:
    normalized_since = _normalize_datetime(since)
    if normalized_since is None:
        return []
    return session.scalars(
        select(NormalizedItem)
        .where(NormalizedItem.updated_at >= normalized_since)
        .order_by(NormalizedItem.updated_at.asc(), NormalizedItem.id.asc())
    ).all()


def get_upcoming_deadlines(session: Session, days: int = 10) -> list[NormalizedItem]:
    now = datetime.now(UTC)
    end = now + timedelta(days=days)
    return session.scalars(
        select(NormalizedItem)
        .where(
            NormalizedItem.due_at.is_not(None),
            NormalizedItem.due_at >= now,
            NormalizedItem.due_at <= end,
        )
        .order_by(NormalizedItem.due_at)
    ).all()


def get_risk_items(session: Session, days: int = 14) -> list[NormalizedItem]:
    now = datetime.now(UTC)
    end = now + timedelta(days=days)
    items = session.scalars(
        select(NormalizedItem)
        .order_by(NormalizedItem.updated_at.desc())
    ).all()
    filtered = [item for item in items if _is_risk_item(item, now=now, end=end)]
    grouped: dict[tuple[int | None, str], NormalizedItem] = {}
    for item in filtered:
        key = (item.course_id, _risk_title(item))
        current = grouped.get(key)
        if current is None or _risk_rank(item) > _risk_rank(current):
            grouped[key] = item
    return sorted(
        grouped.values(),
        key=_risk_sort_key,
    )


def get_course_snapshot(session: Session, course_id: int) -> dict:
    course = session.get(Course, course_id)
    if course is None:
        return {}
    items = session.scalars(
        select(NormalizedItem).where(NormalizedItem.course_id == course_id).order_by(NormalizedItem.updated_at.desc())
    ).all()
    return {"course": course, "items": items}


def get_item_provenance(session: Session, item_id: int) -> dict:
    item = session.get(NormalizedItem, item_id)
    if item is None:
        return {}
    facts = session.scalars(
        select(ItemFact).where(ItemFact.normalized_item_id == item_id).order_by(ItemFact.created_at.desc())
    ).all()
    notifications = session.scalars(
        select(Notification).where(Notification.normalized_item_id == item_id).order_by(Notification.scheduled_for.desc())
    ).all()
    return {"item": item, "facts": facts, "notifications": notifications}


def get_item_course_name(session: Session, item: NormalizedItem) -> str:
    course = resolve_item_course(session, item)
    return course.display_name if course is not None else "General"


def _is_risk_item(item: NormalizedItem, *, now: datetime, end: datetime) -> bool:
    due_at = _normalize_datetime(item.due_at)
    if due_at is not None and now <= due_at <= end:
        return True
    if item.review_status == "watch" and item.review_reason == "high_risk_schedule_document":
        return True
    if item.review_status == "needs_review" and item.review_reason == "text_extraction_failed":
        return item.item_type in {"material_file", "resource"}
    return False


def _risk_rank(item: NormalizedItem) -> tuple[int, int, datetime]:
    due_at = _normalize_datetime(item.due_at)
    starts_at = _normalize_datetime(item.starts_at)
    due_priority = 0 if due_at is not None else 1
    watch_priority = 0 if item.review_status == "watch" else 1
    updated_at = _normalize_datetime(item.updated_at) or datetime.now(UTC)
    return due_priority, watch_priority, updated_at


def _risk_sort_key(item: NormalizedItem) -> tuple[int, datetime]:
    due_at = _normalize_datetime(item.due_at)
    starts_at = _normalize_datetime(item.starts_at)
    if due_at is not None:
        return 0, due_at
    if starts_at is not None:
        return 1, starts_at
    if item.review_status == "watch":
        return 2, _normalize_datetime(item.updated_at) or datetime.now(UTC)
    return 3, _normalize_datetime(item.updated_at) or datetime.now(UTC)


def _risk_title(item: NormalizedItem) -> str:
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


def resolve_item_course(session: Session, item: NormalizedItem) -> Course | None:
    if item.course_id is not None:
        course = session.get(Course, item.course_id)
        if course is not None:
            return course
    source_object = item.source_object
    if source_object is not None:
        if source_object.course_id is not None:
            course = session.get(Course, source_object.course_id)
            if course is not None:
                return course
        categories = source_object.raw_payload.get("categories") or []
        return resolve_course_from_categories(session, categories)
    return None


def resolve_course_from_categories(session: Session, categories: list[str]) -> Course | None:
    if not categories:
        return None
    courses = session.scalars(select(Course)).all()
    normalized_courses = [(_normalize_key(course.shortname), course) for course in courses if course.shortname]
    normalized_courses.extend(
        [(_normalize_key(course.display_name), course) for course in courses]
    )
    for category in categories:
        normalized_category = _normalize_key(category)
        for course_key, course in normalized_courses:
            if not course_key:
                continue
            if course_key == normalized_category:
                return course
            if course_key in normalized_category or normalized_category.endswith(course_key):
                return course
    return None


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
