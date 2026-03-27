from __future__ import annotations

from datetime import UTC, datetime, timedelta
from hashlib import sha256

import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from uni_tracker.models import Course, ItemFact, NormalizedItem, Notification


def get_recent_changes(session: Session, window_hours: int = 48) -> list[NormalizedItem]:
    since = datetime.now(UTC) - timedelta(hours=window_hours)
    return get_changes_since(session, since)


def get_changes_since(
    session: Session,
    since: datetime,
    *,
    include_meaningful_meta: bool = False,
) -> list[NormalizedItem] | list[dict]:
    normalized_since = _normalize_datetime(since)
    if normalized_since is None:
        return []
    items = session.scalars(
        select(NormalizedItem)
        .where(NormalizedItem.updated_at >= normalized_since)
        .order_by(NormalizedItem.updated_at.asc(), NormalizedItem.id.asc())
    ).all()
    if not include_meaningful_meta:
        return items

    candidate_keys = [get_semantic_identity_key(item) for item in items]
    historical_items = session.scalars(
        select(NormalizedItem)
        .where(NormalizedItem.updated_at < normalized_since)
        .order_by(NormalizedItem.updated_at.desc(), NormalizedItem.id.desc())
    ).all()
    previous_by_identity: dict[str, NormalizedItem] = {}
    for previous in historical_items:
        identity_key = get_semantic_identity_key(previous)
        if identity_key in candidate_keys and identity_key not in previous_by_identity:
            previous_by_identity[identity_key] = previous

    payloads: list[dict] = []
    for item in items:
        identity_key = get_semantic_identity_key(item)
        previous = previous_by_identity.get(identity_key)
        meaningful_key = get_meaningful_change_key(item)
        previous_key = get_meaningful_change_key(previous) if previous is not None else None
        payloads.append(
            {
                "item": item,
                "meaningful_key": meaningful_key,
                "meaningful_change": previous_key != meaningful_key,
                "change_kind": get_change_kind(item, previous),
            }
        )
    return payloads


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


def get_semantic_identity_key(item: NormalizedItem | None) -> str | None:
    if item is None:
        return None
    return "|".join(
        [
            str(item.course_id or ""),
            item.item_type,
            _stable_title(item.title),
            item.primary_url or "",
        ]
    )


def get_meaningful_change_key(item: NormalizedItem | None) -> str | None:
    if item is None:
        return None
    title = _stable_title(item.title)
    primary_url = item.primary_url or ""
    review_reason = item.review_reason or ""
    review_status = item.review_status or ""
    body_text = (item.body_text or "").strip()
    payload = "|".join(
        [
            str(item.course_id or ""),
            item.item_type,
            title,
            _datetime_key(item.due_at),
            _datetime_key(item.starts_at),
            review_status,
            review_reason,
            primary_url,
            _body_digest(body_text),
        ]
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def get_change_kind(item: NormalizedItem, previous: NormalizedItem | None) -> str:
    if previous is None:
        return "new"
    if _datetime_key(item.due_at) != _datetime_key(previous.due_at):
        return "deadline_changed"
    if _datetime_key(item.starts_at) != _datetime_key(previous.starts_at):
        return "schedule_changed"
    if (item.review_status or "") != (previous.review_status or "") or (item.review_reason or "") != (previous.review_reason or ""):
        return "review_changed"
    if _stable_text(item.title) != _stable_text(previous.title) or _body_digest(item.body_text or "") != _body_digest(previous.body_text or ""):
        return "content_changed"
    return "refresh_only"


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


def _datetime_key(value: datetime | None) -> str:
    normalized = _normalize_datetime(value)
    if normalized is None:
        return ""
    return normalized.isoformat()


def _stable_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _stable_title(value: str | None) -> str:
    title = _stable_text(value)
    for suffix in (
        ' está en fecha de entrega',
        ' esta en fecha de entrega',
        ' cierra',
        ' abre',
    ):
        if title.endswith(suffix):
            title = title[: -len(suffix)].strip()
            break
    return title


def _body_digest(value: str) -> str:
    normalized = _stable_text(value)
    if not normalized:
        return ""
    return sha256(normalized.encode("utf-8")).hexdigest()
