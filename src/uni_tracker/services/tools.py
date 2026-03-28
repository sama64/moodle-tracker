from __future__ import annotations

from datetime import UTC, datetime, timedelta
from hashlib import sha256

import re

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session

from uni_tracker.config import get_settings
from uni_tracker.models import Course, ItemFact, NormalizedItem, Notification, RawArtifact, SourceObject


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


def get_item_artifacts(session: Session, item_id: int) -> dict:
    item = session.scalar(
        select(NormalizedItem)
        .where(NormalizedItem.id == item_id)
        .options(selectinload(NormalizedItem.source_object))
    )
    if item is None or item.source_object is None:
        return {}

    root_object = item.source_object
    candidate_objects: list[SourceObject] = [root_object]
    if root_object.object_type != "module_file":
        child_objects = session.scalars(
            select(SourceObject)
            .where(
                SourceObject.parent_external_id == root_object.external_id,
                SourceObject.object_type == "module_file",
            )
            .order_by(SourceObject.id.asc())
        ).all()
        candidate_objects.extend(child_objects)

    artifact_map = _load_artifacts_by_source_object(session, [source_object.id for source_object in candidate_objects])
    item_map = _load_items_by_source_object(session, [source_object.id for source_object in candidate_objects])
    entries: dict[str, dict] = {}

    for source_object in candidate_objects:
        _merge_declared_contents(entries, root_object=root_object, source_object=source_object, item_map=item_map)
        _merge_downloaded_artifacts(entries, root_object=root_object, source_object=source_object, item_map=item_map, artifact_map=artifact_map)

    artifacts = sorted(
        entries.values(),
        key=lambda artifact: (
            artifact["filename"].lower(),
            artifact["filepath"] or "",
            artifact["source_object_id"] or 0,
        ),
    )
    return {"item": item, "artifacts": artifacts}


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


def _load_artifacts_by_source_object(session: Session, source_object_ids: list[int]) -> dict[int, list[RawArtifact]]:
    if not source_object_ids:
        return {}
    artifacts = session.scalars(
        select(RawArtifact)
        .where(RawArtifact.source_object_id.in_(source_object_ids))
        .order_by(RawArtifact.id.asc())
    ).all()
    grouped: dict[int, list[RawArtifact]] = {}
    for artifact in artifacts:
        if artifact.source_object_id is None:
            continue
        grouped.setdefault(artifact.source_object_id, []).append(artifact)
    return grouped


def _load_items_by_source_object(session: Session, source_object_ids: list[int]) -> dict[int, NormalizedItem]:
    if not source_object_ids:
        return {}
    items = session.scalars(
        select(NormalizedItem)
        .where(NormalizedItem.source_object_id.in_(source_object_ids))
        .order_by(NormalizedItem.id.asc())
    ).all()
    return {item.source_object_id: item for item in items}


def _merge_declared_contents(
    entries: dict[str, dict],
    *,
    root_object: SourceObject,
    source_object: SourceObject,
    item_map: dict[int, NormalizedItem],
) -> None:
    for content in source_object.raw_payload.get("contents", []) or []:
        filename = content.get("filename")
        if not filename:
            continue
        key = _content_key(source_object.external_id, content.get("filepath"), filename)
        entry = entries.setdefault(
            key,
            _empty_artifact_payload(
                filename=filename,
                filepath=content.get("filepath"),
                source_url=content.get("fileurl"),
                mime_type=content.get("mimetype"),
                size_bytes=_coerce_int(content.get("filesize")),
                source_object_id=source_object.id,
                parent_source_object_id=root_object.id if source_object.id != root_object.id else None,
                item=item_map.get(source_object.id),
            ),
        )
        entry["filepath"] = entry["filepath"] or content.get("filepath")
        entry["source_url"] = entry["source_url"] or content.get("fileurl")
        entry["mime_type"] = entry["mime_type"] or content.get("mimetype")
        entry["size_bytes"] = entry["size_bytes"] or _coerce_int(content.get("filesize"))


def _merge_downloaded_artifacts(
    entries: dict[str, dict],
    *,
    root_object: SourceObject,
    source_object: SourceObject,
    item_map: dict[int, NormalizedItem],
    artifact_map: dict[int, list[RawArtifact]],
) -> None:
    artifacts = artifact_map.get(source_object.id, [])
    if not artifacts:
        return
    raw_payload = source_object.raw_payload
    filename = raw_payload.get("filename") or raw_payload.get("content", {}).get("filename") or source_object.external_id
    filepath = raw_payload.get("content", {}).get("filepath")
    key = _content_key(source_object.parent_external_id or source_object.external_id, filepath, filename)
    file_artifact = next((artifact for artifact in reversed(artifacts) if artifact.artifact_type == "file"), None)
    extracted_text_artifact = None
    extracted_text = None
    for artifact in reversed(artifacts):
        if artifact.artifact_type != "extracted_text":
            continue
        candidate_text = _read_artifact_text(artifact.storage_path)
        if candidate_text is None:
            if extracted_text_artifact is None:
                extracted_text_artifact = artifact
            continue
        extracted_text_artifact = artifact
        extracted_text = candidate_text
        break
    entry = entries.setdefault(
        key,
        _empty_artifact_payload(
            filename=filename,
            filepath=filepath,
            source_url=source_object.source_url,
            mime_type=file_artifact.mime_type if file_artifact else None,
            size_bytes=file_artifact.size_bytes if file_artifact else None,
            source_object_id=source_object.id,
            parent_source_object_id=root_object.id if source_object.id != root_object.id else None,
            item=item_map.get(source_object.id),
        ),
    )
    entry["source_object_id"] = source_object.id
    entry["parent_source_object_id"] = root_object.id if source_object.id != root_object.id else entry["parent_source_object_id"]
    if item_map.get(source_object.id) is not None:
        entry["item_id"] = item_map[source_object.id].id
        entry["item_type"] = item_map[source_object.id].item_type
    if file_artifact is not None:
        entry["file_artifact_id"] = file_artifact.id
        entry["mime_type"] = file_artifact.mime_type
        entry["source_url"] = entry["source_url"] or file_artifact.source_url
        entry["storage_path"] = file_artifact.storage_path
        entry["size_bytes"] = file_artifact.size_bytes
        entry["downloaded"] = True
        entry["extraction_status"] = file_artifact.extraction_status
    if extracted_text_artifact is not None:
        entry["extracted_text_artifact_id"] = extracted_text_artifact.id
        entry["extracted_text"] = extracted_text
        entry["extraction_status"] = extracted_text_artifact.extraction_status


def _empty_artifact_payload(
    *,
    filename: str,
    filepath: str | None,
    source_url: str | None,
    mime_type: str | None,
    size_bytes: int | None,
    source_object_id: int | None,
    parent_source_object_id: int | None,
    item: NormalizedItem | None,
) -> dict:
    return {
        "source_object_id": source_object_id,
        "parent_source_object_id": parent_source_object_id,
        "filename": filename,
        "filepath": filepath,
        "item_id": item.id if item is not None else None,
        "item_type": item.item_type if item is not None else None,
        "mime_type": mime_type,
        "source_url": source_url,
        "file_artifact_id": None,
        "extracted_text_artifact_id": None,
        "storage_path": None,
        "size_bytes": size_bytes,
        "downloaded": False,
        "extracted_text": None,
        "extraction_status": "not_downloaded",
    }


def _content_key(external_id: str, filepath: str | None, filename: str) -> str:
    return f"{external_id}:{filepath or '/'}:{filename}"


def _coerce_int(value: object) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _read_artifact_text(storage_path: str) -> str | None:
    artifact_root = get_settings().raw_storage_path
    artifact_path = (artifact_root / storage_path).resolve()
    try:
        artifact_path.relative_to(artifact_root.resolve())
    except ValueError:
        return None
    if not artifact_path.is_file():
        return None
    return artifact_path.read_text(encoding="utf-8", errors="replace")


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
