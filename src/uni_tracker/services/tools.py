from __future__ import annotations

from datetime import UTC, datetime, timedelta
from hashlib import sha256

import re

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session

from uni_tracker.config import get_settings
from uni_tracker.models import Course, ItemFact, ItemVersion, NormalizedItem, Notification, RawArtifact, SourceObject
from uni_tracker.services.completion import is_completed
from uni_tracker.services.storage import build_artifact_store

ACTIONABLE_COMPLETION_ITEM_TYPES = {"assignment", "quiz"}


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

    items = _dedupe_changes_since_items(items)
    candidate_keys = [get_semantic_identity_key(item) for item in items]
    latest_versions_by_item = _latest_versions_by_item(session, normalized_since, [item.id for item in items])
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
        version = latest_versions_by_item.get(item.id)
        meaningful_key = get_meaningful_change_key(item)
        if version is not None:
            previous_key = _meaningful_change_key_from_version_previous(item, version)
            payloads.append(
                {
                    "item": item,
                    "meaningful_key": meaningful_key,
                    "meaningful_change": previous_key != meaningful_key,
                    "change_kind": _change_type_from_version(version, previous_key != meaningful_key),
                }
            )
            continue

        identity_key = get_semantic_identity_key(item)
        previous = previous_by_identity.get(identity_key)
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
    items = session.scalars(
        select(NormalizedItem)
        .where(
            NormalizedItem.status == "active",
            NormalizedItem.due_at.is_not(None),
            NormalizedItem.due_at >= now,
            NormalizedItem.due_at <= end,
        )
        .order_by(NormalizedItem.due_at)
    ).all()
    completed_keys = _completed_identity_keys(items)
    return [item for item in items if not _is_completed_or_shadowed(item, completed_keys)]


def get_risk_items(session: Session, days: int = 14) -> list[NormalizedItem]:
    now = datetime.now(UTC)
    end = now + timedelta(days=days)
    items = session.scalars(
        select(NormalizedItem)
        .options(selectinload(NormalizedItem.facts))
        .order_by(NormalizedItem.updated_at.desc())
    ).all()
    completed_keys = _completed_identity_keys(items)
    filtered = [
        item
        for item in items
        if not _is_completed_or_shadowed(item, completed_keys) and _is_risk_item(item, now=now, end=end)
    ]
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


def _dedupe_changes_since_items(items: list[NormalizedItem]) -> list[NormalizedItem]:
    grouped: dict[str, NormalizedItem] = {}
    for item in items:
        identity_key = get_semantic_identity_key(item)
        if identity_key is None:
            continue
        current = grouped.get(identity_key)
        if current is None or _change_window_rank(item) > _change_window_rank(current):
            grouped[identity_key] = item
    return sorted(grouped.values(), key=lambda item: (_normalize_datetime(item.updated_at) or datetime.min.replace(tzinfo=UTC), item.id))


def _latest_versions_by_item(session: Session, since: datetime, item_ids: list[int]) -> dict[int, ItemVersion]:
    if not item_ids:
        return {}
    versions = session.scalars(
        select(ItemVersion)
        .where(ItemVersion.normalized_item_id.in_(item_ids), ItemVersion.changed_at >= since)
        .order_by(ItemVersion.changed_at.desc(), ItemVersion.id.desc())
    ).all()
    latest: dict[int, ItemVersion] = {}
    for version in versions:
        if version.normalized_item_id not in latest:
            latest[version.normalized_item_id] = version
    return latest


def _change_window_rank(item: NormalizedItem) -> tuple[int, int, int, datetime, int]:
    return (
        1 if item.due_at is not None else 0,
        1 if item.starts_at is not None else 0,
        1 if item.published_at is not None else 0,
        _normalize_datetime(item.updated_at) or datetime.min.replace(tzinfo=UTC),
        item.id,
    )


def _meaningful_change_key_from_version_previous(item: NormalizedItem, version: ItemVersion) -> str | None:
    previous_values = version.previous_values or {}
    title = previous_values.get("title", item.title)
    body_text = previous_values.get("body_text", item.body_text)
    due_at = _coerce_version_datetime(previous_values.get("due_at", item.due_at))
    starts_at = _coerce_version_datetime(previous_values.get("starts_at", item.starts_at))
    primary_url = previous_values.get("primary_url", item.primary_url)
    review_status = previous_values.get("review_status", item.review_status)
    review_reason = previous_values.get("review_reason", item.review_reason)
    payload = "|".join(
        [
            str(item.course_id or ""),
            item.item_type,
            _stable_title(title),
            _datetime_key(due_at),
            _datetime_key(starts_at),
            review_status or "",
            review_reason or "",
            primary_url or "",
            _body_digest((body_text or "").strip()),
        ]
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _coerce_version_datetime(value: datetime | str | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return _normalize_datetime(value)
    if isinstance(value, str):
        return _normalize_datetime(datetime.fromisoformat(value.replace("Z", "+00:00")))
    return None


def _change_type_from_version(version: ItemVersion, meaningful_change: bool) -> str:
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
    if not meaningful_change:
        return "refresh_only"
    return "content_changed"


def _is_risk_item(item: NormalizedItem, *, now: datetime, end: datetime) -> bool:
    if item.status != "active":
        return False
    due_at = _normalize_datetime(item.due_at)
    if due_at is not None and now <= due_at <= end:
        return True
    if _next_fact_datetime(item, "exam_at", now=now, end=end) is not None:
        return True
    if item.review_status == "watch" and item.review_reason == "high_risk_schedule_document":
        return True
    if _looks_like_schedule_document(item):
        return True
    return False


def _looks_like_schedule_document(item: NormalizedItem) -> bool:
    if item.item_type not in {"material", "material_file"}:
        return False
    title = _stable_text(item.title)
    return any(keyword in title for keyword in ("cronograma", "calendario", "horario", "fechas"))


def _risk_rank(item: NormalizedItem) -> tuple[int, int, int, datetime]:
    due_at = _normalize_datetime(item.due_at)
    exam_at = _next_fact_datetime(item, "exam_at")
    due_priority = 2 if due_at is not None else 0
    exam_priority = 1 if exam_at is not None else 0
    watch_priority = 1 if item.review_status == "watch" else 0
    updated_at = _normalize_datetime(item.updated_at) or datetime.now(UTC)
    return due_priority, exam_priority, watch_priority, updated_at


def _risk_sort_key(item: NormalizedItem) -> tuple[int, datetime]:
    due_at = _normalize_datetime(item.due_at)
    exam_at = _next_fact_datetime(item, "exam_at")
    starts_at = _normalize_datetime(item.starts_at)
    if due_at is not None:
        return 0, due_at
    if exam_at is not None:
        return 1, exam_at
    if starts_at is not None:
        return 2, starts_at
    if item.review_status == "watch":
        return 3, _normalize_datetime(item.updated_at) or datetime.now(UTC)
    return 4, _normalize_datetime(item.updated_at) or datetime.now(UTC)


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


def _completed_identity_keys(items: list[NormalizedItem]) -> set[tuple[int | None, str]]:
    return {
        (item.course_id, _risk_title(item))
        for item in items
        if item.status == "active"
        and is_completed(item)
        and item.item_type in ACTIONABLE_COMPLETION_ITEM_TYPES
    }


def _is_completed_or_shadowed(
    item: NormalizedItem,
    completed_keys: set[tuple[int | None, str]],
) -> bool:
    if is_completed(item):
        return True
    if item.item_type not in ACTIONABLE_COMPLETION_ITEM_TYPES | {"calendar_event"}:
        return False
    return (item.course_id, _risk_title(item)) in completed_keys


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
        candidate_text = _read_artifact_text(artifact)
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


def _read_artifact_text(artifact: RawArtifact) -> str | None:
    store = build_artifact_store(get_settings())
    return store.read_text(
        artifact.storage_path,
        backend=getattr(artifact, "storage_backend", "local"),
        bucket=getattr(artifact, "storage_bucket", None),
        key=getattr(artifact, "storage_key", None),
    )


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


def _next_fact_datetime(
    item: NormalizedItem,
    fact_type: str,
    *,
    now: datetime | None = None,
    end: datetime | None = None,
) -> datetime | None:
    candidates: list[datetime] = []
    for fact in item.facts:
        if fact.fact_type != fact_type:
            continue
        raw_value = fact.value_json.get("value")
        if not raw_value:
            continue
        try:
            parsed = _normalize_datetime(datetime.fromisoformat(raw_value))
        except (TypeError, ValueError):
            continue
        if parsed is None:
            continue
        if now is not None and parsed < now:
            continue
        if end is not None and parsed > end:
            continue
        candidates.append(parsed)
    return min(candidates) if candidates else None
