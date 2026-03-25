from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from uni_tracker.models import ItemFact, ItemVersion, NormalizedItem, RawArtifact, SourceObject
from uni_tracker.services.moodle import stable_hash
from uni_tracker.services.parsing import ExtractedFact


@dataclass
class ItemChange:
    state: str
    change_type: str
    changed_fields: list[str]
    previous_values: dict[str, Any] | None = None
    new_values: dict[str, Any] | None = None


def upsert_source_object(
    session: Session,
    *,
    source_account_id: int,
    external_id: str,
    object_type: str,
    course_id: int | None,
    parent_external_id: str | None,
    source_url: str | None,
    raw_payload: dict[str, Any],
) -> tuple[SourceObject, bool]:
    now = datetime.now(UTC)
    payload_hash = stable_hash(raw_payload)
    source_object = session.scalar(
        select(SourceObject).where(
            SourceObject.source_account_id == source_account_id,
            SourceObject.external_id == external_id,
            SourceObject.object_type == object_type,
        )
    )
    created = False
    if source_object is None:
        source_object = SourceObject(
            source_account_id=source_account_id,
            course_id=course_id,
            external_id=external_id,
            object_type=object_type,
            parent_external_id=parent_external_id,
            source_url=source_url,
            current_hash=payload_hash,
            raw_payload=raw_payload,
            first_seen_at=now,
            last_seen_at=now,
        )
        session.add(source_object)
        session.flush()
        created = True
    else:
        source_object.course_id = course_id
        source_object.parent_external_id = parent_external_id
        source_object.source_url = source_url
        source_object.current_hash = payload_hash
        source_object.raw_payload = raw_payload
        source_object.last_seen_at = now
        source_object.deleted_at = None
    return source_object, created


def create_raw_artifact(
    session: Session,
    *,
    collector_run_id: int,
    source_object_id: int | None,
    artifact_type: str,
    mime_type: str,
    storage_path: str,
    content_hash: str,
    size_bytes: int,
    source_url: str | None,
    metadata_json: dict[str, Any] | None = None,
    extraction_status: str = "not_applicable",
) -> RawArtifact:
    artifact = RawArtifact(
        collector_run_id=collector_run_id,
        source_object_id=source_object_id,
        artifact_type=artifact_type,
        mime_type=mime_type,
        storage_path=storage_path,
        content_hash=content_hash,
        size_bytes=size_bytes,
        extraction_status=extraction_status,
        metadata_json=metadata_json,
        source_url=source_url,
    )
    session.add(artifact)
    session.flush()
    return artifact


def upsert_normalized_item(
    session: Session,
    *,
    source_object_id: int,
    course_id: int | None,
    item_type: str,
    title: str,
    body_text: str | None,
    published_at: datetime | None,
    starts_at: datetime | None,
    due_at: datetime | None,
    primary_url: str | None,
    raw_payload: dict[str, Any],
    review_status: str = "none",
    review_reason: str | None = None,
    source_artifact_id: int | None = None,
    facts_payload: list[dict[str, Any]] | None = None,
) -> tuple[NormalizedItem, ItemChange]:
    facts_payload = _serialize_fact_payload(facts_payload)
    normalized_payload = {
        "title": title,
        "body_text": body_text,
        "published_at": published_at.isoformat() if published_at else None,
        "starts_at": starts_at.isoformat() if starts_at else None,
        "due_at": due_at.isoformat() if due_at else None,
        "primary_url": primary_url,
        "review_status": review_status,
        "review_reason": review_reason,
        "item_type": item_type,
        "facts_payload": facts_payload,
    }
    field_hash = stable_hash(normalized_payload)
    item = session.scalar(
        select(NormalizedItem).where(
            NormalizedItem.source_object_id == source_object_id,
            NormalizedItem.item_type == item_type,
        )
    )
    if item is None:
        item = NormalizedItem(
            source_object_id=source_object_id,
            course_id=course_id,
            item_type=item_type,
            title=title,
            body_text=body_text,
            published_at=published_at,
            starts_at=starts_at,
            due_at=due_at,
            urgency="normal",
            status="active",
            primary_url=primary_url,
            field_hash=field_hash,
            raw_payload=raw_payload,
            review_status=review_status,
            review_reason=review_reason,
        )
        session.add(item)
        session.flush()
        return item, ItemChange(
            state="created",
            change_type="created",
            changed_fields=list(normalized_payload.keys()),
            previous_values=None,
            new_values=normalized_payload,
        )

    if item.field_hash == field_hash:
        item.raw_payload = raw_payload
        item.review_status = review_status
        item.review_reason = review_reason
        return item, ItemChange(
            state="unchanged",
            change_type="unchanged",
            changed_fields=[],
            previous_values=None,
            new_values=normalized_payload,
        )

    previous_values = {
        "title": item.title,
        "body_text": item.body_text,
        "published_at": item.published_at.isoformat() if item.published_at else None,
        "starts_at": item.starts_at.isoformat() if item.starts_at else None,
        "due_at": item.due_at.isoformat() if item.due_at else None,
        "primary_url": item.primary_url,
        "review_status": item.review_status,
        "review_reason": item.review_reason,
        "item_type": item.item_type,
        "facts_payload": _serialize_fact_payload(
            [
                {
                    "fact_type": fact.fact_type,
                    "value": fact.value_json,
                    "extractor_type": fact.extractor_type,
                }
                for fact in item.facts
            ]
        ),
    }
    new_values = normalized_payload
    changed_fields = [field for field, value in new_values.items() if previous_values.get(field) != value]
    change_type = _classify_change_type(previous_values, new_values, changed_fields)

    item.course_id = course_id
    item.title = title
    item.body_text = body_text
    item.published_at = published_at
    item.starts_at = starts_at
    item.due_at = due_at
    item.primary_url = primary_url
    item.field_hash = field_hash
    item.raw_payload = raw_payload
    item.review_status = review_status
    item.review_reason = review_reason
    session.flush()

    version_number = len(item.versions) + 1
    session.add(
        ItemVersion(
            normalized_item_id=item.id,
            source_artifact_id=source_artifact_id,
            version_number=version_number,
            changed_fields=changed_fields,
            previous_values=previous_values,
            new_values=new_values,
        )
    )
    return item, ItemChange(
        state="updated",
        change_type=change_type,
        changed_fields=changed_fields,
        previous_values=previous_values,
        new_values=new_values,
    )


def replace_item_facts(
    session: Session,
    *,
    item: NormalizedItem,
    facts: list[ExtractedFact],
    source_artifact_id: int | None,
) -> None:
    for fact in list(item.facts):
        if fact.extractor_type in {"module_dates", "deterministic_text_dates", "llm_kimi_k2_5"}:
            session.delete(fact)
    for fact in facts:
        session.add(
            ItemFact(
                normalized_item_id=item.id,
                source_artifact_id=source_artifact_id,
                fact_type=fact.fact_type,
                value_json=fact.value,
                confidence=fact.confidence,
                extractor_type=fact.extractor_type,
                source_span=fact.source_span,
            )
        )


def mark_removed_source_objects(
    session: Session,
    *,
    source_account_id: int,
    course_id: int,
    object_types: list[str],
    live_external_ids: set[str],
) -> int:
    removed = 0
    objects = session.scalars(
        select(SourceObject).where(
            SourceObject.source_account_id == source_account_id,
            SourceObject.course_id == course_id,
            SourceObject.object_type.in_(object_types),
        )
    ).all()
    now = datetime.now(UTC)
    for source_object in objects:
        if source_object.external_id in live_external_ids:
            continue
        if source_object.deleted_at is None:
            source_object.deleted_at = now
            for item in source_object.normalized_items:
                item.status = "removed"
            removed += 1
    return removed


def _serialize_fact_payload(facts_payload: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if not facts_payload:
        return []
    return sorted(
        [
            {
                "fact_type": fact.get("fact_type"),
                "value": fact.get("value"),
                "extractor_type": fact.get("extractor_type"),
            }
            for fact in facts_payload
        ],
        key=lambda entry: (
            str(entry.get("fact_type") or ""),
            stable_hash(entry.get("value") or {}),
            str(entry.get("extractor_type") or ""),
        ),
    )


def _classify_change_type(
    previous_values: dict[str, Any],
    new_values: dict[str, Any],
    changed_fields: list[str],
) -> str:
    previous_due = previous_values.get("due_at")
    new_due = new_values.get("due_at")
    if "due_at" in changed_fields:
        if previous_due and new_due:
            return "deadline_changed"
        if previous_due and not new_due:
            return "deadline_removed"
        if not previous_due and new_due:
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
        return "date_mentions_changed"

    review_reason = new_values.get("review_reason") or previous_values.get("review_reason")
    if review_reason == "high_risk_schedule_document" and {"title", "body_text"} & set(changed_fields):
        return "schedule_changed"

    if {"title", "body_text"} & set(changed_fields):
        return "content_changed"

    return "metadata_changed"
