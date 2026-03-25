from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from uni_tracker.models import Course, ItemBrief, NormalizedItem
from uni_tracker.services.timeutils import format_local_date_time


def upsert_item_brief(
    session: Session,
    *,
    item: NormalizedItem,
    payload: dict[str, Any],
    model: str | None,
    llm_job_id: int | None,
    source_artifact_id: int | None = None,
    origin: str = "stored",
) -> ItemBrief:
    brief = session.scalar(select(ItemBrief).where(ItemBrief.normalized_item_id == item.id))
    normalized = _normalize_brief_payload(session=session, item=item, payload=payload, model=model, origin=origin)
    if brief is None:
        brief = ItemBrief(
            normalized_item_id=item.id,
            source_artifact_id=source_artifact_id,
            llm_job_id=llm_job_id,
            origin=normalized["origin"],
            model=normalized["model"],
            summary_short=normalized["summary_short"],
            summary_bullets=normalized["summary_bullets"],
            key_dates=normalized["key_dates"],
            key_requirements=normalized["key_requirements"],
            risk_flags=normalized["risk_flags"],
            course_context=normalized["course_context"],
            confidence=normalized["confidence"],
            source_refs=normalized["source_refs"],
            generated_at=datetime.now(UTC),
        )
        session.add(brief)
        session.flush()
        return brief

    brief.source_artifact_id = source_artifact_id
    brief.llm_job_id = llm_job_id
    brief.origin = normalized["origin"]
    brief.model = normalized["model"]
    brief.summary_short = normalized["summary_short"]
    brief.summary_bullets = normalized["summary_bullets"]
    brief.key_dates = normalized["key_dates"]
    brief.key_requirements = normalized["key_requirements"]
    brief.risk_flags = normalized["risk_flags"]
    brief.course_context = normalized["course_context"]
    brief.confidence = normalized["confidence"]
    brief.source_refs = normalized["source_refs"]
    brief.generated_at = datetime.now(UTC)
    session.flush()
    return brief


def get_item_brief(session: Session, item_id: int) -> dict[str, Any] | None:
    item = session.get(NormalizedItem, item_id)
    if item is None:
        return None
    brief = session.scalar(select(ItemBrief).where(ItemBrief.normalized_item_id == item_id))
    if brief is not None:
        return _brief_to_payload(item, brief)
    return _fallback_item_brief(session, item)


def get_course_brief(session: Session, course_id: int, limit: int = 10) -> dict[str, Any] | None:
    course = session.get(Course, course_id)
    if course is None:
        return None
    items = session.scalars(
        select(NormalizedItem)
        .where(NormalizedItem.course_id == course_id, NormalizedItem.status == "active")
        .order_by(
            NormalizedItem.due_at.is_(None),
            NormalizedItem.due_at,
            NormalizedItem.starts_at.is_(None),
            NormalizedItem.starts_at,
            NormalizedItem.updated_at.desc(),
        )
        .limit(limit)
    ).all()
    briefs = [get_item_brief(session, item.id) for item in items]
    briefs = [brief for brief in briefs if brief is not None]
    if not briefs:
        return {
            "course": course,
            "summary_short": f"{course.display_name}: no briefable items yet.",
            "items": [],
            "origin": "fallback",
        }
    summary_short = _course_summary(course, briefs)
    return {
        "course": course,
        "summary_short": summary_short,
        "items": briefs,
        "origin": "mixed" if any(brief["origin"] == "fallback" for brief in briefs) else "stored",
    }


def _normalize_brief_payload(
    *,
    session: Session,
    item: NormalizedItem,
    payload: dict[str, Any],
    model: str | None,
    origin: str,
) -> dict[str, Any]:
    summary_short = _coerce_string(payload.get("summary_short") or payload.get("summary") or item.title)
    summary_bullets = _coerce_string_list(payload.get("summary_bullets"))
    if not summary_bullets and summary_short:
        summary_bullets = [summary_short]
    key_dates = _coerce_dict_list(payload.get("key_dates") or payload.get("dates"))
    key_requirements = _coerce_string_list(payload.get("key_requirements"))
    risk_flags = _coerce_string_list(payload.get("risk_flags") or payload.get("urgent_signals"))
    course_context = _coerce_dict(payload.get("course_context"))
    if not course_context:
        course_context = {
            "course_id": item.course_id,
            "course_name": _course_name(session, item),
            "item_type": item.item_type,
        }
    source_refs = _coerce_dict_list(payload.get("source_refs"))
    if not source_refs:
        source_refs = [
            {
                "type": "item",
                "item_id": item.id,
                "title": item.title,
                "url": item.primary_url,
            }
        ]
    confidence = payload.get("confidence")
    try:
        confidence_value = float(confidence) if confidence is not None else 0.5
    except (TypeError, ValueError):
        confidence_value = 0.5
    return {
        "origin": origin,
        "model": model,
        "summary_short": summary_short,
        "summary_bullets": summary_bullets,
        "key_dates": key_dates,
        "key_requirements": key_requirements,
        "risk_flags": risk_flags,
        "course_context": course_context,
        "confidence": confidence_value,
        "source_refs": source_refs,
    }


def _brief_to_payload(item: NormalizedItem, brief: ItemBrief) -> dict[str, Any]:
    return {
        "brief_id": brief.id,
        "origin": brief.origin,
        "model": brief.model,
        "generated_at": brief.generated_at,
        "summary_short": brief.summary_short,
        "summary_bullets": brief.summary_bullets,
        "key_dates": brief.key_dates,
        "key_requirements": brief.key_requirements,
        "risk_flags": brief.risk_flags,
        "course_context": brief.course_context,
        "confidence": brief.confidence,
        "source_refs": brief.source_refs,
        "item": item,
    }


def _fallback_item_brief(session: Session, item: NormalizedItem) -> dict[str, Any]:
    dates: list[dict[str, Any]] = []
    if item.due_at is not None:
        dates.append({"type": "due_at", "value": item.due_at.isoformat(), "label": "due_at"})
    if item.starts_at is not None:
        dates.append({"type": "starts_at", "value": item.starts_at.isoformat(), "label": "starts_at"})
    if item.published_at is not None:
        dates.append({"type": "published_at", "value": item.published_at.isoformat(), "label": "published_at"})

    bullets = [item.title]
    if item.due_at is not None:
        bullets.append(f"Due: {format_local_date_time(item.due_at)}")
    if item.starts_at is not None:
        bullets.append(f"Starts: {format_local_date_time(item.starts_at)}")
    if item.review_reason:
        bullets.append(item.review_reason.replace("_", " "))

    return {
        "brief_id": None,
        "origin": "fallback",
        "model": None,
        "generated_at": None,
        "summary_short": item.title,
        "summary_bullets": bullets,
        "key_dates": dates,
        "key_requirements": [],
        "risk_flags": [item.review_reason.replace("_", " ")] if item.review_reason else [],
        "course_context": {
            "course_id": item.course_id,
            "course_name": _course_name(session, item),
            "item_type": item.item_type,
        },
        "confidence": 0.2,
        "source_refs": [
            {
                "type": "item",
                "item_id": item.id,
                "title": item.title,
                "url": item.primary_url,
            }
        ],
        "item": item,
    }


def _course_summary(course: Course, briefs: list[dict[str, Any]]) -> str:
    head = briefs[0]
    if len(briefs) == 1:
        return f"{course.display_name}: {head['summary_short']}"
    highlights = ", ".join(brief["summary_short"] for brief in briefs[:3])
    return f"{course.display_name}: {highlights}"


def _course_name(session: Session | None, item: NormalizedItem) -> str:
    if session is not None and item.course_id is not None:
        course = session.get(Course, item.course_id)
        if course is not None:
            return course.display_name
    if item.course_id is not None:
        return f"Course {item.course_id}"
    return "General"


def _coerce_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _coerce_string_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value.strip()]
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            if isinstance(item, str):
                text = item.strip()
            else:
                text = str(item).strip()
            if text:
                result.append(text)
        return result
    return [str(value).strip()]


def _coerce_dict_list(value: Any) -> list[dict[str, Any]]:
    if not value:
        return []
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _coerce_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
