from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from uni_tracker.models import Course, ItemBrief, NormalizedItem
from uni_tracker.services.parsing import extract_date_facts_from_text, normalize_text
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


def is_item_brief_weak(item: NormalizedItem, brief: ItemBrief) -> bool:
    summary_short = _coerce_string(brief.summary_short)
    title = _coerce_string(item.title)
    bullets = _coerce_string_list(brief.summary_bullets)
    key_dates = brief.key_dates or []
    key_requirements = _coerce_string_list(brief.key_requirements)
    risk_flags = _coerce_string_list(brief.risk_flags)
    course_context = brief.course_context or {}

    if not summary_short:
        return True
    if summary_short.casefold() == title.casefold():
        return True
    if brief.confidence < 0.5 and len(bullets) <= 1:
        return True
    if item.item_type in {"material_file"} and len(bullets) <= 1 and not (key_dates or key_requirements or risk_flags):
        return True
    if not course_context:
        return True
    return False


def build_deterministic_backfill_payload(session: Session, item: NormalizedItem) -> dict[str, Any]:
    course_name = _course_name(session, item)
    body_text = normalize_text(item.body_text or "")
    unit_topics = _extract_unit_topics(body_text)
    date_facts = extract_date_facts_from_text(body_text)
    key_dates = [
        {
            "type": fact.fact_type,
            "iso_datetime": fact.value.get("value"),
            "matched_text": fact.value.get("matched_text"),
        }
        for fact in date_facts
        if fact.value.get("value")
    ]
    key_requirements = _deterministic_requirements(body_text)
    risk_flags = ["deterministic_backfill"]
    if "bibliografía obligatoria" in body_text.casefold():
        risk_flags.append("bibliography_required")
    if "entrega" in body_text.casefold() or key_dates:
        risk_flags.append("date_sensitive")

    summary_short = _deterministic_summary(course_name, unit_topics, key_dates, body_text)
    summary_bullets = _deterministic_bullets(unit_topics, key_dates, body_text)
    source_refs = [
        {
            "type": "item",
            "item_id": item.id,
            "title": item.title,
            "url": item.primary_url,
        }
    ]
    return {
        "summary_short": summary_short,
        "summary_bullets": summary_bullets,
        "key_dates": key_dates,
        "key_requirements": key_requirements,
        "risk_flags": risk_flags,
        "course_context": {
            "course_id": item.course_id,
            "course_name": course_name,
            "item_type": item.item_type,
        },
        "confidence": 0.65 if unit_topics or key_dates else 0.45,
        "source_refs": source_refs,
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


def _extract_unit_topics(body_text: str) -> list[str]:
    matches = re.findall(
        r"UNIDAD\s+\d+:\s*(.*?)(?=\s+Contenidos:|\s+Objetivos específicos|\s+Bibliografía|\s+UNIDAD\s+\d+:|$)",
        body_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    topics = [normalize_text(match) for match in matches]
    return [topic for topic in topics if topic]


def _deterministic_requirements(body_text: str) -> list[str]:
    requirements: list[str] = []
    lowered = body_text.casefold()
    if "bibliografía obligatoria" in lowered:
        requirements.append("Revisar la bibliografía obligatoria")
    if "objetivos específicos" in lowered:
        requirements.append("Seguir los objetivos específicos de cada unidad")
    if "entrega" in lowered:
        requirements.append("Registrar las fechas de entrega mencionadas")
    if "parcial" in lowered or "examen" in lowered:
        requirements.append("Preparar evaluaciones parciales o finales")
    return requirements


def _deterministic_summary(course_name: str, unit_topics: list[str], key_dates: list[dict[str, Any]], body_text: str) -> str:
    details: list[str] = []
    if unit_topics:
        details.append(f"{len(unit_topics)} unidades")
    if key_dates:
        details.append(f"{len(key_dates)} fecha(s) detectada(s)")
    if "bibliografía obligatoria" in body_text.casefold():
        details.append("bibliografía obligatoria y complementaria")
    if details:
        return f"{course_name}: " + ", ".join(details)
    return course_name


def _deterministic_bullets(unit_topics: list[str], key_dates: list[dict[str, Any]], body_text: str) -> list[str]:
    bullets: list[str] = []
    if unit_topics:
        bullets.append(f"Unidades: {', '.join(unit_topics[:3])}")
        bullets.append(f"Total de unidades detectadas: {len(unit_topics)}")
    if "bibliografía obligatoria" in body_text.casefold():
        bullets.append("Incluye bibliografía obligatoria y complementaria")
    if key_dates:
        first_date = key_dates[0]["matched_text"]
        bullets.append(f"Fecha detectada: {first_date}")
    return bullets[:4]


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
