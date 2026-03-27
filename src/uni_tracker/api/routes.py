from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from uni_tracker.config import get_settings
from uni_tracker.db import SessionLocal
from uni_tracker.models import Course, ItemFact, NormalizedItem, Notification
from uni_tracker.schemas import (
    AcknowledgeResponse,
    CourseBriefResponse,
    CourseResponse,
    HealthSnapshotResponse,
    HealthResponse,
    ItemBriefResponse,
    ItemProvenanceResponse,
    ItemResponse,
    NotificationResponse,
    ProvenanceFactResponse,
    SyncResult,
)
from uni_tracker.services.briefs import get_course_brief, get_item_brief
from uni_tracker.services.health import get_health_snapshot
from uni_tracker.services.notifications import acknowledge_item, dispatch_due_notifications, schedule_daily_digest
from uni_tracker.services.notifications import build_digest_message
from uni_tracker.services.sync import COLLECTOR_REGISTRY, run_collector
from uni_tracker.services.tools import (
    get_course_snapshot,
    get_changes_since,
    get_item_provenance,
    get_recent_changes,
    get_risk_items,
    get_upcoming_deadlines,
)


router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(status="ok", environment=settings.app_env)


@router.get("/health/details", response_model=HealthSnapshotResponse)
def health_details() -> HealthSnapshotResponse:
    settings = get_settings()
    with SessionLocal() as session:
        return HealthSnapshotResponse(
            status="ok",
            environment=settings.app_env,
            details=get_health_snapshot(session),
        )


@router.get("/courses", response_model=list[CourseResponse])
def list_courses() -> list[CourseResponse]:
    with SessionLocal() as session:
        courses = session.scalars(select(Course).order_by(Course.display_name)).all()
        return [
            CourseResponse(
                id=course.id,
                external_id=course.external_id,
                display_name=course.display_name,
                shortname=course.shortname,
                course_url=course.course_url,
                updated_at=course.updated_at,
            )
            for course in courses
        ]


def _item_response(item: NormalizedItem) -> ItemResponse:
    return ItemResponse(
        id=item.id,
        course_id=item.course_id,
        item_type=item.item_type,
        title=item.title,
        body_text=item.body_text,
        published_at=item.published_at,
        starts_at=item.starts_at,
        due_at=item.due_at,
        primary_url=item.primary_url,
        review_status=item.review_status,
        review_reason=item.review_reason,
        updated_at=item.updated_at,
    )


def _brief_item_response(payload: dict) -> ItemBriefResponse:
    return ItemBriefResponse(
        brief_id=payload["brief_id"],
        origin=payload["origin"],
        model=payload["model"],
        generated_at=payload["generated_at"],
        summary_short=payload["summary_short"],
        summary_bullets=payload["summary_bullets"],
        key_dates=payload["key_dates"],
        key_requirements=payload["key_requirements"],
        risk_flags=payload["risk_flags"],
        course_context=payload["course_context"],
        confidence=payload["confidence"],
        source_refs=payload["source_refs"],
        item=_item_response(payload["item"]),
    )


@router.get("/items", response_model=list[ItemResponse])
def list_items(limit: int = 100) -> list[ItemResponse]:
    with SessionLocal() as session:
        items = session.scalars(
            select(NormalizedItem).order_by(NormalizedItem.updated_at.desc()).limit(limit)
        ).all()
        return [_item_response(item) for item in items]


@router.get("/items/{item_id}", response_model=ItemResponse)
def get_item(item_id: int) -> ItemResponse:
    with SessionLocal() as session:
        item = session.get(NormalizedItem, item_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return _item_response(item)


@router.get("/items/{item_id}/provenance", response_model=ItemProvenanceResponse)
def item_provenance(item_id: int) -> ItemProvenanceResponse:
    with SessionLocal() as session:
        payload = get_item_provenance(session, item_id)
        if not payload:
            raise HTTPException(status_code=404, detail="Item not found")
        return ItemProvenanceResponse(
            item=_item_response(payload["item"]),
            facts=[
                ProvenanceFactResponse(
                    fact_type=fact.fact_type,
                    value_json=fact.value_json,
                    confidence=fact.confidence,
                    extractor_type=fact.extractor_type,
                    source_span=fact.source_span,
                )
                for fact in payload["facts"]
            ],
            notifications=[
                NotificationResponse(
                    id=notification.id,
                    severity=notification.severity,
                    kind=notification.kind,
                    scheduled_for=notification.scheduled_for,
                    sent_at=notification.sent_at,
                    delivery_error=notification.delivery_error,
                )
                for notification in payload["notifications"]
            ],
        )


@router.get("/items/{item_id}/brief", response_model=ItemBriefResponse)
def item_brief(item_id: int) -> ItemBriefResponse:
    with SessionLocal() as session:
        payload = get_item_brief(session, item_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return _brief_item_response(payload)


@router.get("/changes/recent", response_model=list[ItemResponse])
def recent_changes(window_hours: int = 48) -> list[ItemResponse]:
    with SessionLocal() as session:
        return [_item_response(item) for item in get_recent_changes(session, window_hours=window_hours)]


@router.get("/changes/since", response_model=list[ItemResponse])
def changes_since(since: datetime) -> list[ItemResponse]:
    with SessionLocal() as session:
        return [_item_response(item) for item in get_changes_since(session, since=since)]


@router.get("/deadlines/upcoming", response_model=list[ItemResponse])
def upcoming_deadlines(days: int = 10) -> list[ItemResponse]:
    with SessionLocal() as session:
        return [_item_response(item) for item in get_upcoming_deadlines(session, days=days)]


@router.get("/risks", response_model=list[ItemResponse])
def risk_items(days: int = 14) -> list[ItemResponse]:
    with SessionLocal() as session:
        return [_item_response(item) for item in get_risk_items(session, days=days)]


@router.get("/courses/{course_id}/snapshot")
def course_snapshot(course_id: int) -> dict:
    with SessionLocal() as session:
        payload = get_course_snapshot(session, course_id)
        if not payload:
            raise HTTPException(status_code=404, detail="Course not found")
        return {
            "course": CourseResponse(
                id=payload["course"].id,
                external_id=payload["course"].external_id,
                display_name=payload["course"].display_name,
                shortname=payload["course"].shortname,
                course_url=payload["course"].course_url,
                updated_at=payload["course"].updated_at,
            ).model_dump(),
            "items": [_item_response(item).model_dump() for item in payload["items"]],
        }


@router.get("/courses/{course_id}/brief", response_model=CourseBriefResponse)
def course_brief(course_id: int) -> CourseBriefResponse:
    with SessionLocal() as session:
        payload = get_course_brief(session, course_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="Course not found")
        return CourseBriefResponse(
            course=CourseResponse(
                id=payload["course"].id,
                external_id=payload["course"].external_id,
                display_name=payload["course"].display_name,
                shortname=payload["course"].shortname,
                course_url=payload["course"].course_url,
                updated_at=payload["course"].updated_at,
            ),
            summary_short=payload["summary_short"],
            origin=payload["origin"],
            items=[_brief_item_response(brief) for brief in payload["items"]],
        )


@router.post("/items/{item_id}/acknowledge", response_model=AcknowledgeResponse)
def acknowledge(item_id: int) -> AcknowledgeResponse:
    with SessionLocal() as session:
        ok = acknowledge_item(session, item_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Item not found")
        session.commit()
        return AcknowledgeResponse(ok=True)


@router.post("/sync/run/{collector_name}", response_model=SyncResult)
def sync_collector(collector_name: str) -> SyncResult:
    if collector_name not in COLLECTOR_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Unknown collector: {collector_name}")

    result = run_collector(collector_name)
    return SyncResult(collector=collector_name, status=result["status"], stats=result["stats"])


@router.get("/sync/collectors", response_model=list[str])
def collectors() -> list[str]:
    return sorted(COLLECTOR_REGISTRY.keys())


@router.post("/notifications/dispatch")
def dispatch_notifications() -> dict:
    with SessionLocal() as session:
        schedule_daily_digest(session)
        result = dispatch_due_notifications(session)
        session.commit()
        return result


@router.get("/notifications/digest")
def preview_digest() -> dict[str, str]:
    with SessionLocal() as session:
        return {"digest": build_digest_message(session)}
