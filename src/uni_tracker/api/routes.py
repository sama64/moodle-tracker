from __future__ import annotations

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from uni_tracker.config import get_settings
from uni_tracker.db import SessionLocal
from uni_tracker.models import Course
from uni_tracker.schemas import CollectorRunResponse, CourseResponse, HealthResponse, SyncResult
from uni_tracker.services.sync import COLLECTOR_REGISTRY, run_collector


router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(status="ok", environment=settings.app_env)


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


@router.post("/sync/run/{collector_name}", response_model=SyncResult)
def sync_collector(collector_name: str) -> SyncResult:
    if collector_name not in COLLECTOR_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Unknown collector: {collector_name}")

    result = run_collector(collector_name)
    return SyncResult(collector=collector_name, status=result["status"], stats=result["stats"])


@router.get("/sync/collectors", response_model=list[str])
def collectors() -> list[str]:
    return sorted(COLLECTOR_REGISTRY.keys())
