from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    environment: str


class CourseResponse(BaseModel):
    id: int
    external_id: str
    display_name: str
    shortname: str | None
    course_url: str | None
    updated_at: datetime


class CollectorRunResponse(BaseModel):
    collector_name: str
    status: str
    stats: dict


class SyncResult(BaseModel):
    collector: str
    status: str
    stats: dict
