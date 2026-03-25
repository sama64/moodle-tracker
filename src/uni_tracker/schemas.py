from __future__ import annotations

from datetime import datetime
from typing import Any

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


class ItemResponse(BaseModel):
    id: int
    course_id: int | None
    item_type: str
    title: str
    body_text: str | None
    published_at: datetime | None
    starts_at: datetime | None
    due_at: datetime | None
    primary_url: str | None
    review_status: str
    review_reason: str | None
    updated_at: datetime


class ProvenanceFactResponse(BaseModel):
    fact_type: str
    value_json: dict
    confidence: float
    extractor_type: str
    source_span: str | None


class NotificationResponse(BaseModel):
    id: int
    severity: str
    kind: str
    scheduled_for: datetime
    sent_at: datetime | None
    delivery_error: str | None


class ItemProvenanceResponse(BaseModel):
    item: ItemResponse
    facts: list[ProvenanceFactResponse]
    notifications: list[NotificationResponse]


class ItemBriefResponse(BaseModel):
    brief_id: int | None
    origin: str
    model: str | None
    generated_at: datetime | None
    summary_short: str
    summary_bullets: list[Any]
    key_dates: list[Any]
    key_requirements: list[Any]
    risk_flags: list[Any]
    course_context: dict[str, Any]
    confidence: float
    source_refs: list[Any]
    item: ItemResponse


class CourseBriefResponse(BaseModel):
    course: CourseResponse
    summary_short: str
    origin: str
    items: list[ItemBriefResponse]


class AcknowledgeResponse(BaseModel):
    ok: bool


class HealthSnapshotResponse(BaseModel):
    status: str
    environment: str
    details: dict
