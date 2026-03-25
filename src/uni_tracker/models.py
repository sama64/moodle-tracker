from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, JSON, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from uni_tracker.db import Base


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class SourceAccount(TimestampMixin, Base):
    __tablename__ = "source_accounts"
    __table_args__ = (UniqueConstraint("source_type", "label", name="uq_source_accounts_type_label"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_type: Mapped[str] = mapped_column(String(50))
    label: Mapped[str] = mapped_column(String(100))
    base_url: Mapped[str] = mapped_column(String(500))
    auth_mode: Mapped[str] = mapped_column(String(50))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    auth_health: Mapped[str] = mapped_column(String(50), default="unknown")
    last_auth_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    courses: Mapped[list["Course"]] = relationship(back_populates="source_account")


class CollectorRun(Base):
    __tablename__ = "collector_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    collector_name: Mapped[str] = mapped_column(String(100))
    source_account_id: Mapped[int] = mapped_column(ForeignKey("source_accounts.id"))
    status: Mapped[str] = mapped_column(String(50))
    checkpoint: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    stats: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    source_account: Mapped["SourceAccount"] = relationship()
    raw_artifacts: Mapped[list["RawArtifact"]] = relationship(back_populates="collector_run")


class Course(TimestampMixin, Base):
    __tablename__ = "courses"
    __table_args__ = (UniqueConstraint("source_account_id", "external_id", name="uq_courses_source_external"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_account_id: Mapped[int] = mapped_column(ForeignKey("source_accounts.id"))
    external_id: Mapped[str] = mapped_column(String(100))
    shortname: Mapped[str | None] = mapped_column(String(255), nullable=True)
    fullname: Mapped[str] = mapped_column(String(500))
    display_name: Mapped[str] = mapped_column(String(500))
    course_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    visible: Mapped[bool] = mapped_column(Boolean, default=True)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    source_account: Mapped["SourceAccount"] = relationship(back_populates="courses")
    source_objects: Mapped[list["SourceObject"]] = relationship(back_populates="course")


class SourceObject(TimestampMixin, Base):
    __tablename__ = "source_objects"
    __table_args__ = (
        UniqueConstraint(
            "source_account_id",
            "external_id",
            "object_type",
            name="uq_source_objects_source_external_type",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_account_id: Mapped[int] = mapped_column(ForeignKey("source_accounts.id"))
    course_id: Mapped[int | None] = mapped_column(ForeignKey("courses.id"), nullable=True)
    external_id: Mapped[str] = mapped_column(String(100))
    object_type: Mapped[str] = mapped_column(String(100))
    parent_external_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    source_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    current_hash: Mapped[str] = mapped_column(String(64))
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    course: Mapped["Course | None"] = relationship(back_populates="source_objects")
    raw_artifacts: Mapped[list["RawArtifact"]] = relationship(back_populates="source_object")
    normalized_items: Mapped[list["NormalizedItem"]] = relationship(back_populates="source_object")


class RawArtifact(Base):
    __tablename__ = "raw_artifacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_object_id: Mapped[int | None] = mapped_column(ForeignKey("source_objects.id"), nullable=True)
    collector_run_id: Mapped[int] = mapped_column(ForeignKey("collector_runs.id"))
    artifact_type: Mapped[str] = mapped_column(String(100))
    mime_type: Mapped[str] = mapped_column(String(255))
    storage_path: Mapped[str] = mapped_column(String(1000))
    content_hash: Mapped[str] = mapped_column(String(64))
    size_bytes: Mapped[int] = mapped_column(Integer)
    extraction_status: Mapped[str] = mapped_column(String(50), default="not_applicable")
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    source_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    source_object: Mapped["SourceObject | None"] = relationship(back_populates="raw_artifacts")
    collector_run: Mapped["CollectorRun"] = relationship(back_populates="raw_artifacts")


class NormalizedItem(TimestampMixin, Base):
    __tablename__ = "normalized_items"
    __table_args__ = (
        UniqueConstraint("source_object_id", "item_type", name="uq_normalized_items_source_item_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_object_id: Mapped[int] = mapped_column(ForeignKey("source_objects.id"))
    course_id: Mapped[int | None] = mapped_column(ForeignKey("courses.id"), nullable=True)
    item_type: Mapped[str] = mapped_column(String(100))
    title: Mapped[str] = mapped_column(String(500))
    body_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    starts_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    urgency: Mapped[str] = mapped_column(String(50), default="normal")
    status: Mapped[str] = mapped_column(String(50), default="active")
    primary_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    field_hash: Mapped[str] = mapped_column(String(64))
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    review_status: Mapped[str] = mapped_column(String(50), default="none")
    review_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)

    source_object: Mapped["SourceObject"] = relationship(back_populates="normalized_items")
    versions: Mapped[list["ItemVersion"]] = relationship(back_populates="normalized_item")
    facts: Mapped[list["ItemFact"]] = relationship(back_populates="normalized_item")
    llm_jobs: Mapped[list["LLMJob"]] = relationship(back_populates="normalized_item")


class ItemVersion(Base):
    __tablename__ = "item_versions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    normalized_item_id: Mapped[int] = mapped_column(ForeignKey("normalized_items.id"))
    source_artifact_id: Mapped[int | None] = mapped_column(ForeignKey("raw_artifacts.id"), nullable=True)
    version_number: Mapped[int] = mapped_column(Integer)
    changed_fields: Mapped[list[str]] = mapped_column(JSON)
    previous_values: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    new_values: Mapped[dict[str, Any]] = mapped_column(JSON)
    changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    normalized_item: Mapped["NormalizedItem"] = relationship(back_populates="versions")


class ItemFact(Base):
    __tablename__ = "item_facts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    normalized_item_id: Mapped[int] = mapped_column(ForeignKey("normalized_items.id"))
    source_artifact_id: Mapped[int | None] = mapped_column(ForeignKey("raw_artifacts.id"), nullable=True)
    fact_type: Mapped[str] = mapped_column(String(100))
    value_json: Mapped[dict[str, Any]] = mapped_column(JSON)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    extractor_type: Mapped[str] = mapped_column(String(50))
    source_span: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    normalized_item: Mapped["NormalizedItem"] = relationship(back_populates="facts")


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = (UniqueConstraint("dedup_key", name="uq_notifications_dedup_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    normalized_item_id: Mapped[int] = mapped_column(ForeignKey("normalized_items.id"))
    channel: Mapped[str] = mapped_column(String(50))
    severity: Mapped[str] = mapped_column(String(50))
    kind: Mapped[str] = mapped_column(String(50))
    dedup_key: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    ack_required: Mapped[bool] = mapped_column(Boolean, default=False)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    delivery_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Acknowledgement(Base):
    __tablename__ = "acknowledgements"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    notification_id: Mapped[int | None] = mapped_column(ForeignKey("notifications.id"), nullable=True)
    normalized_item_id: Mapped[int | None] = mapped_column(ForeignKey("normalized_items.id"), nullable=True)
    actor: Mapped[str] = mapped_column(String(100))
    acknowledged_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SystemState(Base):
    __tablename__ = "system_state"
    __table_args__ = (UniqueConstraint("key", name="uq_system_state_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(100))
    value_json: Mapped[dict[str, Any]] = mapped_column(JSON)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class LLMJob(Base):
    __tablename__ = "llm_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    normalized_item_id: Mapped[int] = mapped_column(ForeignKey("normalized_items.id"))
    raw_artifact_id: Mapped[int | None] = mapped_column(ForeignKey("raw_artifacts.id"), nullable=True)
    job_type: Mapped[str] = mapped_column(String(100))
    provider: Mapped[str] = mapped_column(String(100))
    model: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(50))
    request_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    response_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    output_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    normalized_item: Mapped["NormalizedItem"] = relationship(back_populates="llm_jobs")
