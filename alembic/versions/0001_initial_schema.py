"""initial schema

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-03-23 18:18:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "source_accounts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_type", sa.String(length=50), nullable=False),
        sa.Column("label", sa.String(length=100), nullable=False),
        sa.Column("base_url", sa.String(length=500), nullable=False),
        sa.Column("auth_mode", sa.String(length=50), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("auth_health", sa.String(length=50), nullable=False, server_default="unknown"),
        sa.Column("last_auth_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("source_type", "label", name="uq_source_accounts_type_label"),
    )

    op.create_table(
        "collector_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("collector_name", sa.String(length=100), nullable=False),
        sa.Column("source_account_id", sa.Integer(), sa.ForeignKey("source_accounts.id"), nullable=False),
        sa.Column("status", sa.String(length=50), nullable=False),
        sa.Column("checkpoint", sa.JSON(), nullable=True),
        sa.Column("stats", sa.JSON(), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "courses",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_account_id", sa.Integer(), sa.ForeignKey("source_accounts.id"), nullable=False),
        sa.Column("external_id", sa.String(length=100), nullable=False),
        sa.Column("shortname", sa.String(length=255), nullable=True),
        sa.Column("fullname", sa.String(length=500), nullable=False),
        sa.Column("display_name", sa.String(length=500), nullable=False),
        sa.Column("course_url", sa.String(length=1000), nullable=True),
        sa.Column("visible", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("source_account_id", "external_id", name="uq_courses_source_external"),
    )

    op.create_table(
        "source_objects",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_account_id", sa.Integer(), sa.ForeignKey("source_accounts.id"), nullable=False),
        sa.Column("course_id", sa.Integer(), sa.ForeignKey("courses.id"), nullable=True),
        sa.Column("external_id", sa.String(length=100), nullable=False),
        sa.Column("object_type", sa.String(length=100), nullable=False),
        sa.Column("parent_external_id", sa.String(length=100), nullable=True),
        sa.Column("source_url", sa.String(length=1000), nullable=True),
        sa.Column("current_hash", sa.String(length=64), nullable=False),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint(
            "source_account_id",
            "external_id",
            "object_type",
            name="uq_source_objects_source_external_type",
        ),
    )

    op.create_table(
        "raw_artifacts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_object_id", sa.Integer(), sa.ForeignKey("source_objects.id"), nullable=True),
        sa.Column("collector_run_id", sa.Integer(), sa.ForeignKey("collector_runs.id"), nullable=False),
        sa.Column("artifact_type", sa.String(length=100), nullable=False),
        sa.Column("mime_type", sa.String(length=255), nullable=False),
        sa.Column("storage_path", sa.String(length=1000), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("extraction_status", sa.String(length=50), nullable=False, server_default="not_applicable"),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "normalized_items",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_object_id", sa.Integer(), sa.ForeignKey("source_objects.id"), nullable=False),
        sa.Column("course_id", sa.Integer(), sa.ForeignKey("courses.id"), nullable=True),
        sa.Column("item_type", sa.String(length=100), nullable=False),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("body_text", sa.Text(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("starts_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("urgency", sa.String(length=50), nullable=False, server_default="normal"),
        sa.Column("status", sa.String(length=50), nullable=False, server_default="active"),
        sa.Column("primary_url", sa.String(length=1000), nullable=True),
        sa.Column("field_hash", sa.String(length=64), nullable=False),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("source_object_id", "item_type", name="uq_normalized_items_source_item_type"),
    )

    op.create_table(
        "item_versions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("normalized_item_id", sa.Integer(), sa.ForeignKey("normalized_items.id"), nullable=False),
        sa.Column("source_artifact_id", sa.Integer(), sa.ForeignKey("raw_artifacts.id"), nullable=True),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("changed_fields", sa.JSON(), nullable=False),
        sa.Column("previous_values", sa.JSON(), nullable=True),
        sa.Column("new_values", sa.JSON(), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "item_facts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("normalized_item_id", sa.Integer(), sa.ForeignKey("normalized_items.id"), nullable=False),
        sa.Column("source_artifact_id", sa.Integer(), sa.ForeignKey("raw_artifacts.id"), nullable=True),
        sa.Column("fact_type", sa.String(length=100), nullable=False),
        sa.Column("value_json", sa.JSON(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False, server_default="1.0"),
        sa.Column("extractor_type", sa.String(length=50), nullable=False),
        sa.Column("source_span", sa.String(length=500), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "notifications",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("normalized_item_id", sa.Integer(), sa.ForeignKey("normalized_items.id"), nullable=False),
        sa.Column("channel", sa.String(length=50), nullable=False),
        sa.Column("severity", sa.String(length=50), nullable=False),
        sa.Column("kind", sa.String(length=50), nullable=False),
        sa.Column("dedup_key", sa.String(length=255), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("dedup_key", name="uq_notifications_dedup_key"),
    )

    op.create_table(
        "acknowledgements",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("notification_id", sa.Integer(), sa.ForeignKey("notifications.id"), nullable=True),
        sa.Column("normalized_item_id", sa.Integer(), sa.ForeignKey("normalized_items.id"), nullable=True),
        sa.Column("actor", sa.String(length=100), nullable=False),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("acknowledgements")
    op.drop_table("notifications")
    op.drop_table("item_facts")
    op.drop_table("item_versions")
    op.drop_table("normalized_items")
    op.drop_table("raw_artifacts")
    op.drop_table("source_objects")
    op.drop_table("courses")
    op.drop_table("collector_runs")
    op.drop_table("source_accounts")
