"""stage2 stage7 schema

Revision ID: 0002_stage2_stage7_schema
Revises: 0001_initial_schema
Create Date: 2026-03-23 18:55:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0002_stage2_stage7_schema"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("source_accounts", sa.Column("metadata_json", sa.JSON(), nullable=True))

    op.add_column("raw_artifacts", sa.Column("source_url", sa.String(length=1000), nullable=True))

    op.add_column(
        "normalized_items",
        sa.Column("review_status", sa.String(length=50), nullable=False, server_default="none"),
    )
    op.add_column("normalized_items", sa.Column("review_reason", sa.String(length=255), nullable=True))

    op.add_column(
        "notifications",
        sa.Column("ack_required", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "notifications",
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column("notifications", sa.Column("delivery_error", sa.Text(), nullable=True))

    op.create_table(
        "llm_jobs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("normalized_item_id", sa.Integer(), sa.ForeignKey("normalized_items.id"), nullable=False),
        sa.Column("raw_artifact_id", sa.Integer(), sa.ForeignKey("raw_artifacts.id"), nullable=True),
        sa.Column("job_type", sa.String(length=100), nullable=False),
        sa.Column("provider", sa.String(length=100), nullable=False),
        sa.Column("model", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=50), nullable=False),
        sa.Column("request_payload", sa.JSON(), nullable=True),
        sa.Column("response_payload", sa.JSON(), nullable=True),
        sa.Column("output_text", sa.Text(), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("llm_jobs")
    op.drop_column("notifications", "delivery_error")
    op.drop_column("notifications", "attempt_count")
    op.drop_column("notifications", "ack_required")
    op.drop_column("normalized_items", "review_reason")
    op.drop_column("normalized_items", "review_status")
    op.drop_column("raw_artifacts", "source_url")
    op.drop_column("source_accounts", "metadata_json")
