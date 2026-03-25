"""item briefs projection

Revision ID: 0004_item_briefs
Revises: 0003_telegram_command_state
Create Date: 2026-03-25 18:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0004_item_briefs"
down_revision = "0003_telegram_command_state"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "item_briefs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("normalized_item_id", sa.Integer(), sa.ForeignKey("normalized_items.id"), nullable=False),
        sa.Column("source_artifact_id", sa.Integer(), sa.ForeignKey("raw_artifacts.id"), nullable=True),
        sa.Column("llm_job_id", sa.Integer(), sa.ForeignKey("llm_jobs.id"), nullable=True),
        sa.Column("origin", sa.String(length=50), nullable=False, server_default="stored"),
        sa.Column("model", sa.String(length=255), nullable=True),
        sa.Column("summary_short", sa.Text(), nullable=False),
        sa.Column("summary_bullets", sa.JSON(), nullable=False),
        sa.Column("key_dates", sa.JSON(), nullable=False),
        sa.Column("key_requirements", sa.JSON(), nullable=False),
        sa.Column("risk_flags", sa.JSON(), nullable=False),
        sa.Column("course_context", sa.JSON(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False, server_default="0"),
        sa.Column("source_refs", sa.JSON(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("normalized_item_id", name="uq_item_briefs_normalized_item"),
    )


def downgrade() -> None:
    op.drop_table("item_briefs")
