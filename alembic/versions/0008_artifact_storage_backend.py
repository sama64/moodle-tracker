"""add remote artifact storage metadata

Revision ID: 0008_artifact_storage_backend
Revises: 0007_completion_override
Create Date: 2026-04-30 21:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0008_artifact_storage_backend"
down_revision = "0007_completion_override"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        sa.text(
            "ALTER TABLE raw_artifacts "
            "ADD COLUMN IF NOT EXISTS storage_backend VARCHAR(50) DEFAULT 'local' NOT NULL"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE raw_artifacts "
            "ADD COLUMN IF NOT EXISTS storage_bucket VARCHAR(255)"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE raw_artifacts "
            "ADD COLUMN IF NOT EXISTS storage_key VARCHAR(1000)"
        )
    )


def downgrade() -> None:
    op.drop_column("raw_artifacts", "storage_key")
    op.drop_column("raw_artifacts", "storage_bucket")
    op.drop_column("raw_artifacts", "storage_backend")
