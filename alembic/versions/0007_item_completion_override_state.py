"""persist manual item completion override state

Revision ID: 0007_completion_override
Revises: 0006_item_completion
Create Date: 2026-03-29 19:35:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0007_completion_override"
down_revision = "0006_item_completion"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        sa.text(
            "ALTER TABLE normalized_items "
            "ADD COLUMN IF NOT EXISTS completion_override_state VARCHAR(50)"
        )
    )


def downgrade() -> None:
    op.drop_column("normalized_items", "completion_override_state")
