"""persist normalized item completion state

Revision ID: 0006_normalized_item_completion_state
Revises: 0005_source_account_token_cache
Create Date: 2026-03-28 22:35:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0006_normalized_item_completion_state"
down_revision = "0005_source_account_token_cache"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "normalized_items",
        sa.Column("completion_state", sa.String(length=50), nullable=False, server_default="unknown"),
    )


def downgrade() -> None:
    op.drop_column("normalized_items", "completion_state")
