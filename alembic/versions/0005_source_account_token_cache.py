"""cache moodle access token on source account

Revision ID: 0005_source_account_token_cache
Revises: 0004_item_briefs
Create Date: 2026-03-27 18:48:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0005_source_account_token_cache"
down_revision = "0004_item_briefs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("source_accounts", sa.Column("access_token", sa.String(length=255), nullable=True))
    op.add_column("source_accounts", sa.Column("access_token_fetched_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("source_accounts", "access_token_fetched_at")
    op.drop_column("source_accounts", "access_token")
