"""telegram command state

Revision ID: 0003_telegram_command_state
Revises: 0002_stage2_stage7_schema
Create Date: 2026-03-25 17:12:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0003_telegram_command_state"
down_revision = "0002_stage2_stage7_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "system_state",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("key", sa.String(length=100), nullable=False),
        sa.Column("value_json", sa.JSON(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("key", name="uq_system_state_key"),
    )


def downgrade() -> None:
    op.drop_table("system_state")
