"""Add dispatch_outbox and dispatch_log.idempotency_key.

Revision ID: 20260712_0002
Revises: 20260707_0001
Create Date: 2026-07-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "20260712_0002"
down_revision: Union[str, None] = "20260707_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "dispatch_outbox",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("idempotency_key", sa.String(length=256), nullable=False),
        sa.Column("alert_id", sa.String(length=36), nullable=False),
        sa.Column("channel", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_dispatch_outbox_idempotency_key"),
    )
    op.create_index("ix_dispatch_outbox_alert_id", "dispatch_outbox", ["alert_id"], unique=False)
    op.create_index("ix_dispatch_outbox_status", "dispatch_outbox", ["status"], unique=False)
    op.create_index(
        "ix_dispatch_outbox_next_attempt_at",
        "dispatch_outbox",
        ["next_attempt_at"],
        unique=False,
    )

    op.add_column(
        "dispatch_log",
        sa.Column("idempotency_key", sa.String(length=256), nullable=True),
    )
    op.create_index(
        "ix_dispatch_log_idempotency_key",
        "dispatch_log",
        ["idempotency_key"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_dispatch_log_idempotency_key", table_name="dispatch_log")
    op.drop_column("dispatch_log", "idempotency_key")
    op.drop_index("ix_dispatch_outbox_next_attempt_at", table_name="dispatch_outbox")
    op.drop_index("ix_dispatch_outbox_status", table_name="dispatch_outbox")
    op.drop_index("ix_dispatch_outbox_alert_id", table_name="dispatch_outbox")
    op.drop_table("dispatch_outbox")
