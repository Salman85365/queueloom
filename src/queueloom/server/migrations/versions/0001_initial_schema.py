"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-09-05 07:13:23.837060
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "projects",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("api_key_hash", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("api_key_hash"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "alert_rules",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("project_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("environment", sa.String(length=64), nullable=True),
        sa.Column("task_name", sa.String(length=255), nullable=True),
        sa.Column("window_minutes", sa.Integer(), nullable=False),
        sa.Column("threshold", sa.Float(), nullable=False),
        sa.Column("min_runs", sa.Integer(), nullable=False),
        sa.Column("cooldown_minutes", sa.Integer(), nullable=False),
        sa.Column("webhook_url", sa.String(length=2000), nullable=False),
        sa.Column("webhook_format", sa.String(length=16), nullable=False),
        sa.Column("webhook_secret", sa.String(length=200), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_triggered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("alert_rules", schema=None) as batch_op:
        batch_op.create_index(
            "ix_alert_rules_project_enabled", ["project_id", "enabled"], unique=False
        )

    op.create_table(
        "task_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.String(length=64), nullable=False),
        sa.Column("project_id", sa.Integer(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("task_id", sa.String(length=255), nullable=False),
        sa.Column("task_name", sa.String(length=255), nullable=True),
        sa.Column("environment", sa.String(length=64), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "payload", sa.JSON().with_variant(postgresql.JSONB(), "postgresql"), nullable=False
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("event_id"),
    )
    with op.batch_alter_table("task_events", schema=None) as batch_op:
        batch_op.create_index(
            "ix_task_events_project_task", ["project_id", "task_id"], unique=False
        )
        batch_op.create_index(
            "ix_task_events_project_ts", ["project_id", "timestamp"], unique=False
        )

    op.create_table(
        "task_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("project_id", sa.Integer(), nullable=False),
        sa.Column("task_id", sa.String(length=255), nullable=False),
        sa.Column("task_name", sa.String(length=255), nullable=True),
        sa.Column("queue", sa.String(length=255), nullable=True),
        sa.Column("environment", sa.String(length=64), nullable=False),
        sa.Column("worker", sa.String(length=255), nullable=True),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_event_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("eta", sa.DateTime(timezone=True), nullable=True),
        sa.Column("queue_latency_ms", sa.Float(), nullable=True),
        sa.Column("duration_ms", sa.Float(), nullable=True),
        sa.Column("retries", sa.Integer(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("exception_type", sa.String(length=200), nullable=True),
        sa.Column("exception_message", sa.Text(), nullable=True),
        sa.Column("traceback", sa.Text(), nullable=True),
        sa.Column("parent_id", sa.String(length=255), nullable=True),
        sa.Column("root_id", sa.String(length=255), nullable=True),
        sa.Column("args_repr", sa.Text(), nullable=True),
        sa.Column("kwargs_repr", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("project_id", "task_id", name="uq_task_runs_project_task"),
    )
    with op.batch_alter_table("task_runs", schema=None) as batch_op:
        batch_op.create_index(
            "ix_task_runs_project_env", ["project_id", "environment"], unique=False
        )
        batch_op.create_index(
            "ix_task_runs_project_last_event", ["project_id", "last_event_at"], unique=False
        )
        batch_op.create_index(
            "ix_task_runs_project_name", ["project_id", "task_name"], unique=False
        )
        batch_op.create_index("ix_task_runs_project_state", ["project_id", "state"], unique=False)

    op.create_table(
        "alert_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("rule_id", sa.Integer(), nullable=False),
        sa.Column("project_id", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("failure_rate", sa.Float(), nullable=True),
        sa.Column("failed", sa.Integer(), nullable=False),
        sa.Column("finished", sa.Integer(), nullable=False),
        sa.Column("window_since", sa.DateTime(timezone=True), nullable=True),
        sa.Column("window_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered", sa.Boolean(), nullable=False),
        sa.Column("delivery_status", sa.String(length=200), nullable=True),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["rule_id"], ["alert_rules.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("alert_events", schema=None) as batch_op:
        batch_op.create_index(
            "ix_alert_events_project_created", ["project_id", "created_at"], unique=False
        )


def downgrade() -> None:
    with op.batch_alter_table("alert_events", schema=None) as batch_op:
        batch_op.drop_index("ix_alert_events_project_created")

    op.drop_table("alert_events")
    with op.batch_alter_table("task_runs", schema=None) as batch_op:
        batch_op.drop_index("ix_task_runs_project_state")
        batch_op.drop_index("ix_task_runs_project_name")
        batch_op.drop_index("ix_task_runs_project_last_event")
        batch_op.drop_index("ix_task_runs_project_env")

    op.drop_table("task_runs")
    with op.batch_alter_table("task_events", schema=None) as batch_op:
        batch_op.drop_index("ix_task_events_project_ts")
        batch_op.drop_index("ix_task_events_project_task")

    op.drop_table("task_events")
    with op.batch_alter_table("alert_rules", schema=None) as batch_op:
        batch_op.drop_index("ix_alert_rules_project_enabled")

    op.drop_table("alert_rules")
    op.drop_table("projects")
