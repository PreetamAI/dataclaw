"""evals foundation: chat tracing + feedback

Revision ID: 0020_evals_foundation
Revises: 0019_chat_message_action
Create Date: 2026-06-02

Adds the Phase 1 surface for the Evals + Langfuse intelligence layer:
  * chat_threads.kind      - segregate user threads from synthetic eval runs
  * chat_messages.trace_id - cross-link to Langfuse / local span tree
  * chat_spans             - always-on local trace sink (Langfuse is optional)
  * feedback               - thumbs up/down + corrections from chat
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0020_evals_foundation"
down_revision: str | None = "0019_chat_message_action"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("chat_threads") as batch_op:
        batch_op.add_column(
            sa.Column(
                "kind",
                sa.String(length=20),
                nullable=False,
                server_default="user",
            )
        )
        batch_op.create_index("ix_chat_threads_kind", ["kind"])

    with op.batch_alter_table("chat_messages") as batch_op:
        batch_op.add_column(sa.Column("trace_id", sa.String(length=64), nullable=True))
        batch_op.create_index("ix_chat_messages_trace_id", ["trace_id"])

    op.create_table(
        "chat_spans",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "chat_message_id",
            sa.String(),
            sa.ForeignKey("chat_messages.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "parent_span_id",
            sa.String(),
            sa.ForeignKey("chat_spans.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("kind", sa.String(length=20), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="ok"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("input", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("output", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("metadata", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("usage", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("model", sa.String(length=120), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_chat_spans_chat_message_id", "chat_spans", ["chat_message_id"])
    op.create_index("ix_chat_spans_parent_span_id", "chat_spans", ["parent_span_id"])
    op.create_index("ix_chat_spans_kind", "chat_spans", ["kind"])
    op.create_index("ix_chat_spans_started_at", "chat_spans", ["started_at"])

    op.create_table(
        "feedback",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "chat_message_id",
            sa.String(),
            sa.ForeignKey("chat_messages.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            sa.String(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("sentiment", sa.String(length=20), nullable=False),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("langfuse_score_id", sa.String(length=80), nullable=True),
        sa.Column("eval_case_id", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_feedback_chat_message_id", "feedback", ["chat_message_id"])
    op.create_index("ix_feedback_user_id", "feedback", ["user_id"])
    op.create_index("ix_feedback_sentiment", "feedback", ["sentiment"])
    op.create_index("ix_feedback_eval_case_id", "feedback", ["eval_case_id"])


def downgrade() -> None:
    op.drop_index("ix_feedback_eval_case_id", table_name="feedback")
    op.drop_index("ix_feedback_sentiment", table_name="feedback")
    op.drop_index("ix_feedback_user_id", table_name="feedback")
    op.drop_index("ix_feedback_chat_message_id", table_name="feedback")
    op.drop_table("feedback")

    op.drop_index("ix_chat_spans_started_at", table_name="chat_spans")
    op.drop_index("ix_chat_spans_kind", table_name="chat_spans")
    op.drop_index("ix_chat_spans_parent_span_id", table_name="chat_spans")
    op.drop_index("ix_chat_spans_chat_message_id", table_name="chat_spans")
    op.drop_table("chat_spans")

    with op.batch_alter_table("chat_messages") as batch_op:
        batch_op.drop_index("ix_chat_messages_trace_id")
        batch_op.drop_column("trace_id")

    with op.batch_alter_table("chat_threads") as batch_op:
        batch_op.drop_index("ix_chat_threads_kind")
        batch_op.drop_column("kind")
