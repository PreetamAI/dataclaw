"""eval runs + results + per-workspace metric thresholds (Phase 4)

Revision ID: 0022_eval_runs
Revises: 0021_eval_cases
Create Date: 2026-06-02

eval_runs is one row per case-per-execution; eval_results is one row per
metric scored against an eval_run; eval_metric_thresholds is a per-workspace
override of the per-metric pass bar.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0022_eval_runs"
down_revision: str | None = "0021_eval_cases"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "eval_runs",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(),
            sa.ForeignKey("workspaces.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("batch_id", sa.String(length=64), nullable=False),
        sa.Column(
            "eval_case_id",
            sa.String(),
            sa.ForeignKey("eval_cases.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "chat_message_id",
            sa.String(),
            sa.ForeignKey("chat_messages.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("passed", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("failure_category", sa.String(length=40), nullable=True),
        sa.Column("actual_answer", sa.Text(), nullable=True),
        sa.Column("actual_sql", sa.Text(), nullable=True),
        sa.Column("actual_citations", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("actual_result_hash", sa.String(length=64), nullable=True),
        sa.Column("actual_result_preview", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("langfuse_trace_id", sa.String(length=80), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("prompt_tokens", sa.Integer(), nullable=True),
        sa.Column("completion_tokens", sa.Integer(), nullable=True),
        sa.Column("cost_usd", sa.Float(), nullable=True),
        sa.Column("model", sa.String(length=120), nullable=True),
        sa.Column("prompt_version", sa.String(length=80), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_eval_runs_workspace_id", "eval_runs", ["workspace_id"])
    op.create_index("ix_eval_runs_batch_id", "eval_runs", ["batch_id"])
    op.create_index("ix_eval_runs_eval_case_id", "eval_runs", ["eval_case_id"])
    op.create_index("ix_eval_runs_chat_message_id", "eval_runs", ["chat_message_id"])
    op.create_index("ix_eval_runs_passed", "eval_runs", ["passed"])
    op.create_index("ix_eval_runs_failure_category", "eval_runs", ["failure_category"])
    op.create_index("ix_eval_runs_created_at", "eval_runs", ["created_at"])
    op.create_index(
        "ix_eval_runs_workspace_batch",
        "eval_runs",
        ["workspace_id", "batch_id"],
    )
    op.create_index(
        "ix_eval_runs_case_created",
        "eval_runs",
        ["eval_case_id", "created_at"],
    )

    op.create_table(
        "eval_results",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "eval_run_id",
            sa.String(),
            sa.ForeignKey("eval_runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("metric", sa.String(length=60), nullable=False),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("passed", sa.Boolean(), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="ok"),
        sa.Column("detail", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
    )
    op.create_index("ix_eval_results_eval_run_id", "eval_results", ["eval_run_id"])
    op.create_index("ix_eval_results_metric", "eval_results", ["metric"])
    op.create_index("ix_eval_results_status", "eval_results", ["status"])

    op.create_table(
        "eval_metric_thresholds",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(),
            sa.ForeignKey("workspaces.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("metric", sa.String(length=60), nullable=False),
        sa.Column("pass_threshold", sa.Float(), nullable=False, server_default="0.5"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("workspace_id", "metric", name="uq_eval_thresholds_identity"),
    )
    op.create_index(
        "ix_eval_metric_thresholds_workspace_id",
        "eval_metric_thresholds",
        ["workspace_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_eval_metric_thresholds_workspace_id", table_name="eval_metric_thresholds")
    op.drop_table("eval_metric_thresholds")

    op.drop_index("ix_eval_results_status", table_name="eval_results")
    op.drop_index("ix_eval_results_metric", table_name="eval_results")
    op.drop_index("ix_eval_results_eval_run_id", table_name="eval_results")
    op.drop_table("eval_results")

    op.drop_index("ix_eval_runs_case_created", table_name="eval_runs")
    op.drop_index("ix_eval_runs_workspace_batch", table_name="eval_runs")
    op.drop_index("ix_eval_runs_created_at", table_name="eval_runs")
    op.drop_index("ix_eval_runs_failure_category", table_name="eval_runs")
    op.drop_index("ix_eval_runs_passed", table_name="eval_runs")
    op.drop_index("ix_eval_runs_chat_message_id", table_name="eval_runs")
    op.drop_index("ix_eval_runs_eval_case_id", table_name="eval_runs")
    op.drop_index("ix_eval_runs_batch_id", table_name="eval_runs")
    op.drop_index("ix_eval_runs_workspace_id", table_name="eval_runs")
    op.drop_table("eval_runs")
