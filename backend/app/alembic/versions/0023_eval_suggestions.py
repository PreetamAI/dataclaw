"""eval suggestions: diagnose + improvement workflow (Phase 5)

Revision ID: 0023_eval_suggestions
Revises: 0022_eval_runs
Create Date: 2026-06-02

eval_suggestions is one row per (run, suggested-fix). The diagnose service
populates these from a failing run; the apply service writes the proposed
value through a kind-specific path (golden_query → new eval_case;
prompt_diff → AppSetting override; etc.). Nothing in this table mutates
source code or runtime state on its own — every row requires an explicit
Apply.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0023_eval_suggestions"
down_revision: str | None = "0022_eval_runs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "eval_suggestions",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "eval_run_id",
            sa.String(),
            sa.ForeignKey("eval_runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "workspace_id",
            sa.String(),
            sa.ForeignKey("workspaces.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("kind", sa.String(length=40), nullable=False),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("rationale", sa.Text(), nullable=False, server_default=""),
        sa.Column("current_value", sa.Text(), nullable=True),
        sa.Column("proposed_value", sa.Text(), nullable=False),
        sa.Column("target", sa.String(length=200), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=False, server_default="0.7"),
        sa.Column("source", sa.String(length=20), nullable=False, server_default="rules"),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("apply_payload", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "applied_by",
            sa.String(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("apply_result", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_eval_suggestions_eval_run_id", "eval_suggestions", ["eval_run_id"])
    op.create_index("ix_eval_suggestions_workspace_id", "eval_suggestions", ["workspace_id"])
    op.create_index("ix_eval_suggestions_kind", "eval_suggestions", ["kind"])
    op.create_index("ix_eval_suggestions_status", "eval_suggestions", ["status"])


def downgrade() -> None:
    op.drop_index("ix_eval_suggestions_status", table_name="eval_suggestions")
    op.drop_index("ix_eval_suggestions_kind", table_name="eval_suggestions")
    op.drop_index("ix_eval_suggestions_workspace_id", table_name="eval_suggestions")
    op.drop_index("ix_eval_suggestions_eval_run_id", table_name="eval_suggestions")
    op.drop_table("eval_suggestions")
