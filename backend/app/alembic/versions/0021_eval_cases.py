"""eval cases: manual + feedback-derived eval cases (Phase 2)

Revision ID: 0021_eval_cases
Revises: 0020_evals_foundation
Create Date: 2026-06-02

The eval_cases table backs BOTH golden queries (roadmap Theme 2) and the
eval-dataset surface (roadmap Theme 3). status separates them. See the
model docstring in app/models/domain.py for the lifecycle.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0021_eval_cases"
down_revision: str | None = "0020_evals_foundation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "eval_cases",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "workspace_id",
            sa.String(),
            sa.ForeignKey("workspaces.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("expected_answer", sa.Text(), nullable=True),
        sa.Column("expected_sql", sa.Text(), nullable=True),
        sa.Column("expected_connector_slug", sa.String(length=80), nullable=True),
        sa.Column("expected_tool", sa.String(length=160), nullable=True),
        sa.Column("expected_citations", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("expected_result_hash", sa.String(length=64), nullable=True),
        sa.Column("expected_result_preview", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("tags", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="candidate"),
        sa.Column("origin", sa.String(length=40), nullable=False, server_default="manual"),
        sa.Column(
            "source_chat_message_id",
            sa.String(),
            sa.ForeignKey("chat_messages.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_by",
            sa.String(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_eval_cases_workspace_id", "eval_cases", ["workspace_id"])
    op.create_index("ix_eval_cases_status", "eval_cases", ["status"])
    op.create_index("ix_eval_cases_origin", "eval_cases", ["origin"])
    op.create_index(
        "ix_eval_cases_expected_connector_slug",
        "eval_cases",
        ["expected_connector_slug"],
    )
    op.create_index(
        "ix_eval_cases_source_chat_message_id",
        "eval_cases",
        ["source_chat_message_id"],
    )
    # Common filter combo: list all approved/golden cases per workspace.
    op.create_index(
        "ix_eval_cases_workspace_status",
        "eval_cases",
        ["workspace_id", "status"],
    )


def downgrade() -> None:
    op.drop_index("ix_eval_cases_workspace_status", table_name="eval_cases")
    op.drop_index("ix_eval_cases_source_chat_message_id", table_name="eval_cases")
    op.drop_index("ix_eval_cases_expected_connector_slug", table_name="eval_cases")
    op.drop_index("ix_eval_cases_origin", table_name="eval_cases")
    op.drop_index("ix_eval_cases_status", table_name="eval_cases")
    op.drop_index("ix_eval_cases_workspace_id", table_name="eval_cases")
    op.drop_table("eval_cases")
