from datetime import UTC, datetime

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, IdMixin, TimestampMixin


class User(IdMixin, TimestampMixin, Base):
    __tablename__ = "users"

    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    is_admin: Mapped[bool] = mapped_column(Boolean, default=True)


class Workspace(IdMixin, TimestampMixin, Base):
    __tablename__ = "workspaces"

    name: Mapped[str] = mapped_column(String(255))
    onboarding_complete: Mapped[bool] = mapped_column(Boolean, default=False)


class Connector(IdMixin, TimestampMixin, Base):
    __tablename__ = "connectors"

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"))
    slug: Mapped[str] = mapped_column(String(80), index=True)
    category: Mapped[str] = mapped_column(String(80))
    display_name: Mapped[str] = mapped_column(String(120))
    status: Mapped[str] = mapped_column(String(40), default="credential_required")
    credential_state: Mapped[str] = mapped_column(String(40), default="not_configured")
    sync_state: Mapped[str] = mapped_column(String(40), default="never_synced")
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_sync_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    encrypted_credentials: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_test_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    sync_summary: Mapped[dict] = mapped_column(JSON, default=dict)


class Dataset(IdMixin, TimestampMixin, Base):
    __tablename__ = "datasets"

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"))
    connector_id: Mapped[str | None] = mapped_column(ForeignKey("connectors.id", ondelete="CASCADE"), nullable=True)
    name: Mapped[str] = mapped_column(String(255))
    source_type: Mapped[str] = mapped_column(String(80))
    schema_name: Mapped[str] = mapped_column(String(255), default="public")
    tables: Mapped[list["TableAsset"]] = relationship(back_populates="dataset", cascade="all, delete-orphan")


class TableAsset(IdMixin, TimestampMixin, Base):
    __tablename__ = "table_assets"

    dataset_id: Mapped[str] = mapped_column(ForeignKey("datasets.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(255), index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    business_summary: Mapped[str] = mapped_column(Text, default="")
    freshness_status: Mapped[str] = mapped_column(String(80), default="fresh")
    row_count: Mapped[int] = mapped_column(Integer, default=0)
    tags: Mapped[list[str]] = mapped_column(JSON, default=list)
    columns: Mapped[list[dict]] = mapped_column(JSON, default=list)
    dataset: Mapped[Dataset] = relationship(back_populates="tables")


class KnowledgeDocument(IdMixin, TimestampMixin, Base):
    __tablename__ = "knowledge_documents"

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"))
    connector_slug: Mapped[str] = mapped_column(String(80))
    title: Mapped[str] = mapped_column(String(255))
    body: Mapped[str] = mapped_column(Text)
    related_tables: Mapped[list[str]] = mapped_column(JSON, default=list)


class WikiPage(IdMixin, TimestampMixin, Base):
    __tablename__ = "wiki_pages"
    __table_args__ = (UniqueConstraint("workspace_id", "path", name="uq_wiki_pages_workspace_path"),)

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    path: Mapped[str] = mapped_column(String(500), index=True)
    disk_path: Mapped[str] = mapped_column(String(1000))
    tier: Mapped[int] = mapped_column(Integer, default=1, index=True)
    source_type: Mapped[str] = mapped_column(String(80), index=True)
    source_id: Mapped[str] = mapped_column(String(255), index=True)
    title: Mapped[str] = mapped_column(String(255))
    body: Mapped[str] = mapped_column(Text)
    frontmatter: Mapped[dict] = mapped_column(JSON, default=dict)
    entities: Mapped[list[str]] = mapped_column(JSON, default=list)
    content_hash: Mapped[str] = mapped_column(String(64), index=True)
    disk_mtime: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class KnowledgeNode(IdMixin, TimestampMixin, Base):
    __tablename__ = "knowledge_nodes"
    __table_args__ = (
        UniqueConstraint("workspace_id", "type", "canonical_name", "connector_slug", name="uq_knowledge_nodes_identity"),
    )

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    type: Mapped[str] = mapped_column(String(80), index=True)
    canonical_name: Mapped[str] = mapped_column(String(255), index=True)
    connector_slug: Mapped[str] = mapped_column(String(80), default="unknown", index=True)
    source_type: Mapped[str] = mapped_column(String(80), default="unknown", index=True)
    aliases: Mapped[list[str]] = mapped_column(JSON, default=list)
    summary: Mapped[str] = mapped_column(Text, default="")
    summary_embedded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    primary_wiki_page_id: Mapped[str | None] = mapped_column(ForeignKey("wiki_pages.id", ondelete="SET NULL"), nullable=True)
    compile_run_id: Mapped[str | None] = mapped_column(String(80), index=True, nullable=True)


class KnowledgeEdge(IdMixin, TimestampMixin, Base):
    __tablename__ = "knowledge_edges"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "src_node_id",
            "dst_node_id",
            "relationship",
            "source",
            name="uq_knowledge_edges_identity",
        ),
    )

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    src_node_id: Mapped[str] = mapped_column(ForeignKey("knowledge_nodes.id", ondelete="CASCADE"), index=True)
    dst_node_id: Mapped[str] = mapped_column(ForeignKey("knowledge_nodes.id", ondelete="CASCADE"), index=True)
    relationship: Mapped[str] = mapped_column(String(80), index=True)
    evidence: Mapped[str] = mapped_column(Text, default="")
    confidence: Mapped[int] = mapped_column(Integer, default=100)
    source: Mapped[str] = mapped_column(String(80), index=True)
    compile_run_id: Mapped[str | None] = mapped_column(String(80), index=True, nullable=True)


class LineageEdge(IdMixin, TimestampMixin, Base):
    __tablename__ = "lineage_edges"

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"))
    source_table: Mapped[str] = mapped_column(String(255))
    target_table: Mapped[str] = mapped_column(String(255))
    relationship: Mapped[str] = mapped_column(String(120))
    evidence: Mapped[str] = mapped_column(Text)


class ColumnLineageEdge(IdMixin, TimestampMixin, Base):
    __tablename__ = "column_lineage_edges"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "source_connector_slug",
            "source_table",
            "source_column",
            "target_connector_slug",
            "target_table",
            "target_column",
            "relationship",
            name="uq_column_lineage_edges_identity",
        ),
    )

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    source_connector_slug: Mapped[str] = mapped_column(String(80), index=True)
    source_table: Mapped[str] = mapped_column(String(255), index=True)
    source_column: Mapped[str] = mapped_column(String(255), index=True)
    target_connector_slug: Mapped[str] = mapped_column(String(80), index=True)
    target_table: Mapped[str] = mapped_column(String(255), index=True)
    target_column: Mapped[str] = mapped_column(String(255), index=True)
    relationship: Mapped[str] = mapped_column(String(120), default="derives_from", index=True)
    evidence: Mapped[str] = mapped_column(Text, default="")
    source_page_id: Mapped[str | None] = mapped_column(ForeignKey("wiki_pages.id", ondelete="SET NULL"), nullable=True, index=True)
    compile_run_id: Mapped[str | None] = mapped_column(String(80), index=True, nullable=True)


class AgentRun(IdMixin, TimestampMixin, Base):
    __tablename__ = "agent_runs"

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"))
    agent_name: Mapped[str] = mapped_column(String(120))
    status: Mapped[str] = mapped_column(String(40))
    summary: Mapped[str] = mapped_column(Text)
    timeline: Mapped[list[dict]] = mapped_column(JSON, default=list)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str] = mapped_column(String(40), default="completed", index=True)
    lease_token: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True, unique=True)
    budget_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    budget_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)


class Agent(IdMixin, TimestampMixin, Base):
    __tablename__ = "agents"
    __table_args__ = (UniqueConstraint("workspace_id", "name", name="uq_agents_workspace_name"),)

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    name: Mapped[str] = mapped_column(String(80), index=True)
    display_name: Mapped[str] = mapped_column(String(120))
    system_prompt: Mapped[str] = mapped_column(Text, default="")
    sql_query: Mapped[str | None] = mapped_column(Text, nullable=True)
    kind: Mapped[str] = mapped_column(String(40), default="on_demand", index=True)
    is_system: Mapped[bool] = mapped_column(Boolean, default=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    icon_key: Mapped[str] = mapped_column(String(80), default="bot")
    cadence_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    force_run_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    thresholds: Mapped[dict] = mapped_column(JSON, default=dict)
    uses_llm_filter: Mapped[bool] = mapped_column(Boolean, default=False)
    target_connector_id: Mapped[str | None] = mapped_column(ForeignKey("connectors.id", ondelete="SET NULL"), nullable=True)
    created_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    grants: Mapped[list["AgentMcpGrant"]] = relationship(
        back_populates="agent",
        cascade="all, delete-orphan",
    )


class AgentMcpGrant(IdMixin, TimestampMixin, Base):
    __tablename__ = "agent_mcp_grants"
    __table_args__ = (UniqueConstraint("agent_id", "connector_slug", name="uq_agent_grant_slug"),)

    agent_id: Mapped[str] = mapped_column(ForeignKey("agents.id", ondelete="CASCADE"), index=True)
    connector_slug: Mapped[str] = mapped_column(String(80), index=True)
    read_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    write_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    agent: Mapped[Agent] = relationship(back_populates="grants")


class Alert(IdMixin, TimestampMixin, Base):
    __tablename__ = "alerts"

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"))
    fingerprint: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    severity: Mapped[str] = mapped_column(String(40))
    title: Mapped[str] = mapped_column(String(255))
    detail: Mapped[str] = mapped_column(Text)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    requires_approval: Mapped[bool] = mapped_column(Boolean, default=False)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    acknowledged_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[str | None] = mapped_column(String(255), nullable=True)


class MonitoringConfig(IdMixin, TimestampMixin, Base):
    __tablename__ = "monitoring_configs"
    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "agent_name",
            "connector_id",
            name="uq_monitoring_configs_scope",
        ),
    )

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    agent_name: Mapped[str] = mapped_column(String(120), index=True)
    connector_id: Mapped[str] = mapped_column(ForeignKey("connectors.id", ondelete="CASCADE"), index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    thresholds: Mapped[dict] = mapped_column(JSON, default=dict)
    notification_channels: Mapped[dict] = mapped_column(JSON, default=dict)


class QueryAudit(IdMixin, Base):
    __tablename__ = "query_audit"

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    connector_slug: Mapped[str] = mapped_column(String(80), index=True, default="demo")
    sql: Mapped[str] = mapped_column(Text)
    duration_ms: Mapped[int] = mapped_column(Integer, default=0)
    rows_returned: Mapped[int] = mapped_column(Integer, default=0)
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    executed_by: Mapped[str | None] = mapped_column(String(255), nullable=True)


class WorkerHeartbeat(IdMixin, Base):
    __tablename__ = "worker_heartbeat"
    __table_args__ = (UniqueConstraint("worker_name", name="uq_worker_heartbeat_name"),)

    worker_name: Mapped[str] = mapped_column(String(120), index=True)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str] = mapped_column(String(40), default="ok")
    detail: Mapped[str] = mapped_column(Text, default="")


class ChatThread(IdMixin, TimestampMixin, Base):
    __tablename__ = "chat_threads"

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"))
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    title: Mapped[str] = mapped_column(String(255), default="New conversation")
    # kind segregates user-driven threads from synthetic eval/scheduled threads
    # so background eval runs don't pollute the Sessions sidebar.
    kind: Mapped[str] = mapped_column(String(20), default="user", index=True)
    archived: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    messages: Mapped[list["ChatMessage"]] = relationship(
        back_populates="thread",
        cascade="all, delete-orphan",
        order_by="ChatMessage.created_at",
    )


class ChatMessage(IdMixin, TimestampMixin, Base):
    __tablename__ = "chat_messages"

    thread_id: Mapped[str] = mapped_column(ForeignKey("chat_threads.id", ondelete="CASCADE"))
    role: Mapped[str] = mapped_column(String(16))
    content: Mapped[str] = mapped_column(Text)
    sql: Mapped[str | None] = mapped_column(Text, nullable=True)
    provider: Mapped[str | None] = mapped_column(String(40), nullable=True)
    llm_status: Mapped[str | None] = mapped_column(String(40), nullable=True)
    citations: Mapped[list[dict]] = mapped_column(JSON, default=list)
    rows: Mapped[list[dict]] = mapped_column(JSON, default=list)
    chart_spec: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    action: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    retrieval_trace: Mapped[dict] = mapped_column(JSON, default=dict)
    # trace_id is the deterministic Langfuse trace id derived from this row's
    # uuid; populated for assistant messages produced through the traced chat
    # path. Also used as the join key for chat_spans rows.
    trace_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    thread: Mapped[ChatThread] = relationship(back_populates="messages")


class ChatSpan(IdMixin, Base):
    """Hierarchical span for a single chat turn. Mirrors Langfuse's shape so
    rows can be replayed into Langfuse later if a workspace enables it after
    the fact. Always-on local sink; Langfuse is the optional secondary sink.
    """

    __tablename__ = "chat_spans"

    chat_message_id: Mapped[str] = mapped_column(
        ForeignKey("chat_messages.id", ondelete="CASCADE"), index=True
    )
    parent_span_id: Mapped[str | None] = mapped_column(
        ForeignKey("chat_spans.id", ondelete="CASCADE"), nullable=True, index=True
    )
    # kind: retrieval | llm | tool | sql | root
    kind: Mapped[str] = mapped_column(String(20), index=True)
    name: Mapped[str] = mapped_column(String(160))
    status: Mapped[str] = mapped_column(String(20), default="ok")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    input: Mapped[dict] = mapped_column(JSON, default=dict)
    output: Mapped[dict] = mapped_column(JSON, default=dict)
    span_metadata: Mapped[dict] = mapped_column("metadata", JSON, default=dict)
    usage: Mapped[dict] = mapped_column(JSON, default=dict)
    model: Mapped[str | None] = mapped_column(String(120), nullable=True)
    latency_ms: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    ended_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class EvalCase(IdMixin, TimestampMixin, Base):
    """Canonical eval case. Same row backs both golden queries (Theme 2) and
    eval datasets (Theme 3) — status separates the two:
      candidate -> auto- or feedback-generated; pending user review
      approved  -> reviewed and added to the eval suite
      golden    -> approved + designated as the canonical answer for its
                   question (chat will prefer this SQL over re-generating)
      archived  -> retired; kept for history, excluded from runs and lookups
    """

    __tablename__ = "eval_cases"

    workspace_id: Mapped[str] = mapped_column(
        ForeignKey("workspaces.id", ondelete="CASCADE"), index=True
    )
    question: Mapped[str] = mapped_column(Text)
    expected_answer: Mapped[str | None] = mapped_column(Text, nullable=True)
    expected_sql: Mapped[str | None] = mapped_column(Text, nullable=True)
    expected_connector_slug: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    expected_tool: Mapped[str | None] = mapped_column(String(160), nullable=True)
    # [{"source": "...", "table": "...", "columns": ["..."]}]
    expected_citations: Mapped[list[dict]] = mapped_column(JSON, default=list)
    # Hash + small sample of the result-set captured at promote-golden time so
    # eval runs can compare without re-executing the warehouse query.
    expected_result_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    expected_result_preview: Mapped[list[dict]] = mapped_column(JSON, default=list)
    tags: Mapped[list[str]] = mapped_column(JSON, default=list)
    # status enum: candidate | approved | golden | archived
    status: Mapped[str] = mapped_column(String(20), default="candidate", index=True)
    # origin enum: manual | feedback | auto:schema | auto:kg | auto:lineage |
    #              auto:dbt | auto:airflow | auto:dagster | auto:fixture
    origin: Mapped[str] = mapped_column(String(40), default="manual", index=True)
    source_chat_message_id: Mapped[str | None] = mapped_column(
        ForeignKey("chat_messages.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_by: Mapped[str | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )


class EvalRun(IdMixin, Base):
    """One execution of an eval case. A batch is N runs sharing batch_id.

    The synthetic chat_message_id points at the assistant message we created
    in a kind='eval' ChatThread; this means the existing tracing surface
    (chat_spans, Langfuse) covers eval runs for free — no parallel trace
    schema.
    """

    __tablename__ = "eval_runs"

    workspace_id: Mapped[str] = mapped_column(
        ForeignKey("workspaces.id", ondelete="CASCADE"), index=True
    )
    batch_id: Mapped[str] = mapped_column(String(64), index=True)
    eval_case_id: Mapped[str] = mapped_column(
        ForeignKey("eval_cases.id", ondelete="CASCADE"), index=True
    )
    chat_message_id: Mapped[str | None] = mapped_column(
        ForeignKey("chat_messages.id", ondelete="SET NULL"), nullable=True, index=True
    )
    passed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    # enum: wrong_connector | wrong_tool | bad_retrieval | sql_error |
    #       safety_block | hallucination | missing_citation | formatting |
    #       runner_error | unknown
    failure_category: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    actual_answer: Mapped[str | None] = mapped_column(Text, nullable=True)
    actual_sql: Mapped[str | None] = mapped_column(Text, nullable=True)
    actual_citations: Mapped[list[dict]] = mapped_column(JSON, default=list)
    actual_result_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    actual_result_preview: Mapped[list[dict]] = mapped_column(JSON, default=list)
    langfuse_trace_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    duration_ms: Mapped[int] = mapped_column(Integer, default=0)
    prompt_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    completion_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    model: Mapped[str | None] = mapped_column(String(120), nullable=True)
    prompt_version: Mapped[str | None] = mapped_column(String(80), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        index=True,
    )


class EvalResult(IdMixin, Base):
    """One metric score against one EvalRun. Metric plug-ins write these."""

    __tablename__ = "eval_results"

    eval_run_id: Mapped[str] = mapped_column(
        ForeignKey("eval_runs.id", ondelete="CASCADE"), index=True
    )
    metric: Mapped[str] = mapped_column(String(60), index=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    passed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    # status: ok | skipped | error
    status: Mapped[str] = mapped_column(String(20), default="ok", index=True)
    detail: Mapped[dict] = mapped_column(JSON, default=dict)


class EvalSuggestion(IdMixin, TimestampMixin, Base):
    """A diagnose-time suggestion for improving an eval case's outcome.

    Each row points at the EvalRun that triggered it and carries a concrete
    diff the user can Apply (or Dismiss). The kind enum determines which
    apply-path the suggestion goes through (golden query, prompt override,
    workspace rules, etc.).
    """

    __tablename__ = "eval_suggestions"

    eval_run_id: Mapped[str] = mapped_column(
        ForeignKey("eval_runs.id", ondelete="CASCADE"), index=True
    )
    workspace_id: Mapped[str] = mapped_column(
        ForeignKey("workspaces.id", ondelete="CASCADE"), index=True
    )
    # kind enum (see services/evals/diagnose.py:SUGGESTION_KINDS):
    #   golden_query | prompt_diff | rules_md | tool_description_diff |
    #   retrieval_context | connector_routing
    kind: Mapped[str] = mapped_column(String(40), index=True)
    title: Mapped[str] = mapped_column(String(200))
    rationale: Mapped[str] = mapped_column(Text, default="")
    current_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    proposed_value: Mapped[str] = mapped_column(Text)
    target: Mapped[str | None] = mapped_column(String(200), nullable=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.7)
    source: Mapped[str] = mapped_column(String(20), default="rules")  # rules | llm
    # status enum: pending | applied | dismissed
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    apply_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    applied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    applied_by: Mapped[str | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    apply_result: Mapped[dict] = mapped_column(JSON, default=dict)


class EvalMetricThreshold(IdMixin, TimestampMixin, Base):
    """User-configurable per-metric pass bar for a workspace.

    Absent rows fall back to per-metric module defaults (see
    services/evals/metrics/__init__.py:DEFAULT_THRESHOLDS).
    """

    __tablename__ = "eval_metric_thresholds"
    __table_args__ = (
        UniqueConstraint("workspace_id", "metric", name="uq_eval_thresholds_identity"),
    )

    workspace_id: Mapped[str] = mapped_column(
        ForeignKey("workspaces.id", ondelete="CASCADE"), index=True
    )
    metric: Mapped[str] = mapped_column(String(60))
    pass_threshold: Mapped[float] = mapped_column(Float, default=0.5)


class Feedback(IdMixin, TimestampMixin, Base):
    """User reaction to an assistant message. The primary feedback signal we
    later turn into evaluation cases (👎 → corrected expected; 👍 → regression
    eval candidate). Forwarded to Langfuse as a trace score if enabled.
    """

    __tablename__ = "feedback"

    chat_message_id: Mapped[str] = mapped_column(
        ForeignKey("chat_messages.id", ondelete="CASCADE"), index=True
    )
    user_id: Mapped[str | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True
    )
    # sentiment: positive | negative
    sentiment: Mapped[str] = mapped_column(String(20), index=True)
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    langfuse_score_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    # set once the feedback has been promoted to an eval case (Phase 2)
    eval_case_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)


class AgentWriteAudit(IdMixin, Base):
    __tablename__ = "agent_write_audit"

    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    agent_id: Mapped[str | None] = mapped_column(ForeignKey("agents.id", ondelete="SET NULL"), nullable=True, index=True)
    connector_slug: Mapped[str] = mapped_column(String(80), index=True)
    statement_type: Mapped[str] = mapped_column(String(80))
    statement: Mapped[str] = mapped_column(Text)
    target: Mapped[str | None] = mapped_column(String(255), nullable=True)
    affected_rows: Mapped[int | None] = mapped_column(Integer, nullable=True)
    required_approval: Mapped[bool] = mapped_column(Boolean, default=False)
    alert_id: Mapped[str | None] = mapped_column(ForeignKey("alerts.id", ondelete="SET NULL"), nullable=True)
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    executed_by: Mapped[str | None] = mapped_column(String(255), nullable=True)


class AgentToolCall(IdMixin, Base):
    __tablename__ = "agent_tool_call"

    run_id: Mapped[str | None] = mapped_column(ForeignKey("agent_runs.id", ondelete="CASCADE"), nullable=True, index=True)
    agent_name: Mapped[str] = mapped_column(String(120), index=True)
    tool_name: Mapped[str] = mapped_column(String(160), index=True)
    connector_slug: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    args_json: Mapped[dict] = mapped_column(JSON, default=dict)
    result_summary: Mapped[str] = mapped_column(Text, default="")
    result_size_bytes: Mapped[int] = mapped_column(Integer, default=0)
    latency_ms: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(40), default="ok", index=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    called_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class AppSetting(TimestampMixin, Base):
    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    encrypted_value: Mapped[str] = mapped_column(Text)


class LogEntry(IdMixin, Base):
    __tablename__ = "log_entries"

    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    level: Mapped[str] = mapped_column(String(16), index=True)
    logger_name: Mapped[str] = mapped_column(String(120), index=True)
    message: Mapped[str] = mapped_column(Text)
    context: Mapped[dict] = mapped_column(JSON, default=dict)
    exception: Mapped[str | None] = mapped_column(Text, nullable=True)
