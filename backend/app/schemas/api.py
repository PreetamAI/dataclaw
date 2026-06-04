from typing import Any

from pydantic import BaseModel, Field, field_validator


class LoginRequest(BaseModel):
    email: str
    password: str


class ConnectorTestRequest(BaseModel):
    credentials: dict[str, Any] = Field(default_factory=dict)
    persist_on_success: bool = True


class ChatRequest(BaseModel):
    question: str
    thread_id: str | None = None
    table_id: str | None = None
    model: str | None = None
    connector_slug: str | None = None


class ChatAction(BaseModel):
    label: str
    tab: str
    connector_slug: str | None = None
    agent_name: str | None = None


class ChatResponse(BaseModel):
    answer: str
    sql: str | None = None
    table: str | None = None
    rows: list[dict[str, Any]] = Field(default_factory=list)
    citations: list[dict[str, Any]] = Field(default_factory=list)
    chart_spec: dict[str, Any] | None = None
    provider: str | None = None
    llm_status: str | None = None
    detail: str | None = None
    status: str | None = None
    alert_id: str | None = None
    action: ChatAction | None = None
    tool_call: dict[str, Any] | None = None
    tool_calls: list[dict[str, Any]] = Field(default_factory=list)
    tool_result: dict[str, Any] | None = None
    tool_results: list[dict[str, Any]] = Field(default_factory=list)
    retrieval_trace: dict[str, Any] = Field(default_factory=dict)
    # Cross-link handles: trace_id is the deterministic id used by the local
    # span store AND (when configured) Langfuse. langfuse_url is populated
    # only when a Langfuse integration is enabled for the workspace.
    trace_id: str | None = None
    langfuse_url: str | None = None
    message_id: str | None = None
    thread_id: str
    thread_title: str


class ChatThreadCreateRequest(BaseModel):
    title: str | None = None


class ChatThreadRenameRequest(BaseModel):
    title: str


class QueryRequest(BaseModel):
    sql: str
    limit: int = 100
    connector_slug: str | None = None


class LlmProviderUpdate(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)


class AgentGrantUpdate(BaseModel):
    connector_slug: str
    read_enabled: bool = False
    write_enabled: bool = False


class AgentCreate(BaseModel):
    name: str
    display_name: str | None = None
    system_prompt: str = ""
    sql_query: str | None = None
    kind: str = "on_demand"
    enabled: bool = True
    icon_key: str = "bot"
    cadence_minutes: int | None = None
    thresholds: dict[str, Any] = Field(default_factory=dict)
    uses_llm_filter: bool = False
    target_connector_id: str | None = None
    target_connector_slug: str | None = None
    grants: list[AgentGrantUpdate] = Field(default_factory=list)

    @field_validator("cadence_minutes")
    @classmethod
    def validate_min_cadence(cls, value: int | None) -> int | None:
        if value is not None and value < 5:
            raise ValueError("cadence_minutes must be at least 5")
        return value


class AgentUpdate(BaseModel):
    display_name: str | None = None
    system_prompt: str | None = None
    sql_query: str | None = None
    kind: str | None = None
    enabled: bool | None = None
    icon_key: str | None = None
    cadence_minutes: int | None = None
    thresholds: dict[str, Any] | None = None
    uses_llm_filter: bool | None = None
    target_connector_id: str | None = None
    target_connector_slug: str | None = None

    @field_validator("cadence_minutes")
    @classmethod
    def validate_min_cadence(cls, value: int | None) -> int | None:
        if value is not None and value < 5:
            raise ValueError("cadence_minutes must be at least 5")
        return value


class AgentGrantMatrixUpdate(BaseModel):
    grants: list[AgentGrantUpdate] = Field(default_factory=list)


class McpToolCallRequest(BaseModel):
    arguments: dict[str, Any] = Field(default_factory=dict)


class WorkspaceUpdate(BaseModel):
    onboarding_complete: bool | None = None
