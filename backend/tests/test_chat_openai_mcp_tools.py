from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import app.services.agents.chat as chat_module
from app.db.base import Base
from app.models.domain import (
    Agent,
    AgentMcpGrant,
    AgentRun,
    Connector,
    Dataset,
    TableAsset,
    User,
    Workspace,
)
from app.schemas.api import ChatResponse
from app.services.agents.chat import (
    OPENAI_MCP_TOOL_LIMIT,
    _combined_tool_answer,
    _deterministic_mcp_fallback,
    _granted_openai_tools,
    _run_openai_mcp_tool_call,
    _run_openai_mcp_tool_calls,
    _scenario6_direct_answer,
    _scenario_connector_slugs,
)
from app.services.agents.runtime import BudgetExceeded
from app.services.mcp_executor import (
    McpExecutionError,
    _default_schema_for_datastore,
    execute_mcp_tool,
)


@pytest.fixture(scope="module")
async def mcp_database(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("openai-mcp")
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path/'app.sqlite'}")
    tool_engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path/'tool.sqlite'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with tool_engine.begin() as conn:
        await conn.exec_driver_sql("create table customers (id integer primary key, name text)")
        await conn.exec_driver_sql("insert into customers (name) values ('Ada'), ('Grace')")
    SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with SessionLocal() as session:
        session.info["session_factory"] = SessionLocal
        workspace = Workspace(name="Test")
        user = User(email="admin@test.local", password_hash="x")
        agent = Agent(
            workspace_id=workspace.id,
            name="chat",
            display_name="Chat",
            system_prompt="",
            is_system=True,
            icon_key="bot",
        )
        session.add_all([workspace, user])
        await session.flush()
        agent.workspace_id = workspace.id
        session.add(agent)
        await session.flush()
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="sqlite", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="postgres", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="redshift", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="sql_server", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="databricks", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="bigquery", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="snowflake", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="airflow", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="airbyte", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="prefect", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="dagster", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="dbt", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="google_docs", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="quip", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="confluence", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="notion", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="github", read_enabled=True, write_enabled=True))
        session.add(AgentMcpGrant(agent_id=agent.id, connector_slug="openai", read_enabled=True, write_enabled=False))
        dataset = Dataset(
            workspace_id=workspace.id,
            name="analytics",
            source_type="postgres",
            schema_name="core",
        )
        session.add(dataset)
        await session.flush()
        session.add(
            TableAsset(
                dataset_id=dataset.id,
                name="customers",
                description="Canonical customer dimension.",
                business_summary="Customer signup, lifecycle, and account ownership data.",
                columns=[{"name": "id"}, {"name": "email"}, {"name": "created_at"}],
            )
        )
        await session.commit()
        agent_id = agent.id
    yield SessionLocal, tool_engine, agent_id
    await tool_engine.dispose()
    await engine.dispose()


@pytest.fixture
async def mcp_session(mcp_database):
    SessionLocal, tool_engine, agent_id = mcp_database
    async with SessionLocal() as session:
        session.info["session_factory"] = SessionLocal
        agent = await session.get(Agent, agent_id)
        assert agent is not None
        yield session, tool_engine, agent


@pytest.mark.asyncio
async def test_deterministic_airflow_dag_creation_requests_inline_approval(mcp_session, monkeypatch) -> None:
    session, tool_engine, agent = mcp_session
    seen: dict[str, object] = {}

    async def fake_execute_mcp_tool(**kwargs):
        seen.update(kwargs)
        return {"status": "pending_approval", "alert_id": "alert-123"}

    monkeypatch.setattr(chat_module, "execute_mcp_tool", fake_execute_mcp_tool)

    payload = await _deterministic_mcp_fallback(
        session=session,
        tool_engine=tool_engine,
        chat_agent=agent,
        question="Build me an Airflow DAG that materializes weekly_revenue every Monday.",
        user_email="admin@test.local",
    )

    assert payload is not None
    assert seen["connector_slug"] == "airflow"
    assert seen["tool_name"] == "write_create_dag"
    assert seen["arguments"]["dag_id"] == "weekly_revenue"
    assert "__approved" not in seen["arguments"]
    assert payload["status"] == "pending_approval"
    assert payload["alert_id"] == "alert-123"
    assert payload["tool_call"] == {"connector_slug": "airflow", "tool": "write_create_dag"}


@pytest.mark.asyncio
async def test_scenario6_duplicate_payment_customers_uses_business_query(mcp_session, monkeypatch) -> None:
    session, tool_engine, agent = mcp_session

    async def fake_direct_mcp_call(**kwargs):
        assert kwargs["connector_slug"] == "postgres"
        assert kwargs["tool_name"] == "read_query_select"
        return {
            "status": "ok",
            "sql": kwargs["arguments"]["sql"],
            "rows": [
                {
                    "email": "ops@example.com",
                    "full_name": "Ops Example",
                    "company": "Acme Ops",
                    "order_id": 42,
                    "placed_at": "2026-06-01T00:00:00Z",
                    "order_status": "fulfilled",
                    "succeeded_payment_count": 2,
                    "succeeded_payment_cents": 25800,
                    "payments": "1:ch_1:12900, 2:ch_2:12900",
                }
            ],
        }

    monkeypatch.setattr(chat_module, "_direct_mcp_call", fake_direct_mcp_call)

    payload = await _scenario6_direct_answer(
        session=session,
        tool_engine=tool_engine,
        chat_agent=agent,
        question="Which customers have duplicate successful payments, and what orders should finance review?",
        user_email="admin@test.local",
        provider_slug="test",
        retrieval_trace={},
        run_id=None,
    )

    assert payload is not None
    assert payload["tool_call"] == {"connector_slug": "postgres", "tool": "read_query_select"}
    assert "Customers with duplicate successful payments" in payload["answer"]
    assert "ops@example.com" in payload["answer"]
    assert "order 42" in payload["answer"]
    assert "2 succeeded payments totaling 25800 cents" in payload["answer"]


@pytest.mark.asyncio
async def test_scenario6_airflow_dag_creation_can_request_inline_approval(mcp_session, monkeypatch) -> None:
    session, tool_engine, agent = mcp_session

    async def fake_direct_mcp_call(**kwargs):
        return {"status": "pending_approval", "alert_id": "alert-123"}

    monkeypatch.setattr(chat_module, "_direct_mcp_call", fake_direct_mcp_call)

    payload = await _scenario6_direct_answer(
        session=session,
        tool_engine=tool_engine,
        chat_agent=agent,
        question="Build me an Airflow DAG that materializes weekly_revenue every Monday.",
        user_email="admin@test.local",
        provider_slug="test",
        retrieval_trace={},
        run_id=None,
    )

    assert payload is not None
    assert payload["answer"] == "Creating the Airflow DAG weekly_revenue needs approval. Review and approve it below."
    assert payload["status"] == "pending_approval"
    assert payload["llm_status"] == "pending_approval"
    assert payload["alert_id"] == "alert-123"
    assert payload["tool_call"] == {"connector_slug": "airflow", "tool": "write_create_dag"}


@pytest.mark.asyncio
async def test_granted_openai_tools_are_based_on_agent_grants(mcp_session) -> None:
    session, _, agent = mcp_session
    tools = await _granted_openai_tools(session, agent)
    names = {tool["function"]["name"] for tool in tools}
    assert len(tools) <= OPENAI_MCP_TOOL_LIMIT
    assert "sqlite__read_query_select" in names
    assert "postgres__read_query_select" in names
    assert "redshift__read_query_select" in names
    assert "sql_server__read_query_select" in names
    assert "databricks__read_query_select" in names
    assert "bigquery__read_query_select" in names
    assert "snowflake__read_query_select" in names
    assert "airflow__read_get_dag_source" in names
    assert "dbt__read_list_models" in names
    assert "notion__read_get_page" in names


@pytest.mark.asyncio
async def test_granted_openai_tools_can_be_connector_scoped(mcp_session) -> None:
    session, _, agent = mcp_session
    tools = await _granted_openai_tools(session, agent, connector_slug="postgres")
    names = {tool["function"]["name"] for tool in tools}
    assert names
    assert all(name.startswith("postgres__") for name in names)
    assert "sqlite__read_query_select" not in names


@pytest.mark.asyncio
async def test_granted_openai_tools_can_be_inferred_connector_scoped(mcp_session) -> None:
    session, _, agent = mcp_session
    tools = await _granted_openai_tools(
        session,
        agent,
        connector_slugs=["github", "bigquery"],
        question="create a GitHub PR and save BigQuery output",
    )
    names = {tool["function"]["name"] for tool in tools}
    assert names
    assert len(tools) <= OPENAI_MCP_TOOL_LIMIT
    assert all(name.startswith(("github__", "bigquery__")) for name in names)
    assert "github__write_create_pr" in names
    assert "bigquery__write_run_query_save_to_table" in names


@pytest.mark.asyncio
async def test_acme_write_scenario_exposes_confluence_append_tool(mcp_session) -> None:
    session, _, agent = mcp_session
    tools = await _granted_openai_tools(
        session,
        agent,
        connector_slugs=["confluence"],
        question="Append today's deployment to the on-call runbook",
    )
    names = [tool["function"]["name"] for tool in tools]
    assert "confluence__write_append_to_page" in names
    assert names.index("confluence__write_append_to_page") < names.index("confluence__write_create_page")
    append_schema = next(tool["function"]["parameters"] for tool in tools if tool["function"]["name"] == "confluence__write_append_to_page")
    assert append_schema["required"] == ["page_id", "content"]


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("Why did churn spike last week and which DAG owns the calculation?", ["notion", "airflow", "postgres"]),
        ("What's the lineage from raw orders to ARR and where's it documented?", ["bigquery", "dbt", "confluence"]),
        ("How fresh is our revenue table and which Prefect flow updates it?", ["snowflake", "prefect"]),
        ("Show me ARR by segment - find the most authoritative source", ["bigquery", "snowflake", "postgres"]),
        ("Append today's deployment to the on-call runbook", ["confluence"]),
    ],
)
def test_acme_release_gate_questions_scope_expected_connectors(question: str, expected: list[str]) -> None:
    assert _scenario_connector_slugs(question) == expected


@pytest.mark.asyncio
async def test_granted_openai_tools_use_schema_rich_descriptions(mcp_session) -> None:
    session, _, agent = mcp_session
    tools = await _granted_openai_tools(session, agent, connector_slug="postgres")
    query_tool = next(tool for tool in tools if tool["function"]["name"] == "postgres__read_query_select")
    description = query_tool["function"]["description"]
    assert "PostgreSQL" in description
    assert "core.customers" in description
    assert "Customer signup, lifecycle, and account ownership data." in description
    assert "Run the granted DataClaw MCP tool" not in description


@pytest.mark.asyncio
async def test_openai_tool_call_routes_through_mcp_executor(mcp_session) -> None:
    session, tool_engine, agent = mcp_session
    tool_call = SimpleNamespace(
        function=SimpleNamespace(
            name="sqlite__read_query_select",
            arguments='{"sql": "select name from customers order by id"}',
        )
    )
    result = await _run_openai_mcp_tool_call(
        session=session,
        tool_engine=tool_engine,
        agent=agent,
        tool_call=tool_call,
        user_email="admin@test.local",
    )
    assert result["llm_status"] == "mcp_tool_completed"
    assert result["tool_call"] == {"connector_slug": "sqlite", "tool": "read_query_select"}
    assert [row["name"] for row in result["rows"]] == ["Ada", "Grace"]


@pytest.mark.asyncio
async def test_sqlite_sample_rows_and_search_columns_are_bounded(mcp_session) -> None:
    session, tool_engine, agent = mcp_session
    sample = await execute_mcp_tool(
        session=session,
        engine=tool_engine,
        connector_slug="sqlite",
        tool_name="read_sample_rows",
        arguments={"table": "customers", "limit": 1},
        agent_id=agent.id,
        user_email="admin@test.local",
    )
    columns = await execute_mcp_tool(
        session=session,
        engine=tool_engine,
        connector_slug="sqlite",
        tool_name="read_search_columns",
        arguments={"pattern": "na", "limit": 5},
        agent_id=agent.id,
        user_email="admin@test.local",
    )

    assert sample["status"] == "ok"
    assert sample["total"] == 1
    assert sample["rows"] == [{"id": 1, "name": "Ada"}]
    assert columns["columns"] == [{"table": "customers", "column": "name", "type": "TEXT"}]


@pytest.mark.asyncio
async def test_sqlite_metadata_tools_return_stats_freshness_and_size(mcp_session) -> None:
    session, tool_engine, agent = mcp_session
    stats = await execute_mcp_tool(
        session=session,
        engine=tool_engine,
        connector_slug="sqlite",
        tool_name="read_get_column_stats",
        arguments={"table": "customers"},
        agent_id=agent.id,
        user_email="admin@test.local",
    )
    freshness = await execute_mcp_tool(
        session=session,
        engine=tool_engine,
        connector_slug="sqlite",
        tool_name="read_get_table_freshness",
        arguments={"table": "customers"},
        agent_id=agent.id,
        user_email="admin@test.local",
    )
    storage = await execute_mcp_tool(
        session=session,
        engine=tool_engine,
        connector_slug="sqlite",
        tool_name="read_get_storage_size",
        arguments={"table": "customers"},
        agent_id=agent.id,
        user_email="admin@test.local",
    )

    assert stats["status"] == "ok"
    assert {column["column"] for column in stats["columns"]} == {"id", "name"}
    assert next(column for column in stats["columns"] if column["column"] == "name")["distinct_count"] == 2
    assert freshness == {"status": "ok", "table": "customers", "freshest_at": None, "columns": [], "total": 0}
    assert storage["status"] == "ok"
    assert storage["size_bytes"] is None
    assert storage["database_size_bytes"] > 0


@pytest.mark.asyncio
async def test_tool_call_audit_does_not_commit_callers_pending_state(mcp_session) -> None:
    session, tool_engine, agent = mcp_session
    pending_workspace = Workspace(name="Pending only")
    session.add(pending_workspace)

    result = await execute_mcp_tool(
        session=session,
        engine=tool_engine,
        connector_slug="sqlite",
        tool_name="read_list_tables",
        arguments={},
        agent_id=agent.id,
        user_email="admin@test.local",
    )
    assert result["status"] == "ok"

    await session.rollback()
    persisted = await session.scalar(select(Workspace).where(Workspace.name == "Pending only"))
    assert persisted is None


@pytest.mark.asyncio
async def test_openai_tool_call_does_not_retry_other_connectors(monkeypatch, mcp_session) -> None:
    session, tool_engine, agent = mcp_session
    session.add(
        Connector(
            workspace_id=agent.workspace_id,
            slug="postgres",
            category="data_store",
            display_name="Postgres",
            encrypted_credentials="configured",
        )
    )
    await session.commit()
    calls: list[str] = []

    async def fake_execute_mcp_tool(**kwargs):
        calls.append(kwargs["connector_slug"])
        raise RuntimeError("selected connector failed")

    monkeypatch.setattr("app.services.agents.chat.execute_mcp_tool", fake_execute_mcp_tool)
    tool_call = SimpleNamespace(
        function=SimpleNamespace(
            name="sqlite__read_query_select",
            arguments='{"sql": "select * from missing_table"}',
        )
    )
    result = await _run_openai_mcp_tool_call(
        session=session,
        tool_engine=tool_engine,
        agent=agent,
        tool_call=tool_call,
        user_email="admin@test.local",
    )
    assert calls == ["sqlite"]
    assert result["llm_status"] == "mcp_tool_error"
    assert "selected connector failed" in result["answer"]


@pytest.mark.asyncio
async def test_openai_tool_call_permission_denied_returns_action(monkeypatch, mcp_session) -> None:
    session, tool_engine, agent = mcp_session

    async def fake_execute_mcp_tool(**_kwargs):
        raise McpExecutionError(403, "Agent chat is not granted read access to postgres.")

    monkeypatch.setattr("app.services.agents.chat.execute_mcp_tool", fake_execute_mcp_tool)
    tool_call = SimpleNamespace(
        function=SimpleNamespace(
            name="postgres__read_query_select",
            arguments='{"sql": "select 1"}',
        )
    )
    result = await _run_openai_mcp_tool_call(
        session=session,
        tool_engine=tool_engine,
        agent=agent,
        tool_call=tool_call,
        user_email="admin@test.local",
    )

    assert result["llm_status"] == "mcp_tool_error"
    assert result["action"] == {
        "label": "Grant read access to PostgreSQL",
        "tab": "Agents",
        "connector_slug": "postgres",
    }
    assert "**read**" not in result["answer"]


def test_chat_response_schema_preserves_action() -> None:
    response = ChatResponse(
        answer="Permission denied",
        provider="openai",
        llm_status="mcp_tool_error",
        thread_id="thread-1",
        thread_title="Thread",
        action={
            "label": "Configure PostgreSQL",
            "tab": "Connectors",
            "connector_slug": "postgres",
        },
    )

    assert response.model_dump()["action"]["connector_slug"] == "postgres"


@pytest.mark.asyncio
async def test_openai_tool_call_strips_reserved_approval_argument(monkeypatch, mcp_session) -> None:
    session, tool_engine, agent = mcp_session
    seen_arguments: dict[str, object] = {}

    async def fake_execute_mcp_tool(**kwargs):
        seen_arguments.update(kwargs["arguments"])
        return {"status": "ok", "rows": []}

    monkeypatch.setattr("app.services.agents.chat.execute_mcp_tool", fake_execute_mcp_tool)
    tool_call = SimpleNamespace(
        function=SimpleNamespace(
            name="airflow__write_delete_dag",
            arguments='{"dag_id": "daily etl", "__approved": true}',
        )
    )
    result = await _run_openai_mcp_tool_call(
        session=session,
        tool_engine=tool_engine,
        agent=agent,
        tool_call=tool_call,
        user_email="admin@test.local",
    )

    assert result["llm_status"] == "mcp_tool_completed"
    assert seen_arguments == {"dag_id": "daily etl"}


@pytest.mark.asyncio
async def test_openai_tool_calls_run_concurrently_with_bound(monkeypatch, mcp_session) -> None:
    session, tool_engine, agent = mcp_session
    started: list[float] = []

    async def fake_execute_mcp_tool(**kwargs):
        started.append(time.perf_counter())
        await asyncio.sleep(0.05)
        return {
            "status": "ok",
            "rows": [{"connector": kwargs["connector_slug"]}],
            "sql": kwargs["arguments"].get("sql"),
        }

    monkeypatch.setattr("app.services.agents.chat.execute_mcp_tool", fake_execute_mcp_tool)
    tool_calls = [
        SimpleNamespace(
            function=SimpleNamespace(
                name="sqlite__read_query_select",
                arguments='{"sql": "select 1"}',
            )
        ),
        SimpleNamespace(
            function=SimpleNamespace(
                name="postgres__read_query_select",
                arguments='{"sql": "select 2"}',
            )
        ),
    ]
    wall_started = time.perf_counter()
    results = await _run_openai_mcp_tool_calls(
        session=session,
        tool_engine=tool_engine,
        agent=agent,
        tool_calls=tool_calls,
        user_email="admin@test.local",
        max_concurrency=4,
    )

    assert [result["tool_call"]["connector_slug"] for result in results] == ["sqlite", "postgres"]
    assert len(started) == 2
    assert max(started) - min(started) < 0.04
    assert time.perf_counter() - wall_started < 0.2


def test_combined_tool_answer_preserves_sql_table_and_citations() -> None:
    combined = _combined_tool_answer(
        [
            {
                "answer": "first",
                "provider": "openai",
                "llm_status": "mcp_tool_completed",
                "status": "ok",
                "sql": "select 1",
                "table": "customers",
                "rows": [{"id": 1}],
                "citations": [{"title": "customers", "connector": "postgres"}],
            },
            {
                "answer": "second",
                "provider": "openai",
                "llm_status": "mcp_tool_completed",
                "status": "ok",
                "rows": [{"id": 2}],
                "citations": [],
            },
        ]
    )

    assert combined["sql"] == "select 1"
    assert combined["table"] == "customers"
    assert combined["citations"] == [{"title": "customers", "connector": "postgres"}]
    assert combined["rows"] == [{"id": 1}, {"id": 2}]


def test_sql_datastore_default_schema_does_not_use_database_name_for_postgres() -> None:
    assert _default_schema_for_datastore("postgres", {"database": "analytics"}) == "public"
    assert _default_schema_for_datastore("redshift", {"database": "analytics"}) == "public"
    assert _default_schema_for_datastore("mysql", {"database": "analytics"}) == "analytics"


def test_combined_tool_answer_preserves_pending_approval_over_successes() -> None:
    pending_tool_call = {"connector_slug": "postgres", "tool": "write_drop_table"}
    pending_tool_result = {"status": "pending_approval", "operation": "drop table"}
    combined = _combined_tool_answer(
        [
            {
                "answer": "read finished",
                "provider": "openai",
                "llm_status": "mcp_tool_completed",
                "status": "ok",
                "sql": "select 1",
                "table": "customers",
                "rows": [{"id": 1}],
                "tool_call": {"connector_slug": "postgres", "tool": "read_query_select"},
                "tool_result": {"status": "ok"},
            },
            {
                "answer": "approval required",
                "provider": "openai",
                "llm_status": "pending_approval",
                "status": "pending_approval",
                "alert_id": "alert-1",
                "tool_call": pending_tool_call,
                "tool_result": pending_tool_result,
            },
        ]
    )

    assert combined["status"] == "pending_approval"
    assert combined["llm_status"] == "pending_approval"
    assert combined["alert_id"] == "alert-1"
    assert combined["sql"] == "select 1"
    assert combined["table"] == "customers"
    assert combined["tool_call"] == pending_tool_call
    assert combined["tool_result"] == pending_tool_result
    assert len(combined["tool_results"]) == 2
    assert "Already completed before approval was requested" in combined["answer"]
    assert "postgres.read_query_select" in combined["answer"]


@pytest.mark.asyncio
async def test_invalid_openai_tool_call_returns_aligned_error_result(mcp_session) -> None:
    session, tool_engine, agent = mcp_session
    tool_call = SimpleNamespace(
        function=SimpleNamespace(
            name="invalid_tool_name",
            arguments="{}",
        )
    )
    result = await _run_openai_mcp_tool_call(
        session=session,
        tool_engine=tool_engine,
        agent=agent,
        tool_call=tool_call,
        user_email="admin@test.local",
    )

    assert result is not None
    assert result["llm_status"] == "mcp_tool_error"
    assert result["tool_call"] == {"connector_slug": None, "tool": "invalid_tool_name"}


@pytest.mark.asyncio
async def test_openai_tool_call_marks_run_timed_out_when_budget_exceeded(mcp_session) -> None:
    session, tool_engine, agent = mcp_session
    run = AgentRun(
        workspace_id=agent.workspace_id,
        agent_name="chat",
        status="running",
        state="running",
        summary="running",
        timeline=[],
        budget_tokens=0,
        budget_seconds=30,
    )
    session.add(run)
    await session.commit()
    tool_call = SimpleNamespace(
        function=SimpleNamespace(
            name="sqlite__read_query_select",
            arguments='{"sql": "select name from customers order by id"}',
        )
    )

    with pytest.raises(BudgetExceeded):
        await _run_openai_mcp_tool_calls(
            session=session,
            tool_engine=tool_engine,
            agent=agent,
            tool_calls=[tool_call],
            user_email="admin@test.local",
            run_id=run.id,
        )

    refreshed = await session.get(AgentRun, run.id)
    assert refreshed is not None
    assert refreshed.state == "timed_out"
    assert refreshed.error_message == "Run token budget exceeded."
