from __future__ import annotations

import importlib
import os
from uuid import uuid4

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from app.core.config import get_settings
from app.core.security import encrypt_json
from app.models.domain import AgentToolCall, AgentWriteAudit, Connector


@pytest.fixture(scope="module")
async def app_client(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("mcp-grants-app")
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path/'app.sqlite'}"
    os.environ["DEMO_DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path/'demo.sqlite'}"
    os.environ["DEMO_MODE"] = "true"
    os.environ["MASTER_KEY"] = "test-master-key-please-change"
    os.environ["SESSION_SECRET"] = "test-session-secret-please-change"
    os.environ["DATACLAW_VECTOR_TEST_DOUBLE"] = "true"
    os.environ["DATACLAW_TEST_AUTO_CREATE_SCHEMA"] = "true"
    os.environ["DATACLAW_BCRYPT_ROUNDS"] = "4"

    from app.core.config import get_settings

    get_settings.cache_clear()
    import app.db.session as session_module

    importlib.reload(session_module)
    from app import main as main_module

    importlib.reload(main_module)
    transport = ASGITransport(app=main_module.app)
    async with main_module.app.router.lifespan_context(main_module.app):
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            login = await ac.post(
                "/auth/login",
                json={"email": "admin@dataclaw.local", "password": "dataclaw-local-admin"},
            )
            assert login.status_code == 200
            yield ac


@pytest.mark.asyncio
async def test_mcp_sqlite_tools_are_grant_gated_and_audited(app_client: AsyncClient) -> None:
    ac = app_client
    assert (await ac.post("/connectors/sqlite/test", json={"credentials": {}})).status_code == 200
    assert (await ac.post("/connectors/sqlite/sync")).status_code == 200

    agents = (await ac.get("/agents")).json()
    chat_agent = next(agent for agent in agents if agent["name"] == "chat")
    chat_headers = {"X-DataClaw-Agent-Id": chat_agent["id"]}
    table_name = f"grant_test_summary_{uuid4().hex[:8]}"

    listed = await ac.post(
        "/mcp/sqlite/tools/read_list_tables",
        json={"arguments": {}},
        headers=chat_headers,
    )
    assert listed.status_code == 200
    assert {"customers", "orders", "products"}.issubset(set(listed.json()["tables"]))

    grant_update = await ac.put(
        f"/agents/{chat_agent['id']}/grants",
        json={"grants": [{"connector_slug": "sqlite", "read_enabled": True, "write_enabled": True}]},
    )
    assert grant_update.status_code == 200

    created = await ac.post(
        "/mcp/sqlite/tools/write_create_table",
        json={"arguments": {"table": table_name, "columns": [{"name": "month", "type": "text"}]}},
        headers=chat_headers,
    )
    assert created.status_code == 200
    assert created.json()["status"] == "pending_approval"

    before_approval = await ac.post(
        "/mcp/sqlite/tools/read_get_schema",
        json={"arguments": {"table": table_name}},
        headers=chat_headers,
    )
    assert before_approval.status_code == 404

    approved_create = await ac.post(f"/alerts/{created.json()['alert_id']}/approve-and-execute")
    assert approved_create.status_code == 200
    assert approved_create.json()["status"] == "executed"

    schema = await ac.post(
        "/mcp/sqlite/tools/read_get_schema",
        json={"arguments": {"table": table_name}},
        headers=chat_headers,
    )
    assert schema.status_code == 200
    assert schema.json()["columns"][0]["name"] == "month"

    audit = await ac.get(f"/agents/{chat_agent['id']}/audit")
    assert audit.status_code == 200
    assert any(row["statement_type"] == "CREATE_TABLE" and row["target"] == table_name for row in audit.json())

    destructive = await ac.post(
        "/mcp/sqlite/tools/write_execute_sql",
        json={"arguments": {"sql": f"drop table {table_name}"}},
        headers=chat_headers,
    )
    assert destructive.status_code == 200
    assert destructive.json()["status"] == "pending_approval"
    from app.db.session import SessionLocal

    async with SessionLocal() as session:
        tool_call = await session.scalar(select(AgentToolCall).where(AgentToolCall.tool_name == "write_execute_sql"))
        assert tool_call is not None
        assert tool_call.status == "pending_approval"

    still_exists = await ac.post(
        "/mcp/sqlite/tools/read_get_schema",
        json={"arguments": {"table": table_name}},
        headers=chat_headers,
    )
    assert still_exists.status_code == 200

    approved = await ac.post(f"/alerts/{destructive.json()['alert_id']}/approve-and-execute")
    assert approved.status_code == 200
    assert approved.json()["status"] == "executed"

    gone = await ac.post(
        "/mcp/sqlite/tools/read_get_schema",
        json={"arguments": {"table": table_name}},
        headers=chat_headers,
    )
    assert gone.status_code == 404


@pytest.mark.asyncio
async def test_airflow_destructive_write_requires_approval_and_executes_after_approval(
    app_client: AsyncClient,
    monkeypatch,
) -> None:
    ac = app_client
    from app.db.session import SessionLocal

    async with SessionLocal() as session:
        connector = await session.scalar(select(Connector).where(Connector.slug == "airflow"))
        assert connector is not None
        connector.credential_state = "configured"
        connector.encrypted_credentials = encrypt_json(
            get_settings().master_key,
            {"base_url": "http://airflow", "username": "admin", "password": "admin"},
        )
        await session.commit()

    agents = (await ac.get("/agents")).json()
    chat_agent = next(agent for agent in agents if agent["name"] == "chat")
    grant_update = await ac.put(
        f"/agents/{chat_agent['id']}/grants",
        json={"grants": [{"connector_slug": "airflow", "read_enabled": True, "write_enabled": True}]},
    )
    assert grant_update.status_code == 200

    seen: list[tuple[str, str]] = []
    original_client = httpx.AsyncClient

    def _client_factory(*args, **kwargs):
        def handler(request: httpx.Request) -> httpx.Response:
            seen.append((request.method, request.url.path))
            if request.method == "DELETE" and request.url.path == "/api/v1/dags/daily etl":
                return httpx.Response(204, json={})
            return httpx.Response(404, json={"path": request.url.path})

        kwargs["transport"] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", _client_factory)

    pending = await ac.post(
        "/mcp/airflow/tools/write_delete_dag",
        json={"arguments": {"dag_id": "daily etl"}},
        headers={"X-DataClaw-Agent-Id": chat_agent["id"]},
    )
    assert pending.status_code == 200
    assert pending.json()["status"] == "pending_approval"
    assert seen == []

    reserved = await ac.post(
        "/mcp/airflow/tools/write_delete_dag",
        json={"arguments": {"dag_id": "daily etl", "__approved": True}},
        headers={"X-DataClaw-Agent-Id": chat_agent["id"]},
    )
    assert reserved.status_code == 400
    assert reserved.json()["detail"] == "__approved is reserved."
    assert seen == []

    approved = await ac.post(f"/alerts/{pending.json()['alert_id']}/approve-and-execute")
    assert approved.status_code == 200
    assert approved.json()["status"] == "executed"
    assert approved.json()["result"]["status"] == "deleted"
    assert ("DELETE", "/api/v1/dags/daily etl") in seen


@pytest.mark.asyncio
async def test_non_sql_mcp_write_records_write_audit(app_client: AsyncClient, tmp_path) -> None:
    ac = app_client
    from app.db.session import SessionLocal

    project_path = tmp_path / "dbt_project"

    async with SessionLocal() as session:
        connector = await session.scalar(select(Connector).where(Connector.slug == "dbt"))
        assert connector is not None
        connector.credential_state = "configured"
        connector.encrypted_credentials = encrypt_json(
            get_settings().master_key,
            {"api_token": "token", "account_id": "1"},
        )
        await session.commit()

    agents = (await ac.get("/agents")).json()
    chat_agent = next(agent for agent in agents if agent["name"] == "chat")
    grant_update = await ac.put(
        f"/agents/{chat_agent['id']}/grants",
        json={"grants": [{"connector_slug": "dbt", "read_enabled": True, "write_enabled": True}]},
    )
    assert grant_update.status_code == 200

    created = await ac.post(
        "/mcp/dbt/tools/write_create_model",
        json={
            "arguments": {
                "project_path": str(project_path),
                "schema": "marts",
                "name": "dim_test_e2e",
                "sql": "select 1 as id",
            }
        },
        headers={"X-DataClaw-Agent-Id": chat_agent["id"]},
    )
    assert created.status_code == 200
    assert created.json()["status"] == "pending_approval"
    alert_id = created.json()["alert_id"]

    approved = await ac.post(f"/alerts/{alert_id}/approve-and-execute")
    assert approved.status_code == 200
    assert approved.json()["result"]["status"] == "created"

    audit = await ac.get(f"/agents/{chat_agent['id']}/audit")
    assert audit.status_code == 200
    assert any(
        row["connector_slug"] == "dbt"
        and row["statement_type"] == "CREATE_MODEL"
        and row["target"] == "dim_test_e2e"
        and row["required_approval"] is False
        for row in audit.json()
    )

    async with SessionLocal() as session:
        tool_call = await session.scalar(select(AgentToolCall).where(AgentToolCall.tool_name == "write_create_model"))
        write_audit = await session.scalar(select(AgentWriteAudit).where(AgentWriteAudit.connector_slug == "dbt"))
        assert tool_call is not None
        assert tool_call.status in {"pending_approval", "created"}
        assert write_audit is not None


@pytest.mark.asyncio
async def test_custom_agent_without_write_grant_gets_403(app_client: AsyncClient) -> None:
    ac = app_client
    created = await ac.post("/agents", json={"name": "analyst", "system_prompt": "Read only."})
    assert created.status_code == 200
    agent_id = created.json()["id"]
    grants = [
        {"connector_slug": "sqlite", "read_enabled": True, "write_enabled": False},
    ]
    assert (await ac.put(f"/agents/{agent_id}/grants", json={"grants": grants})).status_code == 200

    response = await ac.post(
        "/mcp/sqlite/tools/write_create_table",
        json={"arguments": {"table": "blocked_write", "columns": [{"name": "id", "type": "integer"}]}},
        headers={"X-DataClaw-Agent-Id": agent_id},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_bigquery_and_snowflake_report_missing_optional_drivers(app_client: AsyncClient) -> None:
    ac = app_client
    bigquery_credentials = {
        "project_id": "dataclaw-test",
        "service_account_json": '{"type":"service_account","project_id":"dataclaw-test"}',
    }
    snowflake_credentials = {
        "account": "example",
        "warehouse": "COMPUTE_WH",
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "user": "DATA",
        "password": "secret",
    }
    assert (
        await ac.post(
            "/connectors/bigquery/test",
            json={"credentials": bigquery_credentials},
        )
    ).status_code == 200
    assert (
        await ac.post(
            "/connectors/snowflake/test",
            json={"credentials": snowflake_credentials},
        )
    ).status_code == 200

    from app.db.session import SessionLocal

    async with SessionLocal() as session:
        for slug, credentials in {
            "bigquery": bigquery_credentials,
            "snowflake": snowflake_credentials,
        }.items():
            connector = await session.scalar(select(Connector).where(Connector.slug == slug))
            connector.credential_state = "configured"
            connector.encrypted_credentials = encrypt_json(get_settings().master_key, credentials)
        await session.commit()

    agents = (await ac.get("/agents")).json()
    chat_agent = next(agent for agent in agents if agent["name"] == "chat")
    headers = {"X-DataClaw-Agent-Id": chat_agent["id"]}
    grant_update = await ac.put(
        f"/agents/{chat_agent['id']}/grants",
        json={
            "grants": [
                {"connector_slug": "bigquery", "read_enabled": True, "write_enabled": False},
                {"connector_slug": "snowflake", "read_enabled": True, "write_enabled": False},
            ]
        },
    )
    grant_update.raise_for_status()

    bigquery = await ac.post(
        "/mcp/bigquery/tools/read_list_jobs",
        json={"arguments": {}},
        headers=headers,
    )
    assert bigquery.status_code in {400, 501}
    if bigquery.status_code == 400:
        assert bigquery.json()["detail"] == "Invalid BigQuery service_account_json."
    else:
        assert "google-cloud-bigquery" in bigquery.json()["detail"]

    snowflake = await ac.post(
        "/mcp/snowflake/tools/read_list_tables",
        json={"arguments": {}},
        headers=headers,
    )
    assert snowflake.status_code in {400, 501}
    if snowflake.status_code == 400:
        assert "Snowflake" in snowflake.json()["detail"] or "snowflake" in snowflake.json()["detail"]
    else:
        assert "snowflake-connector-python" in snowflake.json()["detail"]


@pytest.mark.asyncio
async def test_fastmcp_server_exposes_catalog_tools() -> None:
    from app.services.mcp_servers import build_mcp_server

    server = build_mcp_server("sqlite")
    tools = await server.list_tools()
    names = {tool.name for tool in tools}
    assert {"read_list_tables", "read_get_schema", "write_create_table", "write_execute_sql"}.issubset(names)


@pytest.mark.asyncio
async def test_fastmcp_tool_rejects_reserved_approval_argument(monkeypatch) -> None:
    from app.services.mcp_servers import _tool_callable

    async def fail_execute_mcp_tool(**kwargs):
        raise AssertionError("reserved approval argument reached executor")

    monkeypatch.setattr("app.services.mcp_servers.execute_mcp_tool", fail_execute_mcp_tool)
    tool = _tool_callable("airflow", "write_delete_dag")

    with pytest.raises(ValueError, match="__approved is reserved"):
        await tool(agent_id="agent-1", arguments={"dag_id": "daily etl", "__approved": True})
