from __future__ import annotations

import json
import os

import httpx
import pytest
import respx
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

import app.services.mcp_executor as mcp_executor
from app.core.config import get_settings
from app.core.security import encrypt_json
from app.db.base import Base
from app.models.domain import Connector, Workspace
from app.services.mcp_executor import _airbyte_tool, _airflow_tool, _dbt_tool, _fivetran_tool


@pytest.fixture(scope="module")
async def airflow_session(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("airflow")
    os.environ["MASTER_KEY"] = "test-master-key-please-change"
    get_settings.cache_clear()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path/'app.sqlite'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        workspace = Workspace(name="Test")
        session.add(workspace)
        await session.flush()
        credentials_by_slug = {
            "airflow": {"base_url": "http://airflow", "username": "admin", "password": "admin"},
            "airbyte": {"base_url": "http://airbyte", "api_key": "airbyte-key"},
            "fivetran": {"api_key": "fivetran-key", "api_secret": "fivetran-secret"},
            "dbt": {"base_url": "http://dbt", "api_token": "dbt-key"},
        }
        for slug, credentials in credentials_by_slug.items():
            session.add(
                Connector(
                    workspace_id=workspace.id,
                    slug=slug,
                    category="etl_orchestration",
                    display_name=slug.title(),
                    credential_state="configured",
                    encrypted_credentials=encrypt_json(get_settings().master_key, credentials),
                )
            )
        await session.commit()
        yield session
    await engine.dispose()


@respx.mock
@pytest.mark.asyncio
async def test_airflow_operational_reads_use_bounded_params_and_encoded_paths(
    airflow_session: AsyncSession,
) -> None:
    seen: list[tuple[str, str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((str(request.url), request.url.path, dict(request.url.params)))
        if request.url.path.endswith("/taskInstances"):
            return httpx.Response(200, json={"task_instances": [{"task_id": "extract"}], "total_entries": 1})
        if request.url.path.endswith("/dagRuns"):
            return httpx.Response(200, json={"dag_runs": [{"run_id": "manual__1"}], "total_entries": 1})
        if request.url.path.endswith("/xcomEntries/return_value"):
            return httpx.Response(200, json={"key": "return_value", "value": {"rows": 3}})
        if request.url.path.endswith("/logs/3"):
            assert request.headers["Accept"] == "text/plain"
            return httpx.Response(200, text="x" * 50_010)
        if request.url.path == "/api/v1/pools":
            return httpx.Response(200, json={"pools": [{"name": "default_pool"}], "total_entries": 1})
        if request.url.path == "/api/v1/pools/default_pool":
            return httpx.Response(200, json={"name": "default_pool", "slots": 128})
        if request.url.path == "/api/v1/variables":
            return httpx.Response(200, json={"variables": [{"key": "env"}], "total_entries": 1})
        if request.url.path == "/api/v1/variables/env":
            return httpx.Response(200, json={"key": "env", "value": "test"})
        if request.url.path == "/api/v1/dags/daily etl/details":
            return httpx.Response(200, json={"tasks": [{"task_id": "extract", "downstream_task_ids": ["load"]}]})
        if request.url.path == "/api/v1/importErrors":
            return httpx.Response(200, json={"import_errors": [{"filename": "bad.py"}], "total_entries": 1})
        return httpx.Response(404, json={"path": request.url.path})

    respx.route(url__regex=r"http://airflow/.*").mock(side_effect=handler)
    session = airflow_session

    task_instances = await _airflow_tool(
        session,
        "read_list_task_instances",
        {"dag_id": "daily etl", "run_id": "manual/run", "limit": 2000},
        "agent-1",
    )
    dag_runs = await _airflow_tool(
        session,
        "read_list_dag_runs",
        {"dag_id": "daily etl", "since": "2026-05-01T00:00:00Z", "limit": 2},
        "agent-1",
    )
    xcom = await _airflow_tool(
        session,
        "read_get_xcom",
        {"dag_id": "daily etl", "run_id": "manual/run", "task_id": "extract rows", "key": "return_value"},
        "agent-1",
    )
    task_logs = await _airflow_tool(
        session,
        "read_get_task_logs",
        {"dag_id": "daily etl", "run_id": "manual/run", "task_id": "extract rows", "try_number": 3},
        "agent-1",
    )
    pools = await _airflow_tool(session, "read_list_pools", {}, "agent-1")
    pool = await _airflow_tool(session, "read_get_pool", {"name": "default_pool"}, "agent-1")
    variables = await _airflow_tool(session, "read_list_variables", {}, "agent-1")
    variable = await _airflow_tool(session, "read_get_variable", {"key": "env"}, "agent-1")
    dependencies = await _airflow_tool(session, "read_get_dag_dependencies", {"dag_id": "daily etl"}, "agent-1")
    import_errors = await _airflow_tool(session, "read_get_import_errors", {}, "agent-1")

    assert task_instances["task_instances"] == [{"task_id": "extract"}]
    assert dag_runs["dag_runs"] == [{"run_id": "manual__1"}]
    assert xcom["xcom"]["value"] == {"rows": 3}
    assert task_logs["logs"] == "x" * 50_000
    assert pools["pools"][0]["name"] == "default_pool"
    assert pool["pool"]["slots"] == 128
    assert variables["variables"] == [{"key": "env"}]
    assert variable["variable"]["value"] == "test"
    assert dependencies["dependencies"] == [{"task_id": "extract", "downstream_task_ids": ["load"]}]
    assert import_errors["import_errors"] == [{"filename": "bad.py"}]
    assert any(
        url.startswith("http://airflow/api/v1/dags/daily%20etl/dagRuns/manual%2Frun/taskInstances")
        and params == {"limit": "1000"}
        for url, _, params in seen
    )
    assert any(
        url.startswith("http://airflow/api/v1/dags/daily%20etl/dagRuns")
        and params == {"limit": "2", "start_date_gte": "2026-05-01T00:00:00Z"}
        for url, _, params in seen
    )
    assert any(
        url
        == (
            "http://airflow/api/v1/dags/daily%20etl/dagRuns/manual%2Frun/"
            "taskInstances/extract%20rows/logs/3"
        )
        for url, _, _ in seen
    )


@respx.mock
@pytest.mark.asyncio
async def test_airflow_create_dag_requires_approval_then_writes_local_file_when_rest_create_is_unsupported(
    airflow_session: AsyncSession,
    tmp_path,
    monkeypatch,
) -> None:
    dags_dir = tmp_path / "dags"
    monkeypatch.setenv("DATACLAW_AIRFLOW_DAGS_DIR", str(dags_dir))
    monkeypatch.setattr(mcp_executor, "_reserialize_local_airflow_dags", lambda: True)
    respx.post("http://airflow/api/v1/dags").mock(return_value=httpx.Response(405, json={"detail": "unsupported"}))

    pending = await _airflow_tool(
        airflow_session,
        "write_create_dag",
        {"dag_id": "weekly_revenue"},
        "agent-1",
    )
    assert pending["status"] == "pending_approval"

    result = await _airflow_tool(
        airflow_session,
        "write_create_dag",
        {
            "dag_id": "weekly_revenue",
            "schedule_interval": "0 6 * * MON",
            "source": "from airflow import DAG\ndag_id = 'weekly_revenue'\n",
            "__approved": True,
        },
        "agent-1",
    )

    dag_file = dags_dir / "weekly_revenue.py"
    assert result["status"] == "created"
    assert result["dag"]["dag_id"] == "weekly_revenue"
    assert result["dag"]["fileloc"] == str(dag_file)
    assert result["airflow_metadata_refreshed"] is True
    assert dag_file.read_text(encoding="utf-8") == "from airflow import DAG\ndag_id = 'weekly_revenue'\n"


@respx.mock
@pytest.mark.asyncio
async def test_airflow_operational_writes_use_expected_payloads(
    airflow_session: AsyncSession,
) -> None:
    seen: list[tuple[str, str, str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode() or "{}")
        seen.append((request.method, str(request.url), request.url.path, body))
        if request.url.path.endswith("/clearTaskInstances"):
            return httpx.Response(200, json={"task_instances": [{"task_id": body["task_ids"][0]}]})
        if request.url.path.endswith("/taskInstances/extract"):
            return httpx.Response(200, json={"task_id": "extract", "state": body["new_state"]})
        if request.method == "DELETE" and request.url.path.startswith("/api/v1/dags/"):
            return httpx.Response(204, json={})
        if request.url.path.startswith("/api/v1/dags/"):
            return httpx.Response(200, json={"dag_id": "daily etl", "is_paused": body.get("is_paused")})
        if request.url.path.startswith("/api/v1/variables/"):
            return httpx.Response(404, json={})
        if request.url.path == "/api/v1/variables":
            return httpx.Response(200, json={"key": body["key"], "value": body["value"]})
        if request.url.path.startswith("/api/v1/pools/"):
            return httpx.Response(404, json={})
        if request.url.path == "/api/v1/pools":
            return httpx.Response(200, json={"name": body["name"], "slots": body["slots"]})
        return httpx.Response(404, json={"path": request.url.path})

    respx.route(url__regex=r"http://airflow/.*").mock(side_effect=handler)
    session = airflow_session

    unpaused = await _airflow_tool(session, "write_unpause_dag", {"dag_id": "daily etl"}, "agent-1")
    pending_clear = await _airflow_tool(session, "write_clear_task_instance", {"dag_id": "daily etl", "run_id": "manual/run", "task_id": "extract"}, "agent-1")
    cleared = await _airflow_tool(session, "write_clear_task_instance", {"dag_id": "daily etl", "run_id": "manual/run", "task_id": "extract", "__approved": True}, "agent-1")
    marked_success = await _airflow_tool(session, "write_mark_task_success", {"dag_id": "daily etl", "run_id": "manual/run", "task_id": "extract"}, "agent-1")
    pending_failed = await _airflow_tool(session, "write_mark_task_failed", {"dag_id": "daily etl", "run_id": "manual/run", "task_id": "extract"}, "agent-1")
    marked_failed = await _airflow_tool(session, "write_mark_task_failed", {"dag_id": "daily etl", "run_id": "manual/run", "task_id": "extract", "__approved": True}, "agent-1")
    variable = await _airflow_tool(session, "write_set_variable", {"key": "env", "value": "test"}, "agent-1")
    pool = await _airflow_tool(session, "write_set_pool", {"name": "nightly", "slots": 3}, "agent-1")
    pending_delete = await _airflow_tool(session, "write_delete_dag", {"dag_id": "daily etl"}, "agent-1")
    deleted = await _airflow_tool(session, "write_delete_dag", {"dag_id": "daily etl", "__approved": True}, "agent-1")

    assert unpaused["dag"]["is_paused"] is False
    assert pending_clear["status"] == "pending_approval"
    assert pending_failed["status"] == "pending_approval"
    assert pending_delete["status"] == "pending_approval"
    assert cleared["result"] == {"task_instances": [{"task_id": "extract"}]}
    assert marked_success["task_instance"]["state"] == "success"
    assert marked_failed["task_instance"]["state"] == "failed"
    assert variable["variable"] == {"key": "env", "value": "test"}
    assert pool["pool"] == {"name": "nightly", "slots": 3}
    assert deleted["status"] == "deleted"
    assert any(
        method == "PATCH"
        and url == "http://airflow/api/v1/dags/daily%20etl"
        and body == {"is_paused": False}
        for method, url, _, body in seen
    )
    assert any(
        method == "POST"
        and path == "/api/v1/variables"
        and body == {"key": "env", "value": "test", "description": None}
        for method, _, path, body in seen
    )
    assert any(
        method == "POST"
        and path == "/api/v1/pools"
        and body == {"name": "nightly", "slots": 3, "description": None}
        for method, _, path, body in seen
    )
    assert any(method == "POST" and path == "/api/v1/dags/daily etl/clearTaskInstances" and body["task_ids"] == ["extract"] and body["only_failed"] is False for method, _, path, body in seen)
    assert any(method == "PATCH" and path.endswith("/taskInstances/extract") and body == {"new_state": "success", "dry_run": False} for method, _, path, body in seen)
    assert any(method == "PATCH" and path.endswith("/taskInstances/extract") and body == {"new_state": "failed", "dry_run": False} for method, _, path, body in seen)
    assert any(method == "DELETE" and path == "/api/v1/dags/daily etl" for method, _, path, _ in seen)


@respx.mock
@pytest.mark.asyncio
async def test_sibling_log_tools_encode_path_segments(
    airflow_session: AsyncSession,
) -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        if request.url.path.startswith("/v1/jobs/"):
            return httpx.Response(200, json={"job": {"id": "job/1"}, "logs": ["failed"]})
        if request.url.path.startswith("/v1/connectors/"):
            return httpx.Response(200, json={"data": {"id": "connector/1", "status": {"tasks": [{"message": "connector failed"}]}}})
        if request.url.path.startswith("/runs/") and request.url.path.endswith("/artifacts/run_results.json"):
            return httpx.Response(200, json={"results": [{"status": "error"}]})
        if request.url.path.startswith("/runs/"):
            return httpx.Response(200, json={"data": {"id": "run/1"}})
        return httpx.Response(404, json={"path": request.url.path})

    respx.route(url__regex=r"(http://airbyte|https://api\.fivetran\.com|http://dbt)/.*").mock(side_effect=handler)
    session = airflow_session

    await _airbyte_tool(session, "read_get_job_logs", {"job_id": "job/1"}, "agent-1")
    await _fivetran_tool(session, "read_get_connector_logs", {"connector_id": "connector/1"}, "agent-1")
    await _dbt_tool(session, "read_get_run_logs", {"run_id": "run/1"}, "agent-1")

    assert "http://airbyte/v1/jobs/job%2F1" in seen
    assert "https://api.fivetran.com/v1/connectors/connector%2F1" in seen
    assert any(url.startswith("http://dbt/runs/run%2F1/?") for url in seen)
    assert "http://dbt/runs/run%2F1/artifacts/run_results.json" in seen


@respx.mock
@pytest.mark.asyncio
async def test_fivetran_write_tools_encode_connector_id(
    airflow_session: AsyncSession,
) -> None:
    seen: list[tuple[str, str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode() or "{}")
        seen.append((request.method, str(request.url), body))
        if request.url.path.endswith("/force"):
            return httpx.Response(200, json={"code": "Success"})
        return httpx.Response(200, json={"data": {"id": "connector/1", "paused": body.get("paused")}})

    respx.route(url__regex=r"https://api\.fivetran\.com/.*").mock(side_effect=handler)
    session = airflow_session

    triggered = await _fivetran_tool(session, "write_trigger_sync", {"connector_id": "connector/1"}, "agent-1")
    paused = await _fivetran_tool(
        session,
        "write_pause_connector",
        {"connector_id": "connector/1", "paused": True},
        "agent-1",
    )

    assert triggered["status"] == "triggered"
    assert paused["result"]["data"]["paused"] is True
    assert ("POST", "https://api.fivetran.com/v1/connectors/connector%2F1/force", {}) in seen
    assert (
        "PATCH",
        "https://api.fivetran.com/v1/connectors/connector%2F1",
        {"paused": True},
    ) in seen
