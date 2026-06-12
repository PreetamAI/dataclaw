from __future__ import annotations

import importlib
import json
import os
import time
import urllib.error
import urllib.request

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select


def _seed_prefect_flow() -> str:
    base_url = "http://127.0.0.1:18082"
    flow_id: str | None = None
    deadline = time.time() + 30
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            request = urllib.request.Request(
                f"{base_url}/api/flows/",
                data=json.dumps({"name": "orders-flow", "tags": ["dataclaw"]}).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                flow_id = json.loads(response.read().decode())["id"]
            break
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in {409, 422}:
                time.sleep(1)
                continue
        except Exception as exc:  # pragma: no cover - Docker startup timing varies
            last_error = exc
            time.sleep(1)
    if flow_id is None:
        raise RuntimeError(f"Could not seed Prefect flow: {last_error}")

    request = urllib.request.Request(
        f"{base_url}/api/deployments/",
        data=json.dumps(
            {
                "name": "deployment-orders",
                "flow_id": flow_id,
                "entrypoint": "flows.py:orders",
                "version": "integration",
                "tags": ["dataclaw"],
            }
        ).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.loads(response.read().decode())["id"]
    except urllib.error.HTTPError as exc:
        if exc.code not in {409, 422}:
            raise
    request = urllib.request.Request(
        f"{base_url}/api/deployments/filter",
        data=json.dumps({"limit": 50}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        deployments = json.loads(response.read().decode())
    for deployment in deployments:
        if deployment.get("name") == "deployment-orders":
            return deployment["id"]
    raise RuntimeError("Could not seed Prefect deployment.")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_e2e_chat_agent_sqlite_chroma_chart_and_approval(monkeypatch, tmp_path) -> None:
    if os.getenv("RUN_CONNECTOR_INTEGRATION") != "1":
        pytest.skip("Set RUN_CONNECTOR_INTEGRATION=1 to run the chat-agent integration scenario.")

    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path/'app.sqlite'}")
    monkeypatch.setenv("DEMO_DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path/'demo.sqlite'}")
    monkeypatch.setenv("DEMO_MODE", "true")
    monkeypatch.setenv("MASTER_KEY", "test-master-key-please-change")
    monkeypatch.setenv("SESSION_SECRET", "test-session-secret-please-change")
    monkeypatch.setenv("CHROMA_URL", "http://localhost:18001")

    from app.core.config import get_settings

    get_settings.cache_clear()
    import app.db.session as session_module

    importlib.reload(session_module)
    import app.services.vector_store as vector_store_module

    importlib.reload(vector_store_module)
    import app.services.sync_materializer as sync_materializer_module

    importlib.reload(sync_materializer_module)
    import app.services.ingestion.wiki_store as wiki_store_module

    importlib.reload(wiki_store_module)
    import app.services.ingestion.service as ingestion_service_module

    importlib.reload(ingestion_service_module)
    import app.services.agents.chat as chat_module

    importlib.reload(chat_module)
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

            from app.models.domain import Workspace
            from app.services.vector_store import vector_store

            async with session_module.SessionLocal() as db_session:
                workspace = await db_session.scalar(select(Workspace).limit(1))
            collection_name = vector_store._collection_name(workspace.id)
            vector_store._collections.pop(collection_name, None)
            try:
                collection = vector_store._get_collection(workspace.id)
                if collection is not None and vector_store._client is not None:
                    vector_store._client.delete_collection(collection_name)
            except Exception:
                pass
            vector_store._collections.pop(collection_name, None)

            assert (await ac.post("/connectors/sqlite/test", json={"credentials": {}})).json()["status"] == "ok"
            postgres_credentials = {
                "database_url": "postgresql+psycopg://dataclaw:dataclaw@127.0.0.1:15432/dataclaw_integration"
            }
            postgres_test = await ac.post("/connectors/postgres/test", json={"credentials": postgres_credentials})
            assert postgres_test.status_code == 200
            assert postgres_test.json()["status"] == "ok"
            mysql_credentials = {
                "host": "127.0.0.1",
                "port": "13306",
                "database": "dataclaw_integration",
                "user": "dataclaw",
                "password": "dataclaw",
            }
            mysql_test = await ac.post("/connectors/mysql/test", json={"credentials": mysql_credentials})
            assert mysql_test.status_code == 200
            assert mysql_test.json()["status"] == "ok"
            airflow_credentials = {
                "base_url": "http://localhost:18080",
                "username": "admin",
                "password": "admin",
            }
            airflow_test = await ac.post("/connectors/airflow/test", json={"credentials": airflow_credentials})
            assert airflow_test.status_code == 200
            assert airflow_test.json()["status"] == "ok"
            airbyte_credentials = {"api_url": "http://127.0.0.1:18081", "api_key": "integration-token"}
            airbyte_test = await ac.post("/connectors/airbyte/test", json={"credentials": airbyte_credentials})
            assert airbyte_test.status_code == 200
            assert airbyte_test.json()["status"] == "ok"
            prefect_credentials = {"api_url": "http://127.0.0.1:18082", "api_key": "integration-token"}
            prefect_test = await ac.post("/connectors/prefect/test", json={"credentials": prefect_credentials})
            assert prefect_test.status_code == 200
            assert prefect_test.json()["status"] == "ok"
            dagster_credentials = {"graphql_url": "http://127.0.0.1:18083/graphql", "token": "integration-token"}
            dagster_test = await ac.post("/connectors/dagster/test", json={"credentials": dagster_credentials})
            assert dagster_test.status_code == 200
            assert dagster_test.json()["status"] == "ok"
            databricks_credentials = {
                "workspace_url": "http://localhost:18084",
                "http_path": "warehouse-analytics",
                "token": "databricks-token",
            }
            databricks_test = await ac.post("/connectors/databricks/test", json={"credentials": databricks_credentials})
            assert databricks_test.status_code == 200
            assert databricks_test.json()["status"] == "ok"
            notion_credentials = {
                "base_url": "http://localhost:18084",
                "integration_token": "notion-token",
            }
            notion_test = await ac.post("/connectors/notion/test", json={"credentials": notion_credentials})
            assert notion_test.status_code == 200
            assert notion_test.json()["status"] == "ok"
            quip_credentials = {"base_url": "http://localhost:18084", "access_token": "quip-token"}
            quip_test = await ac.post("/connectors/quip/test", json={"credentials": quip_credentials})
            assert quip_test.status_code == 200
            assert quip_test.json()["status"] == "ok"
            confluence_credentials = {
                "base_url": "http://localhost:18084",
                "site_url": "http://localhost:18084",
                "email": "data@dataclaw.local",
                "api_token": "confluence-token",
            }
            confluence_test = await ac.post("/connectors/confluence/test", json={"credentials": confluence_credentials})
            assert confluence_test.status_code == 200
            assert confluence_test.json()["status"] == "ok"
            github_credentials = {
                "base_url": "http://localhost:18084",
                "token": "github-token",
                "repositories": "dataclaw/analytics",
            }
            github_test = await ac.post("/connectors/github/test", json={"credentials": github_credentials})
            assert github_test.status_code == 200
            assert github_test.json()["status"] == "ok"
            dbt_credentials = {
                "base_url": "http://localhost:18084/api/v2/accounts/42",
                "api_token": "dbt-token",
                "account_id": "42",
                "job_id": "100",
            }
            dbt_test = await ac.post("/connectors/dbt/test", json={"credentials": dbt_credentials})
            assert dbt_test.status_code == 200
            assert dbt_test.json()["status"] == "ok"
            sync = await ac.post("/connectors/sqlite/sync")
            assert sync.status_code == 200
            assert sync.json()["objects_synced"] >= 3
            postgres_sync = await ac.post("/connectors/postgres/sync")
            assert postgres_sync.status_code == 200
            assert any(table["name"] == "orders" for table in postgres_sync.json()["tables"])
            mysql_sync = await ac.post("/connectors/mysql/sync")
            assert mysql_sync.status_code == 200
            assert any(table["name"] == "customers" for table in mysql_sync.json()["tables"])
            airflow_sync = await ac.post("/connectors/airflow/sync")
            assert airflow_sync.status_code == 200
            assert any(dag["dag_id"] == "daily_orders_refresh" for dag in airflow_sync.json()["dags"])
            airbyte_sync = await ac.post("/connectors/airbyte/sync")
            assert airbyte_sync.status_code == 200
            airbyte_payload = airbyte_sync.json()
            assert airbyte_payload["objects_synced"] >= 0
            prefect_deployment_id = _seed_prefect_flow()
            prefect_sync = await ac.post("/connectors/prefect/sync")
            assert prefect_sync.status_code == 200
            assert prefect_sync.json()["objects_synced"] >= 1
            dagster_sync = await ac.post("/connectors/dagster/sync")
            assert dagster_sync.status_code == 200
            assert dagster_sync.json()["objects_synced"] >= 1
            databricks_sync = await ac.post("/connectors/databricks/sync")
            assert databricks_sync.status_code == 200
            assert databricks_sync.json()["objects_synced"] >= 1
            assert (await ac.post("/connectors/notion/sync")).status_code == 200
            assert (await ac.post("/connectors/quip/sync")).status_code == 200
            assert (await ac.post("/connectors/confluence/sync")).status_code == 200
            assert (await ac.post("/connectors/github/sync")).status_code == 200
            dbt_sync = await ac.post("/connectors/dbt/sync")
            assert dbt_sync.status_code == 200
            assert dbt_sync.json()["objects_synced"] >= 2
            assert vector_store._client is not None
            indexed = await vector_store.search(workspace.id, "orders", top_k=5)
            assert any(result.metadata.get("table_name") == "orders" for result in indexed)

            listed = await ac.post(
                "/ide/chat",
                json={"question": "list all tables across data stores from Chroma"},
            )
            assert listed.status_code == 200
            listed_payload = listed.json()
            assert listed_payload["llm_status"] == "chroma_grounded"
            assert {"orders", "customers"}.issubset(
                {row["table"] for row in listed_payload["rows"]}
            )
            assert any(citation["connector"] == "chroma" for citation in listed_payload["citations"])

            last_week = await ac.post(
                "/ide/chat",
                json={"question": "how many orders did we get last week?"},
            )
            assert last_week.status_code == 200
            assert "count(*)" in last_week.json()["sql"].lower()
            assert last_week.json()["rows"] == [{"order_count": 4, "revenue": 566400}]

            agents = (await ac.get("/agents")).json()
            chat_agent = next(agent for agent in agents if agent["name"] == "chat")
            chat_headers = {"X-DataClaw-Agent-Id": chat_agent["id"]}

            postgres_tables = await ac.post(
                "/mcp/postgres/tools/read_list_tables",
                json={"arguments": {}},
                headers=chat_headers,
            )
            assert postgres_tables.status_code == 200
            assert any(table["name"] == "orders" for table in postgres_tables.json()["tables"])
            postgres_created = await ac.post(
                "/mcp/postgres/tools/write_create_table",
                json={"arguments": {"table": "pg_test_summary", "columns": [{"name": "month", "type": "text"}]}},
                headers=chat_headers,
            )
            assert postgres_created.status_code == 200
            assert postgres_created.json()["status"] == "executed"
            postgres_described = await ac.post(
                "/mcp/postgres/tools/read_get_schema",
                json={"arguments": {"schema": "public", "table": "pg_test_summary"}},
                headers=chat_headers,
            )
            assert postgres_described.status_code == 200
            assert postgres_described.json()["columns"][0]["name"] == "month"
            postgres_drop = await ac.post(
                "/mcp/postgres/tools/write_execute_sql",
                json={"arguments": {"sql": "drop table pg_test_summary"}},
                headers=chat_headers,
            )
            assert postgres_drop.status_code == 200
            assert postgres_drop.json()["status"] == "pending_approval"
            postgres_approved = await ac.post(f"/alerts/{postgres_drop.json()['alert_id']}/approve-and-execute")
            assert postgres_approved.status_code == 200
            assert postgres_approved.json()["status"] == "executed"
            postgres_audit = await ac.get(f"/agents/{chat_agent['id']}/audit")
            assert any(
                row["connector_slug"] == "postgres" and row["statement_type"] == "DROP_TABLE"
                for row in postgres_audit.json()
            )

            mysql_created = await ac.post(
                "/mcp/mysql/tools/write_create_table",
                json={"arguments": {"table": "mysql_test_summary", "columns": [{"name": "month", "type": "text"}]}},
                headers=chat_headers,
            )
            assert mysql_created.status_code == 200
            assert mysql_created.json()["status"] == "executed"
            mysql_count = await ac.post(
                "/mcp/mysql/tools/read_get_row_count",
                json={"arguments": {"table": "customers"}},
                headers=chat_headers,
            )
            assert mysql_count.status_code == 200
            assert mysql_count.json()["row_count"] == 2

            if airbyte_payload.get("connections"):
                connection_id = airbyte_payload["connections"][0].get("connectionId") or airbyte_payload["connections"][0].get("connection_id")
                airbyte_trigger = await ac.post(
                    "/mcp/airbyte/tools/write_trigger_sync",
                    json={"arguments": {"connection_id": connection_id}},
                    headers=chat_headers,
                )
                assert airbyte_trigger.status_code == 200
                assert airbyte_trigger.json()["status"] == "triggered"

            prefect_trigger = await ac.post(
                "/mcp/prefect/tools/write_trigger_flow_run",
                json={"arguments": {"deployment_id": prefect_deployment_id, "parameters": {"window": "daily"}}},
                headers=chat_headers,
            )
            assert prefect_trigger.status_code == 200
            assert prefect_trigger.json()["status"] == "triggered"
            assert prefect_trigger.json()["run"]["deployment_id"] == prefect_deployment_id

            dagster_trigger = await ac.post(
                "/mcp/dagster/tools/write_trigger_job",
                json={"arguments": {"job_name": "analytics"}},
                headers=chat_headers,
            )
            assert dagster_trigger.status_code == 200
            assert dagster_trigger.json()["status"] == "triggered"
            assert dagster_trigger.json()["run"]["status"] == "STARTED"

            databricks_tables = await ac.post(
                "/mcp/databricks/tools/read_list_tables",
                json={"arguments": {}},
                headers=chat_headers,
            )
            assert databricks_tables.status_code == 200
            assert any(table["name"] == "orders" for table in databricks_tables.json()["tables"])
            databricks_query = await ac.post(
                "/mcp/databricks/tools/read_query_select",
                json={"arguments": {"sql": "select customer_id, revenue from main.orders"}},
                headers=chat_headers,
            )
            assert databricks_query.status_code == 200
            assert databricks_query.json()["rows"][0]["customer_id"] == "demo-acme"
            databricks_job = await ac.post(
                "/mcp/databricks/tools/write_trigger_job",
                json={"arguments": {"job_id": 42}},
                headers=chat_headers,
            )
            assert databricks_job.status_code == 200
            assert databricks_job.json()["status"] == "triggered"
            assert databricks_job.json()["run"]["job_id"] == 42

            quip_search = await ac.post(
                "/mcp/quip/tools/read_search",
                json={"arguments": {"query": "revenue"}},
                headers=chat_headers,
            )
            assert quip_search.status_code == 200
            assert quip_search.json()["threads"][0]["id"] == "thread-revenue-glossary"
            confluence_search = await ac.post(
                "/mcp/confluence/tools/read_search_pages",
                json={"arguments": {"query": "revenue"}},
                headers=chat_headers,
            )
            assert confluence_search.status_code == 200
            assert confluence_search.json()["pages"][0]["id"] == "conf-revenue-glossary"
            confluence_created = await ac.post(
                "/mcp/confluence/tools/write_create_page",
                json={
                    "arguments": {
                        "space_key": "ENG",
                        "title": "Q2 Roadmap",
                        "body": "<p>Q2 roadmap generated by DataClaw.</p>",
                    }
                },
                headers=chat_headers,
            )
            assert confluence_created.status_code == 200
            assert confluence_created.json()["status"] == "created"
            async with AsyncClient(base_url="http://localhost:18084") as fixture_client:
                confluence_pages = (await fixture_client.get("/wiki/rest/api/content-created")).json()["results"]
            assert any(page["title"] == "Q2 Roadmap" for page in confluence_pages)
            google_docs_list = await ac.post(
                "/mcp/google_docs/tools/read_list_docs",
                json={"arguments": {}},
                headers=chat_headers,
            )
            assert google_docs_list.status_code == 200
            assert google_docs_list.json()["docs"][0]["id"] == "gdoc-revenue-glossary"

            notion_page = await ac.post(
                "/mcp/notion/tools/read_get_page",
                json={"arguments": {"page_id": "page-data-glossary"}},
                headers=chat_headers,
            )
            assert notion_page.status_code == 200
            assert notion_page.json()["page"]["id"] == "page-data-glossary"
            notion_append = await ac.post(
                "/mcp/notion/tools/write_append_to_page",
                json={"arguments": {"page_id": "page-data-glossary", "body": "DataClaw reviewed this glossary."}},
                headers=chat_headers,
            )
            assert notion_append.status_code == 200
            assert notion_append.json()["status"] == "appended"

            github_file = await ac.post(
                "/mcp/github/tools/read_get_file",
                json={"arguments": {"repo": "dataclaw/analytics", "path": "README.md"}},
                headers=chat_headers,
            )
            assert github_file.status_code == 200
            assert github_file.json()["file"]["path"] == "README.md"
            github_pr = await ac.post(
                "/mcp/github/tools/write_create_pr",
                json={
                    "arguments": {
                        "repo": "dataclaw/analytics",
                        "title": "Document analytics warehouse",
                        "head": "dataclaw-docs",
                        "base": "main",
                    }
                },
                headers=chat_headers,
            )
            assert github_pr.status_code == 200
            assert github_pr.json()["pull_request"]["state"] == "open"

            dbt_lineage = await ac.post(
                "/mcp/dbt/tools/read_get_lineage",
                json={"arguments": {"project_id": 1}},
                headers=chat_headers,
            )
            assert dbt_lineage.status_code == 200
            assert dbt_lineage.json()["lineage"]["edges"][0]["target"] == "model.dataclaw.fct_revenue_daily"
            dbt_test = await ac.post(
                "/mcp/dbt/tools/write_trigger_test",
                json={"arguments": {"job_id": 100}},
                headers=chat_headers,
            )
            assert dbt_test.status_code == 200
            assert dbt_test.json()["status"] == "triggered"

            created = await ac.post(
                "/ide/chat",
                json={"question": "create a table called chat_test_summary with two columns: month text, total int"},
            )
            assert created.status_code == 200
            assert created.json()["status"] == "executed"
            assert created.json()["tool_call"] == {"connector_slug": "sqlite", "tool": "write_create_table"}
            described = await ac.post(
                "/mcp/sqlite/tools/read_get_schema",
                json={"arguments": {"table": "chat_test_summary"}},
                headers=chat_headers,
            )
            assert described.status_code == 200
            assert {column["name"] for column in described.json()["columns"]} == {"month", "total"}

            chart = await ac.post("/ide/chat", json={"question": "show me revenue by month as a chart"})
            assert chart.status_code == 200
            assert chart.json()["chart_spec"]["$schema"].endswith("/vega-lite/v5.json")
            assert len(chart.json()["chart_spec"]["data"]["values"]) <= 12

            glossary = await ac.post(
                "/ide/chat",
                json={"question": "what does the data glossary say about LTV?"},
            )
            assert glossary.status_code == 200
            assert glossary.json()["llm_status"] == "chroma_grounded"
            assert "customer revenue" in glossary.json()["answer"].lower()
            assert any("glossary" in citation["title"].lower() for citation in glossary.json()["citations"])

            triggered = await ac.post(
                "/ide/chat",
                json={"question": "trigger the daily_orders_refresh DAG"},
            )
            assert triggered.status_code == 200
            assert triggered.json()["status"] == "triggered"
            assert triggered.json()["tool_call"] == {"connector_slug": "airflow", "tool": "write_trigger_dag"}
            assert triggered.json()["tool_result"]["dag_run"]["dag_id"] == "daily_orders_refresh"
            run_id = triggered.json()["tool_result"]["dag_run"]["dag_run_id"]
            last_run = await ac.post(
                "/ide/chat",
                json={"question": "what was the last run for daily_orders_refresh?"},
            )
            assert last_run.status_code == 200
            assert last_run.json()["tool_call"] == {"connector_slug": "airflow", "tool": "read_get_run"}
            assert last_run.json()["tool_result"]["run"]["dag_runs"][0]["dag_run_id"] == run_id

            created_dag = await ac.post(
                "/ide/chat",
                json={"question": "build me an Airflow DAG that materializes weekly_revenue every Monday"},
            )
            assert created_dag.status_code == 200
            assert created_dag.json()["status"] == "created"
            assert created_dag.json()["tool_call"] == {"connector_slug": "airflow", "tool": "write_create_dag"}
            assert created_dag.json()["tool_result"]["dag"]["dag_id"] == "weekly_revenue"
            dag_source = await ac.post(
                "/mcp/airflow/tools/read_get_dag_source",
                json={"arguments": {"dag_id": "weekly_revenue"}},
                headers=chat_headers,
            )
            assert dag_source.status_code == 200
            assert "weekly_revenue" in dag_source.json()["source"]["source"]

            triggered_dbt = await ac.post(
                "/ide/chat",
                json={"question": "trigger the dbt revenue job"},
            )
            assert triggered_dbt.status_code == 200
            assert triggered_dbt.json()["status"] == "triggered"
            assert triggered_dbt.json()["tool_call"] == {"connector_slug": "dbt", "tool": "write_trigger_run"}
            assert triggered_dbt.json()["tool_result"]["run"]["job_id"] == 100

            documented = await ac.post(
                "/ide/chat",
                json={"question": "document the orders table in Notion"},
            )
            assert documented.status_code == 200
            assert documented.json()["status"] == "created"
            assert documented.json()["tool_call"] == {"connector_slug": "notion", "tool": "write_create_page"}
            async with AsyncClient(base_url="http://localhost:18084") as fixture_client:
                notion_pages = (await fixture_client.get("/v1/pages/created")).json()["results"]
            assert any(page["title"] == "Orders table documentation" for page in notion_pages)

            committed = await ac.post(
                "/ide/chat",
                json={"question": "commit a README to the analytics repo describing daily_orders"},
            )
            assert committed.status_code == 200
            assert committed.json()["status"] == "committed"
            assert committed.json()["tool_call"] == {"connector_slug": "github", "tool": "write_commit_file"}
            async with AsyncClient(base_url="http://localhost:18084") as fixture_client:
                readme = (await fixture_client.get("/repos/dataclaw/analytics/contents/README.md")).json()
            assert readme["payload"]["message"] == "Document daily_orders"

            destructive = await ac.post("/ide/chat", json={"question": "drop the test_summary table"})
            assert destructive.status_code == 200
            assert destructive.json()["status"] == "pending_approval"
            events = await ac.get("/observability/events?state=needs_approval")
            assert events.status_code == 200
            assert events.json()["needs_approval"] >= 1

            still_exists = await ac.post(
                "/mcp/sqlite/tools/read_get_schema",
                json={"arguments": {"table": "test_summary"}},
                headers=chat_headers,
            )
            assert still_exists.status_code == 200
            approved = await ac.post(f"/alerts/{destructive.json()['alert_id']}/approve-and-execute")
            assert approved.status_code == 200
            assert approved.json()["status"] == "executed"
            gone = await ac.post(
                "/mcp/sqlite/tools/read_get_schema",
                json={"arguments": {"table": "test_summary"}},
                headers=chat_headers,
            )
            assert gone.status_code != 200
            audit = await ac.get(f"/agents/{chat_agent['id']}/audit")
            assert any(row["required_approval"] and row["statement_type"] == "DROP_TABLE" for row in audit.json())
            logs = await ac.get("/observability/logs?q=approved_write_sql_executed")
            assert logs.status_code == 200
            assert any(
                entry["context"].get("executor") == "admin@dataclaw.local"
                for entry in logs.json()["entries"]
            )

            analyst = await ac.post("/agents", json={"name": "analyst", "system_prompt": "Read-only."})
            assert analyst.status_code == 200
            kb_writer = await ac.post("/agents", json={"name": "kb_writer", "system_prompt": "KB writer."})
            assert kb_writer.status_code == 200
            google_doc_grant = [{"connector_slug": "google_docs", "read_enabled": True, "write_enabled": True}]
            assert (await ac.put(f"/agents/{kb_writer.json()['id']}/grants", json={"grants": google_doc_grant})).status_code == 200
            stubbed_doc = await ac.post(
                "/mcp/google_docs/tools/write_create_doc",
                json={"arguments": {"title": "Orders glossary", "body": "Orders glossary body."}},
                headers={"X-DataClaw-Agent-Id": kb_writer.json()["id"]},
            )
            assert stubbed_doc.status_code == 200
            assert stubbed_doc.json()["status"] == "stubbed"

            denied = await ac.post(
                "/mcp/sqlite/tools/write_create_table",
                json={"arguments": {"table": "blocked_write", "columns": [{"name": "id", "type": "integer"}]}},
                headers={"X-DataClaw-Agent-Id": analyst.json()["id"]},
            )
            assert denied.status_code == 403
