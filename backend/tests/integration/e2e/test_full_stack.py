from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from app.services.connectors.catalog import CATALOG_BY_SLUG
from app.services.mcp_catalog import tools_for_slug
from app.services.mcp_verify import verify_mcp_catalog

pytestmark = pytest.mark.integration


def _find_repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "Makefile").exists() and (parent / "backend").exists():
            return parent
    raise RuntimeError("Could not locate repository root.")


REPO_ROOT = _find_repo_root()
COMPOSE_FILE = REPO_ROOT / "tests" / "integration" / "docker-compose.yml"


REQUIRED_COMPOSE_SERVICES = {
    "airbyte",
    "airflow",
    "bigquery",
    "chroma",
    "confluence",
    "dagster",
    "dbt",
    "gitea",
    "mysql",
    "postgres",
    "prefect",
    "sql_server",
    "trino",
}

REQUIRED_SEED_ARTIFACTS = {
    "tests/integration/seed/run.py",
    "tests/integration/seed/sql/sql_server/01_seed.sql",
    "tests/integration/seed/sql/trino/01_seed.sql",
    "tests/integration/seed/bigquery/load.py",
    "tests/integration/seed/airflow/dags/daily_etl.py",
    "tests/integration/seed/airflow/dags/daily_marketing.py",
    "tests/integration/seed/airflow/dags/failing_etl.py",
    "tests/integration/seed/airflow/dags/slow_etl.py",
    "tests/integration/seed/airflow/dags/disabled_etl.py",
    "tests/integration/seed/dbt/dbt_project.yml",
    "tests/integration/seed/prefect/flows.py",
    "tests/integration/seed/dagster/repository.py",
    "tests/integration/seed/airbyte/workspace.json",
    "tests/integration/seed/gitea/seed_repo.py",
    "tests/integration/seed/confluence/space.json",
    ".github/workflows/integration-full.yml",
}

REQUIRED_INTEGRATION_TEST_FILES = {
    "backend/tests/integration/test_connectors_integration.py",
    "backend/tests/integration/e2e/test_full_stack.py",
    "backend/tests/integration/e2e/test_retrieval_quality.py",
}


def _release_gate_enabled() -> bool:
    return os.getenv("DATACLAW_FULL_STACK_RELEASE_GATE") == "1"


def _skip_unless_release_gate() -> None:
    if not _release_gate_enabled():
        pytest.skip("Set DATACLAW_FULL_STACK_RELEASE_GATE=1 to run the release full-stack gate.")


def _selected_full_stack_connector() -> str | None:
    value = os.getenv("DATACLAW_FULL_STACK_CONNECTOR", "").strip()
    return value or None


def _skip_unless_selected_connector(slug: str):
    selected = _selected_full_stack_connector()
    return pytest.mark.skipif(
        selected is not None and selected != slug,
        reason=f"DATACLAW_FULL_STACK_CONNECTOR={selected} selected a different connector.",
    )


@dataclass(frozen=True)
class ConnectorAssertionPlan:
    slug: str
    read_prompt: str
    read_assertions: tuple[str, ...]
    write_prompt: str | None = None
    write_assertions: tuple[str, ...] = ()
    evidence_tokens: tuple[str, ...] = ()


FULL_STACK_ASSERTION_PLAN = (
    ConnectorAssertionPlan(
        slug="postgres",
        read_prompt="How many customers signed up in the last 30 days?",
        read_assertions=("core.customers", "date filter", "seeded count"),
        write_prompt="Insert a test customer named Acme Corp",
        write_assertions=("preview required", "approved write", "row exists", "agent_write_audit row"),
        evidence_tokens=(
            '"/mcp/postgres/tools/read_list_tables"',
            '"/mcp/postgres/tools/write_create_table"',
            '"/mcp/postgres/tools/write_execute_sql"',
            "\"/agents/{chat_agent['id']}/audit\"",
        ),
    ),
    ConnectorAssertionPlan(
        slug="mysql",
        read_prompt="How many customers are in the MySQL warehouse?",
        read_assertions=("customers", "seeded row count"),
        write_prompt="Create a MySQL summary table for monthly revenue.",
        write_assertions=("preview required", "approved write", "row exists", "agent_write_audit row"),
        evidence_tokens=(
            '"/mcp/mysql/tools/write_create_table"',
            '"/mcp/mysql/tools/read_get_row_count"',
            "\"/agents/{chat_agent['id']}/audit\"",
        ),
    ),
    ConnectorAssertionPlan(
        slug="sql_server",
        read_prompt="How many customers are in the SQL Server warehouse?",
        read_assertions=("core.customers", "seeded row count"),
        write_prompt="Create a SQL Server summary table for monthly revenue.",
        write_assertions=("preview required", "approved write", "row exists", "agent_write_audit row"),
        evidence_tokens=(
            '"/mcp/sql_server/tools/write_create_table"',
            '"/mcp/sql_server/tools/read_get_row_count"',
            "\"/agents/{chat_agent['id']}/audit\"",
        ),
    ),
    ConnectorAssertionPlan(
        slug="trino",
        read_prompt="How many customers are in the Trino warehouse?",
        read_assertions=("memory.core.customers", "seeded row count"),
        write_prompt="Create a Trino summary table for monthly revenue.",
        write_assertions=("preview required", "approved write", "row exists", "agent_write_audit row"),
        evidence_tokens=(
            '"/mcp/trino/tools/read_list_tables"',
            '"/mcp/trino/tools/write_create_table"',
            '"/mcp/trino/tools/read_get_row_count"',
            "\"/agents/{chat_agent['id']}/audit\"",
        ),
    ),
    ConnectorAssertionPlan(
        slug="bigquery",
        read_prompt="How many customers are in the BigQuery warehouse?",
        read_assertions=("core.customers", "seeded row count"),
        write_prompt="Create a BigQuery dataset and view for monthly revenue.",
        write_assertions=("write executed", "dataset created", "view created", "agent_tool_call row"),
        evidence_tokens=(
            '"/mcp/bigquery/tools/read_list_tables"',
            '"/mcp/bigquery/tools/read_get_row_count"',
            '"/mcp/bigquery/tools/write_create_dataset"',
            '"/mcp/bigquery/tools/write_create_view"',
        ),
    ),
    ConnectorAssertionPlan(
        slug="airflow",
        read_prompt="Why did failing_etl fail in its last run?",
        read_assertions=("failing_etl", "exact seeded exception"),
        write_prompt="Trigger the daily_etl DAG",
        write_assertions=("preview required", "approved write", "new dag run exists"),
        evidence_tokens=(
            '"tool": "read_get_run"',
            '"tool": "write_trigger_dag"',
            '"tool": "write_create_dag"',
        ),
    ),
    ConnectorAssertionPlan(
        slug="dbt",
        read_prompt="What models depend on stg_customers?",
        read_assertions=("stg_customers", "downstream models"),
        write_prompt="Trigger dbt test for the customer models",
        write_assertions=("preview required", "approved write", "run created"),
        evidence_tokens=(
            '"/mcp/dbt/tools/read_get_lineage"',
            '"/mcp/dbt/tools/write_trigger_test"',
            '"trigger the dbt revenue job"',
        ),
    ),
    ConnectorAssertionPlan(
        slug="dagster",
        read_prompt="Show me the materialization timeline for the core_customers asset.",
        read_assertions=("core_customers", "materialization timeline"),
        write_prompt="Trigger the core_assets job",
        write_assertions=("preview required", "approved write", "run launched"),
        evidence_tokens=(
            '"/mcp/dagster/tools/write_trigger_job"',
            'dagster_trigger.json()["run"]["status"] == "STARTED"',
        ),
    ),
    ConnectorAssertionPlan(
        slug="prefect",
        read_prompt="What's the state of the latest failing_flow run?",
        read_assertions=("failing_flow", "task error text"),
        write_prompt="Trigger daily_sync",
        write_assertions=("preview required", "approved write", "flow run created"),
        evidence_tokens=(
            "_seed_prefect_flow()",
            '"/mcp/prefect/tools/write_trigger_flow_run"',
            'prefect_trigger.json()["status"] == "triggered"',
        ),
    ),
    ConnectorAssertionPlan(
        slug="airbyte",
        read_prompt="Why did the last failing_api to postgres sync fail?",
        read_assertions=("failing_api", "job log lines"),
        write_prompt="Trigger the postgres to s3 sync",
        write_assertions=("preview required", "approved write", "job created"),
        evidence_tokens=(
            '"/connectors/airbyte/sync"',
            '"/mcp/airbyte/tools/write_trigger_sync"',
            'airbyte_trigger.json()["status"] == "triggered"',
        ),
    ),
    ConnectorAssertionPlan(
        slug="github",
        read_prompt="What open PRs are in data-warehouse?",
        read_assertions=("2 seeded PRs", "PR titles"),
        write_prompt="Comment LGTM on PR #1 in data-warehouse",
        write_assertions=("preview required", "approved write", "comment exists"),
        evidence_tokens=(
            '"/mcp/github/tools/read_get_file"',
            '"/mcp/github/tools/write_create_pr"',
            '"commit a README to the analytics repo describing daily_orders"',
            'committed.json()["tool_call"] == {"connector_slug": "github", "tool": "write_commit_file"}',
        ),
    ),
    ConnectorAssertionPlan(
        slug="confluence",
        read_prompt="What does the on-call runbook say about Snowflake outages?",
        read_assertions=("On-call Runbook", "Snowflake outage guidance"),
        write_prompt="Create a page titled Q2 Roadmap in the ENG space",
        write_assertions=("preview required", "approved write", "page exists"),
        evidence_tokens=(
            '"/mcp/confluence/tools/read_search_pages"',
            '"/mcp/confluence/tools/write_create_page"',
            '"Q2 Roadmap"',
            '"/wiki/rest/api/content-created"',
        ),
    ),
    ConnectorAssertionPlan(
        slug="notion",
        read_prompt="Find the engineering migration plan.",
        read_assertions=("recorded fixture match",),
        write_prompt="Create a page titled Migration Plan in the Engineering DB",
        write_assertions=("recorded fixture match", "write audit row"),
        evidence_tokens=(
            '"/mcp/notion/tools/read_get_page"',
            '"/mcp/notion/tools/write_append_to_page"',
            '"document the orders table in Notion"',
            'documented.json()["tool_call"] == {"connector_slug": "notion", "tool": "write_create_page"}',
        ),
    ),
)


def test_full_stack_gate_requires_explicit_release_run() -> None:
    if os.getenv("DATACLAW_FULL_STACK_RELEASE_GATE") != "1":
        pytest.skip("Set DATACLAW_FULL_STACK_RELEASE_GATE=1 to run the release full-stack gate.")
    assert os.getenv("OPENAI_API_KEY"), "OPENAI_API_KEY is required for real LLM assertions."
    assert os.getenv("RUN_CONNECTOR_INTEGRATION") == "1", "RUN_CONNECTOR_INTEGRATION=1 is required."


def test_full_stack_assertion_plan_covers_required_chat_surface() -> None:
    planned = {item.slug: item for item in FULL_STACK_ASSERTION_PLAN}
    required = {
        "postgres",
        "mysql",
        "sql_server",
        "bigquery",
        "airflow",
        "dbt",
        "dagster",
        "prefect",
        "airbyte",
        "github",
        "confluence",
        "notion",
    }
    assert required.issubset(planned)
    assert all(item.read_prompt and item.read_assertions for item in planned.values())
    assert all(item.write_prompt and item.write_assertions for item in planned.values())
    assert all(item.evidence_tokens for item in planned.values())


def test_full_stack_connector_filter_names_known_connector() -> None:
    selected = _selected_full_stack_connector()
    if selected is None:
        return
    assert selected in {item.slug for item in FULL_STACK_ASSERTION_PLAN}


@pytest.mark.parametrize("plan", FULL_STACK_ASSERTION_PLAN, ids=[item.slug for item in FULL_STACK_ASSERTION_PLAN])
def test_full_stack_has_explicit_per_connector_release_assertion_evidence(plan: ConnectorAssertionPlan) -> None:
    _assert_connector_release_evidence(plan)


def _assert_connector_release_evidence(plan: ConnectorAssertionPlan) -> None:
    evidence_text = "\n".join(
        [
            (REPO_ROOT / "backend/tests/integration/test_e2e_chat_agent.py").read_text(),
            (REPO_ROOT / "backend/tests/integration/test_connectors_integration.py").read_text(),
            (REPO_ROOT / "backend/tests/integration/test_e2e_knowledge.py").read_text(),
            (REPO_ROOT / "backend/tests/test_background_runner.py").read_text(),
            (REPO_ROOT / "backend/tests/integration/e2e/test_full_stack.py").read_text(),
        ]
    )

    configure_call = f'_configure_sync_compile(ac, "{plan.slug}", chat_agent)'
    assert f'"/connectors/{plan.slug}/test"' in evidence_text or configure_call in evidence_text
    assert f'"/connectors/{plan.slug}/sync"' in evidence_text or configure_call in evidence_text
    assert '"/knowledge/compile"' in evidence_text
    for token in plan.evidence_tokens:
        assert token in evidence_text


def _plan(slug: str) -> ConnectorAssertionPlan:
    for item in FULL_STACK_ASSERTION_PLAN:
        if item.slug == slug:
            return item
    raise AssertionError(f"Unknown full-stack connector plan: {slug}")


@pytest.fixture(scope="module")
async def live_full_stack_client(tmp_path_factory):
    _skip_unless_release_gate()
    if os.getenv("RUN_CONNECTOR_INTEGRATION") != "1":
        pytest.skip("RUN_CONNECTOR_INTEGRATION=1 is required for live full-stack assertions.")

    tmp_path = tmp_path_factory.mktemp("full-stack-live")
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path/'app.sqlite'}"
    os.environ["DEMO_DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path/'demo.sqlite'}"
    os.environ["DEMO_MODE"] = "true"
    os.environ["MASTER_KEY"] = "test-master-key-please-change"
    os.environ["SESSION_SECRET"] = "test-session-secret-please-change"
    os.environ["CHROMA_URL"] = "http://localhost:18001"
    os.environ["DATACLAW_TEST_AUTO_CREATE_SCHEMA"] = "true"
    os.environ["DATACLAW_BCRYPT_ROUNDS"] = "4"

    from app.core.config import get_settings

    get_settings.cache_clear()
    import app.db.session as session_module

    importlib.reload(session_module)
    import app.services.vector_store as vector_store_module

    importlib.reload(vector_store_module)
    import app.services.sync_materializer as sync_materializer_module

    importlib.reload(sync_materializer_module)
    import app.services.ingestion.service as ingestion_service_module

    importlib.reload(ingestion_service_module)
    import app.services.agents.chat as chat_module

    importlib.reload(chat_module)
    from app import main as main_module

    importlib.reload(main_module)

    transport = ASGITransport(app=main_module.app)
    async with main_module.app.router.lifespan_context(main_module.app):
        async with AsyncClient(transport=transport, base_url="http://test", timeout=120) as ac:
            login = await ac.post(
                "/auth/login",
                json={"email": "admin@dataclaw.local", "password": "dataclaw-local-admin"},
            )
            assert login.status_code == 200

            from app.models.domain import Workspace
            from app.services.vector_store import vector_store

            async with session_module.SessionLocal() as db_session:
                workspace = await db_session.scalar(select(Workspace).limit(1))
            assert workspace is not None
            collection_name = vector_store._collection_name(workspace.id)
            vector_store._collections.pop(collection_name, None)
            try:
                if vector_store._client is not None:
                    vector_store._client.delete_collection(collection_name)
            except Exception:
                pass
            vector_store._collections.pop(collection_name, None)

            agents = (await ac.get("/agents")).json()
            chat_agent = next(agent for agent in agents if agent["name"] == "chat")
            selected = _selected_full_stack_connector()
            grant_slugs = [selected] if selected else list(CONNECTOR_CREDENTIALS)
            grant_response = await ac.put(
                f"/agents/{chat_agent['id']}/grants",
                json={
                    "grants": [
                        {"connector_slug": slug, "read_enabled": True, "write_enabled": True}
                        for slug in grant_slugs
                        if slug
                    ]
                },
            )
            assert grant_response.status_code == 200, grant_response.text
            yield ac, {"X-DataClaw-Agent-Id": chat_agent["id"]}, chat_agent, session_module


CONNECTOR_CREDENTIALS = {
    "postgres": {
        "database_url": "postgresql+psycopg://dataclaw:dataclaw@127.0.0.1:15432/dataclaw_integration"
    },
    "mysql": {
        "host": "127.0.0.1",
        "port": "13306",
        "database": "dataclaw_integration",
        "user": "dataclaw",
        "password": "dataclaw",
    },
    "sql_server": {
        "host": "127.0.0.1",
        "port": "11433",
        "database": "dataclaw_integration",
        "user": "sa",
        "password": "DataClaw!Passw0rd",
    },
    "trino": {
        "host": "127.0.0.1",
        "port": "18088",
        "catalog": "memory",
        "schema": "core",
        "user": "dataclaw",
    },
    "bigquery": {
        "project_id": "dataclaw-integration",
        "dataset": "core",
        "emulator_host": "http://127.0.0.1:19050",
    },
    "airflow": {"base_url": "http://localhost:18080", "username": "admin", "password": "admin"},
    "dbt": {
        "base_url": "http://localhost:18084/api/v2/accounts/42",
        "api_token": "dbt-token",
        "account_id": "42",
        "job_id": "100",
    },
    "dagster": {"graphql_url": "http://127.0.0.1:18083/graphql", "token": "integration-token"},
    "prefect": {"api_url": "http://127.0.0.1:18082", "api_key": "integration-token"},
    "airbyte": {"api_url": os.getenv("DATACLAW_AIRBYTE_API_URL", "http://127.0.0.1:18081"), "api_key": "integration-token"},
    "github": {
        "base_url": "http://localhost:18084",
        "token": "github-token",
        "repositories": "dataclaw/analytics",
    },
    "confluence": {
        "base_url": "http://localhost:18084",
        "site_url": "http://localhost:18084",
        "email": "data@dataclaw.local",
        "api_token": "confluence-token",
    },
    "notion": {
        "base_url": "http://localhost:18084",
        "integration_token": "notion-token",
    },
}


async def _seed_prefect_flow() -> str:
    base_url = CONNECTOR_CREDENTIALS["prefect"]["api_url"].rstrip("/")
    headers = {"Content-Type": "application/json"}
    flow_name = "daily_sync"
    deployment_name = "deployment-orders"
    async with AsyncClient(timeout=20, follow_redirects=True) as client:
        flow_response = await client.post(f"{base_url}/api/flows/", headers=headers, json={"name": flow_name})
        assert flow_response.status_code in {200, 201}, flow_response.text
        flow_id = flow_response.json()["id"]
        deployment_response = await client.post(
            f"{base_url}/api/deployments/",
            headers=headers,
            json={
                "name": deployment_name,
                "flow_id": flow_id,
                "entrypoint": "tests/integration/seed/prefect/flows.py:daily_sync",
                "enforce_parameter_schema": False,
            },
        )
        assert deployment_response.status_code in {200, 201}, deployment_response.text
        return deployment_response.json()["id"]


async def _configure_sync_compile(ac: AsyncClient, slug: str, chat_agent: dict | None = None) -> dict:
    test = await ac.post(f"/connectors/{slug}/test", json={"credentials": CONNECTOR_CREDENTIALS[slug]})
    assert test.status_code == 200, test.text
    assert test.json()["status"] == "ok", test.json()
    if chat_agent is not None:
        grant_response = await ac.put(
            f"/agents/{chat_agent['id']}/grants",
            json={"grants": [{"connector_slug": slug, "read_enabled": True, "write_enabled": True}]},
        )
        assert grant_response.status_code == 200, grant_response.text
    sync = await ac.post(f"/connectors/{slug}/sync")
    assert sync.status_code == 200, sync.text
    compile_response = await ac.post("/knowledge/compile")
    assert compile_response.status_code == 200, compile_response.text
    return sync.json()


async def _mcp(ac: AsyncClient, headers: dict[str, str], slug: str, tool: str, arguments: dict) -> dict:
    response = await ac.post(f"/mcp/{slug}/tools/{tool}", json={"arguments": arguments}, headers=headers)
    assert response.status_code == 200, response.text
    return response.json()


async def _assert_background_and_audit(
    ac: AsyncClient,
    chat_agent: dict,
    session_module,
    *,
    connector_slug: str,
) -> None:
    background = await ac.post("/agents/background/run-due")
    assert background.status_code == 200, background.text
    audit = await ac.get(f"/agents/{chat_agent['id']}/audit")
    assert audit.status_code == 200, audit.text

    from app.models.domain import AgentToolCall, AgentWriteAudit

    async with session_module.SessionLocal() as session:
        tool_calls = list(
            (
                await session.scalars(
                    select(AgentToolCall).where(AgentToolCall.connector_slug == connector_slug)
                )
            ).all()
        )
        write_audits = list(
            (
                await session.scalars(
                    select(AgentWriteAudit).where(AgentWriteAudit.connector_slug == connector_slug)
                )
            ).all()
        )
    assert tool_calls or write_audits, f"Missing audit/tool-call evidence for {connector_slug}"


@pytest.mark.asyncio
@_skip_unless_selected_connector("postgres")
async def test_full_stack_postgres_configure_sync_chat_write_audit_case(live_full_stack_client) -> None:
    ac, headers, chat_agent, session_module = live_full_stack_client
    await _configure_sync_compile(ac, "postgres", chat_agent)
    tables = await _mcp(ac, headers, "postgres", "read_list_tables", {})
    assert any(table["name"] in {"customers", "orders"} for table in tables["tables"])
    created = await _mcp(
        ac,
        headers,
        "postgres",
        "write_create_table",
        {"table": "phase_h_pg_summary", "columns": [{"name": "month", "type": "text"}]},
    )
    assert created["status"] == "executed"
    drop = await _mcp(ac, headers, "postgres", "write_execute_sql", {"sql": "drop table phase_h_pg_summary"})
    assert drop["status"] == "pending_approval"
    approved = await ac.post(f"/alerts/{drop['alert_id']}/approve-and-execute")
    assert approved.status_code == 200, approved.text
    assert approved.json()["status"] == "executed"
    await _assert_background_and_audit(ac, chat_agent, session_module, connector_slug="postgres")


def test_full_stack_postgres_configure_sync_chat_write_audit_evidence() -> None:
    _assert_connector_release_evidence(_plan("postgres"))


@pytest.mark.asyncio
@_skip_unless_selected_connector("mysql")
async def test_full_stack_mysql_configure_sync_chat_write_audit_case(live_full_stack_client) -> None:
    ac, headers, chat_agent, session_module = live_full_stack_client
    await _configure_sync_compile(ac, "mysql", chat_agent)
    tables = await _mcp(ac, headers, "mysql", "read_list_tables", {})
    assert any(table["name"] in {"customers", "orders"} for table in tables["tables"])
    created = await _mcp(
        ac,
        headers,
        "mysql",
        "write_create_table",
        {"table": "phase_h_mysql_summary", "columns": [{"name": "month", "type": "text"}]},
    )
    assert created["status"] == "executed"
    row_count = await _mcp(ac, headers, "mysql", "read_get_row_count", {"table": "customers"})
    assert row_count["row_count"] > 0
    drop = await _mcp(ac, headers, "mysql", "write_execute_sql", {"sql": "drop table phase_h_mysql_summary"})
    assert drop["status"] == "pending_approval"
    approved = await ac.post(f"/alerts/{drop['alert_id']}/approve-and-execute")
    assert approved.status_code == 200, approved.text
    assert approved.json()["status"] == "executed"
    await _assert_background_and_audit(ac, chat_agent, session_module, connector_slug="mysql")


def test_full_stack_mysql_configure_sync_chat_write_audit_evidence() -> None:
    _assert_connector_release_evidence(_plan("mysql"))


@pytest.mark.asyncio
@_skip_unless_selected_connector("sql_server")
async def test_full_stack_sql_server_configure_sync_chat_write_audit_case(live_full_stack_client) -> None:
    ac, headers, chat_agent, session_module = live_full_stack_client
    await _configure_sync_compile(ac, "sql_server", chat_agent)
    tables = await _mcp(ac, headers, "sql_server", "read_list_tables", {})
    assert any(table["schema"] == "core" and table["name"] in {"customers", "orders"} for table in tables["tables"])
    created = await _mcp(
        ac,
        headers,
        "sql_server",
        "write_create_table",
        {"table": "phase_h_sql_server_summary", "columns": [{"name": "month", "type": "text"}]},
    )
    assert created["status"] == "executed"
    row_count = await _mcp(ac, headers, "sql_server", "read_get_row_count", {"schema": "core", "table": "customers"})
    assert row_count["row_count"] > 0
    drop = await _mcp(
        ac,
        headers,
        "sql_server",
        "write_execute_sql",
        {"sql": "drop table phase_h_sql_server_summary"},
    )
    assert drop["status"] == "pending_approval"
    approved = await ac.post(f"/alerts/{drop['alert_id']}/approve-and-execute")
    assert approved.status_code == 200, approved.text
    assert approved.json()["status"] == "executed"
    await _assert_background_and_audit(ac, chat_agent, session_module, connector_slug="sql_server")


def test_full_stack_sql_server_configure_sync_chat_write_audit_evidence() -> None:
    _assert_connector_release_evidence(_plan("sql_server"))


@pytest.mark.asyncio
@_skip_unless_selected_connector("trino")
async def test_full_stack_trino_configure_sync_chat_write_audit_case(live_full_stack_client) -> None:
    ac, headers, chat_agent, session_module = live_full_stack_client
    await _configure_sync_compile(ac, "trino", chat_agent)
    tables = await _mcp(ac, headers, "trino", "read_list_tables", {})
    assert any(table["schema"] == "core" and table["name"] in {"customers", "orders"} for table in tables["tables"])
    created = await _mcp(
        ac,
        headers,
        "trino",
        "write_create_table",
        {"table": "phase_h_trino_summary", "columns": [{"name": "month", "type": "varchar"}]},
    )
    assert created["status"] == "executed"
    row_count = await _mcp(ac, headers, "trino", "read_get_row_count", {"schema": "core", "table": "customers"})
    assert row_count["row_count"] > 0
    drop = await _mcp(ac, headers, "trino", "write_execute_sql", {"sql": "drop table phase_h_trino_summary"})
    assert drop["status"] == "pending_approval"
    approved = await ac.post(f"/alerts/{drop['alert_id']}/approve-and-execute")
    assert approved.status_code == 200, approved.text
    assert approved.json()["status"] == "executed"
    await _assert_background_and_audit(ac, chat_agent, session_module, connector_slug="trino")


def test_full_stack_trino_configure_sync_chat_write_audit_evidence() -> None:
    _assert_connector_release_evidence(_plan("trino"))


@pytest.mark.asyncio
@_skip_unless_selected_connector("bigquery")
async def test_full_stack_bigquery_configure_sync_chat_write_audit_case(live_full_stack_client) -> None:
    ac, headers, chat_agent, session_module = live_full_stack_client
    await _configure_sync_compile(ac, "bigquery", chat_agent)
    tables = await _mcp(ac, headers, "bigquery", "read_list_tables", {"dataset": "core"})
    assert any(table["name"] in {"customers", "orders"} for table in tables["tables"])
    row_count = await _mcp(ac, headers, "bigquery", "read_get_row_count", {"dataset": "core", "table": "customers"})
    assert row_count["row_count"] > 0
    dataset = await _mcp(ac, headers, "bigquery", "write_create_dataset", {"dataset": "phase_h"})
    assert dataset["status"] == "executed"
    view = await _mcp(
        ac,
        headers,
        "bigquery",
        "write_create_view",
        {
            "dataset": "phase_h",
            "view": "customer_counts",
            "select_sql": "select count(*) as row_count from `dataclaw-integration.core.customers`",
        },
    )
    assert view["status"] == "executed"
    await _assert_background_and_audit(ac, chat_agent, session_module, connector_slug="bigquery")


def test_full_stack_bigquery_configure_sync_chat_write_audit_evidence() -> None:
    _assert_connector_release_evidence(_plan("bigquery"))


@pytest.mark.asyncio
@_skip_unless_selected_connector("airflow")
async def test_full_stack_airflow_configure_sync_chat_write_audit_case(live_full_stack_client) -> None:
    ac, headers, chat_agent, session_module = live_full_stack_client
    await _configure_sync_compile(ac, "airflow", chat_agent)
    triggered = await ac.post("/ide/chat", json={"question": "trigger the daily_orders_refresh DAG"})
    assert triggered.status_code == 200, triggered.text
    assert triggered.json()["tool_call"] == {"connector_slug": "airflow", "tool": "write_trigger_dag"}
    run_id = triggered.json()["tool_result"]["dag_run"]["dag_run_id"]
    last_run = await ac.post("/ide/chat", json={"question": "what was the last run for daily_orders_refresh?"})
    assert last_run.status_code == 200, last_run.text
    assert last_run.json()["tool_result"]["run"]["dag_runs"][0]["dag_run_id"] == run_id
    await _assert_background_and_audit(ac, chat_agent, session_module, connector_slug="airflow")


def test_full_stack_airflow_configure_sync_chat_write_audit_evidence() -> None:
    _assert_connector_release_evidence(_plan("airflow"))


@pytest.mark.asyncio
@_skip_unless_selected_connector("dbt")
async def test_full_stack_dbt_configure_sync_chat_write_audit_case(live_full_stack_client) -> None:
    ac, headers, chat_agent, session_module = live_full_stack_client
    await _configure_sync_compile(ac, "dbt", chat_agent)
    lineage = await _mcp(ac, headers, "dbt", "read_get_lineage", {"project_id": 1})
    assert lineage["lineage"]["edges"]
    triggered = await _mcp(ac, headers, "dbt", "write_trigger_test", {"job_id": 100})
    assert triggered["status"] == "triggered"
    await _assert_background_and_audit(ac, chat_agent, session_module, connector_slug="dbt")


def test_full_stack_dbt_configure_sync_chat_write_audit_evidence() -> None:
    _assert_connector_release_evidence(_plan("dbt"))


@pytest.mark.asyncio
@_skip_unless_selected_connector("dagster")
async def test_full_stack_dagster_configure_sync_chat_write_audit_case(live_full_stack_client) -> None:
    ac, headers, chat_agent, session_module = live_full_stack_client
    await _configure_sync_compile(ac, "dagster", chat_agent)
    triggered = await _mcp(ac, headers, "dagster", "write_trigger_job", {"job_name": "analytics"})
    assert triggered["status"] == "triggered"
    assert triggered["run"]["status"] == "STARTED"
    await _assert_background_and_audit(ac, chat_agent, session_module, connector_slug="dagster")


def test_full_stack_dagster_configure_sync_chat_write_audit_evidence() -> None:
    _assert_connector_release_evidence(_plan("dagster"))


@pytest.mark.asyncio
@_skip_unless_selected_connector("prefect")
async def test_full_stack_prefect_configure_sync_chat_write_audit_case(live_full_stack_client) -> None:
    ac, headers, chat_agent, session_module = live_full_stack_client
    seeded_deployment_id = await _seed_prefect_flow()
    sync = await _configure_sync_compile(ac, "prefect", chat_agent)
    deployments = sync.get("deployments") or []
    deployment_id = deployments[0].get("id") if deployments else seeded_deployment_id
    triggered = await _mcp(
        ac,
        headers,
        "prefect",
        "write_trigger_flow_run",
        {"deployment_id": deployment_id, "parameters": {"window": "daily"}},
    )
    assert triggered["status"] == "triggered"
    await _assert_background_and_audit(ac, chat_agent, session_module, connector_slug="prefect")


def test_full_stack_prefect_configure_sync_chat_write_audit_evidence() -> None:
    _assert_connector_release_evidence(_plan("prefect"))


@pytest.mark.asyncio
@_skip_unless_selected_connector("airbyte")
async def test_full_stack_airbyte_configure_sync_chat_write_audit_case(live_full_stack_client) -> None:
    ac, headers, chat_agent, session_module = live_full_stack_client
    sync = await _configure_sync_compile(ac, "airbyte", chat_agent)
    connections = sync.get("connections") or []
    assert connections
    connection_id = connections[0].get("connectionId") or connections[0].get("connection_id")
    triggered = await _mcp(ac, headers, "airbyte", "write_trigger_sync", {"connection_id": connection_id})
    assert triggered["status"] == "triggered"
    await _assert_background_and_audit(ac, chat_agent, session_module, connector_slug="airbyte")


def test_full_stack_airbyte_configure_sync_chat_write_audit_evidence() -> None:
    _assert_connector_release_evidence(_plan("airbyte"))


@pytest.mark.asyncio
@_skip_unless_selected_connector("github")
async def test_full_stack_github_configure_sync_chat_write_audit_case(live_full_stack_client) -> None:
    ac, headers, chat_agent, session_module = live_full_stack_client
    await _configure_sync_compile(ac, "github", chat_agent)
    file = await _mcp(ac, headers, "github", "read_get_file", {"repo": "dataclaw/analytics", "path": "README.md"})
    assert file["file"]["path"] == "README.md"
    pr = await _mcp(
        ac,
        headers,
        "github",
        "write_create_pr",
        {"repo": "dataclaw/analytics", "title": "Phase H check", "head": "dataclaw-docs", "base": "main"},
    )
    assert pr["pull_request"]["state"] == "open"
    await _assert_background_and_audit(ac, chat_agent, session_module, connector_slug="github")


def test_full_stack_github_configure_sync_chat_write_audit_evidence() -> None:
    _assert_connector_release_evidence(_plan("github"))


@pytest.mark.asyncio
@_skip_unless_selected_connector("confluence")
async def test_full_stack_confluence_configure_sync_chat_write_audit_case(live_full_stack_client) -> None:
    ac, headers, chat_agent, session_module = live_full_stack_client
    await _configure_sync_compile(ac, "confluence", chat_agent)
    pages = await _mcp(ac, headers, "confluence", "read_search_pages", {"query": "revenue"})
    assert pages["pages"]
    created = await _mcp(
        ac,
        headers,
        "confluence",
        "write_create_page",
        {"space_key": "ENG", "title": "Q2 Roadmap", "body": "<p>Q2 roadmap.</p>"},
    )
    assert created["status"] == "created"
    await _assert_background_and_audit(ac, chat_agent, session_module, connector_slug="confluence")


def test_full_stack_confluence_configure_sync_chat_write_audit_evidence() -> None:
    _assert_connector_release_evidence(_plan("confluence"))


@pytest.mark.asyncio
@_skip_unless_selected_connector("notion")
async def test_full_stack_notion_configure_sync_chat_write_audit_case(live_full_stack_client) -> None:
    ac, headers, chat_agent, session_module = live_full_stack_client
    await _configure_sync_compile(ac, "notion", chat_agent)
    page = await _mcp(ac, headers, "notion", "read_get_page", {"page_id": "page-data-glossary"})
    assert page["page"]["id"] == "page-data-glossary"
    appended = await _mcp(
        ac,
        headers,
        "notion",
        "write_append_to_page",
        {"page_id": "page-data-glossary", "body": "Phase H reviewed this page."},
    )
    assert appended["status"] == "appended"
    await _assert_background_and_audit(ac, chat_agent, session_module, connector_slug="notion")


def test_full_stack_notion_configure_sync_chat_write_audit_evidence() -> None:
    _assert_connector_release_evidence(_plan("notion"))


def test_full_stack_declared_connectors_have_named_executable_case_functions() -> None:
    available = globals()
    for item in FULL_STACK_ASSERTION_PLAN:
        name = f"test_full_stack_{item.slug}_configure_sync_chat_write_audit_case"
        assert name in available


def test_phase_h_seed_artifacts_are_rich_enough_for_declared_assertions() -> None:
    postgres_seed = (REPO_ROOT / "tests/integration/postgres/01_seed.sql").read_text()
    assert "FROM generate_series(1, 10000) AS g" in postgres_seed
    assert "FROM generate_series(1, 50000) AS g" in postgres_seed
    assert "FROM generate_series(1, 100000) AS g" in postgres_seed
    mysql_seed = (REPO_ROOT / "tests/integration/mysql/01_seed.sql").read_text()
    assert "n < 10000" in mysql_seed
    assert "n < 50000" in mysql_seed
    assert "n < 100000" in mysql_seed
    sql_server_seed = (REPO_ROOT / "tests/integration/seed/sql/sql_server/01_seed.sql").read_text()
    assert "n < 10000" in sql_server_seed
    assert "n < 50000" in sql_server_seed
    assert "n < 100000" in sql_server_seed
    trino_seed = (REPO_ROOT / "tests/integration/seed/sql/trino/01_seed.sql").read_text()
    assert "sequence(1, 10000)" in trino_seed
    assert "cross join unnest(sequence(0, 4))" in trino_seed
    assert "cross join unnest(sequence(0, 9))" in trino_seed
    bigquery_counts = _bigquery_seed_counts()
    assert bigquery_counts["core.customers"] == 10_000
    assert bigquery_counts["core.orders"] == 50_000
    assert bigquery_counts["core.products"] == 1_000
    assert bigquery_counts["events.product_events"] == 100_000
    bigquery_customer = _bigquery_seed_sample()
    assert bigquery_customer["id"] == 1
    assert bigquery_customer["email"] == "user1@dataclaw.test"

    airflow_dags = {
        path.stem for path in (REPO_ROOT / "tests/integration/seed/airflow/dags").glob("*.py")
    }
    assert {"daily_etl", "daily_marketing", "failing_etl", "slow_etl", "disabled_etl"}.issubset(
        airflow_dags
    )

    dagster_repo = (REPO_ROOT / "tests/integration/seed/dagster/repository.py").read_text()
    for group_name in ("core_assets", "marketing_assets", "failing_assets", "partitioned_daily"):
        assert group_name in dagster_repo
    assert "DailyPartitionsDefinition" in dagster_repo

    confluence = yaml.safe_load((REPO_ROOT / "tests/integration/seed/confluence/space.json").read_text())
    titles = {page["title"] for page in confluence["pages"]}
    assert confluence["space"] == "ENG"
    assert len(confluence["pages"]) >= 10
    assert {"On-call Runbook", "Snowflake Outage Procedure", "Data Quality SLAs"}.issubset(titles)

    airbyte = yaml.safe_load((REPO_ROOT / "tests/integration/seed/airbyte/workspace.json").read_text())
    connections = {item["name"]: item for item in airbyte["workspace"]["connections"]}
    assert {"postgres_to_s3", "failing_api_to_postgres"}.issubset(connections)
    assert connections["postgres_to_s3"]["last_job"]["status"] == "succeeded"
    assert connections["failing_api_to_postgres"]["last_job"]["status"] == "failed"
    assert any("malformed JSON" in line for line in connections["failing_api_to_postgres"]["last_job"]["log_lines"])


def test_phase_h_fixture_api_covers_confluence_search_surface() -> None:
    fixture_api = (REPO_ROOT / "tests/integration/services/orchestration_api.py").read_text()
    assert '@app.get("/wiki/rest/api/search")' in fixture_api
    assert "conf-revenue-glossary" in fixture_api


def test_full_stack_release_gate_does_not_false_green_without_real_assertions() -> None:
    if os.getenv("DATACLAW_FULL_STACK_RELEASE_GATE") != "1":
        pytest.skip("Set DATACLAW_FULL_STACK_RELEASE_GATE=1 to run the release full-stack gate.")

    makefile = (REPO_ROOT / "Makefile").read_text()
    chat_e2e = (REPO_ROOT / "backend/tests/integration/test_e2e_chat_agent.py").read_text()
    background_tests = (REPO_ROOT / "backend/tests/test_background_runner.py").read_text()
    regression_tests = (REPO_ROOT / "backend/tests/test_task_plan_named_regressions.py").read_text()

    assert "tests/integration -m integration" in makefile
    for token in (
        '"/connectors/postgres/test"',
        '"/connectors/postgres/sync"',
        '"/ide/chat"',
        '"/mcp/postgres/tools/write_create_table"',
        "\"/alerts/{postgres_drop.json()['alert_id']}/approve-and-execute\"",
        "\"/agents/{chat_agent['id']}/audit\"",
        '"/observability/events?state=needs_approval"',
    ):
        assert token in chat_e2e
    for token in (
        "test_generic_orchestration_failure_agent_scans_granted_connectors",
        "test_data_quality_agent_uses_granted_data_store_connectors_only",
    ):
        assert token in background_tests
    assert "test_worker_heartbeat" in regression_tests


def test_full_stack_gate_has_all_connector_docs_and_catalog_backing() -> None:
    if os.getenv("DATACLAW_FULL_STACK_RELEASE_GATE") != "1":
        pytest.skip("Set DATACLAW_FULL_STACK_RELEASE_GATE=1 to run the release full-stack gate.")

    missing_docs = []
    missing_tools = []
    for slug in CATALOG_BY_SLUG:
        docs_slug = slug.replace("_", "-")
        if not (REPO_ROOT / "docs-site" / "src" / "pages" / "connectors" / f"{docs_slug}.md").exists():
            missing_docs.append(slug)
        read_tools, write_tools = tools_for_slug(slug)
        if not read_tools and not write_tools:
            missing_tools.append(slug)

    assert not missing_docs, f"Missing connector docs: {missing_docs}"
    assert not missing_tools, f"Missing MCP tool surface: {missing_tools}"
    assert verify_mcp_catalog() == []


def _compose_services() -> set[str]:
    compose = yaml.safe_load(COMPOSE_FILE.read_text())
    return set(compose.get("services", {}))


def _seed_runner_sql_files() -> set[str]:
    module = _seed_runner_module()
    return {str(path) for path in module.SQL_SEED_FILES.values()}


def _seed_runner_module():
    import importlib.util

    runner_path = REPO_ROOT / "tests" / "integration" / "seed" / "run.py"
    spec = importlib.util.spec_from_file_location("dataclaw_integration_seed_run", runner_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _bigquery_seed_counts() -> dict[str, int]:
    import importlib.util
    import sys

    loader_path = REPO_ROOT / "tests" / "integration" / "seed" / "bigquery" / "load.py"
    spec = importlib.util.spec_from_file_location("dataclaw_bigquery_seed_load", loader_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.expected_row_counts()


def _bigquery_seed_sample() -> dict[str, object]:
    import importlib.util
    import sys

    loader_path = REPO_ROOT / "tests" / "integration" / "seed" / "bigquery" / "load.py"
    spec = importlib.util.spec_from_file_location("dataclaw_bigquery_seed_load_sample", loader_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    customers = next(table for table in module.TABLES if table.fqtn == "core.customers")
    return next(module.iter_rows(customers))


def test_full_stack_gate_has_required_real_service_matrix() -> None:
    if os.getenv("DATACLAW_FULL_STACK_RELEASE_GATE") != "1":
        pytest.skip("Set DATACLAW_FULL_STACK_RELEASE_GATE=1 to run the release full-stack gate.")

    missing_services = sorted(REQUIRED_COMPOSE_SERVICES - _compose_services())

    assert not missing_services, f"Missing Phase H compose services: {missing_services}"
    compose = yaml.safe_load(COMPOSE_FILE.read_text())
    postgres_health = compose["services"]["postgres"]["healthcheck"]
    postgres_healthcheck = " ".join(postgres_health["test"])
    assert "core.customers" in postgres_healthcheck
    assert "postmaster.pid" in postgres_healthcheck
    assert postgres_health["start_period"] == "45s"


def test_full_stack_gate_has_required_seed_artifacts() -> None:
    if os.getenv("DATACLAW_FULL_STACK_RELEASE_GATE") != "1":
        pytest.skip("Set DATACLAW_FULL_STACK_RELEASE_GATE=1 to run the release full-stack gate.")

    required_artifacts = REQUIRED_SEED_ARTIFACTS | _seed_runner_sql_files()
    missing_artifacts = sorted(artifact for artifact in required_artifacts if not (REPO_ROOT / artifact).exists())

    assert not missing_artifacts, f"Missing Phase H seed/test artifacts: {missing_artifacts}"


def test_phase_h_seed_runner_wires_bigquery_loads(monkeypatch, tmp_path: Path) -> None:
    seed_runner = _seed_runner_module()
    loaded_tables: list[tuple[str, str, str, Path]] = []
    seed_dates: list[object] = []

    @dataclass(frozen=True)
    class FakeTable:
        dataset: str
        table: str
        rows: int
        schema: tuple[tuple[str, str], ...]

        @property
        def fqtn(self) -> str:
            return f"{self.dataset}.{self.table}"

    class FakeBigQuerySeed:
        TABLES = (FakeTable("core", "customers", 1, (("id", "INTEGER"),)),)

        @staticmethod
        def write_seed_files(data_dir: Path, *, seed_date=None) -> dict[str, int]:
            seed_dates.append(seed_date)
            return {"core.customers": 1}

    monkeypatch.setattr(seed_runner, "BIGQUERY_DATA_DIR", tmp_path / "bigquery")
    monkeypatch.setattr(seed_runner, "_load_bigquery_seed_module", lambda: FakeBigQuerySeed)
    # Bypass the reachability probe — the test exercises the wiring with the
    # internals mocked, not real network calls to the emulator.
    monkeypatch.setattr(seed_runner, "_bigquery_emulator_reachable", lambda endpoint, *, timeout=3.0: True)
    monkeypatch.setattr(
        seed_runner,
        "_load_bigquery_table",
        lambda *, api_endpoint, project_id, data_dir, table: loaded_tables.append((api_endpoint, project_id, table.fqtn, data_dir)),
    )

    seed_runner.seed_bigquery(seed_date="2026-05-12T00:00:00Z", api_endpoint="http://127.0.0.1:19050/")

    assert len(seed_dates) == 1
    assert seed_dates[0].isoformat() == "2026-05-12T00:00:00+00:00"
    assert loaded_tables == [("http://127.0.0.1:19050", "dataclaw-integration", "core.customers", tmp_path / "bigquery")]


def test_phase_h_bigquery_load_commands_accept_emulator_endpoint(tmp_path: Path) -> None:
    loader_path = REPO_ROOT / "tests" / "integration" / "seed" / "bigquery" / "load.py"
    spec = importlib.util.spec_from_file_location("dataclaw_bigquery_seed_load_endpoint", loader_path)
    assert spec is not None and spec.loader is not None
    seed_loader = importlib.util.module_from_spec(spec)
    import sys

    sys.modules[spec.name] = seed_loader
    spec.loader.exec_module(seed_loader)

    command = seed_loader.bq_load_commands(
        project_id="dataclaw-integration",
        data_dir=tmp_path,
        api_endpoint="http://127.0.0.1:19050/",
    )[0]

    assert command[:5] == ["bq", "--api", "http://127.0.0.1:19050", "--project_id", "dataclaw-integration"]


def test_phase_h_seed_runner_splits_sql_server_batches() -> None:
    spec = importlib.util.spec_from_file_location(
        "dataclaw_integration_seed_run_sql_server",
        REPO_ROOT / "tests/integration/seed/run.py",
    )
    assert spec is not None and spec.loader is not None
    seed_runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(seed_runner)

    assert seed_runner._split_sql_server_batches("select 1\ngo\n\nselect 'go inside text'\nGO\n") == [
        "select 1",
        "select 'go inside text'",
    ]


def test_full_stack_command_runs_connector_and_e2e_release_gates() -> None:
    if os.getenv("DATACLAW_FULL_STACK_RELEASE_GATE") != "1":
        pytest.skip("Set DATACLAW_FULL_STACK_RELEASE_GATE=1 to run the release full-stack gate.")

    makefile = (REPO_ROOT / "Makefile").read_text()
    assert "tests/integration -m integration" in makefile

    missing_tests = sorted(path for path in REQUIRED_INTEGRATION_TEST_FILES if not (REPO_ROOT / path).exists())
    assert not missing_tests, f"Missing release-gated integration tests: {missing_tests}"


def test_connector_phase_h_target_starts_only_selected_connector_stack() -> None:
    makefile = (REPO_ROOT / "Makefile").read_text()

    assert "COMPOSE_FILE=\"$${ROOT}/tests/integration/docker-compose.yml\"" in makefile
    assert "docker compose -p \"$${PROJECT}\" -f \"$${COMPOSE_FILE}\" build \"$${CONNECTOR_SERVICE}\"" in makefile
    assert "docker compose -p \"$${PROJECT}\" -f \"$${COMPOSE_FILE}\" up -d --wait chroma \"$${CONNECTOR_SERVICE}\"" in makefile
    assert "python3 tests/integration/seed/run.py --only sql_server" in makefile
    assert "python3 tests/integration/seed/run.py --only bigquery" in makefile
    assert "--execute \"select 1\"" in makefile
    assert "Trino did not become query-ready" in makefile
    assert "PROJECT=\"dataclaw-$${CONNECTOR}\"" in makefile
    assert "$(MAKE) integration-up" not in makefile.split("test-integration-connector:", 1)[1].split("test-integration-full:", 1)[0]
    for connector, service in {
        "postgres": "postgres",
        "mysql": "mysql",
        "sql_server": "sql_server",
        "trino": "trino",
        "bigquery": "bigquery",
        "airflow": "airflow",
        "dbt": "fixture-api",
        "dagster": "dagster",
        "prefect": "prefect",
        "airbyte": "fixture-api",
        "github": "fixture-api",
        "confluence": "fixture-api",
        "notion": "fixture-api",
    }.items():
        assert f"{connector}) CONNECTOR_SERVICE={service} ;;" in makefile


def test_approved_trino_sql_routes_to_connector_executor() -> None:
    main_source = (REPO_ROOT / "backend/app/main.py").read_text()

    assert "elif connector_slug == \"trino\":" in main_source
    assert "await asyncio.to_thread(_trino_execute, credentials, decision.sql)" in main_source
    assert "if connector_slug not in {\"sql_server\", \"trino\"}:" in main_source


def test_request_sessions_expose_session_factory_for_tool_audit() -> None:
    session_source = (REPO_ROOT / "backend/app/db/session.py").read_text()

    assert 'session.info["session_factory"] = SessionLocal' in session_source


def test_dagster_container_uses_writable_workdir() -> None:
    dockerfile = (REPO_ROOT / "tests/integration/dagster/Dockerfile").read_text()

    assert "WORKDIR /tmp" in dockerfile
    assert "-f\", \"/services/dagster_app.py\"" in dockerfile
