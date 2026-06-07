from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import quote, quote_plus

import httpx
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import get_settings
from app.core.security import decrypt_json
from app.models.domain import (
    Agent,
    AgentMcpGrant,
    AgentToolCall,
    AgentWriteAudit,
    Alert,
    Connector,
    LogEntry,
    Workspace,
)
from app.services.connectors.adapters import (
    adapter_for,
    normalize_snowflake_account,
    parse_redshift_endpoint,
)
from app.services.mcp_catalog import tools_for_slug
from app.services.sql_safety import UnsafeSqlError, validate_read_only_sql, validate_write_sql

logger = logging.getLogger("dataclaw.mcp_executor")

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
BQ_PROJECT_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")
CONNECTORS_WITH_NATIVE_WRITE_AUDIT = {"sqlite", "postgres", "mysql", "redshift", "trino", "sql_server", "snowflake"}
_GENERATED_AIRFLOW_DAGS: dict[str, dict[str, Any]] = {}
AIRFLOW_APPROVAL_TOOLS = {"write_clear_task_instance", "write_mark_task_failed", "write_delete_dag"}
FRESHNESS_TYPES = {"timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz", "datetime", "date"}


def _mapping_value(row: Any, key: str, default: Any = None) -> Any:
    if key in row:
        return row[key]
    lower_key = key.lower()
    for row_key, value in row.items():
        if str(row_key).lower() == lower_key:
            return value
    return default


SQL_BASE_TOOLS = {
    "read_list_tables",
    "read_get_schema",
    "read_query_select",
    "read_get_row_count",
    "read_sample_rows",
    "read_search_columns",
    "read_get_column_stats",
    "read_get_table_freshness",
    "read_get_storage_size",
    "write_execute_sql",
    "write_create_table",
    "write_create_view",
    "write_insert_rows",
}
SQL_MUTATION_TOOLS = {"write_update_rows", "write_delete_rows", "write_create_index"}
SQL_ADMIN_TOOLS = {"read_explain_query", "read_list_users", "read_list_grants", "read_get_query_history", "write_grant_permission"}
SQLITE_TOOLS = SQL_BASE_TOOLS | {"read_explain_query"} | SQL_MUTATION_TOOLS
SQL_TRINO_TOOLS = SQL_BASE_TOOLS | {"read_explain_query", "read_list_users", "read_list_grants", "read_get_query_history"}
SQL_SERVER_DATABASE_TOOLS = SQL_BASE_TOOLS | SQL_MUTATION_TOOLS | SQL_ADMIN_TOOLS
SQL_REDSHIFT_TOOLS = SQL_SERVER_DATABASE_TOOLS | {
    "read_get_workload_management",
    "read_list_clusters",
    "read_get_disk_usage",
    "write_pause_cluster",
    "write_resume_cluster",
}
SQL_SNOWFLAKE_TOOLS = SQL_BASE_TOOLS | SQL_MUTATION_TOOLS | (SQL_ADMIN_TOOLS - {"write_grant_permission"}) | {
    "read_list_warehouses",
    "read_query_history",
    "read_get_credit_usage",
    "read_list_pipes",
    "read_list_streams",
    "read_list_tasks",
    "write_resume_warehouse",
    "write_suspend_warehouse",
    "write_create_pipe",
    "write_create_task",
}

IMPLEMENTED_MCP_TOOLS: dict[str, set[str]] = {
    "airflow": {
        "read_list_dags",
        "read_get_run",
        "read_get_dag_source",
        "read_get_task_logs",
        "read_list_task_instances",
        "read_list_dag_runs",
        "read_get_xcom",
        "read_list_pools",
        "read_get_pool",
        "read_list_variables",
        "read_get_variable",
        "read_get_dag_dependencies",
        "read_get_import_errors",
        "write_trigger_dag",
        "write_create_dag",
        "write_pause_dag",
        "write_unpause_dag",
        "write_clear_task_instance",
        "write_mark_task_success",
        "write_mark_task_failed",
        "write_set_variable",
        "write_set_pool",
        "write_delete_dag",
    },
    "airbyte": {
        "read_list_connections",
        "read_get_job_logs",
        "read_list_jobs",
        "read_get_connection_state",
        "read_list_sources",
        "read_get_source",
        "read_list_destinations",
        "read_get_destination",
        "read_get_workspace",
        "read_get_connection_schema",
        "write_trigger_sync",
        "write_reset_connection",
        "write_cancel_job",
        "write_create_connection",
        "write_update_connection",
        "write_disable_connection",
        "write_enable_connection",
    },
    "prefect": {
        "read_list_flows",
        "read_get_run",
        "read_get_run_logs",
        "read_get_task_logs",
        "read_list_flow_runs",
        "read_list_deployments",
        "read_get_deployment",
        "read_get_task_run",
        "read_list_work_pools",
        "read_get_block",
        "read_get_concurrency_limit",
        "read_list_artifacts",
        "write_trigger_flow_run",
        "write_create_deployment",
        "write_pause_deployment",
        "write_resume_deployment",
        "write_cancel_flow_run",
        "write_set_block",
        "write_set_concurrency_limit",
        "write_delete_deployment",
    },
    "dagster": {
        "read_list_assets",
        "read_get_run",
        "read_get_run_logs",
        "read_get_event_logs",
        "read_get_asset_materializations",
        "read_list_jobs",
        "read_list_partitions",
        "read_get_run_steps",
        "read_get_asset_checks",
        "read_get_sensor_state",
        "read_list_sensors",
        "read_list_schedules",
        "read_get_schedule_state",
        "write_materialize_asset",
        "write_trigger_job",
        "write_backfill_partitions",
        "write_terminate_run",
        "write_launch_sensor",
        "write_start_schedule",
        "write_stop_schedule",
    },
    "fivetran": {
        "read_list_connectors",
        "read_get_connector_logs",
        "read_get_connector_status",
        "read_get_connector_schema",
        "read_list_destinations",
        "read_get_destination",
        "read_get_metadata",
        "read_get_data_volume",
        "read_get_sync_history",
        "write_trigger_sync",
        "write_pause_connector",
        "write_resume_connector",
        "write_resync_table",
        "write_modify_connector_schema",
        "write_delete_connector",
    },
    "notion": {
        "read_search_pages",
        "read_get_page",
        "read_get_database",
        "read_query_database",
        "read_get_block_children",
        "read_get_comments",
        "read_list_users",
        "write_create_page",
        "write_append_to_page",
        "write_update_page_properties",
        "write_archive_page",
        "write_create_comment",
        "write_create_database",
        "write_update_block",
    },
    "github": {
        "read_list_repos",
        "read_get_file",
        "read_list_issues",
        "read_get_issue",
        "read_get_pr",
        "read_get_pr_diff",
        "read_list_branches",
        "read_search_code",
        "read_get_commit",
        "read_list_workflows",
        "read_get_workflow_run_logs",
        "read_get_repo_metadata",
        "read_list_releases",
        "write_commit_file",
        "write_create_pr",
        "write_create_issue",
        "write_comment_on_pr",
        "write_comment_on_issue",
        "write_merge_pr",
        "write_create_branch",
        "write_delete_branch",
        "write_close_pr",
        "write_close_issue",
        "write_request_review",
    },
    "google_docs": {
        "read_list_docs",
        "read_get_doc",
        "read_search_docs",
        "read_get_doc_comments",
        "read_get_doc_revisions",
        "read_list_folder_contents",
        "read_list_shared_with_me",
        "read_get_doc_metadata",
        "write_create_doc",
        "write_append_to_doc",
        "write_replace_text",
        "write_create_comment",
        "write_share_doc",
        "write_move_doc",
        "write_rename_doc",
    },
    "quip": {
        "read_search",
        "read_get_thread",
        "read_get_thread_history",
        "read_list_folders",
        "read_get_folder",
        "read_get_messages",
        "write_create_thread",
        "write_edit_thread",
        "write_send_message",
        "write_share_thread",
        "write_create_folder",
    },
    "confluence": {
        "read_search_pages",
        "read_get_page",
        "read_get_page_children",
        "read_get_space",
        "read_get_page_history",
        "read_search_attachments",
        "read_get_comments",
        "read_list_spaces",
        "read_get_labels",
        "write_create_page",
        "write_append_to_page",
        "write_update_page",
        "write_add_label",
        "write_create_comment",
        "write_create_attachment",
        "write_move_page",
        "write_delete_page",
    },
    "dbt": {
        "read_list_models",
        "read_get_lineage",
        "read_get_run_logs",
        "read_list_runs",
        "read_get_run_artifacts",
        "read_get_manifest",
        "read_list_tests",
        "read_get_test_results",
        "read_get_source_freshness",
        "read_get_model_source",
        "read_list_exposures",
        "read_get_model_docs",
        "write_trigger_run",
        "write_trigger_test",
        "write_cancel_run",
        "write_create_model",
        "write_update_model",
        "write_trigger_snapshot",
        "write_trigger_seed",
    },
    "openai": {"read_list_models"},
    "sqlite": SQLITE_TOOLS,
    "postgres": SQL_SERVER_DATABASE_TOOLS,
    "mysql": SQL_SERVER_DATABASE_TOOLS,
    "trino": SQL_TRINO_TOOLS,
    "redshift": SQL_REDSHIFT_TOOLS,
    "sql_server": SQL_SERVER_DATABASE_TOOLS,
    "databricks": {
        "read_list_tables",
        "read_get_schema",
        "read_query_select",
        "read_get_row_count",
        "read_list_jobs",
        "read_get_unity_asset",
        "read_list_clusters",
        "read_list_warehouses",
        "read_get_notebook",
        "read_get_run_logs",
        "read_get_lineage",
        "read_get_query_history",
        "read_get_table_freshness",
        "write_execute_sql",
        "write_create_table",
        "write_trigger_job",
        "write_run_notebook",
        "write_start_cluster",
        "write_stop_cluster",
        "write_create_view",
        "write_update_unity_grants",
    },
    "bigquery": {
        "read_list_tables",
        "read_get_schema",
        "read_query_select",
        "read_get_row_count",
        "read_list_jobs",
        "read_list_datasets",
        "read_get_query_history",
        "read_search_columns",
        "read_get_table_freshness",
        "read_get_storage_size",
        "read_explain_query",
        "read_get_slot_usage",
        "write_execute_sql",
        "write_create_table",
        "write_run_query_save_to_table",
        "write_load_from_gcs",
        "write_export_to_gcs",
        "write_create_view",
        "write_create_dataset",
    },
    "snowflake": SQL_SNOWFLAKE_TOOLS,
}


def implemented_mcp_tools_for_slug(slug: str) -> set[str]:
    return set(IMPLEMENTED_MCP_TOOLS.get(slug, set()))


class McpExecutionError(ValueError):
    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


def _scope_for_tool(tool_name: str) -> str:
    if tool_name.startswith("read_"):
        return "read"
    if tool_name.startswith("write_"):
        return "write"
    raise McpExecutionError(400, "MCP tools must be prefixed with read_ or write_.")


def _safe_identifier(value: str, field_name: str = "table") -> str:
    if not IDENTIFIER.match(value):
        raise McpExecutionError(400, f"Invalid {field_name} identifier.")
    return value


def _safe_bigquery_project(value: str) -> str:
    if not BQ_PROJECT_IDENTIFIER.fullmatch(value or ""):
        raise McpExecutionError(400, "Invalid BigQuery project identifier.")
    return value


def _url_segment(value: str, *, field_name: str) -> str:
    if not value:
        raise McpExecutionError(400, f"{field_name} is required.")
    return quote(value, safe="")


def _bounded_limit(value: Any, *, default: int = 100, maximum: int = 1000) -> int:
    try:
        limit = int(value or default)
    except (TypeError, ValueError) as exc:
        raise McpExecutionError(400, "limit must be an integer.") from exc
    return max(1, min(limit, maximum))


def _since_params(arguments: dict[str, Any], *, date_param: str = "start_date_gte") -> dict[str, Any]:
    params: dict[str, Any] = {"limit": _bounded_limit(arguments.get("limit"))}
    since = arguments.get("since")
    if since:
        params[date_param] = str(since)
    return params


def _data_list(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get("data")
        return data if isinstance(data, list) else []
    return []


def _dagster_repository_context(credentials: dict[str, Any], arguments: dict[str, Any]) -> tuple[str, str]:
    repository_name = str(arguments.get("repository_name") or arguments.get("repositoryName") or credentials.get("repository_name") or "dataclaw")
    location_name = str(
        arguments.get("repository_location_name")
        or arguments.get("repositoryLocationName")
        or credentials.get("repository_location_name")
        or credentials.get("location_name")
        or "default"
    )
    return repository_name, location_name


def _dagster_pipeline_selector(credentials: dict[str, Any], arguments: dict[str, Any]) -> dict[str, Any]:
    selector = dict(arguments.get("selector") or {})
    if selector:
        return selector
    repository_name, location_name = _dagster_repository_context(credentials, arguments)
    return {
        "repositoryName": repository_name,
        "repositoryLocationName": location_name,
        "pipelineName": arguments.get("job_name") or arguments.get("pipeline_name") or "analytics",
    }


def _dagster_instigation_selector(credentials: dict[str, Any], arguments: dict[str, Any], name: str) -> dict[str, str]:
    repository_name, location_name = _dagster_repository_context(credentials, arguments)
    return {
        "repositoryName": repository_name,
        "repositoryLocationName": location_name,
        "name": name,
    }


async def _airbyte_workspace_id(
    client: httpx.AsyncClient,
    base_url: str,
    headers: dict[str, str],
    credentials: dict[str, Any],
) -> str | None:
    explicit = credentials.get("workspace_id") or credentials.get("workspaceId")
    if explicit:
        return str(explicit)
    response = await client.post(f"{base_url}/api/v1/workspaces/list", headers=headers, json={})
    if response.status_code == 404:
        return None
    response.raise_for_status()
    payload = response.json()
    workspaces = payload.get("workspaces") or payload.get("data") or []
    if not workspaces:
        return None
    workspace = workspaces[0]
    return str(workspace.get("workspaceId") or workspace.get("workspace_id") or workspace.get("id") or "")


def _airbyte_rows(payload: dict[str, Any], *keys: str) -> list[Any]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    data = payload.get("data")
    if isinstance(data, list):
        return data
    objects = payload.get("objects")
    if isinstance(objects, list):
        return objects
    return []


def _airbyte_int_id(value: str, *, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise McpExecutionError(400, f"{field_name} must be an integer for the Airbyte Config API.") from exc


def _maybe_airbyte_int_id(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _airbyte_job_logs(payload: dict[str, Any]) -> list[Any]:
    logs = payload.get("logs")
    if isinstance(logs, list):
        return logs
    attempts = payload.get("attempts") or payload.get("job", {}).get("attempts") or []
    lines: list[Any] = []
    for attempt in attempts if isinstance(attempts, list) else []:
        attempt_logs = attempt.get("logs", {}) if isinstance(attempt, dict) else {}
        log_lines = attempt_logs.get("logLines") if isinstance(attempt_logs, dict) else None
        if isinstance(log_lines, list):
            lines.extend(log_lines)
    return lines


def _fivetran_connector_status_events(connector: dict[str, Any]) -> list[dict[str, Any]]:
    status = connector.get("status") if isinstance(connector.get("status"), dict) else {}
    events: list[dict[str, Any]] = []
    for key in ("tasks", "warnings"):
        values = status.get(key)
        if isinstance(values, list):
            for value in values:
                events.append({"type": key.removesuffix("s"), "detail": value})
    for key in ("succeeded_at", "failed_at", "last_sync", "sync_state"):
        value = connector.get(key) or status.get(key)
        if value:
            events.append({"type": key, "detail": value})
    return events


def _fivetran_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", payload)
    if isinstance(data, dict):
        items = data.get("items") or data.get("connections") or data.get("connectors")
        return items if isinstance(items, list) else []
    return data if isinstance(data, list) else []


async def _fivetran_get_json(client: httpx.AsyncClient, base_url: str, headers: dict[str, str], *paths: str) -> dict[str, Any]:
    last_response: httpx.Response | None = None
    for path in paths:
        response = await client.get(f"{base_url}{path}", headers=headers)
        if response.status_code == 404:
            last_response = response
            continue
        response.raise_for_status()
        return response.json()
    if last_response is not None:
        last_response.raise_for_status()
    return {}


async def _fivetran_list_connections(client: httpx.AsyncClient, base_url: str, headers: dict[str, str]) -> list[dict[str, Any]]:
    try:
        return _fivetran_items(await _fivetran_get_json(client, base_url, headers, "/v1/connections", "/v1/connectors"))
    except httpx.HTTPStatusError:
        groups = _fivetran_items(await _fivetran_get_json(client, base_url, headers, "/v1/groups"))
        connections: list[dict[str, Any]] = []
        for group in groups:
            group_id = group.get("id")
            if not group_id:
                continue
            try:
                connections.extend(
                    _fivetran_items(
                        await _fivetran_get_json(
                            client,
                            base_url,
                            headers,
                            f"/v1/groups/{_url_segment(str(group_id), field_name='group_id')}/connections",
                            f"/v1/groups/{_url_segment(str(group_id), field_name='group_id')}/connectors",
                        )
                    )
                )
            except httpx.HTTPStatusError:
                continue
        return connections


async def _fivetran_get_connection(client: httpx.AsyncClient, base_url: str, headers: dict[str, str], connector_id: str) -> dict[str, Any]:
    connector_path = _url_segment(connector_id, field_name="connector_id")
    payload = await _fivetran_get_json(client, base_url, headers, f"/v1/connections/{connector_path}", f"/v1/connectors/{connector_path}")
    data = payload.get("data", payload)
    return data if isinstance(data, dict) else {}


async def _graphql(client: httpx.AsyncClient, base_url: str, headers: dict[str, str], query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    # `base_url` is already the GraphQL endpoint (DagsterAdapter.base_url
    # normalizes both Cloud and OSS URLs to end in `/graphql`).
    response = await client.post(
        base_url,
        headers=headers,
        json={"query": query, "variables": variables or {}},
    )
    response.raise_for_status()
    payload = response.json()
    errors = payload.get("errors") if isinstance(payload, dict) else None
    if errors:
        raise McpExecutionError(502, f"Dagster GraphQL error: {errors}")
    return payload


def _quoted_identifier(connector_slug: str, value: str) -> str:
    identifier = _safe_identifier(value)
    if connector_slug in {"mysql", "bigquery"}:
        return f"`{identifier}`"
    if connector_slug == "sql_server":
        return f"[{identifier}]"
    return f'"{identifier}"'


def _qualified_table(connector_slug: str, table_name: str, schema: str | None = None) -> str:
    if connector_slug == "bigquery":
        table = _safe_identifier(table_name, "table")
        if not schema:
            return f"`{table}`"
        return f"`{_safe_identifier(schema, 'schema')}.{table}`"
    table = _quoted_identifier(connector_slug, table_name)
    if not schema:
        return table
    return f"{_quoted_identifier(connector_slug, schema)}.{table}"


def _default_schema_for_datastore(connector_slug: str, credentials: dict[str, Any]) -> str | None:
    if connector_slug == "mysql":
        return str(credentials.get("database") or "").strip() or None
    if connector_slug == "trino":
        return str(credentials.get("schema") or "").strip() or None
    if connector_slug in {"postgres", "redshift"}:
        return "public"
    return None


async def _workspace(session: AsyncSession) -> Workspace:
    workspace = await session.scalar(select(Workspace).limit(1))
    if workspace is None:
        raise McpExecutionError(400, "Workspace has not been seeded.")
    return workspace


async def _pending_mcp_approval(
    session: AsyncSession,
    *,
    agent_id: str,
    connector_slug: str,
    tool_name: str,
    arguments: dict[str, Any],
    title: str,
) -> dict[str, Any]:
    workspace = await _workspace(session)
    public_arguments = {key: value for key, value in arguments.items() if key != "__approved"}
    alert = Alert(
        workspace_id=workspace.id,
        severity="critical",
        title=title,
        detail=(
            "Approval required for DataClaw MCP write tool.\n"
            f"MCP-Action: {connector_slug}.{tool_name}\n"
            f"Agent-ID: {agent_id}\n"
            f"Arguments: {json.dumps(public_arguments, default=str)}"
        ),
        requires_approval=True,
    )
    session.add(alert)
    await session.commit()
    return {"status": "pending_approval", "alert_id": alert.id, "tool": tool_name, "connector_slug": connector_slug}


async def resolve_granted_agent(
    session: AsyncSession,
    agent_id: str | None,
    connector_slug: str,
    tool_name: str,
) -> Agent:
    if not agent_id:
        raise McpExecutionError(400, "X-DataClaw-Agent-Id header is required.")
    agent = await session.get(Agent, agent_id)
    if agent is None or not agent.enabled:
        raise McpExecutionError(404, "Agent not found or disabled.")
    read_tools, write_tools = tools_for_slug(connector_slug)
    if tool_name not in {*read_tools, *write_tools}:
        raise McpExecutionError(404, f"Unknown tool for connector {connector_slug}: {tool_name}")
    grant = await session.scalar(
        select(AgentMcpGrant).where(
            AgentMcpGrant.agent_id == agent.id,
            AgentMcpGrant.connector_slug == connector_slug,
        )
    )
    scope = _scope_for_tool(tool_name)
    allowed = bool(grant and (grant.read_enabled if scope == "read" else grant.write_enabled))
    if not allowed:
        raise McpExecutionError(403, f"Agent {agent.name} is not granted {scope} access to {connector_slug}.")
    return agent


_NOTION_PAGE_ID_TOOLS: set[str] = {
    "write_append_to_page",
    "write_update_page_properties",
    "write_archive_page",
    "write_create_comment",
}


async def _preflight_write_arguments(
    *,
    session: AsyncSession,
    connector_slug: str,
    tool_name: str,
    arguments: dict[str, Any],
) -> None:
    """Validate write tool arguments before creating an approval alert.

    The chat LLM occasionally hallucinates resource IDs (notably Notion
    page_ids). Pre-validating them here lets the LLM self-correct on its
    next turn instead of polluting the operator's approval queue with an
    alert that will fail at execution time and surface as a 500.
    """
    if connector_slug == "notion" and tool_name in _NOTION_PAGE_ID_TOOLS:
        page_id = str(arguments.get("page_id") or arguments.get("id") or "").strip()
        if not page_id:
            return
        credentials = await _optional_connector_credentials(session, "notion")
        if not credentials:
            return
        try:
            adapter = adapter_for("notion")
            base_url = adapter.base_url(credentials)
            headers = adapter.headers(credentials)
            segment = _url_segment(page_id, field_name="page_id")
        except Exception as exc:
            logger.debug(
                "notion_preflight_skipped_adapter_setup",
                extra={"_error": exc.__class__.__name__, "_message": str(exc)},
            )
            return
        async with httpx.AsyncClient(timeout=10) as client:
            try:
                response = await client.get(f"{base_url}/v1/pages/{segment}", headers=headers)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                logger.debug(
                    "notion_preflight_skipped_network",
                    extra={"_error": exc.__class__.__name__},
                )
                return
        if response.status_code == 404:
            raise McpExecutionError(
                400,
                f"Notion page_id {page_id!r} does not exist. The agent must call notion.read_search_pages "
                "to find the real page_id before attempting this write.",
            )


async def execute_mcp_tool(
    *,
    session: AsyncSession,
    engine: AsyncEngine,
    connector_slug: str,
    tool_name: str,
    arguments: dict[str, Any],
    agent_id: str | None,
    user_email: str,
    run_id: str | None = None,
    record_tool_call: bool = True,
) -> dict[str, Any]:
    with session.no_autoflush:
        agent = await resolve_granted_agent(session, agent_id, connector_slug, tool_name)
    started = perf_counter()
    try:
        if _scope_for_tool(tool_name) == "write" and not arguments.get("__approved"):
            await _preflight_write_arguments(
                session=session,
                connector_slug=connector_slug,
                tool_name=tool_name,
                arguments=arguments,
            )
            result = await _pending_mcp_approval(
                session,
                agent_id=agent.id,
                connector_slug=connector_slug,
                tool_name=tool_name,
                arguments=arguments,
                title=f"Agent {agent.name} wants to run {connector_slug}.{tool_name}",
            )
        else:
            result = await _execute_mcp_tool_inner(
                session=session,
                engine=engine,
                connector_slug=connector_slug,
                tool_name=tool_name,
                arguments=arguments,
                agent=agent,
                user_email=user_email,
            )
    except Exception as exc:
        if record_tool_call:
            try:
                await _record_tool_call(
                    session,
                    agent_name=agent.name,
                    connector_slug=connector_slug,
                    tool_name=tool_name,
                    arguments=arguments,
                    run_id=run_id,
                    status="error",
                    error_message=exc.__class__.__name__,
                    latency_ms=int((perf_counter() - started) * 1000),
                )
            except Exception as audit_exc:
                logger.warning(
                    "tool_call_audit_failed",
                    extra={
                        "_tool": f"{connector_slug}.{tool_name}",
                        "_error": audit_exc.__class__.__name__,
                        "_message": str(audit_exc),
                    },
                )
        raise
    try:
        if record_tool_call:
            await _record_tool_call(
                session,
                agent_name=agent.name,
                connector_slug=connector_slug,
                tool_name=tool_name,
                arguments=arguments,
                run_id=run_id,
                status=str(result.get("status") or "ok"),
                result=result,
                latency_ms=int((perf_counter() - started) * 1000),
            )
        await _record_generic_write_audit(
            session,
            agent=agent,
            connector_slug=connector_slug,
            tool_name=tool_name,
            arguments=arguments,
            result=result,
            user_email=user_email,
        )
    except Exception as audit_exc:
        logger.warning(
            "tool_call_audit_failed",
            extra={
                "_tool": f"{connector_slug}.{tool_name}",
                "_error": audit_exc.__class__.__name__,
                "_message": str(audit_exc),
            },
        )
    return result


async def _record_generic_write_audit(
    session: AsyncSession,
    *,
    agent: Agent,
    connector_slug: str,
    tool_name: str,
    arguments: dict[str, Any],
    result: dict[str, Any],
    user_email: str,
) -> None:
    if _scope_for_tool(tool_name) != "write":
        return
    if connector_slug in CONNECTORS_WITH_NATIVE_WRITE_AUDIT:
        return
    status = str(result.get("status") or "ok")
    if status in {"pending_approval", "error"}:
        return
    target = _write_audit_target(arguments, result)
    statement = json.dumps(
        {
            "tool": tool_name,
            "arguments": {key: value for key, value in arguments.items() if key != "__approved"},
            "result_status": status,
        },
        default=str,
        sort_keys=True,
    )
    row = AgentWriteAudit(
        workspace_id=agent.workspace_id,
        agent_id=agent.id,
        connector_slug=connector_slug,
        statement_type=tool_name.removeprefix("write_").upper(),
        statement=statement,
        target=target,
        affected_rows=None,
        required_approval=False,
        executed_at=datetime.now(UTC),
        executed_by=user_email,
    )
    if session.in_transaction():
        session.add(row)
        await session.flush()
        return
    if session.is_active:
        session.add(row)
        await session.commit()
        return
    session_factory = session.info.get("session_factory")
    if session_factory is None:
        bind = getattr(session, "bind", None)
        if bind is not None:
            session_factory = async_sessionmaker(bind, class_=AsyncSession, expire_on_commit=False)
        else:
            from app.db.session import SessionLocal

            session_factory = SessionLocal
    async with session_factory() as audit_session:
        audit_session.add(row)
        await audit_session.commit()


def _write_audit_target(arguments: dict[str, Any], result: dict[str, Any]) -> str | None:
    for source in (arguments, result):
        for key in (
            "table",
            "destination_table",
            "view",
            "path",
            "page_id",
            "parent_id",
            "connector_id",
            "run_id",
            "job_id",
            "name",
            "title",
        ):
            value = source.get(key)
            if value:
                return str(value)[:255]
    for key in ("page", "pull_request", "issue", "run", "result"):
        value = result.get(key)
        if isinstance(value, dict):
            for nested_key in ("id", "number", "url", "html_url", "title", "name"):
                nested = value.get(nested_key)
                if nested:
                    return str(nested)[:255]
    return None


async def _execute_mcp_tool_inner(
    *,
    session: AsyncSession,
    engine: AsyncEngine,
    connector_slug: str,
    tool_name: str,
    arguments: dict[str, Any],
    agent: Agent,
    user_email: str,
) -> dict[str, Any]:
    if connector_slug == "airflow":
        return await _airflow_tool(session, tool_name, arguments, agent.id)
    if connector_slug == "airbyte":
        return await _airbyte_tool(session, tool_name, arguments, agent.id)
    if connector_slug == "prefect":
        return await _prefect_tool(session, tool_name, arguments, agent.id)
    if connector_slug == "dagster":
        return await _dagster_tool(session, tool_name, arguments, agent.id)
    if connector_slug == "fivetran":
        return await _fivetran_tool(session, tool_name, arguments, agent.id)
    if connector_slug == "notion":
        return await _notion_tool(session, tool_name, arguments, agent.id)
    if connector_slug == "github":
        return await _github_tool(session, tool_name, arguments, agent.id)
    if connector_slug in {"google_docs", "quip", "confluence"}:
        return await _kb_tool(session, connector_slug, tool_name, arguments, agent.id)
    if connector_slug == "dbt":
        return await _dbt_tool(session, tool_name, arguments, agent.id)
    if connector_slug == "databricks":
        return await _databricks_tool(session, tool_name, arguments, agent.id)
    if connector_slug == "bigquery":
        return await _bigquery_tool(session, tool_name, arguments, agent.id)
    if connector_slug == "snowflake":
        return await _snowflake_tool(session, tool_name, arguments, agent.id)
    if connector_slug == "trino":
        return await _trino_tool(
            session=session,
            tool_name=tool_name,
            arguments=arguments,
            agent=agent,
            user_email=user_email,
        )
    if connector_slug == "openai":
        return await _openai_tool(session, tool_name, arguments, agent.id)
    if connector_slug in {"postgres", "mysql", "redshift"}:
        return await _sql_datastore_tool(
            session=session,
            connector_slug=connector_slug,
            tool_name=tool_name,
            arguments=arguments,
            agent=agent,
            user_email=user_email,
        )
    if connector_slug == "sql_server":
        return await _sql_server_tool(
            session=session,
            tool_name=tool_name,
            arguments=arguments,
            agent=agent,
            user_email=user_email,
        )
    if connector_slug != "sqlite":
        raise McpExecutionError(501, f"Connector-specific execution is not implemented for {connector_slug}.{tool_name}.")
    if tool_name == "read_list_tables":
        return await _sqlite_list_tables(engine)
    if tool_name == "read_get_schema":
        return await _sqlite_get_schema(engine, arguments)
    if tool_name == "read_query_select":
        return await _sqlite_query_select(engine, arguments)
    if tool_name == "read_get_row_count":
        return await _sqlite_row_count(engine, arguments)
    if tool_name == "read_sample_rows":
        return await _sqlite_sample_rows(engine, arguments)
    if tool_name == "read_search_columns":
        return await _sqlite_search_columns(engine, arguments)
    if tool_name == "read_get_column_stats":
        return await _sqlite_column_stats(engine, arguments)
    if tool_name == "read_get_table_freshness":
        return await _sqlite_table_freshness(engine, arguments)
    if tool_name == "read_get_storage_size":
        return await _sqlite_storage_size(engine, arguments)
    if tool_name == "read_explain_query":
        return await _sqlite_explain_query(engine, arguments)
    if tool_name in {"write_execute_sql", "write_create_table", "write_create_view", "write_insert_rows", "write_update_rows", "write_delete_rows", "write_create_index"}:
        return await _sqlite_write(
            session=session,
            engine=engine,
            agent=agent,
            tool_name=tool_name,
            arguments=arguments,
            user_email=user_email,
        )
    raise McpExecutionError(404, f"Unsupported SQLite MCP tool: {tool_name}")


async def _record_tool_call(
    session: AsyncSession,
    *,
    agent_name: str,
    connector_slug: str,
    tool_name: str,
    arguments: dict[str, Any],
    run_id: str | None,
    status: str,
    latency_ms: int,
    result: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    result_text = "" if result is None else json.dumps(result, default=str)[:1000]
    result_size = 0 if result is None else len(json.dumps(result, default=str).encode("utf-8"))
    row = AgentToolCall(
        run_id=run_id,
        agent_name=agent_name,
        tool_name=tool_name,
        connector_slug=connector_slug,
        args_json=arguments,
        result_summary=result_text,
        result_size_bytes=result_size,
        latency_ms=latency_ms,
        status=status,
        error_message=error_message,
        called_at=datetime.now(UTC),
    )
    if session.in_transaction():
        session.add(row)
        await session.flush()
        return
    if session.is_active:
        session.add(row)
        await session.commit()
        return
    session_factory = session.info.get("session_factory")
    if session_factory is None:
        bind = getattr(session, "bind", None)
        if bind is not None:
            session_factory = async_sessionmaker(bind, class_=AsyncSession, expire_on_commit=False)
        else:
            from app.db.session import SessionLocal

            session_factory = SessionLocal
    async with session_factory() as audit_session:
        audit_session.add(row)
        await audit_session.commit()


async def _connector_credentials(session: AsyncSession, connector_slug: str) -> dict[str, Any]:
    connector = await session.scalar(select(Connector).where(Connector.slug == connector_slug))
    if connector is None or not connector.encrypted_credentials:
        raise McpExecutionError(400, f"Connector {connector_slug} is not configured.")
    return decrypt_json(get_settings().master_key, connector.encrypted_credentials)


async def _optional_connector_credentials(session: AsyncSession, connector_slug: str) -> dict[str, Any]:
    connector = await session.scalar(select(Connector).where(Connector.slug == connector_slug))
    if connector is None or not connector.encrypted_credentials:
        return {}
    return decrypt_json(get_settings().master_key, connector.encrypted_credentials)


def _sqlalchemy_url_for_datastore(connector_slug: str, credentials: dict[str, Any]) -> str:
    if connector_slug == "postgres":
        if credentials.get("database_url"):
            return str(credentials["database_url"])
        required = ["host", "database", "user", "password"]
        if not all(credentials.get(key) for key in required):
            raise McpExecutionError(400, "Postgres requires database_url or host, database, user, and password.")
        host = credentials["host"]
        port = credentials.get("port") or "5432"
        user = quote_plus(credentials["user"])
        password = quote_plus(credentials["password"])
        database = credentials["database"]
        return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
    if connector_slug == "redshift":
        if credentials.get("database_url"):
            return str(credentials["database_url"])
        required = ["cluster_endpoint", "database", "user", "password"]
        if not all(credentials.get(key) for key in required):
            raise McpExecutionError(400, "Redshift requires database_url or cluster_endpoint, database, user, and password.")
        host, endpoint_port, endpoint_database = parse_redshift_endpoint(str(credentials["cluster_endpoint"]), str(credentials.get("port") or "5439"))
        port = str(credentials.get("port") or endpoint_port)
        user = quote_plus(credentials["user"])
        password = quote_plus(credentials["password"])
        database = credentials["database"] or endpoint_database
        return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
    if connector_slug == "mysql":
        required = ["host", "database", "user", "password"]
        if not all(credentials.get(key) for key in required):
            raise McpExecutionError(400, "MySQL requires host, database, user, and password.")
        host = credentials["host"]
        port = credentials.get("port") or "3306"
        user = quote_plus(credentials["user"])
        password = quote_plus(credentials["password"])
        database = credentials["database"]
        return f"mysql+aiomysql://{user}:{password}@{host}:{port}/{database}"
    if connector_slug == "trino":
        required = ["host", "catalog", "schema", "user"]
        if not all(credentials.get(key) for key in required):
            raise McpExecutionError(400, "Trino requires host, catalog, schema, and user.")
        host = credentials["host"]
        port = credentials.get("port") or "8080"
        user = quote_plus(credentials["user"])
        password = quote_plus(credentials["password"]) if credentials.get("password") else ""
        auth = f"{user}:{password}" if password else user
        catalog = quote_plus(credentials["catalog"])
        schema = quote_plus(credentials["schema"])
        return f"trino://{auth}@{host}:{port}/{catalog}/{schema}"
    raise McpExecutionError(404, f"Unsupported SQL datastore: {connector_slug}")


def _redshift_boto3_client(credentials: dict[str, Any]) -> Any:
    try:
        import boto3  # type: ignore
    except ImportError as exc:
        raise McpExecutionError(501, "Redshift cluster MCP tools require the optional boto3 package.") from exc
    kwargs: dict[str, Any] = {}
    if credentials.get("region_name") or credentials.get("region"):
        kwargs["region_name"] = credentials.get("region_name") or credentials.get("region")
    if credentials.get("aws_access_key_id"):
        kwargs["aws_access_key_id"] = credentials["aws_access_key_id"]
    if credentials.get("aws_secret_access_key"):
        kwargs["aws_secret_access_key"] = credentials["aws_secret_access_key"]
    if credentials.get("aws_session_token"):
        kwargs["aws_session_token"] = credentials["aws_session_token"]
    return boto3.client("redshift", **kwargs)


def _redshift_cluster_identifier(arguments: dict[str, Any], credentials: dict[str, Any]) -> str:
    value = str(arguments.get("cluster_identifier") or arguments.get("cluster_id") or credentials.get("cluster_identifier") or "")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9-]{0,62}", value):
        raise McpExecutionError(400, "cluster_identifier must be a valid Redshift cluster identifier.")
    return value


async def _airflow_tool(session: AsyncSession, tool_name: str, arguments: dict[str, Any], agent_id: str) -> dict[str, Any]:
    credentials = await _connector_credentials(session, "airflow")
    adapter = adapter_for("airflow")
    base_url = adapter.base_url(credentials)
    headers = adapter.headers(credentials)
    if not base_url:
        raise McpExecutionError(400, "Airflow base_url is required.")
    if tool_name in AIRFLOW_APPROVAL_TOOLS and not arguments.get("__approved"):
        return await _pending_mcp_approval(
            session,
            agent_id=agent_id,
            connector_slug="airflow",
            tool_name=tool_name,
            arguments=arguments,
            title=f"Approve Airflow {tool_name.removeprefix('write_').replace('_', ' ')}",
        )
    async with httpx.AsyncClient(timeout=20) as client:
        if tool_name == "read_list_dags":
            response = await client.get(f"{base_url}/api/v1/dags", headers=headers)
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "dags": payload.get("dags", []), "agent_id": agent_id}
        if tool_name == "read_get_run":
            dag_id = str(arguments.get("dag_id") or "")
            run_id = str(arguments.get("run_id") or "")
            if not dag_id:
                raise McpExecutionError(400, "dag_id is required.")
            dag_path = _url_segment(dag_id, field_name="dag_id")
            path = f"{base_url}/api/v1/dags/{dag_path}/dagRuns"
            if run_id:
                path = f"{path}/{_url_segment(run_id, field_name='run_id')}"
            response = await client.get(path, headers=headers)
            response.raise_for_status()
            return {"status": "ok", "run": response.json(), "agent_id": agent_id}
        if tool_name == "read_get_dag_source":
            dag_id = str(arguments.get("dag_id") or "")
            if not dag_id:
                raise McpExecutionError(400, "dag_id is required.")
            dag_path = _url_segment(dag_id, field_name="dag_id")
            response = await client.get(f"{base_url}/api/v1/dags/{dag_path}/source", headers=headers)
            if response.status_code >= 400 and dag_id in _GENERATED_AIRFLOW_DAGS:
                return {
                    "status": "ok",
                    "source": {
                        "dag_id": dag_id,
                        "source": _GENERATED_AIRFLOW_DAGS[dag_id].get("source") or "",
                    },
                    "agent_id": agent_id,
                }
            if response.status_code >= 400:
                details = await client.get(f"{base_url}/api/v1/dags/{dag_path}/details", headers=headers)
                if details.status_code < 400:
                    details_payload = details.json()
                    file_token = str(details_payload.get("file_token") or "")
                    if file_token:
                        source_response = await client.get(
                            f"{base_url}/api/v1/dagSources/{_url_segment(file_token, field_name='file_token')}",
                            headers=headers,
                        )
                        if source_response.status_code < 400:
                            return {
                                "status": "ok",
                                "source": {
                                    "dag_id": dag_id,
                                    "source": source_response.text,
                                    "fileloc": details_payload.get("fileloc"),
                                },
                                "agent_id": agent_id,
                            }
            response.raise_for_status()
            return {"status": "ok", "source": response.json(), "agent_id": agent_id}
        if tool_name == "read_get_task_logs":
            dag_id = str(arguments.get("dag_id") or "")
            run_id = str(arguments.get("run_id") or "")
            task_id = str(arguments.get("task_id") or "")
            try_number = int(arguments.get("try_number") or 1)
            if try_number < 1:
                raise McpExecutionError(400, "try_number must be greater than or equal to 1.")
            if not dag_id or not run_id or not task_id:
                raise McpExecutionError(400, "dag_id, run_id, and task_id are required.")
            response = await client.get(
                f"{base_url}/api/v1/dags/{_url_segment(dag_id, field_name='dag_id')}"
                f"/dagRuns/{_url_segment(run_id, field_name='run_id')}"
                f"/taskInstances/{_url_segment(task_id, field_name='task_id')}/logs/{try_number}",
                headers={**headers, "Accept": "text/plain"},
            )
            response.raise_for_status()
            return {"status": "ok", "logs": response.text[:50_000], "agent_id": agent_id}
        if tool_name == "read_list_task_instances":
            dag_id = str(arguments.get("dag_id") or "")
            run_id = str(arguments.get("run_id") or "")
            if not dag_id or not run_id:
                raise McpExecutionError(400, "dag_id and run_id are required.")
            response = await client.get(
                f"{base_url}/api/v1/dags/{_url_segment(dag_id, field_name='dag_id')}"
                f"/dagRuns/{_url_segment(run_id, field_name='run_id')}/taskInstances",
                headers=headers,
                params={"limit": _bounded_limit(arguments.get("limit"))},
            )
            response.raise_for_status()
            payload = response.json()
            return {
                "status": "ok",
                "task_instances": payload.get("task_instances", payload.get("taskInstances", [])),
                "total": payload.get("total_entries"),
                "agent_id": agent_id,
            }
        if tool_name == "read_list_dag_runs":
            dag_id = str(arguments.get("dag_id") or "")
            if not dag_id:
                raise McpExecutionError(400, "dag_id is required.")
            response = await client.get(
                f"{base_url}/api/v1/dags/{_url_segment(dag_id, field_name='dag_id')}/dagRuns",
                headers=headers,
                params=_since_params(arguments),
            )
            response.raise_for_status()
            payload = response.json()
            return {
                "status": "ok",
                "dag_runs": payload.get("dag_runs", payload.get("dagRuns", [])),
                "total": payload.get("total_entries"),
                "agent_id": agent_id,
            }
        if tool_name == "read_get_xcom":
            dag_id = str(arguments.get("dag_id") or "")
            run_id = str(arguments.get("run_id") or "")
            task_id = str(arguments.get("task_id") or "")
            key = str(arguments.get("key") or "")
            if not dag_id or not run_id or not task_id:
                raise McpExecutionError(400, "dag_id, run_id, and task_id are required.")
            path = (
                f"{base_url}/api/v1/dags/{_url_segment(dag_id, field_name='dag_id')}"
                f"/dagRuns/{_url_segment(run_id, field_name='run_id')}"
                f"/taskInstances/{_url_segment(task_id, field_name='task_id')}/xcomEntries"
            )
            if key:
                path = f"{path}/{_url_segment(key, field_name='key')}"
            response = await client.get(path, headers=headers)
            response.raise_for_status()
            return {"status": "ok", "xcom": response.json(), "agent_id": agent_id}
        if tool_name == "read_list_pools":
            response = await client.get(
                f"{base_url}/api/v1/pools",
                headers=headers,
                params={"limit": _bounded_limit(arguments.get("limit"))},
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "pools": payload.get("pools", []), "total": payload.get("total_entries"), "agent_id": agent_id}
        if tool_name == "read_get_pool":
            name = str(arguments.get("name") or arguments.get("pool_name") or "")
            response = await client.get(f"{base_url}/api/v1/pools/{_url_segment(name, field_name='name')}", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "pool": response.json(), "agent_id": agent_id}
        if tool_name == "read_list_variables":
            response = await client.get(
                f"{base_url}/api/v1/variables",
                headers=headers,
                params={"limit": _bounded_limit(arguments.get("limit"))},
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "variables": payload.get("variables", []), "total": payload.get("total_entries"), "agent_id": agent_id}
        if tool_name == "read_get_variable":
            key = str(arguments.get("key") or "")
            response = await client.get(f"{base_url}/api/v1/variables/{_url_segment(key, field_name='key')}", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "variable": response.json(), "agent_id": agent_id}
        if tool_name == "read_get_dag_dependencies":
            dag_id = str(arguments.get("dag_id") or "")
            if not dag_id:
                raise McpExecutionError(400, "dag_id is required.")
            response = await client.get(
                f"{base_url}/api/v1/dags/{_url_segment(dag_id, field_name='dag_id')}/details",
                headers=headers,
            )
            response.raise_for_status()
            payload = response.json()
            tasks = payload.get("tasks", [])
            dependencies = [
                {"task_id": task.get("task_id"), "downstream_task_ids": task.get("downstream_task_ids", [])}
                for task in tasks
            ]
            return {"status": "ok", "dag_id": dag_id, "dependencies": dependencies, "details": payload, "agent_id": agent_id}
        if tool_name == "read_get_import_errors":
            response = await client.get(
                f"{base_url}/api/v1/importErrors",
                headers=headers,
                params={"limit": _bounded_limit(arguments.get("limit"))},
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "import_errors": payload.get("import_errors", payload.get("importErrors", [])), "total": payload.get("total_entries"), "agent_id": agent_id}
        if tool_name == "write_trigger_dag":
            dag_id = str(arguments.get("dag_id") or "")
            if not dag_id:
                raise McpExecutionError(400, "dag_id is required.")
            response = await client.post(
                f"{base_url}/api/v1/dags/{_url_segment(dag_id, field_name='dag_id')}/dagRuns",
                headers=headers,
                json={"conf": arguments.get("conf") or {}},
            )
            response.raise_for_status()
            return {"status": "triggered", "dag_run": response.json(), "agent_id": agent_id}
        if tool_name == "write_create_dag":
            dag_id = str(arguments.get("dag_id") or arguments.get("dagId") or "")
            if not dag_id:
                raise McpExecutionError(400, "dag_id is required.")
            dag_payload = {
                "dag_id": dag_id,
                "schedule_interval": arguments.get("schedule_interval") or arguments.get("schedule") or "@daily",
                "owners": arguments.get("owners") or ["data"],
                "tags": arguments.get("tags") or ["dataclaw"],
                "source": arguments.get("source")
                or f"from airflow import DAG\n# generated by DataClaw\ndag_id = '{dag_id}'\n",
            }
            response = await client.post(
                f"{base_url}/api/v1/dags",
                headers=headers,
                json=dag_payload,
            )
            if response.status_code in {404, 405}:
                _GENERATED_AIRFLOW_DAGS[dag_id] = dag_payload
                return {"status": "created", "dag": dag_payload, "agent_id": agent_id}
            response.raise_for_status()
            return {"status": "created", "dag": response.json(), "agent_id": agent_id}
        if tool_name == "write_pause_dag":
            dag_id = str(arguments.get("dag_id") or arguments.get("dagId") or "")
            if not dag_id:
                raise McpExecutionError(400, "dag_id is required.")
            response = await client.patch(
                f"{base_url}/api/v1/dags/{_url_segment(dag_id, field_name='dag_id')}",
                headers=headers,
                json={"is_paused": bool(arguments.get("is_paused", arguments.get("paused", True)))},
            )
            response.raise_for_status()
            return {"status": "updated", "dag": response.json(), "agent_id": agent_id}
        if tool_name == "write_unpause_dag":
            dag_id = str(arguments.get("dag_id") or arguments.get("dagId") or "")
            if not dag_id:
                raise McpExecutionError(400, "dag_id is required.")
            response = await client.patch(
                f"{base_url}/api/v1/dags/{_url_segment(dag_id, field_name='dag_id')}",
                headers=headers,
                json={"is_paused": False},
            )
            response.raise_for_status()
            return {"status": "updated", "dag": response.json(), "agent_id": agent_id}
        if tool_name == "write_clear_task_instance":
            dag_id = str(arguments.get("dag_id") or "")
            run_id = str(arguments.get("run_id") or "")
            task_id = str(arguments.get("task_id") or "")
            if not dag_id or not run_id or not task_id:
                raise McpExecutionError(400, "dag_id, run_id, and task_id are required.")
            response = await client.post(
                f"{base_url}/api/v1/dags/{_url_segment(dag_id, field_name='dag_id')}/clearTaskInstances",
                headers=headers,
                json={
                    "dry_run": False,
                    "dag_run_id": run_id,
                    "task_ids": [task_id],
                    "only_failed": False,
                    "include_subdags": False,
                    "include_parentdag": False,
                    "reset_dag_runs": False,
                },
            )
            response.raise_for_status()
            return {"status": "cleared", "result": response.json(), "agent_id": agent_id}
        if tool_name in {"write_mark_task_success", "write_mark_task_failed"}:
            dag_id = str(arguments.get("dag_id") or "")
            run_id = str(arguments.get("run_id") or "")
            task_id = str(arguments.get("task_id") or "")
            if not dag_id or not run_id or not task_id:
                raise McpExecutionError(400, "dag_id, run_id, and task_id are required.")
            state = "success" if tool_name == "write_mark_task_success" else "failed"
            response = await client.patch(
                f"{base_url}/api/v1/dags/{_url_segment(dag_id, field_name='dag_id')}"
                f"/dagRuns/{_url_segment(run_id, field_name='run_id')}"
                f"/taskInstances/{_url_segment(task_id, field_name='task_id')}",
                headers=headers,
                json={"new_state": state, "dry_run": False},
            )
            response.raise_for_status()
            return {"status": "updated", "task_instance": response.json(), "agent_id": agent_id}
        if tool_name == "write_set_variable":
            key = str(arguments.get("key") or "")
            value = str(arguments.get("value") or "")
            body = {"key": key, "value": value, "description": arguments.get("description")}
            response = await client.patch(
                f"{base_url}/api/v1/variables/{_url_segment(key, field_name='key')}",
                headers=headers,
                json=body,
            )
            if response.status_code == 404:
                response = await client.post(f"{base_url}/api/v1/variables", headers=headers, json=body)
            response.raise_for_status()
            return {"status": "updated", "variable": response.json(), "agent_id": agent_id}
        if tool_name == "write_set_pool":
            name = str(arguments.get("name") or arguments.get("pool_name") or "")
            body = {
                "name": name,
                "slots": int(arguments.get("slots") or 1),
                "description": arguments.get("description"),
            }
            response = await client.patch(
                f"{base_url}/api/v1/pools/{_url_segment(name, field_name='name')}",
                headers=headers,
                json=body,
            )
            if response.status_code == 404:
                response = await client.post(f"{base_url}/api/v1/pools", headers=headers, json=body)
            response.raise_for_status()
            return {"status": "updated", "pool": response.json(), "agent_id": agent_id}
        if tool_name == "write_delete_dag":
            dag_id = str(arguments.get("dag_id") or "")
            if not dag_id:
                raise McpExecutionError(400, "dag_id is required.")
            response = await client.delete(f"{base_url}/api/v1/dags/{_url_segment(dag_id, field_name='dag_id')}", headers=headers)
            response.raise_for_status()
            return {"status": "deleted", "dag_id": dag_id, "agent_id": agent_id}
    raise McpExecutionError(404, f"Unsupported Airflow MCP tool: {tool_name}")


async def _airbyte_tool(session: AsyncSession, tool_name: str, arguments: dict[str, Any], agent_id: str) -> dict[str, Any]:
    credentials = await _connector_credentials(session, "airbyte")
    adapter = adapter_for("airbyte")
    base_url = adapter.base_url(credentials)
    headers = adapter.headers(credentials)
    async with httpx.AsyncClient(timeout=20) as client:
        if tool_name == "read_list_connections":
            workspace_id = await _airbyte_workspace_id(client, base_url, headers, credentials)
            response: httpx.Response
            if workspace_id:
                response = await client.post(
                    f"{base_url}/api/v1/connections/list",
                    headers=headers,
                    json={"workspaceId": workspace_id},
                )
            else:
                response = await client.get(f"{base_url}/v1/connections", headers=headers)
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "connections": _airbyte_rows(payload, "connections"), "agent_id": agent_id}
        if tool_name == "read_get_job_logs":
            job_id = str(arguments.get("job_id") or arguments.get("id") or "")
            if not job_id:
                raise McpExecutionError(400, "job_id is required.")
            job_int = _maybe_airbyte_int_id(job_id)
            response: httpx.Response | None = None
            if job_int is not None:
                response = await client.post(f"{base_url}/api/v1/jobs/get", headers=headers, json={"id": job_int})
            if response is None or response.status_code == 404:
                response = await client.get(f"{base_url}/v1/jobs/{_url_segment(job_id, field_name='job_id')}", headers=headers)
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "job": payload.get("job", payload), "logs": _airbyte_job_logs(payload), "agent_id": agent_id}
        if tool_name == "read_list_jobs":
            connection_id = str(arguments.get("connection_id") or arguments.get("connectionId") or "")
            if not connection_id:
                response = await client.post(
                    f"{base_url}/api/v1/jobs/list",
                    headers=headers,
                    json={
                        "configTypes": arguments.get("config_types") or arguments.get("configTypes") or ["sync", "reset"],
                        "pagination": {"pageSize": _bounded_limit(arguments.get("limit"))},
                    },
                )
                if response.status_code == 404:
                    response = await client.get(
                        f"{base_url}/v1/jobs",
                        headers=headers,
                        params={"limit": _bounded_limit(arguments.get("limit"))},
                    )
                response.raise_for_status()
                payload = response.json()
                return {"status": "ok", "jobs": _airbyte_rows(payload, "jobs"), "agent_id": agent_id}
            body: dict[str, Any] = {
                "configId": connection_id,
                "configTypes": arguments.get("config_types") or arguments.get("configTypes") or ["sync", "reset"],
                "pagination": {"pageSize": _bounded_limit(arguments.get("limit"))},
            }
            if connection_id:
                body["configId"] = connection_id
            response = await client.post(f"{base_url}/api/v1/jobs/list", headers=headers, json=body)
            if response.status_code == 404:
                response = await client.get(
                    f"{base_url}/v1/jobs",
                    headers=headers,
                    params={"connectionId": connection_id or None, "limit": _bounded_limit(arguments.get("limit"))},
                )
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "jobs": _airbyte_rows(payload, "jobs"), "agent_id": agent_id}
        if tool_name == "read_get_connection_state":
            connection_id = str(arguments.get("connection_id") or arguments.get("connectionId") or arguments.get("id") or "")
            response = await client.post(
                f"{base_url}/api/v1/state/get",
                headers=headers,
                json={"connectionId": connection_id},
            )
            if response.status_code == 404:
                response = await client.get(f"{base_url}/v1/connections/{_url_segment(connection_id, field_name='connection_id')}", headers=headers)
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "state": payload.get("state", payload.get("connectionState", payload)), "agent_id": agent_id}
        if tool_name in {"read_list_sources", "read_list_destinations"}:
            workspace_id = await _airbyte_workspace_id(client, base_url, headers, credentials)
            resource = "sources" if tool_name == "read_list_sources" else "destinations"
            if workspace_id:
                response = await client.post(
                    f"{base_url}/api/v1/{resource}/list",
                    headers=headers,
                    json={"workspaceId": workspace_id},
                )
            else:
                response = await client.get(f"{base_url}/v1/{resource}", headers=headers)
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", resource: _airbyte_rows(payload, resource), "agent_id": agent_id}
        if tool_name in {"read_get_source", "read_get_destination"}:
            resource = "source" if tool_name == "read_get_source" else "destination"
            resource_id = str(arguments.get(f"{resource}_id") or arguments.get(f"{resource}Id") or arguments.get("id") or "")
            endpoint = "sources" if resource == "source" else "destinations"
            response = await client.post(
                f"{base_url}/api/v1/{endpoint}/get",
                headers=headers,
                json={f"{resource}Id": resource_id},
            )
            if response.status_code == 404:
                response = await client.get(f"{base_url}/v1/{endpoint}/{_url_segment(resource_id, field_name=f'{resource}_id')}", headers=headers)
            response.raise_for_status()
            return {"status": "ok", resource: response.json(), "agent_id": agent_id}
        if tool_name == "read_get_workspace":
            workspace_id = str(arguments.get("workspace_id") or arguments.get("workspaceId") or credentials.get("workspace_id") or credentials.get("workspaceId") or "")
            if workspace_id:
                response = await client.post(f"{base_url}/api/v1/workspaces/get", headers=headers, json={"workspaceId": workspace_id})
                if response.status_code != 404:
                    response.raise_for_status()
                    return {"status": "ok", "workspace": response.json(), "agent_id": agent_id}
            response = await client.post(f"{base_url}/api/v1/workspaces/list", headers=headers, json={})
            response.raise_for_status()
            payload = response.json()
            workspaces = _airbyte_rows(payload, "workspaces")
            return {"status": "ok", "workspace": workspaces[0] if workspaces else None, "agent_id": agent_id}
        if tool_name == "read_get_connection_schema":
            connection_id = str(arguments.get("connection_id") or arguments.get("connectionId") or arguments.get("id") or "")
            response = await client.post(
                f"{base_url}/api/v1/connections/get",
                headers=headers,
                json={"connectionId": connection_id},
            )
            if response.status_code == 404:
                response = await client.get(f"{base_url}/v1/connections/{_url_segment(connection_id, field_name='connection_id')}", headers=headers)
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "connection": payload, "schema": payload.get("syncCatalog") or payload.get("catalog"), "agent_id": agent_id}
        if tool_name in {"write_create_connection", "write_update_connection"}:
            connection_id = str(arguments.get("connection_id") or arguments.get("connectionId") or arguments.get("id") or "")
            config = arguments.get("config") if isinstance(arguments.get("config"), dict) else {}
            body = dict(config)
            if tool_name == "write_create_connection":
                source_id = str(arguments.get("source_id") or arguments.get("sourceId") or body.get("sourceId") or "")
                destination_id = str(arguments.get("destination_id") or arguments.get("destinationId") or body.get("destinationId") or "")
                if not source_id or not destination_id:
                    raise McpExecutionError(400, "source_id and destination_id are required.")
                body.setdefault("sourceId", source_id)
                body.setdefault("destinationId", destination_id)
                response = await client.post(f"{base_url}/api/v1/connections/create", headers=headers, json=body)
                if response.status_code == 404:
                    response = await client.post(f"{base_url}/v1/connections", headers=headers, json=body)
                response.raise_for_status()
                return {"status": "created", "connection": response.json(), "agent_id": agent_id}
            if not connection_id:
                raise McpExecutionError(400, "connection_id is required.")
            body["connectionId"] = connection_id
            response = await client.post(f"{base_url}/api/v1/connections/update", headers=headers, json=body)
            if response.status_code == 404:
                body.pop("connectionId", None)
                response = await client.patch(
                    f"{base_url}/v1/connections/{_url_segment(connection_id, field_name='connection_id')}",
                    headers=headers,
                    json=body,
                )
            response.raise_for_status()
            return {"status": "updated", "connection": response.json(), "agent_id": agent_id}
        if tool_name == "write_trigger_sync":
            connection_id = str(arguments.get("connection_id") or arguments.get("connectionId") or "")
            if not connection_id:
                raise McpExecutionError(400, "connection_id is required.")
            response = await client.post(
                f"{base_url}/v1/jobs",
                headers=headers,
                json={"connectionId": connection_id, "jobType": "sync"},
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "triggered", "job": payload.get("job", payload), "agent_id": agent_id}
        if tool_name == "write_reset_connection":
            connection_id = str(arguments.get("connection_id") or arguments.get("connectionId") or "")
            if not connection_id:
                raise McpExecutionError(400, "connection_id is required.")
            response = await client.post(f"{base_url}/api/v1/connections/reset", headers=headers, json={"connectionId": connection_id})
            if response.status_code == 404:
                response = await client.post(f"{base_url}/v1/jobs", headers=headers, json={"connectionId": connection_id, "jobType": "reset"})
            response.raise_for_status()
            payload = response.json()
            return {"status": "triggered", "job": payload.get("job", payload), "agent_id": agent_id}
        if tool_name == "write_cancel_job":
            job_id = str(arguments.get("job_id") or arguments.get("id") or "")
            job_int = _maybe_airbyte_int_id(job_id)
            response = (
                await client.post(f"{base_url}/api/v1/jobs/cancel", headers=headers, json={"id": job_int})
                if job_int is not None
                else httpx.Response(404)
            )
            if response.status_code == 404:
                response = await client.delete(f"{base_url}/v1/jobs/{_url_segment(job_id, field_name='job_id')}", headers=headers)
            response.raise_for_status()
            payload = response.json()
            return {"status": "cancelled", "job": payload.get("job", payload), "agent_id": agent_id}
        if tool_name in {"write_disable_connection", "write_enable_connection"}:
            connection_id = str(arguments.get("connection_id") or arguments.get("connectionId") or arguments.get("id") or "")
            active = tool_name == "write_enable_connection"
            response = await client.post(
                f"{base_url}/api/v1/connections/update",
                headers=headers,
                json={"connectionId": connection_id, "status": "active" if active else "inactive"},
            )
            if response.status_code == 404:
                response = await client.patch(
                    f"{base_url}/v1/connections/{_url_segment(connection_id, field_name='connection_id')}",
                    headers=headers,
                    json={"status": "active" if active else "inactive"},
                )
            response.raise_for_status()
            return {"status": "updated", "connection": response.json(), "agent_id": agent_id}
    raise McpExecutionError(404, f"Unsupported Airbyte MCP tool: {tool_name}")


async def _prefect_tool(session: AsyncSession, tool_name: str, arguments: dict[str, Any], agent_id: str) -> dict[str, Any]:
    credentials = await _connector_credentials(session, "prefect")
    adapter = adapter_for("prefect")
    base_url = adapter.base_url(credentials)
    headers = adapter.headers(credentials)
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        if tool_name == "read_list_flows":
            response = await client.post(f"{base_url}/api/flows/filter", headers=headers, json={"limit": _bounded_limit(arguments.get("limit"), default=50)})
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "flows": _data_list(payload), "agent_id": agent_id}
        if tool_name == "read_get_run":
            run_id = str(arguments.get("run_id") or "")
            if not run_id:
                raise McpExecutionError(400, "run_id is required.")
            response = await client.get(f"{base_url}/api/flow_runs/{_url_segment(run_id, field_name='run_id')}", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "run": response.json(), "agent_id": agent_id}
        if tool_name in {"read_get_run_logs", "read_get_task_logs"}:
            run_id = str(arguments.get("run_id") or arguments.get("flow_run_id") or "")
            task_run_id = str(arguments.get("task_run_id") or "")
            if tool_name == "read_get_run_logs" and not run_id:
                raise McpExecutionError(400, "run_id is required.")
            if tool_name == "read_get_task_logs" and not run_id and not task_run_id:
                raise McpExecutionError(400, "flow_run_id or task_run_id is required.")
            logs_filter: dict[str, Any] = {}
            if run_id:
                logs_filter["flow_run_id"] = {"any_": [run_id]}
            if task_run_id:
                logs_filter["task_run_id"] = {"any_": [task_run_id]}
            response = await client.post(
                f"{base_url}/api/logs/filter",
                headers=headers,
                json={"logs": logs_filter, "limit": _bounded_limit(arguments.get("limit")), "sort": "TIMESTAMP_DESC"},
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "logs": _data_list(payload), "agent_id": agent_id}
        if tool_name == "read_list_flow_runs":
            flow_id = str(arguments.get("flow_id") or arguments.get("flow") or "")
            body: dict[str, Any] = {"limit": _bounded_limit(arguments.get("limit")), "sort": "START_TIME_DESC"}
            if flow_id:
                body["flows"] = {"id": {"any_": [flow_id]}}
            since = arguments.get("since")
            if since:
                body["flow_runs"] = {"start_time": {"after_": str(since)}}
            response = await client.post(f"{base_url}/api/flow_runs/filter", headers=headers, json=body)
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "flow_runs": _data_list(payload), "agent_id": agent_id}
        if tool_name == "read_list_deployments":
            response = await client.post(
                f"{base_url}/api/deployments/filter",
                headers=headers,
                json={"limit": _bounded_limit(arguments.get("limit"))},
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "deployments": _data_list(payload), "agent_id": agent_id}
        if tool_name == "read_get_deployment":
            deployment_id = str(arguments.get("deployment_id") or arguments.get("id") or "")
            response = await client.get(f"{base_url}/api/deployments/{_url_segment(deployment_id, field_name='deployment_id')}", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "deployment": response.json(), "agent_id": agent_id}
        if tool_name == "read_get_task_run":
            task_run_id = str(arguments.get("task_run_id") or arguments.get("id") or "")
            response = await client.get(f"{base_url}/api/task_runs/{_url_segment(task_run_id, field_name='task_run_id')}", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "task_run": response.json(), "agent_id": agent_id}
        if tool_name == "read_list_work_pools":
            response = await client.post(
                f"{base_url}/api/work_pools/filter",
                headers=headers,
                json={"limit": _bounded_limit(arguments.get("limit"))},
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "work_pools": _data_list(payload), "agent_id": agent_id}
        if tool_name == "read_get_block":
            block_id = str(arguments.get("block_id") or arguments.get("id") or "")
            block_name = str(arguments.get("name") or arguments.get("block_name") or "")
            block_type_slug = str(arguments.get("block_type_slug") or arguments.get("block_type") or "")
            if block_id:
                response = await client.get(f"{base_url}/api/blocks/{_url_segment(block_id, field_name='block_id')}", headers=headers)
            else:
                if not block_name:
                    raise McpExecutionError(400, "block_id or name is required.")
                if block_type_slug:
                    response = await client.get(
                        f"{base_url}/api/block_types/slug/{_url_segment(block_type_slug, field_name='block_type_slug')}/block_documents/name/{_url_segment(block_name, field_name='name')}",
                        headers=headers,
                    )
                else:
                    response = await client.post(
                        f"{base_url}/api/block_documents/filter",
                        headers=headers,
                        json={"block_documents": {"name": {"any_": [block_name]}}, "limit": 1},
                    )
            response.raise_for_status()
            payload = response.json()
            block = _data_list(payload)[0] if isinstance(payload, list) and payload else payload
            return {"status": "ok", "block": block, "agent_id": agent_id}
        if tool_name == "read_get_concurrency_limit":
            tag = str(arguments.get("tag") or arguments.get("name") or "")
            if not tag:
                raise McpExecutionError(400, "tag is required.")
            response = await client.get(f"{base_url}/api/concurrency_limits/tag/{_url_segment(tag, field_name='tag')}", headers=headers)
            if response.status_code == 404:
                response = await client.post(
                    f"{base_url}/api/concurrency_limits/filter",
                    headers=headers,
                    json={"tags": {"name": {"any_": [tag]}}, "limit": 1},
                )
            response.raise_for_status()
            payload = response.json()
            limit = _data_list(payload)[0] if isinstance(payload, list) and payload else payload
            return {"status": "ok", "concurrency_limit": limit, "agent_id": agent_id}
        if tool_name == "read_list_artifacts":
            flow_run_id = str(arguments.get("flow_run_id") or "")
            body: dict[str, Any] = {"limit": _bounded_limit(arguments.get("limit")), "sort": "UPDATED_DESC"}
            if flow_run_id:
                body["flow_runs"] = {"id": {"any_": [flow_run_id]}}
            response = await client.post(f"{base_url}/api/artifacts/filter", headers=headers, json=body)
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "artifacts": _data_list(payload), "agent_id": agent_id}
        if tool_name == "write_trigger_flow_run":
            deployment_id = str(arguments.get("deployment_id") or arguments.get("deploymentId") or "")
            if not deployment_id:
                raise McpExecutionError(400, "deployment_id is required.")
            response = await client.post(
                f"{base_url}/api/deployments/{_url_segment(deployment_id, field_name='deployment_id')}/create_flow_run",
                headers=headers,
                json={"name": arguments.get("name"), "parameters": arguments.get("parameters") or {}},
            )
            response.raise_for_status()
            return {"status": "triggered", "run": response.json(), "agent_id": agent_id}
        if tool_name == "write_create_deployment":
            response = await client.post(
                f"{base_url}/api/deployments",
                headers=headers,
                json={
                    "name": arguments.get("name") or "DataClaw deployment",
                    "flow_id": arguments.get("flow_id") or arguments.get("flowId") or "flow-orders",
                    "entrypoint": arguments.get("entrypoint"),
                },
            )
            response.raise_for_status()
            return {"status": "created", "deployment": response.json(), "agent_id": agent_id}
        if tool_name in {"write_pause_deployment", "write_resume_deployment"}:
            deployment_id = str(arguments.get("deployment_id") or arguments.get("id") or "")
            paused = tool_name == "write_pause_deployment"
            response = await client.patch(
                f"{base_url}/api/deployments/{_url_segment(deployment_id, field_name='deployment_id')}",
                headers=headers,
                json={"paused": paused},
            )
            if response.status_code == 422:
                response = await client.patch(
                    f"{base_url}/api/deployments/{_url_segment(deployment_id, field_name='deployment_id')}",
                    headers=headers,
                    json={"is_schedule_active": not paused},
                )
            response.raise_for_status()
            deployment = response.json() if response.content else None
            return {
                "status": "updated",
                "deployment_id": deployment_id,
                "paused": paused,
                "deployment": deployment,
                "agent_id": agent_id,
            }
        if tool_name == "write_cancel_flow_run":
            run_id = str(arguments.get("run_id") or arguments.get("id") or "")
            response = await client.post(
                f"{base_url}/api/flow_runs/{_url_segment(run_id, field_name='run_id')}/set_state",
                headers=headers,
                json={"state": {"type": "CANCELLING", "name": "Cancelling"}},
            )
            response.raise_for_status()
            return {"status": "cancelled", "run": response.json(), "agent_id": agent_id}
        if tool_name == "write_set_block":
            block_id = str(arguments.get("block_id") or arguments.get("id") or "")
            block_name = str(arguments.get("name") or arguments.get("block_name") or "")
            data = arguments.get("data") if isinstance(arguments.get("data"), dict) else {}
            if block_id:
                response = await client.patch(
                    f"{base_url}/api/blocks/{_url_segment(block_id, field_name='block_id')}",
                    headers=headers,
                    json={"data": data},
                )
            else:
                if not block_name:
                    raise McpExecutionError(400, "block_id or name is required.")
                response = await client.post(
                    f"{base_url}/api/block_documents",
                    headers=headers,
                    json={"name": block_name, "data": data, "block_type_id": arguments.get("block_type_id")},
                )
            response.raise_for_status()
            return {"status": "updated", "block": response.json() if response.content else None, "agent_id": agent_id}
        if tool_name == "write_set_concurrency_limit":
            tag = str(arguments.get("tag") or arguments.get("name") or "")
            limit = arguments.get("limit")
            if not tag or limit is None:
                raise McpExecutionError(400, "tag and limit are required.")
            response = await client.post(
                f"{base_url}/api/concurrency_limits/tag/{_url_segment(tag, field_name='tag')}/reset",
                headers=headers,
                json={"limit": int(limit)},
            )
            if response.status_code == 404:
                response = await client.post(
                    f"{base_url}/api/concurrency_limits",
                    headers=headers,
                    json={"tag": tag, "concurrency_limit": int(limit)},
                )
            response.raise_for_status()
            return {"status": "updated", "concurrency_limit": response.json() if response.content else None, "agent_id": agent_id}
        if tool_name == "write_delete_deployment":
            deployment_id = str(arguments.get("deployment_id") or arguments.get("id") or "")
            response = await client.delete(
                f"{base_url}/api/deployments/{_url_segment(deployment_id, field_name='deployment_id')}",
                headers=headers,
            )
            response.raise_for_status()
            return {"status": "deleted", "deployment_id": deployment_id, "agent_id": agent_id}
    raise McpExecutionError(404, f"Unsupported Prefect MCP tool: {tool_name}")


async def _dagster_tool(session: AsyncSession, tool_name: str, arguments: dict[str, Any], agent_id: str) -> dict[str, Any]:
    credentials = await _connector_credentials(session, "dagster")
    adapter = adapter_for("dagster")
    base_url = adapter.base_url(credentials)
    headers = adapter.headers(credentials)
    async with httpx.AsyncClient(timeout=20) as client:
        if tool_name == "read_list_assets":
            payload = await _graphql(
                client,
                base_url,
                headers,
                "{ assetsOrError { __typename ... on AssetConnection { nodes { id key { path } } } } }",
            )
            assets = payload.get("data", {}).get("assetsOrError", {}).get("nodes", [])
            return {"status": "ok", "assets": assets, "agent_id": agent_id}
        if tool_name == "read_get_run":
            run_id = str(arguments.get("run_id") or "")
            if not run_id:
                raise McpExecutionError(400, "run_id is required.")
            payload = await _graphql(
                client,
                base_url,
                headers,
                "query Run($runId: ID!) { runOrError(runId: $runId) { __typename ... on Run { runId status pipelineName startTime endTime } } }",
                {"runId": run_id},
            )
            return {"status": "ok", "run": payload.get("data", {}).get("runOrError", {}), "agent_id": agent_id}
        if tool_name in {"read_get_run_logs", "read_get_event_logs", "read_get_run_steps"}:
            run_id = str(arguments.get("run_id") or "")
            if not run_id:
                raise McpExecutionError(400, "run_id is required.")
            payload = await _graphql(
                client,
                base_url,
                headers,
                "query RunEvents($runId: ID!, $limit: Int) { runOrError(runId: $runId) { __typename ... on Run { runId status stepKeysToExecute } } logsForRun(runId: $runId, limit: $limit) { __typename ... on EventConnection { events { __typename ... on MessageEvent { message timestamp level } ... on StepEvent { stepKey } } } } }",
                {"runId": run_id, "limit": _bounded_limit(arguments.get("limit"))},
            )
            run = payload.get("data", {}).get("runOrError", {})
            logs = payload.get("data", {}).get("logsForRun", {})
            events = logs.get("events", []) if isinstance(logs, dict) else []
            if tool_name == "read_get_run_steps":
                return {"status": "ok", "run": run, "steps": run.get("stepKeysToExecute", []), "events": events, "agent_id": agent_id}
            return {"status": "ok", "run": run, "logs": events, "events": events, "agent_id": agent_id}
        if tool_name == "read_get_asset_materializations":
            asset_key = arguments.get("asset_key") or arguments.get("asset") or []
            if isinstance(asset_key, str):
                asset_key = asset_key.split(".")
            payload = await _graphql(
                client,
                base_url,
                headers,
                "query AssetMaterializations($assetKey: [String!]!, $limit: Int) { assetNodeOrError(assetKey: {path: $assetKey}) { __typename ... on AssetNode { assetKey { path } assetMaterializations(limit: $limit) { timestamp runId metadataEntries { label description } } } } }",
                {"assetKey": asset_key, "limit": _bounded_limit(arguments.get("limit"))},
            )
            node = payload.get("data", {}).get("assetNodeOrError", {})
            return {"status": "ok", "asset": node, "materializations": node.get("assetMaterializations", []) if isinstance(node, dict) else [], "agent_id": agent_id}
        if tool_name == "read_list_jobs":
            payload = await _graphql(
                client,
                base_url,
                headers,
                "{ repositoriesOrError { __typename ... on RepositoryConnection { nodes { name pipelines { name description modes { name } } } } } }",
            )
            repositories = payload.get("data", {}).get("repositoriesOrError", {}).get("nodes", [])
            jobs = [job for repository in repositories for job in repository.get("pipelines", [])]
            return {"status": "ok", "jobs": jobs, "repositories": repositories, "agent_id": agent_id}
        if tool_name == "read_list_partitions":
            asset_key = arguments.get("asset_key") or arguments.get("asset") or []
            if isinstance(asset_key, str):
                asset_key = asset_key.split(".")
            payload = await _graphql(
                client,
                base_url,
                headers,
                "query AssetPartitions($assetKey: [String!]!) { assetNodeOrError(assetKey: {path: $assetKey}) { __typename ... on AssetNode { partitionKeys } } }",
                {"assetKey": asset_key},
            )
            node = payload.get("data", {}).get("assetNodeOrError", {})
            return {"status": "ok", "partitions": node.get("partitionKeys", []) if isinstance(node, dict) else [], "agent_id": agent_id}
        if tool_name == "read_get_asset_checks":
            asset_key = arguments.get("asset_key") or arguments.get("asset") or []
            if isinstance(asset_key, str):
                asset_key = asset_key.split(".")
            payload = await _graphql(
                client,
                base_url,
                headers,
                "query AssetChecks($assetKey: [String!]!) { assetNodeOrError(assetKey: {path: $assetKey}) { __typename ... on AssetNode { assetChecksOrError { __typename ... on AssetChecks { checks { name executionForLatestMaterialization { status } } } } } } }",
                {"assetKey": asset_key},
            )
            checks = payload.get("data", {}).get("assetNodeOrError", {}).get("assetChecksOrError", {})
            return {"status": "ok", "checks": checks.get("checks", []) if isinstance(checks, dict) else [], "agent_id": agent_id}
        if tool_name in {"read_list_sensors", "read_list_schedules"}:
            key = "sensors" if tool_name == "read_list_sensors" else "schedules"
            payload = await _graphql(
                client,
                base_url,
                headers,
                "{ repositoriesOrError { __typename ... on RepositoryConnection { nodes { name sensors { name sensorState { status } } schedules { name scheduleState { status } } } } } }",
            )
            repositories = payload.get("data", {}).get("repositoriesOrError", {}).get("nodes", [])
            rows = [row for repository in repositories for row in repository.get(key, [])]
            return {"status": "ok", key: rows, "repositories": repositories, "agent_id": agent_id}
        if tool_name in {"read_get_sensor_state", "read_get_schedule_state"}:
            name = str(arguments.get("name") or arguments.get("sensor") or arguments.get("schedule") or "")
            if not name:
                raise McpExecutionError(400, "name is required.")
            key = "sensorState" if tool_name == "read_get_sensor_state" else "scheduleState"
            payload = await _graphql(
                client,
                base_url,
                headers,
                "query InstigatorState($selector: InstigationSelector!) { instigationStateOrError(instigationSelector: $selector) { __typename ... on InstigationState { id name status } } }",
                {"selector": _dagster_instigation_selector(credentials, arguments, name)},
            )
            return {"status": "ok", key: payload.get("data", {}).get("instigationStateOrError", {}), "agent_id": agent_id}
        if tool_name in {"write_materialize_asset", "write_trigger_job"}:
            selector = _dagster_pipeline_selector(credentials, arguments)
            payload = await _graphql(
                client,
                base_url,
                headers,
                "mutation Launch($executionParams: ExecutionParams!) { launchPipelineExecution(executionParams: $executionParams) { __typename ... on LaunchRunSuccess { run { runId status } } } }",
                {"executionParams": {"selector": selector, "runConfigData": arguments.get("run_config") or {}, "mode": arguments.get("mode") or "default"}},
            )
            launch = payload.get("data", {}).get("launchPipelineExecution", {})
            run = launch.get("run", launch)
            if isinstance(run, dict) and not run.get("status"):
                run = {**run, "status": "STARTED"}
            return {"status": "triggered", "run": run, "agent_id": agent_id}
        if tool_name == "write_backfill_partitions":
            asset_key = arguments.get("asset_key") or arguments.get("asset") or []
            if isinstance(asset_key, str):
                asset_key = asset_key.split(".")
            partitions = arguments.get("partitions") or []
            if not asset_key or not isinstance(partitions, list) or not partitions:
                raise McpExecutionError(400, "asset_key and partitions are required.")
            repository_name, location_name = _dagster_repository_context(credentials, arguments)
            payload = await _graphql(
                client,
                base_url,
                headers,
                "mutation Backfill($backfillParams: LaunchBackfillParams!) { launchPartitionBackfill(backfillParams: $backfillParams) { __typename ... on LaunchBackfillSuccess { backfillId launchedRunIds } } }",
                {
                    "backfillParams": {
                        "selector": {
                            "repositoryName": repository_name,
                            "repositoryLocationName": location_name,
                            "assetSelection": [{"path": asset_key}],
                        },
                        "partitionNames": [str(partition) for partition in partitions],
                        "runConfigData": arguments.get("run_config") or {},
                        "tags": arguments.get("tags") or {},
                    }
                },
            )
            backfill = payload.get("data", {}).get("launchPartitionBackfill", {})
            return {"status": "triggered", "backfill": backfill, "agent_id": agent_id}
        if tool_name == "write_terminate_run":
            run_id = str(arguments.get("run_id") or "")
            if not run_id:
                raise McpExecutionError(400, "run_id is required.")
            payload = await _graphql(
                client,
                base_url,
                headers,
                "mutation Terminate($runId: String!) { terminateRun(runId: $runId) { __typename ... on TerminateRunSuccess { run { runId status } } } }",
                {"runId": run_id},
            )
            return {"status": "terminated", "run": payload.get("data", {}).get("terminateRun", {}), "agent_id": agent_id}
        if tool_name in {"write_launch_sensor", "write_start_schedule", "write_stop_schedule"}:
            name = str(arguments.get("name") or arguments.get("sensor") or arguments.get("schedule") or "")
            if not name:
                raise McpExecutionError(400, "name is required.")
            mutation = {
                "write_launch_sensor": "mutation StartSensor($selector: SensorSelector!) { startSensor(sensorSelector: $selector) { __typename ... on Sensor { name } } }",
                "write_start_schedule": "mutation StartSchedule($selector: ScheduleSelector!) { startSchedule(scheduleSelector: $selector) { __typename ... on Schedule { name } } }",
                "write_stop_schedule": "mutation StopSchedule($selector: ScheduleSelector!) { stopRunningSchedule(scheduleSelector: $selector) { __typename ... on Schedule { name } } }",
            }[tool_name]
            base_selector = _dagster_instigation_selector(credentials, arguments, name)
            if tool_name == "write_launch_sensor":
                selector = {
                    "repositoryName": base_selector["repositoryName"],
                    "repositoryLocationName": base_selector["repositoryLocationName"],
                    "sensorName": base_selector["name"],
                }
            else:
                selector = {
                    "repositoryName": base_selector["repositoryName"],
                    "repositoryLocationName": base_selector["repositoryLocationName"],
                    "scheduleName": base_selector["name"],
                }
            payload = await _graphql(client, base_url, headers, mutation, {"selector": selector})
            return {"status": "updated", "result": payload.get("data", {}), "agent_id": agent_id}
    raise McpExecutionError(404, f"Unsupported Dagster MCP tool: {tool_name}")


async def _fivetran_tool(session: AsyncSession, tool_name: str, arguments: dict[str, Any], agent_id: str) -> dict[str, Any]:
    credentials = await _connector_credentials(session, "fivetran")
    adapter = adapter_for("fivetran")
    base_url = adapter.base_url(credentials)
    headers = adapter.headers(credentials)
    async with httpx.AsyncClient(timeout=20) as client:
        if tool_name == "read_list_connectors":
            return {"status": "ok", "connectors": await _fivetran_list_connections(client, base_url, headers), "agent_id": agent_id}
        if tool_name == "read_get_connector_logs":
            connector_id = str(arguments.get("connector_id") or arguments.get("connectorId") or "")
            if not connector_id:
                raise McpExecutionError(400, "connector_id is required.")
            connector = await _fivetran_get_connection(client, base_url, headers, connector_id)
            return {
                "status": "ok",
                "logs": _fivetran_connector_status_events(connector),
                "source": "connector_status",
                "detail": "Fivetran REST API does not expose synchronous log lines; returned connector status tasks, warnings, and sync timestamps.",
                "agent_id": agent_id,
            }
        if tool_name == "read_get_connector_status":
            connector_id = str(arguments.get("connector_id") or arguments.get("connectorId") or "")
            if not connector_id:
                raise McpExecutionError(400, "connector_id is required.")
            return {"status": "ok", "connector": await _fivetran_get_connection(client, base_url, headers, connector_id), "agent_id": agent_id}
        if tool_name == "read_get_connector_schema":
            connector_id = str(arguments.get("connector_id") or arguments.get("connectorId") or "")
            if not connector_id:
                raise McpExecutionError(400, "connector_id is required.")
            connector_path = _url_segment(connector_id, field_name="connector_id")
            payload = await _fivetran_get_json(client, base_url, headers, f"/v1/connections/{connector_path}/schemas", f"/v1/connectors/{connector_path}/schemas")
            return {"status": "ok", "schema": payload.get("data", payload), "agent_id": agent_id}
        if tool_name == "read_list_destinations":
            payload = await _fivetran_get_json(client, base_url, headers, "/v1/destinations", "/v1/groups")
            return {"status": "ok", "destinations": _fivetran_items(payload), "agent_id": agent_id}
        if tool_name == "read_get_destination":
            destination_id = str(arguments.get("destination_id") or arguments.get("destinationId") or arguments.get("id") or "")
            if not destination_id:
                raise McpExecutionError(400, "destination_id is required.")
            destination_path = _url_segment(destination_id, field_name="destination_id")
            payload = await _fivetran_get_json(client, base_url, headers, f"/v1/destinations/{destination_path}", f"/v1/groups/{destination_path}")
            return {"status": "ok", "destination": payload.get("data", payload), "agent_id": agent_id}
        if tool_name == "read_get_metadata":
            connector_id = str(arguments.get("connector_id") or arguments.get("connectorId") or "")
            if not connector_id:
                raise McpExecutionError(400, "connector_id is required.")
            connector_path = _url_segment(connector_id, field_name="connector_id")
            payload = await _fivetran_get_json(
                client,
                base_url,
                headers,
                f"/v1/metadata/connectors/{connector_path}",
                f"/v1/connections/{connector_path}/schemas",
                f"/v1/connectors/{connector_path}/schemas",
            )
            return {"status": "ok", "metadata": payload.get("data", payload), "agent_id": agent_id}
        if tool_name == "read_get_data_volume":
            connector_id = str(arguments.get("connector_id") or arguments.get("connectorId") or "")
            if not connector_id:
                raise McpExecutionError(400, "connector_id is required.")
            params: dict[str, Any] = {}
            if arguments.get("since"):
                params["since"] = str(arguments["since"])
            response = await client.get(
                f"{base_url}/v1/connectors/{_url_segment(connector_id, field_name='connector_id')}/usage",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "data_volume": payload.get("data", payload), "agent_id": agent_id}
        if tool_name == "read_get_sync_history":
            connector_id = str(arguments.get("connector_id") or arguments.get("connectorId") or "")
            if not connector_id:
                raise McpExecutionError(400, "connector_id is required.")
            connector = await _fivetran_get_connection(client, base_url, headers, connector_id)
            return {
                "status": "ok",
                "sync_history": _fivetran_connector_status_events(connector),
                "source": "connector_status",
                "agent_id": agent_id,
            }
        if tool_name == "write_trigger_sync":
            connector_id = str(arguments.get("connector_id") or arguments.get("connectorId") or "")
            if not connector_id:
                raise McpExecutionError(400, "connector_id is required.")
            connector_path = _url_segment(connector_id, field_name="connector_id")
            response = await client.post(f"{base_url}/v1/connectors/{connector_path}/force", headers=headers)
            response.raise_for_status()
            return {"status": "triggered", "result": response.json(), "agent_id": agent_id}
        if tool_name in {"write_pause_connector", "write_resume_connector"}:
            connector_id = str(arguments.get("connector_id") or arguments.get("connectorId") or "")
            paused = tool_name == "write_pause_connector"
            if not connector_id:
                raise McpExecutionError(400, "connector_id is required.")
            connector_path = _url_segment(connector_id, field_name="connector_id")
            response = await client.patch(
                f"{base_url}/v1/connectors/{connector_path}",
                headers=headers,
                json={"paused": paused},
            )
            response.raise_for_status()
            return {"status": "updated", "result": response.json(), "agent_id": agent_id}
        if tool_name == "write_resync_table":
            connector_id = str(arguments.get("connector_id") or arguments.get("connectorId") or "")
            schema = str(arguments.get("schema") or "")
            table = str(arguments.get("table") or "")
            if not connector_id or not schema or not table:
                raise McpExecutionError(400, "connector_id, schema, and table are required.")
            response = await client.post(
                f"{base_url}/v1/connectors/{_url_segment(connector_id, field_name='connector_id')}/schemas/{_url_segment(schema, field_name='schema')}/tables/{_url_segment(table, field_name='table')}/resync",
                headers=headers,
            )
            response.raise_for_status()
            return {"status": "triggered", "result": response.json() if response.content else None, "agent_id": agent_id}
        if tool_name == "write_modify_connector_schema":
            connector_id = str(arguments.get("connector_id") or arguments.get("connectorId") or arguments.get("id") or "")
            config = arguments.get("config") if isinstance(arguments.get("config"), dict) else None
            if not connector_id or config is None:
                raise McpExecutionError(400, "connector_id and config are required.")
            response = await client.patch(
                f"{base_url}/v1/connectors/{_url_segment(connector_id, field_name='connector_id')}/schemas",
                headers=headers,
                json=config,
            )
            response.raise_for_status()
            return {"status": "updated", "schema": response.json() if response.content else None, "agent_id": agent_id}
        if tool_name == "write_delete_connector":
            connector_id = str(arguments.get("connector_id") or arguments.get("connectorId") or "")
            if not connector_id:
                raise McpExecutionError(400, "connector_id is required.")
            response = await client.delete(f"{base_url}/v1/connectors/{_url_segment(connector_id, field_name='connector_id')}", headers=headers)
            response.raise_for_status()
            return {"status": "deleted", "connector_id": connector_id, "agent_id": agent_id}
    raise McpExecutionError(404, f"Unsupported Fivetran MCP tool: {tool_name}")


def _notion_rich_text(content: str) -> list[dict[str, Any]]:
    return [{"type": "text", "text": {"content": content}}]


def _notion_page_size(arguments: dict[str, Any]) -> int:
    return _bounded_limit(arguments.get("page_size"), default=10, maximum=100)


def _notion_cursor_params(arguments: dict[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {"page_size": _notion_page_size(arguments)}
    if arguments.get("start_cursor"):
        params["start_cursor"] = str(arguments["start_cursor"])
    return params


async def _notion_tool(session: AsyncSession, tool_name: str, arguments: dict[str, Any], agent_id: str) -> dict[str, Any]:
    credentials = await _connector_credentials(session, "notion")
    adapter = adapter_for("notion")
    base_url = adapter.base_url(credentials)
    headers = adapter.headers(credentials)
    async with httpx.AsyncClient(timeout=20) as client:
        if tool_name == "read_search_pages":
            response = await client.post(
                f"{base_url}/v1/search",
                headers=headers,
                json={"query": arguments.get("query", ""), "page_size": _notion_page_size(arguments)},
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "pages": payload.get("results", []), "agent_id": agent_id}
        if tool_name == "read_get_page":
            page_id = str(arguments.get("page_id") or arguments.get("id") or "")
            if not page_id:
                raise McpExecutionError(400, "page_id is required.")
            response = await client.get(f"{base_url}/v1/pages/{_url_segment(page_id, field_name='page_id')}", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "page": response.json(), "agent_id": agent_id}
        if tool_name == "read_get_database":
            database_id = str(arguments.get("database_id") or arguments.get("id") or "")
            if not database_id:
                raise McpExecutionError(400, "database_id is required.")
            response = await client.get(
                f"{base_url}/v1/databases/{_url_segment(database_id, field_name='database_id')}",
                headers=headers,
            )
            response.raise_for_status()
            return {"status": "ok", "database": response.json(), "agent_id": agent_id}
        if tool_name == "read_query_database":
            database_id = str(arguments.get("database_id") or arguments.get("id") or "")
            if not database_id:
                raise McpExecutionError(400, "database_id is required.")
            body: dict[str, Any] = {"page_size": _notion_page_size(arguments)}
            if arguments.get("start_cursor"):
                body["start_cursor"] = str(arguments["start_cursor"])
            if arguments.get("filter"):
                body["filter"] = arguments["filter"]
            if arguments.get("sorts"):
                body["sorts"] = arguments["sorts"]
            response = await client.post(
                f"{base_url}/v1/databases/{_url_segment(database_id, field_name='database_id')}/query",
                headers=headers,
                json=body,
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "results": payload.get("results", []), "page": payload, "agent_id": agent_id}
        if tool_name == "read_get_block_children":
            block_id = str(arguments.get("block_id") or arguments.get("id") or "")
            if not block_id:
                raise McpExecutionError(400, "block_id is required.")
            response = await client.get(
                f"{base_url}/v1/blocks/{_url_segment(block_id, field_name='block_id')}/children",
                headers=headers,
                params=_notion_cursor_params(arguments),
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "children": payload.get("results", []), "page": payload, "agent_id": agent_id}
        if tool_name == "read_get_comments":
            block_id = str(arguments.get("block_id") or arguments.get("page_id") or "")
            if not block_id:
                raise McpExecutionError(400, "page_id or block_id is required.")
            params = _notion_cursor_params(arguments)
            params["block_id"] = block_id
            response = await client.get(f"{base_url}/v1/comments", headers=headers, params=params)
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "comments": payload.get("results", []), "page": payload, "agent_id": agent_id}
        if tool_name == "read_list_users":
            response = await client.get(f"{base_url}/v1/users", headers=headers, params=_notion_cursor_params(arguments))
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "users": payload.get("results", []), "page": payload, "agent_id": agent_id}
        if tool_name == "write_create_page":
            title = str(arguments.get("title") or "DataClaw note")
            body = str(arguments.get("body") or "")
            parent_id = str(arguments.get("parent_id") or "")
            if not parent_id:
                raise McpExecutionError(400, "parent_id is required.")
            response = await client.post(
                f"{base_url}/v1/pages",
                headers=headers,
                json={
                    "parent": {"type": "page_id", "page_id": parent_id},
                    "properties": {"title": {"title": _notion_rich_text(title)}},
                    "children": [
                        {
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {"rich_text": _notion_rich_text(body)},
                        }
                    ],
                },
            )
            response.raise_for_status()
            return {"status": "created", "page": response.json(), "agent_id": agent_id}
        if tool_name == "write_append_to_page":
            page_id = str(arguments.get("page_id") or arguments.get("id") or "")
            body = str(arguments.get("body") or arguments.get("content") or "")
            if not page_id:
                raise McpExecutionError(400, "page_id is required.")
            response = await client.patch(
                f"{base_url}/v1/blocks/{_url_segment(page_id, field_name='page_id')}/children",
                headers=headers,
                json={
                    "children": [
                        {
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {"rich_text": _notion_rich_text(body)},
                        }
                    ]
                },
            )
            response.raise_for_status()
            return {"status": "appended", "page_id": page_id, "result": response.json(), "agent_id": agent_id}
        if tool_name == "write_update_page_properties":
            page_id = str(arguments.get("page_id") or arguments.get("id") or "")
            properties = arguments.get("properties")
            if not page_id:
                raise McpExecutionError(400, "page_id is required.")
            if not isinstance(properties, dict):
                raise McpExecutionError(400, "properties must be an object.")
            response = await client.patch(
                f"{base_url}/v1/pages/{_url_segment(page_id, field_name='page_id')}",
                headers=headers,
                json={"properties": properties},
            )
            response.raise_for_status()
            return {"status": "updated", "page": response.json(), "agent_id": agent_id}
        if tool_name == "write_archive_page":
            page_id = str(arguments.get("page_id") or arguments.get("id") or "")
            if not page_id:
                raise McpExecutionError(400, "page_id is required.")
            response = await client.patch(
                f"{base_url}/v1/pages/{_url_segment(page_id, field_name='page_id')}",
                headers=headers,
                json={"archived": True},
            )
            response.raise_for_status()
            return {"status": "archived", "page": response.json(), "agent_id": agent_id}
        if tool_name == "write_create_comment":
            page_id = str(arguments.get("page_id") or arguments.get("id") or "")
            body = str(arguments.get("body") or "")
            if not page_id:
                raise McpExecutionError(400, "page_id is required.")
            response = await client.post(
                f"{base_url}/v1/comments",
                headers=headers,
                json={"parent": {"page_id": page_id}, "rich_text": _notion_rich_text(body)},
            )
            response.raise_for_status()
            return {"status": "created", "comment": response.json(), "agent_id": agent_id}
        if tool_name == "write_create_database":
            parent_page_id = str(arguments.get("parent_page_id") or arguments.get("parent_id") or "")
            title = str(arguments.get("title") or "")
            properties = arguments.get("properties")
            if not parent_page_id:
                raise McpExecutionError(400, "parent_page_id is required.")
            if not title:
                raise McpExecutionError(400, "title is required.")
            if not isinstance(properties, dict):
                raise McpExecutionError(400, "properties must be an object.")
            response = await client.post(
                f"{base_url}/v1/databases",
                headers=headers,
                json={
                    "parent": {"type": "page_id", "page_id": parent_page_id},
                    "title": _notion_rich_text(title),
                    "properties": properties,
                },
            )
            response.raise_for_status()
            return {"status": "created", "database": response.json(), "agent_id": agent_id}
        if tool_name == "write_update_block":
            block_id = str(arguments.get("block_id") or arguments.get("id") or "")
            block_type = str(arguments.get("type") or "")
            content = arguments.get("content")
            if not block_id:
                raise McpExecutionError(400, "block_id is required.")
            if not block_type:
                raise McpExecutionError(400, "type is required.")
            if not isinstance(content, dict):
                raise McpExecutionError(400, "content must be an object.")
            response = await client.patch(
                f"{base_url}/v1/blocks/{_url_segment(block_id, field_name='block_id')}",
                headers=headers,
                json={block_type: content},
            )
            response.raise_for_status()
            return {"status": "updated", "block": response.json(), "agent_id": agent_id}
    raise McpExecutionError(404, f"Unsupported Notion MCP tool: {tool_name}")


def _github_repo_path(repo: str) -> str:
    owner, _, name = repo.strip().partition("/")
    if not owner or not name or "/" in name:
        raise McpExecutionError(400, "repo must be in owner/name format.")
    repo_part = re.compile(r"^[A-Za-z0-9._-]+$")
    if not repo_part.match(owner) or not repo_part.match(name) or owner == ".." or name == "..":
        raise McpExecutionError(400, "repo contains invalid owner or name characters.")
    return f"{_url_segment(owner, field_name='repo_owner')}/{_url_segment(name, field_name='repo_name')}"


def _github_default_repo(credentials: dict[str, Any], arguments: dict[str, Any]) -> str:
    repo = str(arguments.get("repo") or credentials.get("repositories", "").split(",")[0]).strip()
    _github_repo_path(repo)
    return repo


def _github_number(arguments: dict[str, Any], field_name: str = "number") -> int:
    try:
        value = int(arguments.get(field_name) or 0)
    except (TypeError, ValueError) as exc:
        raise McpExecutionError(400, f"{field_name} must be an integer.") from exc
    if value < 1:
        raise McpExecutionError(400, f"{field_name} is required.")
    return value


async def _github_tool(session: AsyncSession, tool_name: str, arguments: dict[str, Any], agent_id: str) -> dict[str, Any]:
    credentials = await _connector_credentials(session, "github")
    adapter = adapter_for("github")
    base_url = adapter.base_url(credentials)
    headers = {
        "Authorization": f"Bearer {credentials['token']}",
        "Accept": "application/vnd.github+json",
    }
    async with httpx.AsyncClient(timeout=20) as client:
        if tool_name == "read_list_repos":
            repos = [repo.strip() for repo in credentials.get("repositories", "").split(",") if repo.strip()]
            return {"status": "ok", "repositories": repos, "agent_id": agent_id}
        if tool_name == "read_get_file":
            repo = _github_default_repo(credentials, arguments)
            path = str(arguments.get("path") or "README.md")
            response = await client.get(
                f"{base_url}/repos/{_github_repo_path(repo)}/contents/{quote(path, safe='/')}",
                headers=headers,
                params=(
                    {"ref": arguments.get("ref") or arguments.get("branch")}
                    if arguments.get("ref") or arguments.get("branch")
                    else None
                ),
            )
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and payload.get("missing") is True:
                root = await client.get(f"{base_url}/repos/{_github_repo_path(repo)}/contents", headers=headers)
                if root.status_code < 400:
                    for item in root.json():
                        if isinstance(item, dict) and item.get("path") == path:
                            payload = item
                            break
            return {"status": "ok", "file": payload, "agent_id": agent_id}
        if tool_name == "read_list_issues":
            repo = _github_default_repo(credentials, arguments)
            params: dict[str, Any] = {
                "state": arguments.get("state") or "open",
                "per_page": _bounded_limit(arguments.get("limit"), default=30, maximum=100),
            }
            if arguments.get("since"):
                params["since"] = str(arguments["since"])
            response = await client.get(f"{base_url}/repos/{_github_repo_path(repo)}/issues", headers=headers, params=params)
            response.raise_for_status()
            issues = [item for item in response.json() if not item.get("pull_request")]
            return {"status": "ok", "issues": issues, "agent_id": agent_id}
        if tool_name == "read_get_issue":
            repo = _github_default_repo(credentials, arguments)
            number = _github_number(arguments)
            response = await client.get(f"{base_url}/repos/{_github_repo_path(repo)}/issues/{number}", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "issue": response.json(), "agent_id": agent_id}
        if tool_name == "read_get_pr":
            repo = _github_default_repo(credentials, arguments)
            number = _github_number(arguments)
            response = await client.get(f"{base_url}/repos/{_github_repo_path(repo)}/pulls/{number}", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "pull_request": response.json(), "agent_id": agent_id}
        if tool_name == "read_get_pr_diff":
            repo = _github_default_repo(credentials, arguments)
            number = _github_number(arguments)
            diff_headers = {**headers, "Accept": "application/vnd.github.v3.diff"}
            response = await client.get(f"{base_url}/repos/{_github_repo_path(repo)}/pulls/{number}", headers=diff_headers)
            response.raise_for_status()
            return {"status": "ok", "diff": response.text[:50_000], "truncated_at": 50_000 if len(response.text) > 50_000 else None, "agent_id": agent_id}
        if tool_name == "read_list_branches":
            repo = _github_default_repo(credentials, arguments)
            response = await client.get(
                f"{base_url}/repos/{_github_repo_path(repo)}/branches",
                headers=headers,
                params={"per_page": _bounded_limit(arguments.get("limit"), default=30, maximum=100)},
            )
            response.raise_for_status()
            return {"status": "ok", "branches": response.json(), "agent_id": agent_id}
        if tool_name == "read_search_code":
            query = str(arguments.get("query") or "").strip()
            if not query:
                raise McpExecutionError(400, "query is required.")
            if arguments.get("repo"):
                _github_repo_path(str(arguments["repo"]))
                query = f"{query} repo:{arguments['repo']}"
            response = await client.get(
                f"{base_url}/search/code",
                headers=headers,
                params={"q": query, "per_page": _bounded_limit(arguments.get("limit"), default=30, maximum=100)},
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "items": payload.get("items", []), "total_count": payload.get("total_count"), "agent_id": agent_id}
        if tool_name == "read_get_commit":
            repo = _github_default_repo(credentials, arguments)
            sha = str(arguments.get("sha") or "")
            response = await client.get(f"{base_url}/repos/{_github_repo_path(repo)}/commits/{_url_segment(sha, field_name='sha')}", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "commit": response.json(), "agent_id": agent_id}
        if tool_name == "read_list_workflows":
            repo = _github_default_repo(credentials, arguments)
            response = await client.get(
                f"{base_url}/repos/{_github_repo_path(repo)}/actions/workflows",
                headers=headers,
                params={"per_page": _bounded_limit(arguments.get("limit"), default=30, maximum=100)},
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "workflows": payload.get("workflows", []), "total_count": payload.get("total_count"), "agent_id": agent_id}
        if tool_name == "read_get_workflow_run_logs":
            repo = _github_default_repo(credentials, arguments)
            run_id = str(arguments.get("run_id") or "")
            response = await client.get(
                f"{base_url}/repos/{_github_repo_path(repo)}/actions/runs/{_url_segment(run_id, field_name='run_id')}/logs",
                headers=headers,
                follow_redirects=True,
            )
            response.raise_for_status()
            return {"status": "ok", "logs": response.text[:50_000], "truncated_at": 50_000 if len(response.text) > 50_000 else None, "agent_id": agent_id}
        if tool_name == "read_get_repo_metadata":
            repo = _github_default_repo(credentials, arguments)
            response = await client.get(f"{base_url}/repos/{_github_repo_path(repo)}", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "repository": response.json(), "agent_id": agent_id}
        if tool_name == "read_list_releases":
            repo = _github_default_repo(credentials, arguments)
            response = await client.get(
                f"{base_url}/repos/{_github_repo_path(repo)}/releases",
                headers=headers,
                params={"per_page": _bounded_limit(arguments.get("limit"), default=30, maximum=100)},
            )
            response.raise_for_status()
            return {"status": "ok", "releases": response.json(), "agent_id": agent_id}
        if tool_name == "write_commit_file":
            repo = _github_default_repo(credentials, arguments)
            path = str(arguments.get("path") or "README.md")
            content = str(arguments.get("content") or "")
            message = str(arguments.get("message") or f"Update {path}")
            body: dict[str, Any] = {"message": message, "content": base64.b64encode(content.encode()).decode()}
            if arguments.get("branch"):
                body["branch"] = str(arguments["branch"])
            if arguments.get("sha"):
                body["sha"] = str(arguments["sha"])
            response = await client.put(
                f"{base_url}/repos/{_github_repo_path(repo)}/contents/{quote(path, safe='/')}",
                headers=headers,
                json=body,
            )
            response.raise_for_status()
            return {"status": "committed", "commit": response.json(), "agent_id": agent_id}
        if tool_name == "write_create_pr":
            repo = _github_default_repo(credentials, arguments)
            response = await client.post(
                f"{base_url}/repos/{_github_repo_path(repo)}/pulls",
                headers=headers,
                json={
                    "title": arguments.get("title") or "DataClaw update",
                    "head": arguments.get("head") or arguments.get("branch") or "dataclaw-update",
                    "base": arguments.get("base") or "main",
                    "body": arguments.get("body") or "",
                },
            )
            response.raise_for_status()
            return {"status": "created", "pull_request": response.json(), "agent_id": agent_id}
        if tool_name == "write_create_issue":
            repo = _github_default_repo(credentials, arguments)
            response = await client.post(
                f"{base_url}/repos/{_github_repo_path(repo)}/issues",
                headers=headers,
                json={"title": arguments.get("title"), "body": arguments.get("body") or "", "labels": arguments.get("labels") or []},
            )
            response.raise_for_status()
            return {"status": "created", "issue": response.json(), "agent_id": agent_id}
        if tool_name in {"write_comment_on_pr", "write_comment_on_issue"}:
            repo = _github_default_repo(credentials, arguments)
            number = _github_number(arguments)
            body = str(arguments.get("body") or "")
            response = await client.post(
                f"{base_url}/repos/{_github_repo_path(repo)}/issues/{number}/comments",
                headers=headers,
                json={"body": body},
            )
            response.raise_for_status()
            return {"status": "created", "comment": response.json(), "agent_id": agent_id}
        if tool_name == "write_merge_pr":
            repo = _github_default_repo(credentials, arguments)
            number = _github_number(arguments)
            payload: dict[str, Any] = {"merge_method": arguments.get("method") or "merge"}
            if arguments.get("commit_title"):
                payload["commit_title"] = arguments["commit_title"]
            if arguments.get("commit_message"):
                payload["commit_message"] = arguments["commit_message"]
            response = await client.put(f"{base_url}/repos/{_github_repo_path(repo)}/pulls/{number}/merge", headers=headers, json=payload)
            response.raise_for_status()
            return {"status": "merged", "merge": response.json(), "agent_id": agent_id}
        if tool_name == "write_create_branch":
            repo = _github_default_repo(credentials, arguments)
            name = str(arguments.get("name") or "")
            from_sha = str(arguments.get("from_sha") or "")
            if not name:
                raise McpExecutionError(400, "name is required.")
            if not from_sha:
                raise McpExecutionError(400, "from_sha is required.")
            response = await client.post(
                f"{base_url}/repos/{_github_repo_path(repo)}/git/refs",
                headers=headers,
                json={"ref": f"refs/heads/{name}", "sha": from_sha},
            )
            response.raise_for_status()
            return {"status": "created", "ref": response.json(), "agent_id": agent_id}
        if tool_name == "write_delete_branch":
            repo = _github_default_repo(credentials, arguments)
            name = str(arguments.get("name") or arguments.get("branch") or "")
            if not name:
                raise McpExecutionError(400, "name or branch is required.")
            response = await client.delete(
                f"{base_url}/repos/{_github_repo_path(repo)}/git/refs/heads/{quote(name, safe='/')}",
                headers=headers,
            )
            response.raise_for_status()
            return {"status": "deleted", "branch": name, "agent_id": agent_id}
        if tool_name == "write_close_pr":
            repo = _github_default_repo(credentials, arguments)
            number = _github_number(arguments)
            response = await client.patch(f"{base_url}/repos/{_github_repo_path(repo)}/issues/{number}", headers=headers, json={"state": "closed"})
            response.raise_for_status()
            return {"status": "closed", "pull_request": response.json(), "agent_id": agent_id}
        if tool_name == "write_close_issue":
            repo = _github_default_repo(credentials, arguments)
            number = _github_number(arguments)
            response = await client.patch(f"{base_url}/repos/{_github_repo_path(repo)}/issues/{number}", headers=headers, json={"state": "closed"})
            response.raise_for_status()
            return {"status": "closed", "issue": response.json(), "agent_id": agent_id}
        if tool_name == "write_request_review":
            repo = _github_default_repo(credentials, arguments)
            number = _github_number(arguments)
            response = await client.post(
                f"{base_url}/repos/{_github_repo_path(repo)}/pulls/{number}/requested_reviewers",
                headers=headers,
                json={"reviewers": arguments.get("reviewers") or [], "team_reviewers": arguments.get("team_reviewers") or []},
            )
            response.raise_for_status()
            return {"status": "requested", "review_request": response.json(), "agent_id": agent_id}
    raise McpExecutionError(404, f"Unsupported GitHub MCP tool: {tool_name}")


def _google_doc_text(doc: dict[str, Any]) -> str:
    chunks: list[str] = []
    for item in doc.get("body", {}).get("content", []):
        paragraph = item.get("paragraph") or {}
        for element in paragraph.get("elements", []):
            text_run = element.get("textRun") or {}
            if text_run.get("content"):
                chunks.append(text_run["content"])
    return "".join(chunks)


def _google_doc_end_index(doc: dict[str, Any]) -> int:
    indexes = [int(item.get("endIndex") or 1) for item in doc.get("body", {}).get("content", [])]
    return max(indexes, default=1)


def _google_drive_query_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _quip_max_created_usec(arguments: dict[str, Any]) -> int | None:
    if arguments.get("max_created_usec"):
        try:
            return int(arguments["max_created_usec"])
        except (TypeError, ValueError) as exc:
            raise McpExecutionError(400, "max_created_usec must be an integer.") from exc
    since = arguments.get("since")
    if not since:
        return None
    value = str(since).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise McpExecutionError(400, "since must be an ISO timestamp.") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1_000_000)


def _confluence_cql_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


async def _google_docs_tool(tool_name: str, arguments: dict[str, Any], credentials: dict[str, Any], agent_id: str) -> dict[str, Any]:
    adapter = adapter_for("google_docs")

    def run() -> dict[str, Any]:
        drive, docs = adapter._services(credentials)  # type: ignore[attr-defined]
        limit = _bounded_limit(arguments.get("limit"), default=50, maximum=100)
        if tool_name == "read_list_docs":
            payload = drive.files().list(
                q="mimeType='application/vnd.google-apps.document' and trashed=false",
                pageSize=limit,
                fields="files(id, name, modifiedTime, webViewLink, parents)",
            ).execute()
            return {"status": "ok", "docs": payload.get("files", []), "agent_id": agent_id}
        if tool_name == "read_search_docs":
            query = _google_drive_query_literal(str(arguments.get("query") or ""))
            payload = drive.files().list(
                q=f"mimeType='application/vnd.google-apps.document' and name contains '{query}' and trashed=false",
                pageSize=limit,
                fields="files(id, name, modifiedTime, webViewLink, parents)",
            ).execute()
            return {"status": "ok", "docs": payload.get("files", []), "agent_id": agent_id}
        if tool_name == "read_get_doc":
            doc_id = str(arguments.get("doc_id") or arguments.get("id") or "")
            if not doc_id:
                raise McpExecutionError(400, "doc_id is required.")
            doc = docs.documents().get(documentId=doc_id).execute()
            return {"status": "ok", "doc": {**doc, "body_text": _google_doc_text(doc)}, "agent_id": agent_id}
        if tool_name == "read_get_doc_comments":
            doc_id = str(arguments.get("doc_id") or "")
            comments = drive.comments().list(
                fileId=doc_id,
                pageSize=limit,
                fields="nextPageToken, comments(id, content, author, createdTime, modifiedTime, resolved, replies)",
            ).execute()
            return {"status": "ok", "comments": comments.get("comments", []), "agent_id": agent_id}
        if tool_name == "read_get_doc_revisions":
            doc_id = str(arguments.get("doc_id") or "")
            revisions = drive.revisions().list(
                fileId=doc_id,
                pageSize=limit,
                fields="nextPageToken, revisions(id, modifiedTime, lastModifyingUser, size, keepForever)",
            ).execute()
            return {"status": "ok", "revisions": revisions.get("revisions", []), "agent_id": agent_id}
        if tool_name == "read_list_folder_contents":
            folder_id = str(arguments.get("folder_id") or "")
            if not folder_id:
                raise McpExecutionError(400, "folder_id is required.")
            payload = drive.files().list(
                q=f"'{_google_drive_query_literal(folder_id)}' in parents and trashed=false",
                pageSize=limit,
                fields="files(id, name, mimeType, modifiedTime, webViewLink)",
            ).execute()
            return {"status": "ok", "files": payload.get("files", []), "agent_id": agent_id}
        if tool_name == "read_list_shared_with_me":
            payload = drive.files().list(
                q="sharedWithMe and trashed=false",
                pageSize=limit,
                fields="files(id, name, mimeType, modifiedTime, webViewLink)",
            ).execute()
            return {"status": "ok", "files": payload.get("files", []), "agent_id": agent_id}
        if tool_name == "read_get_doc_metadata":
            doc_id = str(arguments.get("doc_id") or "")
            metadata = drive.files().get(fileId=doc_id, fields="id, name, mimeType, modifiedTime, webViewLink, parents, owners").execute()
            return {"status": "ok", "metadata": metadata, "agent_id": agent_id}
        if tool_name == "write_create_doc":
            title = str(arguments.get("title") or "")
            if not title:
                raise McpExecutionError(400, "title is required.")
            doc = docs.documents().create(body={"title": title}).execute()
            body = str(arguments.get("body") or "")
            if body:
                docs.documents().batchUpdate(
                    documentId=doc["documentId"],
                    body={"requests": [{"insertText": {"location": {"index": 1}, "text": body}}]},
                ).execute()
            return {"status": "created", "doc": doc, "agent_id": agent_id}
        if tool_name == "write_append_to_doc":
            doc_id = str(arguments.get("doc_id") or "")
            content = str(arguments.get("content") or "")
            doc = docs.documents().get(documentId=doc_id).execute()
            docs.documents().batchUpdate(
                documentId=doc_id,
                body={"requests": [{"insertText": {"location": {"index": max(1, _google_doc_end_index(doc) - 1)}, "text": content}}]},
            ).execute()
            return {"status": "updated", "doc_id": doc_id, "agent_id": agent_id}
        if tool_name == "write_replace_text":
            doc_id = str(arguments.get("doc_id") or "")
            response = docs.documents().batchUpdate(
                documentId=doc_id,
                body={
                    "requests": [
                        {
                            "replaceAllText": {
                                "containsText": {"text": str(arguments.get("find") or ""), "matchCase": True},
                                "replaceText": str(arguments.get("replace") or ""),
                            }
                        }
                    ]
                },
            ).execute()
            return {"status": "updated", "result": response, "agent_id": agent_id}
        if tool_name == "write_create_comment":
            doc_id = str(arguments.get("doc_id") or "")
            body: dict[str, Any] = {"content": str(arguments.get("body") or "")}
            if arguments.get("anchor"):
                body["anchor"] = str(arguments["anchor"])
            comment = drive.comments().create(fileId=doc_id, body=body, fields="id, content").execute()
            return {"status": "created", "comment": comment, "agent_id": agent_id}
        if tool_name == "write_share_doc":
            doc_id = str(arguments.get("doc_id") or "")
            permission = drive.permissions().create(
                fileId=doc_id,
                body={"type": "user", "role": arguments.get("role"), "emailAddress": arguments.get("email")},
                sendNotificationEmail=False,
                fields="id",
            ).execute()
            return {"status": "shared", "permission": permission, "agent_id": agent_id}
        if tool_name == "write_move_doc":
            doc_id = str(arguments.get("doc_id") or "")
            folder_id = str(arguments.get("folder_id") or "")
            current = drive.files().get(fileId=doc_id, fields="parents").execute()
            previous = ",".join(current.get("parents", []))
            kwargs: dict[str, Any] = {"fileId": doc_id, "addParents": folder_id, "fields": "id, parents"}
            if previous:
                kwargs["removeParents"] = previous
            moved = drive.files().update(**kwargs).execute()
            return {"status": "moved", "file": moved, "agent_id": agent_id}
        if tool_name == "write_rename_doc":
            doc_id = str(arguments.get("doc_id") or "")
            renamed = drive.files().update(fileId=doc_id, body={"name": arguments.get("name")}, fields="id, name").execute()
            return {"status": "renamed", "file": renamed, "agent_id": agent_id}
        raise McpExecutionError(404, f"Unsupported Google Docs MCP tool: {tool_name}")

    return await asyncio.to_thread(run)


async def _kb_tool(
    session: AsyncSession,
    connector_slug: str,
    tool_name: str,
    arguments: dict[str, Any],
    agent_id: str,
) -> dict[str, Any]:
    credentials = await _optional_connector_credentials(session, connector_slug)
    if connector_slug == "google_docs":
        if credentials:
            return await _google_docs_tool(tool_name, arguments, credentials, agent_id)
        if tool_name == "read_list_docs":
            return {
                "status": "ok",
                "docs": [{"id": "gdoc-revenue-glossary", "title": "Revenue glossary"}],
                "agent_id": agent_id,
            }
        if tool_name == "read_get_doc":
            doc_id = str(arguments.get("doc_id") or arguments.get("id") or "gdoc-revenue-glossary")
            return {
                "status": "ok",
                "doc": {"id": doc_id, "title": "Revenue glossary", "body": "LTV maps to customer revenue."},
                "agent_id": agent_id,
            }
        if tool_name == "write_create_doc":
            return {
                "status": "stubbed",
                "doc": {"title": arguments.get("title") or "DataClaw document", "body": arguments.get("body") or ""},
                "agent_id": agent_id,
            }
    if connector_slug == "quip":
        if tool_name == "read_search":
            if credentials:
                adapter = adapter_for("quip")
                async with httpx.AsyncClient(timeout=20) as client:
                    query = str(arguments.get("query") or "").strip()
                    if query:
                        response = await client.get(
                            f"{adapter.base_url(credentials)}/1/threads/search",
                            headers=adapter.headers(credentials),
                            params={"query": query, "count": _bounded_limit(arguments.get("limit"), default=10, maximum=100)},
                        )
                    else:
                        response = await client.get(
                            f"{adapter.base_url(credentials)}/1/threads/recent",
                            headers=adapter.headers(credentials),
                            params={"count": _bounded_limit(arguments.get("limit"), default=10, maximum=100)},
                        )
                    response.raise_for_status()
                    payload = response.json()
                return {"status": "ok", "threads": payload.get("threads", payload.get("results", [])), "agent_id": agent_id}
            return {"status": "ok", "threads": [{"id": "thread-revenue-glossary", "title": "Revenue glossary"}], "agent_id": agent_id}
        if tool_name == "read_get_thread":
            thread_id = str(arguments.get("thread_id") or arguments.get("id") or "")
            if not thread_id:
                raise McpExecutionError(400, "thread_id is required.")
            if credentials:
                adapter = adapter_for("quip")
                async with httpx.AsyncClient(timeout=20) as client:
                    response = await client.get(
                        f"{adapter.base_url(credentials)}/1/threads/{thread_id}",
                        headers=adapter.headers(credentials),
                    )
                    response.raise_for_status()
                return {"status": "ok", "thread": response.json().get("thread", response.json()), "agent_id": agent_id}
            return {"status": "ok", "thread": {"id": thread_id, "title": "Revenue glossary"}, "agent_id": agent_id}
        if tool_name in {"read_get_thread_history", "read_get_messages"}:
            thread_id = str(arguments.get("thread_id") or arguments.get("id") or "")
            if not thread_id:
                raise McpExecutionError(400, "thread_id is required.")
            if credentials:
                adapter = adapter_for("quip")
                params: dict[str, Any] = {"count": _bounded_limit(arguments.get("limit"), default=50, maximum=100)}
                max_created_usec = _quip_max_created_usec(arguments)
                if max_created_usec:
                    params["max_created_usec"] = max_created_usec
                async with httpx.AsyncClient(timeout=20) as client:
                    response = await client.get(
                        f"{adapter.base_url(credentials)}/1/messages/{_url_segment(thread_id, field_name='thread_id')}",
                        headers=adapter.headers(credentials),
                        params=params,
                    )
                    response.raise_for_status()
                    payload = response.json()
                messages = payload if isinstance(payload, list) else payload.get("messages", [])
                return {"status": "ok", "messages": messages, "agent_id": agent_id}
            return {"status": "ok", "messages": [], "agent_id": agent_id}
        if tool_name == "read_list_folders":
            if credentials:
                adapter = adapter_for("quip")
                async with httpx.AsyncClient(timeout=20) as client:
                    user_response = await client.get(
                        f"{adapter.base_url(credentials)}/1/users/current",
                        headers=adapter.headers(credentials),
                    )
                    user_response.raise_for_status()
                    user = user_response.json()
                    folder_ids = [
                        *([user["private_folder_id"]] if user.get("private_folder_id") else []),
                        *(user.get("shared_folder_ids") or []),
                        *(user.get("group_folder_ids") or []),
                    ][:_bounded_limit(arguments.get("limit"), default=50, maximum=100)]
                    if not folder_ids:
                        return {"status": "ok", "folders": [], "agent_id": agent_id}
                    response = await client.get(
                        f"{adapter.base_url(credentials)}/1/folders/",
                        headers=adapter.headers(credentials),
                        params={"ids": ",".join(folder_ids)},
                    )
                    response.raise_for_status()
                    payload = response.json()
                if isinstance(payload, dict) and "folders" in payload:
                    folders = payload["folders"]
                elif isinstance(payload, dict):
                    folders = [
                        value.get("folder", value)
                        for value in payload.values()
                        if isinstance(value, dict)
                    ]
                else:
                    folders = []
                return {"status": "ok", "folders": folders, "agent_id": agent_id}
            return {"status": "ok", "folders": [], "agent_id": agent_id}
        if tool_name == "read_get_folder":
            folder_id = str(arguments.get("folder_id") or arguments.get("id") or "")
            if not folder_id:
                raise McpExecutionError(400, "folder_id is required.")
            if credentials:
                adapter = adapter_for("quip")
                async with httpx.AsyncClient(timeout=20) as client:
                    response = await client.get(
                        f"{adapter.base_url(credentials)}/1/folders/{_url_segment(folder_id, field_name='folder_id')}",
                        headers=adapter.headers(credentials),
                    )
                    response.raise_for_status()
                return {"status": "ok", "folder": response.json().get("folder", response.json()), "agent_id": agent_id}
            return {"status": "ok", "folder": {"id": folder_id}, "agent_id": agent_id}
        if tool_name == "write_create_thread":
            if credentials:
                adapter = adapter_for("quip")
                folder_ids = ",".join(arguments.get("folder_ids") or [])
                async with httpx.AsyncClient(timeout=20) as client:
                    response = await client.post(
                        f"{adapter.base_url(credentials)}/1/threads/new-document",
                        headers=adapter.headers(credentials),
                        data={
                            "title": arguments.get("title") or "DataClaw thread",
                            "content": arguments.get("body") or "",
                            "format": "markdown",
                            "member_ids": folder_ids,
                        },
                    )
                    response.raise_for_status()
                return {"status": "created", "thread": response.json().get("thread", response.json()), "agent_id": agent_id}
            return {
                "status": "stubbed",
                "thread": {"title": arguments.get("title") or "DataClaw thread", "body": arguments.get("body") or ""},
                "agent_id": agent_id,
            }
        if tool_name == "write_edit_thread":
            thread_id = str(arguments.get("thread_id") or "")
            if not thread_id:
                raise McpExecutionError(400, "thread_id is required.")
            adapter = adapter_for("quip")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(
                    f"{adapter.base_url(credentials)}/1/threads/edit-document",
                    headers=adapter.headers(credentials),
                    data={
                        "thread_id": thread_id,
                        "content": arguments.get("content") or "",
                        "format": arguments.get("format") or "markdown",
                    },
                )
                response.raise_for_status()
            return {"status": "updated", "thread": response.json().get("thread", response.json()), "agent_id": agent_id}
        if tool_name == "write_send_message":
            thread_id = str(arguments.get("thread_id") or "")
            if not thread_id:
                raise McpExecutionError(400, "thread_id is required.")
            adapter = adapter_for("quip")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(
                    f"{adapter.base_url(credentials)}/1/messages/new",
                    headers=adapter.headers(credentials),
                    data={"thread_id": thread_id, "content": arguments.get("body") or ""},
                )
                response.raise_for_status()
            return {"status": "created", "message": response.json().get("message", response.json()), "agent_id": agent_id}
        if tool_name == "write_share_thread":
            thread_id = str(arguments.get("thread_id") or "")
            if not thread_id:
                raise McpExecutionError(400, "thread_id is required.")
            adapter = adapter_for("quip")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(
                    f"{adapter.base_url(credentials)}/1/threads/add-members",
                    headers=adapter.headers(credentials),
                    data={"thread_id": thread_id, "member_ids": ",".join(arguments.get("member_ids") or [])},
                )
                response.raise_for_status()
            return {"status": "shared", "result": response.json(), "agent_id": agent_id}
        if tool_name == "write_create_folder":
            adapter = adapter_for("quip")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(
                    f"{adapter.base_url(credentials)}/1/folders/new",
                    headers=adapter.headers(credentials),
                    data={
                        "title": arguments.get("name") or "DataClaw folder",
                        "parent_id": arguments.get("parent_id") or "",
                        "member_ids": ",".join(arguments.get("member_ids") or []),
                    },
                )
                response.raise_for_status()
            return {"status": "created", "folder": response.json().get("folder", response.json()), "agent_id": agent_id}
    if connector_slug == "confluence":
        if tool_name == "read_search_pages":
            if credentials:
                adapter = adapter_for("confluence")
                query = _confluence_cql_text(str(arguments.get("query") or "").strip())
                cql = f'type=page and text ~ "{query}"' if query else "type=page"
                async with httpx.AsyncClient(timeout=20) as client:
                    response = await client.get(
                        f"{adapter.base_url(credentials)}/wiki/rest/api/search",
                        headers=adapter.headers(credentials),
                        params={"cql": cql, "limit": _bounded_limit(arguments.get("limit"), default=25, maximum=100)},
                    )
                    response.raise_for_status()
                    payload = response.json()
                return {"status": "ok", "pages": payload.get("results", []), "agent_id": agent_id}
            return {"status": "ok", "pages": [{"id": "conf-revenue-glossary", "title": "Revenue glossary"}], "agent_id": agent_id}
        if tool_name == "read_get_page":
            page_id = str(arguments.get("page_id") or arguments.get("id") or "")
            if not page_id:
                raise McpExecutionError(400, "page_id is required.")
            if credentials:
                adapter = adapter_for("confluence")
                async with httpx.AsyncClient(timeout=20) as client:
                    response = await client.get(
                        f"{adapter.base_url(credentials)}/wiki/rest/api/content/{_url_segment(page_id, field_name='page_id')}",
                        headers=adapter.headers(credentials),
                        params={"expand": "body.storage,version,space,ancestors"},
                    )
                    response.raise_for_status()
                return {"status": "ok", "page": response.json(), "agent_id": agent_id}
            return {"status": "ok", "page": {"id": page_id, "title": "Revenue glossary"}, "agent_id": agent_id}
        if tool_name == "read_get_page_children":
            if not credentials:
                return {"status": "ok", "children": [], "agent_id": agent_id}
            page_id = str(arguments.get("page_id") or "")
            if not page_id:
                raise McpExecutionError(400, "page_id is required.")
            adapter = adapter_for("confluence")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(
                    f"{adapter.base_url(credentials)}/wiki/rest/api/content/{_url_segment(page_id, field_name='page_id')}/child/page",
                    headers=adapter.headers(credentials),
                    params={"limit": _bounded_limit(arguments.get("limit"), default=25, maximum=100)},
                )
                response.raise_for_status()
            return {"status": "ok", "children": response.json().get("results", []), "agent_id": agent_id}
        if tool_name == "read_get_space":
            if not credentials:
                return {"status": "ok", "space": {"key": arguments.get("space_key")}, "agent_id": agent_id}
            space_key = str(arguments.get("space_key") or "")
            if not space_key:
                raise McpExecutionError(400, "space_key is required.")
            adapter = adapter_for("confluence")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(
                    f"{adapter.base_url(credentials)}/wiki/rest/api/space/{_url_segment(space_key, field_name='space_key')}",
                    headers=adapter.headers(credentials),
                )
                response.raise_for_status()
            return {"status": "ok", "space": response.json(), "agent_id": agent_id}
        if tool_name == "read_get_page_history":
            if not credentials:
                return {"status": "ok", "history": {}, "agent_id": agent_id}
            page_id = str(arguments.get("page_id") or "")
            if not page_id:
                raise McpExecutionError(400, "page_id is required.")
            adapter = adapter_for("confluence")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(
                    f"{adapter.base_url(credentials)}/wiki/rest/api/content/{_url_segment(page_id, field_name='page_id')}/history",
                    headers=adapter.headers(credentials),
                )
                response.raise_for_status()
            return {"status": "ok", "history": response.json(), "agent_id": agent_id}
        if tool_name == "read_search_attachments":
            if not credentials:
                return {"status": "ok", "attachments": [], "agent_id": agent_id}
            query = _confluence_cql_text(str(arguments.get("query") or ""))
            adapter = adapter_for("confluence")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(
                    f"{adapter.base_url(credentials)}/wiki/rest/api/search",
                    headers=adapter.headers(credentials),
                    params={
                        "cql": f'type=attachment and text ~ "{query}"',
                        "limit": _bounded_limit(arguments.get("limit"), default=25, maximum=100),
                    },
                )
                response.raise_for_status()
            return {"status": "ok", "attachments": response.json().get("results", []), "agent_id": agent_id}
        if tool_name == "read_get_comments":
            if not credentials:
                return {"status": "ok", "comments": [], "agent_id": agent_id}
            page_id = str(arguments.get("page_id") or "")
            if not page_id:
                raise McpExecutionError(400, "page_id is required.")
            adapter = adapter_for("confluence")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(
                    f"{adapter.base_url(credentials)}/wiki/rest/api/content/{_url_segment(page_id, field_name='page_id')}/child/comment",
                    headers=adapter.headers(credentials),
                    params={"limit": _bounded_limit(arguments.get("limit"), default=25, maximum=100)},
                )
                response.raise_for_status()
            return {"status": "ok", "comments": response.json().get("results", []), "agent_id": agent_id}
        if tool_name == "read_list_spaces":
            if not credentials:
                return {"status": "ok", "spaces": [], "agent_id": agent_id}
            adapter = adapter_for("confluence")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(
                    f"{adapter.base_url(credentials)}/wiki/rest/api/space",
                    headers=adapter.headers(credentials),
                    params={"limit": _bounded_limit(arguments.get("limit"), default=25, maximum=100)},
                )
                response.raise_for_status()
            return {"status": "ok", "spaces": response.json().get("results", []), "agent_id": agent_id}
        if tool_name == "read_get_labels":
            if not credentials:
                return {"status": "ok", "labels": [], "agent_id": agent_id}
            page_id = str(arguments.get("page_id") or "")
            if not page_id:
                raise McpExecutionError(400, "page_id is required.")
            adapter = adapter_for("confluence")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(
                    f"{adapter.base_url(credentials)}/wiki/rest/api/content/{_url_segment(page_id, field_name='page_id')}/label",
                    headers=adapter.headers(credentials),
                    params={"limit": _bounded_limit(arguments.get("limit"), default=25, maximum=100)},
                )
                response.raise_for_status()
            return {"status": "ok", "labels": response.json().get("results", []), "agent_id": agent_id}
        if tool_name == "write_create_page":
            if credentials:
                adapter = adapter_for("confluence")
                body: dict[str, Any] = {
                    "type": "page",
                    "title": arguments.get("title") or "DataClaw page",
                    "space": {"key": arguments.get("space_key")},
                    "body": {"storage": {"value": arguments.get("body") or "", "representation": "storage"}},
                }
                if arguments.get("parent_id"):
                    body["ancestors"] = [{"id": str(arguments["parent_id"])}]
                async with httpx.AsyncClient(timeout=20) as client:
                    response = await client.post(
                        f"{adapter.base_url(credentials)}/wiki/rest/api/content",
                        headers=adapter.headers(credentials),
                        json=body,
                    )
                    response.raise_for_status()
                return {"status": "created", "page": response.json(), "agent_id": agent_id}
            return {
                "status": "stubbed",
                "page": {"title": arguments.get("title") or "DataClaw page", "body": arguments.get("body") or ""},
                "agent_id": agent_id,
            }
        if tool_name == "write_append_to_page":
            page_id = str(arguments.get("page_id") or "")
            content = str(arguments.get("body") or arguments.get("content") or "")
            if credentials:
                adapter = adapter_for("confluence")
                async with httpx.AsyncClient(timeout=20) as client:
                    current = await client.get(
                        f"{adapter.base_url(credentials)}/wiki/rest/api/content/{_url_segment(page_id, field_name='page_id')}",
                        headers=adapter.headers(credentials),
                        params={"expand": "body.storage,version"},
                    )
                    current.raise_for_status()
                    page = current.json()
                    existing = ((page.get("body") or {}).get("storage") or {}).get("value") or ""
                    version = int((page.get("version") or {}).get("number") or 1) + 1
                    response = await client.put(
                        f"{adapter.base_url(credentials)}/wiki/rest/api/content/{_url_segment(page_id, field_name='page_id')}",
                        headers=adapter.headers(credentials),
                        json={
                            "id": page_id,
                            "type": "page",
                            "title": page.get("title"),
                            "version": {"number": version},
                            "body": {"storage": {"value": f"{existing}{content}", "representation": "storage"}},
                        },
                    )
                    response.raise_for_status()
                return {"status": "updated", "page": response.json(), "agent_id": agent_id}
            return {"status": "stubbed", "page": {"id": page_id, "appended": content}, "agent_id": agent_id}
        if tool_name == "write_update_page":
            page_id = str(arguments.get("page_id") or "")
            adapter = adapter_for("confluence")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.put(
                    f"{adapter.base_url(credentials)}/wiki/rest/api/content/{_url_segment(page_id, field_name='page_id')}",
                    headers=adapter.headers(credentials),
                    json={
                        "id": page_id,
                        "type": "page",
                        "title": arguments.get("title"),
                        "version": {"number": int(arguments.get("version") or 1)},
                        "body": {"storage": {"value": arguments.get("content") or "", "representation": "storage"}},
                    },
                )
                response.raise_for_status()
            return {"status": "updated", "page": response.json(), "agent_id": agent_id}
        if tool_name == "write_add_label":
            page_id = str(arguments.get("page_id") or "")
            adapter = adapter_for("confluence")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(
                    f"{adapter.base_url(credentials)}/wiki/rest/api/content/{_url_segment(page_id, field_name='page_id')}/label",
                    headers=adapter.headers(credentials),
                    json=[{"prefix": "global", "name": arguments.get("label")}],
                )
                response.raise_for_status()
            return {"status": "created", "labels": response.json().get("results", response.json()), "agent_id": agent_id}
        if tool_name == "write_create_comment":
            page_id = str(arguments.get("page_id") or "")
            adapter = adapter_for("confluence")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(
                    f"{adapter.base_url(credentials)}/wiki/rest/api/content",
                    headers=adapter.headers(credentials),
                    json={
                        "type": "comment",
                        "container": {"id": page_id, "type": "page"},
                        "body": {"storage": {"value": arguments.get("body") or "", "representation": "storage"}},
                    },
                )
                response.raise_for_status()
            return {"status": "created", "comment": response.json(), "agent_id": agent_id}
        if tool_name == "write_create_attachment":
            page_id = str(arguments.get("page_id") or "")
            adapter = adapter_for("confluence")
            raw_content = str(arguments.get("content") or "")
            if arguments.get("content_encoding") == "base64":
                try:
                    file_content: str | bytes = base64.b64decode(raw_content, validate=True)
                except (ValueError, binascii.Error) as exc:
                    raise McpExecutionError(400, "content must be valid base64 when content_encoding is base64.") from exc
            else:
                file_content = raw_content
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(
                    f"{adapter.base_url(credentials)}/wiki/rest/api/content/{_url_segment(page_id, field_name='page_id')}/child/attachment",
                    headers={**adapter.headers(credentials), "X-Atlassian-Token": "no-check"},
                    files={"file": (str(arguments.get("filename") or "dataclaw.txt"), file_content)},
                )
                response.raise_for_status()
            return {"status": "created", "attachment": response.json(), "agent_id": agent_id}
        if tool_name == "write_move_page":
            page_id = str(arguments.get("page_id") or "")
            parent_id = str(arguments.get("parent_id") or "")
            adapter = adapter_for("confluence")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.put(
                    f"{adapter.base_url(credentials)}/wiki/rest/api/content/{_url_segment(page_id, field_name='page_id')}/move/append/{_url_segment(parent_id, field_name='parent_id')}",
                    headers=adapter.headers(credentials),
                )
                response.raise_for_status()
            return {"status": "moved", "page": response.json() if response.content else {"id": page_id}, "agent_id": agent_id}
        if tool_name == "write_delete_page":
            page_id = str(arguments.get("page_id") or "")
            adapter = adapter_for("confluence")
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.delete(
                    f"{adapter.base_url(credentials)}/wiki/rest/api/content/{_url_segment(page_id, field_name='page_id')}",
                    headers=adapter.headers(credentials),
                )
                response.raise_for_status()
            return {"status": "deleted", "page_id": page_id, "agent_id": agent_id}
    raise McpExecutionError(404, f"Unsupported {connector_slug} MCP tool: {tool_name}")


def _dbt_artifact_path(value: str, *, field_name: str = "artifact_path") -> str:
    if not value or value.startswith("/") or ".." in value.split("/"):
        raise McpExecutionError(400, f"{field_name} must be a relative artifact path.")
    return quote(value, safe="/")


async def _dbt_get_artifact(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    headers: dict[str, str],
    run_id: str,
    path: str,
    required: bool = True,
) -> Any:
    run_path = _url_segment(run_id, field_name="run_id")
    artifact_path = _dbt_artifact_path(path)
    response = await client.get(f"{base_url}/runs/{run_path}/artifacts/{artifact_path}", headers=headers)
    if response.status_code == 404:
        response = await client.get(f"{base_url}/artifacts/{artifact_path}", headers=headers)
    if response.status_code == 404 and path == "manifest.json":
        response = await client.get(f"{base_url}/manifest.json", headers=headers)
    if response.status_code >= 400 and not required:
        return None
    response.raise_for_status()
    content_type = response.headers.get("content-type", "")
    if "json" in content_type or path.endswith(".json"):
        return response.json()
    return response.text


def _dbt_manifest_nodes(manifest: Any, resource_type: str) -> list[dict[str, Any]]:
    if not isinstance(manifest, dict):
        return []
    nodes = manifest.get("nodes") or {}
    if not isinstance(nodes, dict):
        return []
    return [node for node in nodes.values() if isinstance(node, dict) and node.get("resource_type") == resource_type]


def _dbt_manifest_exposures(manifest: Any) -> list[dict[str, Any]]:
    if not isinstance(manifest, dict):
        return []
    exposures = manifest.get("exposures") or {}
    return [node for node in exposures.values() if isinstance(node, dict)] if isinstance(exposures, dict) else []


def _dbt_find_node(manifest: Any, *, resource_type: str, unique_id: str | None, name: str | None) -> dict[str, Any] | None:
    for node in _dbt_manifest_nodes(manifest, resource_type):
        if unique_id and node.get("unique_id") == unique_id:
            return node
        if name and (node.get("name") == name or node.get("alias") == name):
            return node
    return None


def _dbt_catalog_node(catalog: Any, unique_id: str | None) -> dict[str, Any] | None:
    if not unique_id or not isinstance(catalog, dict):
        return None
    nodes = catalog.get("nodes") or {}
    sources = catalog.get("sources") or {}
    for collection in (nodes, sources):
        if isinstance(collection, dict) and isinstance(collection.get(unique_id), dict):
            return collection[unique_id]
    return None


def _dbt_latest_run_id(payload: Any) -> str | None:
    runs = _data_list(payload)
    if not runs or not isinstance(runs[0], dict):
        return None
    value = runs[0].get("id")
    return str(value) if value is not None else None


def _dbt_run_logs(run: Any) -> list[Any]:
    if not isinstance(run, dict):
        return []
    logs: list[Any] = []
    debug_logs = run.get("debug_logs")
    if isinstance(debug_logs, list):
        logs.extend(debug_logs)
    run_steps = run.get("run_steps")
    if isinstance(run_steps, list):
        for step in run_steps:
            if not isinstance(step, dict):
                continue
            step_logs = step.get("logs")
            if isinstance(step_logs, list):
                logs.extend(step_logs)
            elif step_logs:
                logs.append(step_logs)
    return logs


def _dbt_trigger_payload(arguments: dict[str, Any], *, default_cause: str, default_steps: list[str] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"cause": arguments.get("cause") or default_cause}
    optional_fields = [
        "git_sha",
        "git_branch",
        "schema_override",
        "dbt_version_override",
        "threads_override",
        "target_name_override",
        "generate_docs_override",
        "timeout_seconds_override",
    ]
    for field in optional_fields:
        if field in arguments:
            payload[field] = arguments.get(field)
    steps_override = arguments.get("steps_override") or default_steps
    if steps_override:
        payload["steps_override"] = steps_override
    return payload


def _dbt_model_path(credentials: dict[str, Any], arguments: dict[str, Any]) -> Path:
    project_path = str(arguments.get("project_path") or credentials.get("project_path") or "")
    if not project_path:
        raise McpExecutionError(400, "project_path is required in arguments or dbt connector credentials.")
    name = str(arguments.get("name") or "")
    if not IDENTIFIER.match(name):
        raise McpExecutionError(400, "name must be a valid dbt model identifier.")
    schema = str(arguments.get("schema") or "")
    if schema and not IDENTIFIER.match(schema):
        raise McpExecutionError(400, "schema must be a valid dbt model subdirectory identifier.")
    root = Path(project_path).expanduser().resolve()
    models_root = (root / "models").resolve()
    target = (models_root / schema / f"{name}.sql").resolve() if schema else (models_root / f"{name}.sql").resolve()
    if models_root != target.parent and models_root not in target.parents:
        raise McpExecutionError(400, "model path escapes the dbt models directory.")
    return target


async def _dbt_tool(session: AsyncSession, tool_name: str, arguments: dict[str, Any], agent_id: str) -> dict[str, Any]:
    credentials = await _connector_credentials(session, "dbt")
    adapter = adapter_for("dbt")
    base_url = adapter.base_url(credentials)
    headers = adapter.headers(credentials)
    async with httpx.AsyncClient(timeout=20) as client:
        if tool_name == "read_list_models":
            run_id = str(arguments.get("run_id") or "")
            if not run_id:
                response = await client.get(f"{base_url}/runs/", headers=headers, params={"limit": 1})
                response.raise_for_status()
                run_id = _dbt_latest_run_id(response.json()) or ""
            if run_id:
                manifest = await _dbt_get_artifact(client, base_url=base_url, headers=headers, run_id=run_id, path="manifest.json")
                return {"status": "ok", "models": _dbt_manifest_nodes(manifest, "model"), "run_id": run_id, "agent_id": agent_id}
            return {"status": "ok", "models": [], "run_id": None, "agent_id": agent_id}
        if tool_name == "read_get_lineage":
            run_id = str(arguments.get("run_id") or "")
            if run_id:
                manifest = await _dbt_get_artifact(client, base_url=base_url, headers=headers, run_id=run_id, path="manifest.json")
                lineage = {
                    "nodes": manifest.get("nodes", {}) if isinstance(manifest, dict) else {},
                    "sources": manifest.get("sources", {}) if isinstance(manifest, dict) else {},
                    "parent_map": manifest.get("parent_map", {}) if isinstance(manifest, dict) else {},
                    "child_map": manifest.get("child_map", {}) if isinstance(manifest, dict) else {},
                }
                return {"status": "ok", "lineage": lineage, "agent_id": agent_id}
            project_id = arguments.get("project_id") or credentials.get("project_id")
            if not project_id:
                raise McpExecutionError(400, "run_id or project_id is required.")
            path = f"{base_url}/projects/{_url_segment(str(project_id), field_name='project_id')}/lineage/"
            response = await client.get(path, headers=headers)
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "lineage": payload.get("data", payload), "agent_id": agent_id}
        if tool_name == "read_list_runs":
            params: dict[str, Any] = {"limit": _bounded_limit(arguments.get("limit"), default=25, maximum=100)}
            for field in ("job_definition_id", "project_id", "environment_id", "status"):
                if arguments.get(field):
                    params[field] = arguments[field]
            response = await client.get(f"{base_url}/runs/", headers=headers, params=params)
            response.raise_for_status()
            payload = response.json()
            return {"status": "ok", "runs": payload.get("data", []), "agent_id": agent_id}
        if tool_name == "read_get_run_artifacts":
            run_id = str(arguments.get("run_id") or "")
            run_path = _url_segment(run_id, field_name="run_id")
            response = await client.get(f"{base_url}/runs/{run_path}/artifacts/", headers=headers)
            response.raise_for_status()
            payload = response.json()
            requested_paths = arguments.get("paths") or []
            artifacts = {
                path: await _dbt_get_artifact(client, base_url=base_url, headers=headers, run_id=run_id, path=str(path))
                for path in requested_paths
            }
            return {"status": "ok", "artifact_paths": payload.get("data", payload), "artifacts": artifacts, "agent_id": agent_id}
        if tool_name == "read_get_manifest":
            run_id = str(arguments.get("run_id") or "")
            manifest = await _dbt_get_artifact(client, base_url=base_url, headers=headers, run_id=run_id, path="manifest.json")
            return {"status": "ok", "manifest": manifest, "agent_id": agent_id}
        if tool_name == "read_get_run_logs":
            run_id = str(arguments.get("run_id") or "")
            if not run_id:
                raise McpExecutionError(400, "run_id is required.")
            run_path = _url_segment(run_id, field_name="run_id")
            response = await client.get(
                f"{base_url}/runs/{run_path}/",
                headers=headers,
                params={"include_related": '["debug_logs","run_steps"]'},
            )
            response.raise_for_status()
            payload = response.json()
            run = payload.get("data", payload)
            logs = _dbt_run_logs(run)
            artifact = await _dbt_get_artifact(
                client,
                base_url=base_url,
                headers=headers,
                run_id=run_id,
                path="run_results.json",
                required=False,
            )
            return {"status": "ok", "run": run, "logs": logs or artifact or run, "agent_id": agent_id}
        if tool_name == "read_list_tests":
            run_id = str(arguments.get("run_id") or "")
            manifest = await _dbt_get_artifact(client, base_url=base_url, headers=headers, run_id=run_id, path="manifest.json")
            return {"status": "ok", "tests": _dbt_manifest_nodes(manifest, "test"), "agent_id": agent_id}
        if tool_name == "read_get_test_results":
            run_id = str(arguments.get("run_id") or "")
            unique_id = arguments.get("unique_id")
            artifact = await _dbt_get_artifact(client, base_url=base_url, headers=headers, run_id=run_id, path="run_results.json")
            results = artifact.get("results", []) if isinstance(artifact, dict) else []
            if unique_id:
                results = [result for result in results if isinstance(result, dict) and result.get("unique_id") == unique_id]
            else:
                results = [
                    result
                    for result in results
                    if isinstance(result, dict) and str(result.get("unique_id") or "").startswith("test.")
                ]
            return {"status": "ok", "results": results, "agent_id": agent_id}
        if tool_name == "read_get_source_freshness":
            run_id = str(arguments.get("run_id") or "")
            freshness = await _dbt_get_artifact(client, base_url=base_url, headers=headers, run_id=run_id, path="sources.json")
            return {"status": "ok", "freshness": freshness, "agent_id": agent_id}
        if tool_name == "read_get_model_source":
            run_id = str(arguments.get("run_id") or "")
            manifest = await _dbt_get_artifact(client, base_url=base_url, headers=headers, run_id=run_id, path="manifest.json")
            node = _dbt_find_node(
                manifest,
                resource_type="model",
                unique_id=arguments.get("unique_id"),
                name=arguments.get("name"),
            )
            if not node:
                raise McpExecutionError(404, "dbt model was not found in manifest.")
            return {
                "status": "ok",
                "model": node,
                "source": node.get("raw_code") or node.get("raw_sql") or node.get("compiled_code") or node.get("compiled_sql") or "",
                "agent_id": agent_id,
            }
        if tool_name == "read_list_exposures":
            run_id = str(arguments.get("run_id") or "")
            manifest = await _dbt_get_artifact(client, base_url=base_url, headers=headers, run_id=run_id, path="manifest.json")
            return {"status": "ok", "exposures": _dbt_manifest_exposures(manifest), "agent_id": agent_id}
        if tool_name == "read_get_model_docs":
            run_id = str(arguments.get("run_id") or "")
            manifest = await _dbt_get_artifact(client, base_url=base_url, headers=headers, run_id=run_id, path="manifest.json")
            node = _dbt_find_node(
                manifest,
                resource_type="model",
                unique_id=arguments.get("unique_id"),
                name=arguments.get("name"),
            )
            if not node:
                raise McpExecutionError(404, "dbt model was not found in manifest.")
            catalog = await _dbt_get_artifact(client, base_url=base_url, headers=headers, run_id=run_id, path="catalog.json", required=False)
            return {
                "status": "ok",
                "docs": {
                    "model": node,
                    "description": node.get("description") or "",
                    "columns": node.get("columns") or {},
                    "catalog": _dbt_catalog_node(catalog, node.get("unique_id")),
                },
                "agent_id": agent_id,
            }
        if tool_name == "write_trigger_run":
            job_id = str(arguments.get("job_id") or credentials.get("job_id") or "")
            if not job_id:
                raise McpExecutionError(400, "job_id is required.")
            response = await client.post(
                f"{base_url}/jobs/{_url_segment(job_id, field_name='job_id')}/run/",
                headers=headers,
                json=_dbt_trigger_payload(arguments, default_cause="Triggered by DataClaw MCP"),
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "triggered", "run": payload.get("data", payload), "agent_id": agent_id}
        if tool_name == "write_trigger_test":
            job_id = str(arguments.get("job_id") or credentials.get("job_id") or "")
            if not job_id:
                raise McpExecutionError(400, "job_id is required.")
            steps_override = arguments.get("steps_override") or ["dbt test"]
            response = await client.post(
                f"{base_url}/jobs/{_url_segment(job_id, field_name='job_id')}/run/",
                headers=headers,
                json=_dbt_trigger_payload(
                    arguments,
                    default_cause="Triggered by DataClaw MCP test tool",
                    default_steps=steps_override,
                ),
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "triggered", "run": payload.get("data", payload), "agent_id": agent_id}
        if tool_name == "write_cancel_run":
            run_id = str(arguments.get("run_id") or "")
            run_path = _url_segment(run_id, field_name="run_id")
            response = await client.post(f"{base_url}/runs/{run_path}/cancel/", headers=headers)
            response.raise_for_status()
            payload = response.json()
            return {"status": "cancelled", "run": payload.get("data", payload), "agent_id": agent_id}
        if tool_name in {"write_create_model", "write_update_model"}:
            sql = str(arguments.get("sql") or "")
            if not sql.strip():
                raise McpExecutionError(400, "sql is required.")
            path = _dbt_model_path(credentials, arguments)
            exists = path.exists()
            if tool_name == "write_create_model" and exists and not arguments.get("overwrite"):
                raise McpExecutionError(409, "dbt model already exists.")
            if tool_name == "write_update_model" and not exists:
                raise McpExecutionError(404, "dbt model does not exist.")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(sql.rstrip() + "\n", encoding="utf-8")
            return {"status": "created" if not exists else "updated", "path": str(path), "agent_id": agent_id}
        if tool_name in {"write_trigger_snapshot", "write_trigger_seed"}:
            job_id = str(arguments.get("job_id") or credentials.get("job_id") or "")
            if not job_id:
                raise McpExecutionError(400, "job_id is required.")
            command = "dbt snapshot" if tool_name == "write_trigger_snapshot" else "dbt seed"
            response = await client.post(
                f"{base_url}/jobs/{_url_segment(job_id, field_name='job_id')}/run/",
                headers=headers,
                json=_dbt_trigger_payload(
                    arguments,
                    default_cause=f"Triggered by DataClaw MCP {command} tool",
                    default_steps=[command],
                ),
            )
            response.raise_for_status()
            payload = response.json()
            return {"status": "triggered", "run": payload.get("data", payload), "agent_id": agent_id}
    raise McpExecutionError(404, f"Unsupported dbt MCP tool: {tool_name}")


async def _openai_tool(session: AsyncSession, tool_name: str, arguments: dict[str, Any], agent_id: str) -> dict[str, Any]:
    if tool_name != "read_list_models":
        raise McpExecutionError(404, f"Unsupported OpenAI MCP tool: {tool_name}")
    credentials = await _optional_connector_credentials(session, "openai")
    api_key = credentials.get("api_key")
    if not api_key:
        raise McpExecutionError(400, "OpenAI API key is not configured.")
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.get(
            "https://api.openai.com/v1/models",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        response.raise_for_status()
        payload = response.json()
    limit = int(arguments.get("limit") or 50)
    return {"status": "ok", "models": payload.get("data", [])[:limit], "agent_id": agent_id}


def _databricks_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    columns = payload.get("manifest", {}).get("schema", {}).get("columns", [])
    names = [str(column.get("name") or f"col_{index}") for index, column in enumerate(columns)]
    rows = payload.get("result", {}).get("data_array", [])
    return [dict(zip(names, row, strict=False)) for row in rows]


def _databricks_full_name(value: str, field_name: str = "full_name") -> str:
    parts = value.split(".")
    if not 1 <= len(parts) <= 3:
        raise McpExecutionError(400, f"{field_name} must be name, schema.name, or catalog.schema.name.")
    return ".".join(_safe_identifier(part, field_name) for part in parts)


def _iso_timestamp_literal(value: str) -> str:
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise McpExecutionError(400, "since must be an ISO-8601 timestamp.") from exc
    return parsed.isoformat()


async def _databricks_statement(
    client: httpx.AsyncClient,
    base_url: str,
    headers: dict[str, str],
    credentials: dict[str, Any],
    statement: str,
) -> dict[str, Any]:
    response = await client.post(
        f"{base_url}/sql/statements",
        headers=headers,
        json={
            "statement": statement,
            "warehouse_id": credentials.get("warehouse_id") or credentials.get("http_path") or "dataclaw",
            "wait_timeout": "30s",
        },
    )
    response.raise_for_status()
    return response.json()


async def _databricks_tool(session: AsyncSession, tool_name: str, arguments: dict[str, Any], agent_id: str) -> dict[str, Any]:
    credentials = await _connector_credentials(session, "databricks")
    adapter = adapter_for("databricks")
    base_url = adapter.base_url(credentials)
    headers = adapter.headers(credentials)
    async with httpx.AsyncClient(timeout=30) as client:
        if tool_name == "read_list_jobs":
            response = await client.get(f"{base_url}/jobs/list", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "jobs": response.json().get("jobs", []), "agent_id": agent_id}
        if tool_name == "read_list_clusters":
            response = await client.get(f"{base_url}/clusters/list", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "clusters": response.json().get("clusters", []), "agent_id": agent_id}
        if tool_name == "read_list_warehouses":
            response = await client.get(f"{base_url}/sql/warehouses", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "warehouses": response.json().get("warehouses", []), "agent_id": agent_id}
        if tool_name == "read_get_unity_asset":
            full_name = _databricks_full_name(str(arguments.get("full_name") or arguments.get("name") or ""))
            if not full_name:
                raise McpExecutionError(400, "full_name is required.")
            response = await client.get(f"{base_url}/unity-catalog/tables/{quote(full_name, safe='')}", headers=headers)
            response.raise_for_status()
            return {"status": "ok", "asset": response.json(), "agent_id": agent_id}
        if tool_name == "read_get_notebook":
            path = str(arguments.get("path") or "")
            if not path.startswith("/"):
                raise McpExecutionError(400, "path must be an absolute Databricks workspace path.")
            response = await client.get(
                f"{base_url}/workspace/export",
                headers=headers,
                params={"path": path, "format": str(arguments.get("format") or "SOURCE")},
            )
            response.raise_for_status()
            return {"status": "ok", "notebook": response.json(), "agent_id": agent_id}
        if tool_name == "read_get_run_logs":
            run_id = arguments.get("run_id")
            if run_id is None:
                raise McpExecutionError(400, "run_id is required.")
            response = await client.get(f"{base_url}/jobs/runs/get-output", headers=headers, params={"run_id": int(run_id)})
            response.raise_for_status()
            return {"status": "ok", "run_output": response.json(), "agent_id": agent_id}
        if tool_name == "read_list_tables":
            payload = await _databricks_statement(
                client,
                base_url,
                headers,
                credentials,
                "select table_schema, table_name from information_schema.tables order by table_schema, table_name",
            )
            rows = _databricks_rows(payload)
            return {
                "status": "ok",
                "tables": [{"schema": row.get("table_schema"), "name": row.get("table_name"), "table": row.get("table_name")} for row in rows],
                "agent_id": agent_id,
            }
        if tool_name == "read_get_schema":
            table = _safe_identifier(str(arguments.get("table") or ""))
            schema = _safe_identifier(str(arguments.get("schema") or "main"), "schema")
            payload = await _databricks_statement(
                client,
                base_url,
                headers,
                credentials,
                (
                    "select column_name, data_type, is_nullable from information_schema.columns "
                    f"where table_schema = '{schema}' and table_name = '{table}' order by ordinal_position"
                ),
            )
            rows = _databricks_rows(payload)
            if not rows:
                raise McpExecutionError(404, f"Databricks table not found: {schema}.{table}")
            return {
                "status": "ok",
                "schema": schema,
                "table": table,
                "columns": [
                    {"name": row.get("column_name"), "type": row.get("data_type"), "nullable": row.get("is_nullable") == "YES"}
                    for row in rows
                ],
                "agent_id": agent_id,
            }
        if tool_name == "read_query_select":
            try:
                sql = validate_read_only_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
            except UnsafeSqlError as exc:
                raise McpExecutionError(400, str(exc)) from exc
            payload = await _databricks_statement(client, base_url, headers, credentials, sql)
            return {"status": "ok", "sql": sql, "rows": _databricks_rows(payload), "agent_id": agent_id}
        if tool_name == "read_get_row_count":
            table = _safe_identifier(str(arguments.get("table") or ""))
            schema = _safe_identifier(str(arguments.get("schema") or "main"))
            payload = await _databricks_statement(client, base_url, headers, credentials, f"select count(*) as row_count from {schema}.{table}")
            rows = _databricks_rows(payload)
            return {"status": "ok", "schema": schema, "table": table, "row_count": int(rows[0].get("row_count", 0) if rows else 0), "agent_id": agent_id}
        if tool_name == "read_get_table_freshness":
            table = _safe_identifier(str(arguments.get("table") or ""))
            schema = _safe_identifier(str(arguments.get("schema") or "main"))
            payload = await _databricks_statement(
                client,
                base_url,
                headers,
                credentials,
                (
                    "select column_name, data_type from information_schema.columns "
                    f"where table_schema = '{schema}' and table_name = '{table}' order by ordinal_position"
                ),
            )
            candidates = [
                str(row.get("column_name"))
                for row in _databricks_rows(payload)
                if str(row.get("column_name", "")).lower() in {"updated_at", "created_at", "inserted_at", "loaded_at", "placed_at"}
                or str(row.get("data_type", "")).lower() in FRESHNESS_TYPES
            ]
            freshness = []
            for column_name in candidates[:10]:
                safe_column = _safe_identifier(column_name, "column")
                value_payload = await _databricks_statement(client, base_url, headers, credentials, f"select max({safe_column}) as max_value from {schema}.{table}")
                rows = _databricks_rows(value_payload)
                value = rows[0].get("max_value") if rows else None
                freshness.append({"column": safe_column, "max_value": value})
            latest = max((item["max_value"] for item in freshness if item["max_value"] is not None), default=None)
            return {"status": "ok", "schema": schema, "table": table, "freshest_at": latest, "columns": freshness, "total": len(freshness), "agent_id": agent_id}
        if tool_name == "read_get_lineage":
            asset = _databricks_full_name(str(arguments.get("asset") or arguments.get("table") or ""))
            limit = max(1, min(int(arguments.get("limit") or 100), 1000))
            payload = await _databricks_statement(
                client,
                base_url,
                headers,
                credentials,
                (
                    "select source_table_full_name, target_table_full_name, entity_type, entity_id, event_time "
                    "from system.access.table_lineage "
                    f"where source_table_full_name = '{asset}' or target_table_full_name = '{asset}' "
                    f"order by event_time desc limit {limit}"
                ),
            )
            return {"status": "ok", "asset": asset, "lineage": _databricks_rows(payload), "agent_id": agent_id}
        if tool_name == "read_get_query_history":
            limit = max(1, min(int(arguments.get("limit") or 100), 1000))
            since = str(arguments.get("since") or "").strip()
            where = ""
            if since:
                where = f"where start_time >= timestamp('{_iso_timestamp_literal(since)}') "
            payload = await _databricks_statement(
                client,
                base_url,
                headers,
                credentials,
                "select statement_id, executed_by, start_time, end_time, status, statement_text "
                f"from system.query.history {where}order by start_time desc limit {limit}",
            )
            return {"status": "ok", "queries": _databricks_rows(payload), "agent_id": agent_id}
        if tool_name == "write_trigger_job":
            job_id = arguments.get("job_id") or arguments.get("jobId")
            if job_id is None:
                raise McpExecutionError(400, "job_id is required.")
            response = await client.post(f"{base_url}/jobs/run-now", headers=headers, json={"job_id": int(job_id)})
            response.raise_for_status()
            return {"status": "triggered", "run": response.json(), "agent_id": agent_id}
        if tool_name == "write_run_notebook":
            path = str(arguments.get("path") or "")
            if not path.startswith("/"):
                raise McpExecutionError(400, "path must be an absolute Databricks workspace path.")
            payload = {
                "run_name": str(arguments.get("run_name") or "DataClaw notebook run"),
                "notebook_task": {"notebook_path": path, "base_parameters": arguments.get("params") or {}},
            }
            if arguments.get("cluster_id"):
                payload["existing_cluster_id"] = arguments.get("cluster_id")
            response = await client.post(
                f"{base_url}/jobs/runs/submit",
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
            return {"status": "triggered", "run": response.json(), "agent_id": agent_id}
        if tool_name in {"write_start_cluster", "write_stop_cluster"}:
            cluster_id = str(arguments.get("cluster_id") or arguments.get("id") or "")
            if not cluster_id:
                raise McpExecutionError(400, "cluster_id is required.")
            endpoint = "clusters/start" if tool_name == "write_start_cluster" else "clusters/delete"
            response = await client.post(f"{base_url}/{endpoint}", headers=headers, json={"cluster_id": cluster_id})
            response.raise_for_status()
            return {"status": "executed", "cluster_id": cluster_id, "agent_id": agent_id}
        if tool_name == "write_update_unity_grants":
            securable_type = str(arguments.get("securable_type") or "table").lower()
            if securable_type not in {"catalog", "schema", "table", "volume", "function"}:
                raise McpExecutionError(400, "Unsupported Databricks securable_type.")
            full_name = _databricks_full_name(str(arguments.get("full_name") or ""))
            changes = arguments.get("changes")
            if not isinstance(changes, list) or not changes:
                raise McpExecutionError(400, "changes must be a non-empty list.")
            response = await client.patch(
                f"{base_url.replace('/api/2.0', '/api/2.1')}/unity-catalog/permissions/{securable_type}/{quote(full_name, safe='')}",
                headers=headers,
                json={"changes": changes},
            )
            response.raise_for_status()
            return {"status": "executed", "permissions": response.json(), "agent_id": agent_id}
        if tool_name in {"write_execute_sql", "write_create_table", "write_create_view"}:
            if tool_name == "write_create_view":
                view = _databricks_full_name(str(arguments.get("view") or ""))
                select_sql = validate_read_only_sql(str(arguments.get("select_sql") or ""))
                sql = f"create or replace view {view} as {select_sql}"
            else:
                sql = _sql_for_write_tool(tool_name, arguments, "databricks")
            try:
                decision = validate_write_sql(sql)
            except UnsafeSqlError as exc:
                raise McpExecutionError(400, str(exc)) from exc
            if decision.action == "requires_approval" and not arguments.get("__approved"):
                return await _pending_mcp_approval(
                    session,
                    agent_id=agent_id,
                    connector_slug="databricks",
                    tool_name=tool_name,
                    arguments=arguments,
                    title=f"Agent {agent_id} wants to run Databricks SQL",
                )
            payload = await _databricks_statement(client, base_url, headers, credentials, decision.sql)
            return {"status": "executed", "sql": decision.sql, "statement_id": payload.get("statement_id"), "agent_id": agent_id}
    raise McpExecutionError(404, f"Unsupported Databricks MCP tool: {tool_name}")


async def _bigquery_tool(session: AsyncSession, tool_name: str, arguments: dict[str, Any], agent_id: str) -> dict[str, Any]:
    credentials = await _connector_credentials(session, "bigquery")
    try:
        from google.cloud import bigquery  # type: ignore
        from google.oauth2 import service_account  # type: ignore
    except ImportError as exc:
        raise McpExecutionError(
            501,
            "BigQuery MCP execution requires the optional google-cloud-bigquery and google-auth packages.",
        ) from exc

    project_id = _safe_bigquery_project(str(credentials.get("project_id") or ""))
    if not project_id:
        raise McpExecutionError(400, "project_id is required.")
    service_account_json = credentials.get("service_account_json")
    client_credentials = None
    client_options = None
    emulator_host = str(credentials.get("emulator_host") or "").strip().rstrip("/")
    if emulator_host:
        from google.auth.credentials import AnonymousCredentials  # type: ignore

        client_credentials = AnonymousCredentials()
        client_options = {"api_endpoint": emulator_host}
    elif service_account_json:
        import json

        try:
            info = json.loads(service_account_json) if isinstance(service_account_json, str) else service_account_json
            client_credentials = service_account.Credentials.from_service_account_info(info)
        except Exception as exc:
            raise McpExecutionError(400, "Invalid BigQuery service_account_json.") from exc
    client = bigquery.Client(project=project_id, credentials=client_credentials, client_options=client_options)

    def run_query_sync(sql: str, job_config: Any | None = None) -> list[dict[str, Any]]:
        query_job = client.query(sql, job_config=job_config)
        return [dict(row.items()) for row in query_job.result()]

    async def run_query(sql: str, job_config: Any | None = None) -> list[dict[str, Any]]:
        try:
            return await asyncio.wait_for(asyncio.to_thread(run_query_sync, sql, job_config), timeout=20)
        except TimeoutError as exc:
            raise McpExecutionError(504, "BigQuery query timed out.") from exc

    def bq_region() -> str:
        region = str(arguments.get("location") or credentials.get("location") or "region-us").strip().lower()
        if not re.fullmatch(r"region-[a-z0-9-]+", region):
            raise McpExecutionError(400, "BigQuery location must be a region qualifier like region-us.")
        return region

    def bq_table_id(name: str, *, default_dataset: bool = True) -> str:
        raw = str(arguments.get(name) or "")
        if not raw:
            raise McpExecutionError(400, f"{name} is required.")
        parts = raw.split(".")
        if len(parts) == 3:
            return f"{_safe_bigquery_project(parts[0])}.{_safe_identifier(parts[1], 'dataset')}.{_safe_identifier(parts[2], name)}"
        if len(parts) == 2:
            return f"{project_id}.{_safe_identifier(parts[0], 'dataset')}.{_safe_identifier(parts[1], name)}"
        if len(parts) == 1:
            dataset = str(arguments.get("dataset") or credentials.get("dataset") or "") if default_dataset else ""
            if not dataset:
                raise McpExecutionError(400, f"dataset is required when {name} is not fully qualified.")
            return f"{project_id}.{_safe_identifier(dataset, 'dataset')}.{_safe_identifier(parts[0], name)}"
        raise McpExecutionError(400, f"{name} must be table, dataset.table, or project.dataset.table.")

    if tool_name == "read_list_jobs":
        jobs = list(client.list_jobs(max_results=int(arguments.get("limit") or 20)))
        return {"status": "ok", "jobs": [{"job_id": job.job_id, "state": job.state} for job in jobs], "agent_id": agent_id}
    if tool_name == "read_list_datasets":
        datasets = list(client.list_datasets(project=project_id))
        return {"status": "ok", "datasets": [{"dataset_id": dataset.dataset_id} for dataset in datasets], "agent_id": agent_id}
    if tool_name == "read_list_tables":
        dataset_id = str(arguments.get("dataset") or credentials.get("dataset") or "")
        if not dataset_id:
            raise McpExecutionError(400, "dataset is required for BigQuery read_list_tables.")
        tables = list(client.list_tables(f"{project_id}.{dataset_id}"))
        return {"status": "ok", "tables": [{"schema": dataset_id, "name": table.table_id, "table": table.table_id} for table in tables], "agent_id": agent_id}
    if tool_name == "read_get_schema":
        dataset = _safe_identifier(str(arguments.get("dataset") or credentials.get("dataset") or ""))
        table = _safe_identifier(str(arguments.get("table") or ""))
        bq_table = client.get_table(f"{project_id}.{dataset}.{table}")
        return {
            "status": "ok",
            "schema": dataset,
            "table": table,
            "columns": [{"name": field.name, "type": field.field_type, "nullable": field.mode != "REQUIRED"} for field in bq_table.schema],
            "agent_id": agent_id,
        }
    if tool_name == "read_query_select":
        sql = validate_read_only_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
        return {"status": "ok", "sql": sql, "rows": await run_query(sql), "agent_id": agent_id}
    if tool_name == "read_get_row_count":
        dataset = _safe_identifier(str(arguments.get("dataset") or credentials.get("dataset") or ""))
        table = _safe_identifier(str(arguments.get("table") or ""))
        bq_table = await asyncio.to_thread(client.get_table, f"{project_id}.{dataset}.{table}")
        row_count = getattr(bq_table, "num_rows", None)
        if row_count is None:
            rows = await asyncio.to_thread(lambda: list(client.list_rows(bq_table)))
            row_count = len(rows)
        return {"status": "ok", "schema": dataset, "table": table, "row_count": int(row_count or 0), "agent_id": agent_id}
    if tool_name == "read_search_columns":
        pattern = str(arguments.get("pattern") or "").strip().lower()
        if len(pattern) < 2:
            raise McpExecutionError(400, "pattern must be at least 2 characters.")
        dataset = str(arguments.get("dataset") or credentials.get("dataset") or "")
        if not dataset:
            raise McpExecutionError(400, "dataset is required for BigQuery read_search_columns.")
        matches = []
        for table_item in client.list_tables(f"{project_id}.{dataset}"):
            bq_table = client.get_table(table_item.reference)
            for field in bq_table.schema:
                if pattern in field.name.lower():
                    matches.append({"schema": dataset, "table": table_item.table_id, "column": field.name, "type": field.field_type})
                    if len(matches) >= max(1, min(int(arguments.get("limit") or 100), 1000)):
                        return {"status": "ok", "columns": matches, "total": len(matches), "agent_id": agent_id}
        return {"status": "ok", "columns": matches, "total": len(matches), "agent_id": agent_id}
    if tool_name == "read_get_table_freshness":
        dataset = _safe_identifier(str(arguments.get("dataset") or credentials.get("dataset") or ""))
        table = _safe_identifier(str(arguments.get("table") or ""))
        bq_table = client.get_table(f"{project_id}.{dataset}.{table}")
        modified = getattr(bq_table, "modified", None)
        freshest_at = modified.isoformat() if hasattr(modified, "isoformat") else None
        return {"status": "ok", "schema": dataset, "table": table, "freshest_at": freshest_at, "columns": [], "agent_id": agent_id}
    if tool_name == "read_get_storage_size":
        dataset = _safe_identifier(str(arguments.get("dataset") or credentials.get("dataset") or ""))
        table = _safe_identifier(str(arguments.get("table") or ""))
        bq_table = client.get_table(f"{project_id}.{dataset}.{table}")
        return {"status": "ok", "schema": dataset, "table": table, "size_bytes": int(getattr(bq_table, "num_bytes", 0) or 0), "agent_id": agent_id}
    if tool_name == "read_explain_query":
        sql = validate_read_only_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = client.query(sql, job_config=job_config)
        plan = getattr(query_job, "query_plan", None) or []
        return {
            "status": "ok",
            "sql": sql,
            "plan": plan,
            "total_bytes_processed": int(getattr(query_job, "total_bytes_processed", 0) or 0),
            "agent_id": agent_id,
        }
    if tool_name == "read_get_query_history":
        limit = max(1, min(int(arguments.get("limit") or 100), 1000))
        since = str(arguments.get("since") or "").strip()
        params = []
        where = ""
        if since:
            params.append(bigquery.ScalarQueryParameter("since", "TIMESTAMP", since))
            where = "where creation_time >= @since "
        job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
        rows = await run_query(
            "select job_id, user_email, creation_time, start_time, end_time, state, statement_type, query "
            f"from `{project_id}`.`{bq_region()}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT "
            f"{where}order by creation_time desc limit {limit}",
            job_config=job_config,
        )
        return {"status": "ok", "queries": rows, "total": len(rows), "agent_id": agent_id}
    if tool_name == "read_get_slot_usage":
        since = str(arguments.get("since") or "").strip()
        params = []
        where = ""
        if since:
            params.append(bigquery.ScalarQueryParameter("since", "TIMESTAMP", since))
            where = "where creation_time >= @since "
        job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
        rows = await run_query(
            "select sum(total_slot_ms) as total_slot_ms "
            f"from `{project_id}`.`{bq_region()}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT {where}",
            job_config=job_config,
        )
        return {"status": "ok", "slot_usage": rows[0] if rows else {"total_slot_ms": 0}, "agent_id": agent_id}
    if tool_name == "write_load_from_gcs":
        uri = str(arguments.get("uri") or "")
        destination = bq_table_id("table")
        if not uri:
            raise McpExecutionError(400, "uri is required.")
        source_format = str(arguments.get("source_format") or "CSV").upper()
        allowed_formats = {"CSV", "NEWLINE_DELIMITED_JSON", "PARQUET", "AVRO", "ORC"}
        if source_format not in allowed_formats:
            raise McpExecutionError(400, f"source_format must be one of {', '.join(sorted(allowed_formats))}.")
        job_config = bigquery.LoadJobConfig(source_format=source_format, autodetect=bool(arguments.get("autodetect", True)))
        job = client.load_table_from_uri(uri, destination, job_config=job_config)
        job.result()
        return {"status": "executed", "job_id": job.job_id, "destination_table": destination, "agent_id": agent_id}
    if tool_name == "write_export_to_gcs":
        source = bq_table_id("table")
        uri = str(arguments.get("uri") or "")
        if not uri:
            raise McpExecutionError(400, "uri is required.")
        job = client.extract_table(source, uri)
        job.result()
        return {"status": "executed", "job_id": job.job_id, "source_table": source, "agent_id": agent_id}
    if tool_name == "write_create_dataset":
        dataset = _safe_identifier(str(arguments.get("dataset") or ""))
        dataset_ref = f"{project_id}.{dataset}"
        created = await asyncio.to_thread(
            client.create_dataset,
            dataset_ref,
            exists_ok=bool(arguments.get("exists_ok", True)),
        )
        return {"status": "executed", "dataset": getattr(created, "dataset_id", dataset), "agent_id": agent_id}
    if tool_name in {"write_execute_sql", "write_create_table", "write_run_query_save_to_table", "write_create_view"}:
        if tool_name == "write_run_query_save_to_table":
            destination = bq_table_id("destination_table")
            select_sql = validate_read_only_sql(str(arguments.get("select_sql") or ""))
            sql = f"create or replace table `{destination}` as {select_sql}"
        elif tool_name == "write_create_view":
            destination = bq_table_id("view")
            select_sql = validate_read_only_sql(str(arguments.get("select_sql") or ""))
            sql = f"create or replace view `{destination}` as {select_sql}"
        else:
            sql = _sql_for_write_tool(tool_name, arguments, "bigquery")
        decision = validate_write_sql(sql)
        if decision.action == "requires_approval" and not arguments.get("__approved"):
            return await _pending_mcp_approval(
                session,
                agent_id=agent_id,
                connector_slug="bigquery",
                tool_name=tool_name,
                arguments=arguments,
                title=f"Agent {agent_id} wants to run BigQuery SQL",
            )
        if tool_name == "write_create_table":
            dataset = _safe_identifier(str(arguments.get("dataset") or arguments.get("schema") or credentials.get("dataset") or ""))
            table_name = _safe_identifier(str(arguments.get("table") or ""), "table")
            columns = arguments.get("columns")
            if not isinstance(columns, list) or not columns:
                raise McpExecutionError(400, "columns must be a non-empty list.")

            def create_table() -> Any:
                schema = []
                for column in columns:
                    if not isinstance(column, dict):
                        raise McpExecutionError(400, "Each column must be an object.")
                    field_name = _safe_identifier(str(column.get("name") or ""), "column")
                    field_type = str(column.get("type") or "STRING").strip().upper()
                    field_type = {
                        "TEXT": "STRING",
                        "INTEGER": "INT64",
                        "INT": "INT64",
                        "REAL": "FLOAT64",
                        "DOUBLE": "FLOAT64",
                        "BOOLEAN": "BOOL",
                    }.get(field_type, field_type)
                    mode = "NULLABLE" if column.get("nullable", True) else "REQUIRED"
                    schema.append(bigquery.SchemaField(field_name, field_type, mode=mode))
                table_obj = bigquery.Table(f"{project_id}.{dataset}.{table_name}", schema=schema)
                return client.create_table(table_obj, exists_ok=bool(arguments.get("exists_ok", True)))

            created = await asyncio.to_thread(create_table)
            return {
                "status": "executed",
                "sql": decision.sql,
                "table": getattr(created, "table_id", table_name),
                "agent_id": agent_id,
            }

        def execute_query() -> str:
            query_job = client.query(decision.sql)
            query_job.result()
            return query_job.job_id

        job_id = await asyncio.to_thread(execute_query)
        return {"status": "executed", "sql": decision.sql, "job_id": job_id, "agent_id": agent_id}
    raise McpExecutionError(404, f"Unsupported BigQuery MCP tool: {tool_name}")


async def _snowflake_tool(session: AsyncSession, tool_name: str, arguments: dict[str, Any], agent_id: str) -> dict[str, Any]:
    credentials = await _connector_credentials(session, "snowflake")
    try:
        import snowflake.connector  # type: ignore
    except ImportError as exc:
        raise McpExecutionError(
            501,
            "Snowflake MCP execution requires the optional snowflake-connector-python package.",
        ) from exc

    def connect():
        kwargs: dict[str, Any] = {
            "account": normalize_snowflake_account(str(credentials["account"])),
            "warehouse": credentials["warehouse"],
            "database": credentials["database"],
            "schema": credentials.get("schema") or "PUBLIC",
            "user": credentials["user"],
        }
        if credentials.get("private_key"):
            from cryptography.hazmat.primitives import serialization

            key = serialization.load_pem_private_key(
                str(credentials["private_key"]).encode(),
                password=str(credentials.get("private_key_passphrase") or "").encode() or None,
            )
            kwargs["private_key"] = key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        else:
            kwargs["password"] = credentials["password"]
        return snowflake.connector.connect(**kwargs)

    def fetch(sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        try:
            with connect() as conn:
                with conn.cursor(snowflake.connector.DictCursor) as cursor:
                    cursor.execute(sql, params)
                    return list(cursor.fetchall())
        except Exception as exc:
            raise McpExecutionError(400, f"Snowflake MCP request failed: {exc}") from exc

    def execute(sql: str) -> int | None:
        try:
            with connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    return cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else None
        except Exception as exc:
            raise McpExecutionError(400, f"Snowflake MCP request failed: {exc}") from exc

    schema = _safe_identifier(str(arguments.get("schema") or credentials.get("schema") or "PUBLIC"))
    if tool_name == "read_list_tables":
        rows = await asyncio.to_thread(
            fetch,
            "select table_schema, table_name from information_schema.tables "
            f"where table_schema = '{schema}' order by table_schema, table_name",
        )
        return {"status": "ok", "tables": [{"schema": row["TABLE_SCHEMA"], "name": row["TABLE_NAME"], "table": row["TABLE_NAME"]} for row in rows], "agent_id": agent_id}
    if tool_name == "read_get_schema":
        table = _safe_identifier(str(arguments.get("table") or ""))
        rows = await asyncio.to_thread(
            fetch,
            "select column_name, data_type, is_nullable from information_schema.columns "
            f"where table_schema = '{schema}' and table_name = '{table.upper()}' order by ordinal_position",
        )
        if not rows:
            raise McpExecutionError(404, f"Snowflake table not found: {schema}.{table}")
        return {
            "status": "ok",
            "schema": schema,
            "table": table,
            "columns": [{"name": row["COLUMN_NAME"], "type": row["DATA_TYPE"], "nullable": row["IS_NULLABLE"] == "YES"} for row in rows],
            "agent_id": agent_id,
        }
    if tool_name == "read_query_select":
        sql = validate_read_only_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
        return {"status": "ok", "sql": sql, "rows": await asyncio.to_thread(fetch, sql), "agent_id": agent_id}
    if tool_name == "read_get_row_count":
        table = _safe_identifier(str(arguments.get("table") or ""))
        rows = await asyncio.to_thread(fetch, f'select count(*) as ROW_COUNT from "{schema}"."{table}"')
        return {"status": "ok", "schema": schema, "table": table, "row_count": int(rows[0].get("ROW_COUNT", 0) if rows else 0), "agent_id": agent_id}
    if tool_name == "read_sample_rows":
        table = _safe_identifier(str(arguments.get("table") or ""))
        limit = max(1, min(int(arguments.get("limit") or 100), 1000))
        rows = await asyncio.to_thread(fetch, f'select * from "{schema}"."{table}" limit {limit}')
        return {"status": "ok", "schema": schema, "table": table, "rows": rows, "total": len(rows), "agent_id": agent_id}
    if tool_name == "read_search_columns":
        pattern = str(arguments.get("pattern") or "").strip().lower()
        if len(pattern) < 2:
            raise McpExecutionError(400, "pattern must be at least 2 characters.")
        limit = max(1, min(int(arguments.get("limit") or 100), 1000))
        rows = await asyncio.to_thread(
            fetch,
            "select table_schema, table_name, column_name, data_type from information_schema.columns "
            f"where table_schema = '{schema}' and lower(column_name) like %s "
            f"order by table_schema, table_name, ordinal_position limit {limit}",
            (f"%{pattern}%",),
        )
        return {
            "status": "ok",
            "columns": [
                {"schema": row["TABLE_SCHEMA"], "table": row["TABLE_NAME"], "column": row["COLUMN_NAME"], "type": row["DATA_TYPE"]}
                for row in rows
            ],
            "total": len(rows),
            "agent_id": agent_id,
        }
    if tool_name == "read_get_column_stats":
        table = _safe_identifier(str(arguments.get("table") or ""))
        rows = await asyncio.to_thread(
            fetch,
            "select column_name, data_type from information_schema.columns "
            f"where table_schema = '{schema}' and table_name = '{table.upper()}' order by ordinal_position",
        )
        stats = []
        for row in rows[: max(1, min(int(arguments.get("limit") or 50), 100))]:
            column = _safe_identifier(str(row["COLUMN_NAME"]), "column")
            stat_rows = await asyncio.to_thread(
                fetch,
                f'select count(*) as ROW_COUNT, count("{column}") as NON_NULL_COUNT, '
                f'count(distinct "{column}") as DISTINCT_COUNT from "{schema}"."{table}"',
            )
            stat = stat_rows[0] if stat_rows else {}
            row_count = int(stat.get("ROW_COUNT") or 0)
            non_null_count = int(stat.get("NON_NULL_COUNT") or 0)
            stats.append(
                {
                    "column": column,
                    "type": row["DATA_TYPE"],
                    "row_count": row_count,
                    "non_null_count": non_null_count,
                    "null_count": row_count - non_null_count,
                    "distinct_count": int(stat.get("DISTINCT_COUNT") or 0),
                }
            )
        return {"status": "ok", "schema": schema, "table": table, "columns": stats, "total": len(stats), "agent_id": agent_id}
    if tool_name == "read_get_table_freshness":
        table = _safe_identifier(str(arguments.get("table") or ""))
        rows = await asyncio.to_thread(
            fetch,
            "select column_name, data_type from information_schema.columns "
            f"where table_schema = '{schema}' and table_name = '{table.upper()}' order by ordinal_position",
        )
        candidates = [
            str(row["COLUMN_NAME"])
            for row in rows
            if str(row["COLUMN_NAME"]).lower() in {"updated_at", "created_at", "inserted_at", "loaded_at", "placed_at"}
            or str(row["DATA_TYPE"]).lower() in FRESHNESS_TYPES
        ]
        freshness = []
        for column in candidates[:10]:
            safe_column = _safe_identifier(column, "column")
            value_rows = await asyncio.to_thread(fetch, f'select max("{safe_column}") as MAX_VALUE from "{schema}"."{table}"')
            value = value_rows[0].get("MAX_VALUE") if value_rows else None
            freshness.append({"column": safe_column, "max_value": value.isoformat() if hasattr(value, "isoformat") else value})
        latest = max((item["max_value"] for item in freshness if item["max_value"] is not None), default=None)
        return {"status": "ok", "schema": schema, "table": table, "freshest_at": latest, "columns": freshness, "total": len(freshness), "agent_id": agent_id}
    if tool_name == "read_get_storage_size":
        table = _safe_identifier(str(arguments.get("table") or ""))
        rows = await asyncio.to_thread(
            fetch,
            "select bytes as SIZE_BYTES from information_schema.tables "
            f"where table_schema = '{schema}' and table_name = '{table.upper()}'",
        )
        size_bytes = rows[0].get("SIZE_BYTES") if rows else None
        return {"status": "ok", "schema": schema, "table": table, "size_bytes": int(size_bytes) if size_bytes is not None else None, "agent_id": agent_id}
    if tool_name == "read_explain_query":
        sql = validate_read_only_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
        rows = await asyncio.to_thread(fetch, f"explain using text {sql}")
        return {"status": "ok", "sql": sql, "plan": rows, "agent_id": agent_id}
    if tool_name == "read_list_users":
        rows = await asyncio.to_thread(fetch, "show users")
        return {"status": "ok", "users": rows, "total": len(rows), "agent_id": agent_id}
    if tool_name == "read_list_warehouses":
        rows = await asyncio.to_thread(fetch, "show warehouses")
        return {"status": "ok", "warehouses": rows, "total": len(rows), "agent_id": agent_id}
    if tool_name == "read_list_pipes":
        rows = await asyncio.to_thread(fetch, f'show pipes in schema "{schema}"')
        return {"status": "ok", "schema": schema, "pipes": rows, "total": len(rows), "agent_id": agent_id}
    if tool_name == "read_list_streams":
        rows = await asyncio.to_thread(fetch, f'show streams in schema "{schema}"')
        return {"status": "ok", "schema": schema, "streams": rows, "total": len(rows), "agent_id": agent_id}
    if tool_name == "read_list_tasks":
        rows = await asyncio.to_thread(fetch, f'show tasks in schema "{schema}"')
        return {"status": "ok", "schema": schema, "tasks": rows, "total": len(rows), "agent_id": agent_id}
    if tool_name == "read_list_grants":
        table = _safe_identifier(str(arguments.get("table") or ""))
        rows = await asyncio.to_thread(fetch, f'show grants on table "{schema}"."{table}"')
        return {"status": "ok", "schema": schema, "table": table, "grants": rows, "total": len(rows), "agent_id": agent_id}
    if tool_name in {"read_get_query_history", "read_query_history"}:
        limit = max(1, min(int(arguments.get("limit") or 100), 1000))
        since = str(arguments.get("since") or "").strip()
        where = "where start_time >= %s " if since else ""
        params = (_iso_timestamp_literal(since),) if since else None
        rows = await asyncio.to_thread(
            fetch,
            "select query_id, query_text, database_name, schema_name, warehouse_name, start_time, end_time, execution_status "
            f"from table(information_schema.query_history()) {where}"
            f"order by start_time desc limit {limit}",
            params,
        )
        return {"status": "ok", "queries": rows, "total": len(rows), "agent_id": agent_id}
    if tool_name == "read_get_credit_usage":
        since = str(arguments.get("since") or "").strip()
        where = "where start_time >= %s " if since else ""
        params = (_iso_timestamp_literal(since),) if since else None
        rows = await asyncio.to_thread(
            fetch,
            "select warehouse_name, start_time, end_time, credits_used "
            f"from snowflake.account_usage.warehouse_metering_history {where}"
            "order by start_time desc limit 1000",
            params,
        )
        return {"status": "ok", "usage": rows, "total": len(rows), "agent_id": agent_id}
    if tool_name in {"write_resume_warehouse", "write_suspend_warehouse"}:
        warehouse = _safe_identifier(str(arguments.get("name") or arguments.get("warehouse") or ""), "warehouse")
        action = "resume" if tool_name == "write_resume_warehouse" else "suspend"
        affected = await asyncio.to_thread(execute, f'alter warehouse "{warehouse}" {action}')
        return {"status": "executed", "warehouse": warehouse, "affected_rows": affected, "agent_id": agent_id}
    if tool_name == "write_create_pipe":
        pipe = _safe_identifier(str(arguments.get("name") or ""), "pipe")
        table = _safe_identifier(str(arguments.get("table") or ""), "table")
        stage = _safe_identifier(str(arguments.get("stage") or ""), "stage")
        file_format = str(arguments.get("file_format") or "CSV").upper()
        if file_format not in {"CSV", "JSON", "PARQUET", "AVRO", "ORC"}:
            raise McpExecutionError(400, "Unsupported Snowflake pipe file_format.")
        sql = f'create pipe if not exists "{schema}"."{pipe}" as copy into "{schema}"."{table}" from @"{stage}" file_format = (type = {file_format})'
        affected = await asyncio.to_thread(execute, sql)
        return {"status": "executed", "sql": sql, "affected_rows": affected, "agent_id": agent_id}
    if tool_name == "write_create_task":
        task = _safe_identifier(str(arguments.get("name") or ""), "task")
        warehouse = _safe_identifier(str(arguments.get("warehouse") or credentials.get("warehouse") or ""), "warehouse")
        schedule = str(arguments.get("schedule") or "USING CRON 0 * * * * UTC").replace("'", "''")
        task_sql = validate_read_only_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
        sql = f'create task if not exists "{schema}"."{task}" warehouse = "{warehouse}" schedule = \'{schedule}\' as {task_sql}'
        affected = await asyncio.to_thread(execute, sql)
        return {"status": "executed", "sql": sql, "affected_rows": affected, "agent_id": agent_id}
    if tool_name in {
        "write_execute_sql",
        "write_create_table",
        "write_create_view",
        "write_insert_rows",
        "write_update_rows",
        "write_delete_rows",
        "write_create_index",
    }:
        sql = _sql_for_write_tool(tool_name, arguments, "snowflake")
        decision = validate_write_sql(sql)
        if decision.action == "requires_approval" and not arguments.get("__approved"):
            return await _pending_mcp_approval(
                session,
                agent_id=agent_id,
                connector_slug="snowflake",
                tool_name=tool_name,
                arguments=arguments,
                title=f"Agent {agent_id} wants to run Snowflake SQL",
            )
        affected = await asyncio.to_thread(execute, decision.sql)
        return {"status": "executed", "sql": decision.sql, "affected_rows": affected, "agent_id": agent_id}
    raise McpExecutionError(404, f"Unsupported Snowflake MCP tool: {tool_name}")


async def _sql_datastore_tool(
    *,
    session: AsyncSession,
    connector_slug: str,
    tool_name: str,
    arguments: dict[str, Any],
    agent: Agent,
    user_email: str,
) -> dict[str, Any]:
    credentials = await _connector_credentials(session, connector_slug)
    engine = create_async_engine(_sqlalchemy_url_for_datastore(connector_slug, credentials), pool_pre_ping=True)
    try:
        if tool_name == "read_list_tables":
            return await _sql_datastore_list_tables(engine, connector_slug, credentials, arguments)
        if tool_name == "read_get_schema":
            return await _sql_datastore_get_schema(engine, connector_slug, credentials, arguments)
        if tool_name == "read_query_select":
            return await _sql_datastore_query_select(engine, arguments)
        if tool_name == "read_get_row_count":
            return await _sql_datastore_row_count(engine, connector_slug, arguments)
        if tool_name == "read_sample_rows":
            return await _sql_datastore_sample_rows(engine, connector_slug, credentials, arguments)
        if tool_name == "read_search_columns":
            return await _sql_datastore_search_columns(engine, connector_slug, credentials, arguments)
        if tool_name == "read_get_column_stats":
            return await _sql_datastore_column_stats(engine, connector_slug, credentials, arguments)
        if tool_name == "read_get_table_freshness":
            return await _sql_datastore_table_freshness(engine, connector_slug, credentials, arguments)
        if tool_name == "read_get_storage_size":
            return await _sql_datastore_storage_size(engine, connector_slug, credentials, arguments)
        if tool_name == "read_explain_query":
            return await _sql_datastore_explain_query(engine, connector_slug, arguments)
        if tool_name == "read_list_users":
            return await _sql_datastore_list_users(engine, connector_slug)
        if tool_name == "read_list_grants":
            return await _sql_datastore_list_grants(engine, connector_slug, credentials, arguments)
        if tool_name == "read_get_query_history":
            return await _sql_datastore_query_history(engine, connector_slug, arguments)
        if connector_slug == "redshift" and tool_name == "read_get_workload_management":
            return await _redshift_workload_management(engine)
        if connector_slug == "redshift" and tool_name == "read_get_disk_usage":
            return await _redshift_disk_usage(engine, arguments)
        if connector_slug == "redshift" and tool_name == "read_list_clusters":
            return await asyncio.to_thread(_redshift_list_clusters, await _connector_credentials(session, "redshift"))
        if connector_slug == "redshift" and tool_name in {"write_pause_cluster", "write_resume_cluster"}:
            credentials = await _connector_credentials(session, "redshift")
            return await asyncio.to_thread(_redshift_cluster_action, credentials, arguments, tool_name, agent.id)
        if tool_name in {
            "write_execute_sql",
            "write_create_table",
            "write_create_view",
            "write_insert_rows",
            "write_update_rows",
            "write_delete_rows",
            "write_grant_permission",
            "write_create_index",
        }:
            return await _sql_datastore_write(
                session=session,
                engine=engine,
                connector_slug=connector_slug,
                agent=agent,
                tool_name=tool_name,
                arguments=arguments,
                user_email=user_email,
            )
    finally:
        await engine.dispose()
    raise McpExecutionError(404, f"Unsupported {connector_slug} MCP tool: {tool_name}")


async def _sql_datastore_list_tables(
    engine: AsyncEngine, connector_slug: str, credentials: dict[str, Any], arguments: dict[str, Any]
) -> dict[str, Any]:
    requested_schema = str(arguments.get("schema") or "").strip()
    async with engine.connect() as conn:
        if connector_slug == "mysql":
            schema = requested_schema or credentials["database"]
            rows = (
                await conn.execute(
                    text(
                        "select table_name as table_name, table_schema as table_schema "
                        "from information_schema.tables "
                        "where table_schema = :schema and table_type = 'BASE TABLE' "
                        "order by table_name"
                    ),
                    {"schema": schema},
                )
            ).mappings().all()
        else:
            schema_filter = "and table_schema = :schema " if requested_schema else ""
            params = {"schema": requested_schema} if requested_schema else {}
            rows = (
                await conn.execute(
                    text(
                        "select table_name, table_schema "
                        "from information_schema.tables "
                        "where table_schema not in ('pg_catalog', 'information_schema') "
                        f"{schema_filter}"
                        "and table_type = 'BASE TABLE' "
                        "order by table_schema, table_name"
                    ),
                    params,
                )
            ).mappings().all()
    return {
        "status": "ok",
        "tables": [
            {"schema": row["table_schema"], "name": row["table_name"], "table": row["table_name"]} for row in rows
        ],
    }


async def _sql_datastore_get_schema(
    engine: AsyncEngine,
    connector_slug: str,
    credentials: dict[str, Any],
    arguments: dict[str, Any],
) -> dict[str, Any]:
    table_name = _safe_identifier(str(arguments.get("table") or ""))
    schema = str(arguments.get("schema") or _default_schema_for_datastore(connector_slug, credentials) or "public")
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "select column_name, data_type, is_nullable "
                    "from information_schema.columns "
                    "where table_schema = :schema and table_name = :table "
                    "order by ordinal_position"
                ),
                {"schema": schema, "table": table_name},
            )
        ).mappings().all()
    if not rows:
        raise McpExecutionError(404, f"{connector_slug} table not found: {schema}.{table_name}")
    return {
        "status": "ok",
        "schema": schema,
        "table": table_name,
        "columns": [
            {
                "name": _mapping_value(row, "column_name"),
                "type": _mapping_value(row, "data_type"),
                "nullable": _mapping_value(row, "is_nullable") == "YES",
            }
            for row in rows
        ],
    }


async def _sql_datastore_query_select(engine: AsyncEngine, arguments: dict[str, Any]) -> dict[str, Any]:
    try:
        sql = validate_read_only_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
    except UnsafeSqlError as exc:
        raise McpExecutionError(400, str(exc)) from exc
    async with engine.connect() as conn:
        result = await conn.execute(text(sql))
        rows = [dict(row._mapping) for row in result.fetchall()]
    return {"status": "ok", "sql": sql, "rows": rows}


async def _sql_datastore_row_count(engine: AsyncEngine, connector_slug: str, arguments: dict[str, Any]) -> dict[str, Any]:
    table_name = _safe_identifier(str(arguments.get("table") or ""))
    schema = str(arguments.get("schema") or "").strip() or None
    quoted = _qualified_table(connector_slug, table_name, schema)
    async with engine.connect() as conn:
        count = await conn.scalar(text(f"select count(*) from {quoted}"))
    return {"status": "ok", "schema": schema, "table": table_name, "row_count": int(count or 0)}


async def _sql_datastore_sample_rows(
    engine: AsyncEngine,
    connector_slug: str,
    credentials: dict[str, Any],
    arguments: dict[str, Any],
) -> dict[str, Any]:
    table_name = _safe_identifier(str(arguments.get("table") or ""))
    schema = str(arguments.get("schema") or _default_schema_for_datastore(connector_slug, credentials) or "").strip() or None
    limit = max(1, min(int(arguments.get("limit") or 100), 1000))
    table_ref = _qualified_table(connector_slug, table_name, schema)
    async with engine.connect() as conn:
        result = await conn.execute(text(f"select * from {table_ref} limit :limit"), {"limit": limit})
        rows = [dict(row._mapping) for row in result.fetchall()]
    return {"status": "ok", "schema": schema, "table": table_name, "rows": rows, "total": len(rows)}


async def _sql_datastore_search_columns(
    engine: AsyncEngine,
    connector_slug: str,
    credentials: dict[str, Any],
    arguments: dict[str, Any],
) -> dict[str, Any]:
    pattern = str(arguments.get("pattern") or "").strip().lower()
    if len(pattern) < 2:
        raise McpExecutionError(400, "pattern must be at least 2 characters.")
    limit = max(1, min(int(arguments.get("limit") or 100), 1000))
    params: dict[str, Any] = {"pattern": f"%{pattern}%", "limit": limit}
    schema_filter = ""
    if connector_slug == "mysql":
        schema_filter = "and table_schema = :schema"
        params["schema"] = _default_schema_for_datastore(connector_slug, credentials) or ""
    else:
        schema_filter = "and table_schema not in ('pg_catalog', 'information_schema')"
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "select table_schema, table_name, column_name, data_type "
                    "from information_schema.columns "
                    "where lower(column_name) like :pattern "
                    f"{schema_filter} "
                    "order by table_schema, table_name, ordinal_position "
                    "limit :limit"
                ),
                params,
            )
        ).mappings().all()
    return {
        "status": "ok",
        "columns": [
            {
                "schema": row["table_schema"],
                "table": row["table_name"],
                "column": row["column_name"],
                "type": row["data_type"],
            }
            for row in rows
        ],
        "total": len(rows),
    }


async def _sql_datastore_column_stats(
    engine: AsyncEngine,
    connector_slug: str,
    credentials: dict[str, Any],
    arguments: dict[str, Any],
) -> dict[str, Any]:
    table_name = _safe_identifier(str(arguments.get("table") or ""))
    schema = str(arguments.get("schema") or _default_schema_for_datastore(connector_slug, credentials) or "").strip() or None
    table_ref = _qualified_table(connector_slug, table_name, schema)
    schema_for_lookup = schema or _default_schema_for_datastore(connector_slug, credentials) or "public"
    limit = max(1, min(int(arguments.get("limit") or 50), 100))
    async with engine.connect() as conn:
        columns = (
            await conn.execute(
                text(
                    "select column_name, data_type "
                    "from information_schema.columns "
                    "where table_schema = :schema and table_name = :table "
                    "order by ordinal_position "
                    "limit :limit"
                ),
                {"schema": schema_for_lookup, "table": table_name, "limit": limit},
            )
        ).mappings().all()
        stats = []
        for column in columns:
            column_name = _safe_identifier(str(_mapping_value(column, "column_name")), "column")
            quoted_column = _quoted_identifier(connector_slug, column_name)
            row = (
                await conn.execute(
                    text(
                        f"select count(*) as row_count, "
                        f"count({quoted_column}) as non_null_count, "
                        f"count(distinct {quoted_column}) as distinct_count "
                        f"from {table_ref}"
                    )
                )
            ).mappings().first()
            row_count = int(row["row_count"] or 0) if row else 0
            non_null_count = int(row["non_null_count"] or 0) if row else 0
            stats.append(
                {
                    "column": column_name,
                    "type": _mapping_value(column, "data_type"),
                    "row_count": row_count,
                    "non_null_count": non_null_count,
                    "null_count": row_count - non_null_count,
                    "distinct_count": int(row["distinct_count"] or 0) if row else 0,
                }
            )
    return {"status": "ok", "schema": schema, "table": table_name, "columns": stats, "total": len(stats)}


async def _sql_datastore_table_freshness(
    engine: AsyncEngine,
    connector_slug: str,
    credentials: dict[str, Any],
    arguments: dict[str, Any],
) -> dict[str, Any]:
    table_name = _safe_identifier(str(arguments.get("table") or ""))
    schema = str(arguments.get("schema") or _default_schema_for_datastore(connector_slug, credentials) or "").strip() or None
    table_ref = _qualified_table(connector_slug, table_name, schema)
    schema_for_lookup = schema or _default_schema_for_datastore(connector_slug, credentials) or "public"
    async with engine.connect() as conn:
        columns = (
            await conn.execute(
                text(
                    "select column_name, data_type "
                    "from information_schema.columns "
                    "where table_schema = :schema and table_name = :table "
                    "order by ordinal_position"
                ),
                {"schema": schema_for_lookup, "table": table_name},
            )
        ).mappings().all()
        candidates = [
            str(_mapping_value(row, "column_name"))
            for row in columns
            if str(_mapping_value(row, "column_name")).lower()
            in {"updated_at", "created_at", "inserted_at", "loaded_at", "placed_at"}
            or str(_mapping_value(row, "data_type")).lower() in FRESHNESS_TYPES
        ]
        freshness: list[dict[str, Any]] = []
        for column_name in candidates[:10]:
            safe_column = _safe_identifier(column_name, "column")
            value = await conn.scalar(text(f"select max({_quoted_identifier(connector_slug, safe_column)}) from {table_ref}"))
            freshness.append({"column": safe_column, "max_value": value.isoformat() if hasattr(value, "isoformat") else value})
    latest = max((item["max_value"] for item in freshness if item["max_value"] is not None), default=None)
    return {
        "status": "ok",
        "schema": schema,
        "table": table_name,
        "freshest_at": latest,
        "columns": freshness,
        "total": len(freshness),
    }


async def _sql_datastore_storage_size(
    engine: AsyncEngine,
    connector_slug: str,
    credentials: dict[str, Any],
    arguments: dict[str, Any],
) -> dict[str, Any]:
    table_name = _safe_identifier(str(arguments.get("table") or ""))
    schema = str(arguments.get("schema") or _default_schema_for_datastore(connector_slug, credentials) or "").strip() or None
    schema_for_lookup = schema or _default_schema_for_datastore(connector_slug, credentials) or "public"
    size_bytes: int | None = None
    async with engine.connect() as conn:
        if connector_slug == "mysql":
            size_bytes = await conn.scalar(
                text(
                    "select coalesce(data_length, 0) + coalesce(index_length, 0) "
                    "from information_schema.tables "
                    "where table_schema = :schema and table_name = :table"
                ),
                {"schema": schema_for_lookup, "table": table_name},
            )
        elif connector_slug == "postgres":
            qualified = f"{schema_for_lookup}.{table_name}"
            size_bytes = await conn.scalar(text("select pg_total_relation_size(to_regclass(:qualified))"), {"qualified": qualified})
        elif connector_slug == "redshift":
            size_mb = await conn.scalar(
                text(
                    "select size from svv_table_info "
                    "where schema = :schema and \"table\" = :table"
                ),
                {"schema": schema_for_lookup, "table": table_name},
            )
            size_bytes = int(size_mb) * 1024 * 1024 if size_mb is not None else None
    return {
        "status": "ok",
        "schema": schema,
        "table": table_name,
        "size_bytes": int(size_bytes) if size_bytes is not None else None,
        "detail": None if size_bytes is not None else f"Storage size is not available for {connector_slug}.",
    }


async def _sql_datastore_explain_query(
    engine: AsyncEngine,
    connector_slug: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    try:
        sql = validate_read_only_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
    except UnsafeSqlError as exc:
        raise McpExecutionError(400, str(exc)) from exc
    explain_sql = f"explain {sql}"
    async with engine.connect() as conn:
        rows = [dict(row._mapping) for row in (await conn.execute(text(explain_sql))).fetchall()]
    return {"status": "ok", "connector": connector_slug, "sql": sql, "plan": rows}


async def _sql_datastore_list_users(engine: AsyncEngine, connector_slug: str) -> dict[str, Any]:
    if connector_slug == "mysql":
        sql = (
            "select grantee as user_name, privilege_type "
            "from information_schema.user_privileges "
            "order by grantee, privilege_type"
        )
    elif connector_slug == "redshift":
        sql = "select usename as user_name, usesuper as is_superuser from pg_user order by usename"
    else:
        sql = "select rolname as user_name, rolsuper as is_superuser, rolcanlogin as can_login from pg_roles order by rolname"
    async with engine.connect() as conn:
        rows = [dict(row._mapping) for row in (await conn.execute(text(sql))).fetchall()]
    return {"status": "ok", "connector": connector_slug, "users": rows, "total": len(rows)}


async def _sql_datastore_list_grants(
    engine: AsyncEngine,
    connector_slug: str,
    credentials: dict[str, Any],
    arguments: dict[str, Any],
) -> dict[str, Any]:
    table_name = _safe_identifier(str(arguments.get("table") or ""))
    schema = str(arguments.get("schema") or _default_schema_for_datastore(connector_slug, credentials) or "public")
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "select grantee, privilege_type, is_grantable "
                    "from information_schema.table_privileges "
                    "where table_schema = :schema and table_name = :table "
                    "order by grantee, privilege_type"
                ),
                {"schema": schema, "table": table_name},
            )
        ).mappings().all()
    return {
        "status": "ok",
        "connector": connector_slug,
        "schema": schema,
        "table": table_name,
        "grants": [dict(row) for row in rows],
        "total": len(rows),
    }


async def _sql_datastore_query_history(
    engine: AsyncEngine,
    connector_slug: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    limit = max(1, min(int(arguments.get("limit") or 100), 1000))
    since = str(arguments.get("since") or "").strip()
    params: dict[str, Any] = {"limit": limit}
    if connector_slug == "mysql":
        sql = (
            "select event_id, event_name, timer_wait, sql_text, current_schema, rows_affected, rows_sent "
            "from performance_schema.events_statements_history_long "
            "where sql_text is not null "
        )
        if since:
            return {"status": "ok", "queries": [], "total": 0, "detail": "MySQL statement history does not expose wall-clock timestamps through this tool."}
        sql += "order by event_id desc limit :limit"
    elif connector_slug == "redshift":
        sql = "select query, userid, starttime, endtime, aborted, substring(querytxt, 1, 4000) as sql_text from stl_query "
        if since:
            sql += "where starttime >= :since "
            params["since"] = since
        sql += "order by starttime desc limit :limit"
    else:
        async with engine.connect() as conn:
            installed = await conn.scalar(text("select 1 from pg_extension where extname = 'pg_stat_statements'"))
        if not installed:
            return {
                "status": "ok",
                "queries": [],
                "total": 0,
                "detail": "Query history requires pg_stat_statements to be installed.",
            }
        sql = (
            "select query, calls, total_exec_time, mean_exec_time, rows "
            "from pg_stat_statements "
            "order by total_exec_time desc limit :limit"
        )
    try:
        async with engine.connect() as conn:
            rows = [dict(row._mapping) for row in (await conn.execute(text(sql), params)).fetchall()]
    except SQLAlchemyError as exc:
        if connector_slug == "mysql" and "events_statements_history_long" in str(exc):
            return {
                "status": "ok",
                "connector": connector_slug,
                "queries": [],
                "total": 0,
                "detail": "MySQL statement history is unavailable for the configured user.",
            }
        raise
    return {"status": "ok", "connector": connector_slug, "queries": rows, "total": len(rows)}


async def _redshift_workload_management(engine: AsyncEngine) -> dict[str, Any]:
    async with engine.connect() as conn:
        rows = [dict(row._mapping) for row in (await conn.execute(text("select * from stv_wlm_service_class_config order by service_class"))).fetchall()]
    return {"status": "ok", "workload_management": rows, "total": len(rows)}


async def _redshift_disk_usage(engine: AsyncEngine, arguments: dict[str, Any]) -> dict[str, Any]:
    schema = str(arguments.get("schema") or "").strip()
    where = "where schema = :schema " if schema else ""
    params = {"schema": schema} if schema else {}
    async with engine.connect() as conn:
        rows = [
            dict(row._mapping)
            for row in (
                await conn.execute(
                    text(
                        "select schema, \"table\" as table_name, size as size_mb, tbl_rows as row_count "
                        f"from svv_table_info {where}"
                        "order by size desc"
                    ),
                    params,
                )
            ).fetchall()
        ]
    return {"status": "ok", "schema": schema or None, "tables": rows, "total": len(rows)}


def _redshift_list_clusters(credentials: dict[str, Any]) -> dict[str, Any]:
    client = _redshift_boto3_client(credentials)
    response = client.describe_clusters()
    clusters = response.get("Clusters", [])
    return {"status": "ok", "clusters": clusters, "total": len(clusters)}


def _redshift_cluster_action(credentials: dict[str, Any], arguments: dict[str, Any], tool_name: str, agent_id: str) -> dict[str, Any]:
    cluster_identifier = _redshift_cluster_identifier(arguments, credentials)
    client = _redshift_boto3_client(credentials)
    if tool_name == "write_pause_cluster":
        response = client.pause_cluster(ClusterIdentifier=cluster_identifier)
        status = "paused"
    else:
        response = client.resume_cluster(ClusterIdentifier=cluster_identifier)
        status = "resumed"
    return {"status": status, "cluster": response.get("Cluster", response), "agent_id": agent_id}


async def _sqlite_list_tables(engine: AsyncEngine) -> dict[str, Any]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text("select name from sqlite_master where type = 'table' and name not like 'sqlite_%' order by name")
            )
        ).mappings().all()
    return {"status": "ok", "tables": [row["name"] for row in rows]}


async def _sqlite_get_schema(engine: AsyncEngine, arguments: dict[str, Any]) -> dict[str, Any]:
    table = _safe_identifier(str(arguments.get("table") or ""))
    async with engine.connect() as conn:
        columns = (await conn.execute(text(f"pragma table_info({table})"))).mappings().all()
    if not columns:
        raise McpExecutionError(404, f"SQLite table not found: {table}")
    return {
        "status": "ok",
        "table": table,
        "columns": [{"name": row["name"], "type": row["type"], "nullable": not bool(row["notnull"])} for row in columns],
    }


async def _sqlite_query_select(engine: AsyncEngine, arguments: dict[str, Any]) -> dict[str, Any]:
    try:
        sql = validate_read_only_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
    except UnsafeSqlError as exc:
        raise McpExecutionError(400, str(exc)) from exc
    async with engine.connect() as conn:
        result = await conn.execute(text(sql))
        rows = [dict(row._mapping) for row in result.fetchall()]
    return {"status": "ok", "sql": sql, "rows": rows}


async def _sqlite_row_count(engine: AsyncEngine, arguments: dict[str, Any]) -> dict[str, Any]:
    table = _safe_identifier(str(arguments.get("table") or ""))
    async with engine.connect() as conn:
        count = await conn.scalar(text(f"select count(*) from {table}"))
    return {"status": "ok", "table": table, "row_count": int(count or 0)}


async def _sqlite_sample_rows(engine: AsyncEngine, arguments: dict[str, Any]) -> dict[str, Any]:
    table = _safe_identifier(str(arguments.get("table") or ""))
    limit = max(1, min(int(arguments.get("limit") or 100), 1000))
    async with engine.connect() as conn:
        result = await conn.execute(text(f"select * from {table} limit :limit"), {"limit": limit})
        rows = [dict(row._mapping) for row in result.fetchall()]
    return {"status": "ok", "table": table, "rows": rows, "total": len(rows)}


async def _sqlite_search_columns(engine: AsyncEngine, arguments: dict[str, Any]) -> dict[str, Any]:
    pattern = str(arguments.get("pattern") or "").strip().lower()
    if len(pattern) < 2:
        raise McpExecutionError(400, "pattern must be at least 2 characters.")
    limit = max(1, min(int(arguments.get("limit") or 100), 1000))
    matches: list[dict[str, Any]] = []
    async with engine.connect() as conn:
        tables = (
            await conn.execute(
                text("select name from sqlite_master where type = 'table' and name not like 'sqlite_%' order by name")
            )
        ).mappings().all()
        for row in tables:
            table = _safe_identifier(str(row["name"]))
            columns = (await conn.execute(text(f"pragma table_info({table})"))).mappings().all()
            for column in columns:
                if pattern in str(column["name"]).lower():
                    matches.append({"table": table, "column": column["name"], "type": column["type"]})
                    if len(matches) >= limit:
                        return {"status": "ok", "columns": matches, "total": len(matches)}
    return {"status": "ok", "columns": matches, "total": len(matches)}


async def _sqlite_column_stats(engine: AsyncEngine, arguments: dict[str, Any]) -> dict[str, Any]:
    table = _safe_identifier(str(arguments.get("table") or ""))
    limit = max(1, min(int(arguments.get("limit") or 50), 100))
    async with engine.connect() as conn:
        columns = (await conn.execute(text(f"pragma table_info({table})"))).mappings().all()
        if not columns:
            raise McpExecutionError(404, f"SQLite table not found: {table}")
        stats = []
        for column in columns[:limit]:
            column_name = _safe_identifier(str(column["name"]), "column")
            row = (
                await conn.execute(
                    text(
                        f"select count(*) as row_count, "
                        f"count({_quoted_identifier('sqlite', column_name)}) as non_null_count, "
                        f"count(distinct {_quoted_identifier('sqlite', column_name)}) as distinct_count "
                        f"from {_quoted_identifier('sqlite', table)}"
                    )
                )
            ).mappings().first()
            row_count = int(row["row_count"] or 0) if row else 0
            non_null_count = int(row["non_null_count"] or 0) if row else 0
            stats.append(
                {
                    "column": column_name,
                    "type": column["type"],
                    "row_count": row_count,
                    "non_null_count": non_null_count,
                    "null_count": row_count - non_null_count,
                    "distinct_count": int(row["distinct_count"] or 0) if row else 0,
                }
            )
    return {"status": "ok", "table": table, "columns": stats, "total": len(stats)}


async def _sqlite_table_freshness(engine: AsyncEngine, arguments: dict[str, Any]) -> dict[str, Any]:
    table = _safe_identifier(str(arguments.get("table") or ""))
    async with engine.connect() as conn:
        columns = (await conn.execute(text(f"pragma table_info({table})"))).mappings().all()
        if not columns:
            raise McpExecutionError(404, f"SQLite table not found: {table}")
        candidates = [
            str(column["name"])
            for column in columns
            if str(column["name"]).lower() in {"updated_at", "created_at", "inserted_at", "loaded_at", "placed_at"}
            or str(column["type"]).lower() in FRESHNESS_TYPES
        ]
        freshness = []
        for column_name in candidates[:10]:
            safe_column = _safe_identifier(column_name, "column")
            value = await conn.scalar(
                text(
                    f"select max({_quoted_identifier('sqlite', safe_column)}) "
                    f"from {_quoted_identifier('sqlite', table)}"
                )
            )
            freshness.append({"column": safe_column, "max_value": value})
    latest = max((item["max_value"] for item in freshness if item["max_value"] is not None), default=None)
    return {"status": "ok", "table": table, "freshest_at": latest, "columns": freshness, "total": len(freshness)}


async def _sqlite_storage_size(engine: AsyncEngine, arguments: dict[str, Any]) -> dict[str, Any]:
    table = _safe_identifier(str(arguments.get("table") or ""))
    async with engine.connect() as conn:
        columns = (await conn.execute(text(f"pragma table_info({table})"))).mappings().all()
        if not columns:
            raise McpExecutionError(404, f"SQLite table not found: {table}")
        page_count = await conn.scalar(text("pragma page_count"))
        page_size = await conn.scalar(text("pragma page_size"))
    return {
        "status": "ok",
        "table": table,
        "size_bytes": None,
        "database_size_bytes": int(page_count or 0) * int(page_size or 0),
        "detail": "SQLite does not expose reliable per-table storage size.",
    }


async def _sqlite_explain_query(engine: AsyncEngine, arguments: dict[str, Any]) -> dict[str, Any]:
    try:
        sql = validate_read_only_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
    except UnsafeSqlError as exc:
        raise McpExecutionError(400, str(exc)) from exc
    async with engine.connect() as conn:
        result = await conn.execute(text(f"explain query plan {sql}"))
        rows = [dict(row._mapping) for row in result.fetchall()]
    return {"status": "ok", "sql": sql, "plan": rows}


def _write_table_ref(arguments: dict[str, Any], connector_slug: str, field_name: str = "table") -> str:
    table = _safe_identifier(str(arguments.get(field_name) or ""), field_name)
    schema = str(arguments.get("schema") or "").strip() or None
    return _qualified_table(connector_slug, table, _safe_identifier(schema, "schema") if schema else None)


def _sql_for_write_tool(tool_name: str, arguments: dict[str, Any], connector_slug: str = "sqlite") -> str:
    if tool_name == "write_execute_sql":
        return str(arguments.get("sql") or "")
    if tool_name == "write_create_table":
        table = _safe_identifier(str(arguments.get("table") or ""), "table")
        schema = str(arguments.get("schema") or "").strip() or None
        if connector_slug == "bigquery":
            schema = schema or str(arguments.get("dataset") or "").strip() or None
        if connector_slug == "sql_server":
            schema = schema or "dbo"
        table_sql = _qualified_table(connector_slug, table, _safe_identifier(schema, "schema") if schema else None)
        columns = arguments.get("columns")
        if not isinstance(columns, list) or not columns:
            raise McpExecutionError(400, "columns must be a non-empty list.")
        parts = []
        for column in columns:
            if not isinstance(column, dict):
                raise McpExecutionError(400, "Each column must be an object.")
            name = _safe_identifier(str(column.get("name") or ""), "column")
            column_type = str(column.get("type") or "text").strip().upper()
            if connector_slug == "trino":
                column_type = {"TEXT": "VARCHAR", "REAL": "DOUBLE"}.get(column_type, column_type)
                allowed_types = {"VARCHAR", "INTEGER", "BIGINT", "DOUBLE", "DECIMAL", "BOOLEAN", "DATE", "TIMESTAMP"}
            elif connector_slug == "postgres":
                column_type = {"INT": "INTEGER", "BOOL": "BOOLEAN", "DOUBLE": "DOUBLE PRECISION", "BLOB": "BYTEA"}.get(column_type, column_type)
                allowed_types = {
                    "TEXT",
                    "VARCHAR",
                    "INTEGER",
                    "BIGINT",
                    "REAL",
                    "DOUBLE PRECISION",
                    "NUMERIC",
                    "DECIMAL",
                    "BOOLEAN",
                    "DATE",
                    "TIMESTAMP",
                    "TIMESTAMPTZ",
                    "BYTEA",
                }
            elif connector_slug == "mysql":
                column_type = {"INT": "INTEGER", "BOOL": "BOOLEAN", "REAL": "DOUBLE"}.get(column_type, column_type)
                allowed_types = {
                    "TEXT",
                    "INTEGER",
                    "BIGINT",
                    "DOUBLE",
                    "NUMERIC",
                    "DECIMAL",
                    "BOOLEAN",
                    "BOOL",
                    "DATE",
                    "DATETIME",
                    "TIMESTAMP",
                    "BLOB",
                }
                if re.fullmatch(r"VARCHAR\(\d{1,4}\)", column_type):
                    allowed_types = {*allowed_types, column_type}
            elif connector_slug == "bigquery":
                column_type = {
                    "TEXT": "STRING",
                    "INTEGER": "INT64",
                    "INT": "INT64",
                    "REAL": "FLOAT64",
                    "DOUBLE": "FLOAT64",
                    "BOOL": "BOOL",
                    "BOOLEAN": "BOOL",
                }.get(column_type, column_type)
                allowed_types = {
                    "STRING",
                    "INT64",
                    "INTEGER",
                    "FLOAT64",
                    "FLOAT",
                    "NUMERIC",
                    "BIGNUMERIC",
                    "BOOL",
                    "BOOLEAN",
                    "DATE",
                    "DATETIME",
                    "TIMESTAMP",
                    "BYTES",
                    "JSON",
                }
            elif connector_slug == "sql_server":
                column_type = {"TEXT": "NVARCHAR(MAX)", "REAL": "FLOAT", "BLOB": "VARBINARY(MAX)"}.get(column_type, column_type)
                allowed_types = {
                    "NVARCHAR(MAX)",
                    "VARCHAR(MAX)",
                    "INTEGER",
                    "INT",
                    "BIGINT",
                    "FLOAT",
                    "NUMERIC",
                    "DECIMAL",
                    "BIT",
                    "DATE",
                    "DATETIME2",
                }
            else:
                allowed_types = {"TEXT", "INTEGER", "REAL", "NUMERIC", "BLOB"}
            if column_type not in allowed_types:
                raise McpExecutionError(400, f"Unsupported {connector_slug} column type: {column_type}")
            parts.append(f"{_quoted_identifier(connector_slug, name)} {column_type}")
        if connector_slug == "sql_server":
            object_name = f"{schema}.{table}"
            return f"if object_id(N'{object_name}', N'U') is null create table {table_sql} ({', '.join(parts)})"
        return f"create table if not exists {table_sql} ({', '.join(parts)})"
    if tool_name == "write_create_view":
        view = _safe_identifier(str(arguments.get("view") or ""), "view")
        select_sql = validate_read_only_sql(str(arguments.get("select_sql") or ""))
        if connector_slug == "sql_server":
            select_sql = re.sub(r"\s+limit\s+\d+\s*$", "", select_sql, flags=re.IGNORECASE)
        schema = str(arguments.get("schema") or "").strip() or None
        view_ref = _qualified_table(connector_slug, view, _safe_identifier(schema, "schema") if schema else None)
        return f"create view {view_ref} as {select_sql}"
    if tool_name == "write_insert_rows":
        rows = arguments.get("rows")
        if not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
            raise McpExecutionError(400, "rows must be a non-empty list of objects.")
        columns = [_safe_identifier(str(name), "column") for name in rows[0]]
        column_sql = [_quoted_identifier(connector_slug, column) for column in columns]
        values = []
        for row in rows:
            values.append("(" + ", ".join(_literal(row.get(column)) for column in columns) + ")")
        return f"insert into {_write_table_ref(arguments, connector_slug)} ({', '.join(column_sql)}) values {', '.join(values)}"
    if tool_name == "write_update_rows":
        updates = arguments.get("set")
        where = str(arguments.get("where") or "").strip()
        if not isinstance(updates, dict) or not updates:
            raise McpExecutionError(400, "set must be a non-empty object.")
        if not where:
            raise McpExecutionError(400, "where is required for write_update_rows.")
        assignments = [
            f"{_quoted_identifier(connector_slug, _safe_identifier(str(column), 'column'))} = {_literal(value)}"
            for column, value in updates.items()
        ]
        return f"update {_write_table_ref(arguments, connector_slug)} set {', '.join(assignments)} where {where}"
    if tool_name == "write_delete_rows":
        where = str(arguments.get("where") or "").strip()
        if not where:
            raise McpExecutionError(400, "where is required for write_delete_rows.")
        return f"delete from {_write_table_ref(arguments, connector_slug)} where {where}"
    if tool_name == "write_grant_permission":
        table_ref = _write_table_ref(arguments, connector_slug)
        role = _safe_identifier(str(arguments.get("role") or ""), "role")
        scope = str(arguments.get("scope") or "select").strip().upper()
        allowed_scopes = {"SELECT", "INSERT", "UPDATE", "DELETE", "ALL"}
        if scope not in allowed_scopes:
            raise McpExecutionError(400, f"scope must be one of {', '.join(sorted(allowed_scopes))}.")
        return f"grant {scope} on {table_ref} to {_quoted_identifier(connector_slug, role)}"
    if tool_name == "write_create_index":
        table_ref = _write_table_ref(arguments, connector_slug)
        columns = arguments.get("columns")
        if not isinstance(columns, list) or not columns:
            raise McpExecutionError(400, "columns must be a non-empty list.")
        safe_columns = [_quoted_identifier(connector_slug, _safe_identifier(str(column), "column")) for column in columns]
        explicit_name = str(arguments.get("index_name") or "").strip()
        index_name = _safe_identifier(explicit_name, "index_name") if explicit_name else _safe_identifier(
            f"idx_{str(arguments.get('table') or '')}_{'_'.join(str(column) for column in columns)}",
            "index_name",
        )
        return f"create index {_quoted_identifier(connector_slug, index_name)} on {table_ref} ({', '.join(safe_columns)})"
    raise McpExecutionError(404, f"Unsupported write tool: {tool_name}")


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


async def _sqlite_write(
    *,
    session: AsyncSession,
    engine: AsyncEngine,
    agent: Agent,
    tool_name: str,
    arguments: dict[str, Any],
    user_email: str,
) -> dict[str, Any]:
    sql = _sql_for_write_tool(tool_name, arguments)
    try:
        decision = validate_write_sql(sql)
    except UnsafeSqlError as exc:
        raise McpExecutionError(400, str(exc)) from exc
    workspace = await _workspace(session)
    if decision.action == "requires_approval" and not arguments.get("__approved"):
        alert = Alert(
            workspace_id=workspace.id,
            severity="critical",
            title=f"Agent {agent.name} wants to {decision.statement_type} {decision.target or ''}".strip(),
            detail=f"Approval required for DataClaw MCP write SQL.\nSQL: {decision.sql}",
            requires_approval=True,
        )
        session.add(alert)
        await session.commit()
        return {"status": "pending_approval", "alert_id": alert.id, "sql": decision.sql}
    async with engine.begin() as conn:
        result = await conn.execute(text(decision.sql))
        affected = result.rowcount if result.rowcount is not None and result.rowcount >= 0 else None
    session.add(
        AgentWriteAudit(
            workspace_id=workspace.id,
            agent_id=agent.id,
            connector_slug="sqlite",
            statement_type=decision.statement_type,
            statement=decision.sql,
            target=decision.target,
            affected_rows=affected,
            required_approval=False,
            executed_at=datetime.now(UTC),
            executed_by=user_email,
        )
    )
    session.add(
        LogEntry(
            timestamp=datetime.now(UTC),
            level="info",
            logger_name="dataclaw.mcp",
            message="mcp_write_sql_executed",
            context={
                "agent_id": agent.id,
                "connector_slug": "sqlite",
                "tool": tool_name,
                "statement_type": decision.statement_type,
                "target": decision.target,
            },
        )
    )
    await session.commit()
    return {"status": "executed", "sql": decision.sql, "affected_rows": affected}


async def _sql_datastore_write(
    *,
    session: AsyncSession,
    engine: AsyncEngine,
    connector_slug: str,
    agent: Agent,
    tool_name: str,
    arguments: dict[str, Any],
    user_email: str,
) -> dict[str, Any]:
    sql = _sql_for_write_tool(tool_name, arguments, connector_slug)
    try:
        decision = validate_write_sql(sql)
    except UnsafeSqlError as exc:
        raise McpExecutionError(400, str(exc)) from exc
    workspace = await _workspace(session)
    if decision.action == "requires_approval" and not arguments.get("__approved"):
        alert = Alert(
            workspace_id=workspace.id,
            severity="critical",
            title=f"Agent {agent.name} wants to {decision.statement_type} {decision.target or ''}".strip(),
            detail=(
                "Approval required for DataClaw MCP write SQL.\n"
                f"Connector: {connector_slug}\n"
                f"Agent-ID: {agent.id}\n"
                f"SQL: {decision.sql}"
            ),
            requires_approval=True,
        )
        session.add(alert)
        await session.commit()
        return {"status": "pending_approval", "alert_id": alert.id, "sql": decision.sql}
    async with engine.begin() as conn:
        result = await conn.execute(text(decision.sql))
        affected = result.rowcount if result.rowcount is not None and result.rowcount >= 0 else None
    session.add(
        AgentWriteAudit(
            workspace_id=workspace.id,
            agent_id=agent.id,
            connector_slug=connector_slug,
            statement_type=decision.statement_type,
            statement=decision.sql,
            target=decision.target,
            affected_rows=affected,
            required_approval=False,
            executed_at=datetime.now(UTC),
            executed_by=user_email,
        )
    )
    session.add(
        LogEntry(
            timestamp=datetime.now(UTC),
            level="info",
            logger_name="dataclaw.mcp",
            message="mcp_write_sql_executed",
            context={
                "agent_id": agent.id,
                "connector_slug": connector_slug,
                "tool": tool_name,
                "statement_type": decision.statement_type,
                "target": decision.target,
            },
        )
    )
    await session.commit()
    return {"status": "executed", "sql": decision.sql, "affected_rows": affected}


def _trino_connect(credentials: dict[str, Any]) -> Any:
    try:
        import trino  # type: ignore[import-not-found]
        from trino.auth import BasicAuthentication  # type: ignore[import-not-found]
    except ImportError as exc:
        raise McpExecutionError(501, "Trino MCP execution requires the optional trino Python client.") from exc

    kwargs: dict[str, Any] = {
        "host": credentials["host"],
        "port": int(credentials.get("port") or 8080),
        "user": credentials["user"],
        "catalog": credentials["catalog"],
        "schema": credentials["schema"],
        "http_scheme": credentials.get("http_scheme") or ("https" if credentials.get("password") else "http"),
    }
    if credentials.get("password"):
        kwargs["auth"] = BasicAuthentication(credentials["user"], credentials["password"])
    return trino.dbapi.connect(**kwargs)


def _trino_fetch(credentials: dict[str, Any], sql: str) -> list[dict[str, Any]]:
    with _trino_connect(credentials) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        names = [str(col[0]) for col in (cursor.description or [])]
        return [dict(zip(names, row, strict=False)) for row in cursor.fetchall()]


def _trino_execute(credentials: dict[str, Any], sql: str) -> int | None:
    with _trino_connect(credentials) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        rowcount = getattr(cursor, "rowcount", None)
        return int(rowcount) if isinstance(rowcount, int) and rowcount >= 0 else None


def _sql_server_read_sql(sql: str, limit: int) -> str:
    bounded = validate_read_only_sql(sql, limit)
    match = re.search(r"\s+limit\s+(\d+)\s*$", bounded, flags=re.IGNORECASE)
    if not match:
        return bounded
    limit_value = match.group(1)
    without_limit = bounded[: match.start()].strip()
    if re.match(r"^select\s+(distinct\s+)?top\s*\(", without_limit, flags=re.IGNORECASE):
        return without_limit
    select_match = re.match(r"^(select\s+)(distinct\s+)?", without_limit, flags=re.IGNORECASE)
    if select_match:
        prefix = select_match.group(1)
        distinct = select_match.group(2) or ""
        rest = without_limit[select_match.end() :]
        return f"{prefix}{distinct}top ({limit_value}) {rest}"
    return without_limit


def _trino_table_ref(arguments: dict[str, Any], credentials: dict[str, Any]) -> str:
    table = _safe_identifier(str(arguments.get("table") or ""))
    schema = _safe_identifier(str(arguments.get("schema") or credentials.get("schema") or ""), "schema")
    return f'"{schema}"."{table}"'


async def _trino_tool(
    *,
    session: AsyncSession,
    tool_name: str,
    arguments: dict[str, Any],
    agent: Agent,
    user_email: str,
) -> dict[str, Any]:
    credentials = await _connector_credentials(session, "trino")
    catalog = _literal(str(credentials["catalog"]))
    default_schema = str(credentials.get("schema") or "")

    if tool_name == "read_list_tables":
        rows = await asyncio.to_thread(
            _trino_fetch,
            credentials,
            "select table_schema, table_name "
            "from information_schema.tables "
            f"where table_catalog = {catalog} and table_schema not in ('information_schema') "
            "order by table_schema, table_name",
        )
        return {"status": "ok", "tables": [{"schema": row["table_schema"], "name": row["table_name"], "table": row["table_name"]} for row in rows]}

    if tool_name == "read_get_schema":
        table = _safe_identifier(str(arguments.get("table") or ""))
        schema = _safe_identifier(str(arguments.get("schema") or default_schema), "schema")
        rows = await asyncio.to_thread(
            _trino_fetch,
            credentials,
            "select column_name, data_type, is_nullable "
            "from information_schema.columns "
            f"where table_catalog = {catalog} and table_schema = {_literal(schema)} and table_name = {_literal(table)} "
            "order by ordinal_position",
        )
        if not rows:
            raise McpExecutionError(404, f"trino table not found: {schema}.{table}")
        return {
            "status": "ok",
            "schema": schema,
            "table": table,
            "columns": [{"name": row["column_name"], "type": row["data_type"], "nullable": row.get("is_nullable") == "YES"} for row in rows],
        }

    if tool_name == "read_query_select":
        try:
            sql = validate_read_only_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
        except UnsafeSqlError as exc:
            raise McpExecutionError(400, str(exc)) from exc
        rows = await asyncio.to_thread(_trino_fetch, credentials, sql)
        return {"status": "ok", "sql": sql, "rows": rows}

    if tool_name == "read_get_row_count":
        table_ref = _trino_table_ref(arguments, credentials)
        rows = await asyncio.to_thread(_trino_fetch, credentials, f"select count(*) as row_count from {table_ref}")
        return {"status": "ok", "schema": arguments.get("schema") or default_schema, "table": arguments.get("table"), "row_count": int((rows[0] or {}).get("row_count") or 0)}

    if tool_name == "read_sample_rows":
        table_ref = _trino_table_ref(arguments, credentials)
        limit = _bounded_limit(arguments.get("limit"), default=100, maximum=1000)
        rows = await asyncio.to_thread(_trino_fetch, credentials, f"select * from {table_ref} limit {limit}")
        return {"status": "ok", "schema": arguments.get("schema") or default_schema, "table": arguments.get("table"), "rows": rows, "total": len(rows)}

    if tool_name == "read_search_columns":
        pattern = str(arguments.get("pattern") or "").strip().lower()
        if len(pattern) < 2:
            raise McpExecutionError(400, "pattern must be at least 2 characters.")
        limit = _bounded_limit(arguments.get("limit"), default=100, maximum=1000)
        rows = await asyncio.to_thread(
            _trino_fetch,
            credentials,
            "select table_schema, table_name, column_name, data_type "
            "from information_schema.columns "
            f"where table_catalog = {catalog} and lower(column_name) like {_literal('%' + pattern + '%')} "
            "order by table_schema, table_name, ordinal_position "
            f"limit {limit}",
        )
        return {"status": "ok", "columns": [{"schema": row["table_schema"], "table": row["table_name"], "column": row["column_name"], "type": row["data_type"]} for row in rows], "total": len(rows)}

    if tool_name == "read_get_column_stats":
        table_ref = _trino_table_ref(arguments, credentials)
        table = _safe_identifier(str(arguments.get("table") or ""))
        schema = _safe_identifier(str(arguments.get("schema") or default_schema), "schema")
        limit = _bounded_limit(arguments.get("limit"), default=50, maximum=100)
        columns = await asyncio.to_thread(
            _trino_fetch,
            credentials,
            "select column_name, data_type from information_schema.columns "
            f"where table_catalog = {catalog} and table_schema = {_literal(schema)} and table_name = {_literal(table)} "
            f"order by ordinal_position limit {limit}",
        )
        stats = []
        for column in columns:
            column_name = _safe_identifier(str(column["column_name"]), "column")
            quoted = _quoted_identifier("trino", column_name)
            rows = await asyncio.to_thread(
                _trino_fetch,
                credentials,
                f"select count(*) as row_count, count({quoted}) as non_null_count, count(distinct {quoted}) as distinct_count from {table_ref}",
            )
            stat = rows[0] if rows else {}
            row_count = int(stat.get("row_count") or 0)
            non_null = int(stat.get("non_null_count") or 0)
            stats.append({"column": column_name, "type": column["data_type"], "row_count": row_count, "non_null_count": non_null, "null_count": row_count - non_null, "distinct_count": int(stat.get("distinct_count") or 0)})
        return {"status": "ok", "schema": schema, "table": table, "columns": stats, "total": len(stats)}

    if tool_name == "read_get_table_freshness":
        return {"status": "ok", "schema": arguments.get("schema") or default_schema, "table": arguments.get("table"), "freshest_at": None, "columns": [], "total": 0, "detail": "Trino freshness requires connector-specific timestamp metadata or a timestamp column query."}

    if tool_name == "read_get_storage_size":
        return {"status": "ok", "schema": arguments.get("schema") or default_schema, "table": arguments.get("table"), "size_bytes": None, "detail": "Trino does not expose reliable per-table storage size through information_schema."}

    if tool_name == "read_explain_query":
        try:
            sql = validate_read_only_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
        except UnsafeSqlError as exc:
            raise McpExecutionError(400, str(exc)) from exc
        rows = await asyncio.to_thread(_trino_fetch, credentials, f"explain {sql}")
        return {"status": "ok", "connector": "trino", "sql": sql, "plan": rows}

    if tool_name == "read_list_users":
        return {"status": "ok", "connector": "trino", "users": [], "total": 0, "detail": "Trino user listing is access-control specific and may not be exposed by the coordinator."}

    if tool_name == "read_list_grants":
        return {"status": "ok", "connector": "trino", "schema": arguments.get("schema") or default_schema, "table": arguments.get("table"), "grants": [], "total": 0, "detail": "Trino grant visibility is connector and access-control specific."}

    if tool_name == "read_get_query_history":
        return {"status": "ok", "connector": "trino", "queries": [], "total": 0, "detail": "Trino query history requires event listener storage outside the default system tables."}

    if tool_name in {
        "write_execute_sql",
        "write_create_table",
        "write_create_view",
        "write_insert_rows",
        "write_update_rows",
        "write_delete_rows",
        "write_grant_permission",
        "write_create_index",
    }:
        return await _trino_write(session=session, credentials=credentials, agent=agent, tool_name=tool_name, arguments=arguments, user_email=user_email)

    raise McpExecutionError(404, f"Unsupported Trino MCP tool: {tool_name}")


async def _trino_write(
    *,
    session: AsyncSession,
    credentials: dict[str, Any],
    agent: Agent,
    tool_name: str,
    arguments: dict[str, Any],
    user_email: str,
) -> dict[str, Any]:
    sql = _sql_for_write_tool(tool_name, arguments, "trino")
    try:
        decision = validate_write_sql(sql)
    except UnsafeSqlError as exc:
        raise McpExecutionError(400, str(exc)) from exc
    workspace = await _workspace(session)
    if decision.action == "requires_approval" and not arguments.get("__approved"):
        alert = Alert(
            workspace_id=workspace.id,
            severity="critical",
            title=f"Agent {agent.name} wants to {decision.statement_type} {decision.target or ''}".strip(),
            detail=f"Approval required for DataClaw MCP write SQL.\nConnector: trino\nAgent-ID: {agent.id}\nSQL: {decision.sql}",
            requires_approval=True,
        )
        session.add(alert)
        await session.commit()
        return {"status": "pending_approval", "alert_id": alert.id, "sql": decision.sql}
    affected = await asyncio.to_thread(_trino_execute, credentials, decision.sql)
    session.add(
        AgentWriteAudit(
            workspace_id=workspace.id,
            agent_id=agent.id,
            connector_slug="trino",
            statement_type=decision.statement_type,
            statement=decision.sql,
            target=decision.target,
            affected_rows=affected,
            required_approval=False,
            executed_at=datetime.now(UTC),
            executed_by=user_email,
        )
    )
    session.add(
        LogEntry(
            timestamp=datetime.now(UTC),
            level="info",
            logger_name="dataclaw.mcp",
            message="mcp_write_sql_executed",
            context={"agent_id": agent.id, "connector_slug": "trino", "tool": tool_name, "statement_type": decision.statement_type, "target": decision.target},
        )
    )
    await session.commit()
    return {"status": "executed", "sql": decision.sql, "affected_rows": affected}


async def _sql_server_tool(
    *,
    session: AsyncSession,
    tool_name: str,
    arguments: dict[str, Any],
    agent: Agent,
    user_email: str,
) -> dict[str, Any]:
    credentials = await _connector_credentials(session, "sql_server")
    adapter = adapter_for("sql_server")

    def run_read() -> dict[str, Any]:
        with adapter._connect_sync(credentials) as conn:  # type: ignore[attr-defined]
            with conn.cursor(as_dict=True) as cursor:
                if tool_name == "read_list_tables":
                    cursor.execute(
                        "select table_schema, table_name "
                        "from information_schema.tables "
                        "where table_type = 'BASE TABLE' "
                        "and table_schema not in ('sys', 'INFORMATION_SCHEMA') "
                        "order by table_schema, table_name"
                    )
                    return {
                        "status": "ok",
                        "tables": [
                            {"schema": row["table_schema"], "name": row["table_name"], "table": row["table_name"]}
                            for row in cursor.fetchall()
                        ],
                    }
                if tool_name == "read_get_schema":
                    table_name = _safe_identifier(str(arguments.get("table") or ""))
                    schema = _safe_identifier(str(arguments.get("schema") or "dbo"), "schema")
                    cursor.execute(
                        "select column_name, data_type, is_nullable "
                        "from information_schema.columns "
                        "where table_schema = %s and table_name = %s "
                        "order by ordinal_position",
                        (schema, table_name),
                    )
                    rows = cursor.fetchall()
                    if not rows:
                        raise McpExecutionError(404, f"SQL Server table not found: {schema}.{table_name}")
                    return {
                        "status": "ok",
                        "schema": schema,
                        "table": table_name,
                        "columns": [
                            {"name": row["column_name"], "type": row["data_type"], "nullable": row["is_nullable"] == "YES"}
                            for row in rows
                        ],
                    }
                if tool_name == "read_query_select":
                    sql = _sql_server_read_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
                    cursor.execute(sql)
                    return {"status": "ok", "sql": sql, "rows": list(cursor.fetchall())}
                if tool_name == "read_get_row_count":
                    table_name = _safe_identifier(str(arguments.get("table") or ""))
                    schema = _safe_identifier(str(arguments.get("schema") or "dbo"), "schema")
                    cursor.execute(f"select count(*) as row_count from [{schema}].[{table_name}]")
                    row = cursor.fetchone() or {"row_count": 0}
                    return {"status": "ok", "schema": schema, "table": table_name, "row_count": int(row["row_count"])}
                if tool_name == "read_sample_rows":
                    table_name = _safe_identifier(str(arguments.get("table") or ""))
                    schema = _safe_identifier(str(arguments.get("schema") or "dbo"), "schema")
                    limit = max(1, min(int(arguments.get("limit") or 100), 1000))
                    cursor.execute(f"select top ({limit}) * from [{schema}].[{table_name}]")
                    rows = list(cursor.fetchall())
                    return {
                        "status": "ok",
                        "schema": schema,
                        "table": table_name,
                        "rows": rows,
                        "total": len(rows),
                    }
                if tool_name == "read_search_columns":
                    pattern = str(arguments.get("pattern") or "").strip().lower()
                    if len(pattern) < 2:
                        raise McpExecutionError(400, "pattern must be at least 2 characters.")
                    limit = max(1, min(int(arguments.get("limit") or 100), 1000))
                    cursor.execute(
                        f"select top ({limit}) table_schema, table_name, column_name, data_type "
                        "from information_schema.columns "
                        "where lower(column_name) like %s "
                        "order by table_schema, table_name, ordinal_position",
                        (f"%{pattern}%",),
                    )
                    return {
                        "status": "ok",
                        "columns": [
                            {
                                "schema": row["table_schema"],
                                "table": row["table_name"],
                                "column": row["column_name"],
                                "type": row["data_type"],
                            }
                            for row in cursor.fetchall()
                        ],
                    }
                if tool_name == "read_get_column_stats":
                    table_name = _safe_identifier(str(arguments.get("table") or ""))
                    schema = _safe_identifier(str(arguments.get("schema") or "dbo"), "schema")
                    limit = max(1, min(int(arguments.get("limit") or 50), 100))
                    cursor.execute(
                        f"select top ({limit}) column_name, data_type "
                        "from information_schema.columns "
                        "where table_schema = %s and table_name = %s "
                        "order by ordinal_position",
                        (schema, table_name),
                    )
                    stats = []
                    for column in cursor.fetchall():
                        column_name = _safe_identifier(str(column["column_name"]), "column")
                        cursor.execute(
                            f"select count(*) as row_count, "
                            f"count([{column_name}]) as non_null_count, "
                            f"count(distinct [{column_name}]) as distinct_count "
                            f"from [{schema}].[{table_name}]"
                        )
                        row = cursor.fetchone() or {"row_count": 0, "non_null_count": 0, "distinct_count": 0}
                        row_count = int(row["row_count"] or 0)
                        non_null_count = int(row["non_null_count"] or 0)
                        stats.append(
                            {
                                "column": column_name,
                                "type": column["data_type"],
                                "row_count": row_count,
                                "non_null_count": non_null_count,
                                "null_count": row_count - non_null_count,
                                "distinct_count": int(row["distinct_count"] or 0),
                            }
                        )
                    return {"status": "ok", "schema": schema, "table": table_name, "columns": stats, "total": len(stats)}
                if tool_name == "read_get_table_freshness":
                    table_name = _safe_identifier(str(arguments.get("table") or ""))
                    schema = _safe_identifier(str(arguments.get("schema") or "dbo"), "schema")
                    cursor.execute(
                        "select column_name, data_type "
                        "from information_schema.columns "
                        "where table_schema = %s and table_name = %s "
                        "order by ordinal_position",
                        (schema, table_name),
                    )
                    candidates = [
                        str(row["column_name"])
                        for row in cursor.fetchall()
                        if str(row["column_name"]).lower()
                        in {"updated_at", "created_at", "inserted_at", "loaded_at", "placed_at"}
                        or str(row["data_type"]).lower() in FRESHNESS_TYPES
                    ]
                    freshness = []
                    for column_name in candidates[:10]:
                        safe_column = _safe_identifier(column_name, "column")
                        cursor.execute(f"select max([{safe_column}]) as max_value from [{schema}].[{table_name}]")
                        row = cursor.fetchone() or {}
                        value = row.get("max_value")
                        freshness.append({"column": safe_column, "max_value": value.isoformat() if hasattr(value, "isoformat") else value})
                    latest = max((item["max_value"] for item in freshness if item["max_value"] is not None), default=None)
                    return {"status": "ok", "schema": schema, "table": table_name, "freshest_at": latest, "columns": freshness, "total": len(freshness)}
                if tool_name == "read_get_storage_size":
                    table_name = _safe_identifier(str(arguments.get("table") or ""))
                    schema = _safe_identifier(str(arguments.get("schema") or "dbo"), "schema")
                    cursor.execute(
                        "select sum(reserved_page_count) * 8192 as size_bytes "
                        "from sys.dm_db_partition_stats "
                        "where object_id = object_id(%s)",
                        (f"{schema}.{table_name}",),
                    )
                    row = cursor.fetchone() or {}
                    size_bytes = row.get("size_bytes")
                    return {"status": "ok", "schema": schema, "table": table_name, "size_bytes": int(size_bytes) if size_bytes is not None else None}
                if tool_name == "read_explain_query":
                    sql = _sql_server_read_sql(str(arguments.get("sql") or ""), int(arguments.get("limit") or 100))
                    cursor.execute("set showplan_text on")
                    try:
                        cursor.execute(sql)
                        plan = list(cursor.fetchall())
                    finally:
                        cursor.execute("set showplan_text off")
                    return {"status": "ok", "sql": sql, "plan": plan}
                if tool_name == "read_list_users":
                    cursor.execute(
                        "select name as user_name, type_desc, create_date "
                        "from sys.database_principals "
                        "where type in ('S', 'U', 'G', 'R') "
                        "order by name"
                    )
                    rows = list(cursor.fetchall())
                    return {"status": "ok", "users": rows, "total": len(rows)}
                if tool_name == "read_list_grants":
                    table_name = _safe_identifier(str(arguments.get("table") or ""))
                    schema = _safe_identifier(str(arguments.get("schema") or "dbo"), "schema")
                    cursor.execute(
                        "select dp.name as grantee, perm.permission_name as privilege_type, perm.state_desc "
                        "from sys.database_permissions perm "
                        "join sys.database_principals dp on perm.grantee_principal_id = dp.principal_id "
                        "where perm.major_id = object_id(%s) "
                        "order by dp.name, perm.permission_name",
                        (f"{schema}.{table_name}",),
                    )
                    rows = list(cursor.fetchall())
                    return {"status": "ok", "schema": schema, "table": table_name, "grants": rows, "total": len(rows)}
                if tool_name == "read_get_query_history":
                    limit = max(1, min(int(arguments.get("limit") or 100), 1000))
                    cursor.execute(
                        f"select top ({limit}) qs.last_execution_time, qs.execution_count, "
                        "qs.total_elapsed_time, substring(st.text, 1, 4000) as sql_text "
                        "from sys.dm_exec_query_stats qs "
                        "cross apply sys.dm_exec_sql_text(qs.sql_handle) st "
                        "order by qs.last_execution_time desc"
                    )
                    rows = list(cursor.fetchall())
                    return {"status": "ok", "queries": rows, "total": len(rows)}
        raise McpExecutionError(404, f"Unsupported SQL Server MCP tool: {tool_name}")

    if tool_name.startswith("read_"):
        return await asyncio.to_thread(run_read)
    if tool_name not in {
        "write_execute_sql",
        "write_create_table",
        "write_create_view",
        "write_insert_rows",
        "write_update_rows",
        "write_delete_rows",
        "write_grant_permission",
        "write_create_index",
    }:
        raise McpExecutionError(404, f"Unsupported SQL Server MCP tool: {tool_name}")

    sql = _sql_for_write_tool(tool_name, arguments, "sql_server")
    try:
        decision = validate_write_sql(sql)
    except UnsafeSqlError as exc:
        raise McpExecutionError(400, str(exc)) from exc
    workspace = await _workspace(session)
    if decision.action == "requires_approval" and not arguments.get("__approved"):
        alert = Alert(
            workspace_id=workspace.id,
            severity="critical",
            title=f"Agent {agent.name} wants to {decision.statement_type} {decision.target or ''}".strip(),
            detail=(
                "Approval required for DataClaw MCP write SQL.\n"
                "Connector: sql_server\n"
                f"Agent-ID: {agent.id}\n"
                f"SQL: {decision.sql}"
            ),
            requires_approval=True,
        )
        session.add(alert)
        await session.commit()
        return {"status": "pending_approval", "alert_id": alert.id, "sql": decision.sql}

    def run_write() -> int | None:
        with adapter._connect_sync(credentials) as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cursor:
                cursor.execute(decision.sql)
                affected = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else None
            conn.commit()
            return affected

    affected = await asyncio.to_thread(run_write)
    session.add(
        AgentWriteAudit(
            workspace_id=workspace.id,
            agent_id=agent.id,
            connector_slug="sql_server",
            statement_type=decision.statement_type,
            statement=decision.sql,
            target=decision.target,
            affected_rows=affected,
            required_approval=False,
            executed_at=datetime.now(UTC),
            executed_by=user_email,
        )
    )
    session.add(
        LogEntry(
            timestamp=datetime.now(UTC),
            level="info",
            logger_name="dataclaw.mcp",
            message="mcp_write_sql_executed",
            context={
                "agent_id": agent.id,
                "connector_slug": "sql_server",
                "tool": tool_name,
                "statement_type": decision.statement_type,
                "target": decision.target,
            },
        )
    )
    await session.commit()
    return {"status": "executed", "sql": decision.sql, "affected_rows": affected}
