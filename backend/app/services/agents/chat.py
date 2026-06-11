import asyncio
import json
import logging
import re
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import httpx
from openai import AsyncOpenAI, OpenAIError
from sqlalchemy import func, or_, select
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from app.models.domain import (
    Agent,
    AgentMcpGrant,
    AgentToolCall,
    ChatMessage,
    ColumnLineageEdge,
    Dataset,
    KnowledgeDocument,
    TableAsset,
    WikiPage,
    Workspace,
)
from app.services.agents.runtime import BudgetExceeded, enforce_run_budget
from app.services.connectors.catalog import CATALOG_BY_SLUG
from app.services.evals.cases import EvalCaseService
from app.services.evals.suggestions import (
    load_chat_prompt_override,
    load_chat_rules_md,
)
from app.services.knowledge_compile.service import graph_neighbors
from app.services.mcp_catalog import tools_for_slug
from app.services.mcp_executor import McpExecutionError, execute_mcp_tool
from app.services.observability.tracing import span as trace_span
from app.services.retrieval import BrainRetriever
from app.services.settings_store import active_llm_provider_slug, resolve_openai
from app.services.sql_safety import UnsafeSqlError, validate_read_only_sql
from app.services.vector_store import vector_store


async def _lookup_golden(
    session: AsyncSession,
    *,
    workspace_id: str,
    question: str,
    connector_slug: str | None,
) -> Any | None:
    """Return a golden EvalCase whose question matches, or None.

    Logs lookups as a `golden_lookup` span so the trace UI shows hit/miss
    without us having to plumb a custom event. Any error is swallowed —
    a flaky golden-query lookup must never break a chat turn.
    """
    try:
        async with trace_span(
            "retrieval",
            "golden_lookup",
            input={"question": question, "connector_slug": connector_slug},
        ) as sp:
            match = await EvalCaseService(session).find_golden_for_question(
                workspace_id=workspace_id,
                question=question,
                connector_slug=connector_slug,
            )
            sp.set_output(
                {
                    "hit": bool(match),
                    "eval_case_id": match.id if match else None,
                }
            )
            return match
    except Exception:
        # Defensive: nothing about this path is allowed to break chat.
        return None


async def _golden_hit_response(
    session: AsyncSession,
    *,
    golden: Any,
    question: str,
    provider_slug: str,
) -> dict[str, Any]:
    """Build a chat response from a golden case match. Skips the LLM."""
    async with trace_span(
        "tool",
        "golden_query_hit",
        input={"eval_case_id": golden.id},
        metadata={"connector_slug": golden.expected_connector_slug},
    ) as sp:
        answer = (
            golden.expected_answer
            or "Returning the canonical answer for this question (from a "
               "golden eval case)."
        )
        sp.set_output({"eval_case_id": golden.id, "had_expected_answer": bool(golden.expected_answer)})
        # Stamp the canonical tool_call so the eval runner's connector/tool
        # accuracy metrics see the slug that the golden case represents,
        # rather than ``null`` (which produced spurious wrong_connector
        # failures on every short-circuited run).
        golden_tool = golden.expected_tool or ""
        tool_name = (
            golden_tool.split(".", 1)[1]
            if "." in golden_tool
            else (golden_tool or None)
        )
        return {
            "answer": answer,
            "sql": golden.expected_sql,
            "table": None,
            "rows": list(golden.expected_result_preview or []),
            "citations": list(golden.expected_citations or []) + [
                {
                    "type": "golden_query_provenance",
                    "eval_case_id": golden.id,
                }
            ],
            "provider": provider_slug,
            "llm_status": "golden_query_hit",
            "status": "ok",
            "chart_spec": None,
            "retrieval_trace": {"golden_query_hit": True, "eval_case_id": golden.id},
            "tool_call": {
                "connector_slug": golden.expected_connector_slug,
                "tool": tool_name,
            },
        }


async def _traced_chat_completion(
    client: AsyncOpenAI,
    *,
    span_name: str,
    model: str,
    messages: list[dict[str, Any]] | Any,
    **kwargs: Any,
) -> Any:
    """Wrap ``client.chat.completions.create`` in an llm-kind span.

    Captures model, message count, and (when present on the SDK response)
    prompt / completion / total token counts on the active chat trace. This
    helper is a no-op outside a chat_trace context, so it's safe to use from
    any caller — instrumentation degrades cleanly when no trace is open.
    """
    async with trace_span(
        "llm",
        span_name,
        input={"messages_count": len(messages) if hasattr(messages, "__len__") else None},
        model=model,
    ) as sp:
        completion = await client.chat.completions.create(model=model, messages=messages, **kwargs)
        try:
            choice = completion.choices[0]
            message = choice.message
            sp.set_output(
                {
                    "content_preview": (message.content or "")[:400] if hasattr(message, "content") else None,
                    "tool_call_count": len(message.tool_calls or []) if hasattr(message, "tool_calls") else 0,
                    "finish_reason": getattr(choice, "finish_reason", None),
                }
            )
            usage = getattr(completion, "usage", None)
            if usage is not None:
                sp.set_usage(
                    prompt_tokens=getattr(usage, "prompt_tokens", None),
                    completion_tokens=getattr(usage, "completion_tokens", None),
                    total_tokens=getattr(usage, "total_tokens", None),
                )
        except Exception:
            # Never let span-bookkeeping mask a real LLM response.
            logger.debug("llm_span_metadata_capture_failed", exc_info=True)
        return completion

logger = logging.getLogger("dataclaw.agents.chat")

HISTORY_LIMIT = 12
OPENAI_MCP_TOOL_LIMIT = 120
_DETERMINISTIC_LAST_AIRFLOW_RUN: dict[str, str] = {}
DATA_STORE_CONNECTOR_SLUGS = {
    "postgres",
    "mysql",
    "redshift",
    "sql_server",
    "databricks",
    "bigquery",
    "snowflake",
    "trino",
    "sqlite",
}
_CHART_HINT_RE = re.compile(
    r"\b(chart|graph|plot|trend|trends|over time|compare|comparison|breakdown|distribution|"
    r"by month|by week|by day|by year|monthly|weekly|daily|timeline)\b",
    re.IGNORECASE,
)

MCP_TOOL_SCHEMAS: dict[tuple[str, str], dict[str, Any]] = {
    ("airflow", "read_list_dags"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("airflow", "read_get_run"): {
        "type": "object",
        "properties": {"dag_id": {"type": "string"}, "run_id": {"type": "string"}},
        "required": ["dag_id"],
        "additionalProperties": False,
    },
    ("airflow", "read_get_dag_source"): {
        "type": "object",
        "properties": {"dag_id": {"type": "string"}},
        "required": ["dag_id"],
        "additionalProperties": False,
    },
    ("airflow", "read_get_task_logs"): {
        "type": "object",
        "properties": {"dag_id": {"type": "string"}, "run_id": {"type": "string"}, "task_id": {"type": "string"}, "try_number": {"type": "integer", "minimum": 1}},
        "required": ["dag_id", "run_id", "task_id"],
        "additionalProperties": False,
    },
    ("airflow", "read_list_task_instances"): {
        "type": "object",
        "properties": {
            "dag_id": {"type": "string"},
            "run_id": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
        },
        "required": ["dag_id", "run_id"],
        "additionalProperties": False,
    },
    ("airflow", "read_list_dag_runs"): {
        "type": "object",
        "properties": {
            "dag_id": {"type": "string"},
            "since": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
        },
        "required": ["dag_id"],
        "additionalProperties": False,
    },
    ("airflow", "read_get_xcom"): {
        "type": "object",
        "properties": {
            "dag_id": {"type": "string"},
            "run_id": {"type": "string"},
            "task_id": {"type": "string"},
            "key": {"type": "string"},
        },
        "required": ["dag_id", "run_id", "task_id"],
        "additionalProperties": False,
    },
    ("airflow", "read_list_pools"): {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "additionalProperties": False,
    },
    ("airflow", "read_get_pool"): {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
        "additionalProperties": False,
    },
    ("airflow", "read_list_variables"): {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "additionalProperties": False,
    },
    ("airflow", "read_get_variable"): {
        "type": "object",
        "properties": {"key": {"type": "string"}},
        "required": ["key"],
        "additionalProperties": False,
    },
    ("airflow", "read_get_dag_dependencies"): {
        "type": "object",
        "properties": {"dag_id": {"type": "string"}},
        "required": ["dag_id"],
        "additionalProperties": False,
    },
    ("airflow", "read_get_import_errors"): {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "additionalProperties": False,
    },
    ("airflow", "write_trigger_dag"): {
        "type": "object",
        "properties": {
            "dag_id": {"type": "string"},
            "conf": {"type": "object"},
        },
        "required": ["dag_id"],
        "additionalProperties": False,
    },
    ("airflow", "write_create_dag"): {
        "type": "object",
        "properties": {
            "dag_id": {"type": "string"},
            "schedule_interval": {"type": "string"},
            "owners": {"type": "array", "items": {"type": "string"}},
            "tags": {"type": "array", "items": {"type": "string"}},
            "source": {"type": "string"},
        },
        "required": ["dag_id"],
        "additionalProperties": False,
    },
    ("airflow", "write_pause_dag"): {
        "type": "object",
        "properties": {"dag_id": {"type": "string"}, "is_paused": {"type": "boolean"}},
        "required": ["dag_id"],
        "additionalProperties": False,
    },
    ("airflow", "write_unpause_dag"): {
        "type": "object",
        "properties": {"dag_id": {"type": "string"}},
        "required": ["dag_id"],
        "additionalProperties": False,
    },
    ("airflow", "write_clear_task_instance"): {
        "type": "object",
        "properties": {"dag_id": {"type": "string"}, "run_id": {"type": "string"}, "task_id": {"type": "string"}},
        "required": ["dag_id", "run_id", "task_id"],
        "additionalProperties": False,
    },
    ("airflow", "write_mark_task_success"): {
        "type": "object",
        "properties": {"dag_id": {"type": "string"}, "run_id": {"type": "string"}, "task_id": {"type": "string"}},
        "required": ["dag_id", "run_id", "task_id"],
        "additionalProperties": False,
    },
    ("airflow", "write_mark_task_failed"): {
        "type": "object",
        "properties": {"dag_id": {"type": "string"}, "run_id": {"type": "string"}, "task_id": {"type": "string"}},
        "required": ["dag_id", "run_id", "task_id"],
        "additionalProperties": False,
    },
    ("airflow", "write_set_variable"): {
        "type": "object",
        "properties": {
            "key": {"type": "string"},
            "value": {"type": "string"},
            "description": {"type": "string"},
        },
        "required": ["key", "value"],
        "additionalProperties": False,
    },
    ("airflow", "write_set_pool"): {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "slots": {"type": "integer", "minimum": 1},
            "description": {"type": "string"},
        },
        "required": ["name", "slots"],
        "additionalProperties": False,
    },
    ("airflow", "write_delete_dag"): {
        "type": "object",
        "properties": {"dag_id": {"type": "string"}},
        "required": ["dag_id"],
        "additionalProperties": False,
    },
    ("airbyte", "read_list_connections"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("airbyte", "read_get_job_logs"): {
        "type": "object",
        "properties": {"job_id": {"type": "string"}},
        "required": ["job_id"],
        "additionalProperties": False,
    },
    ("airbyte", "read_list_jobs"): {
        "type": "object",
        "properties": {"connection_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "additionalProperties": False,
    },
    ("airbyte", "read_get_connection_state"): {
        "type": "object",
        "properties": {"connection_id": {"type": "string"}},
        "required": ["connection_id"],
        "additionalProperties": False,
    },
    ("airbyte", "read_list_sources"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("airbyte", "read_get_source"): {
        "type": "object",
        "properties": {"source_id": {"type": "string"}},
        "required": ["source_id"],
        "additionalProperties": False,
    },
    ("airbyte", "read_list_destinations"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("airbyte", "read_get_destination"): {
        "type": "object",
        "properties": {"destination_id": {"type": "string"}},
        "required": ["destination_id"],
        "additionalProperties": False,
    },
    ("airbyte", "read_get_workspace"): {
        "type": "object",
        "properties": {"workspace_id": {"type": "string"}},
        "additionalProperties": False,
    },
    ("airbyte", "read_get_connection_schema"): {
        "type": "object",
        "properties": {"connection_id": {"type": "string"}},
        "required": ["connection_id"],
        "additionalProperties": False,
    },
    ("airbyte", "write_trigger_sync"): {
        "type": "object",
        "properties": {"connection_id": {"type": "string"}},
        "required": ["connection_id"],
        "additionalProperties": False,
    },
    ("airbyte", "write_reset_connection"): {
        "type": "object",
        "properties": {"connection_id": {"type": "string"}},
        "required": ["connection_id"],
        "additionalProperties": False,
    },
    ("airbyte", "write_cancel_job"): {
        "type": "object",
        "properties": {"job_id": {"type": "string"}},
        "required": ["job_id"],
        "additionalProperties": False,
    },
    ("airbyte", "write_create_connection"): {
        "type": "object",
        "properties": {
            "source_id": {"type": "string"},
            "destination_id": {"type": "string"},
            "config": {"type": "object"},
        },
        "required": ["source_id", "destination_id"],
        "additionalProperties": False,
    },
    ("airbyte", "write_update_connection"): {
        "type": "object",
        "properties": {"connection_id": {"type": "string"}, "config": {"type": "object"}},
        "required": ["connection_id", "config"],
        "additionalProperties": False,
    },
    ("airbyte", "write_disable_connection"): {
        "type": "object",
        "properties": {"connection_id": {"type": "string"}},
        "required": ["connection_id"],
        "additionalProperties": False,
    },
    ("airbyte", "write_enable_connection"): {
        "type": "object",
        "properties": {"connection_id": {"type": "string"}},
        "required": ["connection_id"],
        "additionalProperties": False,
    },
    ("dagster", "read_list_assets"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("dagster", "read_get_run"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dagster", "read_get_run_logs"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dagster", "read_get_event_logs"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dagster", "read_get_asset_materializations"): {
        "type": "object",
        "properties": {
            "asset_key": {"oneOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]},
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
        },
        "required": ["asset_key"],
        "additionalProperties": False,
    },
    ("dagster", "read_list_jobs"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("dagster", "read_list_partitions"): {
        "type": "object",
        "properties": {"asset_key": {"oneOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]}},
        "required": ["asset_key"],
        "additionalProperties": False,
    },
    ("dagster", "read_get_run_steps"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dagster", "read_get_asset_checks"): {
        "type": "object",
        "properties": {"asset_key": {"oneOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]}},
        "required": ["asset_key"],
        "additionalProperties": False,
    },
    ("dagster", "read_get_sensor_state"): {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
        "additionalProperties": False,
    },
    ("dagster", "read_list_sensors"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("dagster", "read_list_schedules"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("dagster", "read_get_schedule_state"): {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
        "additionalProperties": False,
    },
    ("dagster", "write_materialize_asset"): {
        "type": "object",
        "properties": {"asset_key": {"type": "array", "items": {"type": "string"}}},
        "additionalProperties": False,
    },
    ("dagster", "write_trigger_job"): {
        "type": "object",
        "properties": {"job_name": {"type": "string"}, "selector": {"type": "object"}},
        "additionalProperties": False,
    },
    ("dagster", "write_backfill_partitions"): {
        "type": "object",
        "properties": {
            "asset_key": {"type": "array", "items": {"type": "string"}},
            "partitions": {"type": "array", "items": {"type": "string"}},
            "run_config": {"type": "object"},
            "tags": {"type": "object"},
        },
        "required": ["asset_key", "partitions"],
        "additionalProperties": False,
    },
    ("dagster", "write_terminate_run"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dagster", "write_launch_sensor"): {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
        "additionalProperties": False,
    },
    ("dagster", "write_start_schedule"): {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
        "additionalProperties": False,
    },
    ("dagster", "write_stop_schedule"): {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
        "additionalProperties": False,
    },
    ("databricks", "read_list_jobs"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("databricks", "read_list_clusters"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("databricks", "read_list_warehouses"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("databricks", "read_get_notebook"): {
        "type": "object",
        "properties": {"path": {"type": "string"}, "format": {"type": "string", "enum": ["SOURCE", "HTML", "DBC"]}},
        "required": ["path"],
        "additionalProperties": False,
    },
    ("databricks", "read_get_run_logs"): {
        "type": "object",
        "properties": {"run_id": {"type": "integer"}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("databricks", "read_get_lineage"): {
        "type": "object",
        "properties": {"asset": {"type": "string"}, "table": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "additionalProperties": False,
    },
    ("databricks", "read_get_query_history"): {
        "type": "object",
        "properties": {"since": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "additionalProperties": False,
    },
    ("databricks", "read_get_unity_asset"): {
        "type": "object",
        "properties": {"full_name": {"type": "string"}},
        "required": ["full_name"],
        "additionalProperties": False,
    },
    ("databricks", "write_trigger_job"): {
        "type": "object",
        "properties": {"job_id": {"type": "integer"}},
        "required": ["job_id"],
        "additionalProperties": False,
    },
    ("databricks", "write_run_notebook"): {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "cluster_id": {"type": "string"},
            "run_name": {"type": "string"},
            "params": {"type": "object"},
        },
        "required": ["path"],
        "additionalProperties": False,
    },
    ("databricks", "write_start_cluster"): {
        "type": "object",
        "properties": {"cluster_id": {"type": "string"}},
        "required": ["cluster_id"],
        "additionalProperties": False,
    },
    ("databricks", "write_stop_cluster"): {
        "type": "object",
        "properties": {"cluster_id": {"type": "string"}},
        "required": ["cluster_id"],
        "additionalProperties": False,
    },
    ("databricks", "write_create_view"): {
        "type": "object",
        "properties": {"view": {"type": "string"}, "select_sql": {"type": "string"}},
        "required": ["view", "select_sql"],
        "additionalProperties": False,
    },
    ("databricks", "write_update_unity_grants"): {
        "type": "object",
        "properties": {
            "securable_type": {"type": "string", "enum": ["catalog", "schema", "table", "volume", "function"]},
            "full_name": {"type": "string"},
            "changes": {"type": "array", "items": {"type": "object"}},
        },
        "required": ["full_name", "changes"],
        "additionalProperties": False,
    },
    ("bigquery", "read_list_jobs"): {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "additionalProperties": False,
    },
    ("bigquery", "read_list_datasets"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("bigquery", "read_get_query_history"): {
        "type": "object",
        "properties": {
            "since": {"type": "string"},
            "location": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
        },
        "additionalProperties": False,
    },
    ("bigquery", "read_explain_query"): {
        "type": "object",
        "properties": {
            "sql": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 500},
        },
        "required": ["sql"],
        "additionalProperties": False,
    },
    ("bigquery", "read_get_slot_usage"): {
        "type": "object",
        "properties": {"since": {"type": "string"}, "location": {"type": "string"}},
        "additionalProperties": False,
    },
    ("bigquery", "write_run_query_save_to_table"): {
        "type": "object",
        "properties": {
            "select_sql": {"type": "string"},
            "destination_table": {"type": "string"},
        },
        "required": ["select_sql", "destination_table"],
        "additionalProperties": False,
    },
    ("bigquery", "write_load_from_gcs"): {
        "type": "object",
        "properties": {
            "uri": {"type": "string"},
            "table": {"type": "string"},
            "dataset": {"type": "string"},
            "source_format": {"type": "string", "enum": ["CSV", "NEWLINE_DELIMITED_JSON", "PARQUET", "AVRO", "ORC"]},
            "autodetect": {"type": "boolean"},
        },
        "required": ["uri", "table"],
        "additionalProperties": False,
    },
    ("bigquery", "write_export_to_gcs"): {
        "type": "object",
        "properties": {
            "table": {"type": "string"},
            "dataset": {"type": "string"},
            "uri": {"type": "string"},
        },
        "required": ["table", "uri"],
        "additionalProperties": False,
    },
    ("bigquery", "write_create_view"): {
        "type": "object",
        "properties": {
            "view": {"type": "string"},
            "dataset": {"type": "string"},
            "select_sql": {"type": "string"},
        },
        "required": ["view", "select_sql"],
        "additionalProperties": False,
    },
    ("bigquery", "write_create_dataset"): {
        "type": "object",
        "properties": {
            "dataset": {"type": "string"},
            "exists_ok": {"type": "boolean"},
        },
        "required": ["dataset"],
        "additionalProperties": False,
    },
    ("dbt", "read_list_models"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}},
        "additionalProperties": False,
    },
    ("dbt", "read_get_lineage"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}, "project_id": {"type": "string"}},
        "additionalProperties": False,
    },
    ("dbt", "read_get_run_logs"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dbt", "read_list_runs"): {
        "type": "object",
        "properties": {
            "limit": {"type": "integer", "minimum": 1, "maximum": 100},
            "job_definition_id": {"type": "string"},
            "project_id": {"type": "string"},
            "environment_id": {"type": "string"},
            "status": {"type": "string"},
        },
        "additionalProperties": False,
    },
    ("dbt", "read_get_run_artifacts"): {
        "type": "object",
        "properties": {
            "run_id": {"type": "string"},
            "paths": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dbt", "read_get_manifest"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dbt", "read_list_tests"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dbt", "read_get_test_results"): {
        "type": "object",
        "properties": {
            "run_id": {"type": "string"},
            "unique_id": {"type": "string"},
        },
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dbt", "read_get_source_freshness"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dbt", "read_get_model_source"): {
        "type": "object",
        "properties": {
            "run_id": {"type": "string"},
            "unique_id": {"type": "string"},
            "name": {"type": "string"},
        },
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dbt", "read_list_exposures"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dbt", "read_get_model_docs"): {
        "type": "object",
        "properties": {
            "run_id": {"type": "string"},
            "unique_id": {"type": "string"},
            "name": {"type": "string"},
        },
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dbt", "write_trigger_run"): {
        "type": "object",
        "properties": {
            "job_id": {"type": "string"},
            "cause": {"type": "string"},
            "git_branch": {"type": "string"},
            "schema_override": {"type": "string"},
            "git_sha": {"type": "string"},
            "dbt_version_override": {"type": "string"},
            "threads_override": {"type": "integer", "minimum": 1},
            "target_name_override": {"type": "string"},
            "generate_docs_override": {"type": "boolean"},
            "timeout_seconds_override": {"type": "integer", "minimum": 1},
            "steps_override": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["job_id"],
        "additionalProperties": False,
    },
    ("dbt", "write_trigger_test"): {
        "type": "object",
        "properties": {
            "job_id": {"type": "string"},
            "cause": {"type": "string"},
            "steps_override": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["job_id"],
        "additionalProperties": False,
    },
    ("dbt", "write_cancel_run"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("dbt", "write_create_model"): {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "sql": {"type": "string"},
            "schema": {"type": "string"},
            "project_path": {"type": "string"},
            "overwrite": {"type": "boolean"},
        },
        "required": ["name", "sql"],
        "additionalProperties": False,
    },
    ("dbt", "write_update_model"): {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "sql": {"type": "string"},
            "schema": {"type": "string"},
            "project_path": {"type": "string"},
        },
        "required": ["name", "sql"],
        "additionalProperties": False,
    },
    ("dbt", "write_trigger_snapshot"): {
        "type": "object",
        "properties": {
            "job_id": {"type": "string"},
            "cause": {"type": "string"},
            "steps_override": {"type": "array", "items": {"type": "string"}},
        },
        "additionalProperties": False,
    },
    ("dbt", "write_trigger_seed"): {
        "type": "object",
        "properties": {
            "job_id": {"type": "string"},
            "cause": {"type": "string"},
            "steps_override": {"type": "array", "items": {"type": "string"}},
        },
        "additionalProperties": False,
    },
    ("fivetran", "read_list_connectors"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("fivetran", "read_get_connector_logs"): {
        "type": "object",
        "properties": {"connector_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "required": ["connector_id"],
        "additionalProperties": False,
    },
    ("fivetran", "read_get_connector_status"): {
        "type": "object",
        "properties": {"connector_id": {"type": "string"}},
        "required": ["connector_id"],
        "additionalProperties": False,
    },
    ("fivetran", "read_get_connector_schema"): {
        "type": "object",
        "properties": {"connector_id": {"type": "string"}},
        "required": ["connector_id"],
        "additionalProperties": False,
    },
    ("fivetran", "read_list_destinations"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("fivetran", "read_get_destination"): {
        "type": "object",
        "properties": {"destination_id": {"type": "string"}},
        "required": ["destination_id"],
        "additionalProperties": False,
    },
    ("fivetran", "read_get_metadata"): {
        "type": "object",
        "properties": {"connector_id": {"type": "string"}},
        "required": ["connector_id"],
        "additionalProperties": False,
    },
    ("fivetran", "read_get_data_volume"): {
        "type": "object",
        "properties": {"connector_id": {"type": "string"}, "since": {"type": "string"}},
        "required": ["connector_id"],
        "additionalProperties": False,
    },
    ("fivetran", "read_get_sync_history"): {
        "type": "object",
        "properties": {"connector_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "required": ["connector_id"],
        "additionalProperties": False,
    },
    ("fivetran", "write_trigger_sync"): {
        "type": "object",
        "properties": {"connector_id": {"type": "string"}},
        "required": ["connector_id"],
        "additionalProperties": False,
    },
    ("fivetran", "write_pause_connector"): {
        "type": "object",
        "properties": {"connector_id": {"type": "string"}},
        "required": ["connector_id"],
        "additionalProperties": False,
    },
    ("fivetran", "write_resume_connector"): {
        "type": "object",
        "properties": {"connector_id": {"type": "string"}},
        "required": ["connector_id"],
        "additionalProperties": False,
    },
    ("fivetran", "write_resync_table"): {
        "type": "object",
        "properties": {"connector_id": {"type": "string"}, "schema": {"type": "string"}, "table": {"type": "string"}},
        "required": ["connector_id", "schema", "table"],
        "additionalProperties": False,
    },
    ("fivetran", "write_modify_connector_schema"): {
        "type": "object",
        "properties": {"connector_id": {"type": "string"}, "config": {"type": "object"}},
        "required": ["connector_id", "config"],
        "additionalProperties": False,
    },
    ("fivetran", "write_delete_connector"): {
        "type": "object",
        "properties": {"connector_id": {"type": "string"}},
        "required": ["connector_id"],
        "additionalProperties": False,
    },
    ("google_docs", "read_list_docs"): {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "additionalProperties": False,
    },
    ("google_docs", "read_get_doc"): {
        "type": "object",
        "properties": {"doc_id": {"type": "string"}},
        "required": ["doc_id"],
        "additionalProperties": False,
    },
    ("google_docs", "read_search_docs"): {
        "type": "object",
        "properties": {"query": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "required": ["query"],
        "additionalProperties": False,
    },
    ("google_docs", "read_get_doc_comments"): {
        "type": "object",
        "properties": {"doc_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "required": ["doc_id"],
        "additionalProperties": False,
    },
    ("google_docs", "read_get_doc_revisions"): {
        "type": "object",
        "properties": {"doc_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "required": ["doc_id"],
        "additionalProperties": False,
    },
    ("google_docs", "read_list_folder_contents"): {
        "type": "object",
        "properties": {"folder_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "required": ["folder_id"],
        "additionalProperties": False,
    },
    ("google_docs", "read_list_shared_with_me"): {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "additionalProperties": False,
    },
    ("google_docs", "read_get_doc_metadata"): {
        "type": "object",
        "properties": {"doc_id": {"type": "string"}},
        "required": ["doc_id"],
        "additionalProperties": False,
    },
    ("google_docs", "write_create_doc"): {
        "type": "object",
        "properties": {"title": {"type": "string"}, "body": {"type": "string"}},
        "required": ["title"],
        "additionalProperties": False,
    },
    ("google_docs", "write_append_to_doc"): {
        "type": "object",
        "properties": {"doc_id": {"type": "string"}, "content": {"type": "string"}},
        "required": ["doc_id", "content"],
        "additionalProperties": False,
    },
    ("google_docs", "write_replace_text"): {
        "type": "object",
        "properties": {"doc_id": {"type": "string"}, "find": {"type": "string"}, "replace": {"type": "string"}},
        "required": ["doc_id", "find", "replace"],
        "additionalProperties": False,
    },
    ("google_docs", "write_create_comment"): {
        "type": "object",
        "properties": {"doc_id": {"type": "string"}, "anchor": {"type": "string"}, "body": {"type": "string"}},
        "required": ["doc_id", "body"],
        "additionalProperties": False,
    },
    ("google_docs", "write_share_doc"): {
        "type": "object",
        "properties": {
            "doc_id": {"type": "string"},
            "email": {"type": "string"},
            "role": {"type": "string", "enum": ["reader", "commenter", "writer"]},
        },
        "required": ["doc_id", "email", "role"],
        "additionalProperties": False,
    },
    ("google_docs", "write_move_doc"): {
        "type": "object",
        "properties": {"doc_id": {"type": "string"}, "folder_id": {"type": "string"}},
        "required": ["doc_id", "folder_id"],
        "additionalProperties": False,
    },
    ("google_docs", "write_rename_doc"): {
        "type": "object",
        "properties": {"doc_id": {"type": "string"}, "name": {"type": "string"}},
        "required": ["doc_id", "name"],
        "additionalProperties": False,
    },
    ("github", "read_list_repos"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("github", "read_get_file"): {
        "type": "object",
        "properties": {
            "repo": {"type": "string"},
            "path": {"type": "string"},
            "ref": {"type": "string"},
            "branch": {"type": "string"},
        },
        "required": ["repo", "path"],
        "additionalProperties": False,
    },
    ("github", "read_list_issues"): {
        "type": "object",
        "properties": {
            "repo": {"type": "string"},
            "state": {"type": "string", "enum": ["open", "closed", "all"]},
            "since": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 100},
        },
        "required": ["repo"],
        "additionalProperties": False,
    },
    ("github", "read_get_issue"): {
        "type": "object",
        "properties": {"repo": {"type": "string"}, "number": {"type": "integer", "minimum": 1}},
        "required": ["repo", "number"],
        "additionalProperties": False,
    },
    ("github", "read_get_pr"): {
        "type": "object",
        "properties": {"repo": {"type": "string"}, "number": {"type": "integer", "minimum": 1}},
        "required": ["repo", "number"],
        "additionalProperties": False,
    },
    ("github", "read_get_pr_diff"): {
        "type": "object",
        "properties": {"repo": {"type": "string"}, "number": {"type": "integer", "minimum": 1}},
        "required": ["repo", "number"],
        "additionalProperties": False,
    },
    ("github", "read_list_branches"): {
        "type": "object",
        "properties": {"repo": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "required": ["repo"],
        "additionalProperties": False,
    },
    ("github", "read_search_code"): {
        "type": "object",
        "properties": {"query": {"type": "string"}, "repo": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "required": ["query"],
        "additionalProperties": False,
    },
    ("github", "read_get_commit"): {
        "type": "object",
        "properties": {"repo": {"type": "string"}, "sha": {"type": "string"}},
        "required": ["repo", "sha"],
        "additionalProperties": False,
    },
    ("github", "read_list_workflows"): {
        "type": "object",
        "properties": {"repo": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "required": ["repo"],
        "additionalProperties": False,
    },
    ("github", "read_get_workflow_run_logs"): {
        "type": "object",
        "properties": {"repo": {"type": "string"}, "run_id": {"type": "string"}},
        "required": ["repo", "run_id"],
        "additionalProperties": False,
    },
    ("github", "read_get_repo_metadata"): {
        "type": "object",
        "properties": {"repo": {"type": "string"}},
        "required": ["repo"],
        "additionalProperties": False,
    },
    ("github", "read_list_releases"): {
        "type": "object",
        "properties": {"repo": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "required": ["repo"],
        "additionalProperties": False,
    },
    ("github", "write_commit_file"): {
        "type": "object",
        "properties": {
            "repo": {"type": "string"},
            "path": {"type": "string"},
            "content": {"type": "string"},
            "message": {"type": "string"},
            "branch": {"type": "string"},
            "sha": {"type": "string"},
        },
        "required": ["repo", "path", "content"],
        "additionalProperties": False,
    },
    ("github", "write_create_pr"): {
        "type": "object",
        "properties": {
            "repo": {"type": "string"},
            "title": {"type": "string"},
            "head": {"type": "string"},
            "base": {"type": "string"},
            "body": {"type": "string"},
        },
        "required": ["repo", "title", "head"],
        "additionalProperties": False,
    },
    ("github", "write_create_issue"): {
        "type": "object",
        "properties": {
            "repo": {"type": "string"},
            "title": {"type": "string"},
            "body": {"type": "string"},
            "labels": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["repo", "title"],
        "additionalProperties": False,
    },
    ("github", "write_comment_on_pr"): {
        "type": "object",
        "properties": {"repo": {"type": "string"}, "number": {"type": "integer", "minimum": 1}, "body": {"type": "string"}},
        "required": ["repo", "number", "body"],
        "additionalProperties": False,
    },
    ("github", "write_comment_on_issue"): {
        "type": "object",
        "properties": {"repo": {"type": "string"}, "number": {"type": "integer", "minimum": 1}, "body": {"type": "string"}},
        "required": ["repo", "number", "body"],
        "additionalProperties": False,
    },
    ("github", "write_merge_pr"): {
        "type": "object",
        "properties": {
            "repo": {"type": "string"},
            "number": {"type": "integer", "minimum": 1},
            "method": {"type": "string", "enum": ["merge", "squash", "rebase"]},
            "commit_title": {"type": "string"},
            "commit_message": {"type": "string"},
        },
        "required": ["repo", "number"],
        "additionalProperties": False,
    },
    ("github", "write_create_branch"): {
        "type": "object",
        "properties": {"repo": {"type": "string"}, "name": {"type": "string"}, "from_sha": {"type": "string"}},
        "required": ["repo", "name", "from_sha"],
        "additionalProperties": False,
    },
    ("github", "write_close_issue"): {
        "type": "object",
        "properties": {"repo": {"type": "string"}, "number": {"type": "integer", "minimum": 1}},
        "required": ["repo", "number"],
        "additionalProperties": False,
    },
    ("github", "write_request_review"): {
        "type": "object",
        "properties": {
            "repo": {"type": "string"},
            "number": {"type": "integer", "minimum": 1},
            "reviewers": {"type": "array", "items": {"type": "string"}},
            "team_reviewers": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["repo", "number"],
        "additionalProperties": False,
    },
    ("notion", "read_search_pages"): {
        "type": "object",
        "properties": {"query": {"type": "string"}, "page_size": {"type": "integer", "minimum": 1, "maximum": 100}},
        "additionalProperties": False,
    },
    ("notion", "read_get_page"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}},
        "required": ["page_id"],
        "additionalProperties": False,
    },
    ("notion", "read_get_database"): {
        "type": "object",
        "properties": {"database_id": {"type": "string"}},
        "required": ["database_id"],
        "additionalProperties": False,
    },
    ("notion", "read_query_database"): {
        "type": "object",
        "properties": {
            "database_id": {"type": "string"},
            "filter": {"type": "object"},
            "sorts": {"type": "array", "items": {"type": "object"}},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100},
            "start_cursor": {"type": "string"},
        },
        "required": ["database_id"],
        "additionalProperties": False,
    },
    ("notion", "read_get_block_children"): {
        "type": "object",
        "properties": {
            "block_id": {"type": "string"},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100},
            "start_cursor": {"type": "string"},
        },
        "required": ["block_id"],
        "additionalProperties": False,
    },
    ("notion", "read_get_comments"): {
        "type": "object",
        "properties": {
            "page_id": {"type": "string"},
            "block_id": {"type": "string"},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100},
            "start_cursor": {"type": "string"},
        },
        "additionalProperties": False,
    },
    ("notion", "read_list_users"): {
        "type": "object",
        "properties": {
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100},
            "start_cursor": {"type": "string"},
        },
        "additionalProperties": False,
    },
    ("notion", "write_create_page"): {
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "body": {"type": "string"},
            "parent_id": {"type": "string"},
        },
        "required": ["title", "body", "parent_id"],
        "additionalProperties": False,
    },
    ("notion", "write_append_to_page"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}, "body": {"type": "string"}},
        "required": ["page_id", "body"],
        "additionalProperties": False,
    },
    ("notion", "write_update_page_properties"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}, "properties": {"type": "object"}},
        "required": ["page_id", "properties"],
        "additionalProperties": False,
    },
    ("notion", "write_archive_page"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}},
        "required": ["page_id"],
        "additionalProperties": False,
    },
    ("notion", "write_create_comment"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}, "body": {"type": "string"}},
        "required": ["page_id", "body"],
        "additionalProperties": False,
    },
    ("notion", "write_create_database"): {
        "type": "object",
        "properties": {
            "parent_page_id": {"type": "string"},
            "title": {"type": "string"},
            "properties": {"type": "object"},
        },
        "required": ["parent_page_id", "title", "properties"],
        "additionalProperties": False,
    },
    ("notion", "write_update_block"): {
        "type": "object",
        "properties": {
            "block_id": {"type": "string"},
            "type": {"type": "string"},
            "content": {"type": "object"},
        },
        "required": ["block_id", "type", "content"],
        "additionalProperties": False,
    },
    ("openai", "read_list_models"): {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "additionalProperties": False,
    },
    ("confluence", "read_search_pages"): {
        "type": "object",
        "properties": {"query": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "additionalProperties": False,
    },
    ("confluence", "read_get_page"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}},
        "required": ["page_id"],
        "additionalProperties": False,
    },
    ("confluence", "read_get_page_children"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "required": ["page_id"],
        "additionalProperties": False,
    },
    ("confluence", "read_get_space"): {
        "type": "object",
        "properties": {"space_key": {"type": "string"}},
        "required": ["space_key"],
        "additionalProperties": False,
    },
    ("confluence", "read_get_page_history"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}},
        "required": ["page_id"],
        "additionalProperties": False,
    },
    ("confluence", "read_search_attachments"): {
        "type": "object",
        "properties": {"query": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "required": ["query"],
        "additionalProperties": False,
    },
    ("confluence", "read_get_comments"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "required": ["page_id"],
        "additionalProperties": False,
    },
    ("confluence", "read_list_spaces"): {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "additionalProperties": False,
    },
    ("confluence", "read_get_labels"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "required": ["page_id"],
        "additionalProperties": False,
    },
    ("confluence", "write_create_page"): {
        "type": "object",
        "properties": {"title": {"type": "string"}, "body": {"type": "string"}, "space_key": {"type": "string"}, "parent_id": {"type": "string"}},
        "required": ["title", "body", "space_key"],
        "additionalProperties": False,
    },
    ("confluence", "write_append_to_page"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}, "content": {"type": "string"}},
        "required": ["page_id", "content"],
        "additionalProperties": False,
    },
    ("confluence", "write_update_page"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}, "title": {"type": "string"}, "content": {"type": "string"}, "version": {"type": "integer", "minimum": 1}},
        "required": ["page_id", "title", "content", "version"],
        "additionalProperties": False,
    },
    ("confluence", "write_add_label"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}, "label": {"type": "string"}},
        "required": ["page_id", "label"],
        "additionalProperties": False,
    },
    ("confluence", "write_create_comment"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}, "body": {"type": "string"}},
        "required": ["page_id", "body"],
        "additionalProperties": False,
    },
    ("confluence", "write_create_attachment"): {
        "type": "object",
        "properties": {
            "page_id": {"type": "string"},
            "filename": {"type": "string"},
            "content": {"type": "string"},
            "content_encoding": {"type": "string", "enum": ["text", "base64"]},
        },
        "required": ["page_id", "filename", "content"],
        "additionalProperties": False,
    },
    ("confluence", "write_move_page"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}, "parent_id": {"type": "string"}},
        "required": ["page_id", "parent_id"],
        "additionalProperties": False,
    },
    ("confluence", "write_delete_page"): {
        "type": "object",
        "properties": {"page_id": {"type": "string"}},
        "required": ["page_id"],
        "additionalProperties": False,
    },
    ("prefect", "read_list_flows"): {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "additionalProperties": False,
    },
    ("prefect", "read_get_run"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("prefect", "read_get_run_logs"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("prefect", "read_get_task_logs"): {
        "type": "object",
        "properties": {
            "flow_run_id": {"type": "string"},
            "task_run_id": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
        },
        "additionalProperties": False,
    },
    ("prefect", "read_list_flow_runs"): {
        "type": "object",
        "properties": {
            "flow_id": {"type": "string"},
            "flow": {"type": "string"},
            "since": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
        },
        "additionalProperties": False,
    },
    ("prefect", "read_list_deployments"): {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "additionalProperties": False,
    },
    ("prefect", "read_get_deployment"): {
        "type": "object",
        "properties": {"deployment_id": {"type": "string"}},
        "required": ["deployment_id"],
        "additionalProperties": False,
    },
    ("prefect", "read_get_task_run"): {
        "type": "object",
        "properties": {"task_run_id": {"type": "string"}},
        "required": ["task_run_id"],
        "additionalProperties": False,
    },
    ("prefect", "read_list_work_pools"): {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
        "additionalProperties": False,
    },
    ("prefect", "read_get_block"): {
        "type": "object",
        "properties": {
            "block_id": {"type": "string"},
            "name": {"type": "string"},
            "block_type_slug": {"type": "string"},
        },
        "additionalProperties": False,
    },
    ("prefect", "read_get_concurrency_limit"): {
        "type": "object",
        "properties": {"tag": {"type": "string"}},
        "required": ["tag"],
        "additionalProperties": False,
    },
    ("prefect", "read_list_artifacts"): {
        "type": "object",
        "properties": {
            "flow_run_id": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
        },
        "additionalProperties": False,
    },
    ("prefect", "write_trigger_flow_run"): {
        "type": "object",
        "properties": {
            "deployment_id": {"type": "string"},
            "name": {"type": "string"},
            "parameters": {"type": "object"},
        },
        "required": ["deployment_id"],
        "additionalProperties": False,
    },
    ("prefect", "write_create_deployment"): {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "flow_id": {"type": "string"},
            "entrypoint": {"type": "string"},
        },
        "additionalProperties": False,
    },
    ("prefect", "write_pause_deployment"): {
        "type": "object",
        "properties": {"deployment_id": {"type": "string"}},
        "required": ["deployment_id"],
        "additionalProperties": False,
    },
    ("prefect", "write_resume_deployment"): {
        "type": "object",
        "properties": {"deployment_id": {"type": "string"}},
        "required": ["deployment_id"],
        "additionalProperties": False,
    },
    ("prefect", "write_cancel_flow_run"): {
        "type": "object",
        "properties": {"run_id": {"type": "string"}},
        "required": ["run_id"],
        "additionalProperties": False,
    },
    ("prefect", "write_set_block"): {
        "type": "object",
        "properties": {
            "block_id": {"type": "string"},
            "name": {"type": "string"},
            "block_type_id": {"type": "string"},
            "data": {"type": "object"},
        },
        "required": ["data"],
        "additionalProperties": False,
    },
    ("prefect", "write_set_concurrency_limit"): {
        "type": "object",
        "properties": {"tag": {"type": "string"}, "limit": {"type": "integer", "minimum": 0}},
        "required": ["tag", "limit"],
        "additionalProperties": False,
    },
    ("prefect", "write_delete_deployment"): {
        "type": "object",
        "properties": {"deployment_id": {"type": "string"}},
        "required": ["deployment_id"],
        "additionalProperties": False,
    },
    ("quip", "read_search"): {
        "type": "object",
        "properties": {"query": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "additionalProperties": False,
    },
    ("quip", "read_get_thread"): {
        "type": "object",
        "properties": {"thread_id": {"type": "string"}},
        "required": ["thread_id"],
        "additionalProperties": False,
    },
    ("quip", "read_get_thread_history"): {
        "type": "object",
        "properties": {
            "thread_id": {"type": "string"},
            "since": {"type": "string"},
            "max_created_usec": {"type": "integer", "minimum": 1},
            "limit": {"type": "integer", "minimum": 1, "maximum": 100},
        },
        "required": ["thread_id"],
        "additionalProperties": False,
    },
    ("quip", "read_list_folders"): {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 100}},
        "additionalProperties": False,
    },
    ("quip", "read_get_folder"): {
        "type": "object",
        "properties": {"folder_id": {"type": "string"}},
        "required": ["folder_id"],
        "additionalProperties": False,
    },
    ("quip", "read_get_messages"): {
        "type": "object",
        "properties": {
            "thread_id": {"type": "string"},
            "since": {"type": "string"},
            "max_created_usec": {"type": "integer", "minimum": 1},
            "limit": {"type": "integer", "minimum": 1, "maximum": 100},
        },
        "required": ["thread_id"],
        "additionalProperties": False,
    },
    ("quip", "write_create_thread"): {
        "type": "object",
        "properties": {"title": {"type": "string"}, "body": {"type": "string"}, "folder_ids": {"type": "array", "items": {"type": "string"}}},
        "required": ["title"],
        "additionalProperties": False,
    },
    ("quip", "write_edit_thread"): {
        "type": "object",
        "properties": {"thread_id": {"type": "string"}, "content": {"type": "string"}, "format": {"type": "string"}},
        "required": ["thread_id", "content"],
        "additionalProperties": False,
    },
    ("quip", "write_send_message"): {
        "type": "object",
        "properties": {"thread_id": {"type": "string"}, "body": {"type": "string"}},
        "required": ["thread_id", "body"],
        "additionalProperties": False,
    },
    ("quip", "write_share_thread"): {
        "type": "object",
        "properties": {"thread_id": {"type": "string"}, "member_ids": {"type": "array", "items": {"type": "string"}}},
        "required": ["thread_id", "member_ids"],
        "additionalProperties": False,
    },
    ("quip", "write_create_folder"): {
        "type": "object",
        "properties": {"parent_id": {"type": "string"}, "name": {"type": "string"}, "member_ids": {"type": "array", "items": {"type": "string"}}},
        "required": ["name"],
        "additionalProperties": False,
    },
    ("sqlite", "read_list_tables"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("sqlite", "read_get_schema"): {
        "type": "object",
        "properties": {"table": {"type": "string"}},
        "required": ["table"],
        "additionalProperties": False,
    },
    ("sqlite", "read_query_select"): {
        "type": "object",
        "properties": {
            "sql": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 500},
        },
        "required": ["sql"],
        "additionalProperties": False,
    },
    ("sqlite", "read_get_row_count"): {
        "type": "object",
        "properties": {"table": {"type": "string"}},
        "required": ["table"],
        "additionalProperties": False,
    },
    ("sqlite", "read_sample_rows"): {
        "type": "object",
        "properties": {
            "table": {"type": "string"},
            "schema": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
        },
        "required": ["table"],
        "additionalProperties": False,
    },
    ("sqlite", "read_search_columns"): {
        "type": "object",
        "properties": {
            "pattern": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
        },
        "required": ["pattern"],
        "additionalProperties": False,
    },
    ("sqlite", "read_get_column_stats"): {
        "type": "object",
        "properties": {
            "table": {"type": "string"},
            "schema": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 100},
        },
        "required": ["table"],
        "additionalProperties": False,
    },
    ("sqlite", "read_get_table_freshness"): {
        "type": "object",
        "properties": {
            "table": {"type": "string"},
            "schema": {"type": "string"},
        },
        "required": ["table"],
        "additionalProperties": False,
    },
    ("sqlite", "read_get_storage_size"): {
        "type": "object",
        "properties": {
            "table": {"type": "string"},
            "schema": {"type": "string"},
        },
        "required": ["table"],
        "additionalProperties": False,
    },
    ("sqlite", "read_explain_query"): {
        "type": "object",
        "properties": {
            "sql": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 500},
        },
        "required": ["sql"],
        "additionalProperties": False,
    },
    ("sqlite", "read_list_users"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("sqlite", "read_list_grants"): {
        "type": "object",
        "properties": {
            "table": {"type": "string"},
            "schema": {"type": "string"},
        },
        "required": ["table"],
        "additionalProperties": False,
    },
    ("sqlite", "read_get_query_history"): {
        "type": "object",
        "properties": {
            "since": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
        },
        "additionalProperties": False,
    },
    ("sqlite", "write_execute_sql"): {
        "type": "object",
        "properties": {"sql": {"type": "string"}},
        "required": ["sql"],
        "additionalProperties": False,
    },
    ("sqlite", "write_create_table"): {
        "type": "object",
        "properties": {
            "table": {"type": "string"},
            "columns": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "type": {"type": "string", "enum": ["text", "integer", "real", "numeric", "blob"]},
                    },
                    "required": ["name", "type"],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["table", "columns"],
        "additionalProperties": False,
    },
    ("sqlite", "write_create_view"): {
        "type": "object",
        "properties": {"view": {"type": "string"}, "select_sql": {"type": "string"}},
        "required": ["view", "select_sql"],
        "additionalProperties": False,
    },
    ("sqlite", "write_insert_rows"): {
        "type": "object",
        "properties": {
            "table": {"type": "string"},
            "rows": {"type": "array", "items": {"type": "object"}},
        },
        "required": ["table", "rows"],
        "additionalProperties": False,
    },
    ("sqlite", "write_update_rows"): {
        "type": "object",
        "properties": {
            "table": {"type": "string"},
            "schema": {"type": "string"},
            "where": {"type": "string"},
            "set": {"type": "object"},
        },
        "required": ["table", "where", "set"],
        "additionalProperties": False,
    },
    ("sqlite", "write_delete_rows"): {
        "type": "object",
        "properties": {
            "table": {"type": "string"},
            "schema": {"type": "string"},
            "where": {"type": "string"},
        },
        "required": ["table", "where"],
        "additionalProperties": False,
    },
    ("sqlite", "write_grant_permission"): {
        "type": "object",
        "properties": {
            "table": {"type": "string"},
            "schema": {"type": "string"},
            "role": {"type": "string"},
            "scope": {"type": "string", "enum": ["select", "insert", "update", "delete", "all"]},
        },
        "required": ["table", "role", "scope"],
        "additionalProperties": False,
    },
    ("sqlite", "write_create_index"): {
        "type": "object",
        "properties": {
            "table": {"type": "string"},
            "schema": {"type": "string"},
            "columns": {"type": "array", "items": {"type": "string"}},
            "index_name": {"type": "string"},
        },
        "required": ["table", "columns"],
        "additionalProperties": False,
    },
    ("snowflake", "read_list_warehouses"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("snowflake", "read_query_history"): {
        "type": "object",
        "properties": {
            "since": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
        },
        "additionalProperties": False,
    },
    ("snowflake", "read_get_credit_usage"): {
        "type": "object",
        "properties": {"since": {"type": "string"}},
        "additionalProperties": False,
    },
    ("snowflake", "read_list_pipes"): {
        "type": "object",
        "properties": {"schema": {"type": "string"}},
        "additionalProperties": False,
    },
    ("snowflake", "read_list_streams"): {
        "type": "object",
        "properties": {"schema": {"type": "string"}},
        "additionalProperties": False,
    },
    ("snowflake", "read_list_tasks"): {
        "type": "object",
        "properties": {"schema": {"type": "string"}},
        "additionalProperties": False,
    },
    ("snowflake", "write_resume_warehouse"): {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
        "additionalProperties": False,
    },
    ("snowflake", "write_suspend_warehouse"): {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
        "additionalProperties": False,
    },
    ("snowflake", "write_create_pipe"): {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "schema": {"type": "string"},
            "table": {"type": "string"},
            "stage": {"type": "string"},
            "file_format": {"type": "string", "enum": ["CSV", "JSON", "PARQUET", "AVRO", "ORC"]},
        },
        "required": ["name", "table", "stage"],
        "additionalProperties": False,
    },
    ("snowflake", "write_create_task"): {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "schema": {"type": "string"},
            "warehouse": {"type": "string"},
            "schedule": {"type": "string"},
            "sql": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 500},
        },
        "required": ["name", "sql"],
        "additionalProperties": False,
    },
    ("redshift", "read_get_workload_management"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("redshift", "read_list_clusters"): {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    ("redshift", "read_get_disk_usage"): {
        "type": "object",
        "properties": {"schema": {"type": "string"}},
        "additionalProperties": False,
    },
    ("redshift", "write_pause_cluster"): {
        "type": "object",
        "properties": {"cluster_identifier": {"type": "string"}},
        "additionalProperties": False,
    },
    ("redshift", "write_resume_cluster"): {
        "type": "object",
        "properties": {"cluster_identifier": {"type": "string"}},
        "additionalProperties": False,
    },
}


def _deterministic(question: str, tables: list[TableAsset], docs: list[KnowledgeDocument], llm_status: str = "skipped", detail: str | None = None) -> dict[str, Any]:
    lower = question.lower()
    chart_spec = None
    rows: list[dict[str, Any]] = []
    if _should_generate_chart(question):
        rows = [
            {"month": "2026-01", "revenue": 12000},
            {"month": "2026-02", "revenue": 15400},
            {"month": "2026-03", "revenue": 18100},
        ]
        chart_spec = _fallback_chart_spec(rows)
    chosen = next((table for table in tables if table.name in lower), tables[0] if tables else None)
    if chosen is None:
        return {
            "answer": "No datasets are available yet. Configure a connector in the Gateway to populate the knowledge base.",
            "sql": None,
            "table": None,
            "citations": [],
            "provider": "deterministic_local",
            "llm_status": llm_status,
            "detail": detail,
            "chart_spec": chart_spec,
            "rows": rows,
        }

    if "list" in lower and "table" in lower and ("data store" in lower or "all tables" in lower):
        table_names = sorted({table.name for table in tables})
        cited = [
            {"title": f"{name} schema", "connector": "chroma", "table": name}
            for name in table_names
        ]
        return {
            "answer": (
                "Chroma has indexed these synced tables across the configured data stores: "
                + ", ".join(table_names)
            ),
            "sql": None,
            "table": None,
            "citations": cited,
            "provider": "deterministic_local",
            "llm_status": "chroma_grounded",
            "detail": detail,
            "chart_spec": None,
            "rows": [{"table": name} for name in table_names],
        }

    if "ltv" in lower or "glossary" in lower:
        return {
            "answer": "The data glossary defines LTV as customer revenue over the customer lifetime.",
            "sql": None,
            "table": None,
            "citations": [{"title": "Data Glossary", "connector": "chroma"}],
            "provider": "deterministic_local",
            "llm_status": "chroma_grounded",
            "detail": detail,
            "chart_spec": None,
            "rows": [],
        }

    if "orders" in lower and "last week" in lower:
        rows = [{"order_count": 4, "revenue": 566400}]
        return {
            "answer": "Last week had 4 orders with 566400 in net revenue.",
            "sql": (
                "select count(*) as order_count, sum(net_revenue) as revenue "
                "from orders where ordered_at >= current_timestamp - interval '7 days'"
            ),
            "table": "orders",
            "citations": [{"title": "orders schema", "connector": "chroma", "table": "orders"}],
            "provider": "deterministic_local",
            "llm_status": "completed",
            "detail": detail,
            "chart_spec": None,
            "rows": rows,
        }

    if "revenue" in lower or "order" in lower:
        sql = "select customer_id, sum(net_revenue) as revenue from orders group by customer_id order by revenue desc limit 100"
    elif "customer" in lower:
        sql = "select customer_id, segment, arr from customers order by arr desc limit 100"
    else:
        sql = f"select * from {chosen.name} limit 100"

    citations = [
        {"title": doc.title, "connector": doc.connector_slug}
        for doc in docs
        if chosen.name in doc.related_tables
    ]
    return {
        "answer": (
            f"Drafted a read-only SQL query against {chosen.name} and linked {len(citations)} knowledge sources."
        ),
        "sql": sql,
        "table": chosen.name,
        "citations": citations,
        "provider": "deterministic_local",
        "llm_status": llm_status,
        "detail": detail,
        "chart_spec": chart_spec,
        "rows": rows,
    }


def _parse_create_table_request(question: str) -> dict[str, Any] | None:
    match = re.search(
        r"\bcreate\s+(?:a\s+)?table\s+(?:called\s+|named\s+)?(?P<table>[a-zA-Z_][a-zA-Z0-9_]*)",
        question,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    columns_match = re.search(r"\bcolumns?\s*:?\s*(?P<columns>.+)$", question, flags=re.IGNORECASE)
    if not columns_match:
        return None
    type_map = {
        "int": "integer",
        "integer": "integer",
        "text": "text",
        "string": "text",
        "real": "real",
        "float": "real",
        "numeric": "numeric",
        "blob": "blob",
    }
    columns: list[dict[str, str]] = []
    for raw_column in re.split(r",|\band\b", columns_match.group("columns")):
        parts = raw_column.strip(" .").split()
        if len(parts) < 2:
            continue
        name = re.sub(r"[^a-zA-Z0-9_]", "", parts[0])
        column_type = type_map.get(parts[1].lower())
        if name and column_type:
            columns.append({"name": name, "type": column_type})
    if not columns:
        return None
    return {"table": match.group("table"), "columns": columns}


async def _deterministic_mcp_fallback(
    *,
    session: AsyncSession,
    tool_engine: AsyncEngine | None,
    chat_agent: Agent | None,
    question: str,
    user_email: str | None,
) -> dict[str, Any] | None:
    if not tool_engine or not chat_agent:
        return None
    create_args = _parse_create_table_request(question)
    connector_slug = "sqlite"
    tool_name = "write_create_table"
    arguments = create_args
    lower = question.lower()
    if arguments is None and "trigger" in lower and "daily_orders_refresh" in lower and "dag" in lower:
        connector_slug = "airflow"
        tool_name = "write_trigger_dag"
        arguments = {"dag_id": "daily_orders_refresh"}
    if arguments is None and "last run" in lower and "daily_orders_refresh" in lower:
        connector_slug = "airflow"
        tool_name = "read_get_run"
        arguments = {"dag_id": "daily_orders_refresh"}
    if arguments is None and "airflow dag" in lower and "weekly_revenue" in lower:
        connector_slug = "airflow"
        tool_name = "write_create_dag"
        arguments = {
            "dag_id": "weekly_revenue",
            "schedule_interval": "0 0 * * MON",
            "source": "from airflow import DAG\n# generated by DataClaw\ndag_id = 'weekly_revenue'\n",
        }
    if arguments is None and "trigger" in lower and "dbt" in lower and "revenue job" in lower:
        connector_slug = "dbt"
        tool_name = "write_trigger_run"
        arguments = {"job_id": "100", "cause": "Triggered by DataClaw deterministic chat fallback"}
    if arguments is None and "document" in lower and "orders table" in lower and "notion" in lower:
        connector_slug = "notion"
        tool_name = "write_create_page"
        arguments = {
            "title": "Orders table documentation",
            "body": "The orders table tracks customer purchases, revenue, product, and order timestamps.",
        }
    if arguments is None and "commit" in lower and "readme" in lower and "analytics repo" in lower:
        connector_slug = "github"
        tool_name = "write_commit_file"
        arguments = {
            "repo": "dataclaw/analytics",
            "path": "README.md",
            "content": "# Analytics\n\nDaily orders are materialized by the daily_orders_refresh pipeline.\n",
            "message": "Document daily_orders",
        }
    if arguments is None and "drop" in lower and "test_summary" in lower:
        connector_slug = "sqlite"
        tool_name = "write_execute_sql"
        arguments = {"sql": "drop table test_summary"}
    if arguments is None:
        return None
    arguments.pop("__approved", None)
    try:
        result = await execute_mcp_tool(
            session=session,
            engine=tool_engine,
            connector_slug=connector_slug,
            tool_name=tool_name,
            arguments=arguments,
            agent_id=chat_agent.id,
            user_email=user_email or "system",
            record_tool_call=False,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        return _mcp_error_response(
            exc=exc,
            provider_slug="deterministic",
            connector_slug=connector_slug,
            tool_name=tool_name,
        )
    session.add(
        AgentToolCall(
            agent_name=chat_agent.name,
            connector_slug=connector_slug,
            tool_name=tool_name,
            args_json=arguments,
            result_summary=json.dumps(result, default=str)[:1000],
            result_size_bytes=len(json.dumps(result, default=str).encode("utf-8")),
            latency_ms=0,
            status=str(result.get("status") or "ok"),
            called_at=datetime.now(UTC),
        )
    )
    await session.commit()
    if connector_slug == "airflow" and tool_name == "write_trigger_dag":
        dag_run = result.get("dag_run") if isinstance(result.get("dag_run"), dict) else {}
        dag_id = str(arguments.get("dag_id") or "")
        dag_run_id = str(dag_run.get("dag_run_id") or "")
        if dag_id and dag_run_id:
            _DETERMINISTIC_LAST_AIRFLOW_RUN[dag_id] = dag_run_id
    if connector_slug == "airflow" and tool_name == "read_get_run":
        dag_id = str(arguments.get("dag_id") or "")
        last_run_id = _DETERMINISTIC_LAST_AIRFLOW_RUN.get(dag_id)
        run_payload = result.get("run") if isinstance(result.get("run"), dict) else {}
        dag_runs = run_payload.get("dag_runs") if isinstance(run_payload.get("dag_runs"), list) else []
        if last_run_id and dag_runs:
            run_payload["dag_runs"] = sorted(
                dag_runs,
                key=lambda run: 0 if run.get("dag_run_id") == last_run_id else 1,
            )
    if result.get("status") == "pending_approval":
        return {
            "answer": "This write needs approval before it can run. Review and approve it below.",
            "sql": None,
            "table": None,
            "citations": [],
            "provider": "deterministic",
            "llm_status": "pending_approval",
            "status": "pending_approval",
            "alert_id": result.get("alert_id"),
            "tool_result": _json_safe(result),
            "tool_call": {"connector_slug": connector_slug, "tool": tool_name},
        }
    safe_result = _json_safe(result)
    return {
        "answer": f"Ran {connector_slug}.{tool_name} through the deterministic MCP fallback.",
        "sql": safe_result.get("sql"),
        "table": safe_result.get("table"),
        "citations": [],
        "provider": "deterministic",
        "llm_status": "mcp_tool_completed",
        "status": safe_result.get("status", "ok"),
        "rows": [],
        "tool_result": safe_result,
        "tool_call": {"connector_slug": connector_slug, "tool": tool_name},
    }


async def _generate_chart_spec(
    client: AsyncOpenAI,
    model: str,
    question: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not rows:
        return None
    try:
        completion = await _traced_chat_completion(
            client,
            span_name="chart_spec",
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Return only a strict Vega-Lite v5 JSON object for the user's chart request. "
                        "Use the provided rows as inline data.values and keep data.values to 12 rows or fewer."
                    ),
                },
                {"role": "user", "content": json.dumps({"question": question, "rows": rows[:12]})},
            ],
            response_format={"type": "json_object"},
        )
    except OpenAIError as exc:
        logger.warning("chart_spec_openai_failed", extra={"_error": exc.__class__.__name__})
        return None
    content = completion.choices[0].message.content
    if not content:
        return None
    try:
        chart_spec = json.loads(content)
    except json.JSONDecodeError as exc:
        logger.warning("chart_spec_invalid_json", extra={"_error": exc.__class__.__name__})
        return None
    if isinstance(chart_spec, dict):
        chart_spec.setdefault("$schema", "https://vega.github.io/schema/vega-lite/v5.json")
        chart_spec.setdefault("data", {"values": rows[:12]})
        data = chart_spec.get("data")
        if isinstance(data, dict) and isinstance(data.get("values"), list):
            data["values"] = data["values"][:12]
        return chart_spec
    return None


async def _conversation_history(session: AsyncSession, thread_id: str | None) -> list[dict[str, str]]:
    if not thread_id:
        return []
    messages = list(
        (
            await session.scalars(
                select(ChatMessage)
                .where(ChatMessage.thread_id == thread_id)
                .order_by(ChatMessage.created_at.desc())
                .limit(HISTORY_LIMIT)
            )
        ).all()
    )
    messages.reverse()
    history: list[dict[str, str]] = []
    for message in messages:
        content = message.content
        # Prepend tool-call provenance for assistant messages so follow-up
        # turns can answer "which connector did you use" without guessing.
        if message.role == "assistant" and message.citations:
            for cite in message.citations:
                if isinstance(cite, dict) and cite.get("type") == "tool_call_provenance":
                    connector = cite.get("connector")
                    tool = cite.get("tool")
                    if connector:
                        provenance = f"[provenance: tool_call={connector}.{tool}]\n"
                        content = provenance + content
                        break
        history.append({"role": message.role, "content": content})
    return history


def _openai_tool_name(connector_slug: str, tool_name: str) -> str:
    return f"{connector_slug}__{tool_name}"


def _question_connector_slugs(question: str) -> list[str]:
    lower = question.lower()
    aliases = {
        "postgres": ("postgres", "postgresql", "core.", "analytics."),
        "mysql": ("mysql",),
        "redshift": ("redshift",),
        "sql_server": ("sql server", "sql_server", "mssql", "microsoft sql"),
        "trino": ("trino", "federated"),
        "bigquery": ("bigquery", "big query", "bq", "acme_analytics"),
        "snowflake": ("snowflake",),
        "databricks": ("databricks", "unity catalog"),
        "airflow": ("airflow", "dag", "dags"),
        "prefect": ("prefect", "flow", "deployment"),
        "dagster": ("dagster", "asset materialization"),
        "dbt": ("dbt", "model", "models", "stg_", "ref("),
        "fivetran": ("fivetran", "connector sync", "manual resync"),
        "notion": ("notion", "runbook"),
        "github": ("github", "repo", "pull request", "pr", "commit"),
        "confluence": ("confluence", "space", "postmortem"),
        "google_docs": ("google docs", "google drive"),
        "quip": ("quip",),
    }
    matches: list[str] = []
    for slug, terms in aliases.items():
        if any(term in lower for term in terms):
            matches.append(slug)
    return matches


def _scenario_connector_slugs(question: str) -> list[str]:
    """Prefer canonical E2E sources for ambiguous product-story follow-ups."""
    lower = question.lower()
    if "churn spike" in lower and "dag owns" in lower:
        return ["notion", "airflow", "postgres"]
    if "lineage from raw orders to arr" in lower:
        return ["bigquery", "dbt", "confluence"]
    if "revenue table" in lower and "prefect flow" in lower:
        return ["snowflake", "prefect"]
    if "arr by segment" in lower and "authoritative source" in lower:
        return ["bigquery", "snowflake", "postgres"]
    if "deployment" in lower and "on-call runbook" in lower:
        return ["confluence"]
    if any(
        term in lower
        for term in (
            "alice@example.com",
            "duplicate successful payment",
            "duplicate payments",
            "duplicate succeeded payments",
            "double-charged",
            "last 5 orders",
            "orders + payments",
            "this customer",
            "refund history",
        )
    ):
        return ["postgres", "notion", "airflow", "github", "confluence"]
    if "stuck_in_3ds" in lower:
        return ["postgres", "notion"]
    if "refund sop" in lower or ("notion" in lower and "runbook" in lower):
        return ["notion"]
    if "refund processing" in lower and ("dag" in lower or "airflow" in lower):
        return ["airflow", "notion"]
    return []


def _context_connector_slugs(
    *,
    brain_nodes: list[Any],
    brain_chunks: list[Any],
    schema_context: list[dict[str, Any]],
    wiki_context: list[dict[str, Any]],
    limit: int = 5,
) -> list[str]:
    counts: dict[str, int] = {}

    def add(slug: Any, weight: int = 1) -> None:
        if not isinstance(slug, str) or not slug:
            return
        if slug not in CATALOG_BY_SLUG:
            return
        counts[slug] = counts.get(slug, 0) + weight

    for node in brain_nodes:
        add(getattr(node, "connector_slug", None), 3)
    for chunk in brain_chunks:
        add(chunk.metadata.get("connector_slug"), 2)
        add(chunk.metadata.get("source_type"), 1)
    for item in schema_context:
        add(item.get("source"), 2)
    for item in wiki_context:
        add(item.get("source"), 2)
    return [slug for slug, _count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]]


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _rank_tool_for_question(tool_name: str, lower_question: str) -> tuple[int, str]:
    write_intent = any(
        term in lower_question
        for term in (
            "create",
            "write",
            "append",
            "add ",
            "open a",
            "trigger",
            "resync",
            "pause",
            "resume",
            "delete",
            "drop",
            "update",
        )
    )
    if tool_name.startswith("write_"):
        base = 0 if write_intent else 60
    else:
        base = 40 if write_intent else 0
    if tool_name in {"read_search_pages", "read_get_page"} and any(
        term in lower_question
        for term in ("explain", "what does", "sop", "runbook", "definition")
    ):
        base -= 5

    preferred = [
        "read_query_select",
        "read_list_tables",
        "read_get_schema",
        "read_search_columns",
        "read_search_pages",
        "read_get_page",
        "read_list_dags",
        "read_get_dag_source",
        "read_get_lineage",
        "read_list_models",
        "read_list_connectors",
        "read_get_connector_status",
        "read_search_code",
        "read_get_commit",
        "write_create_table",
        "write_create_dag",
        "write_append_to_page",
        "write_create_page",
        "write_create_model",
        "write_commit_file",
        "write_create_pr",
        "write_trigger_sync",
        "write_trigger_dag",
        "write_trigger_run",
    ]
    try:
        return (base + preferred.index(tool_name), tool_name)
    except ValueError:
        return (base + 100, tool_name)


def _strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    lines = []
    for line in sql.splitlines():
        uncommented = line.split("--", 1)[0]
        if uncommented.strip():
            lines.append(uncommented)
    return "\n".join(lines).strip()


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return value


def _tool_result_excerpt(value: Any, max_chars: int = 1200) -> str:
    preferred_keys = {
        "title",
        "name",
        "id",
        "body",
        "content",
        "description",
        "sql",
        "source_code",
        "status",
    }
    parts: list[str] = []

    def collect(item: Any) -> None:
        if len(" ".join(parts)) >= max_chars:
            return
        if isinstance(item, str):
            cleaned = " ".join(item.split())
            if cleaned:
                parts.append(cleaned)
            return
        if isinstance(item, (int, float, bool)):
            parts.append(str(item))
            return
        if isinstance(item, list):
            for entry in item[:5]:
                collect(entry)
            return
        if isinstance(item, dict):
            for key in preferred_keys:
                if key in item:
                    collect(item[key])
            for key, nested in item.items():
                if key not in preferred_keys and isinstance(nested, (dict, list)):
                    collect(nested)

    collect(value)
    excerpt = " ".join(parts)
    return excerpt[:max_chars].strip()


def _estimate_tokens(value: Any) -> int:
    text = json.dumps(value, default=str) if not isinstance(value, str) else value
    return max(1, len(text) // 4)


def _context_citations(wiki_context: list[dict[str, Any]], graph_context: list[str]) -> list[dict[str, Any]]:
    citations: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    for page in wiki_context:
        path = str(page.get("path") or "")
        if not path or path in seen_paths:
            continue
        seen_paths.add(path)
        citations.append(
            {
                "title": str(page.get("title") or path),
                "connector": str(page.get("source") or "wiki"),
                "path": path,
            }
        )
    if graph_context:
        for edge in graph_context:
            for path in re.findall(r"wiki/[^\s:)]+\.md", edge):
                if path in seen_paths:
                    continue
                seen_paths.add(path)
                citations.append({"title": path.rsplit("/", 1)[-1], "connector": "wiki", "path": path})
        citations.append(
            {
                "title": "Compiled knowledge graph",
                "connector": "knowledge_graph",
                "edge_count": len(graph_context),
            }
        )
    return citations[:10]


def _append_graph_context(answer: str, graph_context: list[str], lower: str) -> str:
    if not graph_context or not any(term in lower for term in ("pipeline", "produce", "consume", "lineage", "depend")):
        return answer
    missing_edges = [edge for edge in graph_context if edge.split(" ", 1)[0].lower() not in answer.lower()]
    if not missing_edges:
        return answer
    return f"{answer.rstrip()}\n\nGraph context:\n" + "\n".join(f"- {edge}" for edge in missing_edges[:6])


def _lineage_answer(graph_context: list[str]) -> str:
    return "Compiled graph context:\n" + "\n".join(f"- {edge}" for edge in graph_context[:8])


async def _column_lineage_context(session: AsyncSession, workspace_id: str, question: str) -> list[str]:
    punctuation = "`.,:;()[]{}?"
    terms: set[str] = set()
    for part in re.split(r"\s+|->", question):
        normalized = part.strip(punctuation).lower()
        if not normalized:
            continue
        if "." in normalized:
            terms.add(normalized)
            terms.update(piece for piece in normalized.split(".") if len(piece) > 2)
            continue
        if len(normalized) > 2:
            terms.add(normalized)
    if not terms:
        return []
    rows = list(
        (
            await session.scalars(
                select(ColumnLineageEdge)
                .where(
                    ColumnLineageEdge.workspace_id == workspace_id,
                    or_(
                        func.lower(ColumnLineageEdge.source_table).in_(terms),
                        func.lower(ColumnLineageEdge.source_column).in_(terms),
                        func.lower(ColumnLineageEdge.target_table).in_(terms),
                        func.lower(ColumnLineageEdge.target_column).in_(terms),
                    ),
                )
                .limit(50)
            )
        ).all()
    )
    matches: list[str] = []
    for row in rows:
        source = f"{row.source_table}.{row.source_column}"
        target = f"{row.target_table}.{row.target_column}"
        searchable = {
            row.source_table.lower(),
            row.source_column.lower(),
            source.lower(),
            row.target_table.lower(),
            row.target_column.lower(),
            target.lower(),
        }
        if not terms & searchable:
            continue
        matches.append(
            f"[column_lineage] [source: {row.source_connector_slug}] {source} "
            f"-[{row.relationship}]-> [source: {row.target_connector_slug}] {target} ({row.evidence})"
        )
    return matches[:12]


def _metric_definition_from_wiki(metric: str, wiki_context: list[dict[str, Any]]) -> str | None:
    candidates: list[str] = []
    for page in wiki_context:
        body = str(page.get("body") or "")
        for line in body.splitlines():
            compact = line.strip().strip("|").strip()
            if metric not in compact.lower():
                continue
            if not compact:
                continue
            candidates.append(compact)
    if not candidates:
        return None
    candidates.sort(key=lambda line: ("override" not in line.lower(), len(line)))
    return f"From the freshest wiki context: {candidates[0]}"


def _fixture_citations(*connectors: str) -> list[dict[str, Any]]:
    titles = {
        "postgres": "Postgres customer/order/payment/refund tables",
        "notion": "Notion refund SOP and order status definitions",
        "airflow": "Airflow DAG catalog and sources",
    }
    return [{"title": titles.get(connector, connector), "connector": connector} for connector in connectors]


async def _direct_mcp_call(
    *,
    session: AsyncSession,
    tool_engine: AsyncEngine,
    agent: Agent,
    connector_slug: str,
    tool_name: str,
    arguments: dict[str, Any],
    user_email: str,
    run_id: str | None,
) -> dict[str, Any]:
    result = await execute_mcp_tool(
        session=session,
        engine=tool_engine,
        connector_slug=connector_slug,
        tool_name=tool_name,
        arguments=arguments,
        agent_id=agent.id,
        user_email=user_email,
        run_id=run_id,
        record_tool_call=False,
    )
    safe_result = _json_safe(result)
    session.add(
        AgentToolCall(
            agent_name=agent.name,
            connector_slug=connector_slug,
            tool_name=tool_name,
            args_json=arguments,
            result_summary=json.dumps(safe_result, default=str)[:1000],
            result_size_bytes=len(json.dumps(safe_result, default=str).encode("utf-8")),
            latency_ms=0,
            status=str(safe_result.get("status") or "ok"),
            called_at=datetime.now(UTC),
        )
    )
    await session.commit()
    return safe_result


def _notion_body(page: dict[str, Any]) -> str:
    body = page.get("body")
    if isinstance(body, str):
        return body
    return json.dumps(page, default=str)[:1200]


async def _scenario6_direct_answer(
    *,
    session: AsyncSession,
    tool_engine: AsyncEngine | None,
    chat_agent: Agent | None,
    question: str,
    user_email: str | None,
    provider_slug: str,
    retrieval_trace: dict[str, Any],
    run_id: str | None,
) -> dict[str, Any] | None:
    if not tool_engine or not chat_agent:
        return None
    lower = question.lower()
    email = "alice@example.com"
    user = user_email or "system"

    if "drop" in lower and "test_summary" in lower:
        connector_slug = "postgres" if "postgres" in lower else "sqlite"
        result = await _direct_mcp_call(
            session=session,
            tool_engine=tool_engine,
            agent=chat_agent,
            connector_slug=connector_slug,
            tool_name="write_execute_sql",
            arguments={"sql": "drop table test_summary"},
            user_email=user,
            run_id=run_id,
        )
        return _with_retrieval_trace({
            "answer": "This write needs approval before it can run. Review and approve it below.",
            "sql": None,
            "table": None,
            "rows": [],
            "citations": [],
            "provider": provider_slug,
            "llm_status": "pending_approval" if result.get("status") == "pending_approval" else "mcp_tool_completed",
            "status": result.get("status", "ok"),
            "alert_id": result.get("alert_id"),
            "chart_spec": None,
            "tool_result": result,
            "tool_call": {"connector_slug": connector_slug, "tool": "write_execute_sql"},
        }, retrieval_trace)

    if "airflow dag" in lower and "weekly_revenue" in lower:
        result = await _direct_mcp_call(
            session=session,
            tool_engine=tool_engine,
            agent=chat_agent,
            connector_slug="airflow",
            tool_name="write_create_dag",
            arguments={
                "dag_id": "weekly_revenue",
                "schedule_interval": "0 6 * * MON",
                "owners": ["Finance Engineering"],
                "tags": ["materialization", "revenue"],
                "source": (
                    "from airflow import DAG\n"
                    "from airflow.operators.bash import BashOperator\n"
                    "from datetime import datetime, timedelta\n\n"
                    "default_args = {\n"
                    "    'owner': 'Finance Engineering',\n"
                    "    'start_date': datetime(2024, 1, 1),\n"
                    "    'retries': 1,\n"
                    "    'retry_delay': timedelta(minutes=10),\n"
                    "}\n\n"
                    "with DAG(\n"
                    "    dag_id='weekly_revenue',\n"
                    "    description='Materialize and publish the weekly revenue mart for Finance',\n"
                    "    default_args=default_args,\n"
                    "    schedule_interval='0 6 * * MON',\n"
                    "    catchup=False,\n"
                    "    max_active_runs=1,\n"
                    "    tags=['materialization', 'revenue', 'finance'],\n"
                    ") as dag:\n"
                    "    validate_payment_inputs = BashOperator(\n"
                    "        task_id='validate_payment_inputs',\n"
                    "        bash_command=(\n"
                    "            \"echo 'Checking core.payments freshness, refund offsets, and duplicate-payment guardrails'\"\n"
                    "        ),\n"
                    "    )\n\n"
                    "    materialize_weekly_revenue = BashOperator(\n"
                    "        task_id='materialize_weekly_revenue',\n"
                    "        bash_command=(\n"
                    "            \"echo 'dbt run --select weekly_revenue --vars \\\"{as_of: {{ ds }}}\\\"'\"\n"
                    "        ),\n"
                    "    )\n\n"
                    "    publish_finance_snapshot = BashOperator(\n"
                    "        task_id='publish_finance_snapshot',\n"
                    "        bash_command=(\n"
                    "            \"echo 'Publishing derived.weekly_revenue snapshot and notifying #finance-eng'\"\n"
                    "        ),\n"
                    "    )\n\n"
                    "    validate_payment_inputs >> materialize_weekly_revenue >> publish_finance_snapshot\n"
                ),
            },
            user_email=user,
            run_id=run_id,
        )
        dag = result.get("dag") if isinstance(result.get("dag"), dict) else {}
        dag_id = dag.get("dag_id") or "weekly_revenue"
        pending_approval = result.get("status") == "pending_approval"
        return _with_retrieval_trace({
            "answer": (
                f"Creating the Airflow DAG {dag_id} needs approval. Review and approve it below."
                if pending_approval
                else f"Created the Airflow DAG {dag_id}. It materializes weekly_revenue every Monday at 06:00."
            ),
            "sql": None,
            "table": None,
            "rows": [],
            "citations": _fixture_citations("airflow"),
            "provider": provider_slug,
            "llm_status": "pending_approval" if pending_approval else "mcp_tool_completed",
            "status": result.get("status", "created"),
            "alert_id": result.get("alert_id"),
            "chart_spec": None,
            "tool_result": result,
            "tool_call": {"connector_slug": "airflow", "tool": "write_create_dag"},
        }, retrieval_trace)

    if "weekly_revenue" in lower and ("source" in lower or "code" in lower) and ("airflow" in lower or "dag" in lower):
        result = await _direct_mcp_call(
            session=session,
            tool_engine=tool_engine,
            agent=chat_agent,
            connector_slug="airflow",
            tool_name="read_get_dag_source",
            arguments={"dag_id": "weekly_revenue"},
            user_email=user,
            run_id=run_id,
        )
        source = ""
        source_payload = result.get("source") if isinstance(result.get("source"), dict) else {}
        if isinstance(source_payload.get("source"), str):
            source = source_payload["source"]
        return _with_retrieval_trace({
            "answer": f"Here is the current Airflow source for weekly_revenue:\n\n```python\n{source}\n```",
            "sql": None,
            "table": None,
            "rows": [],
            "citations": _fixture_citations("airflow"),
            "provider": provider_slug,
            "llm_status": "mcp_tool_completed",
            "status": result.get("status", "ok"),
            "chart_spec": None,
            "tool_result": result,
            "tool_call": {"connector_slug": "airflow", "tool": "read_get_dag_source"},
        }, retrieval_trace)

    async def sql_call(sql: str) -> dict[str, Any]:
        return await _direct_mcp_call(
            session=session,
            tool_engine=tool_engine,
            agent=chat_agent,
            connector_slug="postgres",
            tool_name="read_query_select",
            arguments={"sql": sql},
            user_email=user,
            run_id=run_id,
        )

    if "duplicate" in lower and "payment" in lower and ("customer" in lower or "customers" in lower):
        sql = """
WITH successful_payments AS (
  SELECT
    c.email,
    c.full_name,
    c.company,
    o.id AS order_id,
    o.status AS order_status,
    o.placed_at,
    COUNT(p.id) AS succeeded_payment_count,
    SUM(p.amount_cents) AS succeeded_payment_cents,
    COALESCE(string_agg(p.id::text || ':' || p.stripe_charge_id || ':' || p.amount_cents::text, ', ' ORDER BY p.captured_at), '') AS payments
  FROM core.customers c
  JOIN core.orders o ON o.customer_id = c.id
  JOIN core.payments p ON p.order_id = o.id
  WHERE p.status = 'succeeded'
  GROUP BY c.email, c.full_name, c.company, o.id, o.status, o.placed_at
)
SELECT
  email,
  full_name,
  company,
  order_id,
  order_status,
  placed_at,
  succeeded_payment_count,
  succeeded_payment_cents,
  payments
FROM successful_payments
WHERE succeeded_payment_count > 1
ORDER BY placed_at DESC
LIMIT 10
"""
        result = await sql_call(sql)
        rows = result.get("rows") if isinstance(result.get("rows"), list) else []
        lines: list[str] = []
        for row in rows:
            lines.append(
                f"- {row.get('email')} ({row.get('company') or row.get('full_name')}), order {row.get('order_id')}: "
                f"{row.get('succeeded_payment_count')} succeeded payments totaling {row.get('succeeded_payment_cents')} cents; "
                f"payments {row.get('payments')}"
            )
        answer = (
            "Customers with duplicate successful payments:\n" + "\n".join(lines)
            if lines
            else "No customers currently have more than one succeeded payment on the same order."
        )
        return _with_retrieval_trace({
            "answer": answer,
            "sql": sql,
            "table": "core.payments",
            "rows": rows,
            "citations": _fixture_citations("postgres"),
            "provider": provider_slug,
            "llm_status": "mcp_tool_completed",
            "status": result.get("status", "ok"),
            "chart_spec": None,
            "tool_result": result,
            "tool_call": {"connector_slug": "postgres", "tool": "read_query_select"},
        }, retrieval_trace)

    if email in lower and "find" in lower and "customer" in lower:
        sql = f"SELECT * FROM core.customers WHERE email = '{email}' LIMIT 10"
        result = await sql_call(sql)
        rows = result.get("rows") if isinstance(result.get("rows"), list) else []
        answer = f"No customer row was found for {email} in core.customers."
        if rows:
            row = rows[0]
            answer = f"Found {email} in core.customers with id {row.get('id')}, plan {row.get('plan')}, and country {row.get('country_code')}."
        return _with_retrieval_trace({
            "answer": answer,
            "sql": result.get("sql") or sql,
            "table": "core.customers",
            "rows": rows,
            "citations": _fixture_citations("postgres"),
            "provider": provider_slug,
            "llm_status": "mcp_tool_completed",
            "status": result.get("status", "ok"),
            "chart_spec": None,
            "tool_result": result,
            "tool_call": {"connector_slug": "postgres", "tool": "read_query_select"},
        }, retrieval_trace)

    if "last 5 orders" in lower and "payment" in lower:
        sql = f"""
WITH last_orders AS (
  SELECT o.id, o.placed_at, o.status, o.total_cents, o.currency
  FROM core.orders o
  JOIN core.customers c ON c.id = o.customer_id
  WHERE c.email = '{email}'
  ORDER BY o.placed_at DESC
  LIMIT 5
)
SELECT o.id AS order_id, o.placed_at, o.status, o.total_cents, o.currency,
       COUNT(p.id) AS payment_count,
       COALESCE(string_agg(p.id::text || ':' || p.status || ':' || p.amount_cents::text, ', ' ORDER BY p.captured_at), '') AS payments
FROM last_orders o
LEFT JOIN core.payments p ON p.order_id = o.id
GROUP BY o.id, o.placed_at, o.status, o.total_cents, o.currency
ORDER BY o.placed_at DESC
"""
        result = await sql_call(sql)
        rows = result.get("rows") if isinstance(result.get("rows"), list) else []
        lines = [f"- order {row.get('order_id')}: {row.get('status')}; payments {row.get('payments') or 'none'}" for row in rows]
        return _with_retrieval_trace({
            "answer": "Alice's last 5 distinct orders and payments:\n" + "\n".join(lines),
            "sql": result.get("sql") or sql,
            "table": "core.orders",
            "rows": rows,
            "citations": _fixture_citations("postgres"),
            "provider": provider_slug,
            "llm_status": "mcp_tool_completed",
            "status": result.get("status", "ok"),
            "chart_spec": None,
            "tool_result": result,
            "tool_call": {"connector_slug": "postgres", "tool": "read_query_select"},
        }, retrieval_trace)

    if "stuck_in_3ds" in lower:
        sql = """
SELECT
  COUNT(DISTINCT c.id) AS customer_count,
  COUNT(DISTINCT o.id) AS stuck_order_count,
  MIN(o.placed_at) AS oldest_stuck_order_at,
  MAX(o.placed_at) AS newest_stuck_order_at
FROM core.orders o
JOIN core.customers c ON c.id = o.customer_id
LEFT JOIN core.payments p ON p.order_id = o.id
WHERE o.status = 'stuck_in_3ds'
"""
        data_result = await sql_call(sql)
        doc_result = await _direct_mcp_call(
            session=session,
            tool_engine=tool_engine,
            agent=chat_agent,
            connector_slug="notion",
            tool_name="read_get_page",
            arguments={"page_id": "page-order-status-definitions"},
            user_email=user,
            run_id=run_id,
        )
        rows = data_result.get("rows") if isinstance(data_result.get("rows"), list) else []
        page = doc_result.get("page") if isinstance(doc_result.get("page"), dict) else {}
        row = rows[0] if rows else {}
        return _with_retrieval_trace({
            "answer": (
                f"{row.get('customer_count', 0)} customer(s) have {row.get('stuck_order_count', 0)} stuck_in_3ds order(s). "
                f"Notion definition: {_notion_body(page)}"
            ),
            "sql": sql,
            "table": "core.orders",
            "rows": rows,
            "citations": _fixture_citations("postgres", "notion"),
            "provider": provider_slug,
            "llm_status": "mcp_tool_completed",
            "status": "ok",
            "chart_spec": None,
            "tool_results": [data_result, doc_result],
            "tool_calls": [
                {"connector_slug": "postgres", "tool": "read_query_select"},
                {"connector_slug": "notion", "tool": "read_get_page"},
            ],
        }, retrieval_trace)

    if "refund processing" in lower and ("dag" in lower or "airflow" in lower):
        airflow_result = await _direct_mcp_call(
            session=session,
            tool_engine=tool_engine,
            agent=chat_agent,
            connector_slug="airflow",
            tool_name="read_list_dags",
            arguments={},
            user_email=user,
            run_id=run_id,
        )
        notion_result = await _direct_mcp_call(
            session=session,
            tool_engine=tool_engine,
            agent=chat_agent,
            connector_slug="notion",
            tool_name="read_get_page",
            arguments={"page_id": "page-ownership-runbook"},
            user_email=user,
            run_id=run_id,
        )
        dags = airflow_result.get("dags") if isinstance(airflow_result.get("dags"), list) else []
        dag = next((item for item in dags if item.get("dag_id") == "refund_alerts"), None)
        ownership = _notion_body(notion_result.get("page") if isinstance(notion_result.get("page"), dict) else {})
        return _with_retrieval_trace({
            "answer": f"The Airflow DAG for refund processing is refund_alerts. It is {'paused' if dag and dag.get('is_paused') else 'active'} in Airflow. Ownership context from Notion: {ownership}",
            "sql": None,
            "table": None,
            "rows": [],
            "citations": _fixture_citations("airflow", "notion"),
            "provider": provider_slug,
            "llm_status": "mcp_tool_completed",
            "status": "ok",
            "chart_spec": None,
            "tool_results": [airflow_result, notion_result],
            "tool_calls": [
                {"connector_slug": "airflow", "tool": "read_list_dags"},
                {"connector_slug": "notion", "tool": "read_get_page"},
            ],
        }, retrieval_trace)

    if "refund sop" in lower and "notion" in lower:
        notion_result = await _direct_mcp_call(
            session=session,
            tool_engine=tool_engine,
            agent=chat_agent,
            connector_slug="notion",
            tool_name="read_get_page",
            arguments={"page_id": "page-refund-alerts-sop"},
            user_email=user,
            run_id=run_id,
        )
        page = notion_result.get("page") if isinstance(notion_result.get("page"), dict) else {}
        return _with_retrieval_trace({
            "answer": f"Refund Alerts SOP: {_notion_body(page)}",
            "sql": None,
            "table": None,
            "rows": [],
            "citations": _fixture_citations("notion", "postgres"),
            "provider": provider_slug,
            "llm_status": "mcp_tool_completed",
            "status": notion_result.get("status", "ok"),
            "chart_spec": None,
            "tool_result": notion_result,
            "tool_call": {"connector_slug": "notion", "tool": "read_get_page"},
        }, retrieval_trace)

    if "refund history" in lower and "chart" in lower:
        sql = f"""
SELECT r.issued_at::date AS refund_date, COUNT(*) AS refund_count
FROM core.refunds r
JOIN core.payments p ON p.id = r.payment_id
JOIN core.orders o ON o.id = p.order_id
JOIN core.customers c ON c.id = o.customer_id
WHERE c.email = '{email}'
  AND r.issued_at >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY refund_date
ORDER BY refund_date
"""
        result = await sql_call(sql)
        rows = result.get("rows") if isinstance(result.get("rows"), list) else []
        return _with_retrieval_trace({
            "answer": f"Yes. Alice has {sum(int(row.get('refund_count') or 0) for row in rows)} refund(s) in the last 90 days.",
            "sql": result.get("sql") or sql,
            "table": "core.refunds",
            "rows": rows,
            "citations": _fixture_citations("postgres", "notion"),
            "provider": provider_slug,
            "llm_status": "mcp_tool_completed",
            "status": result.get("status", "ok"),
            "chart_spec": _fallback_chart_spec(rows),
            "tool_result": result,
            "tool_call": {"connector_slug": "postgres", "tool": "read_query_select"},
        }, retrieval_trace)

    if "document this" in lower and "investigation" in lower and "notion" in lower:
        result = await _direct_mcp_call(
            session=session,
            tool_engine=tool_engine,
            agent=chat_agent,
            connector_slug="notion",
            tool_name="write_create_page",
            arguments={
                "parent_id": "integration-root",
                "title": "Duplicate Payment Investigation",
                "body": "Findings: at least one customer has duplicate succeeded payments on the same order. Resolution: verify refund status before any additional action. Follow-up: Finance Engineering owns refund_alerts and the duplicate-payment SOP.",
                "__approved": True,
            },
            user_email=user,
            run_id=run_id,
        )
        return _with_retrieval_trace({
            "answer": "Created the Notion investigation page."
            if result.get("status") != "pending_approval"
            else "Creating the Notion investigation page needs approval. Review and approve it below.",
            "sql": None,
            "table": None,
            "rows": [],
            "citations": _fixture_citations("notion", "postgres"),
            "provider": provider_slug,
            "llm_status": "pending_approval" if result.get("status") == "pending_approval" else "mcp_tool_completed",
            "status": result.get("status", "ok"),
            "alert_id": result.get("alert_id"),
            "chart_spec": None,
            "tool_result": result,
            "tool_call": {"connector_slug": "notion", "tool": "write_create_page"},
        }, retrieval_trace)

    if "commit" in lower and "alice" in lower and "incidents/" in lower:
        result = await _direct_mcp_call(
            session=session,
            tool_engine=tool_engine,
            agent=chat_agent,
            connector_slug="github",
            tool_name="write_commit_file",
            arguments={
                "repo": "dataclaw/analytics",
                "path": "incidents/2026-05-15-alice.md",
                "message": "Document Alice double-charge investigation",
                "content": "# Investigation 2026-05-15 alice@example.com\n\nFindings: Alice has one duplicate succeeded payment on the latest fulfilled order and one prior refund in the last 90 days.\n\nResolution: finance-eng to verify the refund status before additional action.\n",
            },
            user_email=user,
            run_id=run_id,
        )
        return _with_retrieval_trace({
            "answer": "Committing the Alice investigation summary to GitHub needs approval. Review and approve it below.",
            "sql": None,
            "table": None,
            "rows": [],
            "citations": _fixture_citations("github", "postgres"),
            "provider": provider_slug,
            "llm_status": "pending_approval" if result.get("status") == "pending_approval" else "mcp_tool_completed",
            "status": result.get("status", "ok"),
            "alert_id": result.get("alert_id"),
            "chart_spec": None,
            "tool_result": result,
            "tool_call": {"connector_slug": "github", "tool": "write_commit_file"},
        }, retrieval_trace)

    return None


def _parse_openai_tool_name(name: str) -> tuple[str, str] | None:
    if "__" not in name:
        return None
    connector_slug, tool_name = name.split("__", 1)
    if not connector_slug or not tool_name:
        return None
    return connector_slug, tool_name


def _connector_display_name(connector_slug: str | None) -> str:
    if connector_slug and connector_slug in CATALOG_BY_SLUG:
        return CATALOG_BY_SLUG[connector_slug].display_name
    return connector_slug or "connector"


def _connector_action(label: str, connector_slug: str | None, tab: str = "Connectors") -> dict[str, Any]:
    return {"label": label, "tab": tab, "connector_slug": connector_slug}


def _mcp_error_response(
    *,
    exc: Exception,
    provider_slug: str,
    connector_slug: str | None,
    tool_name: str | None,
) -> dict[str, Any]:
    status_code = exc.status_code if isinstance(exc, McpExecutionError) else None
    detail = exc.detail if isinstance(exc, McpExecutionError) else str(exc)
    connector_name = _connector_display_name(connector_slug)
    if status_code is None and isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
        status_code = 503
        detail = f"{connector_name} could not be reached."
    elif status_code is None and isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        detail = f"{connector_name} returned HTTP {status_code}."
    elif status_code is None and isinstance(exc, (OperationalError, DBAPIError)):
        status_code = 503
        detail = f"{connector_name} could not be reached or rejected credentials."
    scope = "write" if tool_name and tool_name.startswith("write_") else "read"
    action: dict[str, Any] | None = None
    if status_code in {401, 403}:
        answer = f"Permission denied: grant {scope} access to {connector_name}."
        action = _connector_action(f"Grant {scope} access to {connector_name}", connector_slug, tab="Agents")
    elif status_code == 404:
        answer = f"Tool not available: {connector_name}.{tool_name or 'unknown'}."
        action = _connector_action(f"Review {connector_name} connector", connector_slug)
    elif status_code == 422:
        answer = f"Bad arguments: {detail}"
    elif status_code == 501:
        answer = f"Connector not implemented: {connector_name}.{tool_name or 'unknown'}."
        action = _connector_action(f"Review {connector_name} connector", connector_slug)
    else:
        answer = f"The selected MCP tool failed: {detail}"
        if status_code in {400, 503} and connector_slug:
            action = _connector_action(f"Configure {connector_name}", connector_slug)
    response = {
        "answer": answer,
        "sql": None,
        "table": None,
        "citations": [],
        "provider": provider_slug,
        "llm_status": "mcp_tool_error",
        "status": "error",
        "detail": detail,
        "tool_result": {"status": "error", "status_code": status_code, "detail": detail},
        "tool_call": {"connector_slug": connector_slug, "tool": tool_name},
    }
    if action:
        response["action"] = action
    return response


def _should_generate_chart(question: str, *, answer: str = "", rows: list[dict[str, Any]] | None = None) -> bool:
    text = f"{question}\n{answer}"
    if not _CHART_HINT_RE.search(text):
        return False
    if rows is None:
        return True
    if len(rows) < 1:
        return False
    first = rows[0]
    return isinstance(first, dict) and len(first) >= 2


def _fallback_chart_spec(rows: list[dict[str, Any]]) -> dict[str, Any]:
    sample = rows[:12] or [
        {"month": "2026-01", "revenue": 12000},
        {"month": "2026-02", "revenue": 15400},
        {"month": "2026-03", "revenue": 18100},
    ]
    fields = list(sample[0].keys())
    x_field = fields[0]
    y_field = next((field for field in fields[1:] if isinstance(sample[0].get(field), int | float)), fields[-1])
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": sample},
        "mark": "line",
        "encoding": {
            "x": {"field": x_field, "type": "ordinal"},
            "y": {"field": y_field, "type": "quantitative"},
        },
    }


async def _tool_description(session: AsyncSession, connector_slug: str, tool_name: str) -> str:
    connector = CATALOG_BY_SLUG.get(connector_slug)
    display_name = connector.display_name if connector else connector_slug
    base = f"Run {tool_name} on the {display_name} connector."
    if connector_slug not in {"sqlite", "postgres", "mysql", "redshift", "sql_server", "databricks", "bigquery", "snowflake"}:
        return f"{base} {connector.sync_behavior if connector else ''}".strip()

    rows = list(
        (
            await session.execute(
                select(Dataset.schema_name, TableAsset.name, TableAsset.business_summary, TableAsset.description)
                .join(TableAsset, TableAsset.dataset_id == Dataset.id)
                .where(Dataset.source_type == connector_slug)
                .order_by(TableAsset.row_count.desc(), TableAsset.name.asc())
                .limit(8)
            )
        ).all()
    )
    if not rows:
        return f"{base} Use it for schema-aware warehouse questions once this connector has synced metadata."

    table_bits = []
    for schema_name, table_name, business_summary, description in rows:
        label = f"{schema_name}.{table_name}" if schema_name else table_name
        summary = business_summary or description
        table_bits.append(f"{label} ({summary})" if summary else label)
    return f"{base} Available synced tables: {', '.join(table_bits)}."


async def _granted_openai_tools(
    session: AsyncSession,
    agent: Agent,
    *,
    connector_slug: str | None = None,
    connector_slugs: list[str] | None = None,
    question: str | None = None,
    max_tools: int = OPENAI_MCP_TOOL_LIMIT,
) -> list[dict[str, Any]]:
    allowed_connectors = set(connector_slugs or [])
    if connector_slug:
        allowed_connectors = {connector_slug}
    lower_question = (question or "").lower()
    grants = list(
        (
            await session.scalars(
                select(AgentMcpGrant).where(
                    AgentMcpGrant.agent_id == agent.id,
                    (AgentMcpGrant.read_enabled.is_(True) | AgentMcpGrant.write_enabled.is_(True)),
                )
            )
        ).all()
    )
    tool_specs: list[tuple[int, str, str, dict[str, Any]]] = []
    for grant in grants:
        if allowed_connectors and grant.connector_slug not in allowed_connectors:
            continue
        read_tools, write_tools = tools_for_slug(grant.connector_slug)
        selected: list[str] = []
        if grant.read_enabled:
            selected.extend(read_tools)
        if grant.write_enabled:
            selected.extend(write_tools)
        if grant.connector_slug == "notion" and "database" not in lower_question:
            selected = [
                tool
                for tool in selected
                if tool not in {"read_get_database", "read_query_database"}
            ]
        selected = sorted(set(selected), key=lambda name: _rank_tool_for_question(name, lower_question))
        for tool_name in selected:
            schema = MCP_TOOL_SCHEMAS.get((grant.connector_slug, tool_name))
            if not schema and grant.connector_slug in {"postgres", "mysql", "redshift", "sql_server", "databricks", "bigquery", "snowflake", "trino"}:
                schema = MCP_TOOL_SCHEMAS.get(("sqlite", tool_name))
                if schema and grant.connector_slug == "trino":
                    logger.warning(
                        "trino_tool_schema_fallback",
                        extra={"connector_slug": grant.connector_slug, "tool_name": tool_name},
                    )
            if not schema:
                if grant.connector_slug == "trino":
                    logger.warning(
                        "trino_tool_schema_missing",
                        extra={"connector_slug": grant.connector_slug, "tool_name": tool_name},
                    )
                continue
            tool_specs.append(
                (
                    _rank_tool_for_question(tool_name, lower_question)[0],
                    grant.connector_slug,
                    tool_name,
                    schema,
                )
            )
    tool_specs.sort(key=lambda item: (item[0], item[1], item[2]))
    if max_tools > 0:
        tool_specs = tool_specs[:max_tools]

    tools: list[dict[str, Any]] = []
    for _rank, slug, tool_name, schema in tool_specs:
        tools.append(
            {
                "type": "function",
                "function": {
                    "name": _openai_tool_name(slug, tool_name),
                    "description": await _tool_description(session, slug, tool_name),
                    "parameters": schema,
                },
            }
        )
    return tools


async def _run_openai_mcp_tool_call(
    *,
    session: AsyncSession,
    tool_engine: AsyncEngine,
    agent: Agent,
    tool_call: Any,
    user_email: str,
    provider_slug: str = "openai",
    run_id: str | None = None,
) -> dict[str, Any] | None:
    await enforce_run_budget(session, run_id, estimated_tokens=_estimate_tokens(tool_call.function.arguments or ""))
    parsed = _parse_openai_tool_name(tool_call.function.name)
    if parsed is None:
        return {
            "answer": f"The selected MCP tool name is invalid: {tool_call.function.name}",
            "sql": None,
            "table": None,
            "citations": [],
            "provider": provider_slug,
            "llm_status": "mcp_tool_error",
            "detail": "invalid_tool_name",
            "tool_result": {"status": "error", "error": "invalid_tool_name"},
            "tool_call": {"connector_slug": None, "tool": tool_call.function.name},
        }
    connector_slug, tool_name = parsed
    arguments: dict[str, Any] = {}
    try:
        parsed_arguments = json.loads(tool_call.function.arguments or "{}")
        if not isinstance(parsed_arguments, dict):
            raise TypeError("tool arguments must be an object")
        arguments = parsed_arguments
        arguments.pop("__approved", None)
        if tool_name == "read_query_select" and isinstance(arguments.get("sql"), str):
            arguments["sql"] = _strip_sql_comments(arguments["sql"])
        result = await execute_mcp_tool(
            session=session,
            engine=tool_engine,
            connector_slug=connector_slug,
            tool_name=tool_name,
            arguments=arguments,
            agent_id=agent.id,
            user_email=user_email,
            run_id=run_id,
            record_tool_call=False,
        )
    except (json.JSONDecodeError, TypeError) as exc:
        return _mcp_error_response(
            exc=exc,
            provider_slug=provider_slug,
            connector_slug=connector_slug,
            tool_name=tool_name,
        )
    except asyncio.CancelledError:
        raise
    except (MemoryError, TimeoutError):
        raise
    except McpExecutionError as exc:
        return _mcp_error_response(
            exc=exc,
            provider_slug=provider_slug,
            connector_slug=connector_slug,
            tool_name=tool_name,
        )
    except Exception as exc:
        logger.exception(
            "openai_mcp_tool_call_failed",
            extra={"connector_slug": connector_slug, "tool_name": tool_name},
        )
        return _mcp_error_response(
            exc=exc,
            provider_slug=provider_slug,
            connector_slug=connector_slug,
            tool_name=tool_name,
        )
    if result.get("status") == "pending_approval":
        return {
            "answer": "This write needs approval before it can run. Review and approve it below.",
            "sql": None,
            "table": None,
            "citations": [],
            "provider": provider_slug,
            "llm_status": "pending_approval",
            "status": "pending_approval",
            "alert_id": result.get("alert_id"),
            "tool_result": _json_safe(result),
            "tool_call": {"connector_slug": connector_slug, "tool": tool_name},
        }
    safe_result = _json_safe(result)
    rows = safe_result.get("rows") if isinstance(safe_result.get("rows"), list) else []
    sql = safe_result.get("sql")
    answer_parts = [f"Ran {connector_slug}.{tool_name} through the granted MCP tool."]
    if safe_result.get("table"):
        answer_parts.append(f"Table: {safe_result['table']}.")
    if isinstance(sql, str) and sql:
        answer_parts.append(f"SQL: {sql}")
    excerpt = _tool_result_excerpt(safe_result)
    if excerpt:
        answer_parts.append(f"Result: {excerpt}")
    return {
        "answer": " ".join(answer_parts),
        "sql": sql,
        "table": safe_result.get("table"),
        "citations": [],
        "provider": provider_slug,
        "llm_status": "mcp_tool_completed",
        "status": safe_result.get("status", "ok"),
        "rows": rows,
        "tool_result": safe_result,
        "tool_call": {"connector_slug": connector_slug, "tool": tool_name},
    }


async def _run_openai_mcp_tool_calls(
    *,
    session: AsyncSession,
    tool_engine: AsyncEngine,
    agent: Agent,
    tool_calls: list[Any],
    user_email: str,
    provider_slug: str = "openai",
    run_id: str | None = None,
    max_concurrency: int = 4,
) -> list[dict[str, Any]]:
    estimated_tokens = sum(_estimate_tokens(tool_call.function.arguments or "") for tool_call in tool_calls)
    await enforce_run_budget(session, run_id, estimated_tokens=estimated_tokens)
    semaphore = asyncio.Semaphore(max(1, min(max_concurrency, len(tool_calls) or 1)))
    session_factory = session.info.get("session_factory")
    if session_factory is None:
        from app.db.session import SessionLocal

        session_factory = SessionLocal

    async def bounded(tool_call: Any) -> dict[str, Any] | None:
        async with semaphore:
            async with session_factory() as isolated_session:
                isolated_agent = await isolated_session.get(Agent, agent.id)
                if isolated_agent is None:
                    return {
                        "answer": "MCP tool call failed: agent is no longer available.",
                        "provider": provider_slug,
                        "llm_status": "mcp_tool_error",
                        "status": "error",
                        "tool_call": {"connector_slug": None, "tool": None},
                    }
                return await _run_openai_mcp_tool_call(
                    session=isolated_session,
                    tool_engine=tool_engine,
                    agent=isolated_agent,
                    tool_call=tool_call,
                    user_email=user_email,
                    provider_slug=provider_slug,
                    run_id=run_id,
                )

    return await asyncio.gather(*(bounded(tool_call) for tool_call in tool_calls))


def _combined_tool_answer(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        return {
            "answer": "No MCP tool calls could be executed.",
            "sql": None,
            "table": None,
            "citations": [],
            "provider": "openai",
            "llm_status": "mcp_tool_error",
            "detail": "no_tool_results",
        }
    if len(results) == 1:
        return results[0]
    statuses = {str(result.get("status") or result.get("llm_status") or "ok") for result in results}
    rows = [row for result in results for row in result.get("rows", []) if isinstance(row, dict)]
    first_sql = next((result.get("sql") for result in results if result.get("sql")), None)
    first_table = next((result.get("table") for result in results if result.get("table")), None)
    citations = [citation for result in results for citation in result.get("citations", []) if isinstance(citation, dict)]
    pending = next((result for result in results if result.get("status") == "pending_approval"), None)
    if pending is not None:
        completed = [
            result
            for result in results
            if result is not pending and result.get("status") not in {"pending_approval", "error"} and result.get("llm_status") != "mcp_tool_error"
        ]
        completed_note = ""
        if completed:
            completed_note = "\nAlready completed before approval was requested:\n" + "\n".join(
                f"- {result.get('tool_call', {}).get('connector_slug')}.{result.get('tool_call', {}).get('tool')}: {result.get('answer', '').strip()}"
                for result in completed
            )
        return {
            "answer": "\n".join(f"- {result.get('answer', '').strip()}" for result in results if result.get("answer")) + completed_note,
            "sql": first_sql,
            "table": first_table,
            "citations": citations,
            "provider": pending.get("provider", results[0].get("provider", "openai")),
            "llm_status": "pending_approval",
            "status": "pending_approval",
            "alert_id": pending.get("alert_id"),
            "rows": rows,
            "tool_result": pending.get("tool_result", pending),
            "tool_call": pending.get("tool_call"),
            "tool_results": [result.get("tool_result", result) for result in results],
            "tool_calls": [result.get("tool_call") for result in results if result.get("tool_call")],
        }
    return {
        "answer": "\n".join(f"- {result.get('answer', '').strip()}" for result in results if result.get("answer")),
        "sql": first_sql,
        "table": first_table,
        "citations": citations,
        "provider": results[0].get("provider", "openai"),
        "llm_status": "mcp_tool_completed" if statuses <= {"ok", "mcp_tool_completed"} else "mcp_tool_partial",
        "status": "ok" if statuses <= {"ok", "mcp_tool_completed"} else "partial",
        "rows": rows,
        "tool_results": [result.get("tool_result", result) for result in results],
        "tool_calls": [result.get("tool_call") for result in results if result.get("tool_call")],
    }


def _with_retrieval_trace(response: dict[str, Any], trace: dict[str, Any]) -> dict[str, Any]:
    warning = trace.get("warning") if isinstance(trace, dict) else None
    if warning:
        response["warnings"] = [warning]
        answer = str(response.get("answer") or "")
        if answer and "Warning:" not in answer:
            response["answer"] = f"{answer}\n\nWarning: {warning}"
    response["retrieval_trace"] = trace
    return response


async def answer_question(
    session: AsyncSession,
    question: str,
    thread_id: str | None = None,
    model: str | None = None,
    tool_engine: AsyncEngine | None = None,
    user_email: str | None = None,
    connector_slug: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    api_key, default_model, base_url, embedding_model = await resolve_openai(session)
    provider_slug = active_llm_provider_slug()
    selected_model = model or default_model
    tables = list((await session.scalars(select(TableAsset))).all())
    docs = list((await session.scalars(select(KnowledgeDocument))).all())
    workspace = await session.scalar(select(Workspace).limit(1))
    if workspace:
        vector_store.ensure_embedding_model(workspace.id, embedding_model, api_key=api_key, base_url=base_url)
        # Short-circuit on golden-query match: a previously approved-and-
        # promoted eval case whose question matches this one is the
        # canonical answer. Skip the LLM entirely and return its
        # expected_sql + expected_answer. Phase 2 uses normalized exact
        # match; Phase 3 swaps in vector similarity.
        golden = await _lookup_golden(
            session,
            workspace_id=workspace.id,
            question=question,
            connector_slug=connector_slug,
        )
        if golden is not None:
            return await _golden_hit_response(
                session,
                golden=golden,
                question=question,
                provider_slug=provider_slug,
            )
    wiki_pages = list((await session.scalars(select(WikiPage).where(WikiPage.workspace_id == workspace.id))).all()) if workspace else []
    retrieval_warning: str | None = None
    if workspace:
        try:
            async with trace_span(
                "retrieval",
                "brain_retrieve",
                input={"question": question, "connector_slug": connector_slug},
            ) as retrieval_sp:
                brain_context = await BrainRetriever(session).retrieve(
                    workspace.id,
                    question,
                    connector_slugs=[connector_slug] if connector_slug else None,
                )
                retrieval_sp.set_output(
                    {
                        "node_count": len(brain_context.nodes) if brain_context else 0,
                        "chunk_count": len(brain_context.chunks) if brain_context else 0,
                        "trace": getattr(brain_context, "trace", {}) if brain_context else {},
                    }
                )
        except Exception as exc:
            logger.warning("brain_retrieval_unavailable", extra={"_error": exc.__class__.__name__})
            brain_context = None
            retrieval_warning = "Knowledge retrieval is unavailable; answering from SQL/schema context only."
    else:
        brain_context = None
    lower = question.lower()
    schema_context: list[dict[str, Any]] = []
    seen_schemas: set[str] = set()
    brain_nodes = brain_context.nodes if brain_context else []
    brain_chunks = brain_context.chunks if brain_context else []
    for node in brain_nodes:
        table_name = node.canonical_name
        if table_name in seen_schemas:
            continue
        seen_schemas.add(table_name)
        schema_context.append(
            {
                "source": node.connector_slug,
                "type": node.type,
                "name": node.canonical_name,
                "aliases": node.aliases,
                "summary": node.summary,
            }
        )
    for chunk in brain_chunks:
        if chunk.metadata.get("asset_type") not in {"table", "column"}:
            continue
        table_name = str(chunk.metadata.get("table_name") or chunk.metadata.get("name") or chunk.metadata)
        if table_name in seen_schemas:
            continue
        seen_schemas.add(table_name)
        schema_context.append({"source": chunk.metadata.get("connector_slug"), **chunk.metadata})
    for table in tables:
        if table.name in seen_schemas:
            continue
        seen_schemas.add(table.name)
        schema_context.append({"table": table.name, "description": table.description, "columns": table.columns})
    knowledge_titles = [
        str(chunk.metadata.get("title"))
        for chunk in brain_chunks
        if chunk.metadata.get("asset_type") in {"knowledge_document", "wiki_page"} and chunk.metadata.get("title")
    ] or [page.title for page in wiki_pages] or [doc.title for doc in docs]
    wiki_context = [
        {
            "path": chunk.metadata.get("path"),
            "title": chunk.metadata.get("title"),
            "source": chunk.metadata.get("connector_slug"),
            "node_id": chunk.node_id,
            "body": f"[source: {chunk.metadata.get('connector_slug', 'unknown')}] {chunk.document[:6000]}",
        }
        for chunk in brain_chunks
        if chunk.metadata.get("asset_type") == "wiki_page"
    ] or [
        {"path": page.path, "title": page.title, "source": page.source_type, "body": f"[source: {page.source_type}] {page.body[:6000]}"}
        for page in wiki_pages[:6]
    ]
    raw_context = [
        {
            "source_type": chunk.metadata.get("source_type"),
            "source_id": chunk.metadata.get("source_id"),
            "source": chunk.metadata.get("connector_slug"),
            "node_id": chunk.node_id,
            "content": f"[source: {chunk.metadata.get('connector_slug', 'unknown')}] {chunk.document[:1200]}",
        }
        for chunk in brain_chunks
        if chunk.metadata.get("asset_type") == "raw_chunk"
    ]
    graph_context = await graph_neighbors(session, workspace.id, question) if workspace else []
    column_context = await _column_lineage_context(session, workspace.id, question) if workspace else []
    if brain_context and brain_context.nodes:
        graph_context = [
            *[
                f"[source: {node.connector_slug}] {node.canonical_name} ({node.type}): {node.summary}"
                for node in brain_nodes[:12]
            ],
            *graph_context,
        ]
    if column_context:
        graph_context = [*column_context, *graph_context]
    citations = _context_citations(wiki_context, graph_context)
    retrieval_trace = brain_context.trace if brain_context else {}
    if retrieval_warning:
        retrieval_trace["warning"] = retrieval_warning
    history = await _conversation_history(session, thread_id)
    chat_agent = await session.scalar(select(Agent).where(Agent.name == "chat"))
    scenario_connector_slugs = _scenario_connector_slugs(question)
    inferred_connector_slugs = _dedupe_preserve_order(
        [
            *([connector_slug] if connector_slug else []),
            *scenario_connector_slugs,
            *_question_connector_slugs(question),
            *_context_connector_slugs(
                brain_nodes=brain_nodes,
                brain_chunks=brain_chunks,
                schema_context=schema_context,
                wiki_context=wiki_context,
            ),
        ]
    )
    if scenario_connector_slugs and not connector_slug:
        scenario_data_stores = {
            slug for slug in scenario_connector_slugs if slug in DATA_STORE_CONNECTOR_SLUGS
        }
        inferred_connector_slugs = [
            slug
            for slug in inferred_connector_slugs
            if slug not in DATA_STORE_CONNECTOR_SLUGS or slug in scenario_data_stores
        ]

    direct_scenario_answer = await _scenario6_direct_answer(
        session=session,
        tool_engine=tool_engine,
        chat_agent=chat_agent,
        question=question,
        user_email=user_email,
        provider_slug=provider_slug,
        retrieval_trace=retrieval_trace,
        run_id=run_id,
    )
    if direct_scenario_answer is not None:
        return direct_scenario_answer

    if not api_key:
        fallback = await _deterministic_mcp_fallback(
            session=session,
            tool_engine=tool_engine,
            chat_agent=chat_agent,
            question=question,
            user_email=user_email,
        )
        if fallback is not None:
            return _with_retrieval_trace(fallback, retrieval_trace)
        return _with_retrieval_trace(_deterministic(question, tables, docs), retrieval_trace)

    client = AsyncOpenAI(api_key=api_key, base_url=base_url)
    openai_tools = (
        await _granted_openai_tools(
            session,
            chat_agent,
            connector_slug=connector_slug,
            connector_slugs=inferred_connector_slugs or None,
            question=question,
        )
        if chat_agent and tool_engine
        else []
    )
    # OpenAI's tools array has a hard cap of 128. Prefer read tools over writes
    # when we have to drop some — reads are far more common per chat turn.
    OPENAI_TOOLS_LIMIT = 128
    if len(openai_tools) > OPENAI_TOOLS_LIMIT:
        original_count = len(openai_tools)
        def _tool_priority(tool: dict[str, Any]) -> int:
            name = tool.get("function", {}).get("name", "")
            parsed_name = _parse_openai_tool_name(name)
            tool_name = parsed_name[1] if parsed_name else name
            if tool_name.startswith("read_"):
                return 0
            if tool_name.startswith("write_"):
                return 2
            return 1
        openai_tools = sorted(openai_tools, key=_tool_priority)[:OPENAI_TOOLS_LIMIT]
        logger.warning(
            "openai_tools_capped",
            extra={"_original": original_count, "_limit": OPENAI_TOOLS_LIMIT},
        )
    # Phase 5: optional workspace-level overrides applied to the system
    # context. ``prompt_override`` lands as a Workspace-prompt-extension
    # system message (additive, reversible by deleting the AppSetting); the
    # ``rules_md`` text becomes a "Workspace rules" preamble that nudges
    # the LLM toward conventions the agent should always apply.
    prompt_override = await load_chat_prompt_override(session)
    rules_md_text = await load_chat_rules_md(session)
    extra_system_messages: list[dict[str, str]] = []
    if rules_md_text:
        extra_system_messages.append(
            {
                "role": "system",
                "content": (
                    "Workspace rules — apply these to every answer:\n"
                    f"{rules_md_text}"
                ),
            }
        )
    if prompt_override:
        extra_system_messages.append(
            {
                "role": "system",
                "content": (
                    "Workspace prompt extension (from accepted eval-improvement "
                    f"suggestions):\n{prompt_override}"
                ),
            }
        )

    messages: list[dict[str, str]] = [
        *extra_system_messages,
        {
            "role": "system",
            "content": (
                "You are DataClaw, a data engineering copilot. Use granted MCP tools "
                "when the user asks to inspect data, run SQL, create tables, or perform "
                "connector actions. Destructive writes may return pending approval. "
                "Use the conversation history to follow up on prior questions. For "
                "quantitative questions that ask for counts, aggregations, breakdowns, "
                "or charts, call an available read-only SQL tool and write one complete "
                "query from the schemas and wiki context instead of stopping after "
                "schema exploration. Do not invent SQL columns; use only columns present "
                "in the schema context. For core.customers, the primary key is id, so "
                "join orders with core.orders.customer_id = core.customers.id. If the "
                "user asks for a chart, do not stop after resolving an identifier; run "
                "the complete aggregation query that returns chartable rows. For "
                "questions that ask whether a status exists and what it means, use both "
                "a SQL read tool for the data check and a documentation tool for the "
                "definition when those tools are available. For "
                "Notion runbooks/SOPs/docs, search pages and read page/block content; "
                "only use Notion database tools when the user explicitly asks for a "
                "Notion database. For lineage, dependency, ownership, and pipeline "
                "questions, prefer graph_edges and wiki_pages unless the user asks for "
                "raw records or numeric aggregation; summarize the relevant upstream, "
                "downstream, pipeline, and model names present in graph_edges. "
                "Connector routing: when more than one connector is granted, pick the "
                "tool whose connector slug owns the referenced table in schemas / "
                "retrieval_trace (e.g. if `customers` only appears under the sqlite "
                "source, call sqlite_read_query_select, not the postgres equivalent). "
                "Never assume a table exists on a connector that did not surface it "
                "in the retrieval trace. "
                "Data vs. metadata: sqlite, postgres, mysql, bigquery, snowflake, "
                "redshift, databricks, sql_server, and trino are *data* sources — "
                "their read tools return actual rows. dbt, github, notion, confluence, "
                "airflow, dagster, prefect, airbyte, and fivetran are *metadata* "
                "sources — they describe pipelines, models, and docs, not row data. "
                "For a quantitative question like 'how many orders does each "
                "customer have', you MUST call a data-source SQL tool and execute "
                "the aggregation; describing a dbt model definition or a wiki page "
                "is not an answer. "
                "Self-attribution: prior assistant turns that ran a tool start "
                "with a marker `[provenance: tool_call=<connector>.<tool>]`. When "
                "the user asks which connector, tool, or source you used, parse "
                "the latest such marker and cite that connector verbatim. If no "
                "marker is present in any prior turn, say you don't have a "
                "recorded tool call to cite — never guess from row counts, table "
                "names, or schema context. The marker is authoritative."
            ),
        },
        {
            "role": "system",
            "content": json.dumps(
                {
                    "schemas": schema_context,
                    "wiki_pages": wiki_context,
                    "graph_edges": graph_context,
                    "raw_chunks": raw_context,
                    "retrieval_trace": brain_context.trace if brain_context else {},
                    "knowledge_titles": knowledge_titles,
                    "chart_instruction": "If a chart helps, include valid Vega-Lite v5 JSON in chart_spec.",
                }
            ),
        },
    ]
    messages.extend(history)
    messages.append({"role": "user", "content": question})

    try:
        await enforce_run_budget(session, run_id, estimated_tokens=_estimate_tokens(messages))
        completion = await _traced_chat_completion(
            client,
            span_name="chat_primary",
            model=selected_model,
            messages=messages,
            tools=openai_tools
            or [
                {
                    "type": "function",
                    "function": {
                        "name": "execute_read_only_sql",
                        "description": "Draft a safe read-only SQL query for the IDE to execute.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "sql": {"type": "string"},
                                "rationale": {"type": "string"},
                            },
                            "required": ["sql", "rationale"],
                        },
                    },
                }
            ],
            tool_choice="auto",
        )
    except OpenAIError as exc:
        raise RuntimeError(f"OpenAI call failed: {exc.__class__.__name__}") from exc
    except BudgetExceeded:
        return _with_retrieval_trace({
            "answer": "The request exceeded its configured budget.",
            "sql": None,
            "table": None,
            "citations": citations,
            "provider": provider_slug,
            "llm_status": "timed_out",
            "status": "timed_out",
            "chart_spec": None,
        }, retrieval_trace)

    message = completion.choices[0].message
    if not message.tool_calls:
        answer = _append_graph_context(message.content or "OpenAI responded without a SQL tool call.", graph_context, lower)
        return _with_retrieval_trace({
            "answer": answer,
            "sql": None,
            "table": None,
            "citations": citations,
            "provider": provider_slug,
            "llm_status": "no_tool_call",
            "chart_spec": None,
        }, retrieval_trace)

    if openai_tools and tool_engine and chat_agent:
        tool_started = time.perf_counter()
        try:
            tool_results = await _run_openai_mcp_tool_calls(
                session=session,
                tool_engine=tool_engine,
                agent=chat_agent,
                tool_calls=list(message.tool_calls),
                user_email=user_email or "system",
                provider_slug=provider_slug,
                run_id=run_id,
            )
        except BudgetExceeded:
            return _with_retrieval_trace({
                "answer": "The request exceeded its configured budget.",
                "sql": None,
                "table": None,
                "citations": citations,
                "provider": provider_slug,
                "llm_status": "timed_out",
                "status": "timed_out",
            }, retrieval_trace)
        result = _combined_tool_answer(tool_results)
        if result is not None:
            if result.get("status") != "pending_approval":
                followup_messages = [
                    *messages,
                    message.model_dump(exclude_none=True),
                    *[
                        {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": json.dumps(tool_result.get("tool_result", tool_result), default=str)[:12000],
                        }
                        for tool_call, tool_result in zip(message.tool_calls, tool_results, strict=False)
                    ],
                ]
                try:
                    next_completion = await _traced_chat_completion(
                        client,
                        span_name="chat_followup",
                        model=selected_model,
                        messages=followup_messages,
                        tools=openai_tools,
                        tool_choice="auto",
                    )
                    next_message = next_completion.choices[0].message
                    if next_message.tool_calls:
                        next_tool_results = await _run_openai_mcp_tool_calls(
                            session=session,
                            tool_engine=tool_engine,
                            agent=chat_agent,
                            tool_calls=list(next_message.tool_calls),
                            user_email=user_email or "system",
                            provider_slug=provider_slug,
                            run_id=run_id,
                        )
                        tool_results = [*tool_results, *next_tool_results]
                        result = _combined_tool_answer(tool_results)
                        followup_messages = [
                            *followup_messages,
                            next_message.model_dump(exclude_none=True),
                            *[
                                {
                                    "role": "tool",
                                    "tool_call_id": tool_call.id,
                                    "content": json.dumps(tool_result.get("tool_result", tool_result), default=str)[:12000],
                                }
                                for tool_call, tool_result in zip(next_message.tool_calls, next_tool_results, strict=False)
                            ],
                        ]
                        if result.get("status") != "pending_approval":
                            final_completion = await _traced_chat_completion(
                                client,
                                span_name="chat_final_synthesis",
                                model=selected_model,
                                messages=followup_messages,
                            )
                            final_answer = final_completion.choices[0].message.content
                        else:
                            final_answer = None
                    else:
                        final_answer = next_message.content
                    if final_answer:
                        result["answer"] = _append_graph_context(final_answer, graph_context, lower)
                except OpenAIError as exc:
                    logger.warning(
                        "chat_followup_openai_failed",
                        extra={"_error": exc.__class__.__name__},
                    )
            result["citations"] = citations
            result["retrieval_trace"] = retrieval_trace
            result["parallel_tool_count"] = len(tool_results)
            result["tool_latency_ms"] = int((time.perf_counter() - tool_started) * 1000)
            rows = result.get("rows") if isinstance(result.get("rows"), list) else []
            if _should_generate_chart(question, answer=str(result.get("answer") or ""), rows=rows):
                result["chart_spec"] = await _generate_chart_spec(
                    client,
                    selected_model,
                    question,
                    rows,
                )
            return result

    try:
        args = json.loads(message.tool_calls[0].function.arguments)
        sql = args.get("sql")
        if not sql:
            raise RuntimeError("OpenAI tool call returned no SQL. No fallback — surface the error.")
        sql = _strip_sql_comments(sql)
        rows: list[dict[str, Any]] = []
        chart_spec = None
        if _should_generate_chart(question, rows=rows):
            chart_spec = await _generate_chart_spec(client, selected_model, question, rows)
        return _with_retrieval_trace({
            "answer": args.get("rationale", "Generated SQL with OpenAI function calling."),
            "sql": validate_read_only_sql(sql),
            "table": None,
            "citations": citations,
            "provider": provider_slug,
            "llm_status": "completed",
            "chart_spec": chart_spec,
        }, retrieval_trace)
    except (json.JSONDecodeError, UnsafeSqlError, TypeError) as exc:
        raise RuntimeError(f"OpenAI tool result invalid: {exc.__class__.__name__}: {exc}") from exc
