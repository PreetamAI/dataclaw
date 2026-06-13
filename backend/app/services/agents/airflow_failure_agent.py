from __future__ import annotations

import logging
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.domain import Agent, AgentRun
from app.services.agents.alert_llm_filter import should_alert
from app.services.agents.monitoring_common import (
    MONITORING_AGENTS,
    connector_credentials,
    create_alert_once,
    enabled_monitoring_configs,
    finish_agent_run,
    workspace_or_raise,
)
from app.services.connectors.adapters import adapter_for

logger = logging.getLogger("dataclaw.agents.airflow_failure")

AGENT_NAME = "airflow_failure_agent"
FAILED_STATES = {"failed", "upstream_failed"}


def _run_identity(dag: dict[str, Any], run: dict[str, Any]) -> tuple[str, str] | None:
    dag_id = str(dag.get("dag_id") or run.get("dag_id") or "")
    run_id = str(run.get("dag_run_id") or run.get("run_id") or run.get("execution_date") or run.get("logical_date") or "")
    if not dag_id or not run_id:
        return None
    return dag_id, run_id


async def run_airflow_failure_agent(session: AsyncSession) -> AgentRun:
    workspace = await workspace_or_raise(session)
    alerting_agent = await session.scalar(
        select(Agent).where(
            Agent.workspace_id == workspace.id,
            Agent.name == "alerting",
            Agent.kind == "background",
            Agent.enabled.is_(True),
        )
    )
    configs = await enabled_monitoring_configs(
        session,
        workspace.id,
        AGENT_NAME,
        MONITORING_AGENTS[AGENT_NAME]["connectors"],
    )
    created = 0
    checked = 0
    unreachable = 0
    for config, connector in configs:
        try:
            payload = await adapter_for("airflow").fetch_content(connector_credentials(connector))  # type: ignore[attr-defined]
        except httpx.HTTPError as exc:
            unreachable += 1
            logger.warning(
                "airflow_failure_agent.connector_unreachable",
                extra={"_connector": connector.slug, "_error": exc.__class__.__name__},
            )
            continue
        for dag in payload.get("dags") or []:
            for run in dag.get("recent_runs") or []:
                checked += 1
                state = str(run.get("state") or "").lower()
                if state not in FAILED_STATES:
                    continue
                identity = _run_identity(dag, run)
                if identity is None:
                    continue
                dag_id, run_id = identity
                rationale = "LLM filter not configured."
                if alerting_agent is not None:
                    keep, rationale = await should_alert(
                        session,
                        alerting_agent,
                        {
                            "connector": "airflow",
                            "dag_id": dag_id,
                            "run_id": run_id,
                            "state": state,
                        },
                    )
                    if not keep:
                        continue
                alert = await create_alert_once(
                    session,
                    config,
                    workspace_id=workspace.id,
                    fingerprint=f"airflow_failure:{connector.id}:{dag_id}:{run_id}",
                    severity="critical",
                    title=f"{dag_id} failed in Airflow",
                    detail=f"Airflow DAG {dag_id} has failed run {run_id}. LLM filter: {rationale}",
                    requires_approval=False,
                )
                if alert is not None:
                    created += 1

    run = AgentRun(
        workspace_id=workspace.id,
        agent_name=MONITORING_AGENTS[AGENT_NAME]["display_name"],
        status="completed",
        summary=(
            f"Checked {checked} Airflow runs; created {created} alerts."
            + (f" {unreachable} connector(s) unreachable." if unreachable else "")
        ),
        timeline=[
            {"step": "load_configs", "status": "completed", "detail": f"{len(configs)} enabled configs."},
            {"step": "scan_runs", "status": "completed", "detail": f"{checked} runs checked."},
        ],
    )
    return await finish_agent_run(session, run)
