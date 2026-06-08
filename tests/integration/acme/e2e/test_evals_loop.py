"""End-to-end Acme evals loop on Postgres + Notion + Airflow.

Walks the canonical investor-demo flow against the running app:

    connect/sync → chat → feedback → eval case → approve → promote-golden
    → chat short-circuit → run evals → diagnose → apply suggestion

This is the reproducible "we both see the same state" verification Sairam
asked for. Live execution requires the same prerequisites as every other
Acme e2e scenario (docker compose up for Postgres + Airflow, Notion
credentials in env, ``OPENAI_API_KEY`` set, ``make acme-seed`` already
run, ``RUN_ACME_E2E=1``).

The chat short-circuit step + run+diagnose+apply step pin SQL to a known
canonical query so the test fails honestly if the chat agent drifts off
the documented investor-demo path.
"""

from __future__ import annotations

from typing import Any

import pytest

from tests.integration.acme.e2e.helpers import (
    assert_answer_contains,
    assert_no_error_events,
    assert_tool_called,
    chat,
    chat_tool_calls,
    configure_connectors,
    event_ids,
    grant_chat_write,
)

pytestmark = pytest.mark.integration


# The canonical investor-demo question + golden artefacts. Kept inline so
# the test is the single source of truth for "the state two operators
# should reproduce".
ACME_QUESTION = "Why did churn spike last week and which DAG owns the calculation?"
ACME_GOLDEN_SQL = (
    "SELECT COUNT(*) FROM raw.customers WHERE status = 'churned' "
    "AND churned_at >= now() - interval '7 days'"
)
ACME_GOLDEN_ANSWER = (
    "Churn spiked last week because the acme_churn_calc DAG flagged a batch "
    "of paying customers who downgraded or had no successful order for 30 "
    "days. The canonical churn-count query is pinned above."
)


@pytest.mark.asyncio
async def test_evals_loop_postgres_notion_airflow(acme_client) -> None:
    # --- 1. connect/sync: postgres + notion + airflow land in status=ok ---
    await configure_connectors(acme_client, "postgres", "notion", "airflow")
    await grant_chat_write(acme_client, "postgres", "notion", "airflow")
    connectors = (await acme_client.get("/connectors")).json()
    statuses = {c["slug"]: c.get("status") for c in connectors if c["slug"] in {"postgres", "notion", "airflow"}}
    assert statuses == {"postgres": "ok", "notion": "ok", "airflow": "ok"}, statuses

    # --- 2. chat (turn 1): real LLM, real connectors ---
    before_ids = await event_ids(acme_client)
    payload = await chat(acme_client, ACME_QUESTION)
    assistant_message_id = payload.get("message_id")
    assert assistant_message_id, "chat response must include message_id for downstream feedback"
    calls = await chat_tool_calls(acme_client, payload, before_ids)
    # Sairam's brief says the demo must reach all three connectors. If the
    # chat agent drops one, fail loudly so the agent can be re-tuned.
    assert any(c in calls for c in {"notion.read_search_pages", "notion.read_get_page"}), calls
    assert any(c in calls for c in {"airflow.read_list_dags", "airflow.read_get_run", "airflow.read_get_dag_source"}), calls
    assert_tool_called(calls, "postgres.read_query_select")
    assert_answer_contains(payload, "acme_churn_calc")
    await assert_no_error_events(acme_client, before_ids)

    # --- 3. feedback (negative + comment) on the assistant message ---
    fb_resp = await acme_client.post(
        "/feedback",
        json={
            "chat_message_id": assistant_message_id,
            "sentiment": "negative",
            "comment": "we want the canonical churn-count SQL pinned",
        },
    )
    fb_resp.raise_for_status()
    feedback = fb_resp.json()
    assert feedback["sentiment"] == "negative"
    assert feedback["eval_case_id"] is None, "feedback shouldn't auto-attach to an eval case yet"

    # --- 4. eval case from feedback (status=candidate, expected_sql pinned) ---
    case_resp = await acme_client.post(
        "/evals/cases/from-feedback",
        json={
            "chat_message_id": assistant_message_id,
            "expected_sql": ACME_GOLDEN_SQL,
            "expected_answer": ACME_GOLDEN_ANSWER,
            "expected_connector_slug": "postgres",
        },
    )
    case_resp.raise_for_status()
    case = case_resp.json()
    eval_case_id = case["id"]
    assert case["status"] == "candidate"
    assert case["origin"] == "feedback"
    assert case["expected_sql"].startswith("SELECT COUNT(*)")

    # --- 5. approve → promote-golden ---
    approved = (await acme_client.post(f"/evals/cases/{eval_case_id}/approve")).json()
    assert approved["status"] == "approved"
    golden = (await acme_client.post(f"/evals/cases/{eval_case_id}/promote-golden")).json()
    assert golden["status"] == "golden"

    # --- 6. chat (turn 2): same question must short-circuit on golden hit ---
    second = await chat(acme_client, ACME_QUESTION)
    assert second.get("llm_status") == "golden_query_hit", (
        f"expected golden_query_hit short-circuit, got {second.get('llm_status')!r}"
    )
    assert second.get("sql") == ACME_GOLDEN_SQL
    assert any(
        c.get("type") == "golden_query_provenance" and c.get("eval_case_id") == eval_case_id
        for c in (second.get("citations") or [])
    ), second.get("citations")

    # --- 7. run evals against this golden case ---
    run_resp = await acme_client.post(
        "/evals/runs",
        json={"case_ids": [eval_case_id], "status_filter": ["golden"]},
    )
    run_resp.raise_for_status()
    batch = run_resp.json()
    assert batch["total"] >= 1
    run_ids: list[str] = batch.get("run_ids") or []
    assert run_ids, batch
    run_id = run_ids[0]

    # --- 8. diagnose (rules-only — deterministic + free) ---
    diag_resp = await acme_client.post(
        f"/evals/runs/{run_id}/diagnose",
        json={"use_llm": False},
    )
    diag_resp.raise_for_status()
    suggestions: list[dict[str, Any]] = diag_resp.json()
    # A passing golden-vs-golden run may legitimately produce zero suggestions.
    # That's a valid steady-state for the demo, so don't fail — but if there
    # IS a suggestion, the next step must drive it through Apply.
    if not suggestions:
        return

    # --- 9. apply the first actionable suggestion ---
    target = next(
        (s for s in suggestions if s.get("kind") in {"golden_query", "prompt_diff", "rules_md"}),
        suggestions[0],
    )
    apply_resp = await acme_client.post(f"/evals/suggestions/{target['id']}/apply")
    apply_resp.raise_for_status()
    applied = apply_resp.json()
    assert applied["status"] == "applied", applied
    assert applied["id"] == target["id"]
