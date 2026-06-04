"""Diagnose service — failure analysis + improvement suggestions.

Given a failed (or even passed-but-imperfect) ``EvalRun``, produce a list of
concrete, user-reviewable suggestions across six kinds:

* ``golden_query``         - "this question + this SQL ought to be canonical"
* ``prompt_diff``          - extend the chat system prompt
* ``rules_md``             - append a workspace rule (e.g. routing hint)
* ``tool_description_diff``- clarify an MCP tool description (preview-only v1)
* ``retrieval_context``    - add aliases / definitions (preview-only v1)
* ``connector_routing``    - hard-code a question→connector hint (preview-only v1)

Design:
* Always run a **rules-based pass** so we produce something useful even
  with no LLM credentials. The rules read failure_category + per-metric
  outcomes + the case/run rows.
* Optionally augment with one **LLM-judge call** (workspace's configured
  provider, smallest model) that returns JSON-structured suggestions; we
  merge them in with ``source="llm"``.
* Both passes are best-effort and isolated — if the LLM call throws, the
  rules-based suggestions still come through.

Nothing here mutates anything. ``EvalSuggestionService.apply`` is the only
thing that writes — and only when the user explicitly clicks Apply.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from dataclasses import dataclass, field

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.domain import EvalCase, EvalResult, EvalRun, EvalSuggestion

logger = logging.getLogger(__name__)


SUGGESTION_KINDS = (
    "golden_query",
    "prompt_diff",
    "rules_md",
    "tool_description_diff",
    "retrieval_context",
    "connector_routing",
)

# Apply paths fully wired in Phase 5 v1; the others are emitted but Apply
# returns 501 with a "preview only" message until follow-up work lands.
APPLY_SUPPORTED_KINDS = frozenset({"golden_query", "prompt_diff", "rules_md"})


# ---------- P2.5: in-flight background diagnose tracker ----------
#
# Process-local — fine for the single-process runtime; multi-worker
# deployments would need to migrate this to a DB row (next iteration).
_INFLIGHT_LOCK = threading.Lock()
_INFLIGHT: dict[str, str] = {}  # run_id -> "running" | "completed" | "error: ..."


def diagnose_status(run_id: str) -> str | None:
    """Return the current background diagnose status for a run id, or
    None when no background diagnose has been started recently."""
    with _INFLIGHT_LOCK:
        return _INFLIGHT.get(run_id)


def _mark_inflight(run_id: str, status: str) -> None:
    with _INFLIGHT_LOCK:
        _INFLIGHT[run_id] = status


def schedule_background_diagnose(run_id: str, *, use_llm: bool = True) -> None:
    """Fire-and-forget background diagnose. Spawns an asyncio task that
    opens its own session, runs DiagnoseService, and updates the
    in-flight marker. Callers should respond 202 immediately."""
    # Already in flight — no-op so duplicate clicks don't double-run.
    with _INFLIGHT_LOCK:
        if _INFLIGHT.get(run_id) == "running":
            return
        _INFLIGHT[run_id] = "running"

    async def _run() -> None:
        from app.db import session as _session_module
        try:
            async with _session_module.SessionLocal() as session:
                await DiagnoseService(session).diagnose(run_id, use_llm=use_llm)
            _mark_inflight(run_id, "completed")
        except DiagnoseError as exc:
            _mark_inflight(run_id, f"error: {exc}")
        except Exception as exc:
            logger.exception("background_diagnose_failed", extra={"_run_id": run_id})
            _mark_inflight(run_id, f"error: {exc.__class__.__name__}")

    try:
        asyncio.create_task(_run())
    except RuntimeError:
        # No running loop (e.g. called from sync context). Fall back to
        # marking error so the caller can retry; this shouldn't happen
        # from the API path which is always inside the FastAPI loop.
        _mark_inflight(run_id, "error: no_running_event_loop")


@dataclass
class _DraftSuggestion:
    """Producer-side draft before persistence. Mirrors EvalSuggestion fields
    that the rules / LLM paths know how to fill."""

    kind: str
    title: str
    rationale: str
    proposed_value: str
    current_value: str | None = None
    target: str | None = None
    confidence: float = 0.6
    source: str = "rules"
    apply_payload: dict = field(default_factory=dict)


class DiagnoseError(Exception):
    """Surface as 400/500 at the API layer."""


class DiagnoseService:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def diagnose(self, run_id: str, *, use_llm: bool = True) -> list[EvalSuggestion]:
        """Generate + persist suggestions for one run. Idempotent: if
        suggestions already exist for the run, return them as-is instead of
        duplicating. Pass ``use_llm=False`` to skip the LLM augmentation
        (useful for tests + CI)."""
        run = await self._session.get(EvalRun, run_id)
        if run is None:
            raise DiagnoseError(f"eval_run {run_id!r} not found")
        existing = list(
            (
                await self._session.scalars(
                    select(EvalSuggestion).where(EvalSuggestion.eval_run_id == run_id)
                )
            ).all()
        )
        if existing:
            return existing

        case = await self._session.get(EvalCase, run.eval_case_id)
        if case is None:
            raise DiagnoseError(
                f"eval_case {run.eval_case_id!r} for run {run_id!r} not found"
            )
        results = list(
            (
                await self._session.scalars(
                    select(EvalResult).where(EvalResult.eval_run_id == run_id)
                )
            ).all()
        )
        metrics_by_name = {r.metric: r for r in results}

        drafts = list(_rules_based(run, case, metrics_by_name))
        if use_llm:
            try:
                drafts.extend(await self._llm_augment(run, case, metrics_by_name, drafts))
            except Exception:
                logger.debug("diagnose_llm_failed", exc_info=True)

        # De-dupe by (kind, proposed_value) — rules + LLM frequently overlap
        # on the obvious cases (e.g. wrong_connector → routing rule).
        seen: set[tuple[str, str]] = set()
        dedup: list[_DraftSuggestion] = []
        for d in drafts:
            key = (d.kind, d.proposed_value.strip())
            if key in seen:
                continue
            seen.add(key)
            dedup.append(d)

        rows = [
            EvalSuggestion(
                eval_run_id=run_id,
                workspace_id=run.workspace_id,
                kind=d.kind,
                title=d.title,
                rationale=d.rationale,
                current_value=d.current_value,
                proposed_value=d.proposed_value,
                target=d.target,
                confidence=d.confidence,
                source=d.source,
                status="pending",
                apply_payload=d.apply_payload,
            )
            for d in dedup
        ]
        if rows:
            self._session.add_all(rows)
            await self._session.flush()
        await self._session.commit()
        return rows

    # ---------- LLM augmentation ----------

    async def _llm_augment(
        self,
        run: EvalRun,
        case: EvalCase,
        metrics_by_name: dict[str, EvalResult],
        rules_drafts: list[_DraftSuggestion],
    ) -> list[_DraftSuggestion]:
        """Best-effort LLM call returning structured suggestions. Skips
        gracefully when no OpenAI-compatible provider is configured."""
        from app.services.settings_store import resolve_openai

        api_key, model, base_url, _ = await resolve_openai(self._session)
        if not api_key or not model:
            return []
        try:
            from openai import AsyncOpenAI, OpenAIError
        except ImportError:
            return []

        # 30s timeout — diagnose runs synchronously inside the API request,
        # so a hung LLM call would block the user. The SDK accepts a per-call
        # `timeout` kwarg as a float (seconds); on timeout it raises an
        # OpenAIError subclass which we catch alongside the others.
        client = AsyncOpenAI(api_key=api_key, base_url=base_url, timeout=30.0)
        prompt = _build_llm_prompt(run, case, metrics_by_name, rules_drafts)
        try:
            completion = await client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are an eval-failure diagnostician. Read the "
                            "failed eval and return ONLY a JSON array of "
                            "suggestions matching the schema described."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.0,
                timeout=30.0,
            )
        except OpenAIError as exc:
            logger.debug("diagnose_openai_call_failed", extra={"_error": exc.__class__.__name__})
            return []
        except Exception as exc:
            # Defensive: timeouts raised by httpx may surface as
            # TimeoutException rather than an OpenAIError subclass on some
            # SDK versions. Diagnose must never break the API request.
            logger.debug("diagnose_call_unexpected", extra={"_error": exc.__class__.__name__})
            return []

        raw = (completion.choices[0].message.content or "").strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        items = parsed.get("suggestions") if isinstance(parsed, dict) else parsed
        if not isinstance(items, list):
            return []

        # Imported lazily to avoid a circular import (suggestions.py
        # imports SUGGESTION_KINDS from this module).
        from app.services.evals.suggestions import _looks_like_sql

        out: list[_DraftSuggestion] = []
        for item in items[:5]:  # cap LLM contributions
            if not isinstance(item, dict):
                continue
            kind = str(item.get("kind") or "").strip()
            if kind not in SUGGESTION_KINDS:
                continue
            proposed = str(item.get("proposed_value") or "").strip()
            if not proposed:
                continue
            # LLMs sometimes emit golden_query suggestions whose
            # proposed_value is a column listing or prose instead of a
            # query (we've seen "id (INTEGER), segment (TEXT)" in the
            # wild). Drop these — they'd just 422 on Apply.
            if kind == "golden_query" and not _looks_like_sql(proposed):
                logger.debug(
                    "diagnose_llm_golden_query_dropped",
                    extra={"_reason": "proposed_value not SQL"},
                )
                continue
            out.append(
                _DraftSuggestion(
                    kind=kind,
                    title=str(item.get("title") or f"LLM: {kind} suggestion")[:200],
                    rationale=str(item.get("rationale") or "")[:2000],
                    proposed_value=proposed[:4000],
                    current_value=item.get("current_value"),
                    target=str(item.get("target") or "")[:200] or None,
                    confidence=float(item.get("confidence") or 0.5),
                    source="llm",
                )
            )
        return out


# ---------- rules-based pass ----------


def _rules_based(
    run: EvalRun,
    case: EvalCase,
    metrics_by_name: dict[str, EvalResult],
) -> list[_DraftSuggestion]:
    drafts: list[_DraftSuggestion] = []
    cat = run.failure_category or ""

    # 1) Golden query — most useful when expected_sql exists, the case isn't
    #    already golden, and the SQL gating metric failed. Promoting to
    #    golden short-circuits the LLM next time (per the Phase 2 flow).
    sql_result = metrics_by_name.get("sql_correctness")
    if (
        case.expected_sql
        and case.status != "golden"
        and (cat in {"sql_error", "wrong_result", "missing_citation"} or
             (sql_result is not None and sql_result.passed is False))
    ):
        drafts.append(
            _DraftSuggestion(
                kind="golden_query",
                title="Promote this question + SQL as a golden eval",
                rationale=(
                    "The expected SQL is known and gating metrics didn't "
                    "pass. Promoting to golden will short-circuit chat to "
                    "this canonical SQL the next time the question is asked."
                ),
                proposed_value=case.expected_sql,
                current_value=run.actual_sql,
                target=f"eval_case:{case.id}",
                confidence=0.9,
                apply_payload={
                    "question": case.question,
                    "expected_sql": case.expected_sql,
                    "expected_answer": case.expected_answer,
                    "expected_connector_slug": case.expected_connector_slug,
                    "expected_tool": case.expected_tool,
                    "expected_citations": case.expected_citations or [],
                },
            )
        )

    # 2) Connector routing rule — when the chat reached for the wrong slug.
    if cat == "wrong_connector" and case.expected_connector_slug:
        rule = (
            f"For questions about {case.question[:80].rstrip('?')!r}, "
            f"prefer the `{case.expected_connector_slug}` connector."
        )
        drafts.append(
            _DraftSuggestion(
                kind="connector_routing",
                title=f"Route this intent to {case.expected_connector_slug}",
                rationale=(
                    "Chat selected the wrong connector. Adding an explicit "
                    "routing rule in workspace RULES.md teaches the agent "
                    "the right binding without changing prompts."
                ),
                proposed_value=rule,
                target=case.expected_connector_slug,
                confidence=0.75,
                apply_payload={"rule": rule},
            )
        )

    # 3) Tool description hint — when the wrong tool was chosen.
    if cat == "wrong_tool" and case.expected_tool:
        drafts.append(
            _DraftSuggestion(
                kind="tool_description_diff",
                title=f"Clarify when to use `{case.expected_tool}`",
                rationale=(
                    "Chat picked a different tool than expected. A clearer "
                    "description on the expected tool would steer the LLM "
                    "to it for similar questions."
                ),
                proposed_value=(
                    f"Use `{case.expected_tool}` for questions like: "
                    f"{case.question.strip()}"
                ),
                target=case.expected_tool,
                confidence=0.6,
                apply_payload={"tool": case.expected_tool},
            )
        )

    # 4) Retrieval context — when expected citations weren't recalled.
    if cat in {"bad_retrieval", "missing_citation"} and case.expected_citations:
        cited_targets = ", ".join(
            f"{c.get('source')}/{c.get('table')}"
            for c in (case.expected_citations or [])
            if isinstance(c, dict) and c.get("source") and c.get("table")
        )
        if cited_targets:
            drafts.append(
                _DraftSuggestion(
                    kind="retrieval_context",
                    title="Add retrieval aliases for the expected sources",
                    rationale=(
                        "Retrieval didn't surface the expected citations. "
                        "Adding aliases / definitions for these entities "
                        "should pull them into context for similar questions."
                    ),
                    proposed_value=(
                        f"Add aliases linking the question text to: {cited_targets}"
                    ),
                    target=cited_targets,
                    confidence=0.55,
                    apply_payload={"citations": case.expected_citations or []},
                )
            )

    # 5) Prompt nudge — last-resort generic for hallucination / formatting /
    #    unknown failures. Phrased as an additive instruction so applying
    #    it is reversible.
    if cat in {"hallucination", "formatting", "unknown"} or (
        cat == "" and not run.passed
    ):
        nudge = (
            "When a user asks a question similar to the following, ground the "
            "answer strictly in available retrieval context and SQL results; "
            "do not invent identifiers, columns, or row values.\n\n"
            f"Example question: {case.question.strip()}"
        )
        drafts.append(
            _DraftSuggestion(
                kind="prompt_diff",
                title="Add a grounding nudge to the chat system prompt",
                rationale=(
                    "Failure category suggests the model produced something "
                    "the schema/retrieval doesn't support. A short grounding "
                    "instruction tends to remove this class of error."
                ),
                proposed_value=nudge,
                target="chat:system_prompt_override",
                confidence=0.5,
                apply_payload={"append_to_prompt": nudge},
            )
        )

    # 6) Rules.md addition — when the user-supplied case implies a workspace
    #    convention (e.g. "always exclude internal accounts"). We only emit
    #    a generic prompt for the user to edit; the LLM pass usually fills in
    #    a specific rule when one applies.
    if cat == "wrong_result" and case.expected_sql and run.actual_sql:
        drafts.append(
            _DraftSuggestion(
                kind="rules_md",
                title="Capture a workspace rule for this difference",
                rationale=(
                    "The actual SQL ran but produced the wrong rows. There "
                    "is likely a workspace convention (filter, exclusion, "
                    "metric definition) the agent isn't applying. Edit the "
                    "proposed rule before applying."
                ),
                proposed_value=(
                    f"For questions like {case.question.strip()!r}, "
                    "ensure the query respects: <fill in the missing rule>."
                ),
                target="chat:rules_md",
                confidence=0.4,
                apply_payload={},
            )
        )
    return drafts


def _build_llm_prompt(
    run: EvalRun,
    case: EvalCase,
    metrics_by_name: dict[str, EvalResult],
    rules_drafts: list[_DraftSuggestion],
) -> str:
    metric_summary = {
        name: {"status": r.status, "score": r.score, "passed": r.passed}
        for name, r in metrics_by_name.items()
    }
    payload = {
        "question": case.question,
        "case_status": case.status,
        "case_origin": case.origin,
        "expected_answer": case.expected_answer,
        "expected_sql": case.expected_sql,
        "expected_connector_slug": case.expected_connector_slug,
        "expected_tool": case.expected_tool,
        "expected_citations": case.expected_citations or [],
        "actual_answer": run.actual_answer,
        "actual_sql": run.actual_sql,
        "failure_category": run.failure_category,
        "passed": run.passed,
        "metrics": metric_summary,
        "rules_based_suggestions_already_proposed": [
            {"kind": d.kind, "title": d.title} for d in rules_drafts
        ],
    }
    schema = """
Respond with a JSON object: {"suggestions": [ ... ]}

Each suggestion is an object with:
  kind         (one of: golden_query, prompt_diff, rules_md,
                tool_description_diff, retrieval_context,
                connector_routing)
  title        (short label, max 200 chars)
  rationale    (1-3 sentences explaining the suggestion)
  current_value   (the existing value being replaced, if any)
  proposed_value  (the new text to apply — full, ready to use)
  target       (optional: identifier of what is being changed)
  confidence   (float 0.0-1.0)

Avoid duplicating any suggestion already in
`rules_based_suggestions_already_proposed`. Prefer concrete,
actionable text over generic advice. Up to 3 suggestions.
""".strip()
    return f"{schema}\n\nFailing eval:\n{json.dumps(payload, indent=2, default=str)}"
