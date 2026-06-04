"""Eval batch runner.

For each selected ``EvalCase``:
  1. Reuse the workspace's hidden ``kind='eval'`` ChatThread (created
     lazily on first run; one per workspace keeps batches grouped).
  2. Invoke the same ``answer_question`` the user-facing chat invokes —
     traces, spans, and the golden-query short-circuit all flow naturally.
  3. Build an ``EvalContext`` from the resulting ``ChatMessage`` plus its
     ``chat_spans``.
  4. Run every registered metric, persist ``eval_runs`` + N ``eval_results``.
  5. Derive ``failure_category`` from the gating metric outcomes and stamp
     ``passed`` on the eval_run.

The runner is intentionally synchronous-per-case so a failure in one run
records cleanly and the next still executes. Concurrent batches sharing a
workspace are safe — each run creates its own assistant ``ChatMessage``
row inside the same eval thread.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import new_id
from app.models.domain import (
    ChatMessage,
    ChatSpan,
    ChatThread,
    EvalCase,
    EvalMetricThreshold,
    EvalResult,
    EvalRun,
    User,
    Workspace,
)
from app.services.evals.metrics import ALL_METRICS, DEFAULT_THRESHOLDS, EvalContext

logger = logging.getLogger(__name__)


EVAL_THREAD_TITLE_PREFIX = "eval:"


@dataclass
class BatchResult:
    batch_id: str
    workspace_id: str
    total: int = 0
    passed: int = 0
    failed: int = 0
    errored: int = 0
    run_ids: list[str] = field(default_factory=list)
    # Cost-budget abort: when the cumulative cost_usd across runs in this
    # batch crosses ``evals:config.batch_max_cost_usd``, the runner stops
    # before the next case. ``aborted`` and ``abort_reason`` surface in
    # the API + UI so users see "we stopped you at $5.02 of $5 budget"
    # instead of a silently-short batch.
    aborted: bool = False
    abort_reason: str | None = None
    cost_usd: float = 0.0


async def run_batch(
    session: AsyncSession,
    *,
    workspace_id: str,
    case_ids: list[str] | None = None,
    status_filter: tuple[str, ...] = ("approved", "golden"),
    user: User | None = None,
    repeat: int = 1,
) -> BatchResult:
    """Execute one batch. Returns aggregate counts + the list of created
    eval_run ids. Always commits a final transaction even on partial errors
    so the dashboard reflects what actually happened."""

    batch_id = _new_batch_id()
    workspace = await session.get(Workspace, workspace_id)
    if workspace is None:
        raise LookupError(f"Workspace {workspace_id} not found")

    cases = await _select_cases(session, workspace_id, case_ids, status_filter)
    if not cases:
        return BatchResult(batch_id=batch_id, workspace_id=workspace_id)

    thread = await _ensure_eval_thread(session, workspace_id, user)
    thresholds = await _load_thresholds(session, workspace_id)
    result = BatchResult(batch_id=batch_id, workspace_id=workspace_id)

    # Per-batch cost ceiling + concurrency from settings_store.evals:config.
    # 0 cost budget = unlimited. concurrency=1 keeps the original serial
    # semantics; >1 runs cases concurrently bounded by a semaphore.
    from app.services.settings_store import get_eval_config

    eval_config = await get_eval_config(session)
    cost_budget = float(eval_config.get("batch_max_cost_usd") or 0.0)
    max_concurrency = int(eval_config.get("batch_max_concurrency") or 1)

    # Track per-case repeat scores so flakiness can be computed at the end.
    repeat_scores: dict[str, list[float]] = {}

    repeat_count = max(1, int(repeat))

    # We commit the request session FIRST so per-case workers can see the
    # eval thread + reload workspace/user from their own sessions. Without
    # this commit the freshly-created thread row would be invisible to
    # other connections in WAL-mode SQLite.
    await session.commit()

    if max_concurrency <= 1:
        await _run_batch_serial(
            workspace_id=workspace_id,
            thread_id=thread.id,
            user_id=(user.id if user else None),
            cases=cases,
            batch_id=batch_id,
            thresholds=thresholds,
            result=result,
            cost_budget=cost_budget,
            repeat_count=repeat_count,
            repeat_scores=repeat_scores,
        )
    else:
        await _run_batch_concurrent(
            workspace_id=workspace_id,
            thread_id=thread.id,
            user_id=(user.id if user else None),
            cases=cases,
            batch_id=batch_id,
            thresholds=thresholds,
            result=result,
            cost_budget=cost_budget,
            max_concurrency=max_concurrency,
            repeat_count=repeat_count,
            repeat_scores=repeat_scores,
        )
    return result


async def _run_batch_serial(
    *,
    workspace_id: str,
    thread_id: str,
    user_id: str | None,
    cases: list[EvalCase],
    batch_id: str,
    thresholds: dict[str, float],
    result: BatchResult,
    cost_budget: float,
    repeat_count: int,
    repeat_scores: dict[str, list[float]],
) -> None:
    """Original serial path. Kept as a distinct function so the
    concurrency branch can stay clean."""
    aborted = False
    for _ in range(repeat_count):
        if aborted:
            break
        for case in cases:
            if cost_budget > 0 and result.cost_usd >= cost_budget:
                result.aborted = True
                result.abort_reason = (
                    f"cost_budget_exceeded: spent "
                    f"${result.cost_usd:.4f} of ${cost_budget:.4f} budget"
                )
                aborted = True
                break
            run = await _execute_in_fresh_session(
                workspace_id=workspace_id,
                thread_id=thread_id,
                user_id=user_id,
                case_id=case.id,
                batch_id=batch_id,
                thresholds=thresholds,
                repeat_scores=repeat_scores,
            )
            _aggregate(result, run)


async def _run_batch_concurrent(
    *,
    workspace_id: str,
    thread_id: str,
    user_id: str | None,
    cases: list[EvalCase],
    batch_id: str,
    thresholds: dict[str, float],
    result: BatchResult,
    cost_budget: float,
    max_concurrency: int,
    repeat_count: int,
    repeat_scores: dict[str, list[float]],
) -> None:
    """Bounded asyncio.gather. Each case runs in its own AsyncSession so
    flushes don't collide. Cost budget is enforced AFTER each batch wave
    (a soft cap with up to ``max_concurrency``-1 cases of overshoot
    worst-case — documented in the manual guide P2.T2)."""
    semaphore = asyncio.Semaphore(max_concurrency)
    aggregate_lock = asyncio.Lock()
    aborted = False

    async def _run_one(case: EvalCase) -> None:
        async with semaphore:
            if aborted:
                return
            run = await _execute_in_fresh_session(
                workspace_id=workspace_id,
                thread_id=thread_id,
                user_id=user_id,
                case_id=case.id,
                batch_id=batch_id,
                thresholds=thresholds,
                repeat_scores=repeat_scores,
            )
            async with aggregate_lock:
                _aggregate(result, run)

    for _ in range(repeat_count):
        if aborted:
            break
        await asyncio.gather(*(_run_one(case) for case in cases))
        async with aggregate_lock:
            if cost_budget > 0 and result.cost_usd >= cost_budget:
                aborted = True
                result.aborted = True
                result.abort_reason = (
                    f"cost_budget_exceeded: spent "
                    f"${result.cost_usd:.4f} of ${cost_budget:.4f} budget"
                )


def _aggregate(result: BatchResult, run: EvalRun) -> None:
    """Per-case aggregation. Called inside aggregate_lock in the
    concurrent path."""
    result.run_ids.append(run.id)
    result.total += 1
    if run.cost_usd:
        result.cost_usd += float(run.cost_usd)
    if run.error:
        result.errored += 1
    elif run.passed:
        result.passed += 1
    else:
        result.failed += 1


async def _execute_in_fresh_session(
    *,
    workspace_id: str,
    thread_id: str,
    user_id: str | None,
    case_id: str,
    batch_id: str,
    thresholds: dict[str, float],
    repeat_scores: dict[str, list[float]],
) -> EvalRun:
    """Open a fresh AsyncSession per case so concurrent cases don't
    contend on the same session's transaction. Reloads workspace + case +
    thread from the fresh session, then defers to ``_execute_one``.

    repeat_scores is a shared dict. The only mutation site is
    ``repeat_scores.setdefault(case_id, []).append(...)`` at the end of
    _execute_one; under the GIL the worst case is a lost append during
    repeat mode, which would degrade flakiness accuracy for one batch
    but not crash. We accept that trade-off rather than serializing
    everything under a lock."""
    from app.db import session as _session_module

    async with _session_module.SessionLocal() as session:
        workspace = await session.get(Workspace, workspace_id)
        case = await session.get(EvalCase, case_id)
        thread = await session.get(ChatThread, thread_id)
        user = await session.get(User, user_id) if user_id else None
        if workspace is None or case is None or thread is None:
            raise LookupError("workspace/case/thread missing in fresh session")
        run = await _execute_one(
            session,
            workspace=workspace,
            thread=thread,
            case=case,
            batch_id=batch_id,
            thresholds=thresholds,
            user=user,
            repeat_scores=repeat_scores,
        )
        await session.commit()
        return run


# ---------- helpers ----------


def _new_batch_id() -> str:
    return uuid.uuid4().hex[:24]


async def _select_cases(
    session: AsyncSession,
    workspace_id: str,
    case_ids: list[str] | None,
    status_filter: tuple[str, ...],
) -> list[EvalCase]:
    stmt = select(EvalCase).where(EvalCase.workspace_id == workspace_id)
    if case_ids:
        stmt = stmt.where(EvalCase.id.in_(case_ids))
    else:
        stmt = stmt.where(EvalCase.status.in_(status_filter))
    return list((await session.scalars(stmt)).all())


async def _ensure_eval_thread(
    session: AsyncSession,
    workspace_id: str,
    user: User | None,
) -> ChatThread:
    """One eval thread per workspace + creating user so the Sessions
    sidebar's `kind='user'` filter naturally excludes it."""
    if user is None:
        # Fall back to any user in the workspace — required for the FK.
        user = await session.scalar(select(User).limit(1))
        if user is None:
            raise LookupError("No user in DB to own the eval thread")
    thread = await session.scalar(
        select(ChatThread).where(
            ChatThread.workspace_id == workspace_id,
            ChatThread.user_id == user.id,
            ChatThread.kind == "eval",
        )
    )
    if thread is None:
        thread = ChatThread(
            workspace_id=workspace_id,
            user_id=user.id,
            title=f"{EVAL_THREAD_TITLE_PREFIX}runner",
            kind="eval",
        )
        session.add(thread)
        await session.flush()
    return thread


async def _load_thresholds(session: AsyncSession, workspace_id: str) -> dict[str, float]:
    overrides = {
        row.metric: row.pass_threshold
        for row in (
            await session.scalars(
                select(EvalMetricThreshold).where(
                    EvalMetricThreshold.workspace_id == workspace_id
                )
            )
        ).all()
    }
    return {**DEFAULT_THRESHOLDS, **overrides}


async def _execute_one(
    session: AsyncSession,
    *,
    workspace: Workspace,
    thread: ChatThread,
    case: EvalCase,
    batch_id: str,
    thresholds: dict[str, float],
    user: User | None,
    repeat_scores: dict[str, list[float]],
) -> EvalRun:
    """Run one case end-to-end. Persists eval_run + eval_results within the
    caller's session; final commit happens at the batch boundary."""

    # Import lazily — chat.py is heavy and only needed when actually executing.
    from app.services.agents.chat import answer_question
    from app.services.observability.tracing import (
        allocate_chat_message_id,
        chat_trace,
    )

    started = datetime.now(UTC)
    assistant_message_id = allocate_chat_message_id()
    response: dict[str, Any] | None = None
    runner_error: str | None = None

    try:
        async with chat_trace(
            session=session,
            chat_message_id=assistant_message_id,
            workspace_id=workspace.id,
            user_id=user.id if user else None,
            thread_id=thread.id,
            question=case.question,
        ):
            response = await answer_question(
                session,
                case.question,
                thread.id,
                None,
                tool_engine=None,
                user_email=user.email if user else "evals-runner",
                connector_slug=case.expected_connector_slug,
            )
    except Exception as exc:
        logger.exception("eval_runner_chat_failed", extra={"_case_id": case.id})
        runner_error = f"{exc.__class__.__name__}: {exc}"

    # Persist the assistant ChatMessage so eval runs show up in /chat-threads/{id}
    # debugging if needed. Use the pre-allocated id so trace_id round-trips.
    if response is not None:
        msg = ChatMessage(
            id=assistant_message_id,
            thread_id=thread.id,
            role="assistant",
            content=response.get("answer", "") or "",
            sql=response.get("sql"),
            provider=response.get("provider"),
            llm_status=response.get("llm_status"),
            citations=list(response.get("citations") or []),
            rows=list(response.get("rows") or []),
            chart_spec=response.get("chart_spec"),
            action=response.get("action"),
            retrieval_trace=response.get("retrieval_trace") or {},
            trace_id=assistant_message_id.replace("-", ""),
        )
        session.add(msg)
        try:
            await session.flush()
        except Exception:
            logger.exception("eval_runner_persist_message_failed")

    duration_ms = int((datetime.now(UTC) - started).total_seconds() * 1000)

    # Build the EvalContext from the (best-effort) response + spans, plus
    # the per-workspace judge config so Ragas-backed metrics can call out.
    spans = await _load_spans(session, assistant_message_id) if response else []
    judge_config = await _resolve_judge_config(session)
    ctx = _build_context(
        case=case,
        response=response,
        spans=spans,
        duration_ms=duration_ms,
        previous_run_passed=await _previous_run_passed(session, case.id),
        judge_config=judge_config,
    )

    # Score every metric. Capture pass-gating outcomes for failure category.
    gating_results: list[tuple[str, "_GatingOutcome"]] = []
    run = EvalRun(
        workspace_id=workspace.id,
        batch_id=batch_id,
        eval_case_id=case.id,
        chat_message_id=assistant_message_id if response else None,
        actual_answer=(response or {}).get("answer") if response else None,
        actual_sql=(response or {}).get("sql") if response else None,
        actual_citations=list((response or {}).get("citations") or []),
        actual_result_preview=list((response or {}).get("rows") or []),
        actual_result_hash=_hash_rows(list((response or {}).get("rows") or [])),
        langfuse_trace_id=assistant_message_id.replace("-", "") if response else None,
        duration_ms=duration_ms,
        prompt_tokens=ctx.prompt_tokens,
        completion_tokens=ctx.completion_tokens,
        cost_usd=ctx.cost_usd,
        model=ctx.model,
        error=runner_error,
    )
    session.add(run)
    await session.flush()

    if runner_error is not None:
        run.passed = False
        run.failure_category = "runner_error"
        # Still record an EvalResult row so the run-detail page renders
        # something coherent.
        session.add(
            EvalResult(
                eval_run_id=run.id,
                metric="runner",
                status="error",
                detail={"error": runner_error},
            )
        )
        return run

    # Track latest sql_correctness for flakiness aggregation.
    per_score_for_flakiness: float | None = None

    for metric in ALL_METRICS:
        # Inject flakiness samples lazily when we enter the flakiness metric.
        if metric.name == "flakiness":
            ctx.repeat_scores = list(repeat_scores.get(case.id, []))
        threshold = thresholds.get(metric.name, 0.5)
        try:
            outcome = metric.score(ctx, threshold=threshold)
        except Exception as exc:
            logger.exception("eval_metric_failed", extra={"_metric": metric.name, "_case": case.id})
            outcome_detail = {"error": f"{exc.__class__.__name__}: {exc}"}
            session.add(
                EvalResult(
                    eval_run_id=run.id,
                    metric=metric.name,
                    status="error",
                    detail=outcome_detail,
                )
            )
            if metric.gates_pass:
                gating_results.append((metric.name, _GatingOutcome("error", 0.0, False)))
            continue
        session.add(
            EvalResult(
                eval_run_id=run.id,
                metric=outcome.metric,
                score=outcome.score,
                passed=outcome.passed,
                status=outcome.status,
                detail=outcome.detail,
            )
        )
        if metric.gates_pass:
            gating_results.append((metric.name, _GatingOutcome.from_score(outcome)))
        if metric.name == "sql_correctness" and outcome.status == "ok":
            per_score_for_flakiness = float(outcome.score or 0.0)

    if per_score_for_flakiness is not None:
        repeat_scores.setdefault(case.id, []).append(per_score_for_flakiness)

    # Aggregate gating outcomes -> overall pass + failure category.
    run.passed, run.failure_category = _derive_pass_and_category(gating_results, ctx)
    return run


# ---------- context construction ----------


async def _resolve_judge_config(session: AsyncSession) -> dict[str, Any] | None:
    """Per the locked decisions: judge = workspace's configured LLM at its
    cheapest tier. We reuse ``resolve_openai`` (which already returns the
    api_key + default_model + base_url) and stamp it onto EvalContext so
    judge metrics can hand it to Ragas. Returns None when no provider is
    configured — judges skip cleanly in that case."""
    try:
        from app.services.settings_store import resolve_openai
    except Exception:
        return None
    try:
        api_key, model, base_url, embedding_model = await resolve_openai(session)
    except Exception:
        logger.debug("judge_config_resolve_failed", exc_info=True)
        return None
    if not api_key or not model:
        return None
    return {
        "api_key": api_key,
        "model": model,
        "base_url": base_url,
        "embedding_model": embedding_model,
    }


def _build_context(
    *,
    case: EvalCase,
    response: dict[str, Any] | None,
    spans: list[ChatSpan],
    duration_ms: int,
    previous_run_passed: bool | None,
    judge_config: dict[str, Any] | None = None,
) -> EvalContext:
    response = response or {}
    tool_call = response.get("tool_call") or {}
    actual_connector_slug = (
        tool_call.get("connector_slug")
        if isinstance(tool_call, dict)
        else None
    )
    actual_tool = (
        f"{actual_connector_slug}.{tool_call.get('tool')}"
        if actual_connector_slug and tool_call.get("tool")
        else None
    )

    # Pull tokens/cost/model from any LLM span.
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    cost_usd: float | None = None
    model: str | None = None
    retrieval_candidates: list[str] = []
    for sp in spans:
        if sp.kind == "llm":
            usage = sp.usage or {}
            prompt_tokens = prompt_tokens or usage.get("input_tokens")
            completion_tokens = completion_tokens or usage.get("output_tokens")
            total_tokens = total_tokens or usage.get("total_tokens")
            cost_usd = cost_usd if cost_usd is not None else usage.get("cost_usd")
            model = model or sp.model
        elif sp.kind == "retrieval":
            output = sp.output or {}
            trace = output.get("trace") if isinstance(output, dict) else None
            if isinstance(trace, dict):
                cand = trace.get("candidate_ids") or []
                if isinstance(cand, list):
                    retrieval_candidates.extend(str(x) for x in cand)
                expanded = trace.get("expanded_ids") or []
                if isinstance(expanded, list):
                    retrieval_candidates.extend(str(x) for x in expanded)

    rows = list(response.get("rows") or [])
    return EvalContext(
        expected_answer=case.expected_answer,
        expected_sql=case.expected_sql,
        expected_connector_slug=case.expected_connector_slug,
        expected_tool=case.expected_tool,
        expected_citations=list(case.expected_citations or []),
        expected_result_hash=case.expected_result_hash,
        expected_result_preview=list(case.expected_result_preview or []),
        actual_answer=response.get("answer"),
        actual_sql=response.get("sql"),
        actual_connector_slug=actual_connector_slug,
        actual_tool=actual_tool,
        actual_citations=list(response.get("citations") or []),
        actual_result_hash=_hash_rows(rows),
        actual_result_preview=rows,
        retrieval_candidates=retrieval_candidates,
        chat_spans=[{"kind": sp.kind, "name": sp.name} for sp in spans],
        duration_ms=duration_ms,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
        cost_usd=cost_usd,
        model=model,
        previous_run_passed=previous_run_passed,
        question=case.question,
        judge_config=judge_config,
    )


async def _load_spans(session: AsyncSession, chat_message_id: str) -> list[ChatSpan]:
    return list(
        (
            await session.scalars(
                select(ChatSpan)
                .where(ChatSpan.chat_message_id == chat_message_id)
                .order_by(ChatSpan.started_at.asc())
            )
        ).all()
    )


async def _previous_run_passed(session: AsyncSession, case_id: str) -> bool | None:
    prev = await session.scalar(
        select(EvalRun)
        .where(EvalRun.eval_case_id == case_id)
        .order_by(EvalRun.created_at.desc())
        .limit(1)
    )
    return prev.passed if prev is not None else None


def _hash_rows(rows: list[dict]) -> str | None:
    if not rows:
        return None
    # Sort rows + keys for stable hashing — eval comparisons should be
    # order-insensitive (the LLM's ORDER BY may not be deterministic).
    try:
        canonical = sorted(
            json.dumps(row, sort_keys=True, default=str) for row in rows
        )
    except Exception:
        return None
    digest = hashlib.sha256("\n".join(canonical).encode()).hexdigest()
    return digest


# ---------- pass / failure category derivation ----------


@dataclass(frozen=True)
class _GatingOutcome:
    status: str
    score: float
    passed: bool

    @classmethod
    def from_score(cls, outcome: Any) -> "_GatingOutcome":
        return cls(
            status=outcome.status,
            score=float(outcome.score or 0.0),
            passed=bool(outcome.passed),
        )


_CATEGORY_ORDER = (
    "connector_accuracy",
    "tool_accuracy",
    "retrieval_quality",
    "sql_correctness",
    "result_accuracy",
    "citation_accuracy",
)
_CATEGORY_MAP = {
    "connector_accuracy": "wrong_connector",
    "tool_accuracy": "wrong_tool",
    "retrieval_quality": "bad_retrieval",
    "sql_correctness": "sql_error",
    "result_accuracy": "wrong_result",
    "citation_accuracy": "missing_citation",
}


def _derive_pass_and_category(
    gating_results: list[tuple[str, _GatingOutcome]],
    ctx: EvalContext,
) -> tuple[bool, str | None]:
    by_name = {name: outcome for name, outcome in gating_results}
    # If everything that scored is passing (skipped doesn't gate), the run passes.
    gating = [o for o in by_name.values() if o.status == "ok"]
    if not gating:
        # Nothing scoreable (e.g. case had no expected_* fields). Treat as
        # passing-but-uninformative; surface a category to signal it.
        return True, None
    if all(o.passed for o in gating):
        return True, None

    # Walk the prioritized list — first metric that failed (with status=ok)
    # is the category. This matches the order users will mentally apply
    # ("did it use the right tool? did it generate the right SQL?").
    for metric in _CATEGORY_ORDER:
        outcome = by_name.get(metric)
        if outcome and outcome.status == "ok" and not outcome.passed:
            return False, _CATEGORY_MAP[metric]
    return False, "unknown"
